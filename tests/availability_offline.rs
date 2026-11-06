//! 收报超时 → 离线历史端到端：真实摄取写 `last_seen_at`，检测器按间隔开/关记录。
//!
//! 这条链路不再读可用性桶：观测点看不到节点不等于主端收不到上报。

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::OptionalExtension;

use santaizi_dashboard::config::{Config, new_config_handle};
use santaizi_dashboard::db::{self, Db};
use santaizi_dashboard::proto;
use santaizi_dashboard::repo::bindings;
use santaizi_dashboard::secret;
use santaizi_dashboard::state::AppState;
use santaizi_dashboard::telemetry::offline::OfflineHistoryPolicy;
use santaizi_dashboard::telemetry::offline_detector::OfflineDetector;
use santaizi_dashboard::telemetry::runtime;
use santaizi_dashboard::telemetry::store::Store;

const SERVER_ID: i64 = 7;
const THRESHOLD_NANOS: i64 = 60_000_000_000;
const BASE: i64 = 1_700_000_000_000_000_000;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "santaizi-offline-e2e-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

struct Fixture {
    db: Db,
    detector: OfflineDetector,
    _dir: PathBuf,
}

fn fixture_conf() -> Config {
    let mut conf = Config::default();
    conf.enable_offline_history = true;
    conf.offline_threshold_seconds = 60;
    conf.offline_check_interval_seconds = 10;
    conf.offline_merge_gap_seconds = 0;
    conf
}

async fn fixture(tag: &str) -> Fixture {
    let dir = temp_dir(tag);
    let db = db::migrate::open_primary(dir.join("panel.db").to_str().expect("db path"))
        .expect("open primary db");
    let mut conf = fixture_conf();
    conf.mode = "primary".to_string();
    conf.telemetry.data_dir = dir.to_string_lossy().into_owned();
    conf.telemetry.signing_key_path = dir
        .join("telemetry-signing.key")
        .to_string_lossy()
        .into_owned();
    conf.telemetry.secret_key_path = dir
        .join("business-secret.key")
        .to_string_lossy()
        .into_owned();
    let state = AppState::primary(conf.clone(), db.clone())
        .await
        .expect("app state");
    drop(state);

    let ciphertext = secret::encrypt_secret("test-client-secret").expect("encrypt");
    db.write(|tx| {
        tx.execute(
            "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) \
             VALUES (?1, 'node-7', 'tag1', ?2, 1, 0)",
            rusqlite::params![SERVER_ID, &ciphertext],
        )?;
        bindings::bind_server_node_for_protocol(tx, SERVER_ID, &node(), BASE, true)?;
        Ok(())
    })
    .expect("seed");

    Fixture {
        detector: OfflineDetector::new(db.clone(), new_config_handle(conf), None),
        db,
        _dir: dir,
    }
}

fn node() -> Vec<u8> {
    vec![0x41; 16]
}

fn session() -> Vec<u8> {
    vec![0x42; 16]
}

fn policy() -> OfflineHistoryPolicy {
    OfflineHistoryPolicy {
        enabled: true,
        merge_gap_seconds: 0,
    }
}

/// 一帧真实 Host 事件：走 `Store::ingest` + `apply_event_runtime`，与主端 Ingest 同序。
fn report(fixture: &Fixture, store: &Store, sequence: u64, received_at_nanos: i64) {
    let event_id = Store::event_id(&node(), &session(), sequence).expect("event id");
    let batch = proto::TelemetryBatch {
        records: vec![proto::TelemetryRecord {
            record: Some(proto::telemetry_record::Record::Event(
                proto::TelemetryEvent {
                    event_id: event_id.to_vec(),
                    node_uuid: node(),
                    session_id: session(),
                    sequence,
                    event_type: proto::TelemetryEventType::Host as i32,
                    priority: proto::TelemetryPriority::P1Important as i32,
                    collected_at_unix_nano: received_at_nanos,
                    protocol_version: 2,
                    source_protocol: proto::SourceProtocol::SantaiziV2 as i32,
                    reliability: proto::Reliability::ReliableReplay as i32,
                    payload: Some(proto::telemetry_event::Payload::Host(proto::Host {
                        ip: "10.9.9.9".to_string(),
                        version: "0.1.0".to_string(),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )),
        }],
    };
    let offline = OfflineHistoryPolicy::from_conf(&fixture_conf());
    let result = fixture
        .db
        .write(|tx| store.ingest(tx, &batch, "primary", received_at_nanos, 0))
        .expect("ingest");
    for event in &result.fresh_events {
        fixture
            .db
            .write(|tx| {
                runtime::apply_event_runtime(tx, event, received_at_nanos, offline).map(|_| ())
            })
            .expect("apply runtime");
    }
}

/// 运行态 `last_seen_at` 应等于主端接收钟（纳秒）。
fn last_seen_nanos(fixture: &Fixture) -> Option<i64> {
    fixture
        .db
        .read(|conn| {
            conn.query_row(
                "SELECT last_seen_at FROM server_runtimes WHERE server_id = ?1",
                [SERVER_ID],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map_err(Into::into)
        })
        .expect("read last_seen")
        .flatten()
        .as_deref()
        .and_then(santaizi_dashboard::util::time::sqlite_to_unix_nanos)
}

#[tokio::test]
async fn report_gap_opens_history_and_next_report_closes_it() {
    let fixture = fixture("gap").await;
    let store = Store::new(30);
    report(&fixture, &store, 1, BASE);
    assert_eq!(last_seen_nanos(&fixture), Some(BASE));

    // 阈值内：30s < 60s。
    let (notices, recoveries) = fixture
        .detector
        .detect(BASE + 30_000_000_000, THRESHOLD_NANOS, policy())
        .expect("detect inside threshold");
    assert!(notices.is_empty() && recoveries.is_empty());

    // 超过阈值：离线起点 = 最后上报 + 阈值。
    let (notices, recoveries) = fixture
        .detector
        .detect(BASE + 90_000_000_000, THRESHOLD_NANOS, policy())
        .expect("detect past threshold");
    assert!(recoveries.is_empty());
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].started_at_nanos, BASE + THRESHOLD_NANOS);
    assert_eq!(notices[0].last_seen_nanos, BASE);
    assert_eq!(notices[0].last_ip, "10.9.9.9");

    let (status, offline_id): (String, Option<i64>) = fixture
        .db
        .read(|conn| {
            conn.query_row(
                "SELECT status, current_offline_id FROM server_runtimes WHERE server_id = ?1",
                [SERVER_ID],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(Into::into)
        })
        .expect("read runtime");
    assert_eq!(status, "offline");
    assert_eq!(offline_id, Some(notices[0].history_id));

    // 恢复上报：摄取路径在同一事务里关闭历史并把运行态翻回在线。
    report(&fixture, &store, 2, BASE + 120_000_000_000);
    assert_eq!(last_seen_nanos(&fixture), Some(BASE + 120_000_000_000));
    let (notices, recoveries) = fixture
        .detector
        .detect(BASE + 120_000_000_000, THRESHOLD_NANOS, policy())
        .expect("detect after recovery");
    assert!(notices.is_empty());
    assert_eq!(recoveries.len(), 1);
    assert_eq!(recoveries[0].recovered_at_nanos, BASE + 120_000_000_000);
    assert_eq!(recoveries[0].offline_seconds, 60);
    assert_eq!(recoveries[0].last_ip, "10.9.9.9");

    let (status, offline_id): (String, Option<i64>) = fixture
        .db
        .read(|conn| {
            conn.query_row(
                "SELECT status, current_offline_id FROM server_runtimes WHERE server_id = ?1",
                [SERVER_ID],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(Into::into)
        })
        .expect("read runtime after recovery");
    assert_eq!(status, "online");
    assert_eq!(offline_id, None);
    let closed: i64 = fixture
        .db
        .read(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM server_offline_histories WHERE status = 'closed'",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .expect("count closed");
    assert_eq!(closed, 1);
}

#[tokio::test]
async fn short_outage_stays_online() {
    let fixture = fixture("short").await;
    let store = Store::new(30);
    report(&fixture, &store, 1, BASE);
    let (notices, recoveries) = fixture
        .detector
        .detect(BASE + 30_000_000_000, THRESHOLD_NANOS, policy())
        .expect("detect");
    assert!(notices.is_empty() && recoveries.is_empty());
    let histories: i64 = fixture
        .db
        .read(|conn| {
            conn.query_row("SELECT COUNT(*) FROM server_offline_histories", [], |row| {
                row.get(0)
            })
            .map_err(Into::into)
        })
        .expect("count histories");
    assert_eq!(histories, 0);
}
