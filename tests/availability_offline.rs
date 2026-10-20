//! 可用性共识 → V2 离线历史端到端：摄取写证据、引擎算桶、检测器开/关记录。
//!
//! 覆盖 Go `TestDetectV2OfflineServers`（`offline_detector_v2_test.go`）的同一条
//! 真实链路，但证据由 `Store::ingest` 落桶（而不是直接插表），队列由引擎自己消费。

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::OptionalExtension;

use santaizi_dashboard::availability::engine::{Engine, HOST_STATE_OFFLINE, HOST_STATE_ONLINE};
use santaizi_dashboard::config::Config;
use santaizi_dashboard::db::{self, Db};
use santaizi_dashboard::proto;
use santaizi_dashboard::repo::bindings;
use santaizi_dashboard::secret;
use santaizi_dashboard::state::AppState;
use santaizi_dashboard::telemetry::offline::OfflineHistoryPolicy;
use santaizi_dashboard::telemetry::offline_detector::V2OfflineDetector;
use santaizi_dashboard::telemetry::runtime;
use santaizi_dashboard::telemetry::store::Store;

const SERVER_ID: i64 = 7;
const BUCKET_NANOS: i64 = 30_000_000_000;
const THRESHOLD_NANOS: i64 = 60_000_000_000;
/// 离线段起点（第 1 个桶），第 0 个桶在线。
const BASE: i64 = 1_700_000_000_000_000_000 / BUCKET_NANOS * BUCKET_NANOS;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "santaizi-avail-e2e-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

struct Fixture {
    db: Db,
    engine: Engine,
    detector: V2OfflineDetector,
    _dir: PathBuf,
}

async fn fixture(tag: &str) -> Fixture {
    let dir = temp_dir(tag);
    let db = db::migrate::open_primary(dir.join("panel.db").to_str().expect("db path"))
        .expect("open primary db");
    let mut conf = Config::default();
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
    conf.enable_offline_history = true;
    conf.offline_threshold_seconds = 60;
    conf.offline_check_interval_seconds = 10;
    conf.offline_merge_gap_seconds = 0;
    let state = AppState::primary(conf, db.clone())
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
        engine: Engine::new(db.clone(), 30, 1),
        detector: V2OfflineDetector::from_conf(db.clone(), &fixture_conf()),
        db,
        _dir: dir,
    }
}

fn fixture_conf() -> Config {
    let mut conf = Config::default();
    conf.enable_offline_history = true;
    conf.offline_threshold_seconds = 60;
    conf.offline_check_interval_seconds = 10;
    conf.offline_merge_gap_seconds = 0;
    conf
}

fn node() -> Vec<u8> {
    vec![0x41; 16]
}

fn session() -> Vec<u8> {
    vec![0x42; 16]
}

/// 一帧真实 Host 事件：摄取路径会写入事件、观测与 path 证据桶。
fn host_batch(sequence: u64, collected_at_nanos: i64) -> proto::TelemetryBatch {
    let event_id = Store::event_id(&node(), &session(), sequence).expect("event id");
    proto::TelemetryBatch {
        records: vec![proto::TelemetryRecord {
            record: Some(proto::telemetry_record::Record::Event(
                proto::TelemetryEvent {
                    event_id: event_id.to_vec(),
                    node_uuid: node(),
                    session_id: session(),
                    sequence,
                    event_type: proto::TelemetryEventType::Host as i32,
                    priority: proto::TelemetryPriority::P1Important as i32,
                    collected_at_unix_nano: collected_at_nanos,
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
    }
}

/// 探针在 `received_at_nanos` 上报一帧：摄取 → 证据桶 → 运行态 → 入队 → 引擎重算。
/// 与 Ingest worker 同序（`store.ingest` 后逐条 `apply_event_runtime`）。
fn report(fixture: &Fixture, store: &Store, sequence: u64, received_at_nanos: i64) {
    let batch = host_batch(sequence, received_at_nanos);
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
    fixture.engine.process_queue(200).expect("process queue");
}

/// 主面板健康 tick 推进到某个桶（健康行 + 入队 + 重算）。
fn health_tick(fixture: &Fixture, sampled_at_nanos: i64) {
    fixture
        .engine
        .record_primary_health(sampled_at_nanos)
        .expect("record primary health");
    fixture.engine.process_queue(200).expect("process queue");
}

fn host_state(fixture: &Fixture, bucket_start: i64) -> Option<String> {
    fixture
        .db
        .read(|conn| {
            conn.query_row(
                "SELECT host_state FROM availability_buckets WHERE node_uuid = ? AND bucket_start = ?",
                rusqlite::params![node(), bucket_start],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into)
        })
        .expect("read bucket")
}

#[tokio::test]
async fn silence_past_threshold_opens_history_and_reporting_closes_it() {
    let fixture = fixture("silence").await;
    let store = Store::new(30);

    // 第 0 个桶正常上报：共识在线。
    report(&fixture, &store, 1, BASE + 5);
    assert_eq!(
        host_state(&fixture, BASE).as_deref(),
        Some(HOST_STATE_ONLINE)
    );

    // 之后三个桶只有主面板健康、没有探针证据：封窗后判离线。
    for index in 1..=3i64 {
        let bucket = BASE + index * BUCKET_NANOS;
        health_tick(&fixture, bucket + 5);
        assert_eq!(
            host_state(&fixture, bucket).as_deref(),
            Some(HOST_STATE_OFFLINE)
        );
    }

    // 离线时长 = 3 个桶 = 90s ≥ 阈值 60s：开记录，起点取持续离线的第一个桶。
    let now = BASE + 4 * BUCKET_NANOS + 1;
    let (notices, recoveries) = fixture
        .detector
        .detect(now, THRESHOLD_NANOS, BUCKET_NANOS)
        .expect("detect");
    assert!(recoveries.is_empty());
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].started_at_nanos, BASE + BUCKET_NANOS);

    let (status, offline_id, reason): (String, Option<i64>, Option<String>) = fixture
        .db
        .read(|conn| {
            conn.query_row(
                "SELECT r.status, r.current_offline_id, h.reason FROM server_runtimes r \
                 LEFT JOIN server_offline_histories h ON h.id = r.current_offline_id \
                 WHERE r.server_id = ?1",
                [SERVER_ID],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(Into::into)
        })
        .expect("read runtime");
    assert_eq!(status, "offline");
    assert_eq!(offline_id, Some(notices[0].history_id));
    assert_eq!(reason.as_deref(), Some("unknown"));

    // 恢复上报：证据落在最后一个离线桶，共识转在线，记录被关闭。
    report(&fixture, &store, 2, BASE + 3 * BUCKET_NANOS + 5);
    assert_eq!(
        host_state(&fixture, BASE + 3 * BUCKET_NANOS).as_deref(),
        Some(HOST_STATE_ONLINE)
    );
    let (notices, recoveries) = fixture
        .detector
        .detect(now, THRESHOLD_NANOS, BUCKET_NANOS)
        .expect("detect after recovery");
    assert!(notices.is_empty() && recoveries.is_empty());

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
    report(&fixture, &store, 1, BASE + 5);
    // 只有一个离线桶：30s < 阈值 60s，不开记录。
    health_tick(&fixture, BASE + BUCKET_NANOS + 5);
    let now = BASE + 2 * BUCKET_NANOS + 1;
    let (notices, recoveries) = fixture
        .detector
        .detect(now, THRESHOLD_NANOS, BUCKET_NANOS)
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
