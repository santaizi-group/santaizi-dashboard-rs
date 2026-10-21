//! 运行态应用：`ApplyV2Event` 的落库部分，对齐 Go `touchV2Runtime`。
//!
//! 每个可信事件把 server_runtimes 翻回在线并刷新快照；乱序/重复帧按
//! (last_collected_at, session, sequence) 幂等丢弃。进程级 ServerList 快照
//! 属 Phase 5 HTTP 层，本阶段数据库即真相。

use prost::Message as _;
use rusqlite::{OptionalExtension, Transaction};

use crate::proto;
use crate::repo::bindings;
use crate::telemetry::credential;
use crate::telemetry::offline::{self, OfflineHistoryPolicy};

/// agent 上报的运行态行，对齐 Go `model.AgentTelemetryRuntime`。
pub struct AgentRuntimeRow {
    pub node_uuid: Vec<u8>,
    pub wal_pressure: i32,
    pub wal_bytes: u64,
    pub pending_events: u64,
    pub oldest_pending_nanos: u64,
    pub sink_cursors: Vec<u8>,
    pub clock_untrusted: bool,
    pub protocol_version: String,
    pub agent_version: String,
}

/// `UpsertAgentTelemetryRuntime`：按 node_uuid 冲突全量更新。
pub fn upsert_agent_runtime(tx: &Transaction<'_>, row: &AgentRuntimeRow) -> anyhow::Result<()> {
    tx.execute(
        "INSERT INTO agent_telemetry_runtimes (node_uuid, wal_pressure, wal_bytes, pending_events, oldest_pending, sink_cursors, clock_untrusted, protocol_version, agent_version, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, CURRENT_TIMESTAMP) \
         ON CONFLICT (node_uuid) DO UPDATE SET \
         wal_pressure = ?2, wal_bytes = ?3, pending_events = ?4, oldest_pending = ?5, sink_cursors = ?6, \
         clock_untrusted = ?7, protocol_version = ?8, agent_version = ?9, updated_at = CURRENT_TIMESTAMP",
        rusqlite::params![
            row.node_uuid,
            row.wal_pressure,
            row.wal_bytes as i64,
            row.pending_events as i64,
            if row.oldest_pending_nanos == 0 { None } else { Some(row.oldest_pending_nanos as i64) },
            row.sink_cursors,
            row.clock_untrusted as i32,
            row.protocol_version,
            row.agent_version,
        ],
    )?;
    Ok(())
}

struct ServerRuntimeRow {
    last_collected_at: i64,
    last_received_at: i64,
    current_session_id: Vec<u8>,
    current_sequence: u64,
    last_ip: String,
    host_payload: Vec<u8>,
    current_offline_id: i64,
    last_boot_time: u64,
    last_uptime: u64,
}

fn load_server_runtime(
    tx: &Transaction<'_>,
    server_id: i64,
) -> anyhow::Result<Option<ServerRuntimeRow>> {
    tx.query_row(
        "SELECT last_collected_at, last_received_at, current_session_id, current_sequence, last_ip, \
         host_payload, current_offline_id, last_boot_time, last_uptime \
         FROM server_runtimes WHERE server_id = ?",
        [server_id],
        |row| {
            Ok(ServerRuntimeRow {
                last_collected_at: row.get::<_, Option<i64>>(0)?.unwrap_or(0),
                last_received_at: row.get::<_, Option<i64>>(1)?.unwrap_or(0),
                current_session_id: row.get::<_, Option<Vec<u8>>>(2)?.unwrap_or_default(),
                current_sequence: row.get::<_, Option<i64>>(3)?.unwrap_or(0).max(0) as u64,
                last_ip: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                host_payload: row.get::<_, Option<Vec<u8>>>(5)?.unwrap_or_default(),
                current_offline_id: row.get::<_, Option<i64>>(6)?.unwrap_or(0),
                last_boot_time: row.get::<_, Option<i64>>(7)?.unwrap_or(0).max(0) as u64,
                last_uptime: row.get::<_, Option<i64>>(8)?.unwrap_or(0).max(0) as u64,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

/// `touchV2Runtime`：可信帧刷新运行态。返回是否真正应用（乱序帧返回 false）。
/// v1 unary 上报无序列号，调 `touch_legacy_runtime`（判旧只看接收钟）。
///
/// 同一事务内、覆盖 LastBootTime/LastUptime/LastIP 之前收口离线历史
/// （Go `closeV2OfflineHistory` 同序）：原因判定比较「离线前」运行态与
/// 「本次恢复」上报。恢复通知属 Phase 6。
#[allow(clippy::too_many_arguments)]
pub fn touch_server_runtime(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    session_id: &[u8],
    sequence: u64,
    collected_at_nanos: i64,
    received_at_nanos: i64,
    state: Option<&proto::State>,
    host: Option<&proto::Host>,
    offline: OfflineHistoryPolicy,
) -> anyhow::Result<bool> {
    let server_id = bindings::server_id_from_node_uuid(tx, node_uuid)?
        .ok_or_else(|| anyhow::anyhow!("node is not bound to a server"))?;
    let runtime = load_server_runtime(tx, server_id)?;
    if let Some(runtime) = &runtime {
        let stale = runtime.last_collected_at > collected_at_nanos
            || (runtime.last_collected_at == collected_at_nanos
                && runtime.current_session_id == session_id
                && runtime.current_sequence >= sequence);
        if stale {
            return Ok(false);
        }
    }
    touch_server_runtime_row(
        tx,
        server_id,
        runtime,
        node_uuid,
        session_id,
        sequence,
        collected_at_nanos,
        received_at_nanos,
        state,
        host,
        offline,
    )
}

/// v1 unary 上报（`ReportSystemState` / `ReportSystemInfo`）的运行态刷新：
/// 无 session/sequence，只有接收钟参与判旧，`protocol` 落非 `v2`
/// （读侧 `runtime_presentation` 因此回落运行态列，不查可用性桶）。
#[allow(clippy::too_many_arguments)]
pub fn touch_legacy_runtime(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    sequence: u64,
    received_at_nanos: i64,
    state: Option<&proto::State>,
    host: Option<&proto::Host>,
    offline: OfflineHistoryPolicy,
) -> anyhow::Result<bool> {
    let server_id = bindings::server_id_from_node_uuid(tx, node_uuid)?
        .ok_or_else(|| anyhow::anyhow!("node is not bound to a server"))?;
    let runtime = load_server_runtime(tx, server_id)?;
    if let Some(runtime) = &runtime {
        // v1 判旧：Go `Server.LastActive` 语义（后到的上报总是覆盖），
        // 同一接收钟按调用序覆盖（sequence 恒 0，不再比对 session）。
        if runtime.last_received_at > received_at_nanos {
            return Ok(false);
        }
    }
    touch_server_runtime_row(
        tx,
        server_id,
        runtime,
        node_uuid,
        &[0u8; 16],
        sequence,
        received_at_nanos,
        received_at_nanos,
        state,
        host,
        offline,
    )
}

/// 共享写路径：合并 Host 继承/GeoIP、收口离线记录、upsert 运行态行。
#[allow(clippy::too_many_arguments)]
fn touch_server_runtime_row(
    tx: &Transaction<'_>,
    server_id: i64,
    runtime: Option<ServerRuntimeRow>,
    node_uuid: &[u8],
    session_id: &[u8],
    sequence: u64,
    collected_at_nanos: i64,
    received_at_nanos: i64,
    state: Option<&proto::State>,
    host: Option<&proto::Host>,
    offline: OfflineHistoryPolicy,
) -> anyhow::Result<bool> {
    let legacy = session_id.iter().all(|byte| *byte == 0);

    let merged_host = host.map(|host| {
        // 空 IP / 国家码继承上一帧 + GeoIP 按地址兜底（Go `enrichPBHost`）。
        let mut merged = host.clone();
        let previous_country = previous_host_country(
            &runtime
                .as_ref()
                .map(|r| r.host_payload.clone())
                .unwrap_or_default(),
        );
        if merged.ip.trim().is_empty()
            && let Some(runtime) = &runtime
        {
            merged.ip = runtime.last_ip.clone();
        }
        if merged.country_code.trim().is_empty() && !previous_country.is_empty() {
            merged.country_code = previous_country;
        }
        crate::geoip::enrich_country_code(&mut merged, None);
        merged
    });

    if let Some(runtime) = &runtime
        && runtime.current_offline_id > 0
    {
        let closed = offline::close_offline_history_tx(
            tx,
            server_id,
            &offline::RuntimeOfflineFields {
                current_offline_id: runtime.current_offline_id,
                last_boot_time: runtime.last_boot_time,
                last_uptime: runtime.last_uptime,
                last_ip: runtime.last_ip.clone(),
            },
            state,
            merged_host.as_ref(),
            received_at_nanos,
            offline,
        )?;
        if let Some(closed) = &closed {
            tracing::debug!(
                server_id,
                history_id = closed.id,
                reason = closed.reason,
                duration_seconds = closed.duration_seconds,
                "offline history closed"
            );
        }
    }

    let state_payload = state.map(prost::Message::encode_to_vec);
    let host_payload = merged_host.as_ref().map(prost::Message::encode_to_vec);
    let mut last_boot_time: Option<u64> = None;
    let mut last_ip: Option<String> = None;
    let mut last_agent_version: Option<String> = None;
    let mut last_uptime: Option<u64> = None;
    if let Some(merged) = &merged_host {
        last_boot_time = Some(merged.boot_time);
        if !merged.ip.trim().is_empty() {
            last_ip = Some(merged.ip.clone());
        }
        last_agent_version = Some(merged.version.clone());
    }
    if let Some(state) = state {
        last_uptime = Some(state.uptime);
    }

    let received_text = crate::util::time::unix_nanos_to_sqlite(received_at_nanos);
    // v1 上报无采集钟与序列号：last_collected_at 跟接收钟、sequence 恒 0，
    // `protocol` 落非 `v2` 让读侧回落运行态列（Go `ServerRuntime.Protocol` 语义）。
    let protocol = if legacy { "v1" } else { "v2" };
    let updated = tx.execute(
        "INSERT INTO server_runtimes (server_id, status, first_seen_at, last_seen_at, last_online_at, \
         last_boot_time, last_uptime, last_ip, last_agent_version, current_node_uuid, current_session_id, \
         current_sequence, protocol, host_state, connectivity_state, last_collected_at, last_received_at, \
         state_payload, host_payload, created_at, updated_at) \
         VALUES (?1, 'online', ?2, ?2, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?14, 'online', 'full', ?10, ?11, ?12, ?13, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT (server_id) DO UPDATE SET \
         status = 'online', last_seen_at = ?2, last_online_at = ?2, \
         last_boot_time = COALESCE(?3, server_runtimes.last_boot_time), \
         last_uptime = COALESCE(?4, server_runtimes.last_uptime), \
         last_ip = COALESCE(?5, server_runtimes.last_ip), \
         last_agent_version = COALESCE(?6, server_runtimes.last_agent_version), \
         current_node_uuid = ?7, current_session_id = ?8, current_sequence = ?9, protocol = ?14, \
         host_state = 'online', connectivity_state = 'full', last_collected_at = ?10, last_received_at = ?11, \
         state_payload = COALESCE(?12, server_runtimes.state_payload), \
         host_payload = COALESCE(?13, server_runtimes.host_payload), \
         current_offline_id = NULL, updated_at = CURRENT_TIMESTAMP",
        rusqlite::params![
            server_id,
            received_text,
            last_boot_time.map(|v| v as i64),
            last_uptime.map(|v| v as i64),
            last_ip,
            last_agent_version,
            node_uuid,
            session_id,
            sequence as i64,
            collected_at_nanos,
            received_at_nanos,
            state_payload,
            host_payload,
            protocol,
        ],
    )?;
    Ok(updated > 0)
}

/// 上一帧 Host 的国家码：payload 损坏时按空处理。
fn previous_host_country(host_payload: &[u8]) -> String {
    if host_payload.is_empty() {
        return String::new();
    }
    match proto::Host::decode(host_payload) {
        Ok(host) => host.country_code.trim().to_string(),
        Err(_) => String::new(),
    }
}

/// 服务器的当前摄入序列：新挂从端 activation = current_sequence + 1。
pub fn current_sequence_for_server(tx: &Transaction<'_>, server_id: i64) -> anyhow::Result<u64> {
    let sequence = tx
        .query_row(
            "SELECT current_sequence FROM server_runtimes WHERE server_id = ?",
            [server_id],
            |row| Ok(row.get::<_, Option<i64>>(0)?.unwrap_or(0).max(0) as u64),
        )
        .optional()?
        .unwrap_or(0);
    Ok(sequence)
}

/// `ApplyV2Event`：按 payload 取 state / host 后刷新运行态。
/// 乱序帧由 `touch_server_runtime` 幂等丢弃，重复批次可再调。
pub fn apply_event_runtime(
    tx: &Transaction<'_>,
    event: &proto::TelemetryEvent,
    received_at_nanos: i64,
    offline: OfflineHistoryPolicy,
) -> anyhow::Result<bool> {
    let mut state = None;
    let mut host = None;
    match &event.payload {
        Some(proto::telemetry_event::Payload::State(payload)) => state = Some(payload),
        Some(proto::telemetry_event::Payload::Host(payload)) => host = Some(payload),
        _ => {}
    }
    touch_server_runtime(
        tx,
        &event.node_uuid,
        &event.session_id,
        event.sequence,
        event.collected_at_unix_nano,
        received_at_nanos,
        state,
        host,
        offline,
    )
}

/// `ApplyRealtimeSnapshot`：超过离线阈值的快照直接丢弃，否则刷新运行态。
pub fn apply_realtime_snapshot(
    tx: &Transaction<'_>,
    snapshot: &proto::RealtimeSnapshot,
    received_at_nanos: i64,
    offline_threshold_secs: u64,
    offline: OfflineHistoryPolicy,
) -> anyhow::Result<()> {
    if snapshot.node_uuid.len() != 16 || snapshot.session_id.len() != 16 {
        anyhow::bail!("invalid realtime snapshot identity");
    }
    let threshold_nanos = offline_threshold_secs as i64 * 1_000_000_000;
    if received_at_nanos - snapshot.collected_at_unix_nano > threshold_nanos {
        return Ok(());
    }
    touch_server_runtime(
        tx,
        &snapshot.node_uuid,
        &snapshot.session_id,
        snapshot.latest_sequence,
        snapshot.collected_at_unix_nano,
        received_at_nanos,
        snapshot.state.as_ref(),
        snapshot.host.as_ref(),
        offline,
    )?;
    Ok(())
}

/// Control 首帧的 host 上报也走运行态刷新（Go 只在 Ingest 路径 touch，
/// hello.host 仅作能力声明）；此处提供显式入口供 Phase 5 使用。
#[allow(clippy::too_many_arguments)]
pub fn apply_control_hello_host(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    session_id: &[u8],
    sequence: u64,
    collected_at_nanos: i64,
    received_at_nanos: i64,
    host: &proto::Host,
    offline: OfflineHistoryPolicy,
) -> anyhow::Result<bool> {
    touch_server_runtime(
        tx,
        node_uuid,
        session_id,
        sequence,
        collected_at_nanos,
        received_at_nanos,
        None,
        Some(host),
        offline,
    )
}

/// 凭证宽限：曾授权的过期凭证在宽限期内仍放行（探针续签竞态）。
pub fn credential_grace_secs(conf: &crate::config::Config) -> i64 {
    (conf.telemetry.credential_grace_days * 24 * 3600) as i64
}

/// 验签入口：封装 `verify_credential` 的调用方错误映射。
pub fn verify_ingest_credential(
    signer: &credential::Signer,
    credential: &proto::SignedAgentCredential,
    now_unix: i64,
    grace_secs: i64,
    previously_authorized: bool,
) -> anyhow::Result<credential::Verification> {
    credential::verify_credential(
        &signer.public_key(),
        &signer.key_id(),
        credential,
        now_unix,
        grace_secs,
        previously_authorized,
    )
    .map_err(|err| anyhow::anyhow!(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;

    fn temp_db(tag: &str) -> String {
        let dir =
            std::env::temp_dir().join(format!("santaizi-runtime-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("panel.db").to_string_lossy().into_owned()
    }

    fn seed_server(db: &crate::db::Db, server_id: i64, node: &[u8]) {
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) \
                 VALUES (?, 'n', 't', X'00', 1, 0)",
                [server_id],
            )?;
            tx.execute(
                "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason) \
                 VALUES (?, ?, 1, NULL, 1, 'test')",
                rusqlite::params![server_id, node],
            )?;
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn touch_flips_runtime_online_and_is_idempotent_for_stale_frames() {
        let path = temp_db("touch");
        let db = migrate::open_primary(&path).unwrap();
        let node = [5u8; 16];
        let session = [6u8; 16];
        seed_server(&db, 7, &node);
        let host = proto::Host {
            ip: "10.0.0.1".into(),
            version: "0.1.0".into(),
            ..Default::default()
        };
        let applied = db
            .write(|tx| {
                touch_server_runtime(
                    tx,
                    &node,
                    &session,
                    10,
                    1_000,
                    2_000,
                    None,
                    Some(&host),
                    OfflineHistoryPolicy::disabled(),
                )
            })
            .unwrap();
        assert!(applied);
        // 乱序旧帧被丢弃
        let stale = db
            .write(|tx| {
                touch_server_runtime(
                    tx,
                    &node,
                    &session,
                    9,
                    1_000,
                    2_000,
                    None,
                    Some(&host),
                    OfflineHistoryPolicy::disabled(),
                )
            })
            .unwrap();
        assert!(!stale);
        // 空 IP 继承上一帧
        let empty_ip = proto::Host {
            version: "0.1.1".into(),
            ..Default::default()
        };
        db.write(|tx| {
            touch_server_runtime(
                tx,
                &node,
                &session,
                11,
                1_001,
                2_001,
                None,
                Some(&empty_ip),
                OfflineHistoryPolicy::disabled(),
            )
        })
        .unwrap();
        let ip: String = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT last_ip FROM server_runtimes WHERE server_id = 7",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(ip, "10.0.0.1");
        let status: String = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT status FROM server_runtimes WHERE server_id = 7",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(status, "online");
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn touch_closes_offline_history_in_same_transaction() {
        let path = temp_db("touch-offline");
        let db = migrate::open_primary(&path).unwrap();
        let node = [5u8; 16];
        let session = [6u8; 16];
        seed_server(&db, 7, &node);
        // 运行态离线并指向一条进行中的记录（模拟检测器已判离线）。
        db.write(|tx| {
            tx.execute(
                "INSERT INTO server_runtimes (server_id, status, last_collected_at, last_boot_time, \
                 last_uptime, last_ip, current_offline_id) VALUES (7, 'offline', 0, 100, 50, '10.0.0.1', 1)",
                [],
            )?;
            tx.execute(
                "INSERT INTO server_offline_histories (id, server_id, started_at, detected_at, status, \
                 threshold_seconds, last_seen_at, last_boot_time, last_uptime, last_ip) \
                 VALUES (1, 7, ?, ?, 'open', 30, ?, 100, 50, '10.0.0.1')",
                rusqlite::params![
                    crate::util::time::unix_nanos_to_sqlite(1_000_000_000),
                    crate::util::time::unix_nanos_to_sqlite(1_000_000_000),
                    crate::util::time::unix_nanos_to_sqlite(1_000_000_000),
                ],
            )?;
            Ok(())
        })
        .unwrap();

        let policy = OfflineHistoryPolicy {
            enabled: true,
            merge_gap_seconds: 10,
        };
        let host = proto::Host {
            ip: "10.0.0.2".into(),
            boot_time: 100,
            version: "0.2.0".into(),
            ..Default::default()
        };
        let state = proto::State {
            uptime: 80,
            ..Default::default()
        };
        // received = 3.5s，started_at = 1s → 离线时长 3s（半秒起进）。
        let applied = db
            .write(|tx| {
                touch_server_runtime(
                    tx,
                    &node,
                    &session,
                    12,
                    2_000,
                    3_500_000_000,
                    Some(&state),
                    Some(&host),
                    policy,
                )
            })
            .unwrap();
        assert!(applied);

        let (status, reason, recovered_ip, duration): (String, String, String, i64) = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT status, reason, recovered_ip, duration_seconds FROM server_offline_histories WHERE id = 1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )?)
            })
            .unwrap();
        assert_eq!(status, "closed");
        assert_eq!(reason, offline::OFFLINE_REASON_NETWORK_DISCONNECT);
        assert_eq!(recovered_ip, "10.0.0.2");
        assert_eq!(duration, 3);
        let (runtime_status, offline_id): (String, Option<i64>) = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT status, current_offline_id FROM server_runtimes WHERE server_id = 7",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .unwrap();
        assert_eq!(runtime_status, "online");
        assert_eq!(offline_id, None);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn agent_runtime_upsert_roundtrip() {
        let path = temp_db("agent-runtime");
        let db = migrate::open_primary(&path).unwrap();
        let node = [8u8; 16];
        let row = AgentRuntimeRow {
            node_uuid: node.to_vec(),
            wal_pressure: 2,
            wal_bytes: 1024,
            pending_events: 5,
            oldest_pending_nanos: 42,
            sink_cursors: vec![1, 2, 3],
            clock_untrusted: false,
            protocol_version: "2".to_string(),
            agent_version: "0.1.0".to_string(),
        };
        db.write(|tx| upsert_agent_runtime(tx, &row)).unwrap();
        db.write(|tx| upsert_agent_runtime(tx, &row)).unwrap();
        let (pressure, bytes): (i64, i64) = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT wal_pressure, wal_bytes FROM agent_telemetry_runtimes WHERE node_uuid = ?",
                    [&node],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .unwrap();
        assert_eq!(pressure, 2);
        assert_eq!(bytes, 1024);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }
}
