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
            row.wal_bytes,
            row.pending_events,
            if row.oldest_pending_nanos == 0 { None } else { Some(row.oldest_pending_nanos) },
            row.sink_cursors,
            row.clock_untrusted as i32,
            row.protocol_version,
            row.agent_version,
        ],
    )?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpChange {
    pub server_id: i64,
    pub old_ip: String,
    pub new_ip: String,
}

/// 上报 IP 归一：只接受可解析的单地址（v4/v6）或 `v4/v6` 双栈，
/// 控制字符与超长串一律拒绝（保留 `last_ip` 旧值）。
/// 探针报文不可信——脏值会一路带到通知正文、脱敏输出与静音键。
pub(crate) fn normalize_reported_ip(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > 64 {
        return None;
    }
    if trimmed.chars().any(char::is_control) {
        return None;
    }
    match trimmed.split_once('/') {
        Some((v4, v6)) => {
            let v4_clean = v4.trim();
            let v6_clean = v6.trim();
            (v4_clean.parse::<std::net::IpAddr>().is_ok()
                && v6_clean.parse::<std::net::IpAddr>().is_ok())
            .then(|| format!("{v4_clean}/{v6_clean}"))
        }
        None => trimmed
            .parse::<std::net::IpAddr>()
            .is_ok()
            .then(|| trimmed.to_string()),
    }
}

struct ServerRuntimeRow {
    status: String,
    last_seen_nanos: i64,
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
        "SELECT COALESCE(status, ''), last_seen_at, last_collected_at, last_received_at, \
         current_session_id, current_sequence, last_ip, host_payload, current_offline_id, \
         last_boot_time, last_uptime \
         FROM server_runtimes WHERE server_id = ?",
        [server_id],
        |row| {
            let last_seen_text: Option<String> = row.get(1)?;
            Ok(ServerRuntimeRow {
                status: row.get(0)?,
                last_seen_nanos: last_seen_text
                    .as_deref()
                    .and_then(crate::util::time::sqlite_to_unix_nanos)
                    .unwrap_or(0),
                last_collected_at: row.get::<_, Option<i64>>(2)?.unwrap_or(0),
                last_received_at: row.get::<_, Option<i64>>(3)?.unwrap_or(0),
                current_session_id: row.get::<_, Option<Vec<u8>>>(4)?.unwrap_or_default(),
                current_sequence: row.get::<_, Option<i64>>(5)?.unwrap_or(0).max(0) as u64,
                last_ip: row.get::<_, Option<String>>(6)?.unwrap_or_default(),
                host_payload: row.get::<_, Option<Vec<u8>>>(7)?.unwrap_or_default(),
                current_offline_id: row.get::<_, Option<i64>>(8)?.unwrap_or(0),
                last_boot_time: row.get::<_, Option<i64>>(9)?.unwrap_or(0).max(0) as u64,
                last_uptime: row.get::<_, Option<i64>>(10)?.unwrap_or(0).max(0) as u64,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn is_stale_v2_frame(
    runtime: &ServerRuntimeRow,
    session_id: &[u8],
    sequence: u64,
    collected_at_nanos: i64,
    received_at_nanos: i64,
) -> bool {
    if runtime.current_session_id == session_id {
        // 同一会话内序列号严格单调递增：即使探针经 NTP 回拨了本机时钟，也不得拒收新帧导致假离线。
        return runtime.current_sequence >= sequence;
    }
    if collected_at_nanos >= runtime.last_collected_at {
        return false;
    }
    // 跨会话但采集钟更小：若机器已离线，或距上次接收已超出时钟可信窗，说明是重连/重启后时钟被校准的新会话；
    // 否则视作多路径下迟到的旧会话尾巴丢弃。
    let was_offline = runtime.status == "offline" || runtime.current_offline_id > 0;
    let beyond_skew = received_at_nanos.saturating_sub(runtime.last_received_at)
        > crate::telemetry::store::CLOCK_SKEW_LIMIT_NANOS;
    !(was_offline || beyond_skew)
}

/// `touchV2Runtime`：可信帧刷新运行态。返回是否真正应用（乱序帧返回 false）。
/// v1 unary 上报无序列号，调 `touch_legacy_runtime`（判旧只看接收钟）。
///
/// 同一事务内、覆盖 LastBootTime/LastUptime/LastIP 之前收口离线历史
/// （Go `closeV2OfflineHistory` 同序）：原因判定比较「离线前」运行态与
/// 「本次恢复」上报。
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
    touch_server_runtime_with_ip(
        tx,
        node_uuid,
        session_id,
        sequence,
        collected_at_nanos,
        received_at_nanos,
        state,
        host,
        offline,
    )
    .map(|(applied, _)| applied)
}

#[allow(clippy::too_many_arguments)]
pub fn touch_server_runtime_with_ip(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    session_id: &[u8],
    sequence: u64,
    collected_at_nanos: i64,
    received_at_nanos: i64,
    state: Option<&proto::State>,
    host: Option<&proto::Host>,
    offline: OfflineHistoryPolicy,
) -> anyhow::Result<(bool, Option<IpChange>)> {
    let server_id = bindings::server_id_from_node_uuid(tx, node_uuid)?
        .ok_or_else(|| anyhow::anyhow!("node is not bound to a server"))?;
    let runtime = load_server_runtime(tx, server_id)?;
    if let Some(runtime) = &runtime
        && is_stale_v2_frame(
            runtime,
            session_id,
            sequence,
            collected_at_nanos,
            received_at_nanos,
        )
    {
        return Ok((false, None));
    }
    let effective_received = runtime
        .as_ref()
        .map(|r| received_at_nanos.max(r.last_seen_nanos))
        .unwrap_or(received_at_nanos);
    touch_server_runtime_row(
        tx,
        server_id,
        runtime,
        node_uuid,
        session_id,
        sequence,
        collected_at_nanos,
        effective_received,
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
    touch_legacy_runtime_with_ip(
        tx,
        node_uuid,
        sequence,
        received_at_nanos,
        state,
        host,
        offline,
    )
    .map(|(applied, _)| applied)
}

#[allow(clippy::too_many_arguments)]
pub fn touch_legacy_runtime_with_ip(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    sequence: u64,
    received_at_nanos: i64,
    state: Option<&proto::State>,
    host: Option<&proto::Host>,
    offline: OfflineHistoryPolicy,
) -> anyhow::Result<(bool, Option<IpChange>)> {
    let server_id = bindings::server_id_from_node_uuid(tx, node_uuid)?
        .ok_or_else(|| anyhow::anyhow!("node is not bound to a server"))?;
    let runtime = load_server_runtime(tx, server_id)?;
    if let Some(runtime) = &runtime {
        // v1 判旧：Go `Server.LastActive` 语义（后到的上报总是覆盖），
        // 同一接收钟按调用序覆盖（sequence 恒 0，不再比对 session）。
        if runtime.last_received_at > received_at_nanos {
            return Ok((false, None));
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
) -> anyhow::Result<(bool, Option<IpChange>)> {
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
    let mut ip_change: Option<IpChange> = None;
    if let Some(merged) = &merged_host {
        last_boot_time = Some(merged.boot_time);
        let candidate_ip = normalize_reported_ip(&merged.ip);
        if let Some(candidate_ip) = candidate_ip {
            if let Some(existing) = &runtime
                && let Some(prev_clean) = normalize_reported_ip(&existing.last_ip)
                && prev_clean != candidate_ip
            {
                // 新旧值都必须是可解析单栈或双栈地址：探针报文不可信，脏值既会写进
                // 通知正文，也会被拿来构造静音键（单栈 `parse::<IpAddr>()` 会漏掉双栈主机）。
                ip_change = Some(IpChange {
                    server_id,
                    old_ip: prev_clean,
                    new_ip: candidate_ip.clone(),
                });
            }
            last_ip = Some(candidate_ip);
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
            last_boot_time,
            last_uptime,
            last_ip,
            last_agent_version,
            node_uuid,
            session_id,
            sequence,
            collected_at_nanos,
            received_at_nanos,
            state_payload,
            host_payload,
            protocol,
        ],
    )?;
    Ok((updated > 0, if updated > 0 { ip_change } else { None }))
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
    apply_event_runtime_with_ip(tx, event, received_at_nanos, offline).map(|(applied, _)| applied)
}

pub fn apply_event_runtime_with_ip(
    tx: &Transaction<'_>,
    event: &proto::TelemetryEvent,
    received_at_nanos: i64,
    offline: OfflineHistoryPolicy,
) -> anyhow::Result<(bool, Option<IpChange>)> {
    let mut state = None;
    let mut host = None;
    match &event.payload {
        Some(proto::telemetry_event::Payload::State(payload)) => state = Some(payload),
        Some(proto::telemetry_event::Payload::Host(payload)) => host = Some(payload),
        _ => {}
    }
    touch_server_runtime_with_ip(
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

/// 从端复制批次的运行态刷新：必须以观测方真实接收钟（`observed_at_nanos`）而非主端复制到达时刻判活。
///
/// 若事件被观测到的时间距主端当前已超过离线阈值（例如主机早已宕机，从端积压数分钟后才补传），
/// 只推进序号与最新载荷快照，**绝不关闭离线历史、绝不把已离线主机翻回在线或把 `last_seen_at` 伪造成当前时间**。
pub fn apply_replicated_event_runtime(
    tx: &Transaction<'_>,
    event: &proto::TelemetryEvent,
    observed_at_nanos: i64,
    primary_now_nanos: i64,
    offline_threshold_nanos: i64,
    offline: OfflineHistoryPolicy,
) -> anyhow::Result<bool> {
    apply_replicated_event_runtime_with_ip(
        tx,
        event,
        observed_at_nanos,
        primary_now_nanos,
        offline_threshold_nanos,
        offline,
    )
    .map(|(applied, _)| applied)
}

pub fn apply_replicated_event_runtime_with_ip(
    tx: &Transaction<'_>,
    event: &proto::TelemetryEvent,
    observed_at_nanos: i64,
    primary_now_nanos: i64,
    offline_threshold_nanos: i64,
    offline: OfflineHistoryPolicy,
) -> anyhow::Result<(bool, Option<IpChange>)> {
    let effective_seen = if observed_at_nanos > 0 {
        observed_at_nanos.min(primary_now_nanos)
    } else {
        event.collected_at_unix_nano.clamp(0, primary_now_nanos)
    };
    if primary_now_nanos.saturating_sub(effective_seen) < offline_threshold_nanos {
        return apply_event_runtime_with_ip(tx, event, effective_seen, offline);
    }

    // 迟到的历史复制帧：仅在不乱序时补齐载荷与序列号，保持离线态与离线记录不动。
    let server_id = bindings::server_id_from_node_uuid(tx, &event.node_uuid)?
        .ok_or_else(|| anyhow::anyhow!("node is not bound to a server"))?;
    let runtime = load_server_runtime(tx, server_id)?;
    if let Some(runtime) = &runtime
        && is_stale_v2_frame(
            runtime,
            &event.session_id,
            event.sequence,
            event.collected_at_unix_nano,
            effective_seen,
        )
    {
        return Ok((false, None));
    }

    let mut state = None;
    let mut host = None;
    match &event.payload {
        Some(proto::telemetry_event::Payload::State(payload)) => state = Some(payload),
        Some(proto::telemetry_event::Payload::Host(payload)) => host = Some(payload),
        _ => {}
    }
    let merged_host = host.map(|host| {
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
    let state_payload = state.map(prost::Message::encode_to_vec);
    let host_payload = merged_host.as_ref().map(prost::Message::encode_to_vec);
    let last_boot_time = merged_host.as_ref().map(|h| h.boot_time);
    let mut last_ip: Option<String> = None;
    let mut ip_change: Option<IpChange> = None;
    if let Some(merged) = &merged_host
        && let Some(candidate_ip) = normalize_reported_ip(&merged.ip)
    {
        if let Some(existing) = &runtime
            && let Some(prev_clean) = normalize_reported_ip(&existing.last_ip)
            && prev_clean != candidate_ip
        {
            ip_change = Some(IpChange {
                server_id,
                old_ip: prev_clean,
                new_ip: candidate_ip.clone(),
            });
        }
        last_ip = Some(candidate_ip);
    }
    let last_agent_version = merged_host.as_ref().map(|h| h.version.clone());
    let last_uptime = state.map(|s| s.uptime);
    let bumped_seen = runtime
        .as_ref()
        .map(|r| effective_seen.max(r.last_seen_nanos))
        .unwrap_or(effective_seen);
    let seen_text = crate::util::time::unix_nanos_to_sqlite(bumped_seen);

    if runtime.is_some() {
        let updated = tx.execute(
            "UPDATE server_runtimes SET \
             last_seen_at = ?1, \
             last_boot_time = COALESCE(?2, server_runtimes.last_boot_time), \
             last_uptime = COALESCE(?3, server_runtimes.last_uptime), \
             last_ip = COALESCE(?4, server_runtimes.last_ip), \
             last_agent_version = COALESCE(?5, server_runtimes.last_agent_version), \
             current_node_uuid = ?6, current_session_id = ?7, current_sequence = ?8, protocol = 'v2', \
             last_collected_at = ?9, last_received_at = ?10, \
             state_payload = COALESCE(?11, server_runtimes.state_payload), \
             host_payload = COALESCE(?12, server_runtimes.host_payload), \
             updated_at = CURRENT_TIMESTAMP \
             WHERE server_id = ?13",
            rusqlite::params![
                seen_text,
                last_boot_time,
                last_uptime,
                last_ip,
                last_agent_version,
                event.node_uuid,
                event.session_id,
                event.sequence,
                event.collected_at_unix_nano,
                bumped_seen,
                state_payload,
                host_payload,
                server_id,
            ],
        )?;
        Ok((updated > 0, if updated > 0 { ip_change } else { None }))
    } else {
        let inserted = tx.execute(
            "INSERT INTO server_runtimes (server_id, status, first_seen_at, last_seen_at, \
             last_boot_time, last_uptime, last_ip, last_agent_version, current_node_uuid, current_session_id, \
             current_sequence, protocol, host_state, connectivity_state, last_collected_at, last_received_at, \
             state_payload, host_payload, created_at, updated_at) \
             VALUES (?1, 'offline', ?2, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'v2', 'offline', 'unavailable', ?10, ?11, ?12, ?13, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            rusqlite::params![
                server_id,
                seen_text,
                last_boot_time,
                last_uptime,
                last_ip,
                last_agent_version,
                event.node_uuid,
                event.session_id,
                event.sequence,
                event.collected_at_unix_nano,
                bumped_seen,
                state_payload,
                host_payload,
            ],
        )?;
        Ok((inserted > 0, None))
    }
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

    /// 上报 IP 归一：只收可解析地址与 `v4/v6` 双栈；控制字符/超长/域名拒绝。
    #[test]
    fn reported_ip_is_validated() {
        assert_eq!(
            normalize_reported_ip(" 203.0.113.9 "),
            Some("203.0.113.9".into())
        );
        assert_eq!(
            normalize_reported_ip("203.0.113.9/2001:db8::1"),
            Some("203.0.113.9/2001:db8::1".into())
        );
        assert_eq!(
            normalize_reported_ip("2001:db8::1"),
            Some("2001:db8::1".into())
        );
        assert_eq!(normalize_reported_ip(""), None);
        assert_eq!(normalize_reported_ip("   "), None);
        assert_eq!(normalize_reported_ip("edge.example"), None);
        assert_eq!(normalize_reported_ip("203.0.113.9:80"), None);
        assert_eq!(normalize_reported_ip("203.0.113.9\nX: 1"), None);
        assert_eq!(normalize_reported_ip(&"9".repeat(65)), None);
        assert_eq!(normalize_reported_ip("203.0.113.9/not-an-ip"), None);
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

    /// IP 变更只在「新旧值都是可解析地址」时上报：探针报文不可信，
    /// 脏 IP 既会进通知正文，也会被拿去构造静音键绕过防骚扰。
    #[test]
    fn touch_reports_ip_change_only_for_parseable_addresses() {
        let path = temp_db("ip-change");
        let db = migrate::open_primary(&path).unwrap();
        let node = [5u8; 16];
        let session = [6u8; 16];
        seed_server(&db, 7, &node);
        // 上一帧留下不可解析的旧值（历史脏数据 / 外部写入）。
        db.write(|tx| {
            tx.execute(
                "INSERT INTO server_runtimes (server_id, status, last_ip) VALUES (7, 'online', 'not-an-ip')",
                [],
            )?;
            Ok(())
        })
        .unwrap();

        let host = |ip: &str| proto::Host {
            ip: ip.into(),
            version: "0.1.0".into(),
            ..Default::default()
        };
        // 旧值不可解析：不产生 IP 变更，但仍写回新值。
        let (applied, change) = db
            .write(|tx| {
                touch_server_runtime_with_ip(
                    tx,
                    &node,
                    &session,
                    10,
                    1_000,
                    2_000,
                    None,
                    Some(&host("203.0.113.9")),
                    OfflineHistoryPolicy::disabled(),
                )
            })
            .unwrap();
        assert!(applied);
        assert!(change.is_none(), "旧 IP 不可解析时不得上报变更");
        let stored: String = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT last_ip FROM server_runtimes WHERE server_id = 7",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(stored, "203.0.113.9");

        // 新旧都可解析且不同：上报变更。
        let (_, change) = db
            .write(|tx| {
                touch_server_runtime_with_ip(
                    tx,
                    &node,
                    &session,
                    11,
                    1_001,
                    2_001,
                    None,
                    Some(&host("198.51.100.2")),
                    OfflineHistoryPolicy::disabled(),
                )
            })
            .unwrap();
        assert_eq!(
            change,
            Some(IpChange {
                server_id: 7,
                old_ip: "203.0.113.9".into(),
                new_ip: "198.51.100.2".into(),
            })
        );

        // 新值不可解析：既不写脏值也不上报变更（保留上一帧的可解析地址）。
        let (_, change) = db
            .write(|tx| {
                touch_server_runtime_with_ip(
                    tx,
                    &node,
                    &session,
                    12,
                    1_002,
                    2_002,
                    None,
                    Some(&host("edge.example")),
                    OfflineHistoryPolicy::disabled(),
                )
            })
            .unwrap();
        assert!(change.is_none());
        let stored: String = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT last_ip FROM server_runtimes WHERE server_id = 7",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(stored, "198.51.100.2", "不可解析的新 IP 不得覆盖旧值");

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

    #[test]
    fn same_session_ntp_clock_step_back_still_advances_runtime() {
        let path = temp_db("ntp-stepback");
        let db = migrate::open_primary(&path).unwrap();
        let node = [9u8; 16];
        let session = [10u8; 16];
        seed_server(&db, 9, &node);
        let t0 = 1_700_000_120_000_000_000i64;
        assert!(
            db.write(|tx| {
                touch_server_runtime(
                    tx,
                    &node,
                    &session,
                    10,
                    t0,
                    t0,
                    None,
                    None,
                    OfflineHistoryPolicy::disabled(),
                )
            })
            .unwrap()
        );
        // NTP 将探针本机时间回拨了 90 秒，但同一 session 序列号递增到 11：必须接受并推进 last_seen_at。
        let stepped_back_collected = t0 - 90_000_000_000;
        let received = t0 + 2_000_000_000;
        assert!(
            db.write(|tx| {
                touch_server_runtime(
                    tx,
                    &node,
                    &session,
                    11,
                    stepped_back_collected,
                    received,
                    None,
                    None,
                    OfflineHistoryPolicy::disabled(),
                )
            })
            .unwrap()
        );
        let seq: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT current_sequence FROM server_runtimes WHERE server_id = 9",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(seq, 11);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn delayed_replicated_event_preserves_offline_state() {
        let path = temp_db("delayed-repl");
        let db = migrate::open_primary(&path).unwrap();
        let node = [11u8; 16];
        let session = [12u8; 16];
        seed_server(&db, 11, &node);
        let now = 1_700_000_200_000_000_000i64;
        db.write(|tx| {
            tx.execute(
                "INSERT INTO server_runtimes (server_id, status, last_seen_at, last_collected_at, \
                 current_session_id, current_sequence, current_offline_id) \
                 VALUES (11, 'offline', ?1, ?2, ?3, 5, 88)",
                rusqlite::params![
                    crate::util::time::unix_nanos_to_sqlite(now - 90_000_000_000),
                    now - 90_000_000_000,
                    session.to_vec()
                ],
            )?;
            Ok(())
        })
        .unwrap();

        let event = proto::TelemetryEvent {
            event_id: vec![1u8; 16],
            node_uuid: node.to_vec(),
            session_id: session.to_vec(),
            sequence: 6,
            collected_at_unix_nano: now - 88_000_000_000,
            payload: Some(proto::telemetry_event::Payload::State(proto::State {
                uptime: 500,
                ..Default::default()
            })),
            ..Default::default()
        };
        let applied = db
            .write(|tx| {
                apply_replicated_event_runtime(
                    tx,
                    &event,
                    now - 88_000_000_000,
                    now,
                    30_000_000_000,
                    OfflineHistoryPolicy {
                        enabled: true,
                        merge_gap_seconds: 0,
                    },
                )
            })
            .unwrap();
        assert!(applied);
        let (status, offline_id, seq): (String, Option<i64>, i64) = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT status, current_offline_id, current_sequence FROM server_runtimes WHERE server_id = 11",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )?)
            })
            .unwrap();
        assert_eq!(
            status, "offline",
            "迟到的历史复制帧不得将已离线主机翻回在线"
        );
        assert_eq!(offline_id, Some(88), "不得错误关闭正在进行的离线历史");
        assert_eq!(seq, 6, "序列号与最新载荷正常补齐");
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn dual_stack_and_replicated_ip_changes_are_detected() {
        let path = temp_db("dual-stack-ip");
        let db = migrate::open_primary(&path).unwrap();
        let node = [15u8; 16];
        let session = [16u8; 16];
        seed_server(&db, 15, &node);
        let now = 1_700_000_500_000_000_000i64;

        let host_v1 = proto::Host {
            ip: "1.2.3.4/2001:db8::1".into(),
            version: "0.2.0".into(),
            ..Default::default()
        };
        let (applied, first_change) = db
            .write(|tx| {
                touch_server_runtime_with_ip(
                    tx,
                    &node,
                    &session,
                    1,
                    now,
                    now,
                    None,
                    Some(&host_v1),
                    OfflineHistoryPolicy::disabled(),
                )
            })
            .unwrap();
        assert!(applied);
        assert!(first_change.is_none());

        // 双栈主机换 IP（旧版用 `prev_ip.parse::<IpAddr>()` 会把 `v4/v6` 判为无效而永远漏报）。
        let host_v2 = proto::Host {
            ip: "5.6.7.8/2001:db8::2".into(),
            version: "0.2.0".into(),
            ..Default::default()
        };
        let (_, change) = db
            .write(|tx| {
                touch_server_runtime_with_ip(
                    tx,
                    &node,
                    &session,
                    2,
                    now + 1_000_000_000,
                    now + 1_000_000_000,
                    None,
                    Some(&host_v2),
                    OfflineHistoryPolicy::disabled(),
                )
            })
            .unwrap();
        assert_eq!(
            change,
            Some(IpChange {
                server_id: 15,
                old_ip: "1.2.3.4/2001:db8::1".into(),
                new_ip: "5.6.7.8/2001:db8::2".into(),
            })
        );

        // 从端复制路径（含迟到历史帧）同样清洗并检测 IP 变更，拒绝脏 IP。
        let repl_event = proto::TelemetryEvent {
            event_id: vec![2u8; 16],
            node_uuid: node.to_vec(),
            session_id: session.to_vec(),
            sequence: 3,
            collected_at_unix_nano: now + 2_000_000_000,
            payload: Some(proto::telemetry_event::Payload::Host(proto::Host {
                ip: "9.9.9.9/2001:db8::9".into(),
                version: "0.2.0".into(),
                ..Default::default()
            })),
            ..Default::default()
        };
        let (applied, repl_change) = db
            .write(|tx| {
                apply_replicated_event_runtime_with_ip(
                    tx,
                    &repl_event,
                    now + 2_000_000_000,
                    now + 120_000_000_000,
                    30_000_000_000,
                    OfflineHistoryPolicy::disabled(),
                )
            })
            .unwrap();
        assert!(applied);
        assert_eq!(
            repl_change,
            Some(IpChange {
                server_id: 15,
                old_ip: "5.6.7.8/2001:db8::2".into(),
                new_ip: "9.9.9.9/2001:db8::9".into(),
            })
        );

        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }
}
