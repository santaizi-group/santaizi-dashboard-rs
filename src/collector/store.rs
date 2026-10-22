//! 从端库访问：授权缓存、节点授权判定、主端可见时间、运行态统计、spool 摄入
//! 与 spool 出库。
//! 对齐 Go `service/collector/store.go` 的 `SaveAuthorization` / `Authorization` /
//! `IsNodeAuthorized` / `TouchPrimarySeen` / `RuntimeStats` / `Ingest` /
//! `ReadOutbox` / `CommitReplicationAck` / `DiscardOutbox`。
//!
//! 授权配置里的探测目标整块换入 `collector_cached_probe_targets`。
//! spool 摄入已落地（`ingest`：本地事实表幂等写入 + 出库入队 + 会话游标 ack），
//! 当前生产者为测试；collector 侧 agent 服务（Ingest / GetStatus）属后续刀。

use std::collections::{BTreeMap, HashSet};

use prost::Message as _;
use rusqlite::{Connection, OptionalExtension, Transaction};

use crate::proto;
use crate::telemetry::probes;
use crate::telemetry::store::Store;
use crate::util::time;

/// `model.CollectorAuthorizationCache`：从端对主端授权配置的缓存（单行 id=1）。
#[derive(Debug, Clone)]
pub struct AuthorizationCache {
    pub collector_uuid: String,
    pub primary_public_key: Vec<u8>,
    pub key_id: Vec<u8>,
    pub config_version: u64,
    pub last_primary_seen_nano: i64,
    pub agent_ca_certificate_pem: String,
    pub kind: String,
    pub probe_config_json: Option<Vec<u8>>,
}

/// `collector.RuntimeStats`：outbox 存量统计。
#[derive(Debug, Default, Clone, Copy)]
pub struct RuntimeStats {
    pub spool_bytes: u64,
    pub pending: u64,
    pub oldest_pending_nano: i64,
}

/// `Store.Authorization`：读取授权缓存；未注册返回 `None`。
pub fn authorization(conn: &Connection) -> anyhow::Result<Option<AuthorizationCache>> {
    let row = conn
        .query_row(
            "SELECT collector_uuid, primary_public_key, key_id, config_version, \
             last_primary_seen_nano, agent_ca_certificate_pem, kind, probe_config_json \
             FROM collector_authorization_caches WHERE id = 1",
            [],
            |row| {
                Ok(AuthorizationCache {
                    collector_uuid: row.get(0)?,
                    primary_public_key: row.get(1)?,
                    key_id: row.get(2)?,
                    config_version: row.get::<_, i64>(3)?.max(0) as u64,
                    last_primary_seen_nano: row.get(4)?,
                    agent_ca_certificate_pem: row.get::<_, Option<String>>(5)?.unwrap_or_default(),
                    kind: row.get::<_, Option<String>>(6)?.unwrap_or_default(),
                    probe_config_json: row.get(7)?,
                })
            },
        )
        .optional()?;
    Ok(row)
}

/// `Store.SaveAuthorization`：授权配置整块换存（缓存行 + 指派 + 吊销清单）。
///
/// Agent CA 为空时保留盘上旧值（主端个别响应不下发该字段，Go 同样保留）。
/// `probe_config_json` 存 protobuf 二进制（Go `proto.Marshal`）。
pub fn save_authorization(
    tx: &Transaction<'_>,
    collector_uuid: &str,
    config: &proto::CollectorAuthorizationConfig,
    seen_at_nanos: i64,
) -> anyhow::Result<()> {
    if collector_uuid.is_empty() || config.primary_public_key.is_empty() {
        anyhow::bail!("invalid collector authorization config");
    }
    let kind = if config.kind == proto::CollectorKind::Probe as i32 {
        probes::COLLECTOR_KIND_PROBE
    } else {
        probes::COLLECTOR_KIND_OBSERVER
    };
    let probe_config_json = config.probe.as_ref().map(prost::Message::encode_to_vec);
    let existing_ca: Option<String> = tx
        .query_row(
            "SELECT agent_ca_certificate_pem FROM collector_authorization_caches WHERE id = 1",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten();
    let agent_ca = if config.agent_ca_certificate_pem.is_empty() {
        existing_ca.unwrap_or_default()
    } else {
        config.agent_ca_certificate_pem.clone()
    };
    let now = time::unix_nanos_to_sqlite(seen_at_nanos);
    tx.execute(
        "INSERT INTO collector_authorization_caches \
         (id, collector_uuid, primary_public_key, key_id, config_version, last_primary_seen_nano, \
          agent_ca_certificate_pem, kind, probe_config_json, updated_at) \
         VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
         ON CONFLICT(id) DO UPDATE SET \
          collector_uuid = excluded.collector_uuid, \
          primary_public_key = excluded.primary_public_key, \
          key_id = excluded.key_id, \
          config_version = excluded.config_version, \
          last_primary_seen_nano = excluded.last_primary_seen_nano, \
          agent_ca_certificate_pem = excluded.agent_ca_certificate_pem, \
          kind = excluded.kind, \
          probe_config_json = excluded.probe_config_json, \
          updated_at = excluded.updated_at",
        rusqlite::params![
            collector_uuid,
            &config.primary_public_key,
            &config.key_id,
            i64::try_from(config.config_version).unwrap_or(i64::MAX),
            seen_at_nanos,
            agent_ca,
            kind,
            probe_config_json.as_deref(),
            now,
        ],
    )?;
    tx.execute("DELETE FROM collector_cached_assignments", [])?;
    tx.execute("DELETE FROM collector_cached_revocations", [])?;
    for assignment in &config.assignments {
        tx.execute(
            "INSERT INTO collector_cached_assignments \
             (node_uuid, observer_id, valid_from, valid_to, generation, config_version, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                &assignment.node_uuid,
                &assignment.observer_id,
                assignment.valid_from_unix_nano,
                assignment.valid_to_unix_nano,
                i64::try_from(assignment.generation).unwrap_or(i64::MAX),
                i64::try_from(assignment.config_version).unwrap_or(i64::MAX),
                time::unix_nanos_to_sqlite(time::now_nanos()),
            ],
        )?;
    }
    for node_uuid in &config.revoked_node_uuids {
        tx.execute(
            "INSERT INTO collector_cached_revocations (node_uuid, config_version, updated_at) \
             VALUES (?1, ?2, ?3)",
            rusqlite::params![
                node_uuid,
                i64::try_from(config.config_version).unwrap_or(i64::MAX),
                time::unix_nanos_to_sqlite(time::now_nanos()),
            ],
        )?;
    }
    replace_probe_targets(tx, config, &now)?;
    Ok(())
}

/// 从端缓存的一条探测目标。地址族来自授权里的 `ProbeConfig`，不是目标自己的字段。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedProbeTarget {
    pub server_id: u64,
    pub server_name: String,
    pub ipv4: String,
    pub ipv6: String,
    pub hostname: String,
    pub tcp_ports: String,
    pub enable_icmp: bool,
    pub enable_tcp: bool,
    pub enable_mtr: bool,
    pub enable_ipv4: bool,
    pub enable_ipv6: bool,
    pub interval_sec: u64,
    pub mtr_interval_sec: u64,
    pub mtr_probes: u64,
    pub route_interval_sec: u64,
}

fn replace_probe_targets(
    tx: &Transaction<'_>,
    config: &proto::CollectorAuthorizationConfig,
    updated_at: &str,
) -> anyhow::Result<()> {
    tx.execute("DELETE FROM collector_cached_probe_targets", [])?;
    let (enable_ipv4, enable_ipv6) = probe_ip_families(config.probe.as_ref());
    let (route_fallback, mtr_fallback) = config
        .probe
        .as_ref()
        .map(|probe| (probe.route_interval_seconds, probe.mtr_probes))
        .unwrap_or((0, 0));
    for target in &config.targets {
        let ports = target
            .tcp_ports
            .iter()
            .filter(|port| **port > 0)
            .map(|port| port.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let route_interval = if target.route_interval_seconds == 0 {
            route_fallback
        } else {
            target.route_interval_seconds
        };
        let mtr_probes = probes::normalize_mtr_probes(i64::from(if target.mtr_probes == 0 {
            mtr_fallback
        } else {
            target.mtr_probes
        }));
        tx.execute(
            "INSERT INTO collector_cached_probe_targets \
             (server_id, server_name, ipv4, ipv6, hostname, tcp_ports, enable_icmp, enable_tcp, \
              enable_mtr, enable_ipv4, enable_ipv6, interval_sec, mtr_interval_sec, mtr_probes, \
              route_interval_sec, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            rusqlite::params![
                i64::try_from(target.server_id).unwrap_or(i64::MAX),
                target.server_name,
                target.ipv4,
                target.ipv6,
                target.hostname,
                ports,
                i64::from(target.enable_icmp),
                i64::from(target.enable_tcp),
                i64::from(target.enable_mtr),
                i64::from(enable_ipv4),
                i64::from(enable_ipv6),
                i64::from(target.interval_seconds),
                i64::from(target.mtr_interval_seconds),
                mtr_probes,
                i64::from(route_interval),
                updated_at,
            ],
        )?;
    }
    Ok(())
}

/// `Store.ProbeTargets`。
pub fn list_probe_targets(conn: &Connection) -> anyhow::Result<Vec<CachedProbeTarget>> {
    let mut stmt = conn.prepare(
        "SELECT server_id, COALESCE(server_name, ''), COALESCE(ipv4, ''), COALESCE(ipv6, ''), \
         COALESCE(hostname, ''), COALESCE(tcp_ports, ''), COALESCE(enable_icmp, 0), \
         COALESCE(enable_tcp, 0), COALESCE(enable_mtr, 0), COALESCE(enable_ipv4, 1), \
         COALESCE(enable_ipv6, 1), COALESCE(interval_sec, 0), COALESCE(mtr_interval_sec, 0), \
         COALESCE(mtr_probes, 0), COALESCE(route_interval_sec, 0) \
         FROM collector_cached_probe_targets ORDER BY server_id ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(CachedProbeTarget {
            server_id: row.get::<_, i64>(0)?.max(0) as u64,
            server_name: row.get(1)?,
            ipv4: row.get(2)?,
            ipv6: row.get(3)?,
            hostname: row.get(4)?,
            tcp_ports: row.get(5)?,
            enable_icmp: row.get::<_, i64>(6)? != 0,
            enable_tcp: row.get::<_, i64>(7)? != 0,
            enable_mtr: row.get::<_, i64>(8)? != 0,
            enable_ipv4: row.get::<_, i64>(9)? != 0,
            enable_ipv6: row.get::<_, i64>(10)? != 0,
            interval_sec: row.get::<_, i64>(11)?.max(0) as u64,
            mtr_interval_sec: row.get::<_, i64>(12)?.max(0) as u64,
            mtr_probes: row.get::<_, i64>(13)?.max(0) as u64,
            route_interval_sec: row.get::<_, i64>(14)?.max(0) as u64,
        })
    })?;
    let mut targets = Vec::new();
    for row in rows {
        targets.push(row?);
    }
    Ok(targets)
}

/// `ProbeConfigIPFamilies`：空列表表示不限制。
fn probe_ip_families(probe: Option<&proto::ProbeConfig>) -> (bool, bool) {
    let Some(probe) = probe else {
        return (true, true);
    };
    if probe.ip_families.is_empty() {
        return (true, true);
    }
    let mut v4 = false;
    let mut v6 = false;
    for family in &probe.ip_families {
        if *family == proto::ProbeIpFamily::Ipv4 as i32 {
            v4 = true;
        }
        if *family == proto::ProbeIpFamily::Ipv6 as i32 {
            v6 = true;
        }
    }
    if !v4 && !v6 { (true, true) } else { (v4, v6) }
}

/// `Store.TouchPrimarySeen`：刷新主端可见时间（收到 accepted 回执时）。
pub fn touch_primary_seen(tx: &Transaction<'_>, seen_at_nanos: i64) -> anyhow::Result<()> {
    tx.execute(
        "UPDATE collector_authorization_caches SET last_primary_seen_nano = ?1, updated_at = ?2 \
         WHERE id = 1",
        rusqlite::params![seen_at_nanos, time::unix_nanos_to_sqlite(seen_at_nanos)],
    )?;
    Ok(())
}

/// `Store.IsNodeAuthorized`：未吊销且存在覆盖 `at` 的指派。
pub fn is_node_authorized(
    conn: &Connection,
    node_uuid: &[u8],
    at_nanos: i64,
) -> anyhow::Result<bool> {
    let revoked: i64 = conn.query_row(
        "SELECT COUNT(*) FROM collector_cached_revocations WHERE node_uuid = ?1",
        [node_uuid],
        |row| row.get(0),
    )?;
    if revoked > 0 {
        return Ok(false);
    }
    let assigned: i64 = conn.query_row(
        "SELECT COUNT(*) FROM collector_cached_assignments \
         WHERE node_uuid = ?1 AND valid_from <= ?2 AND (valid_to = 0 OR valid_to > ?2)",
        rusqlite::params![node_uuid, at_nanos],
        |row| row.get(0),
    )?;
    Ok(assigned > 0)
}

/// `Store.RuntimeStats`：outbox 字节数 / 待复制条数 / 最早待复制时间。
pub fn runtime_stats(conn: &Connection) -> anyhow::Result<RuntimeStats> {
    let (spool_bytes, pending): (i64, i64) = conn.query_row(
        "SELECT COALESCE(SUM(length(payload)), 0), COUNT(*) FROM collector_outboxes",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let oldest_pending_nano: i64 = conn
        .query_row(
            "SELECT created_at FROM collector_outboxes ORDER BY spool_id ASC LIMIT 1",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten()
        .and_then(|text| time::sqlite_to_unix_nanos(&text))
        .unwrap_or(0);
    Ok(RuntimeStats {
        spool_bytes: spool_bytes.max(0) as u64,
        pending: pending.max(0) as u64,
        oldest_pending_nano,
    })
}

/// 出库记录类型（Go `outboxEvent` 等）。
pub const OUTBOX_EVENT: &str = "event";
pub const OUTBOX_OBSERVATION: &str = "observation";
pub const OUTBOX_GAP: &str = "gap";
pub const OUTBOX_HEALTH: &str = "health";
pub const OUTBOX_DATA_LOSS: &str = "data_loss";

/// `OutboxBatch`：一页出库记录（按 `spool_id` 升序，`Through` 为页内最大 spool_id）。
#[derive(Debug, Default)]
pub struct OutboxBatch {
    pub through: u64,
    pub events: Vec<proto::TelemetryEvent>,
    pub observations: Vec<proto::TelemetryObservation>,
    pub gaps: Vec<proto::SequenceGap>,
    pub health: Vec<proto::ObserverHealthSample>,
    pub data_loss: Vec<proto::CollectorDataLossFact>,
}

impl OutboxBatch {
    pub fn is_empty(&self) -> bool {
        self.through == 0
    }
}

/// `Store.ReadOutbox`：取一页出库记录并解码；`through == 0` 表示无待复制记录。
pub fn read_outbox(conn: &Connection, limit: i64) -> anyhow::Result<OutboxBatch> {
    let limit = if limit <= 0 { 256 } else { limit };
    let mut stmt = conn.prepare(
        "SELECT spool_id, record_type, payload FROM collector_outboxes \
         ORDER BY spool_id ASC LIMIT ?1",
    )?;
    let rows = stmt.query_map([limit], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Vec<u8>>(2)?,
        ))
    })?;
    let mut batch = OutboxBatch::default();
    for row in rows {
        let (spool_id, record_type, payload) = row?;
        batch.through = spool_id.max(0) as u64;
        match record_type.as_str() {
            OUTBOX_EVENT => batch.events.push(decode(&payload)?),
            OUTBOX_OBSERVATION => batch.observations.push(decode(&payload)?),
            OUTBOX_GAP => batch.gaps.push(decode(&payload)?),
            OUTBOX_HEALTH => batch.health.push(decode(&payload)?),
            OUTBOX_DATA_LOSS => batch.data_loss.push(decode(&payload)?),
            other => anyhow::bail!("unknown collector outbox record type {other:?}"),
        }
    }
    Ok(batch)
}

/// `Store.CommitReplicationAck`：同事务内清理已确认的本地事实、删出库记录、推进游标。
pub fn commit_replication_ack(tx: &Transaction<'_>, through: u64) -> anyhow::Result<()> {
    if through == 0 {
        return Ok(());
    }
    let mut stmt =
        tx.prepare("SELECT record_type, payload FROM collector_outboxes WHERE spool_id <= ?1")?;
    let rows = stmt.query_map([i64::try_from(through).unwrap_or(i64::MAX)], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    let mut acknowledged = Vec::new();
    for row in rows {
        acknowledged.push(row?);
    }
    drop(stmt);
    cleanup_local_facts(tx, &acknowledged)?;
    tx.execute(
        "DELETE FROM collector_outboxes WHERE spool_id <= ?1",
        [i64::try_from(through).unwrap_or(i64::MAX)],
    )?;
    tx.execute(
        "INSERT INTO collector_replication_cursors (id, committed_spool_through_id, next_batch_sequence, updated_at) \
         VALUES (1, ?1, 0, CURRENT_TIMESTAMP) \
         ON CONFLICT(id) DO UPDATE SET committed_spool_through_id = excluded.committed_spool_through_id, \
         updated_at = excluded.updated_at",
        [i64::try_from(through).unwrap_or(i64::MAX)],
    )?;
    Ok(())
}

/// `Store.DiscardOutbox`：清空出库（probe 从端不复制）。
pub fn discard_outbox(tx: &Transaction<'_>) -> anyhow::Result<()> {
    tx.execute("DELETE FROM collector_outboxes", [])?;
    Ok(())
}

/// `IngestResult`：一批摄入的会话 ack 与入队条数。
#[derive(Debug, Default)]
pub struct IngestResult {
    pub acks: Vec<proto::SessionAck>,
    pub enqueued: usize,
}

/// `Store.Ingest`：agent 批次摄入。事件/缺口落本地事实表并入出库，每个事件
/// 自动生成一条本从端观测；同事务按会话推进 `collector_agent_cursors`。
/// 重复记录（幂等冲突）不入队。
pub fn ingest(
    tx: &Transaction<'_>,
    batch: &proto::TelemetryBatch,
    observer_id: &str,
    received_at_nanos: i64,
) -> anyhow::Result<IngestResult> {
    if observer_id.is_empty() {
        anyhow::bail!("telemetry batch and observer ID are required");
    }
    let mut result = IngestResult::default();
    // session key = (node, session)；BTreeMap 保证 ack 顺序确定（Go 排序同 key）。
    let mut max_by_session: BTreeMap<(Vec<u8>, Vec<u8>), u64> = BTreeMap::new();
    for record in &batch.records {
        match &record.record {
            Some(proto::telemetry_record::Record::Event(event)) => {
                validate_event(event)?;
                let encoded = event.encode_to_vec();
                let inserted = tx.execute(
                    "INSERT INTO collector_stored_events \
                     (event_id, node_uuid, session_id, sequence, event_type, priority, collected_at, payload) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT (event_id) DO NOTHING",
                    rusqlite::params![
                        event.event_id,
                        event.node_uuid,
                        event.session_id,
                        event.sequence,
                        event.event_type,
                        event.priority,
                        event.collected_at_unix_nano,
                        encoded,
                    ],
                )?;
                if inserted > 0 {
                    enqueue(tx, OUTBOX_EVENT, &encoded)?;
                    result.enqueued += 1;
                }
                let observation = proto::TelemetryObservation {
                    event_id: event.event_id.clone(),
                    observer_id: observer_id.to_string(),
                    received_at_unix_nano: received_at_nanos,
                    ..Default::default()
                };
                let observation_bytes = observation.encode_to_vec();
                let observed = tx.execute(
                    "INSERT INTO collector_stored_observations (event_id, observer_id, node_uuid, received_at) \
                     VALUES (?1, ?2, ?3, ?4) ON CONFLICT (event_id, observer_id) DO NOTHING",
                    rusqlite::params![
                        event.event_id,
                        observer_id,
                        event.node_uuid,
                        received_at_nanos,
                    ],
                )?;
                if observed > 0 {
                    enqueue(tx, OUTBOX_OBSERVATION, &observation_bytes)?;
                    result.enqueued += 1;
                }
                note_session(
                    &mut max_by_session,
                    &event.node_uuid,
                    &event.session_id,
                    event.sequence,
                );
            }
            Some(proto::telemetry_record::Record::Gap(gap)) => {
                validate_gap(gap)?;
                let encoded = gap.encode_to_vec();
                let inserted = tx.execute(
                    "INSERT INTO collector_stored_gaps \
                     (gap_id, node_uuid, session_id, start_sequence, end_sequence, reason, payload, created_at_unix_nano) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT (gap_id) DO NOTHING",
                    rusqlite::params![
                        gap.gap_id,
                        gap.node_uuid,
                        gap.session_id,
                        gap.start_sequence,
                        gap.end_sequence,
                        gap.reason,
                        encoded,
                        gap.created_at_unix_nano,
                    ],
                )?;
                if inserted > 0 {
                    enqueue(tx, OUTBOX_GAP, &encoded)?;
                    result.enqueued += 1;
                }
                note_session(
                    &mut max_by_session,
                    &gap.node_uuid,
                    &gap.session_id,
                    gap.end_sequence,
                );
            }
            None => anyhow::bail!("empty telemetry record"),
        }
    }
    for ((node_uuid, session_id), max_sequence) in max_by_session {
        let ack = advance_cursor(tx, &node_uuid, &session_id, max_sequence)?;
        result.acks.push(proto::SessionAck {
            node_uuid,
            session_id,
            ack_through: ack,
        });
    }
    Ok(result)
}

/// `enqueue`：写入一条出库记录。
fn enqueue(tx: &Transaction<'_>, record_type: &str, payload: &[u8]) -> anyhow::Result<()> {
    tx.execute(
        "INSERT INTO collector_outboxes (record_type, payload, created_at) \
         VALUES (?1, ?2, CURRENT_TIMESTAMP)",
        rusqlite::params![record_type, payload],
    )?;
    Ok(())
}

fn note_session(
    max_by_session: &mut BTreeMap<(Vec<u8>, Vec<u8>), u64>,
    node_uuid: &[u8],
    session_id: &[u8],
    sequence: u64,
) {
    let key = (node_uuid.to_vec(), session_id.to_vec());
    let entry = max_by_session.entry(key).or_insert(0);
    *entry = (*entry).max(sequence);
}

/// `validateEvent`：16 字节 ID 三元组 + event_id 必须由身份元组派生。
fn validate_event(event: &proto::TelemetryEvent) -> anyhow::Result<()> {
    if event.event_id.len() != 16
        || event.node_uuid.len() != 16
        || event.session_id.len() != 16
        || event.sequence == 0
        || event.payload.is_none()
    {
        anyhow::bail!("invalid telemetry event");
    }
    let expected = Store::event_id(&event.node_uuid, &event.session_id, event.sequence)
        .map_err(|_| anyhow::anyhow!("telemetry event ID does not match identity tuple"))?;
    if expected != event.event_id.as_slice() {
        anyhow::bail!("telemetry event ID does not match identity tuple");
    }
    Ok(())
}

/// `validateGap`：16 字节 ID 三元组 + 序号区间合法 + 原因非 unspecified。
fn validate_gap(gap: &proto::SequenceGap) -> anyhow::Result<()> {
    if gap.gap_id.len() != 16
        || gap.node_uuid.len() != 16
        || gap.session_id.len() != 16
        || gap.start_sequence == 0
        || gap.end_sequence < gap.start_sequence
        || gap.reason == proto::GapReason::Unspecified as i32
    {
        anyhow::bail!("invalid telemetry sequence gap");
    }
    Ok(())
}

/// `advanceCursor`：按已存事件与缺口把会话 ack 推进到 `max_sequence` 前最近的
/// 连续位；空洞前停住（与 Go 一致，不跨空洞确认）。
fn advance_cursor(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    session_id: &[u8],
    max_sequence: u64,
) -> anyhow::Result<u64> {
    let mut ack_through: u64 = tx
        .query_row(
            "SELECT ack_through FROM collector_agent_cursors \
             WHERE node_uuid = ?1 AND session_id = ?2",
            rusqlite::params![node_uuid, session_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .map(|value| value.max(0) as u64)
        .unwrap_or(0);
    if max_sequence <= ack_through {
        return Ok(ack_through);
    }
    let mut stmt = tx.prepare(
        "SELECT sequence FROM collector_stored_events \
         WHERE node_uuid = ?1 AND session_id = ?2 AND sequence > ?3 AND sequence <= ?4",
    )?;
    let present = stmt
        .query_map(
            rusqlite::params![node_uuid, session_id, ack_through, max_sequence],
            |row| row.get::<_, i64>(0).map(|value| value.max(0) as u64),
        )?
        .collect::<Result<Vec<u64>, _>>()?;
    drop(stmt);
    let present: HashSet<u64> = present.into_iter().collect();
    let mut stmt = tx.prepare(
        "SELECT start_sequence, end_sequence FROM collector_stored_gaps \
         WHERE node_uuid = ?1 AND session_id = ?2 AND end_sequence > ?3 AND start_sequence <= ?4",
    )?;
    let gaps = stmt
        .query_map(
            rusqlite::params![node_uuid, session_id, ack_through, max_sequence],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?.max(0) as u64,
                    row.get::<_, i64>(1)?.max(0) as u64,
                ))
            },
        )?
        .collect::<Result<Vec<(u64, u64)>, _>>()?;
    drop(stmt);
    while ack_through < max_sequence {
        let next = ack_through + 1;
        if present.contains(&next) {
            ack_through = next;
            continue;
        }
        let mut advanced = false;
        for (start, end) in &gaps {
            if *start <= next && *end >= next {
                ack_through = *end;
                advanced = true;
                break;
            }
        }
        if !advanced {
            break;
        }
    }
    tx.execute(
        "INSERT INTO collector_agent_cursors (node_uuid, session_id, ack_through, updated_at) \
         VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP) \
         ON CONFLICT (node_uuid, session_id) DO UPDATE SET \
         ack_through = excluded.ack_through, updated_at = excluded.updated_at",
        rusqlite::params![node_uuid, session_id, ack_through],
    )?;
    Ok(ack_through)
}

/// `cleanupLocalFacts`：按记录类型删除已确认对应的本地事实行。
fn cleanup_local_facts(
    tx: &Transaction<'_>,
    acknowledged: &[(String, Vec<u8>)],
) -> anyhow::Result<()> {
    for (record_type, payload) in acknowledged {
        match record_type.as_str() {
            OUTBOX_EVENT => {
                let item: proto::TelemetryEvent = decode(payload)?;
                tx.execute(
                    "DELETE FROM collector_stored_events WHERE event_id = ?1",
                    [item.event_id.as_slice()],
                )?;
            }
            OUTBOX_OBSERVATION => {
                let item: proto::TelemetryObservation = decode(payload)?;
                tx.execute(
                    "DELETE FROM collector_stored_observations \
                     WHERE event_id = ?1 AND observer_id = ?2",
                    rusqlite::params![item.event_id, item.observer_id],
                )?;
            }
            OUTBOX_GAP => {
                let item: proto::SequenceGap = decode(payload)?;
                tx.execute(
                    "DELETE FROM collector_stored_gaps WHERE gap_id = ?1",
                    [item.gap_id.as_slice()],
                )?;
            }
            OUTBOX_HEALTH => {
                let item: proto::ObserverHealthSample = decode(payload)?;
                tx.execute(
                    "DELETE FROM collector_health_evidences \
                     WHERE observer_id = ?1 AND sampled_at = ?2 AND process_session = ?3",
                    rusqlite::params![
                        item.observer_id,
                        item.sampled_at_unix_nano,
                        item.process_session
                    ],
                )?;
            }
            OUTBOX_DATA_LOSS => {
                let item: proto::CollectorDataLossFact = decode(payload)?;
                tx.execute(
                    "DELETE FROM collector_data_losses WHERE fact_id = ?1",
                    [item.fact_id.as_slice()],
                )?;
            }
            other => anyhow::bail!("unknown collector outbox record type {other:?}"),
        }
    }
    Ok(())
}

fn decode<T: prost::Message + Default>(payload: &[u8]) -> anyhow::Result<T> {
    Ok(T::decode(payload)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    fn temp_db(tag: &str) -> db::Db {
        let dir = std::env::temp_dir().join(format!(
            "santaizi-collector-store-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = db::migrate::open_collector(dir.join("collector.db").to_str().unwrap()).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        db
    }

    fn config(
        assignments: Vec<proto::NodeAssignment>,
        revoked: Vec<Vec<u8>>,
    ) -> proto::CollectorAuthorizationConfig {
        proto::CollectorAuthorizationConfig {
            config_version: 7,
            primary_public_key: vec![0xab; 32],
            key_id: vec![0xcd; 8],
            revoked_node_uuids: revoked,
            assignments,
            agent_ca_certificate_pem: "agent-ca".to_string(),
            kind: proto::CollectorKind::Observer as i32,
            ..Default::default()
        }
    }

    #[test]
    fn authorization_roundtrip_and_node_checks() {
        let db = temp_db("auth");
        let assignment = proto::NodeAssignment {
            node_uuid: vec![0x11; 16],
            observer_id: "collector-edge".to_string(),
            valid_from_unix_nano: 1_000,
            valid_to_unix_nano: 0,
            generation: 3,
            config_version: 7,
        };
        db.write(|tx| {
            save_authorization(
                tx,
                "collector-edge",
                &config(vec![assignment.clone()], vec![vec![0x22; 16]]),
                5_000,
            )
        })
        .unwrap();

        let cache = db
            .read(|conn| Ok(authorization(conn)?.expect("cache")))
            .unwrap();
        assert_eq!(cache.collector_uuid, "collector-edge");
        assert_eq!(cache.config_version, 7);
        assert_eq!(cache.primary_public_key, vec![0xab; 32]);
        assert_eq!(cache.last_primary_seen_nano, 5_000);
        assert_eq!(cache.agent_ca_certificate_pem, "agent-ca");
        assert_eq!(cache.kind, probes::COLLECTOR_KIND_OBSERVER);

        let now = 2_000_i64;
        assert!(
            db.read(|conn| is_node_authorized(conn, &[0x11; 16], now))
                .unwrap()
        );
        assert!(
            !db.read(|conn| is_node_authorized(conn, &[0x22; 16], now))
                .unwrap()
        );
        assert!(
            !db.read(|conn| is_node_authorized(conn, &[0x33; 16], now))
                .unwrap()
        );

        // 二次保存：指派/吊销整块替换，Agent CA 空值时保留旧值。
        db.write(|tx| {
            save_authorization(
                tx,
                "collector-edge",
                &proto::CollectorAuthorizationConfig {
                    agent_ca_certificate_pem: String::new(),
                    ..config(vec![], vec![])
                },
                9_000,
            )
        })
        .unwrap();
        let cache = db
            .read(|conn| Ok(authorization(conn)?.expect("cache")))
            .unwrap();
        assert_eq!(cache.agent_ca_certificate_pem, "agent-ca");
        assert_eq!(cache.last_primary_seen_nano, 9_000);
        assert!(
            !db.read(|conn| is_node_authorized(conn, &[0x11; 16], now))
                .unwrap()
        );

        db.write(|tx| touch_primary_seen(tx, 12_345)).unwrap();
        let cache = db
            .read(|conn| Ok(authorization(conn)?.expect("cache")))
            .unwrap();
        assert_eq!(cache.last_primary_seen_nano, 12_345);
    }

    #[test]
    fn authorization_replaces_probe_targets() {
        let db = temp_db("targets");
        let mut config = config(vec![], vec![]);
        config.kind = proto::CollectorKind::Probe as i32;
        config.probe = Some(proto::ProbeConfig {
            ip_families: vec![proto::ProbeIpFamily::Ipv4 as i32],
            route_interval_seconds: 3_600,
            mtr_probes: 4,
            ..Default::default()
        });
        config.targets = vec![proto::ProbeTarget {
            server_id: 7,
            server_name: "edge".into(),
            hostname: "edge.example".into(),
            tcp_ports: vec![443, 0, 22],
            enable_icmp: true,
            enable_tcp: true,
            enable_mtr: true,
            interval_seconds: 30,
            mtr_interval_seconds: 300,
            ..Default::default()
        }];
        db.write(|tx| save_authorization(tx, "collector-edge", &config, 1))
            .unwrap();
        let targets = db.read(list_probe_targets).unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].hostname, "edge.example");
        assert_eq!(targets[0].tcp_ports, "443,22");
        assert!(targets[0].enable_ipv4);
        assert!(!targets[0].enable_ipv6);
        assert_eq!(targets[0].route_interval_sec, 3_600);
        assert_eq!(targets[0].mtr_probes, 4);

        config.targets.clear();
        db.write(|tx| save_authorization(tx, "collector-edge", &config, 2))
            .unwrap();
        assert!(db.read(list_probe_targets).unwrap().is_empty());
    }

    #[test]
    fn assignment_validity_window() {
        let db = temp_db("window");
        let assignment = proto::NodeAssignment {
            node_uuid: vec![0x11; 16],
            observer_id: "collector-edge".to_string(),
            valid_from_unix_nano: 1_000,
            valid_to_unix_nano: 2_000,
            generation: 1,
            config_version: 1,
        };
        db.write(|tx| {
            save_authorization(tx, "collector-edge", &config(vec![assignment], vec![]), 1)
        })
        .unwrap();
        let node = vec![0x11; 16];
        assert!(
            !db.read(|conn| is_node_authorized(conn, &node, 999))
                .unwrap()
        );
        assert!(
            db.read(|conn| is_node_authorized(conn, &node, 1_000))
                .unwrap()
        );
        assert!(
            db.read(|conn| is_node_authorized(conn, &node, 1_999))
                .unwrap()
        );
        assert!(
            !db.read(|conn| is_node_authorized(conn, &node, 2_000))
                .unwrap()
        );
    }

    #[test]
    fn rejects_invalid_config() {
        let db = temp_db("invalid");
        let err = db
            .write(|tx| save_authorization(tx, "", &config(vec![], vec![]), 1))
            .unwrap_err();
        assert!(err.to_string().contains("invalid collector authorization"));
    }

    #[test]
    fn runtime_stats_counts_outbox() {
        let db = temp_db("stats");
        let stats = db.read(runtime_stats).unwrap();
        assert_eq!(stats.spool_bytes, 0);
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.oldest_pending_nano, 0);

        let created = time::unix_nanos_to_sqlite(777_000);
        db.write(|tx| {
            tx.execute(
                "INSERT INTO collector_outboxes (record_type, payload, created_at) \
                 VALUES ('event', ?1, ?2)",
                rusqlite::params![vec![0u8; 10], created],
            )?;
            tx.execute(
                "INSERT INTO collector_outboxes (record_type, payload, created_at) \
                 VALUES ('observation', ?1, ?2)",
                rusqlite::params![vec![0u8; 5], created],
            )?;
            Ok(())
        })
        .unwrap();
        let stats = db.read(runtime_stats).unwrap();
        assert_eq!(stats.spool_bytes, 15);
        assert_eq!(stats.pending, 2);
        assert_eq!(stats.oldest_pending_nano, 777_000);
    }

    fn push_outbox(
        tx: &Transaction<'_>,
        record_type: &str,
        payload: &impl prost::Message,
    ) -> anyhow::Result<()> {
        tx.execute(
            "INSERT INTO collector_outboxes (record_type, payload, created_at) \
             VALUES (?1, ?2, CURRENT_TIMESTAMP)",
            rusqlite::params![record_type, payload.encode_to_vec()],
        )?;
        Ok(())
    }

    fn sample_event() -> proto::TelemetryEvent {
        proto::TelemetryEvent {
            event_id: vec![0x5a; 16],
            node_uuid: vec![0x11; 16],
            session_id: vec![0x22; 16],
            sequence: 4,
            event_type: proto::TelemetryEventType::Host as i32,
            ..Default::default()
        }
    }

    #[test]
    fn read_outbox_decodes_record_types() {
        let db = temp_db("read-outbox");
        let observation = proto::TelemetryObservation {
            event_id: vec![0x5a; 16],
            observer_id: "collector-edge".to_string(),
            received_at_unix_nano: 10,
            ..Default::default()
        };
        let gap = proto::SequenceGap {
            gap_id: vec![0x77; 16],
            node_uuid: vec![0x11; 16],
            session_id: vec![0x22; 16],
            start_sequence: 1,
            end_sequence: 3,
            ..Default::default()
        };
        let health = proto::ObserverHealthSample {
            observer_id: "collector-edge".to_string(),
            sampled_at_unix_nano: 20,
            healthy: true,
            process_session: "sess".to_string(),
        };
        let loss = proto::CollectorDataLossFact {
            fact_id: vec![0x88; 16],
            collector_uuid: "collector-edge".to_string(),
            occurred_at_unix_nano: 30,
            lost_records: 2,
            ..Default::default()
        };
        db.write(|tx| {
            push_outbox(tx, OUTBOX_EVENT, &sample_event())?;
            push_outbox(tx, OUTBOX_OBSERVATION, &observation)?;
            push_outbox(tx, OUTBOX_GAP, &gap)?;
            push_outbox(tx, OUTBOX_HEALTH, &health)?;
            push_outbox(tx, OUTBOX_DATA_LOSS, &loss)?;
            Ok(())
        })
        .unwrap();

        let batch = db.read(|conn| read_outbox(conn, 16)).unwrap();
        assert_eq!(batch.through, 5);
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].event_id, vec![0x5a; 16]);
        assert_eq!(batch.observations[0].observer_id, "collector-edge");
        assert_eq!(batch.gaps[0].end_sequence, 3);
        assert_eq!(batch.health[0].sampled_at_unix_nano, 20);
        assert_eq!(batch.data_loss[0].lost_records, 2);

        // 限量分页：只取前两行，through 为页内最大 spool_id。
        let page = db.read(|conn| read_outbox(conn, 2)).unwrap();
        assert_eq!(page.through, 2);
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.observations.len(), 1);
        assert!(!page.is_empty());

        // limit <= 0 回退默认 256（Go 同）：读全部分页。
        let fallback = db.read(|conn| read_outbox(conn, 0)).unwrap();
        assert_eq!(fallback.through, 5);
    }

    #[test]
    fn read_outbox_rejects_unknown_record_type() {
        let db = temp_db("read-outbox-unknown");
        db.write(|tx| {
            tx.execute(
                "INSERT INTO collector_outboxes (record_type, payload) VALUES ('mystery', ?1)",
                rusqlite::params![vec![0u8; 4]],
            )?;
            Ok(())
        })
        .unwrap();
        let err = db.read(|conn| read_outbox(conn, 8)).unwrap_err();
        assert!(
            err.to_string()
                .contains("unknown collector outbox record type")
        );
    }

    #[test]
    fn commit_replication_ack_clears_outbox_and_facts() {
        let db = temp_db("commit-ack");
        let event = sample_event();
        let observation = proto::TelemetryObservation {
            event_id: event.event_id.clone(),
            observer_id: "collector-edge".to_string(),
            received_at_unix_nano: 10,
            ..Default::default()
        };
        db.write(|tx| {
            tx.execute(
                "INSERT INTO collector_stored_events (event_id, node_uuid, session_id, sequence, \
                 event_type, priority, collected_at, payload) VALUES (?1, ?2, ?3, 4, 1, 1, 5, ?4)",
                rusqlite::params![event.event_id, event.node_uuid, event.session_id, vec![0u8; 8]],
            )?;
            tx.execute(
                "INSERT INTO collector_stored_observations (event_id, observer_id, node_uuid, received_at) \
                 VALUES (?1, ?2, ?3, 10)",
                rusqlite::params![event.event_id, "collector-edge", event.node_uuid],
            )?;
            push_outbox(tx, OUTBOX_EVENT, &event)?;
            push_outbox(tx, OUTBOX_OBSERVATION, &observation)?;
            push_outbox(tx, OUTBOX_HEALTH, &proto::ObserverHealthSample {
                observer_id: "collector-edge".to_string(),
                sampled_at_unix_nano: 20,
                healthy: true,
                process_session: "sess".to_string(),
            })?;
            tx.execute(
                "INSERT INTO collector_health_evidences (observer_id, sampled_at, healthy, process_session, replicated) \
                 VALUES ('collector-edge', 20, 1, 'sess', 0)",
                [],
            )?;
            Ok(())
        })
        .unwrap();

        db.write(|tx| commit_replication_ack(tx, 2)).unwrap();

        let stats = db.read(runtime_stats).unwrap();
        assert_eq!(stats.pending, 1, "第三行（health）未被确认");
        let events: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM collector_stored_events WHERE event_id = ?1",
                    [event.event_id.as_slice()],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(events, 0);
        let observations: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM collector_stored_observations WHERE event_id = ?1",
                    [event.event_id.as_slice()],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(observations, 0);
        let health: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM collector_health_evidences WHERE observer_id = 'collector-edge'",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(health, 1, "health 出库行（spool_id=3）未被确认，证据行保留");
        let cursor: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT committed_spool_through_id FROM collector_replication_cursors WHERE id = 1",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(cursor, 2);

        // 确认剩余行：health 证据行随之清理。
        db.write(|tx| commit_replication_ack(tx, 3)).unwrap();
        let stats = db.read(runtime_stats).unwrap();
        assert_eq!(stats.pending, 0);
        let health: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM collector_health_evidences WHERE observer_id = 'collector-edge'",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(health, 0);

        // 重复提交同一游标不报错（回执去重场景）。
        db.write(|tx| commit_replication_ack(tx, 3)).unwrap();
        let stats = db.read(runtime_stats).unwrap();
        assert_eq!(stats.pending, 0);
        // through=0 是空操作。
        db.write(|tx| commit_replication_ack(tx, 0)).unwrap();
        assert_eq!(db.read(runtime_stats).unwrap().pending, 0);
    }

    #[test]
    fn discard_outbox_clears_everything() {
        let db = temp_db("discard");
        db.write(|tx| {
            push_outbox(tx, OUTBOX_EVENT, &sample_event())?;
            push_outbox(
                tx,
                OUTBOX_HEALTH,
                &proto::ObserverHealthSample {
                    observer_id: "collector-edge".to_string(),
                    sampled_at_unix_nano: 20,
                    healthy: true,
                    process_session: "sess".to_string(),
                },
            )?;
            Ok(())
        })
        .unwrap();
        db.write(discard_outbox).unwrap();
        let stats = db.read(runtime_stats).unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.spool_bytes, 0);
    }

    fn node() -> Vec<u8> {
        vec![0x11; 16]
    }

    fn session() -> Vec<u8> {
        vec![0x22; 16]
    }

    fn valid_event(sequence: u64) -> proto::TelemetryEvent {
        let event_id = Store::event_id(&node(), &session(), sequence).expect("event id");
        proto::TelemetryEvent {
            event_id: event_id.to_vec(),
            node_uuid: node(),
            session_id: session(),
            sequence,
            event_type: proto::TelemetryEventType::Host as i32,
            priority: proto::TelemetryPriority::P1Important as i32,
            collected_at_unix_nano: 1_000,
            payload: Some(proto::telemetry_event::Payload::Host(proto::Host {
                ip: "10.0.0.1".to_string(),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn valid_gap(start: u64, end: u64) -> proto::SequenceGap {
        proto::SequenceGap {
            gap_id: vec![0x33; 16],
            node_uuid: node(),
            session_id: session(),
            start_sequence: start,
            end_sequence: end,
            reason: proto::GapReason::Compacted as i32,
            created_at_unix_nano: 1_000,
            ..Default::default()
        }
    }

    fn event_record(sequence: u64) -> proto::TelemetryRecord {
        proto::TelemetryRecord {
            record: Some(proto::telemetry_record::Record::Event(valid_event(
                sequence,
            ))),
        }
    }

    fn gap_record(start: u64, end: u64) -> proto::TelemetryRecord {
        proto::TelemetryRecord {
            record: Some(proto::telemetry_record::Record::Gap(valid_gap(start, end))),
        }
    }

    fn outbox_types(db: &db::Db) -> Vec<String> {
        db.read(|conn| {
            let mut stmt =
                conn.prepare("SELECT record_type FROM collector_outboxes ORDER BY spool_id")?;
            let types = stmt
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(types)
        })
        .unwrap()
    }

    fn agent_cursor(db: &db::Db) -> Option<u64> {
        db.read(|conn| {
            conn.query_row(
                "SELECT ack_through FROM collector_agent_cursors \
                 WHERE node_uuid = ?1 AND session_id = ?2",
                rusqlite::params![node(), session()],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map(|row| row.map(|value| value.max(0) as u64))
            .map_err(anyhow::Error::from)
        })
        .ok()
        .flatten()
    }

    #[test]
    fn ingest_enqueues_events_observations_and_gaps() {
        let db = temp_db("ingest");
        let batch = proto::TelemetryBatch {
            records: vec![event_record(1), event_record(3), gap_record(2, 2)],
        };
        let result = db
            .write(|tx| ingest(tx, &batch, "collector-edge", 5_000))
            .unwrap();
        // 两条事件各带一条自动观测 + 一条缺口 = 5 条入队。
        assert_eq!(result.enqueued, 5);
        assert_eq!(result.acks.len(), 1);
        assert_eq!(result.acks[0].node_uuid, node());
        assert_eq!(result.acks[0].session_id, session());
        assert_eq!(result.acks[0].ack_through, 3, "缺口 2 被跨过，ack 到 3");

        assert_eq!(
            outbox_types(&db),
            vec!["event", "observation", "event", "observation", "gap"]
        );
        let counts: (i64, i64, i64) = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT (SELECT COUNT(*) FROM collector_stored_events), \
                     (SELECT COUNT(*) FROM collector_stored_observations), \
                     (SELECT COUNT(*) FROM collector_stored_gaps)",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )?)
            })
            .unwrap();
        assert_eq!(counts, (2, 2, 1));
        assert_eq!(agent_cursor(&db), Some(3));
    }

    #[test]
    fn ingest_is_idempotent_for_replayed_records() {
        let db = temp_db("ingest-replay");
        let batch = proto::TelemetryBatch {
            records: vec![event_record(1), event_record(2)],
        };
        let first = db
            .write(|tx| ingest(tx, &batch, "collector-edge", 5_000))
            .unwrap();
        assert_eq!(first.enqueued, 4);
        let second = db
            .write(|tx| ingest(tx, &batch, "collector-edge", 6_000))
            .unwrap();
        assert_eq!(second.enqueued, 0, "重放记录不得重复入队");
        assert_eq!(second.acks[0].ack_through, 2);
        assert_eq!(outbox_types(&db).len(), 4);
    }

    #[test]
    fn ingest_ack_stops_at_hole_without_gap() {
        let db = temp_db("ingest-hole");
        let batch = proto::TelemetryBatch {
            records: vec![event_record(1), event_record(3)],
        };
        let result = db
            .write(|tx| ingest(tx, &batch, "collector-edge", 5_000))
            .unwrap();
        assert_eq!(result.acks[0].ack_through, 1, "无缺口证据时不得跨空洞确认");

        // 补一条覆盖 2 的缺口记录：本批 max=2，ack 推到 2（Go 同：不超本批上界）。
        let batch = proto::TelemetryBatch {
            records: vec![gap_record(2, 2)],
        };
        let result = db
            .write(|tx| ingest(tx, &batch, "collector-edge", 6_000))
            .unwrap();
        assert_eq!(result.acks[0].ack_through, 2);
        assert_eq!(agent_cursor(&db), Some(2));

        // 事件 3 已在库：下一批带到上界 3 时 ack 补齐到 3。
        let batch = proto::TelemetryBatch {
            records: vec![event_record(3)],
        };
        let result = db
            .write(|tx| ingest(tx, &batch, "collector-edge", 7_000))
            .unwrap();
        assert_eq!(result.acks[0].ack_through, 3);
        assert_eq!(agent_cursor(&db), Some(3));
    }

    #[test]
    fn ingest_rejects_invalid_records() {
        let db = temp_db("ingest-invalid");
        // event_id 与身份元组不符。
        let mut tampered = valid_event(1);
        tampered.event_id = vec![0x99; 16];
        let err = db
            .write(|tx| {
                ingest(
                    tx,
                    &proto::TelemetryBatch {
                        records: vec![proto::TelemetryRecord {
                            record: Some(proto::telemetry_record::Record::Event(tampered)),
                        }],
                    },
                    "collector-edge",
                    1,
                )
            })
            .unwrap_err();
        assert!(err.to_string().contains("identity tuple"));

        // 序号为 0。
        let mut zero = valid_event(1);
        zero.sequence = 0;
        let err = db
            .write(|tx| {
                ingest(
                    tx,
                    &proto::TelemetryBatch {
                        records: vec![proto::TelemetryRecord {
                            record: Some(proto::telemetry_record::Record::Event(zero)),
                        }],
                    },
                    "collector-edge",
                    1,
                )
            })
            .unwrap_err();
        assert!(err.to_string().contains("invalid telemetry event"));

        // 缺口区间反了。
        let err = db
            .write(|tx| {
                ingest(
                    tx,
                    &proto::TelemetryBatch {
                        records: vec![gap_record(5, 2)],
                    },
                    "collector-edge",
                    1,
                )
            })
            .unwrap_err();
        assert!(err.to_string().contains("invalid telemetry sequence gap"));

        // 空记录体。
        let err = db
            .write(|tx| {
                ingest(
                    tx,
                    &proto::TelemetryBatch {
                        records: vec![proto::TelemetryRecord { record: None }],
                    },
                    "collector-edge",
                    1,
                )
            })
            .unwrap_err();
        assert!(err.to_string().contains("empty telemetry record"));

        // 观察者为空。
        let err = db
            .write(|tx| {
                ingest(
                    tx,
                    &proto::TelemetryBatch {
                        records: vec![event_record(1)],
                    },
                    "",
                    1,
                )
            })
            .unwrap_err();
        assert!(err.to_string().contains("observer ID"));
        assert!(outbox_types(&db).is_empty(), "失败批次不得留下出库记录");
    }

    #[test]
    fn ingest_tracks_sessions_independently() {
        let db = temp_db("ingest-sessions");
        let mut other = valid_event(1);
        other.node_uuid = vec![0x44; 16];
        other.event_id = Store::event_id(&other.node_uuid, &session(), 1)
            .expect("event id")
            .to_vec();
        let batch = proto::TelemetryBatch {
            records: vec![
                event_record(1),
                proto::TelemetryRecord {
                    record: Some(proto::telemetry_record::Record::Event(other)),
                },
            ],
        };
        let result = db
            .write(|tx| ingest(tx, &batch, "collector-edge", 5_000))
            .unwrap();
        assert_eq!(result.acks.len(), 2, "每个会话各回一条 ack");
        let throughs: Vec<u64> = result.acks.iter().map(|ack| ack.ack_through).collect();
        assert_eq!(throughs, vec![1, 1]);
        assert_eq!(outbox_types(&db).len(), 4);
    }
}
