//! collectors 表访问：注册令牌匹配、UUID 查找与配置版本递增。
//!
//! 对齐 Go `service/rpc/collector_primary.go` 的 `findCollectorByToken` /
//! `findCollectorByUUID`：令牌只存 SHA-256，逐条常量时间比较。

use rusqlite::{Connection, OptionalExtension, Transaction};

use crate::proto;
use crate::telemetry::credential;

#[derive(Debug, Clone)]
pub struct CollectorRow {
    pub collector_uuid: String,
    pub name: String,
    pub address: String,
    pub token_hash: Vec<u8>,
    pub generation: u64,
    pub config_version: u64,
    pub tls: bool,
    pub insecure_tls: bool,
    pub kind: String,
    pub listen_port: i64,
    pub location: Option<String>,
    pub probe_interval_sec: i64,
    pub mtr_interval_sec: i64,
    pub mtr_probes: i64,
    pub tcp_ports: String,
    pub enable_icmp: bool,
    pub enable_tcp: bool,
    pub enable_mtr: bool,
    pub enable_ipv4: bool,
    pub enable_ipv6: bool,
    pub probe_notify: bool,
    pub notification_tag: Option<String>,
    pub latency_notify: bool,
    pub min_latency_ms: f64,
    pub max_latency_ms: f64,
    pub fail_threshold: i64,
    pub route_interval_sec: i64,
    pub route_keep: i64,
    pub revoked: bool,
}

impl CollectorRow {
    pub fn is_probe(&self) -> bool {
        crate::telemetry::probes::is_probe_kind(&self.kind)
    }
}

impl CollectorRuntimeRow {
    /// `CollectorRuntimeFromProto`：同步流与复制流共用的行形态；`status`/`last_seen`
    /// 由写入方填。
    pub fn from_proto(collector_uuid: &str, runtime: &proto::CollectorRuntime) -> Self {
        Self {
            collector_uuid: collector_uuid.to_string(),
            spool_size: runtime.spool_size,
            pending_records: runtime.pending_records,
            oldest_pending: runtime.oldest_pending_unix_nano,
            replication_cursor: runtime.replication_cursor,
            connected_agents: runtime.connected_agents,
            protocol_version: runtime.protocol_version.clone(),
            software_version: runtime.software_version.clone(),
            last_primary_seen: runtime.last_primary_seen_unix_nano,
            heartbeat_rtt_ms: runtime.heartbeat_rtt_ms,
            heartbeat_rtt_sampled_at: runtime.heartbeat_rtt_sampled_at_unix_nano,
            replication_rtt_ms: runtime.replication_rtt_ms,
            replication_rtt_sampled_at: runtime.replication_rtt_sampled_at_unix_nano,
        }
    }
}

const COLUMNS: &str = "collector_uuid, name, address, token_hash, generation, config_version, tls, insecure_tls, kind, listen_port, location, probe_interval_sec, mtr_interval_sec, mtr_probes, tcp_ports, enable_icmp, enable_tcp, enable_mtr, enable_ipv4, enable_ipv6, probe_notify, notification_tag, latency_notify, min_latency_ms, max_latency_ms, fail_threshold, route_interval_sec, route_keep, revoked";

fn row_to_collector(row: &rusqlite::Row<'_>) -> rusqlite::Result<CollectorRow> {
    Ok(CollectorRow {
        collector_uuid: row.get(0)?,
        name: row.get(1)?,
        address: row.get(2)?,
        token_hash: row.get(3)?,
        generation: row.get(4)?,
        config_version: row.get(5)?,
        tls: row.get::<_, Option<i64>>(6)?.unwrap_or(0) != 0,
        insecure_tls: row.get::<_, Option<i64>>(7)?.unwrap_or(0) != 0,
        kind: row.get(8)?,
        listen_port: row.get::<_, Option<i64>>(9)?.unwrap_or(0),
        location: row.get(10)?,
        probe_interval_sec: row.get::<_, Option<i64>>(11)?.unwrap_or(0),
        mtr_interval_sec: row.get::<_, Option<i64>>(12)?.unwrap_or(0),
        mtr_probes: row.get::<_, Option<i64>>(13)?.unwrap_or(0),
        tcp_ports: row.get::<_, Option<String>>(14)?.unwrap_or_default(),
        enable_icmp: row.get::<_, Option<i64>>(15)?.unwrap_or(0) != 0,
        enable_tcp: row.get::<_, Option<i64>>(16)?.unwrap_or(0) != 0,
        enable_mtr: row.get::<_, Option<i64>>(17)?.unwrap_or(0) != 0,
        enable_ipv4: row.get::<_, Option<i64>>(18)?.unwrap_or(1) != 0,
        enable_ipv6: row.get::<_, Option<i64>>(19)?.unwrap_or(1) != 0,
        probe_notify: row.get::<_, Option<i64>>(20)?.unwrap_or(0) != 0,
        notification_tag: row.get(21)?,
        latency_notify: row.get::<_, Option<i64>>(22)?.unwrap_or(0) != 0,
        min_latency_ms: row.get::<_, Option<f64>>(23)?.unwrap_or(0.0),
        max_latency_ms: row.get::<_, Option<f64>>(24)?.unwrap_or(0.0),
        fail_threshold: row.get::<_, Option<i64>>(25)?.unwrap_or(0),
        route_interval_sec: row.get::<_, Option<i64>>(26)?.unwrap_or(0),
        route_keep: row.get::<_, Option<i64>>(27)?.unwrap_or(0),
        revoked: row.get::<_, Option<i64>>(28)?.unwrap_or(0) != 0,
    })
}

/// 注册令牌匹配：扫描未吊销未删除的 collector，常量时间比较哈希。
pub fn find_by_token(conn: &Connection, token: &str) -> anyhow::Result<Option<CollectorRow>> {
    if token.is_empty() {
        return Ok(None);
    }
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLUMNS} FROM collectors WHERE revoked = 0 AND deleted = 0"
    ))?;
    let rows = stmt.query_map([], row_to_collector)?;
    for row in rows {
        let collector = row?;
        if credential::registration_token_matches(token, &collector.token_hash) {
            return Ok(Some(collector));
        }
    }
    Ok(None)
}

/// UUID 查找：仅未吊销未删除。
pub fn find_by_uuid(
    conn: &Connection,
    collector_uuid: &str,
) -> anyhow::Result<Option<CollectorRow>> {
    if collector_uuid.is_empty() {
        return Ok(None);
    }
    conn.query_row(&format!("SELECT {COLUMNS} FROM collectors WHERE collector_uuid = ? AND revoked = 0 AND deleted = 0"), [collector_uuid], row_to_collector)
        .optional()
        .map_err(Into::into)
}

/// 事务内 UUID 查找：指派装配与写入同事务，避免读到半提交状态。
pub fn find_by_uuid_tx(
    tx: &Transaction<'_>,
    collector_uuid: &str,
) -> anyhow::Result<Option<CollectorRow>> {
    if collector_uuid.is_empty() {
        return Ok(None);
    }
    tx.query_row(&format!("SELECT {COLUMNS} FROM collectors WHERE collector_uuid = ? AND revoked = 0 AND deleted = 0"), [collector_uuid], row_to_collector)
        .optional()
        .map_err(Into::into)
}

pub fn list_active(conn: &Connection) -> anyhow::Result<Vec<CollectorRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLUMNS} FROM collectors WHERE revoked = 0 AND deleted = 0"
    ))?;
    let rows = stmt
        .query_map([], row_to_collector)?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

/// 事务内版本：绑定同步等写路径需要与 `bump_config_version` 同一事务读取。
pub fn list_active_tx(tx: &Transaction<'_>) -> anyhow::Result<Vec<CollectorRow>> {
    let mut stmt = tx.prepare(&format!(
        "SELECT {COLUMNS} FROM collectors WHERE revoked = 0 AND deleted = 0"
    ))?;
    let rows = stmt
        .query_map([], row_to_collector)?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

/// `CollectorRuntimeFromProto` 的行形态；`status`/`last_seen` 由写入方填。
#[derive(Debug, Clone)]
pub struct CollectorRuntimeRow {
    pub collector_uuid: String,
    pub spool_size: u64,
    pub pending_records: u64,
    pub oldest_pending: i64,
    pub replication_cursor: u64,
    pub connected_agents: u64,
    pub protocol_version: String,
    pub software_version: String,
    pub last_primary_seen: i64,
    pub heartbeat_rtt_ms: f64,
    pub heartbeat_rtt_sampled_at: i64,
    pub replication_rtt_ms: f64,
    pub replication_rtt_sampled_at: i64,
}

/// `UpsertCollectorRuntime`：同步流运行态快照 upsert，`status=online`。
/// Go 仅在 `software_version` 非空时更新该列；`include_last_sync` 对齐 Go 的
/// `includeLastSync`——复制路径额外刷新 `last_sync`（同步流心跳不刷新）。
/// connection_latency_buckets 由调用方（`telemetry::latency::record_collector_latency`）
/// 在本事务提交后落桶。
pub fn upsert_runtime(
    tx: &Transaction<'_>,
    row: &CollectorRuntimeRow,
    now_nanos: i64,
    include_last_sync: bool,
) -> anyhow::Result<()> {
    // 首插与冲突更新都要带上 last_sync，否则复制路径首帧后该列仍是 NULL。
    let (insert_column, insert_value, update_set) = if include_last_sync {
        (
            ", last_sync",
            ", ?15",
            ",\n           last_sync = excluded.last_seen",
        )
    } else {
        ("", "", "")
    };
    let spool_size = row.spool_size as i64;
    let pending_records = row.pending_records as i64;
    let replication_cursor = row.replication_cursor as i64;
    let connected_agents = row.connected_agents as i64;
    let sql = format!(
        "INSERT INTO collector_runtimes (collector_uuid, status, last_seen, spool_size, pending_records, oldest_pending, replication_cursor, connected_agents, protocol_version, software_version, last_primary_seen, heartbeat_rtt_ms, heartbeat_rtt_sampled_at, replication_rtt_ms, replication_rtt_sampled_at, updated_at{insert_column})
         VALUES (?, 'online', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP{insert_value})
         ON CONFLICT(collector_uuid) DO UPDATE SET
           status = 'online',
           last_seen = excluded.last_seen,
           spool_size = excluded.spool_size,
           pending_records = excluded.pending_records,
           oldest_pending = excluded.oldest_pending,
           replication_cursor = excluded.replication_cursor,
           connected_agents = excluded.connected_agents,
           protocol_version = excluded.protocol_version,
           software_version = CASE WHEN excluded.software_version = '' THEN collector_runtimes.software_version ELSE excluded.software_version END,
           last_primary_seen = excluded.last_primary_seen,
           heartbeat_rtt_ms = excluded.heartbeat_rtt_ms,
           heartbeat_rtt_sampled_at = excluded.heartbeat_rtt_sampled_at,
           replication_rtt_ms = excluded.replication_rtt_ms,
           replication_rtt_sampled_at = excluded.replication_rtt_sampled_at{update_set},
           updated_at = CURRENT_TIMESTAMP"
    );
    let mut params: Vec<&dyn rusqlite::ToSql> = vec![
        &row.collector_uuid,
        &now_nanos,
        &spool_size,
        &pending_records,
        &row.oldest_pending,
        &replication_cursor,
        &connected_agents,
        &row.protocol_version,
        &row.software_version,
        &row.last_primary_seen,
        &row.heartbeat_rtt_ms,
        &row.heartbeat_rtt_sampled_at,
        &row.replication_rtt_ms,
        &row.replication_rtt_sampled_at,
    ];
    if include_last_sync {
        params.push(&now_nanos);
    }
    tx.execute(&sql, params.as_slice())?;
    Ok(())
}

/// 管理端列表：含已吊销、不含已删除。
pub fn list_admin(conn: &Connection) -> anyhow::Result<Vec<CollectorRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLUMNS} FROM collectors WHERE deleted = 0 ORDER BY created_at DESC, collector_uuid ASC"
    ))?;
    let rows = stmt
        .query_map([], row_to_collector)?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

/// 管理端按 UUID 查找：已吊销仍可见，已删除不可见。
pub fn find_admin(conn: &Connection, collector_uuid: &str) -> anyhow::Result<Option<CollectorRow>> {
    conn.query_row(
        &format!("SELECT {COLUMNS} FROM collectors WHERE collector_uuid = ? AND deleted = 0"),
        [collector_uuid],
        row_to_collector,
    )
    .optional()
    .map_err(Into::into)
}

pub fn list_scopes(
    conn: &Connection,
    collector_uuid: &str,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT scope_type, scope_value FROM collector_scopes WHERE collector_uuid = ? ORDER BY id ASC",
    )?;
    let rows = stmt
        .query_map([collector_uuid], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

#[derive(Debug, Clone, Default)]
pub struct CollectorRuntimeView {
    pub last_seen: i64,
    pub last_sync: i64,
    pub last_primary_seen: i64,
    pub spool_size: i64,
    pub pending_records: i64,
    pub oldest_pending: i64,
    pub replication_cursor: i64,
    pub connected_agents: i64,
    pub protocol_version: String,
    pub software_version: String,
    pub heartbeat_rtt_ms: f64,
    pub heartbeat_rtt_sampled_at: i64,
    pub replication_rtt_ms: f64,
    pub replication_rtt_sampled_at: i64,
}

pub fn runtime_view(
    conn: &Connection,
    collector_uuid: &str,
) -> anyhow::Result<CollectorRuntimeView> {
    conn.query_row(
        "SELECT COALESCE(last_seen, 0), COALESCE(last_sync, 0), COALESCE(last_primary_seen, 0), \
         COALESCE(spool_size, 0), COALESCE(pending_records, 0), COALESCE(oldest_pending, 0), \
         COALESCE(replication_cursor, 0), COALESCE(connected_agents, 0), \
         COALESCE(protocol_version, ''), COALESCE(software_version, ''), \
         COALESCE(heartbeat_rtt_ms, 0), COALESCE(heartbeat_rtt_sampled_at, 0), \
         COALESCE(replication_rtt_ms, 0), COALESCE(replication_rtt_sampled_at, 0) \
         FROM collector_runtimes WHERE collector_uuid = ?",
        [collector_uuid],
        |row| {
            Ok(CollectorRuntimeView {
                last_seen: row.get(0)?,
                last_sync: row.get(1)?,
                last_primary_seen: row.get(2)?,
                spool_size: row.get(3)?,
                pending_records: row.get(4)?,
                oldest_pending: row.get(5)?,
                replication_cursor: row.get(6)?,
                connected_agents: row.get(7)?,
                protocol_version: row.get(8)?,
                software_version: row.get(9)?,
                heartbeat_rtt_ms: row.get(10)?,
                heartbeat_rtt_sampled_at: row.get(11)?,
                replication_rtt_ms: row.get(12)?,
                replication_rtt_sampled_at: row.get(13)?,
            })
        },
    )
    .optional()
    .map(|row| row.unwrap_or_default())
    .map_err(Into::into)
}

pub fn registration_token_plain(
    conn: &Connection,
    collector_uuid: &str,
) -> anyhow::Result<Option<String>> {
    let cipher: Option<Vec<u8>> = conn
        .query_row(
            "SELECT token_ciphertext FROM collectors WHERE collector_uuid = ? AND deleted = 0",
            [collector_uuid],
            |row| row.get(0),
        )
        .optional()?;
    Ok(cipher.map(|bytes| crate::secret::decrypt_secret(&bytes).unwrap_or_default()))
}

#[derive(Debug, Clone)]
pub struct ScopeWrite {
    pub scope_type: String,
    pub scope_value: String,
}

/// 整块替换观测范围，并按范围开关 observer 指派。探测型关闭全部未结束指派。
pub fn replace_scopes(
    tx: &Transaction<'_>,
    collector: &CollectorRow,
    scopes: &[ScopeWrite],
    now_nanos: i64,
) -> anyhow::Result<()> {
    tx.execute(
        "DELETE FROM collector_scopes WHERE collector_uuid = ?",
        [&collector.collector_uuid],
    )?;
    for scope in scopes {
        tx.execute(
            "INSERT INTO collector_scopes (collector_uuid, scope_type, scope_value, created_at) \
             VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
            [
                &collector.collector_uuid,
                &scope.scope_type,
                &scope.scope_value,
            ],
        )?;
    }
    if collector.is_probe() {
        tx.execute(
            "UPDATE observer_assignments SET valid_to = ?1 WHERE observer_id = ?2 AND valid_to = 0",
            rusqlite::params![now_nanos, collector.collector_uuid],
        )?;
        return Ok(());
    }
    let bindings: Vec<(i64, Vec<u8>, String)> = {
        let mut stmt = tx.prepare(
            "SELECT b.server_id, b.node_uuid, COALESCE(s.tag, '') \
             FROM server_node_bindings b JOIN servers s ON s.id = b.server_id \
             WHERE b.current = 1 AND s.deleted_at IS NULL",
        )?;
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (server_id, node_uuid, tag) in bindings {
        let selected = scopes_match(scopes, server_id, &tag);
        let open: Option<i64> = tx
            .query_row(
                "SELECT id FROM observer_assignments WHERE node_uuid = ?1 AND observer_id = ?2 AND valid_to = 0",
                rusqlite::params![node_uuid, collector.collector_uuid],
                |row| row.get(0),
            )
            .optional()?;
        if selected && open.is_none() {
            tx.execute(
                "INSERT INTO observer_assignments (node_uuid, observer_id, valid_from, valid_to, config_version, generation, created_at) \
                 VALUES (?1, ?2, ?3, 0, ?4, ?5, CURRENT_TIMESTAMP)",
                rusqlite::params![
                    node_uuid,
                    collector.collector_uuid,
                    now_nanos,
                    collector.config_version as i64,
                    collector.generation as i64,
                ],
            )?;
        } else if !selected && open.is_some() {
            tx.execute(
                "UPDATE observer_assignments SET valid_to = ?1 WHERE node_uuid = ?2 AND observer_id = ?3 AND valid_to = 0",
                rusqlite::params![now_nanos, node_uuid, collector.collector_uuid],
            )?;
        }
    }
    Ok(())
}

fn scopes_match(scopes: &[ScopeWrite], server_id: i64, tag: &str) -> bool {
    scopes.iter().any(|scope| match scope.scope_type.as_str() {
        "all" => true,
        "server" => scope.scope_value == server_id.to_string(),
        "group" | "tag" => scope.scope_value == tag,
        _ => false,
    })
}

pub struct NewCollector {
    pub row: CollectorRow,
    pub token_plain: String,
    pub token_ciphertext: Vec<u8>,
    pub scopes: Vec<ScopeWrite>,
}

pub fn insert_collector(
    tx: &Transaction<'_>,
    draft: &NewCollector,
    now_nanos: i64,
) -> anyhow::Result<()> {
    tx.execute(
        "INSERT INTO collectors (collector_uuid, name, address, listen_port, token_hash, token_ciphertext, \
         generation, config_version, tls, insecure_tls, location, kind, probe_interval_sec, mtr_interval_sec, \
         mtr_probes, tcp_ports, enable_icmp, enable_tcp, enable_mtr, enable_ipv4, enable_ipv6, probe_notify, \
         notification_tag, latency_notify, min_latency_ms, max_latency_ms, fail_threshold, route_interval_sec, \
         route_keep, revoked, deleted, created_at, updated_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        rusqlite::params![
            draft.row.collector_uuid,
            draft.row.name,
            draft.row.address,
            draft.row.listen_port,
            draft.row.token_hash,
            draft.token_ciphertext,
            draft.row.generation as i64,
            draft.row.config_version as i64,
            draft.row.tls as i64,
            draft.row.insecure_tls as i64,
            draft.row.location.clone().unwrap_or_default(),
            draft.row.kind,
            draft.row.probe_interval_sec,
            draft.row.mtr_interval_sec,
            draft.row.mtr_probes,
            draft.row.tcp_ports,
            draft.row.enable_icmp as i64,
            draft.row.enable_tcp as i64,
            draft.row.enable_mtr as i64,
            draft.row.enable_ipv4 as i64,
            draft.row.enable_ipv6 as i64,
            draft.row.probe_notify as i64,
            draft.row.notification_tag.clone().unwrap_or_default(),
            draft.row.latency_notify as i64,
            draft.row.min_latency_ms,
            draft.row.max_latency_ms,
            draft.row.fail_threshold,
            draft.row.route_interval_sec,
            draft.row.route_keep,
        ],
    )?;
    replace_scopes(tx, &draft.row, &draft.scopes, now_nanos)
}

pub fn save_collector(
    tx: &Transaction<'_>,
    row: &CollectorRow,
    scopes: &[ScopeWrite],
    now_nanos: i64,
) -> anyhow::Result<()> {
    let changed = tx.execute(
        "UPDATE collectors SET name=?1, address=?2, listen_port=?3, tls=?4, insecure_tls=?5, location=?6, \
         probe_interval_sec=?7, mtr_interval_sec=?8, mtr_probes=?9, tcp_ports=?10, enable_icmp=?11, enable_tcp=?12, \
         enable_mtr=?13, enable_ipv4=?14, enable_ipv6=?15, probe_notify=?16, notification_tag=?17, latency_notify=?18, \
         min_latency_ms=?19, max_latency_ms=?20, fail_threshold=?21, route_interval_sec=?22, route_keep=?23, \
         config_version=?24, updated_at=CURRENT_TIMESTAMP \
         WHERE collector_uuid=?25 AND deleted=0",
        rusqlite::params![
            row.name,
            row.address,
            row.listen_port,
            row.tls as i64,
            row.insecure_tls as i64,
            row.location.clone().unwrap_or_default(),
            row.probe_interval_sec,
            row.mtr_interval_sec,
            row.mtr_probes,
            row.tcp_ports,
            row.enable_icmp as i64,
            row.enable_tcp as i64,
            row.enable_mtr as i64,
            row.enable_ipv4 as i64,
            row.enable_ipv6 as i64,
            row.probe_notify as i64,
            row.notification_tag.clone().unwrap_or_default(),
            row.latency_notify as i64,
            row.min_latency_ms,
            row.max_latency_ms,
            row.fail_threshold,
            row.route_interval_sec,
            row.route_keep,
            row.config_version as i64,
            row.collector_uuid,
        ],
    )?;
    if changed == 0 {
        anyhow::bail!("collector not found");
    }
    replace_scopes(tx, row, scopes, now_nanos)
}

pub fn disable_collector(
    tx: &Transaction<'_>,
    collector_uuid: &str,
    deleted: bool,
    now_nanos: i64,
) -> anyhow::Result<()> {
    let current: Option<(i64,)> = tx
        .query_row(
            "SELECT config_version FROM collectors WHERE collector_uuid = ? AND deleted = 0",
            [collector_uuid],
            |row| Ok((row.get(0)?,)),
        )
        .optional()?;
    let Some((version,)) = current else {
        anyhow::bail!("collector not found");
    };
    tx.execute(
        "UPDATE collectors SET revoked = 1, deleted = ?1, config_version = ?2, updated_at = CURRENT_TIMESTAMP \
         WHERE collector_uuid = ?3",
        rusqlite::params![deleted as i64, version + 1, collector_uuid],
    )?;
    tx.execute(
        "UPDATE observer_assignments SET valid_to = ?1 WHERE observer_id = ?2 AND valid_to = 0",
        rusqlite::params![now_nanos, collector_uuid],
    )?;
    Ok(())
}

pub fn rotate_registration_token(
    tx: &Transaction<'_>,
    collector_uuid: &str,
    plain: &str,
    hash: &[u8],
    ciphertext: &[u8],
) -> anyhow::Result<u64> {
    let current: Option<i64> = tx
        .query_row(
            "SELECT config_version FROM collectors WHERE collector_uuid = ? AND deleted = 0",
            [collector_uuid],
            |row| row.get(0),
        )
        .optional()?;
    let Some(version) = current else {
        anyhow::bail!("collector not found");
    };
    let next = version + 1;
    tx.execute(
        "UPDATE collectors SET token_hash = ?1, token_ciphertext = ?2, revoked = 0, config_version = ?3, \
         updated_at = CURRENT_TIMESTAMP WHERE collector_uuid = ?4",
        rusqlite::params![hash, ciphertext, next, collector_uuid],
    )?;
    let _ = plain;
    Ok(next as u64)
}

pub fn next_config_version(tx: &Transaction<'_>) -> anyhow::Result<u64> {
    let max: i64 = tx.query_row(
        "SELECT COALESCE(MAX(config_version), 0) FROM collectors",
        [],
        |row| row.get(0),
    )?;
    Ok((max + 1).max(1) as u64)
}

/// `bumpCollectorConfigVersion`：版本 +1 并返回新值。
pub fn bump_config_version(tx: &Transaction<'_>, collector_uuid: &str) -> anyhow::Result<u64> {
    let current: u64 = tx
        .query_row(
            "SELECT config_version FROM collectors WHERE collector_uuid = ?",
            [collector_uuid],
            |row| row.get(0),
        )
        .optional()?
        .ok_or_else(|| anyhow::anyhow!("collector {collector_uuid} not found"))?;
    let next = current + 1;
    tx.execute(
        "UPDATE collectors SET config_version = ?, updated_at = CURRENT_TIMESTAMP WHERE collector_uuid = ?",
        [&next.to_string(), collector_uuid],
    )?;
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;
    use crate::telemetry::credential;

    fn temp_db(tag: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "santaizi-repo-collectors-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn token_and_uuid_lookup() {
        let path = temp_db("lookup");
        let db = migrate::open_primary(&path).unwrap();
        let (token, hash) = credential::new_registration_token().unwrap();
        db.write(|tx| {
            tx.execute(
                "INSERT INTO collectors (collector_uuid, name, address, token_hash, token_ciphertext, kind, tcp_ports, revoked, deleted) VALUES ('c1', 'n', 'addr', ?, X'00', 'observer', '', 0, 0)",
                [&hash.to_vec()],
            )?;
            Ok(())
        })
        .unwrap();

        let found = db
            .read(|conn| find_by_token(conn, &token))
            .unwrap()
            .unwrap();
        assert_eq!(found.collector_uuid, "c1");
        assert_eq!(found.config_version, 1);
        assert!(!found.is_probe());
        assert!(
            db.read(|conn| find_by_token(conn, "stc_wrong"))
                .unwrap()
                .is_none()
        );
        assert!(db.read(|conn| find_by_token(conn, "")).unwrap().is_none());

        let by_uuid = db.read(|conn| find_by_uuid(conn, "c1")).unwrap().unwrap();
        assert_eq!(by_uuid.collector_uuid, "c1");
        assert!(
            db.read(|conn| find_by_uuid(conn, "missing"))
                .unwrap()
                .is_none()
        );
        assert_eq!(db.read(list_active).unwrap().len(), 1);

        let bumped = db.write(|tx| bump_config_version(tx, "c1")).unwrap();
        assert_eq!(bumped, 2);
        assert!(db.write(|tx| bump_config_version(tx, "nope")).is_err());
        let _ = std::fs::remove_file(&path);
    }
}
