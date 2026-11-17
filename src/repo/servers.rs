//! 服务器表访问与 `client_secret → server_id` 内存索引。
//!
//! Go 版在启动时把所有服务器密钥解密进 `SecretToID` 映射（`service/singleton/server.go`）；
//! Rust 版同样只在进程内持有明文索引，密钥文件永不落库。

use std::collections::HashMap;
use std::sync::RwLock;

use rusqlite::{Connection, OptionalExtension, Transaction};

use crate::db::Db;
use crate::secret;

/// 进程内 `client_secret → server_id` 索引；服务器增删改后需 `rebuild`。
#[derive(Debug, Default)]
pub struct SecretIndex {
    inner: RwLock<HashMap<String, i64>>,
}

impl SecretIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn rebuild(&self, db: &Db) -> anyhow::Result<()> {
        let secrets = db.read(load_secrets)?;
        *self.inner.write().unwrap() = secrets;
        Ok(())
    }

    pub fn server_id(&self, secret: &str) -> Option<i64> {
        self.inner.read().unwrap().get(secret).copied()
    }

    /// 建号/重置密钥后热更新：替换该服务器的明文项（Go `SecretToID` 写侧）。
    pub fn rebind(&self, server_id: i64, secret: &str) {
        let mut guard = self.inner.write().unwrap();
        guard.retain(|_, &mut id| id != server_id);
        guard.insert(secret.to_string(), server_id);
    }

    /// 删除服务器后移除其明文项。
    pub fn forget_server(&self, server_id: i64) {
        self.inner
            .write()
            .unwrap()
            .retain(|_, &mut id| id != server_id);
    }
}

fn load_secrets(conn: &Connection) -> anyhow::Result<HashMap<String, i64>> {
    let mut stmt =
        conn.prepare("SELECT id, secret_ciphertext FROM servers WHERE deleted_at IS NULL")?;
    let rows = stmt.query_map([], |row| {
        let id: i64 = row.get(0)?;
        let ciphertext: Vec<u8> = row.get(1)?;
        Ok((id, ciphertext))
    })?;
    let mut index = HashMap::new();
    for row in rows {
        let (id, ciphertext) = row?;
        if let Ok(secret) = secret::decrypt_secret(&ciphertext)
            && !secret.is_empty()
        {
            index.insert(secret, id);
        }
    }
    Ok(index)
}

/// 单次查询版本：供索引未建时的兜底（解密全表代价高，仅测试/小规模用）。
pub fn server_id_by_secret(conn: &Connection, secret: &str) -> anyhow::Result<Option<i64>> {
    let mut stmt =
        conn.prepare("SELECT id, secret_ciphertext FROM servers WHERE deleted_at IS NULL")?;
    let rows = stmt.query_map([], |row| {
        let id: i64 = row.get(0)?;
        let ciphertext: Vec<u8> = row.get(1)?;
        Ok((id, ciphertext))
    })?;
    for row in rows {
        let (id, ciphertext) = row?;
        if secret::decrypt_secret(&ciphertext)
            .map(|s| s == secret)
            .unwrap_or(false)
        {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

pub struct ServerRow {
    pub id: i64,
    pub name: String,
    pub tag: String,
}

pub fn server_by_id(conn: &Connection, id: i64) -> anyhow::Result<Option<ServerRow>> {
    conn.query_row(
        "SELECT id, COALESCE(name, ''), COALESCE(tag, '') FROM servers WHERE id = ? AND deleted_at IS NULL",
        [id],
        |row| {
            Ok(ServerRow { id: row.get(0)?, name: row.get(1)?, tag: row.get(2)? })
        },
    )
    .optional()
    .map_err(Into::into)
}

/// 公开/管理列表行：`servers` 左连 `server_runtimes`，再带一个「最新已封窗
/// 可用性桶」的相关子查询（Go `runtimeForServer` 的 `LatestClosedAvailabilityBucket`）。
/// 一次查询出整页，避免 N+1。
pub struct PublicServerRow {
    pub id: i64,
    pub name: String,
    pub tag: String,
    pub note: String,
    pub public_note: String,
    pub display_index: i64,
    pub hide_for_guest: bool,
    pub monitoring_options_raw: String,
    // 运行态（无 runtime 行时全为 None）。
    pub protocol: Option<String>,
    pub node_uuid: Option<Vec<u8>>,
    pub runtime_host_state: Option<String>,
    pub runtime_connectivity_state: Option<String>,
    pub last_collected_at: Option<i64>,
    pub last_received_at: Option<i64>,
    pub state_payload: Option<Vec<u8>>,
    pub host_payload: Option<Vec<u8>>,
    // 最新已封窗可用性桶（V2 共识的唯一信源）。
    pub bucket_host_state: Option<String>,
    pub bucket_connectivity_state: Option<String>,
    pub bucket_seen_observers: Option<i64>,
    pub bucket_expected_observers: Option<i64>,
}

/// 按 Go `SortedServerList` 顺序（display_index DESC, id ASC）取服务器快照。
/// `include_hidden` 为 false 时过滤 `hide_for_guest`（Go 的 ForGuest 列表）。
pub fn list_public(
    conn: &Connection,
    include_hidden: bool,
    now_nanos: i64,
) -> anyhow::Result<Vec<PublicServerRow>> {
    // 可用性子查询：window_end <= now 的最新桶（进行中的桶不是共识）。
    const BUCKET_HOST: &str = "(SELECT b.host_state FROM availability_buckets b \
         WHERE b.node_uuid = r.current_node_uuid AND b.window_end <= ?1 \
         ORDER BY b.window_end DESC, b.bucket_start DESC LIMIT 1)";
    const BUCKET_CONNECTIVITY: &str = "(SELECT b.connectivity_state FROM availability_buckets b \
         WHERE b.node_uuid = r.current_node_uuid AND b.window_end <= ?1 \
         ORDER BY b.window_end DESC, b.bucket_start DESC LIMIT 1)";
    const BUCKET_SEEN: &str = "(SELECT b.seen_observers FROM availability_buckets b \
         WHERE b.node_uuid = r.current_node_uuid AND b.window_end <= ?1 \
         ORDER BY b.window_end DESC, b.bucket_start DESC LIMIT 1)";
    const BUCKET_EXPECTED: &str = "(SELECT b.expected_observers FROM availability_buckets b \
         WHERE b.node_uuid = r.current_node_uuid AND b.window_end <= ?1 \
         ORDER BY b.window_end DESC, b.bucket_start DESC LIMIT 1)";
    let sql = format!(
        "SELECT s.id, COALESCE(s.name, ''), COALESCE(s.tag, ''), COALESCE(s.note, ''), \
         COALESCE(s.public_note, ''), COALESCE(s.display_index, 0), COALESCE(s.hide_for_guest, 0), \
         COALESCE(s.monitoring_options_raw, '{{}}'), \
         r.protocol, r.current_node_uuid, r.host_state, r.connectivity_state, \
         r.last_collected_at, r.last_received_at, r.state_payload, r.host_payload, \
         {BUCKET_HOST}, {BUCKET_CONNECTIVITY}, {BUCKET_SEEN}, {BUCKET_EXPECTED} \
         FROM servers s \
         LEFT JOIN server_runtimes r ON r.server_id = s.id \
         WHERE s.deleted_at IS NULL AND (?2 <> 0 OR COALESCE(s.hide_for_guest, 0) = 0) \
         ORDER BY s.display_index DESC, s.id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params![now_nanos, include_hidden as i64], |row| {
        Ok(PublicServerRow {
            id: row.get(0)?,
            name: row.get(1)?,
            tag: row.get(2)?,
            note: row.get(3)?,
            public_note: row.get(4)?,
            display_index: row.get(5)?,
            hide_for_guest: row.get::<_, i64>(6)? != 0,
            monitoring_options_raw: row.get(7)?,
            protocol: row.get(8)?,
            node_uuid: row.get(9)?,
            runtime_host_state: row.get(10)?,
            runtime_connectivity_state: row.get(11)?,
            last_collected_at: row.get(12)?,
            last_received_at: row.get(13)?,
            state_payload: row.get(14)?,
            host_payload: row.get(15)?,
            bucket_host_state: row.get(16)?,
            bucket_connectivity_state: row.get(17)?,
            bucket_seen_observers: row.get(18)?,
            bucket_expected_observers: row.get(19)?,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

/// admin 端行：公开行全部字段 + 管理侧专有列（密文与探测开关原值）。
pub struct AdminServerRow {
    pub public: PublicServerRow,
    pub secret_ciphertext: Vec<u8>,
    pub probe_target: String,
    pub probe_tcp_ports: String,
    /// `*bool` 语义：`None` = 从未显式设置（DTO 按 BoolOrTrue 展示为 true）。
    pub probe_enable_icmp: Option<i64>,
    pub probe_enable_tcp: Option<i64>,
    pub probe_enable_mtr: Option<i64>,
}

/// 服务器可编辑字段（Go `applyServerWriteFields` 落库前的最终值；
/// 探测开关已按 boolOrKeep 解析为 0/1）。
pub struct ServerWriteFields {
    pub name: String,
    pub tag: String,
    pub note: String,
    pub public_note_raw: String,
    pub monitoring_options_raw: String,
    pub display_index: i64,
    pub hide_for_guest: bool,
    pub probe_target: String,
    pub probe_tcp_ports: String,
    pub probe_enable_icmp: bool,
    pub probe_enable_tcp: bool,
    pub probe_enable_mtr: bool,
}

/// 18 位随机字母数字密钥（Go `utils.GenerateRandomString(18)`）。
pub fn new_client_secret() -> String {
    use rand::distr::{Alphanumeric, SampleString as _};
    Alphanumeric.sample_string(&mut rand::rng(), 18)
}

/// admin 列表/详情共用的列清单（含可用性桶子查询；`?1` 为当前时刻）。
const ADMIN_SELECT: &str = "s.id, COALESCE(s.name, ''), COALESCE(s.tag, ''), COALESCE(s.note, ''), \
     COALESCE(s.public_note, ''), COALESCE(s.display_index, 0), COALESCE(s.hide_for_guest, 0), \
     COALESCE(s.monitoring_options_raw, '{}'), \
     r.protocol, r.current_node_uuid, r.host_state, r.connectivity_state, \
     r.last_collected_at, r.last_received_at, r.state_payload, r.host_payload, \
     (SELECT b.host_state FROM availability_buckets b \
         WHERE b.node_uuid = r.current_node_uuid AND b.window_end <= ?1 \
         ORDER BY b.window_end DESC, b.bucket_start DESC LIMIT 1), \
     (SELECT b.connectivity_state FROM availability_buckets b \
         WHERE b.node_uuid = r.current_node_uuid AND b.window_end <= ?1 \
         ORDER BY b.window_end DESC, b.bucket_start DESC LIMIT 1), \
     (SELECT b.seen_observers FROM availability_buckets b \
         WHERE b.node_uuid = r.current_node_uuid AND b.window_end <= ?1 \
         ORDER BY b.window_end DESC, b.bucket_start DESC LIMIT 1), \
     (SELECT b.expected_observers FROM availability_buckets b \
         WHERE b.node_uuid = r.current_node_uuid AND b.window_end <= ?1 \
         ORDER BY b.window_end DESC, b.bucket_start DESC LIMIT 1), \
     s.secret_ciphertext, COALESCE(s.probe_target, ''), COALESCE(s.probe_tcp_ports, ''), \
     s.probe_enable_icmp, s.probe_enable_tcp, s.probe_enable_mtr";

fn admin_row_from(row: &rusqlite::Row<'_>) -> rusqlite::Result<AdminServerRow> {
    Ok(AdminServerRow {
        public: PublicServerRow {
            id: row.get(0)?,
            name: row.get(1)?,
            tag: row.get(2)?,
            note: row.get(3)?,
            public_note: row.get(4)?,
            display_index: row.get(5)?,
            hide_for_guest: row.get::<_, i64>(6)? != 0,
            monitoring_options_raw: row.get(7)?,
            protocol: row.get(8)?,
            node_uuid: row.get(9)?,
            runtime_host_state: row.get(10)?,
            runtime_connectivity_state: row.get(11)?,
            last_collected_at: row.get(12)?,
            last_received_at: row.get(13)?,
            state_payload: row.get(14)?,
            host_payload: row.get(15)?,
            bucket_host_state: row.get(16)?,
            bucket_connectivity_state: row.get(17)?,
            bucket_seen_observers: row.get(18)?,
            bucket_expected_observers: row.get(19)?,
        },
        secret_ciphertext: row.get(20)?,
        probe_target: row.get(21)?,
        probe_tcp_ports: row.get(22)?,
        probe_enable_icmp: row.get(23)?,
        probe_enable_tcp: row.get(24)?,
        probe_enable_mtr: row.get(25)?,
    })
}

/// admin 服务器列表分组过滤条件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminGroupFilter {
    /// 未分组（`COALESCE(s.tag, '') = ''`）。
    Ungrouped,
    /// 指定分组名（精确匹配 `COALESCE(s.tag, '')`）。
    Named(String),
}

/// admin 服务器列表在线状态过滤条件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminStatusFilter {
    Online,
    Offline,
}

/// admin 服务器列表（Go `v2AdminServers`）：q 模糊匹配 name/tag/note，
/// 排序白名单 `id|name|tag|display_index|last_active`（其余回落 display_index
/// DESC），同序键以 `id ASC` 收尾。返回整行 + 总数。
pub fn list_admin(
    conn: &Connection,
    q: &str,
    sort: &str,
    ascending: bool,
    offset: i64,
    limit: i64,
    now_nanos: i64,
) -> anyhow::Result<(Vec<AdminServerRow>, i64)> {
    list_admin_filtered(
        conn, q, None, None, 0, sort, ascending, offset, limit, now_nanos,
    )
}

/// 支持可选分组与在线状态筛选的 admin 服务器列表查询。
#[allow(clippy::too_many_arguments)]
pub fn list_admin_filtered(
    conn: &Connection,
    q: &str,
    group: Option<&AdminGroupFilter>,
    status: Option<AdminStatusFilter>,
    offline_threshold_seconds: u64,
    sort: &str,
    ascending: bool,
    offset: i64,
    limit: i64,
    now_nanos: i64,
) -> anyhow::Result<(Vec<AdminServerRow>, i64)> {
    let sort_column = match sort {
        "id" => "s.id",
        "name" => "s.name",
        "tag" => "s.tag",
        "display_index" => "COALESCE(s.display_index, 0)",
        "last_active" => "COALESCE(r.last_received_at, 0)",
        _ => "COALESCE(s.display_index, 0)",
    };
    let direction = if ascending { "ASC" } else { "DESC" };
    // `q` 是用户输入：`%`/`_` 必须转义，否则「100%」会匹配所有行。
    // COUNT 与 SELECT 用同一个转义值，避免计数与行数不一致。
    let like = format!("%{}%", crate::util::text::like_contains(q));
    let (group_mode, group_name): (i64, &str) = match group {
        None => (0, ""),
        Some(AdminGroupFilter::Ungrouped) => (1, ""),
        Some(AdminGroupFilter::Named(name)) => (2, name.as_str()),
    };
    let status_mode: i64 = match status {
        None => 0,
        Some(AdminStatusFilter::Online) => 1,
        Some(AdminStatusFilter::Offline) => 2,
    };
    let threshold_nanos = (offline_threshold_seconds as i128) * 1_000_000_000;
    let online_cutoff: i64 =
        (now_nanos as i128 - threshold_nanos).clamp(0, i64::MAX as i128) as i64;

    let filter_clause = "s.deleted_at IS NULL \
        AND (?2 = '' OR s.name LIKE ?3 ESCAPE '\\' OR s.tag LIKE ?3 ESCAPE '\\' OR s.note LIKE ?3 ESCAPE '\\') \
        AND (?4 = 0 OR (?4 = 1 AND COALESCE(s.tag, '') = '') OR (?4 = 2 AND COALESCE(s.tag, '') = ?5)) \
        AND (?6 = 0 OR (?6 = 1 AND COALESCE(r.last_received_at, 0) > ?7) OR (?6 = 2 AND COALESCE(r.last_received_at, 0) <= ?7))";

    let count_sql = format!(
        "SELECT COUNT(*) FROM servers s \
         LEFT JOIN server_runtimes r ON r.server_id = s.id \
         WHERE {filter_clause}"
    );
    let total: i64 = conn.query_row(
        &count_sql,
        rusqlite::params![
            now_nanos,
            q,
            like,
            group_mode,
            group_name,
            status_mode,
            online_cutoff
        ],
        |row| row.get(0),
    )?;
    let sql = format!(
        "SELECT {ADMIN_SELECT} FROM servers s \
         LEFT JOIN server_runtimes r ON r.server_id = s.id \
         WHERE {filter_clause} \
         ORDER BY {sort_column} {direction}, s.id ASC LIMIT ?8 OFFSET ?9"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(
        rusqlite::params![
            now_nanos,
            q,
            like,
            group_mode,
            group_name,
            status_mode,
            online_cutoff,
            limit,
            offset
        ],
        admin_row_from,
    )?;
    let mut items = Vec::new();
    for row in rows {
        items.push(row?);
    }
    Ok((items, total))
}

/// admin 单查（Go `v2AdminServer`）。
pub fn admin_server_by_id(
    conn: &Connection,
    id: i64,
    now_nanos: i64,
) -> anyhow::Result<Option<AdminServerRow>> {
    let sql = format!(
        "SELECT {ADMIN_SELECT} FROM servers s \
         LEFT JOIN server_runtimes r ON r.server_id = s.id \
         WHERE s.id = ?2 AND s.deleted_at IS NULL"
    );
    conn.query_row(&sql, rusqlite::params![now_nanos, id], admin_row_from)
        .optional()
        .map_err(Into::into)
}

/// 建号（Go `v2SaveServer` created 分支）：写完整行，返回新 ID。
pub fn insert_admin_server(
    tx: &Transaction<'_>,
    secret_ciphertext: &[u8],
    fields: &ServerWriteFields,
) -> anyhow::Result<i64> {
    tx.execute(
        "INSERT INTO servers (name, tag, secret_ciphertext, note, public_note, display_index, \
         hide_for_guest, monitoring_options_raw, probe_target, probe_tcp_ports, \
         probe_enable_icmp, probe_enable_tcp, probe_enable_mtr) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        rusqlite::params![
            fields.name,
            fields.tag,
            secret_ciphertext,
            fields.note,
            fields.public_note_raw,
            fields.display_index,
            fields.hide_for_guest as i64,
            fields.monitoring_options_raw,
            fields.probe_target,
            fields.probe_tcp_ports,
            fields.probe_enable_icmp as i64,
            fields.probe_enable_tcp as i64,
            fields.probe_enable_mtr as i64,
        ],
    )?;
    Ok(tx.last_insert_rowid())
}

/// 全量更新可编辑字段（Go `tx.Save(&server)` 的字段子集）。
pub fn update_admin_server(
    tx: &Transaction<'_>,
    id: i64,
    fields: &ServerWriteFields,
) -> anyhow::Result<usize> {
    Ok(tx.execute(
        "UPDATE servers SET name = ?1, tag = ?2, note = ?3, public_note = ?4, display_index = ?5, \
         hide_for_guest = ?6, monitoring_options_raw = ?7, probe_target = ?8, probe_tcp_ports = ?9, \
         probe_enable_icmp = ?10, probe_enable_tcp = ?11, probe_enable_mtr = ?12, \
         updated_at = CURRENT_TIMESTAMP \
         WHERE id = ?13 AND deleted_at IS NULL",
        rusqlite::params![
            fields.name,
            fields.tag,
            fields.note,
            fields.public_note_raw,
            fields.display_index,
            fields.hide_for_guest as i64,
            fields.monitoring_options_raw,
            fields.probe_target,
            fields.probe_tcp_ports,
            fields.probe_enable_icmp as i64,
            fields.probe_enable_tcp as i64,
            fields.probe_enable_mtr as i64,
            id,
        ],
    )?)
}

/// 硬删服务器及其所有关联数据。
///
/// Go `onServerDelete` 还会 `EndServerNodeBinding` + `OnDeleteServer` + 删
/// `Transfer` / `ServerRuntime` / `ServerOfflineHistory`；Rust 侧原先只删
/// `servers` + `monitor_histories`，留下一堆引用已删 server_id 的孤儿行
/// （重新占用同一 id 时会读到上一任的传输量与运行态）。
///
/// `server_node_bindings` 只终结未来 assignment（`current=0` + `valid_to`），
/// **不删行**：历史观测与可用性证据必须保持不可变（Go 注释同义）。
/// `availability_incidents` / `incident_revisions` 同样保留。
pub fn delete_server(tx: &Transaction<'_>, id: i64) -> anyhow::Result<usize> {
    let deleted = tx.execute("DELETE FROM servers WHERE id = ?1", [id])?;
    tx.execute("DELETE FROM monitor_histories WHERE server_id = ?1", [id])?;
    tx.execute("DELETE FROM transfers WHERE server_id = ?1", [id])?;
    tx.execute("DELETE FROM server_runtimes WHERE server_id = ?1", [id])?;
    tx.execute(
        "DELETE FROM server_offline_histories WHERE server_id = ?1",
        [id],
    )?;
    tx.execute(
        "DELETE FROM traffic_policy_states WHERE policy_id IN \
         (SELECT id FROM traffic_policies WHERE server_id = ?1)",
        [id],
    )?;
    tx.execute("DELETE FROM traffic_policies WHERE server_id = ?1", [id])?;
    tx.execute("DELETE FROM probe_latests WHERE server_id = ?1", [id])?;
    tx.execute("DELETE FROM probe_traces WHERE server_id = ?1", [id])?;
    tx.execute("DELETE FROM probe_routes WHERE server_id = ?1", [id])?;
    tx.execute("DELETE FROM probe_route_jobs WHERE server_id = ?1", [id])?;
    tx.execute(
        "DELETE FROM probe_sample_buckets WHERE server_id = ?1",
        [id],
    )?;
    tx.execute("DELETE FROM probe_alert_states WHERE server_id = ?1", [id])?;
    let id_text = id.to_string();
    tx.execute(
        "UPDATE collectors SET config_version = config_version + 1, updated_at = CURRENT_TIMESTAMP \
         WHERE collector_uuid IN (SELECT collector_uuid FROM collector_scopes \
         WHERE scope_type = 'server' AND scope_value = ?1)",
        [&id_text],
    )?;
    tx.execute(
        "DELETE FROM collector_scopes WHERE scope_type = 'server' AND scope_value = ?1",
        [&id_text],
    )?;
    tx.execute(
        "DELETE FROM observer_assignments WHERE node_uuid IN \
         (SELECT node_uuid FROM server_node_bindings WHERE server_id = ?1)",
        [id],
    )?;
    tx.execute(
        "UPDATE server_node_bindings SET current = 0, valid_to = COALESCE(valid_to, \
         CAST(strftime('%s', 'now') AS INTEGER) * 1000000000) \
         WHERE server_id = ?1 AND current = 1",
        [id],
    )?;
    Ok(deleted)
}

/// 重置可用性（Go `ResetServerAvailability`）：硬删离线历史并复位运行态；
/// 上报过的运行态把 `last_seen_at` 前移到重置时刻并置 online。
pub fn reset_availability(
    tx: &Transaction<'_>,
    server_id: i64,
    now_nanos: i64,
) -> anyhow::Result<i64> {
    tx.execute(
        "DELETE FROM server_offline_histories WHERE server_id = ?1",
        [server_id],
    )?;
    let deleted = tx.changes() as i64;
    let last_seen: Option<String> = tx
        .query_row(
            "SELECT last_seen_at FROM server_runtimes WHERE server_id = ?1",
            [server_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(last_seen) = last_seen else {
        return Ok(deleted);
    };
    let status = if last_seen.is_empty() {
        "unknown"
    } else {
        "online"
    };
    let last_seen_value: Option<String> = if last_seen.is_empty() {
        None
    } else {
        Some(crate::util::time::unix_nanos_to_sqlite(now_nanos))
    };
    tx.execute(
        "UPDATE server_runtimes SET current_offline_id = 0, status = ?2, last_seen_at = ?3 \
         WHERE server_id = ?1",
        rusqlite::params![server_id, status, last_seen_value],
    )?;
    Ok(deleted)
}

/// 分组清单（Go `v2ListServerGroups`）：tag 计数，空组名排最后。
pub fn list_groups(conn: &Connection) -> anyhow::Result<Vec<(String, i64)>> {
    let mut stmt = conn.prepare(
        "SELECT COALESCE(tag, ''), COUNT(*) FROM servers WHERE deleted_at IS NULL \
         GROUP BY COALESCE(tag, '') ORDER BY (COALESCE(tag, '') = '') ASC, 1 ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;

    fn temp_db(tag: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "santaizi-repo-servers-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn secret_index_maps_secret_to_server() {
        let path = temp_db("index");
        let db = migrate::open_primary(&path).unwrap();
        secret::configure_secret_encryption(&[5u8; 32]).unwrap();
        db.write(|tx| {
            let ciphertext = secret::encrypt_secret("secret-a").unwrap();
            tx.execute(
                "INSERT INTO servers (name, tag, secret_ciphertext, display_index, hide_for_guest) VALUES ('a', 'tag1', ?, 1, 0)",
                [&ciphertext],
            )?;
            let ciphertext = secret::encrypt_secret("secret-b").unwrap();
            tx.execute(
                "INSERT INTO servers (name, tag, secret_ciphertext, display_index, hide_for_guest) VALUES ('b', 'tag2', ?, 2, 0)",
                [&ciphertext],
            )?;
            Ok(())
        })
        .unwrap();

        let index = SecretIndex::new();
        index.rebuild(&db).unwrap();
        assert_eq!(index.server_id("secret-a"), Some(1));
        assert_eq!(index.server_id("secret-b"), Some(2));
        assert_eq!(index.server_id("nope"), None);

        assert_eq!(
            db.read(|conn| server_id_by_secret(conn, "secret-b"))
                .unwrap(),
            Some(2)
        );
        let row = db.read(|conn| server_by_id(conn, 1)).unwrap().unwrap();
        assert_eq!(row.name, "a");
        assert_eq!(row.tag, "tag1");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn delete_server_returns_server_row_count_and_purges_orphan_tables() {
        let path = temp_db("delete-orphans");
        let db = migrate::open_primary(&path).unwrap();
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) \
                 VALUES (42, 'victim', 'prod', X'11', 1, 0)",
                [],
            )?;
            // 3 条 monitor_histories：验证返回值是 servers 受影响行数 (1) 而非 3。
            for i in 1..=3 {
                tx.execute(
                    "INSERT INTO monitor_histories (id, monitor_id, server_id, avg_delay, up, down) \
                     VALUES (?1, 1, 42, 1.0, 1, 0)",
                    [i],
                )?;
            }
            tx.execute(
                "INSERT INTO traffic_policies (id, server_id, name, direction, mode, quota_bytes, warning_percent, enabled) \
                 VALUES (100, 42, 'p', 'total', 'cumulative', 1024, 80, 1)",
                [],
            )?;
            tx.execute(
                "INSERT INTO traffic_policy_states (policy_id, window_start, window_end, used_bytes, last_evaluated_at) \
                 VALUES (100, 1, 2, 10, 2)",
                [],
            )?;
            tx.execute(
                "INSERT INTO collectors (collector_uuid, name, address, token_hash, token_ciphertext, tcp_ports, revoked, deleted) \
                 VALUES ('col-1', 'c1', '1.2.3.4:5555', X'01', X'02', '22,443', 0, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO collector_scopes (collector_uuid, scope_type, scope_value) VALUES ('col-1', 'server', '42')",
                [],
            )?;
            tx.execute(
                "INSERT INTO probe_latests (collector_uuid, server_id, sampled_at, reachable) VALUES ('col-1', 42, 10, 1)",
                [],
            )?;
            tx.execute(
                "INSERT INTO probe_traces (collector_uuid, server_id, sampled_at) VALUES ('col-1', 42, 10)",
                [],
            )?;
            tx.execute(
                "INSERT INTO probe_routes (collector_uuid, server_id, protocol, sampled_at) VALUES ('col-1', 42, 'icmp', 10)",
                [],
            )?;
            tx.execute(
                "INSERT INTO probe_route_jobs (collector_uuid, server_id, protocol, status, requested_at) VALUES ('col-1', 42, 'icmp', 'pending', 10)",
                [],
            )?;
            tx.execute(
                "INSERT INTO probe_sample_buckets (collector_uuid, server_id, kind, port, bucket_start, count) VALUES ('col-1', 42, 'icmp', 0, 10, 1)",
                [],
            )?;
            tx.execute(
                "INSERT INTO probe_alert_states (collector_uuid, server_id, consecutive_fails) VALUES ('col-1', 42, 3)",
                [],
            )?;

            let removed = delete_server(tx, 42)?;
            assert_eq!(removed, 1);
            assert_eq!(delete_server(tx, 42)?, 0);

            for sql in [
                "SELECT COUNT(*) FROM traffic_policies WHERE server_id = 42",
                "SELECT COUNT(*) FROM traffic_policy_states WHERE policy_id = 100",
                "SELECT COUNT(*) FROM probe_latests WHERE server_id = 42",
                "SELECT COUNT(*) FROM probe_traces WHERE server_id = 42",
                "SELECT COUNT(*) FROM probe_routes WHERE server_id = 42",
                "SELECT COUNT(*) FROM probe_route_jobs WHERE server_id = 42",
                "SELECT COUNT(*) FROM probe_sample_buckets WHERE server_id = 42",
                "SELECT COUNT(*) FROM probe_alert_states WHERE server_id = 42",
                "SELECT COUNT(*) FROM collector_scopes WHERE scope_type = 'server' AND scope_value = '42'",
            ] {
                let c: i64 = tx.query_row(sql, [], |r| r.get(0))?;
                assert_eq!(c, 0, "孤儿清理遗漏: {sql}");
            }
            Ok(())
        })
        .unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn list_admin_filtered_by_group_status_and_sort() {
        let path = temp_db("list-filtered");
        let db = migrate::open_primary(&path).unwrap();
        let now = 1_800_000_000_000_000_000i64;
        db.write(|tx| {
            for (id, name, tag, display_idx, last_recv) in [
                (1, "beta", "", 10, Some(now - 5_000_000_000)),
                (2, "alpha", "prod", 30, Some(now - 60_000_000_000)),
                (3, "gamma", "prod", 20, None),
            ] {
                tx.execute(
                    "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) \
                     VALUES (?1, ?2, ?3, ?4, ?5, 0)",
                    rusqlite::params![id, name, tag, vec![id as u8], display_idx],
                )?;
                if let Some(ts) = last_recv {
                    tx.execute(
                        "INSERT INTO server_runtimes (server_id, status, last_received_at) VALUES (?1, 'online', ?2)",
                        rusqlite::params![id, ts],
                    )?;
                }
            }
            Ok(())
        })
        .unwrap();

        db.read(|conn| {
            // 未分组过滤
            let (rows, total) = list_admin_filtered(
                conn,
                "",
                Some(&AdminGroupFilter::Ungrouped),
                None,
                30,
                "id",
                true,
                0,
                20,
                now,
            )?;
            assert_eq!(total, 1);
            assert_eq!(rows[0].public.id, 1);

            // prod + offline（阈值 30s → id=2 60s 前离线，id=3 从未上报离线）
            let (rows, total) = list_admin_filtered(
                conn,
                "",
                Some(&AdminGroupFilter::Named("prod".into())),
                Some(AdminStatusFilter::Offline),
                30,
                "name",
                true,
                0,
                20,
                now,
            )?;
            assert_eq!(total, 2);
            assert_eq!(
                rows.iter()
                    .map(|r| r.public.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["alpha", "gamma"]
            );

            // online（阈值 30s → 仅 id=1 在线）
            let (rows, total) = list_admin_filtered(
                conn,
                "",
                None,
                Some(AdminStatusFilter::Online),
                30,
                "last_active",
                false,
                0,
                20,
                now,
            )?;
            assert_eq!(total, 1);
            assert_eq!(rows[0].public.id, 1);
            Ok(())
        })
        .unwrap();
        let _ = std::fs::remove_file(&path);
    }
}
