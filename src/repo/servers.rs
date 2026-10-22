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
    let sort_column = match sort {
        "id" => "s.id",
        "name" => "s.name",
        "tag" => "s.tag",
        "last_active" => "COALESCE(r.last_received_at, 0)",
        _ => "COALESCE(s.display_index, 0)",
    };
    let direction = if ascending { "ASC" } else { "DESC" };
    // `q` 是用户输入：`%`/`_` 必须转义，否则「100%」会匹配所有行。
    // COUNT 与 SELECT 用同一个转义值，避免计数与行数不一致。
    let like = format!("%{}%", crate::util::text::like_contains(q));
    let total: i64 = conn.query_row(
        "SELECT COUNT(*) FROM servers s WHERE s.deleted_at IS NULL \
         AND (?1 = '' OR s.name LIKE ?2 ESCAPE '\\' OR s.tag LIKE ?2 ESCAPE '\\' OR s.note LIKE ?2 ESCAPE '\\')",
        rusqlite::params![q, like],
        |row| row.get(0),
    )?;
    let sql = format!(
        "SELECT {ADMIN_SELECT} FROM servers s \
         LEFT JOIN server_runtimes r ON r.server_id = s.id \
         WHERE s.deleted_at IS NULL AND (?2 = '' OR s.name LIKE ?3 ESCAPE '\\' OR s.tag LIKE ?3 ESCAPE '\\' OR s.note LIKE ?3 ESCAPE '\\') \
         ORDER BY {sort_column} {direction}, s.id ASC LIMIT ?4 OFFSET ?5"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(
        rusqlite::params![now_nanos, q, like, limit, offset],
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
    tx.execute("DELETE FROM servers WHERE id = ?1", [id])?;
    let deleted = tx.execute("DELETE FROM monitor_histories WHERE server_id = ?1", [id])?;
    tx.execute("DELETE FROM transfers WHERE server_id = ?1", [id])?;
    tx.execute("DELETE FROM server_runtimes WHERE server_id = ?1", [id])?;
    tx.execute(
        "DELETE FROM server_offline_histories WHERE server_id = ?1",
        [id],
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
}
