//! API Token 表访问：对齐 Go `controller.v2ListAPITokens` 等的取数。
//!
//! 明文 token 只出现在创建响应；库内存加密密文 + sha256 哈希（唯一索引）。

use rusqlite::{Connection, OptionalExtension};

use crate::util::time as util_time;

/// `api_tokens` 全列行（管理端 CRUD 用）。
#[derive(Debug, Clone)]
pub struct ApiTokenRow {
    pub id: i64,
    pub user_id: i64,
    pub token_ciphertext: Vec<u8>,
    pub note: String,
    /// 空值按 write（Go `NormalizedPermission`）。
    pub permission: String,
    /// datetime 列的纳秒值；`None` = 永不过期。
    pub expires_at_nanos: Option<i64>,
    pub enabled: bool,
    pub created_at_nanos: Option<i64>,
}

fn row_from(row: &rusqlite::Row<'_>) -> rusqlite::Result<ApiTokenRow> {
    Ok(ApiTokenRow {
        id: row.get(0)?,
        user_id: row.get(1)?,
        token_ciphertext: row.get(2)?,
        note: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
        permission: row.get::<_, String>(4)?,
        expires_at_nanos: row
            .get::<_, Option<String>>(5)?
            .as_deref()
            .and_then(util_time::sqlite_to_unix_nanos),
        enabled: row.get::<_, i64>(6)? != 0,
        created_at_nanos: row
            .get::<_, Option<String>>(7)?
            .as_deref()
            .and_then(util_time::sqlite_to_unix_nanos),
    })
}

/// 站点全部未删除 Token（后台管理员统一审计与吊销，含已被移出管理员名单的协管员遗留 Token）。
pub fn list_all(conn: &Connection) -> anyhow::Result<Vec<ApiTokenRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, COALESCE(user_id, 0), token_ciphertext, note, permission, expires_at, enabled, created_at \
         FROM api_tokens WHERE deleted_at IS NULL ORDER BY created_at DESC, id DESC",
    )?;
    let rows = stmt
        .query_map([], row_from)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// 用户的全部 Token（Go `Order("created_at DESC")`；软删排除）。
pub fn list_by_user(conn: &Connection, user_id: i64) -> anyhow::Result<Vec<ApiTokenRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, COALESCE(user_id, 0), token_ciphertext, note, permission, expires_at, enabled, created_at \
         FROM api_tokens WHERE user_id = ?1 AND deleted_at IS NULL ORDER BY created_at DESC",
    )?;
    let rows = stmt
        .query_map([user_id], row_from)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// 按 ID 单查（不限归属用户，供管理员查看/启停/吊销任意 Token）。
pub fn find_by_id(conn: &Connection, id: i64) -> anyhow::Result<Option<ApiTokenRow>> {
    conn.query_row(
        "SELECT id, COALESCE(user_id, 0), token_ciphertext, note, permission, expires_at, enabled, created_at \
         FROM api_tokens WHERE id = ?1 AND deleted_at IS NULL",
        [id],
        row_from,
    )
    .optional()
    .map_err(Into::into)
}

/// 单查（归属校验在 SQL：user_id + id 双条件）。
pub fn get_by_id(conn: &Connection, user_id: i64, id: i64) -> anyhow::Result<Option<ApiTokenRow>> {
    conn.query_row(
        "SELECT id, COALESCE(user_id, 0), token_ciphertext, note, permission, expires_at, enabled, created_at \
         FROM api_tokens WHERE user_id = ?1 AND id = ?2 AND deleted_at IS NULL",
        [user_id, id],
        row_from,
    )
    .optional()
    .map_err(Into::into)
}

/// 新建 Token（明文由调用方生成并加密），返回新 ID。
pub fn insert(
    tx: &rusqlite::Transaction<'_>,
    user_id: i64,
    token_ciphertext: &[u8],
    token_hash: &[u8],
    note: &str,
    permission: &str,
    expires_at: Option<i64>,
) -> anyhow::Result<i64> {
    let expires_text = expires_at
        .filter(|nanos| *nanos > 0)
        .map(util_time::unix_nanos_to_sqlite);
    tx.execute(
        "INSERT INTO api_tokens (created_at, updated_at, user_id, token_ciphertext, token_hash, \
         note, permission, expires_at, enabled) \
         VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?1, ?2, ?3, ?4, ?5, ?6, 1)",
        rusqlite::params![
            user_id,
            token_ciphertext,
            token_hash,
            note,
            permission,
            expires_text
        ],
    )?;
    Ok(tx.last_insert_rowid())
}

/// 硬删（不限归属用户，供管理员吊销任意 Token），返回是否命中。
pub fn delete_by_id(tx: &rusqlite::Transaction<'_>, id: i64) -> anyhow::Result<bool> {
    Ok(tx.execute("DELETE FROM api_tokens WHERE id = ?1", [id])? > 0)
}

/// 硬删（Go `Unscoped().Delete`），返回是否命中。
pub fn delete(tx: &rusqlite::Transaction<'_>, user_id: i64, id: i64) -> anyhow::Result<bool> {
    Ok(tx.execute(
        "DELETE FROM api_tokens WHERE user_id = ?1 AND id = ?2",
        [user_id, id],
    )? > 0)
}

/// 启停（不限归属用户），返回是否命中。
pub fn set_enabled_by_id(
    tx: &rusqlite::Transaction<'_>,
    id: i64,
    enabled: bool,
) -> anyhow::Result<bool> {
    Ok(tx.execute(
        "UPDATE api_tokens SET enabled = ?2, updated_at = CURRENT_TIMESTAMP WHERE id = ?1",
        rusqlite::params![id, enabled as i64],
    )? > 0)
}

/// 启停（Go `Update("enabled", ...)`），返回是否命中。
pub fn set_enabled(
    tx: &rusqlite::Transaction<'_>,
    user_id: i64,
    id: i64,
    enabled: bool,
) -> anyhow::Result<bool> {
    Ok(tx.execute(
        "UPDATE api_tokens SET enabled = ?3, updated_at = CURRENT_TIMESTAMP \
         WHERE user_id = ?1 AND id = ?2",
        rusqlite::params![user_id, id, enabled as i64],
    )? > 0)
}
