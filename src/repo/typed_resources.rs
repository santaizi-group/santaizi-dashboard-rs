//! Typed 资源（monitors / notifications / alert-rules）表访问：
//! 对齐 Go `model.Monitor` / `model.Notification` / `model.AlertRule`。
//!
//! 常量值沿 Go iota：监控类型 http=1/icmp=2/tcp=3；cover all=0/ignore_all=1；
//! 通知 method get=1/post=2、request_type json=1/form=2；告警触发 always=0/once=1。

use rusqlite::{Connection, OptionalExtension};

use crate::util::time as util_time;

pub const MONITOR_TYPE_HTTP: i64 = 1;
pub const MONITOR_TYPE_ICMP: i64 = 2;
pub const MONITOR_TYPE_TCP: i64 = 3;
pub const MONITOR_COVER_ALL: i64 = 0;
pub const MONITOR_COVER_IGNORE_ALL: i64 = 1;
pub const NOTIFY_METHOD_GET: i64 = 1;
pub const NOTIFY_METHOD_POST: i64 = 2;
pub const NOTIFY_TYPE_JSON: i64 = 1;
pub const NOTIFY_TYPE_FORM: i64 = 2;
pub const ALERT_TRIGGER_ALWAYS: i64 = 0;
pub const ALERT_TRIGGER_ONCE: i64 = 1;

fn datetime(value: Option<String>) -> Option<i64> {
    value.as_deref().and_then(util_time::sqlite_to_unix_nanos)
}

#[derive(Debug, Clone)]
pub struct MonitorRow {
    pub id: i64,
    pub name: String,
    pub monitor_type: i64,
    pub target: String,
    /// JSON 数组文本（Go `SkipServersRaw`）。
    pub skip_servers_raw: String,
    pub duration: i64,
    pub notify: bool,
    pub notification_tag: String,
    pub cover: i64,
    pub enable_show_in_service: bool,
    pub min_latency: f64,
    pub max_latency: f64,
    pub latency_notify: bool,
    pub created_at_nanos: Option<i64>,
    pub updated_at_nanos: Option<i64>,
}

const MONITOR_COLUMNS: &str = "id, COALESCE(name, ''), COALESCE(type, 0), COALESCE(target, ''), \
     COALESCE(skip_servers_raw, '[]'), COALESCE(duration, 30), COALESCE(notify, 0), \
     COALESCE(notification_tag, ''), COALESCE(cover, 0), COALESCE(enable_show_in_service, 0), \
     COALESCE(min_latency, 0), COALESCE(max_latency, 0), COALESCE(latency_notify, 0), \
     created_at, updated_at";

fn monitor_from(row: &rusqlite::Row<'_>) -> rusqlite::Result<MonitorRow> {
    Ok(MonitorRow {
        id: row.get(0)?,
        name: row.get(1)?,
        monitor_type: row.get(2)?,
        target: row.get(3)?,
        skip_servers_raw: row.get(4)?,
        duration: row.get(5)?,
        notify: row.get::<_, i64>(6)? != 0,
        notification_tag: row.get(7)?,
        cover: row.get(8)?,
        enable_show_in_service: row.get::<_, i64>(9)? != 0,
        min_latency: row.get(10)?,
        max_latency: row.get(11)?,
        latency_notify: row.get::<_, i64>(12)? != 0,
        created_at_nanos: datetime(row.get(13)?),
        updated_at_nanos: datetime(row.get(14)?),
    })
}

/// 管理端列表（q 匹配 name/target；排序白名单 id|name|updated_at）。
pub fn list_monitors(
    conn: &Connection,
    q: &str,
    sort: &str,
    ascending: bool,
    offset: i64,
    limit: i64,
) -> anyhow::Result<(Vec<MonitorRow>, i64)> {
    let sort_column = match sort {
        "name" => "name",
        "updated_at" => "updated_at",
        _ => "id",
    };
    let direction = if ascending { "ASC" } else { "DESC" };
    let like = format!("%{}%", crate::util::text::like_contains(q));
    let total: i64 = conn.query_row(
        "SELECT COUNT(*) FROM monitors WHERE deleted_at IS NULL AND (?1 = '' OR name LIKE ?2 ESCAPE '\\' OR target LIKE ?2 ESCAPE '\\')",
        rusqlite::params![q, like],
        |row| row.get(0),
    )?;
    let sql = format!(
        "SELECT {MONITOR_COLUMNS} FROM monitors \
         WHERE deleted_at IS NULL AND (?1 = '' OR name LIKE ?2 ESCAPE '\\' OR target LIKE ?2 ESCAPE '\\') \
         ORDER BY {sort_column} {direction}, id ASC LIMIT ?3 OFFSET ?4"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(rusqlite::params![q, like, limit, offset], monitor_from)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok((rows, total))
}

pub fn monitor_by_id(conn: &Connection, id: i64) -> anyhow::Result<Option<MonitorRow>> {
    let sql =
        format!("SELECT {MONITOR_COLUMNS} FROM monitors WHERE id = ?1 AND deleted_at IS NULL");
    conn.query_row(&sql, [id], monitor_from)
        .optional()
        .map_err(Into::into)
}

/// 写入（Go `Save`：新建或全量更新，保留 created_at）。
/// 返回资源 id：新建走 `last_insert_rowid()`，调用方不必再 `SELECT MAX(id)`
/// （并发写入下 MAX(id) 会读到别人刚插入的行）。
pub fn save_monitor(tx: &rusqlite::Transaction<'_>, row: &MonitorRow) -> anyhow::Result<i64> {
    if row.id == 0 {
        tx.execute(
            "INSERT INTO monitors (created_at, updated_at, name, type, target, skip_servers_raw, \
             duration, notify, notification_tag, cover, enable_show_in_service, min_latency, \
             max_latency, latency_notify) \
             VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            rusqlite::params![
                row.name,
                row.monitor_type,
                row.target,
                row.skip_servers_raw,
                row.duration,
                row.notify as i64,
                row.notification_tag,
                row.cover,
                row.enable_show_in_service as i64,
                row.min_latency,
                row.max_latency,
                row.latency_notify as i64,
            ],
        )?;
        return Ok(tx.last_insert_rowid());
    }
    tx.execute(
        "UPDATE monitors SET updated_at = CURRENT_TIMESTAMP, name = ?2, type = ?3, target = ?4, \
         skip_servers_raw = ?5, duration = ?6, notify = ?7, notification_tag = ?8, cover = ?9, \
         enable_show_in_service = ?10, min_latency = ?11, max_latency = ?12, latency_notify = ?13 \
         WHERE id = ?1 AND deleted_at IS NULL",
        rusqlite::params![
            row.id,
            row.name,
            row.monitor_type,
            row.target,
            row.skip_servers_raw,
            row.duration,
            row.notify as i64,
            row.notification_tag,
            row.cover,
            row.enable_show_in_service as i64,
            row.min_latency,
            row.max_latency,
            row.latency_notify as i64,
        ],
    )?;
    Ok(row.id)
}

/// 硬删 + 连带监控历史（Go `v2DeleteMonitor`），返回是否命中。
pub fn delete_monitor(tx: &rusqlite::Transaction<'_>, id: i64) -> anyhow::Result<bool> {
    let changed = tx.execute("DELETE FROM monitors WHERE id = ?1", [id])? > 0;
    if changed {
        tx.execute("DELETE FROM monitor_histories WHERE monitor_id = ?1", [id])?;
    }
    Ok(changed)
}

#[derive(Debug, Clone)]
pub struct NotificationRow {
    pub id: i64,
    pub name: String,
    pub tag: String,
    pub url: String,
    pub request_method: i64,
    pub request_type: i64,
    /// JSON 对象文本（Go `RequestHeader`）。
    pub request_header: String,
    pub request_body: String,
    /// `*bool`：NULL 视为 false（Go `VerifySSL != nil && *VerifySSL`）。
    pub verify_ssl: Option<i64>,
    pub created_at_nanos: Option<i64>,
    pub updated_at_nanos: Option<i64>,
}

const NOTIFICATION_COLUMNS: &str = "id, COALESCE(name, ''), COALESCE(tag, ''), COALESCE(url, ''), \
     COALESCE(request_method, 1), COALESCE(request_type, 1), COALESCE(request_header, '{}'), \
     COALESCE(request_body, ''), verify_ssl, created_at, updated_at";

fn notification_from(row: &rusqlite::Row<'_>) -> rusqlite::Result<NotificationRow> {
    Ok(NotificationRow {
        id: row.get(0)?,
        name: row.get(1)?,
        tag: row.get(2)?,
        url: row.get(3)?,
        request_method: row.get(4)?,
        request_type: row.get(5)?,
        request_header: row.get(6)?,
        request_body: row.get(7)?,
        verify_ssl: row.get(8)?,
        created_at_nanos: datetime(row.get(9)?),
        updated_at_nanos: datetime(row.get(10)?),
    })
}

pub fn list_notifications(
    conn: &Connection,
    q: &str,
    sort: &str,
    ascending: bool,
    offset: i64,
    limit: i64,
) -> anyhow::Result<(Vec<NotificationRow>, i64)> {
    let sort_column = match sort {
        "name" => "name",
        "updated_at" => "updated_at",
        _ => "id",
    };
    let direction = if ascending { "ASC" } else { "DESC" };
    let like = format!("%{}%", crate::util::text::like_contains(q));
    let total: i64 = conn.query_row(
        "SELECT COUNT(*) FROM notifications WHERE deleted_at IS NULL AND (?1 = '' OR name LIKE ?2 ESCAPE '\\')",
        rusqlite::params![q, like],
        |row| row.get(0),
    )?;
    let sql = format!(
        "SELECT {NOTIFICATION_COLUMNS} FROM notifications \
         WHERE deleted_at IS NULL AND (?1 = '' OR name LIKE ?2 ESCAPE '\\') \
         ORDER BY {sort_column} {direction}, id ASC LIMIT ?3 OFFSET ?4"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(rusqlite::params![q, like, limit, offset], notification_from)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok((rows, total))
}

pub fn notification_by_id(conn: &Connection, id: i64) -> anyhow::Result<Option<NotificationRow>> {
    let sql = format!(
        "SELECT {NOTIFICATION_COLUMNS} FROM notifications WHERE id = ?1 AND deleted_at IS NULL"
    );
    conn.query_row(&sql, [id], notification_from)
        .optional()
        .map_err(Into::into)
}

pub fn save_notification(
    tx: &rusqlite::Transaction<'_>,
    row: &NotificationRow,
) -> anyhow::Result<i64> {
    if row.id == 0 {
        tx.execute(
            "INSERT INTO notifications (created_at, updated_at, name, tag, url, request_method, \
             request_type, request_header, request_body, verify_ssl) \
             VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                row.name,
                row.tag,
                row.url,
                row.request_method,
                row.request_type,
                row.request_header,
                row.request_body,
                row.verify_ssl,
            ],
        )?;
        return Ok(tx.last_insert_rowid());
    }
    tx.execute(
        "UPDATE notifications SET updated_at = CURRENT_TIMESTAMP, name = ?2, tag = ?3, url = ?4, \
         request_method = ?5, request_type = ?6, request_header = ?7, request_body = ?8, \
         verify_ssl = ?9 WHERE id = ?1 AND deleted_at IS NULL",
        rusqlite::params![
            row.id,
            row.name,
            row.tag,
            row.url,
            row.request_method,
            row.request_type,
            row.request_header,
            row.request_body,
            row.verify_ssl,
        ],
    )?;
    Ok(row.id)
}

pub fn delete_notification(tx: &rusqlite::Transaction<'_>, id: i64) -> anyhow::Result<bool> {
    Ok(tx.execute("DELETE FROM notifications WHERE id = ?1", [id])? > 0)
}

#[derive(Debug, Clone)]
pub struct AlertRuleRow {
    pub id: i64,
    pub name: String,
    /// JSON 数组文本（Go `Rules`，元素含 type/min/max/duration/cover/ignore）。
    pub rules_raw: String,
    /// `*bool`：NULL = 未启用（Go `Enabled()`）。
    pub enable: Option<i64>,
    pub trigger_mode: i64,
    pub notification_tag: String,
    pub created_at_nanos: Option<i64>,
    pub updated_at_nanos: Option<i64>,
}

const ALERT_RULE_COLUMNS: &str = "id, COALESCE(name, ''), COALESCE(rules_raw, '[]'), enable, \
     COALESCE(trigger_mode, 0), COALESCE(notification_tag, ''), created_at, updated_at";

fn alert_rule_from(row: &rusqlite::Row<'_>) -> rusqlite::Result<AlertRuleRow> {
    Ok(AlertRuleRow {
        id: row.get(0)?,
        name: row.get(1)?,
        rules_raw: row.get(2)?,
        enable: row.get(3)?,
        trigger_mode: row.get(4)?,
        notification_tag: row.get(5)?,
        created_at_nanos: datetime(row.get(6)?),
        updated_at_nanos: datetime(row.get(7)?),
    })
}

pub fn list_alert_rules(
    conn: &Connection,
    q: &str,
    sort: &str,
    ascending: bool,
    offset: i64,
    limit: i64,
) -> anyhow::Result<(Vec<AlertRuleRow>, i64)> {
    let sort_column = match sort {
        "name" => "name",
        "updated_at" => "updated_at",
        _ => "id",
    };
    let direction = if ascending { "ASC" } else { "DESC" };
    let like = format!("%{}%", crate::util::text::like_contains(q));
    let total: i64 = conn.query_row(
        "SELECT COUNT(*) FROM alert_rules WHERE deleted_at IS NULL AND (?1 = '' OR name LIKE ?2 ESCAPE '\\')",
        rusqlite::params![q, like],
        |row| row.get(0),
    )?;
    let sql = format!(
        "SELECT {ALERT_RULE_COLUMNS} FROM alert_rules \
         WHERE deleted_at IS NULL AND (?1 = '' OR name LIKE ?2 ESCAPE '\\') \
         ORDER BY {sort_column} {direction}, id ASC LIMIT ?3 OFFSET ?4"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(rusqlite::params![q, like, limit, offset], alert_rule_from)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok((rows, total))
}

pub fn alert_rule_by_id(conn: &Connection, id: i64) -> anyhow::Result<Option<AlertRuleRow>> {
    let sql = format!(
        "SELECT {ALERT_RULE_COLUMNS} FROM alert_rules WHERE id = ?1 AND deleted_at IS NULL"
    );
    conn.query_row(&sql, [id], alert_rule_from)
        .optional()
        .map_err(Into::into)
}

/// 写入：新建或全量更新。返回资源 id（新建走 `last_insert_rowid()`）。
pub fn save_alert_rule(tx: &rusqlite::Transaction<'_>, row: &AlertRuleRow) -> anyhow::Result<i64> {
    if row.id == 0 {
        tx.execute(
            "INSERT INTO alert_rules (created_at, updated_at, name, rules_raw, enable, \
             trigger_mode, notification_tag) \
             VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                row.name,
                row.rules_raw,
                row.enable,
                row.trigger_mode,
                row.notification_tag
            ],
        )?;
        return Ok(tx.last_insert_rowid());
    }
    tx.execute(
        "UPDATE alert_rules SET updated_at = CURRENT_TIMESTAMP, name = ?2, rules_raw = ?3, \
         enable = ?4, trigger_mode = ?5, notification_tag = ?6 \
         WHERE id = ?1 AND deleted_at IS NULL",
        rusqlite::params![
            row.id,
            row.name,
            row.rules_raw,
            row.enable,
            row.trigger_mode,
            row.notification_tag,
        ],
    )?;
    Ok(row.id)
}

pub fn delete_alert_rule(tx: &rusqlite::Transaction<'_>, id: i64) -> anyhow::Result<bool> {
    Ok(tx.execute("DELETE FROM alert_rules WHERE id = ?1", [id])? > 0)
}
