//! 启动时补运行态骨架、修离线历史不一致，并按保留天数清理已结束的离线记录。

use rusqlite::{Connection, OptionalExtension, params};

use crate::repo::insights;
use crate::telemetry::offline::{detect_offline_reason, duration_seconds_between};
use crate::util::time::{self, sqlite_to_unix_nanos, unix_nanos_to_sqlite};

const STATUS_UNKNOWN: &str = "unknown";
const STATUS_OPEN: &str = "open";
const STATUS_CLOSED: &str = "closed";
const STATUS_ONLINE: &str = "online";
const STATUS_OFFLINE: &str = "offline";

struct RuntimeSeen {
    last_seen_at: Option<String>,
    last_boot: Option<i64>,
    last_uptime: Option<i64>,
    last_ip: Option<String>,
    last_offline_at: Option<String>,
}

fn load_runtime(conn: &Connection, server_id: i64) -> anyhow::Result<Option<RuntimeSeen>> {
    conn.query_row(
        "SELECT last_seen_at, last_boot_time, last_uptime, last_ip, last_offline_at \
         FROM server_runtimes WHERE server_id = ?1",
        [server_id],
        |row| {
            Ok(RuntimeSeen {
                last_seen_at: row.get(0)?,
                last_boot: row.get(1)?,
                last_uptime: row.get(2)?,
                last_ip: row.get(3)?,
                last_offline_at: row.get(4)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

/// `InitServerRuntimes`：没有运行态的服务器补一行 unknown，不预填上报时间。
pub fn init_server_runtimes(conn: &Connection) -> anyhow::Result<usize> {
    let mut stmt = conn.prepare(
        "SELECT id FROM servers WHERE deleted_at IS NULL AND id NOT IN \
         (SELECT server_id FROM server_runtimes)",
    )?;
    let ids = stmt.query_map([], |row| row.get::<_, i64>(0))?;
    let mut created = 0usize;
    let mut missing = Vec::new();
    for id in ids {
        missing.push(id?);
    }
    drop(stmt);
    for id in missing {
        created += conn.execute(
            "INSERT INTO server_runtimes (server_id, status, created_at, updated_at) \
             VALUES (?1, ?2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            params![id, STATUS_UNKNOWN],
        )?;
    }
    Ok(created)
}

/// `ReconcileOfflineHistories`：正在上报则关掉遗留的未结束记录；确实离线则只留最早一条。
pub fn reconcile_offline_histories(
    conn: &Connection,
    threshold_seconds: u64,
    now_nanos: i64,
) -> anyhow::Result<()> {
    let threshold_seconds = threshold_seconds.max(10);
    let threshold_nanos = (threshold_seconds as i64).saturating_mul(1_000_000_000);
    let mut stmt = conn.prepare(
        "SELECT DISTINCT server_id FROM server_offline_histories \
         WHERE status = ?1 AND deleted_at IS NULL AND server_id IS NOT NULL",
    )?;
    let ids = stmt.query_map([STATUS_OPEN], |row| row.get::<_, i64>(0))?;
    let mut servers = Vec::new();
    for id in ids {
        servers.push(id?);
    }
    drop(stmt);
    for server_id in servers {
        reconcile_server(conn, server_id, threshold_nanos, now_nanos)?;
    }
    Ok(())
}

fn reconcile_server(
    conn: &Connection,
    server_id: i64,
    threshold_nanos: i64,
    now_nanos: i64,
) -> anyhow::Result<()> {
    let mut stmt = conn.prepare(
        "SELECT id, started_at, last_boot_time, last_uptime FROM server_offline_histories \
         WHERE server_id = ?1 AND status = ?2 AND deleted_at IS NULL ORDER BY id ASC",
    )?;
    let opens = stmt.query_map(params![server_id, STATUS_OPEN], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<i64>>(2)?.unwrap_or(0).max(0) as u64,
            row.get::<_, Option<i64>>(3)?.unwrap_or(0).max(0) as u64,
        ))
    })?;
    let mut rows = Vec::new();
    for row in opens {
        rows.push(row?);
    }
    drop(stmt);
    if rows.is_empty() {
        return Ok(());
    }
    init_server_runtimes(conn)?;
    let runtime = load_runtime(conn, server_id)?;
    let Some(runtime) = runtime else {
        return Ok(());
    };
    let last_seen_at = runtime.last_seen_at;
    let last_boot = runtime.last_boot;
    let last_uptime = runtime.last_uptime;
    let last_ip = runtime.last_ip;
    let last_offline_at = runtime.last_offline_at;
    let last_seen_nanos = last_seen_at
        .as_deref()
        .and_then(sqlite_to_unix_nanos)
        .unwrap_or(0);
    let reporting =
        last_seen_nanos > 0 && now_nanos.saturating_sub(last_seen_nanos) <= threshold_nanos;
    let boot = last_boot.unwrap_or(0).max(0) as u64;
    let uptime = last_uptime.unwrap_or(0).max(0) as u64;
    let ip = last_ip.unwrap_or_default();
    let now_text = unix_nanos_to_sqlite(now_nanos);
    if reporting {
        for (id, started_at, hist_boot, hist_uptime) in &rows {
            let started = started_at
                .as_deref()
                .and_then(sqlite_to_unix_nanos)
                .unwrap_or(last_seen_nanos);
            let end = last_seen_nanos.max(started);
            let reason = detect_offline_reason(*hist_boot, boot, *hist_uptime, uptime);
            conn.execute(
                "UPDATE server_offline_histories SET ended_at = ?1, recovered_at = ?1, \
                 duration_seconds = ?2, status = ?3, reason = ?4, recovered_boot_time = ?5, \
                 recovered_uptime = ?6, recovered_ip = ?7, note = 'auto_reconcile', updated_at = ?8 \
                 WHERE id = ?9",
                params![
                    unix_nanos_to_sqlite(end),
                    duration_seconds_between(started, end) as i64,
                    STATUS_CLOSED,
                    reason,
                    boot as i64,
                    uptime as i64,
                    ip,
                    now_text,
                    id,
                ],
            )?;
        }
        conn.execute(
            "UPDATE server_runtimes SET status = ?1, current_offline_id = 0, updated_at = ?2 \
             WHERE server_id = ?3",
            params![STATUS_ONLINE, now_text, server_id],
        )?;
        return Ok(());
    }
    let keep_id = rows[0].0;
    for (id, _, _, _) in rows.iter().skip(1) {
        conn.execute("DELETE FROM server_offline_histories WHERE id = ?1", [id])?;
    }
    if last_seen_nanos == 0 {
        return Ok(());
    }
    let offline_at = if last_offline_at
        .as_deref()
        .is_some_and(|text| !text.is_empty())
    {
        None
    } else {
        rows[0].1.clone()
    };
    conn.execute(
        "UPDATE server_runtimes SET status = ?1, current_offline_id = ?2, \
         last_offline_at = COALESCE(last_offline_at, ?3), updated_at = ?4 WHERE server_id = ?5",
        params![STATUS_OFFLINE, keep_id, offline_at, now_text, server_id],
    )?;
    Ok(())
}

pub async fn run_offline_retention(
    db: crate::db::Db,
    conf: crate::config::ConfigHandle,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    purge_ended_offline_history(&db, &conf);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(24 * 3_600)) => {
                purge_ended_offline_history(&db, &conf);
            }
        }
    }
}

fn purge_ended_offline_history(db: &crate::db::Db, conf: &crate::config::ConfigHandle) {
    let days = conf
        .read()
        .expect("config lock")
        .offline_history_retention_days;
    if days == 0 {
        return;
    }
    let before = time::now_nanos() - (days as i64).saturating_mul(24 * 3_600 * 1_000_000_000);
    match db.write(|tx| insights::cleanup_offline_history(tx, None, before)) {
        Ok(deleted) => tracing::info!(deleted, "离线历史定时清理"),
        Err(err) => tracing::warn!("离线历史定时清理失败: {err:#}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;

    fn temp_db() -> crate::db::Db {
        let dir = std::env::temp_dir().join(format!(
            "santaizi-housekeeping-{}-{}",
            std::process::id(),
            time::now_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        migrate::open_primary(dir.join("panel.db").to_str().unwrap()).unwrap()
    }

    #[test]
    fn skeleton_runtime_and_reconcile_open_histories() {
        let db = temp_db();
        let now = time::now_nanos();
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, secret_ciphertext) VALUES (4, 'bare', X'04')",
                [],
            )?;
            tx.execute(
                "INSERT INTO servers (id, name, secret_ciphertext) VALUES (5, 'back', X'05')",
                [],
            )?;
            tx.execute(
                "INSERT INTO server_runtimes (server_id, status, last_seen_at, last_boot_time, last_uptime, last_ip) \
                 VALUES (5, 'offline', ?1, 10, 20, '203.0.113.5')",
                [unix_nanos_to_sqlite(now - 1_000_000_000)],
            )?;
            for _ in 0..2 {
                tx.execute(
                    "INSERT INTO server_offline_histories (server_id, started_at, detected_at, status, last_boot_time, last_uptime) \
                     VALUES (5, ?1, ?1, 'open', 10, 20)",
                    [unix_nanos_to_sqlite(now - 60_000_000_000)],
                )?;
            }
            init_server_runtimes(tx)?;
            reconcile_offline_histories(tx, 30, now)?;
            Ok(())
        })
        .unwrap();
        let status: String = db
            .read(|conn| {
                conn.query_row(
                    "SELECT status FROM server_runtimes WHERE server_id = 4",
                    [],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(status, "unknown");
        let open: i64 = db
            .read(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM server_offline_histories WHERE server_id = 5 AND status = 'open'",
                    [],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(open, 0);
        let runtime: String = db
            .read(|conn| {
                conn.query_row(
                    "SELECT status FROM server_runtimes WHERE server_id = 5",
                    [],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(runtime, "online");
    }

    #[test]
    fn stale_server_keeps_the_earliest_open_history() {
        let db = temp_db();
        let now = time::now_nanos();
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, secret_ciphertext) VALUES (8, 'down', X'08')",
                [],
            )?;
            tx.execute(
                "INSERT INTO server_runtimes (server_id, status, last_seen_at) VALUES (8, 'online', ?1)",
                [unix_nanos_to_sqlite(now - 3_600 * 1_000_000_000)],
            )?;
            for _ in 0..2 {
                tx.execute(
                    "INSERT INTO server_offline_histories (server_id, started_at, detected_at, status) \
                     VALUES (8, ?1, ?1, 'open')",
                    [unix_nanos_to_sqlite(now - 7_200 * 1_000_000_000)],
                )?;
            }
            reconcile_offline_histories(tx, 30, now)?;
            let open: i64 = tx.query_row(
                "SELECT COUNT(*) FROM server_offline_histories WHERE server_id = 8 AND status = 'open'",
                [],
                |row| row.get(0),
            )?;
            let keep: i64 = tx.query_row(
                "SELECT current_offline_id FROM server_runtimes WHERE server_id = 8 AND status = 'offline'",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(open, 1);
            assert!(keep > 0);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn retention_deletes_only_ended_rows() {
        let db = temp_db();
        let now = time::now_nanos();
        let old = unix_nanos_to_sqlite(now - 400 * 24 * 3_600 * 1_000_000_000);
        db.write(|tx| {
            tx.execute(
                "INSERT INTO server_offline_histories (server_id, started_at, ended_at, status) \
                 VALUES (1, ?1, ?1, 'closed')",
                [&old],
            )?;
            tx.execute(
                "INSERT INTO server_offline_histories (server_id, started_at, status) \
                 VALUES (1, ?1, 'open')",
                [&old],
            )?;
            let deleted = insights::cleanup_offline_history(
                tx,
                None,
                now - 365 * 24 * 3_600 * 1_000_000_000,
            )?;
            assert_eq!(deleted, 1);
            Ok(())
        })
        .unwrap();
    }
}
