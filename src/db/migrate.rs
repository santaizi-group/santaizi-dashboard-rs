//! 迁移门。对齐 Go 版行为：
//! * 只有文件里没有任何用户表时才先写 `PRAGMA auto_vacuum=INCREMENTAL`
//!   （该值在文件头，建表后无法更改）；
//! * 「有表但没有 schema_migrations 表」的库拒绝打开——不迁旧库；
//! * 全新库一次建全量终态 DDL，记录版本号。

use crate::db::Db;
use crate::db::ddl::{COLLECTOR_DDL, PRIMARY_DDL};
use crate::util::time;

/// 主库终态版本（对齐 Go schema v20；Go 的 20 步迁移在 Rust 侧折叠为一套幂等 DDL）。
pub const PRIMARY_SCHEMA_VERSION: u64 = 20;
/// 从端库终态版本（对齐 Go collector store v7）。
pub const COLLECTOR_SCHEMA_VERSION: u64 = 7;

fn user_table_count(conn: &rusqlite::Connection) -> anyhow::Result<i64> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?)
}

fn current_version(conn: &rusqlite::Connection, table: &str) -> anyhow::Result<Option<u64>> {
    let exists: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
        [table],
        |row| row.get(0),
    )?;
    if exists == 0 {
        return Ok(None);
    }
    let version: Option<i64> = conn
        .query_row(&format!("SELECT MAX(version) FROM {table}"), [], |row| {
            row.get(0)
        })
        .optional()?;
    Ok(version.map(|v| v as u64))
}

/// 幂等 DDL 重跑：`ddl.rs` 只含 `CREATE ... IF NOT EXISTS`，用于给存量库补上
/// 后续版本新增的索引/表。schema version 不 bump（bump 会让存量库直接 bail），
/// 所以这条路径是存量库获得新索引的唯一入口。
fn ensure_idempotent_ddl(conn: &mut rusqlite::Connection, ddl: &str) -> anyhow::Result<()> {
    conn.execute_batch(ddl)?;
    Ok(())
}

/// 打开主库并保证 schema 为终态。
pub fn open_primary(path: &str) -> anyhow::Result<Db> {
    let db = Db::open(path)?;
    {
        let mut conn = db.get()?;
        let tables = user_table_count(&conn)?;
        let recorded = current_version(&conn, "schema_migrations")?;
        if tables > 0 && recorded.is_none() {
            anyhow::bail!(
                "existing database without schema_migrations is unsupported; configure an empty Santaizi database"
            );
        }
        if let Some(version) = recorded {
            if version != PRIMARY_SCHEMA_VERSION {
                anyhow::bail!(
                    "unsupported schema version {version}; expected {PRIMARY_SCHEMA_VERSION} (fresh install only)"
                );
            }
            ensure_idempotent_ddl(&mut conn, PRIMARY_DDL)?;
            return Ok(db);
        }
        // 全新库：先写 auto_vacuum（文件头），再建表。
        if tables == 0 {
            conn.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;
        }
        // 建表与版本号记录必须同生同死：非原子的旧行为会在 DDL 执行后崩溃时留下
        // 「有表但无版本号」的库，下次启动只能 bail。
        let applied_at = time::unix_nanos_to_sqlite(time::now_nanos());
        {
            let tx = conn.transaction()?;
            tx.execute_batch(PRIMARY_DDL)?;
            tx.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?1, ?2)",
                rusqlite::params![PRIMARY_SCHEMA_VERSION as i64, applied_at],
            )?;
            tx.commit()?;
        }
    }
    Ok(db)
}

/// 打开从端库并保证 schema 为终态。
pub fn open_collector(path: &str) -> anyhow::Result<Db> {
    let db = Db::open(path)?;
    {
        let mut conn = db.get()?;
        let tables = user_table_count(&conn)?;
        let recorded = current_version(&conn, "collector_schema_migrations")?;
        if tables > 0 && recorded.is_none() {
            anyhow::bail!(
                "existing collector database without collector_schema_migrations is unsupported"
            );
        }
        if let Some(version) = recorded {
            if version != COLLECTOR_SCHEMA_VERSION {
                anyhow::bail!(
                    "unsupported collector schema version {version}; expected {COLLECTOR_SCHEMA_VERSION} (fresh install only)"
                );
            }
            ensure_idempotent_ddl(&mut conn, COLLECTOR_DDL)?;
            return Ok(db);
        }
        let applied_at = time::unix_nanos_to_sqlite(time::now_nanos());
        {
            let tx = conn.transaction()?;
            tx.execute_batch(COLLECTOR_DDL)?;
            tx.execute(
                "INSERT INTO collector_schema_migrations (version, applied_at) VALUES (?1, ?2)",
                rusqlite::params![COLLECTOR_SCHEMA_VERSION as i64, applied_at],
            )?;
            tx.commit()?;
        }
        // Go v1 会预置复制游标单行。
        conn.execute(
            "INSERT OR IGNORE INTO collector_replication_cursors (id, committed_spool_through_id, next_batch_sequence) VALUES (1, 0, 1)",
            [],
        )?;
    }
    Ok(db)
}

use rusqlite::OptionalExtension;

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> String {
        let dir =
            std::env::temp_dir().join(format!("santaizi-migrate-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("sqlite.db").to_string_lossy().to_string()
    }

    #[test]
    fn fresh_primary_creates_full_schema() {
        let path = temp_path("primary");
        let db = open_primary(&path).unwrap();
        let tables: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                    [],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        // 34 张业务表 + schema_migrations。
        assert!(tables >= 35, "tables: {tables}");
        let version: u64 = db
            .read(|conn| {
                Ok(
                    conn.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                        row.get::<_, i64>(0)
                    })? as u64,
                )
            })
            .unwrap();
        assert_eq!(version, PRIMARY_SCHEMA_VERSION);
        // 幂等：再跑一次 DDL 不炸。
        db.exec_batch(PRIMARY_DDL).unwrap();
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    /// 存量库（版本号已是终态）必须在重开时补上后续版本新增的索引——
    /// 否则只改 DDL 对已部署库完全无效（版本门会提前 return）。
    #[test]
    fn existing_primary_gains_new_indexes_on_reopen() {
        let path = temp_path("reopen");
        let db = open_primary(&path).unwrap();
        db.exec_batch("DROP INDEX idx_monitor_histories_monitor_created")
            .unwrap();
        assert!(index_exists(&db, "idx_monitor_histories_monitor_created").is_none());
        drop(db);
        let reopened = open_primary(&path).unwrap();
        assert_eq!(
            index_exists(&reopened, "idx_monitor_histories_monitor_created").as_deref(),
            Some("monitor_histories"),
            "存量库重开应补回新索引"
        );
        assert_eq!(
            index_exists(&reopened, "idx_monitor_histories_server_created").as_deref(),
            Some("monitor_histories"),
        );
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    fn index_exists(db: &Db, name: &str) -> Option<String> {
        db.read(|conn| {
            Ok(conn
                .query_row(
                    "SELECT tbl_name FROM sqlite_master WHERE type='index' AND name=?1",
                    [name],
                    |row| row.get(0),
                )
                .optional()?)
        })
        .unwrap()
    }

    #[test]
    fn fresh_collector_creates_schema_and_cursor_row() {
        let path = temp_path("collector");
        let db = open_collector(&path).unwrap();
        let version: u64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT MAX(version) FROM collector_schema_migrations",
                    [],
                    |row| row.get::<_, i64>(0),
                )? as u64)
            })
            .unwrap();
        assert_eq!(version, COLLECTOR_SCHEMA_VERSION);
        let next_seq: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT next_batch_sequence FROM collector_replication_cursors WHERE id = 1",
                    [],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(next_seq, 1);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn rejects_tables_without_migrations_table() {
        let path = temp_path("orphan");
        {
            let db = Db::open(&path).unwrap();
            db.exec_batch("CREATE TABLE stray (x INTEGER)").unwrap();
        }
        assert!(open_primary(&path).is_err());
        assert!(open_collector(&path).is_err());
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn rejects_wrong_version() {
        let path = temp_path("oldver");
        {
            let db = Db::open(&path).unwrap();
            db.exec_batch("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at datetime NOT NULL)").unwrap();
            db.write(|tx| {
                tx.execute(
                    "INSERT INTO schema_migrations (version, applied_at) VALUES (19, '2026-01-01 00:00:00+00:00')",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        }
        assert!(open_primary(&path).is_err());
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }
}
