//! SQLite 连接池。PRAGMA 与事务策略对齐 Go `service/sqliteutil`：
//! WAL + synchronous=NORMAL + 每连接 busy_timeout=5000；所有写走 BEGIN IMMEDIATE。

use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::ffi::ErrorCode;

/// 池大小与 Go 版一致：最多 8 个连接，至少保留 4 个空闲。
const MAX_OPEN_CONNS: u32 = 8;
const MIN_IDLE_CONNS: u32 = 4;
const BUSY_TIMEOUT_MS: u32 = 5000;

pub type PooledConnection = r2d2::PooledConnection<SqliteConnectionManager>;

#[derive(Clone)]
pub struct Db {
    pool: Pool<SqliteConnectionManager>,
}

impl Db {
    /// 打开（或创建）数据库文件并应用每连接 PRAGMA。
    pub fn open(path: &str) -> anyhow::Result<Self> {
        if let Some(parent) = Path::new(path).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create db dir {}", parent.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
            }
        }
        let manager = SqliteConnectionManager::file(path).with_init(|conn| {
            // database/sql 不会把首连接的 PRAGMA 复制给新连接，必须在每个连接上设置。
            conn.pragma_update(None, "busy_timeout", BUSY_TIMEOUT_MS)?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;
            Ok(())
        });
        let pool = Pool::builder()
            .max_size(MAX_OPEN_CONNS)
            .min_idle(Some(MIN_IDLE_CONNS))
            .connection_timeout(Duration::from_secs(10))
            .build(manager)
            .with_context(|| format!("open sqlite pool {path}"))?;
        // WAL 持久化在文件头：只确认一次。若放进 with_init，min_idle 的并发预热
        // 会同时改 journal_mode 而互撞锁（启动即报 database is locked）。
        pool.get()?.pragma_update(None, "journal_mode", "WAL")?;
        Ok(Self { pool })
    }

    pub fn get(&self) -> anyhow::Result<PooledConnection> {
        self.pool.get().map_err(anyhow::Error::from)
    }

    /// 读操作：从池取连接执行。调用方负责 `spawn_blocking`。
    pub fn read<T, F>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce(&rusqlite::Connection) -> anyhow::Result<T>,
    {
        let conn = self.get()?;
        f(&conn)
    }

    /// 写操作：`BEGIN IMMEDIATE` 事务，遇 BUSY 由 busy_timeout 排队。
    pub fn write<T, F>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce(&rusqlite::Transaction<'_>) -> anyhow::Result<T>,
    {
        let mut conn = self.get()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let out = f(&tx)?;
        tx.commit()?;
        Ok(out)
    }

    /// 便捷：直接执行一段 SQL（建表/维护用）。
    pub fn exec_batch(&self, sql: &str) -> anyhow::Result<()> {
        let conn = self.get()?;
        conn.execute_batch(sql)?;
        Ok(())
    }

    pub fn pool(&self) -> &Pool<SqliteConnectionManager> {
        &self.pool
    }
}

/// SQLITE_BUSY / SQLITE_LOCKED 分类——对齐 Go `sqliteutil.Busy`，
/// 让调用方可以把工作留下轮而不是报错。
pub fn is_busy(err: &rusqlite::Error) -> bool {
    match err {
        rusqlite::Error::SqliteFailure(code, message) => {
            matches!(
                code.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
            ) || message
                .as_deref()
                .map(|message| {
                    let message = message.to_lowercase();
                    message.contains("database is locked")
                        || message.contains("database table is locked")
                })
                .unwrap_or(false)
        }
        _ => false,
    }
}

/// `anyhow` 包装后的 BUSY 分类：后台 worker 用来决定「留下轮」还是报错。
pub fn is_busy_error(err: &anyhow::Error) -> bool {
    err.chain()
        .filter_map(|cause| cause.downcast_ref::<rusqlite::Error>())
        .any(is_busy)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db(name: &str) -> String {
        let dir = std::env::temp_dir().join(format!("santaizi-db-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("test.db").to_string_lossy().to_string()
    }

    #[test]
    fn pragmas_on_every_connection() {
        let path = temp_db("pragmas");
        let db = Db::open(&path).unwrap();
        for _ in 0..3 {
            let busy: u32 = db
                .read(|conn| Ok(conn.pragma_query_value(None, "busy_timeout", |row| row.get(0))?))
                .unwrap();
            assert_eq!(busy, BUSY_TIMEOUT_MS);
            let sync: i64 = db
                .read(|conn| Ok(conn.pragma_query_value(None, "synchronous", |row| row.get(0))?))
                .unwrap();
            assert_eq!(sync, 1); // NORMAL
            let fk: bool = db
                .read(|conn| Ok(conn.pragma_query_value(None, "foreign_keys", |row| row.get(0))?))
                .unwrap();
            assert!(fk);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_tx_commits_and_rolls_back() {
        let path = temp_db("tx");
        let db = Db::open(&path).unwrap();
        db.exec_batch("CREATE TABLE t (v INTEGER)").unwrap();
        db.write(|tx| {
            tx.execute("INSERT INTO t (v) VALUES (1)", [])?;
            Ok(())
        })
        .unwrap();
        let count: i64 = db
            .read(|conn| Ok(conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))?))
            .unwrap();
        assert_eq!(count, 1);

        let failed: anyhow::Result<()> = db.write(|tx| {
            tx.execute("INSERT INTO t (v) VALUES (2)", [])?;
            anyhow::bail!("boom");
        });
        assert!(failed.is_err());
        let count: i64 = db
            .read(|conn| Ok(conn.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))?))
            .unwrap();
        assert_eq!(count, 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn is_busy_classifies() {
        let err = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: ErrorCode::DatabaseBusy,
                extended_code: 5,
            },
            None,
        );
        assert!(is_busy(&err));
        let err = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: ErrorCode::DatabaseLocked,
                extended_code: 6,
            },
            None,
        );
        assert!(is_busy(&err));
        assert!(!is_busy(&rusqlite::Error::QueryReturnedNoRows));
    }
}
