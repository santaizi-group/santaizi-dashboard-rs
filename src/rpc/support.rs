//! gRPC 处理器的公共辅助：阻塞 DB 访问与 gRPC 状态映射。

use crate::db::Db;

/// 阻塞 DB 读：rusqlite 是同步 API，必须移出 async 上下文。
pub async fn blocking_read<T, F>(db: &Db, f: F) -> anyhow::Result<T>
where
    F: FnOnce(&rusqlite::Connection) -> anyhow::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let db = db.clone();
    tokio::task::spawn_blocking(move || db.read(f)).await?
}

/// 阻塞 DB 写：`BEGIN IMMEDIATE` 事务。
pub async fn blocking_write<T, F>(db: &Db, f: F) -> anyhow::Result<T>
where
    F: FnOnce(&rusqlite::Transaction<'_>) -> anyhow::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let db = db.clone();
    tokio::task::spawn_blocking(move || db.write(f)).await?
}
