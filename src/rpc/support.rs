//! gRPC 处理器的公共辅助：阻塞 DB 访问与 gRPC 状态映射。

use crate::db::{Db, RepoError};

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

/// 阻塞 DB 写，错误带分类：闭包里 `bail!` 出的 `tonic::Status` 会经
/// `RepoError::Other` 原样穿过，最外层映射不再把它拍成 `internal`。
pub async fn blocking_write_repo<T, F>(db: &Db, f: F) -> Result<T, RepoError>
where
    F: FnOnce(&rusqlite::Transaction<'_>) -> Result<T, RepoError> + Send + 'static,
    T: Send + 'static,
{
    let db = db.clone();
    tokio::task::spawn_blocking(move || db.write_repo(f))
        .await
        .map_err(|err| RepoError::Other(anyhow::Error::from(err)))?
}

/// 阻塞 DB 读，错误带分类。
pub async fn blocking_read_repo<T, F>(db: &Db, f: F) -> Result<T, RepoError>
where
    F: FnOnce(&rusqlite::Connection) -> Result<T, RepoError> + Send + 'static,
    T: Send + 'static,
{
    let db = db.clone();
    tokio::task::spawn_blocking(move || db.read_repo(f))
        .await
        .map_err(|err| RepoError::Other(anyhow::Error::from(err)))?
}

/// `Result<_, RepoError>` → `Result<_, tonic::Status>`：rpc handler 统一收口。
pub fn status_from<T>(result: Result<T, RepoError>) -> Result<T, tonic::Status> {
    result.map_err(tonic::Status::from)
}
