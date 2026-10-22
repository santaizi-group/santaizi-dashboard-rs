//! 存储层：schema DDL、迁移门、rusqlite 连接池。
//!
//! 与 Go 版的对应关系：主库 schema v20（去掉 bot/ddns/nat 相关表与列），
//! 从端库 v7。PRAGMA 与事务策略逐条对齐 `service/sqliteutil`。

pub mod ddl;
pub mod error;
pub mod migrate;
pub mod pool;

pub use error::RepoError;
pub use pool::{Db, is_busy};
