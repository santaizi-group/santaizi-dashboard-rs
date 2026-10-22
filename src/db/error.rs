//! 仓储层错误：把「资源不存在 / 参数非法 / 冲突 / 忙」从 `anyhow` 的字符串堆里
//! 提出来，让上层能映射到正确的 gRPC `Status` 与 HTTP 状态码。
//!
//! 背景：`Db::write` / `Db::read` 的闭包返回 `anyhow::Result<T>`，rpc 层只能用
//! `.map_err(|err| Status::internal(err.to_string()))` 收口。于是业务里精心构造的
//! `Status::permission_denied(...)` 会被 anyhow 包一层，再被 `internal` 拍平——
//! 调用方看到 500 而不是 403，消息还带上一串 `error: ...` 前后缀。
//! `RepoError` 实现 `Into<anyhow::Error>`（保留可 downcast）与
//! `From<RepoError> for tonic::Status`，两侧都能原样取回。

use rusqlite::OptionalExtension;

/// 仓储层可预期错误。`Display` 只输出面向调用方的短句（不含 `error:` 前缀）。
#[derive(Debug, thiserror::Error)]
pub enum RepoError {
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    /// SQLITE_BUSY / SQLITE_LOCKED：调用方可以留下轮。
    #[error("database busy")]
    Busy,
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl RepoError {
    pub fn not_found(what: impl Into<String>) -> Self {
        Self::NotFound(what.into())
    }
    pub fn invalid(what: impl Into<String>) -> Self {
        Self::Invalid(what.into())
    }
    pub fn conflict(what: impl Into<String>) -> Self {
        Self::Conflict(what.into())
    }

    /// 把底层错误按 BUSY 分类：命中即 `Busy`，否则原样保留。
    pub fn from_rusqlite(err: rusqlite::Error) -> Self {
        if super::pool::is_busy(&err) {
            Self::Busy
        } else {
            Self::Db(err)
        }
    }

    /// `OptionalExtension` 的 None → `NotFound`。
    pub fn require<T>(value: Option<T>, what: impl Into<String>) -> Result<T, Self> {
        value.ok_or_else(|| Self::NotFound(what.into()))
    }
}

impl From<RepoError> for tonic::Status {
    fn from(value: RepoError) -> Self {
        match value {
            RepoError::NotFound(message) => tonic::Status::not_found(message),
            RepoError::Invalid(message) => tonic::Status::invalid_argument(message),
            RepoError::Conflict(message) => tonic::Status::failed_precondition(message),
            RepoError::Busy => tonic::Status::unavailable("database busy"),
            RepoError::Db(err) => tonic::Status::internal(err.to_string()),
            RepoError::Other(err) => match err.downcast::<tonic::Status>() {
                Ok(status) => status,
                Err(err) => tonic::Status::internal(err.to_string()),
            },
        }
    }
}

/// HTTP 侧映射：NotFound → 404、Invalid → 400、Conflict → 409、Busy → 503。
impl axum::response::IntoResponse for RepoError {
    fn into_response(self) -> axum::response::Response {
        let (status, code, message) = match self {
            RepoError::NotFound(message) => {
                (axum::http::StatusCode::NOT_FOUND, "not_found", message)
            }
            RepoError::Invalid(message) => (
                axum::http::StatusCode::BAD_REQUEST,
                "invalid_argument",
                message,
            ),
            RepoError::Conflict(message) => (axum::http::StatusCode::CONFLICT, "conflict", message),
            RepoError::Busy => (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "database_busy",
                "服务忙，请稍后重试".to_string(),
            ),
            RepoError::Db(err) => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                err.to_string(),
            ),
            RepoError::Other(err) => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                err.to_string(),
            ),
        };
        crate::api::auth::problem(status, code, &message)
    }
}

/// `anyhow::Result` → `Result<_, RepoError>`：闭包里可以继续用 `?` 传 anyhow，
/// 但显式 `bail!` 出来的业务语义仍会经 `Other` 保留。
pub trait RepoResultExt<T> {
    fn repo(self) -> Result<T, RepoError>;
}

impl<T> RepoResultExt<T> for anyhow::Result<T> {
    fn repo(self) -> Result<T, RepoError> {
        self.map_err(RepoError::Other)
    }
}

impl<T> RepoResultExt<T> for rusqlite::Result<T> {
    fn repo(self) -> Result<T, RepoError> {
        self.map_err(RepoError::from_rusqlite)
    }
}

/// `query_row(...)` 的 `QueryReturnedNoRows` → `RepoError::NotFound`。
pub fn optional_or<T>(value: rusqlite::Result<T>, what: impl Into<String>) -> Result<T, RepoError> {
    match value.optional() {
        Ok(Some(value)) => Ok(value),
        Ok(None) => Err(RepoError::NotFound(what.into())),
        Err(err) => Err(RepoError::from_rusqlite(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_has_no_error_prefix() {
        let err = RepoError::not_found("服务器不存在");
        assert_eq!(err.to_string(), "服务器不存在");
    }

    #[test]
    fn into_status_keeps_semantics() {
        let status: tonic::Status = RepoError::not_found("x").into();
        assert_eq!(status.code(), tonic::Code::NotFound);
        let status: tonic::Status = RepoError::invalid("y").into();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let status: tonic::Status = RepoError::conflict("z").into();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        let status: tonic::Status = RepoError::Busy.into();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn inner_status_survives_other_wrapping() {
        // 业务闭包里 bail 出的 Status 不能被 `internal` 拍平。
        let err = RepoError::Other(anyhow::anyhow!(tonic::Status::permission_denied(
            "telemetry batch node mismatch"
        )));
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(status.message(), "telemetry batch node mismatch");
    }

    #[test]
    fn busy_is_classified() {
        let err = RepoError::from_rusqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ffi::ErrorCode::DatabaseBusy,
                extended_code: 5,
            },
            None,
        ));
        assert!(matches!(err, RepoError::Busy));
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }
}
