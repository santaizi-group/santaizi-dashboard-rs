//! HTTP 认证层：对齐 Go `pkg/mygin` 的 `Authorize` / `CSRF` 与
//! `cmd/dashboard/controller/api_v2_spa.go` 的会话语义。
//!
//! 会话来源两条：Cookie（`site.cookie_name` → `users.token`）与 Bearer
//! API Token（内存索引）。CSRF 为 HMAC 令牌：非安全方法且未带
//! `Authorization` 时必须提交与 Cookie 同值的 `X-CSRF-Token`。

use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use hmac::{Hmac, KeyInit as _, Mac as _};
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::db::Db;
use crate::repo::users::UserRow;
use crate::rpc::support::blocking_read;
use crate::util::time as util_time;

use super::tokens::ApiTokenIndex;

type HmacSha256 = Hmac<Sha256>;

/// CSRF Cookie 存活期（Go `csrfTokenAge`：24h）。
const CSRF_TOKEN_AGE_SECS: i64 = 60 * 60 * 24;

/// 安全方法不校验 CSRF（Go `isSafeMethod`）。
fn is_safe_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
    )
}

/// 当前请求的 CSRF 令牌（Go `ctxKeyCSRFToken`）：中间件写入，处理器读取。
#[derive(Debug, Clone)]
pub struct CsrfToken(pub String);

/// 认证通过的用户上下文（Go `CtxKeyAuthorizedUser` + `CtxKeyIsAPI`）。
#[derive(Debug, Clone)]
pub struct CurrentUser {
    pub user: UserRow,
    /// Bearer API Token 来源（Cookie 会话为 false）。
    pub is_api: bool,
    /// API Token 权限（`read` / `write`）；Cookie 会话为空。
    pub api_permission: Option<String>,
}

/// API 共享状态：认证层与处理器共用。
#[derive(Clone)]
pub struct ApiShared {
    pub db: Db,
    pub tokens: Arc<ApiTokenIndex>,
    /// CSRF HMAC 密钥（进程级随机，Go `csrfSecret`）。
    pub csrf_secret: [u8; 32],
    pub cookie_name: String,
    pub version: String,
    /// 站点配置（settings PATCH 后热更新；与 `AppState` 共享同一句柄）。
    pub conf: crate::config::ConfigHandle,
    /// OAuth2 登录 state 缓存（每路由器一份，测试互不干扰）。
    pub oauth2_states: Arc<super::oauth2::Oauth2StateStore>,
    /// `client_secret → server_id` 索引（与 `AppState` 共享同一实例；
    /// admin 建号/重置/删除后热更新）。
    pub secrets: Arc<crate::repo::servers::SecretIndex>,
    /// 数据库维护器（/admin/database 消费；未接线为 None）。
    pub maintainer: Option<Arc<crate::telemetry::maintenance::DatabaseMaintainer>>,
    /// 通知引擎（test 端点与语言切换共用）。
    pub notifier: Option<Arc<crate::notify::Notifier>>,
}

impl ApiShared {
    /// 当前配置快照（读侧一次锁定、请求内一致）。
    pub fn conf(&self) -> Arc<crate::config::Config> {
        self.conf.read().expect("config lock").clone()
    }
}

/// 从请求读 Cookie 值（取第一个匹配项）。
pub fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    for pair in raw.split(';') {
        let (key, value) = pair.trim().split_once('=')?;
        if key == name {
            return Some(value.to_string());
        }
    }
    None
}

/// 种 Cookie：`Secure` 按代理头判定（Go `c.SetCookie` 的 secure 参数）；
/// CSRF Cookie 等进程内自签场景传 `false`。
pub fn set_cookie(
    response: &mut Response,
    name: &str,
    value: &str,
    max_age: i64,
    http_only: bool,
    secure: bool,
) {
    let cookie = format!(
        "{name}={value}; Path=/; Max-Age={max_age}; SameSite=Lax{}{}",
        if http_only { "; HttpOnly" } else { "" },
        if secure { "; Secure" } else { "" },
    );
    if let Ok(value) = HeaderValue::from_str(&cookie) {
        response.headers_mut().append(header::SET_COOKIE, value);
    }
}

fn csrf_cookie_name(cookie_name: &str) -> String {
    format!("{cookie_name}-csrf")
}

/// CSRF 会话标识：会话 Cookie 与查看密码 Cookie 的组合（Go `csrfSessionID`）。
fn csrf_session_id(headers: &HeaderMap, cookie_name: &str) -> String {
    let auth = cookie_value(headers, cookie_name).unwrap_or_default();
    let vp = cookie_value(headers, &format!("{cookie_name}-vp")).unwrap_or_default();
    format!("{auth}|{vp}")
}

fn csrf_signature(secret: &[u8], session_id: &str, nonce: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("hmac key");
    mac.update(session_id.as_bytes());
    mac.update(&[0]);
    mac.update(nonce.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

fn new_csrf_token(secret: &[u8], session_id: &str) -> String {
    let mut nonce = [0u8; 32];
    rand::Rng::fill_bytes(&mut rand::rng(), &mut nonce);
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(nonce);
    format!(
        "{}.{}",
        encoded,
        csrf_signature(secret, session_id, &encoded)
    )
}

/// 校验已呈现令牌：nonce.signature 且签名与当前会话匹配（Go `validCSRFToken`）。
fn csrf_token_valid(token: &str, secret: &[u8], session_id: &str) -> bool {
    let Some((nonce, signature)) = token.split_once('.') else {
        return false;
    };
    if nonce.is_empty() || signature.is_empty() {
        return false;
    }
    if base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(nonce)
        .is_err()
    {
        return false;
    }
    let expected = csrf_signature(secret, session_id, nonce);
    signature.as_bytes().ct_eq(expected.as_bytes()).into()
}

/// 当前请求的 CSRF 令牌：Cookie 有效则复用，否则新签（Go `currentCSRFToken`）。
fn current_csrf_token(headers: &HeaderMap, shared: &ApiShared, session_id: &str) -> String {
    if let Some(existing) = cookie_value(headers, &csrf_cookie_name(&shared.cookie_name))
        && csrf_token_valid(&existing, &shared.csrf_secret, session_id)
    {
        return existing;
    }
    new_csrf_token(&shared.csrf_secret, session_id)
}

/// problem+json 错误体（Go `writeV2Problem`）。
pub fn problem(status: StatusCode, code: &str, detail: &str) -> Response {
    let body = serde_json::json!({
        "type": format!("https://santaizi.dev/problems/{code}"),
        "title": status.canonical_reason().unwrap_or("Error"),
        "status": status.as_u16(),
        "code": code,
        "detail": detail,
    });
    let mut response = Response::new(serde_json::to_string(&body).unwrap_or_default().into());
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/problem+json"),
    );
    response
}

/// 数据信封（Go `writeV2Data`）。
pub fn data_json(value: serde_json::Value) -> Response {
    let body = serde_json::json!({ "data": value });
    let mut response = Response::new(serde_json::to_string(&body).unwrap_or_default().into());
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

/// 列表信封（Go `writeV2List`）：`data` + 分页 meta，**不再**外包一层。
pub fn list_json(data: serde_json::Value, total: i64) -> Response {
    list_json_paged(data, 1, total, total)
}

/// 带真实分页参数的列表信封（admin 列表用）。
pub fn list_json_paged(data: serde_json::Value, page: i64, page_size: i64, total: i64) -> Response {
    let body = serde_json::json!({
        "data": data,
        "meta": { "page": page, "page_size": page_size, "total": total },
    });
    let mut response = Response::new(serde_json::to_string(&body).unwrap_or_default().into());
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

/// 认证 + CSRF 中间件（Go `r.Use(CSRF, Authorize)` 的合并等价物）。
pub async fn authenticate(
    axum::extract::State(shared): axum::extract::State<ApiShared>,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    let session_id = csrf_session_id(request.headers(), &shared.cookie_name);
    let csrf_token = current_csrf_token(request.headers(), &shared, &session_id);

    // CSRF：非安全方法且非 Bearer 请求必须提交匹配令牌。
    let has_bearer = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().starts_with("bearer "));
    if !is_safe_method(request.method()) && !has_bearer {
        let submitted = request
            .headers()
            .get("X-CSRF-Token")
            .or_else(|| request.headers().get("X-XSRF-Token"))
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if submitted.is_empty()
            || !csrf_token_valid(submitted, &shared.csrf_secret, &session_id)
            || submitted != csrf_token
        {
            return problem(StatusCode::FORBIDDEN, "csrf_invalid", "CSRF token invalid");
        }
    }

    let current = resolve_user(&shared, request.headers()).await;
    if let Some(user) = current {
        request.extensions_mut().insert(user);
    }
    request
        .extensions_mut()
        .insert(CsrfToken(csrf_token.clone()));

    let mut response = next.run(request).await;
    // CSRF Cookie 每次刷新（HttpOnly=false，前端可读）。
    set_cookie(
        &mut response,
        &csrf_cookie_name(&shared.cookie_name),
        &csrf_token,
        CSRF_TOKEN_AGE_SECS,
        false,
        false,
    );
    response
}

/// 会话解析：Cookie 优先，Bearer 次之（Go 顺序：先 Cookie 后 API）。
async fn resolve_user(shared: &ApiShared, headers: &HeaderMap) -> Option<CurrentUser> {
    let now_nanos = util_time::now_nanos();
    if let Some(token) = cookie_value(headers, &shared.cookie_name)
        && !token.is_empty()
    {
        let user = blocking_read(&shared.db, move |conn| {
            crate::repo::users::find_by_token(conn, &token, now_nanos)
        })
        .await
        .ok()
        .flatten();
        if let Some(user) = user {
            return Some(CurrentUser {
                user,
                is_api: false,
                api_permission: None,
            });
        }
    }
    let bearer = headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .to_string();
    let bearer = bearer.trim().to_string();
    let token = bearer
        .get(..7)
        .filter(|prefix| prefix.eq_ignore_ascii_case("bearer "))
        .map(|_| bearer[7..].trim().to_string())?;
    if token.is_empty() {
        return None;
    }
    let auth = shared.tokens.authenticate(&token, now_nanos)?;
    let user = blocking_read(&shared.db, move |conn| {
        crate::repo::users::find_by_id(conn, auth.user_id)
    })
    .await
    .ok()
    .flatten()?;
    Some(CurrentUser {
        user,
        is_api: true,
        api_permission: Some(auth.permission),
    })
}

impl<S> FromRequestParts<S> for CurrentUser
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<CurrentUser>()
            .cloned()
            .ok_or_else(|| {
                problem(
                    StatusCode::UNAUTHORIZED,
                    "authentication_required",
                    "管理员会话已过期或 API Token 无效",
                )
            })
    }
}

/// 可选当前用户（公开端点用）。
pub struct OptionalUser(pub Option<CurrentUser>);

impl<S> FromRequestParts<S> for OptionalUser
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self(parts.extensions.get::<CurrentUser>().cloned()))
    }
}

/// 管理端守卫（Go `Authorize(MemberOnly)` + `v2RequireAdmin` +
/// `RejectReadOnlyAPITokenWrites`）：member 会话或 API Token；
/// read-only Token 的写方法 403。
pub struct AdminUser(pub CurrentUser);

impl<S> FromRequestParts<S> for AdminUser
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let current = parts
            .extensions
            .get::<CurrentUser>()
            .cloned()
            .ok_or_else(|| {
                problem(
                    StatusCode::UNAUTHORIZED,
                    "authentication_required",
                    "管理员会话已过期或 API Token 无效",
                )
            })?;
        if current.is_api
            && current.api_permission.as_deref() == Some("read")
            && !is_safe_method(&parts.method)
        {
            return Err(problem(
                StatusCode::FORBIDDEN,
                "read_only_api_token",
                "只读 API Token 不允许执行写操作",
            ));
        }
        Ok(Self(current))
    }
}

impl IntoResponse for CurrentUser {
    fn into_response(self) -> Response {
        unreachable!("CurrentUser is an extractor, never rendered directly")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csrf_token_round_trip_and_tamper_rejection() {
        let secret = [7u8; 32];
        let session_id = "session-token|";
        let token = new_csrf_token(&secret, session_id);
        assert!(csrf_token_valid(&token, &secret, session_id));
        // 换会话（登录态变化）即失效。
        assert!(!csrf_token_valid(&token, &secret, "other|"));
        // 改签名 / 改 nonce 都拒。
        let (nonce, _) = token.split_once('.').unwrap();
        let forged = format!("{nonce}.{}", "A".repeat(43));
        assert!(!csrf_token_valid(&forged, &secret, session_id));
        assert!(!csrf_token_valid("no-dot", &secret, session_id));
        assert!(!csrf_token_valid("", &secret, session_id));
    }
}
