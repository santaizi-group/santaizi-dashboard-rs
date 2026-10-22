//! Public v2 端点：对齐 Go `api_v2_spa.go` 的 `v2PublicBootstrap` /
//! `v2ViewPasswordSession` / `v2PublicServers` / `v2PublicServer`。
//!
//! 访问门槛（Go `requireV2PublicAccess`）：站点未配查看密码放行；
//! 否则管理员会话或 `-vp` Cookie 二者有一即可。匿名与查看会话的
//! `host` 不含公网 IP，仅 Bearer Token 展开（Go `publicHostView`）。

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use hmac::{Hmac, KeyInit as _, Mac as _};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::config;
use crate::repo::servers::PublicServerRow;
use crate::rpc::support::blocking_read;
use crate::util::time as util_time;

use super::auth::{
    ApiShared, CsrfToken, CurrentUser, OptionalUser, cookie_value, data_json, list_json, problem,
};
use super::presentation;

type HmacSha256 = Hmac<Sha256>;

/// 查看密码会话 Cookie 的消息常量（HMAC 消息，非秘密）。
const VIEW_PASSWORD_MESSAGE: &[u8] = b"santaizi-view-password-session";

/// `-vp` Cookie 有效期（Go：86400s）。
const VIEW_PASSWORD_COOKIE_MAX_AGE: i64 = 24 * 60 * 60;

fn vp_cookie_name(cookie_name: &str) -> String {
    format!("{cookie_name}-vp")
}

/// 查看密码会话值：`HMAC-SHA256(key=查看密码, msg=常量)`。
///
/// Go 存 `bcrypt(查看密码)` 并由 bcrypt 比对；这里改用密钥化摘要——
/// 同一 Cookie 自验证、重启不失效，且不必引入 bcrypt 依赖。
/// 该 Cookie 只由本进程签发与校验，格式无需与 Go 兼容。
fn view_password_token(view_password: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(view_password.as_bytes()).expect("hmac key");
    mac.update(VIEW_PASSWORD_MESSAGE);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

/// `-vp` Cookie 是否与当前查看密码匹配（Go `ValidateViewPassword`）。
pub(crate) fn view_password_verified(headers: &HeaderMap, shared: &ApiShared) -> bool {
    let view_password = shared.conf().site.view_password.clone();
    if view_password.is_empty() {
        return false;
    }
    let expected = view_password_token(&view_password);
    cookie_value(headers, &vp_cookie_name(&shared.cookie_name))
        .is_some_and(|value| value.as_bytes().ct_eq(expected.as_bytes()).into())
}

/// Go `safeAssetURL`：只信 `/static/` 与 data URL，其余回退默认值。
pub(crate) fn safe_asset_url(value: &str, fallback: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        return fallback.to_string();
    }
    if value.starts_with("/static/") || value.starts_with("data:image/") {
        return value.to_string();
    }
    fallback.to_string()
}

/// 公开访问门槛；不通过时返回 403 problem（Go `requireV2PublicAccess`）。
/// `Err` 装箱：`Response` 较大，避免 `result_large_err`。
pub(crate) fn require_public_access(
    headers: &HeaderMap,
    shared: &ApiShared,
    user: Option<&CurrentUser>,
) -> Result<(), Box<Response>> {
    if shared.conf().site.view_password.is_empty() || user.is_some() {
        return Ok(());
    }
    if view_password_verified(headers, shared) {
        return Ok(());
    }
    Err(Box::new(problem(
        StatusCode::FORBIDDEN,
        "view_password_required",
        "公开状态页需要查看密码",
    )))
}

/// `GET /api/v2/public/bootstrap`：站点引导信息（Go `v2PublicBootstrap`）。
async fn bootstrap(
    State(shared): State<ApiShared>,
    OptionalUser(user): OptionalUser,
    axum::Extension(CsrfToken(csrf_token)): axum::Extension<CsrfToken>,
    headers: HeaderMap,
) -> Response {
    let conf = shared.conf();
    let member = user.is_some();
    let verified = member || view_password_verified(&headers, &shared);
    data_json(json!({
        "brand": conf.site.brand,
        "locale": conf.language,
        "version": shared.version,
        "csrf_token": csrf_token,
        "logo_url": safe_asset_url(&conf.site.logo_url, "/static/logo.svg"),
        "footer_text": conf.site.footer_text,
        "primary_color": conf.site.primary_color,
        // 写路径已按白名单校验；这里再兜一层——配置文件可被手工编辑，
        // 而这份 CSS 是 `style.textContent` 直接注入公开站的。
        "custom_css": if super::admin_settings::css_is_safe(&conf.site.safe_custom_css) {
            conf.site.safe_custom_css.as_str()
        } else {
            ""
        },
        "requires_view_password": !conf.site.view_password.is_empty(),
        "view_password_verified": verified,
        "show_availability": conf.show_availability_to_guest,
        "authenticated": member,
        "theme": config::normalize_public_theme(&conf.site.theme),
        "allow_frontend_theme_switch": !conf.disable_switch_template_in_frontend,
    }))
}

#[derive(Deserialize, Default)]
struct ViewPasswordRequest {
    #[serde(default)]
    password: String,
}

/// `POST /api/v2/public/view-password/session`：校验查看密码并下发
/// `-vp` Cookie（Go `v2ViewPasswordSession`）。
///
/// 查看密码是站点级单值、无账号可锁，失败不限速等于裸奔的爆破面：
/// 全局桶按尝试速率压住（密钥只有一个，桶也只需要一个）。
async fn view_password_session(
    State(shared): State<ApiShared>,
    body: axum::body::Bytes,
) -> Response {
    let request: ViewPasswordRequest = match serde_json::from_slice::<ViewPasswordRequest>(&body) {
        Ok(request) if !request.password.is_empty() => request,
        _ => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "password 不能为空",
            );
        }
    };
    // 节流键与查看密码本身无关：锁的是「这个站点的这道门」。
    const THROTTLE_KEY: &str = "view-password";
    if let Err(retry) = shared.throttle.check(THROTTLE_KEY) {
        let mut response = problem(
            StatusCode::TOO_MANY_REQUESTS,
            "too_many_attempts",
            "尝试过于频繁，请稍后重试",
        );
        if let Ok(value) = header::HeaderValue::from_str(&retry.seconds().to_string()) {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
        return response;
    }
    if request.password != shared.conf().site.view_password {
        shared.throttle.record_failure(THROTTLE_KEY);
        return problem(
            StatusCode::FORBIDDEN,
            "invalid_view_password",
            "查看密码不正确",
        );
    }
    shared.throttle.record_success(THROTTLE_KEY);
    let mut response = StatusCode::NO_CONTENT.into_response();
    let cookie = format!(
        "{}={}; Path=/; Max-Age={VIEW_PASSWORD_COOKIE_MAX_AGE}; SameSite=Lax; HttpOnly",
        vp_cookie_name(&shared.cookie_name),
        view_password_token(&shared.conf().site.view_password)
    );
    if let Ok(value) = header::HeaderValue::from_str(&cookie) {
        response.headers_mut().append(header::SET_COOKIE, value);
    }
    response
}

/// 快照查询的一次性封装：门 + 取行（共享给 list/detail 与扩展端点）。
pub(crate) async fn snapshot(
    headers: &HeaderMap,
    shared: &ApiShared,
    user: Option<&CurrentUser>,
) -> Result<Vec<PublicServerRow>, Box<Response>> {
    require_public_access(headers, shared, user)?;
    let member = user.is_some();
    let verified = view_password_verified(headers, shared);
    let now_nanos = util_time::now_nanos();
    let rows = blocking_read(&shared.db, move |conn| {
        crate::repo::servers::list_public(conn, member || verified, now_nanos)
    })
    .await;
    match rows {
        Ok(rows) => Ok(rows),
        Err(err) => {
            tracing::warn!(error = %err, "public server snapshot query failed");
            Err(Box::new(problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "查询服务器失败",
            )))
        }
    }
}

/// 行 → JSON（Go `publicServerSnapshot` 单行形态；ws 帧复用）。
pub(crate) fn server_json(
    row: &PublicServerRow,
    presentation: &presentation::RuntimePresentation,
    token_full: bool,
    offline_threshold_seconds: u64,
) -> Value {
    let mut item = json!({
        "id": row.id,
        "name": row.name,
        "tag": row.tag,
        "public_note": presentation::decode_public_note(&row.public_note),
        "display_index": row.display_index,
        "hide_for_guest": row.hide_for_guest,
        "host": presentation::host_json(row.host_payload.as_deref(), token_full),
        "state": presentation::state_json(row.state_payload.as_deref()),
        "last_active": row
            .last_received_at
            .filter(|nanos| *nanos > 0)
            .map(util_time::unix_nanos_to_rfc3339)
            .unwrap_or_default(),
        "online": presentation::online_flag(presentation, offline_threshold_seconds),
        "telemetry": presentation::telemetry_json(presentation),
    });
    if token_full && let Some(object) = item.as_object_mut() {
        object.insert("note".to_string(), json!(row.note));
        object.insert(
            "monitoring_options".to_string(),
            serde_json::from_str::<Value>(&row.monitoring_options_raw)
                .unwrap_or_else(|_| json!({})),
        );
    }
    item
}

/// `GET /api/v2/public/servers`：公开服务器列表（Go `v2PublicServers`）。
async fn public_servers(
    State(shared): State<ApiShared>,
    OptionalUser(user): OptionalUser,
    headers: HeaderMap,
) -> Response {
    let token_full = user.as_ref().is_some_and(|current| current.is_api);
    let offline_threshold_seconds = shared.conf().telemetry.offline_threshold_seconds;
    let rows = match snapshot(&headers, &shared, user.as_ref()).await {
        Ok(rows) => rows,
        Err(response) => return *response,
    };
    let items: Vec<Value> = rows
        .iter()
        .map(|row| {
            let presentation = presentation::runtime_presentation(row);
            server_json(row, &presentation, token_full, offline_threshold_seconds)
        })
        .collect();
    let total = items.len() as i64;
    list_json(serde_json::Value::Array(items), total)
}

/// `GET /api/v2/public/servers/{serverId}`：公开服务器详情（Go `v2PublicServer`）。
async fn public_server(
    State(shared): State<ApiShared>,
    Path(server_id): Path<i64>,
    OptionalUser(user): OptionalUser,
    headers: HeaderMap,
) -> Response {
    if server_id <= 0 {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_server_id",
            "服务器 ID 无效",
        );
    }
    let token_full = user.as_ref().is_some_and(|current| current.is_api);
    let offline_threshold_seconds = shared.conf().telemetry.offline_threshold_seconds;
    let rows = match snapshot(&headers, &shared, user.as_ref()).await {
        Ok(rows) => rows,
        Err(response) => return *response,
    };
    match rows.iter().find(|row| row.id == server_id) {
        Some(row) => {
            let presentation = presentation::runtime_presentation(row);
            data_json(server_json(
                row,
                &presentation,
                token_full,
                offline_threshold_seconds,
            ))
        }
        None => problem(StatusCode::NOT_FOUND, "server_not_found", "服务器不存在"),
    }
}

/// public 路由组（Go `v2Public*` 注册）。
pub fn routes() -> axum::Router<ApiShared> {
    axum::Router::new()
        .route("/api/v2/public/bootstrap", axum::routing::get(bootstrap))
        .route(
            "/api/v2/public/view-password/session",
            axum::routing::post(view_password_session),
        )
        .route("/api/v2/public/servers", axum::routing::get(public_servers))
        .route(
            "/api/v2/public/servers/{serverId}",
            axum::routing::get(public_server),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_password_token_is_stable_and_keyed() {
        assert_eq!(view_password_token("s3cret"), view_password_token("s3cret"));
        assert_ne!(view_password_token("s3cret"), view_password_token("other"));
        assert!(!view_password_token("s3cret").is_empty());
    }

    #[test]
    fn safe_asset_url_only_trusts_static_and_data() {
        assert_eq!(safe_asset_url("", "/static/logo.svg"), "/static/logo.svg");
        assert_eq!(safe_asset_url("  ", "/static/logo.svg"), "/static/logo.svg");
        assert_eq!(
            safe_asset_url("/static/brand.png", "/static/logo.svg"),
            "/static/brand.png"
        );
        assert_eq!(
            safe_asset_url("data:image/png;base64,AAA", "/static/logo.svg"),
            "data:image/png;base64,AAA"
        );
        assert_eq!(
            safe_asset_url("https://evil.example/x.png", "/static/logo.svg"),
            "/static/logo.svg"
        );
    }
}
