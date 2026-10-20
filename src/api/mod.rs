//! HTTP API（Phase 5）：axum 路由 + v2 端点 + 内嵌前端托管。
//!
//! 对齐 Go `cmd/dashboard/controller`：`/api/v2/*` 三组（auth / public /
//! admin），`/static/*` 与 SPA 回退来自 `resource/static`（rust-embed），
//! 未命中返回 problem+json 404。本刀落地认证组与托管骨架；public /
//! admin 业务端点按后续刀追加。

pub mod admin_collectors;
pub mod admin_insights;
pub mod admin_resources;
pub mod admin_servers;
pub mod admin_settings;
pub mod auth;
pub mod oauth2;
pub mod presentation;
pub mod public;
pub mod public_extra;
pub mod tokens;
pub mod ws;

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tokio_util::sync::CancellationToken;

use crate::state::AppState;
use crate::util::time as util_time;

use auth::{ApiShared, OptionalUser, data_json};

/// 内嵌前端产物（Go `resource.WebFS`）：`resource/web/{admin,status,api-docs}`。
#[derive(rust_embed::Embed)]
#[folder = "resource/web/"]
struct WebAssets;

/// 内嵌公开静态资源（Go `resource.StaticFS` 的嵌入半边，不含 `custom/`）。
#[derive(rust_embed::Embed)]
#[folder = "resource/static/"]
struct StaticAssets;

/// 从指定 app 子目录取资产；目录穿越已由 rust-embed 归一化处理。
fn web_asset(app: &str, path: &str, cache: &'static str) -> Option<Response> {
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        return None;
    }
    let key = format!("{app}/{path}");
    asset_response_key(&key, cache)
}

fn asset_response_key(key: &str, cache: &'static str) -> Option<Response> {
    let asset = WebAssets::get(key)?;
    let content_type = content_type(key);
    let mut response = Response::new(Body::from(asset.data.into_owned()));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type.parse().unwrap());
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, cache.parse().unwrap());
    Some(response)
}

/// 常见 Web 资产的内容类型（后端只服务内嵌前端，够用即止）。
fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "map" => "application/json; charset=utf-8",
        "txt" => "text/plain; charset=utf-8",
        "yaml" | "yml" => "application/yaml; charset=utf-8",
        _ => "application/octet-stream",
    }
}

/// SPA 应用托管（Go `serveWebApp`）：命中资产直出，未命中回 app 的
/// index.html（前端路由）。
fn static_asset(path: &str) -> Option<Response> {
    let path = path.trim_start_matches('/');
    if path.is_empty() || path.contains("..") {
        return None;
    }
    let asset = StaticAssets::get(path)?;
    let content_type = content_type(path);
    let mut response = Response::new(Body::from(asset.data.into_owned()));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type.parse().unwrap());
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "public, max-age=3600".parse().unwrap(),
    );
    Some(response)
}

async fn spa_app(app: &str, path: &str, assets_only: bool) -> Response {
    let app = app.trim_matches('/');
    if app != "admin" && app != "status" && app != "api-docs" {
        return problem(StatusCode::NOT_FOUND, "route_not_found", "请求的路由不存在");
    }
    let path = path.trim_start_matches('/');
    if !path.is_empty()
        && let Some(asset) = web_asset(app, path, "public, max-age=31536000, immutable")
    {
        return asset;
    }
    if assets_only {
        return problem(StatusCode::NOT_FOUND, "route_not_found", "请求的路由不存在");
    }
    asset_response_key(&format!("{app}/index.html"), "no-cache").unwrap_or_else(spa_fallback)
}

/// 纯状态壳（内嵌产物缺失时的兜底，Phase 7 前的占位路径）。
fn spa_fallback() -> Response {
    let mut response = Response::new(Body::from(
        &b"<!doctype html><title>Santaizi</title><meta charset=utf-8>"[..],
    ));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        "text/html; charset=utf-8".parse().unwrap(),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-cache".parse().unwrap());
    response
}

const OPENAPI_YAML: &str = include_str!("../../resource/openapi/v2.yaml");

/// `GET /openapi/v2.yaml`：内嵌 OpenAPI 契约（Go `serveOpenAPIYAML`）。
async fn openapi_yaml() -> Response {
    let mut response = Response::new(Body::from(OPENAPI_YAML));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/yaml; charset=utf-8".parse().unwrap(),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "public, max-age=300".parse().unwrap(),
    );
    response
}

/// `GET /openapi/v2.json`：同一份契约的 JSON（Go `serveOpenAPIJSON`）。
async fn openapi_json() -> Response {
    let value: serde_json::Value = match serde_yaml::from_str(OPENAPI_YAML) {
        Ok(value) => value,
        Err(err) => {
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "openapi_conversion_failed",
                &err.to_string(),
            );
        }
    };
    let encoded = match serde_json::to_vec(&value) {
        Ok(encoded) => encoded,
        Err(err) => {
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "openapi_conversion_failed",
                &err.to_string(),
            );
        }
    };
    let mut response = Response::new(Body::from(encoded));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/json; charset=utf-8".parse().unwrap(),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "public, max-age=300".parse().unwrap(),
    );
    response
}

/// SPA 外壳：前端路由（`/admin/*`、`/`、`/service` 等）一律回 index.html。
/// Phase 7 前为占位页面；未内嵌任何资产时返回同一外壳。
async fn spa_shell(request: axum::extract::Request) -> Response {
    // `/api/*` 未注册路径按 Go NoRoute 语义返回 404 problem+json，
    // 只有前端路由回落到 SPA 外壳。
    if request.uri().path().starts_with("/api/") {
        return problem(StatusCode::NOT_FOUND, "route_not_found", "请求的路由不存在");
    }
    asset_response_key("status/index.html", "no-cache").unwrap_or_else(spa_fallback)
}

/// `GET /api/v2/auth/session`：当前会话 + CSRF 令牌（Go `v2Session`）。
async fn get_session(
    State(shared): State<ApiShared>,
    OptionalUser(current): OptionalUser,
    axum::Extension(auth::CsrfToken(csrf_token)): axum::Extension<auth::CsrfToken>,
) -> Response {
    let authenticated = current.is_some();
    let mut state = serde_json::json!({
        "authenticated": authenticated,
        "csrf_token": csrf_token,
        "login_url": "/oauth2/login",
        "capabilities": [],
        "version": shared.version,
    });
    if let Some(current) = current {
        let capabilities = if current.is_api && current.api_permission.as_deref() == Some("read") {
            serde_json::json!([
                "servers:read",
                "monitors:read",
                "notifications:read",
                "network:read",
                "telemetry:read",
                "settings:read"
            ])
        } else {
            serde_json::json!([
                "servers:write",
                "monitors:write",
                "notifications:write",
                "network:write",
                "telemetry:write",
                "settings:write"
            ])
        };
        state["capabilities"] = capabilities;
        state["user"] = serde_json::json!({
            "id": current.user.id,
            "login": current.user.login,
            "name": current.user.name,
            "avatar_url": current.user.avatar_url,
            "super_admin": current.user.super_admin,
        });
    }
    data_json(state)
}

/// `POST /api/v2/auth/logout`：清会话 Cookie 与库内令牌（Go `v2Logout`）。
async fn logout(State(shared): State<ApiShared>, OptionalUser(user): OptionalUser) -> Response {
    if let Some(current) = user {
        let now = util_time::now_nanos();
        let id = current.user.id;
        let _ = crate::rpc::support::blocking_write(&shared.db, move |tx| {
            tx.execute(
                "UPDATE users SET token = '', token_expired = datetime(?1, 'unixepoch'), updated_at = datetime(?1, 'unixepoch') WHERE id = ?2",
                rusqlite::params![now / 1_000_000_000, id],
            )?;
            Ok(())
        })
        .await;
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    let cookie = format!(
        "{}=; Path=/; Max-Age=-1; SameSite=Lax; HttpOnly",
        shared.cookie_name
    );
    response
        .headers_mut()
        .append(header::SET_COOKIE, cookie.parse().unwrap());
    response
}

/// 探测端点：HTTP 面就绪信号（负载均衡 / 部署脚本用）。
async fn healthz() -> Response {
    let mut response = Response::new(Body::from(&b"ok"[..]));
    *response.status_mut() = StatusCode::OK;
    response
}

fn problem(status: StatusCode, code: &str, detail: &str) -> Response {
    auth::problem(status, code, detail)
}

/// 构建路由。`ApiShared` 既作中间件状态又作处理器状态。
pub fn router(state: Arc<AppState>) -> Router {
    let shared = ApiShared {
        db: state.db().clone(),
        tokens: Arc::new(tokens::ApiTokenIndex::load(state.db()).unwrap_or_default()),
        csrf_secret: rand::random(),
        cookie_name: state.conf().site.cookie_name.clone(),
        version: crate::VERSION.to_string(),
        conf: state.conf_handle(),
        oauth2_states: Arc::new(oauth2::Oauth2StateStore::default()),
        secrets: state.secrets_arc(),
        maintainer: state.maintainer(),
        notifier: Some(state.notifier()),
    };
    Router::new()
        .route("/api/v2/auth/session", get(get_session))
        .route("/api/v2/auth/logout", axum::routing::post(logout))
        .merge(public::routes())
        .merge(public_extra::routes())
        .merge(admin_servers::routes())
        .merge(admin_settings::routes())
        .merge(admin_collectors::routes())
        .merge(admin_insights::routes())
        .merge(admin_resources::routes())
        .merge(ws::routes())
        // 页面级登录流：须在 SPA fallback 之前注册。
        .merge(oauth2::routes())
        .route("/healthz", get(healthz))
        .route("/openapi/v2.yaml", get(openapi_yaml))
        .route("/openapi/v2.json", get(openapi_json))
        .route(
            "/static/{*path}",
            get(|Path(path): Path<String>| async move {
                static_asset(&path).unwrap_or_else(|| {
                    problem(StatusCode::NOT_FOUND, "route_not_found", "请求的路由不存在")
                })
            }),
        )
        .route(
            "/admin",
            get(|| async { axum::response::Redirect::permanent("/admin/") }),
        )
        .route(
            "/docs/api",
            get(|| async { axum::response::Redirect::permanent("/docs/api/") }),
        )
        // 通配 `{*path}` 不匹配空路径，根路径必须显式注册，否则回落到公开站外壳。
        .route(
            "/docs/api/",
            get(|| async { spa_app("api-docs", "", false).await }),
        )
        .route(
            "/docs/api/{*path}",
            get(|Path(path): Path<String>| async move { spa_app("api-docs", &path, false).await }),
        )
        .route(
            "/admin/",
            get(|| async { spa_app("admin", "", false).await }),
        )
        .route(
            "/admin/{*path}",
            get(|Path(path): Path<String>| async move { spa_app("admin", &path, false).await }),
        )
        .route(
            "/assets/{*path}",
            get(|Path(path): Path<String>| async move {
                spa_app("status", &format!("assets/{path}"), true).await
            }),
        )
        .route(
            "/logo.svg",
            get(|| async {
                web_asset("status", "logo.svg", "public, max-age=3600").unwrap_or_else(|| {
                    problem(StatusCode::NOT_FOUND, "route_not_found", "请求的路由不存在")
                })
            }),
        )
        .fallback(spa_shell)
        .layer(axum::middleware::from_fn_with_state(
            shared.clone(),
            auth::authenticate,
        ))
        .with_state(shared)
}

/// `ServeWeb`：HTTP 监听与优雅退出（Go `controller.ServeWeb` + graceful）。
/// 仅主端起用；从端不服务 HTTP（Go `runCollector` 同）。
pub async fn serve_http(
    state: Arc<AppState>,
    listener: tokio::net::TcpListener,
    cancel: CancellationToken,
) -> crate::Result<()> {
    let app = router(state);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel.cancelled().await;
        })
        .await
        .map_err(|err| anyhow::anyhow!("serve http: {err}"))?;
    tracing::info!("HTTP 服务退出");
    Ok(())
}
