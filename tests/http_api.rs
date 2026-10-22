//! HTTP API 集成测试：真实 axum 服务 + reqwest 客户端。
//!
//! 覆盖认证三态（未登录 / Cookie 会话 / Bearer API Token）、CSRF 门槛、
//! 登出清理、SPA 回退与静态/OpenAPI 托管。

mod common;

use common::*;

/// 未登录：会话匿名、CSRF 令牌随 Cookie 下发。
#[tokio::test]
async fn session_unauthenticated_returns_csrf() {
    let fixture = setup_primary("http-anon", false).await;
    let (port, cancel, server) = spawn_http(&fixture).await;

    let response = client()
        .get(format!("{}/api/v2/auth/session", base_url(port)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let csrf = cookie_value(&response, "santaizi-dashboard-csrf").expect("csrf cookie");
    assert!(!csrf.is_empty() && csrf.contains('.'), "csrf token shape");
    let body: serde_json::Value = response.json().await.unwrap();
    let data = &body["data"];
    assert_eq!(data["authenticated"], false);
    assert_eq!(data["login_url"], "/oauth2/login");
    assert_eq!(data["version"], santaizi_dashboard::VERSION);
    assert_eq!(data["csrf_token"], serde_json::json!(csrf));
    assert!(data["capabilities"].as_array().unwrap().is_empty());
    assert!(data.get("user").is_none());

    cancel.cancel();
    let _ = server.await;
}

/// Cookie 会话：认该用户并回写档案。
#[tokio::test]
async fn session_accepts_cookie() {
    let fixture = setup_primary("http-cookie", false).await;
    let token = insert_user(&fixture.db, "alice", true);
    let (port, cancel, server) = spawn_http(&fixture).await;

    let response = client()
        .get(format!("{}/api/v2/auth/session", base_url(port)))
        .header(
            reqwest::header::COOKIE,
            format!("santaizi-dashboard={token}"),
        )
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["data"]["authenticated"], true);
    assert_eq!(body["data"]["user"]["login"], "alice");
    assert_eq!(body["data"]["user"]["super_admin"], true);
    let capabilities = body["data"]["capabilities"].as_array().unwrap();
    assert_eq!(capabilities.len(), 6);
    assert_eq!(capabilities[0], "servers:write");

    cancel.cancel();
    let _ = server.await;
}

/// Bearer（write）：API Token 认该用户，能力为写集合。
#[tokio::test]
async fn session_accepts_bearer_write() {
    let fixture = setup_primary("http-bearer", false).await;
    let token = insert_user(&fixture.db, "bob", false);
    let user_id = user_id_by_login(&fixture.db, "bob");
    let plain = insert_api_token(&fixture.db, user_id, "write");
    let _ = token;
    let (port, cancel, server) = spawn_http(&fixture).await;

    let response = client()
        .get(format!("{}/api/v2/auth/session", base_url(port)))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {plain}"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["data"]["authenticated"], true);
    assert_eq!(body["data"]["user"]["login"], "bob");
    assert_eq!(body["data"]["capabilities"][0], "servers:write");

    cancel.cancel();
    let _ = server.await;
}

/// Bearer（read）：能力退化为读集合（Go 读-only 令牌语义）。
#[tokio::test]
async fn session_bearer_read_grants_read_capabilities() {
    let fixture = setup_primary("http-bearer-read", false).await;
    insert_user(&fixture.db, "carol", false);
    let user_id = user_id_by_login(&fixture.db, "carol");
    let plain = insert_api_token(&fixture.db, user_id, "read");
    let (port, cancel, server) = spawn_http(&fixture).await;

    let response = client()
        .get(format!("{}/api/v2/auth/session", base_url(port)))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {plain}"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["data"]["capabilities"][0], "servers:read");

    cancel.cancel();
    let _ = server.await;
}

/// 过期会话与陌生令牌都不认。
#[tokio::test]
async fn session_rejects_expired_and_unknown_credentials() {
    let fixture = setup_primary("http-expired", false).await;
    let expired = "expired-session";
    let past = santaizi_dashboard::util::time::now_nanos() / 1_000_000_000 - 60;
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO users (login, name, token, token_expired) \
                 VALUES ('dave', 'dave', ?1, datetime(?2, 'unixepoch'))",
                rusqlite::params![expired, past],
            )?;
            Ok(())
        })
        .unwrap();
    let (port, cancel, server) = spawn_http(&fixture).await;

    let url = format!("{}/api/v2/auth/session", base_url(port));
    let expired_response = client()
        .get(&url)
        .header(
            reqwest::header::COOKIE,
            format!("santaizi-dashboard={expired}"),
        )
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = expired_response.json().await.unwrap();
    assert_eq!(body["data"]["authenticated"], false);

    let bearer_response = client()
        .get(&url)
        .header(reqwest::header::AUTHORIZATION, "Bearer nope-not-a-token")
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = bearer_response.json().await.unwrap();
    assert_eq!(body["data"]["authenticated"], false);

    cancel.cancel();
    let _ = server.await;
}

/// 非安全方法无 CSRF 头 → 403 problem+json（Go `csrf_invalid`）。
#[tokio::test]
async fn unsafe_method_requires_csrf() {
    let fixture = setup_primary("http-csrf", false).await;
    let (port, cancel, server) = spawn_http(&fixture).await;

    let response = client()
        .post(format!("{}/api/v2/auth/logout", base_url(port)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .unwrap(),
        "application/problem+json"
    );
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["code"], "csrf_invalid");

    cancel.cancel();
    let _ = server.await;
}

/// 登出：Cookie + CSRF 配对 → 204，会话 Cookie 被清，库内令牌作废。
#[tokio::test]
async fn logout_clears_session() {
    let fixture = setup_primary("http-logout", false).await;
    let token = insert_user(&fixture.db, "erin", false);
    let (port, cancel, server) = spawn_http(&fixture).await;

    // CSRF 令牌绑定会话标识（auth+vp Cookie）：须带上会话 Cookie 现取，
    // 与已登录浏览器的行为一致（登录前后令牌会轮换）。
    let session = client()
        .get(format!("{}/api/v2/auth/session", base_url(port)))
        .header(
            reqwest::header::COOKIE,
            format!("santaizi-dashboard={token}"),
        )
        .send()
        .await
        .unwrap();
    let csrf = cookie_value(&session, "santaizi-dashboard-csrf").expect("csrf cookie");

    let response = client()
        .post(format!("{}/api/v2/auth/logout", base_url(port)))
        .header(
            reqwest::header::COOKIE,
            format!("santaizi-dashboard={token}; santaizi-dashboard-csrf={csrf}"),
        )
        .header("X-CSRF-Token", &csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 204);
    let cleared = cookie_value(&response, "santaizi-dashboard").unwrap_or_default();
    assert!(
        cleared.is_empty(),
        "session cookie should be cleared: {cleared:?}"
    );

    // 库内令牌已作废：再带原 Cookie 认不出来。
    let after = client()
        .get(format!("{}/api/v2/auth/session", base_url(port)))
        .header(
            reqwest::header::COOKIE,
            format!("santaizi-dashboard={token}"),
        )
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = after.json().await.unwrap();
    assert_eq!(body["data"]["authenticated"], false);

    cancel.cancel();
    let _ = server.await;
}

/// Bearer 请求免 CSRF（Go `Authorization` 非空即跳过）。
#[tokio::test]
async fn bearer_request_skips_csrf() {
    let fixture = setup_primary("http-bearer-csrf", false).await;
    insert_user(&fixture.db, "frank", false);
    let user_id = user_id_by_login(&fixture.db, "frank");
    let plain = insert_api_token(&fixture.db, user_id, "write");
    let (port, cancel, server) = spawn_http(&fixture).await;

    let response = client()
        .post(format!("{}/api/v2/auth/logout", base_url(port)))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {plain}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 204);

    cancel.cancel();
    let _ = server.await;
}

/// Bearer（含只读）调 logout 不得销毁同账号管理员的登录会话：
/// API Token 是设计上可外发的凭据，`users.token` 不是它拥有的东西。
#[tokio::test]
async fn bearer_logout_does_not_kill_admin_session() {
    let fixture = setup_primary("http-logout-bearer", false).await;
    let session_token = insert_user(&fixture.db, "gina", false);
    let user_id = user_id_by_login(&fixture.db, "gina");
    // 同一账号下再发一个只读令牌——正是会外发给看板的那种。
    let read_only = insert_api_token(&fixture.db, user_id, "read");
    let (port, cancel, server) = spawn_http(&fixture).await;

    let logout = client()
        .post(format!("{}/api/v2/auth/logout", base_url(port)))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {read_only}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(logout.status(), 204);

    // 管理员会话必须仍然有效。
    let after = client()
        .get(format!("{}/api/v2/auth/session", base_url(port)))
        .header(
            reqwest::header::COOKIE,
            format!("santaizi-dashboard={session_token}"),
        )
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = after.json().await.unwrap();
    assert_eq!(
        body["data"]["authenticated"], true,
        "只读 Bearer 不得把管理员会话踢下线：{body}"
    );

    cancel.cancel();
    let _ = server.await;
}

/// SPA 回退：前端路由回 index.html 外壳。
#[tokio::test]
async fn spa_routes_serve_shell() {
    let fixture = setup_primary("http-spa", false).await;
    let (port, cancel, server) = spawn_http(&fixture).await;

    // 前端路由回落：status 壳 / admin 壳（根路径不能被公开站外壳吞掉）。
    for (path, marker) in [
        ("/", "三太子监控"),
        ("/service", "三太子监控"),
        ("/admin/servers", "管理后台"),
        ("/admin/", "管理后台"),
    ] {
        let response = client()
            .get(format!("{}{path}", base_url(port)))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200, "{path} should serve the shell");
        assert_eq!(
            response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .unwrap(),
            "text/html; charset=utf-8"
        );
        let body = response.text().await.unwrap();
        assert!(
            body.contains("<!doctype html>") || body.contains("<!DOCTYPE html>"),
            "{path} shell body"
        );
        assert!(body.contains(marker), "{path} 应回落 {marker} 外壳");
    }

    // 带 hash 资产命中（status index 引用的 js）。
    let shell = client()
        .get(format!("{}/", base_url(port)))
        .send()
        .await
        .unwrap();
    let shell_body = shell.text().await.unwrap();
    let asset = shell_body
        .split('"')
        .find(|part| part.starts_with("/assets/") && part.ends_with(".js"))
        .map(str::to_string)
        .expect("status shell 应引用 /assets/*.js");
    let asset_response = client()
        .get(format!("{}{asset}", base_url(port)))
        .send()
        .await
        .unwrap();
    assert_eq!(asset_response.status(), 200, "{asset}");
    assert_eq!(
        asset_response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .unwrap(),
        "text/javascript; charset=utf-8"
    );

    cancel.cancel();
    let _ = server.await;
}

/// 静态资产命中与未命中；OpenAPI 与健康探测。
#[tokio::test]
async fn static_and_openapi_and_healthz() {
    let fixture = setup_primary("http-static", false).await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);

    let missing = client()
        .get(format!("{base}/assets/nope.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    assert_eq!(
        missing
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .unwrap(),
        "application/problem+json"
    );

    let admin_redirect = client().get(format!("{base}/admin")).send().await.unwrap();
    assert!(
        admin_redirect.status().is_redirection(),
        "/admin 应重定向到 /admin/"
    );

    let openapi = client()
        .get(format!("{base}/openapi/v2.yaml"))
        .send()
        .await
        .unwrap();
    assert_eq!(openapi.status(), 200);
    assert_eq!(
        openapi
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .unwrap(),
        "application/yaml; charset=utf-8"
    );
    let body = openapi.text().await.unwrap();
    assert!(body.contains("/api/v2/auth/session"), "openapi body");

    let openapi_json = client()
        .get(format!("{base}/openapi/v2.json"))
        .send()
        .await
        .unwrap();
    assert_eq!(openapi_json.status(), 200);
    assert_eq!(
        openapi_json
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .unwrap(),
        "application/json; charset=utf-8"
    );
    let json_body = openapi_json.text().await.unwrap();
    assert!(json_body.contains("/api/v2/auth/session"), "openapi json");

    let logo = client()
        .get(format!("{base}/static/logo.svg"))
        .send()
        .await
        .unwrap();
    assert_eq!(logo.status(), 200);
    assert_eq!(
        logo.headers().get(reqwest::header::CONTENT_TYPE).unwrap(),
        "image/svg+xml"
    );
    assert!(logo.text().await.unwrap().contains("<svg"), "logo svg");

    let map = client()
        .get(format!("{base}/static/theme-nazhua/maps/world.geo.json"))
        .send()
        .await
        .unwrap();
    assert_eq!(map.status(), 200);

    let docs_redirect = client()
        .get(format!("{base}/docs/api"))
        .send()
        .await
        .unwrap();
    assert!(docs_redirect.status().is_redirection());
    let docs = client()
        .get(format!("{base}/docs/api/"))
        .send()
        .await
        .unwrap();
    assert_eq!(docs.status(), 200);
    let docs_body = docs.text().await.unwrap();
    assert!(
        docs_body.contains("<!doctype html>") || docs_body.contains("<!DOCTYPE html>"),
        "api docs shell"
    );
    assert!(
        docs_body.contains("Santaizi API"),
        "/docs/api/ 应回落文档站外壳"
    );

    let health = client()
        .get(format!("{base}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), 200);
    assert_eq!(health.text().await.unwrap(), "ok");

    cancel.cancel();
    let _ = server.await;
}

/// 未知 API 路由：404 problem+json（Go NoRoute 语义；只有前端路由回落 SPA）。
#[tokio::test]
async fn unknown_api_path_returns_problem() {
    let fixture = setup_primary("http-unknown", false).await;
    let (port, cancel, server) = spawn_http(&fixture).await;

    let response = client()
        .get(format!("{}/api/v2/public/nope", base_url(port)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .unwrap(),
        "application/problem+json"
    );
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["code"], "route_not_found");

    cancel.cancel();
    let _ = server.await;
}
