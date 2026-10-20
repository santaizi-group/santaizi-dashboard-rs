//! Admin settings 与 API Token 端点集成测试：DTO 形态、校验失败形态、
//! 配置热更新（查看密码即时生效）+ yaml 回写、token 生命周期与索引联动。

mod common;

use common::*;

/// 带成员会话的夹具：oauth2 mock（validate 需要）+ 可写的 yaml 路径。
async fn setup_settings_fixture(tag: &str) -> (PrimaryFixture, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "santaizi-admin-settings-{tag}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let yaml_path = dir.join("dashboard.yaml");
    let yaml = yaml_path.clone();
    let fixture = setup_primary_with(tag, false, move |conf| {
        conf.debug = true;
        conf.oauth2.type_ = "mock".into();
        conf.oauth2.admin = "dev".into();
        conf.set_file_path(yaml.to_string_lossy().as_ref());
    })
    .await;
    (fixture, yaml_path)
}

/// (cookie 头, csrf 头)。
async fn member_headers(fixture: &PrimaryFixture, port: u16, login: &str) -> (String, String) {
    let token = insert_user(&fixture.db, login, true);
    let response = client()
        .get(format!("{}/api/v2/auth/session", base_url(port)))
        .header(
            reqwest::header::COOKIE,
            format!("santaizi-dashboard={token}"),
        )
        .send()
        .await
        .unwrap();
    let csrf = cookie_value(&response, "santaizi-dashboard-csrf").expect("csrf cookie");
    (
        format!("santaizi-dashboard={token}; santaizi-dashboard-csrf={csrf}"),
        csrf,
    )
}

/// DTO 默认形态 + PATCH 生效 + yaml 回写。
#[tokio::test]
async fn settings_dto_and_patch_round_trip() {
    let (fixture, yaml_path) = setup_settings_fixture("settings-basic").await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, csrf) = member_headers(&fixture, port, "gina").await;

    let response = client()
        .get(format!("{base}/api/v2/admin/settings"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let data = response.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data["site_title"], "Santaizi Monitoring");
    assert_eq!(data["language"], "zh-CN");
    assert_eq!(data["view_password"], "");
    assert_eq!(data["view_password_configured"], false);
    assert_eq!(data["theme"], "server-status");
    assert_eq!(data["allow_frontend_theme_switch"], true);
    assert_eq!(data["nameservers"], serde_json::json!([]));
    assert!(data["retention"].is_object());
    assert_eq!(data["web_delivery"], "embedded");

    // PATCH：文案与开关。
    let patched = client()
        .patch(format!("{base}/api/v2/admin/settings"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "site_title": "三太子监控",
            "footer_text": "自建 · 用心",
            "show_availability_guest": true,
            "nameservers": ["1.1.1.1", "8.8.8.8"],
            "theme": "nazhua",
            "primary_color": "#12ab34",
            "check_interval": 10,
            "offline_threshold": 30
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(patched.status(), 200);
    let data = patched.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data["site_title"], "三太子监控");
    assert_eq!(data["footer_text"], "自建 · 用心");
    assert_eq!(data["show_availability_guest"], true);
    assert_eq!(
        data["nameservers"],
        serde_json::json!(["1.1.1.1", "8.8.8.8"])
    );
    assert_eq!(data["theme"], "nazhua");
    assert_eq!(data["primary_color"], "#12ab34");

    // yaml 落盘（Go Conf.Save 语义）：重新加载应还原同一份配置。
    let text = std::fs::read_to_string(&yaml_path).unwrap();
    assert!(text.contains("site:"), "yaml 应包含 site 段");
    let reloaded = santaizi_dashboard::config::Config::load(yaml_path.to_str().unwrap()).unwrap();
    assert_eq!(reloaded.site.brand, "三太子监控");
    assert_eq!(reloaded.site.theme, "nazhua");
    assert_eq!(reloaded.dns_servers, "1.1.1.1,8.8.8.8");

    // 主题白名单：server-grid 可往返，未知值收敛回 server-status。
    for (wanted, expected) in [("server-grid", "server-grid"), ("bogus", "server-status")] {
        let patched = client()
            .patch(format!("{base}/api/v2/admin/settings"))
            .header(reqwest::header::COOKIE, &cookie_header)
            .header("X-CSRF-Token", &csrf)
            .json(&serde_json::json!({ "theme": wanted }))
            .send()
            .await
            .unwrap();
        assert_eq!(patched.status(), 200);
        let data = patched.json::<serde_json::Value>().await.unwrap()["data"].clone();
        assert_eq!(
            data["theme"], expected,
            "theme {wanted} 应归一为 {expected}"
        );
    }

    cancel.cancel();
    let _ = server.await;
}

/// 查看密码热更新：PATCH 即时改变公开站门槛，无需重启。
#[tokio::test]
async fn settings_hot_effect_on_public_gate() {
    let (fixture, _yaml) = setup_settings_fixture("settings-hot").await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, csrf) = member_headers(&fixture, port, "henry").await;

    let open = client()
        .get(format!("{base}/api/v2/public/servers"))
        .send()
        .await
        .unwrap();
    assert_eq!(open.status(), 200, "未配查看密码时公开站放行");

    let patched = client()
        .patch(format!("{base}/api/v2/admin/settings"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"view_password": "s3cret"}))
        .send()
        .await
        .unwrap();
    assert_eq!(patched.status(), 200);
    assert_eq!(
        patched.json::<serde_json::Value>().await.unwrap()["data"]["view_password_configured"],
        true
    );

    let gated = client()
        .get(format!("{base}/api/v2/public/servers"))
        .send()
        .await
        .unwrap();
    assert_eq!(gated.status(), 403, "热更新后匿名立即被查看密码拦下");

    let cleared = client()
        .patch(format!("{base}/api/v2/admin/settings"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"clear_view_password": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(cleared.status(), 200);
    let reopened = client()
        .get(format!("{base}/api/v2/public/servers"))
        .send()
        .await
        .unwrap();
    assert_eq!(reopened.status(), 200);

    cancel.cancel();
    let _ = server.await;
}

/// 校验失败形态：语言白名单、离线参数范围、CSS 安全、Logo 白名单、端口、颜色。
#[tokio::test]
async fn settings_validation_failures() {
    let (fixture, _yaml) = setup_settings_fixture("settings-invalid").await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, csrf) = member_headers(&fixture, port, "iris").await;

    let cases: Vec<(serde_json::Value, &str)> = vec![
        (serde_json::json!({"language": "fr-FR"}), "invalid_language"),
        (
            serde_json::json!({"offline_threshold": 5}),
            "invalid_offline_settings",
        ),
        (
            serde_json::json!({"check_interval": 100, "offline_threshold": 30}),
            "invalid_offline_settings",
        ),
        (
            serde_json::json!({"custom_css": "@import url(https://evil/x.css);"}),
            "unsafe_custom_css",
        ),
        (
            serde_json::json!({"custom_css": "a { background: url(https://evil/x.png); }"}),
            "unsafe_custom_css",
        ),
        (
            serde_json::json!({"logo_url": "https://evil.example/x.png"}),
            "unsafe_asset_url",
        ),
        (
            serde_json::json!({"proxy_grpc_port": 70000}),
            "invalid_proxy_grpc_port",
        ),
        (
            serde_json::json!({"primary_color": "red"}),
            "invalid_primary_color",
        ),
    ];
    for (body, code) in cases {
        let response = client()
            .patch(format!("{base}/api/v2/admin/settings"))
            .header(reqwest::header::COOKIE, &cookie_header)
            .header("X-CSRF-Token", &csrf)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400, "body {body} 应 400");
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["code"],
            code,
            "body {body}"
        );
    }

    cancel.cancel();
    let _ = server.await;
}

/// API Token 生命周期：创建即生效（Bearer 直用）、明文只回一次、
/// 启停即时生效、删除后索引清理。
#[tokio::test]
async fn api_tokens_lifecycle() {
    let fixture = setup_primary("settings-tokens", false).await;
    let member = insert_user(&fixture.db, "judy", true);
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, csrf) = member_headers(&fixture, port, "judy").await;

    // 校验：permission 白名单。
    let bad = client()
        .post(format!("{base}/api/v2/admin/api-tokens"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"note": "ro", "permission": "admin"}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    assert_eq!(
        bad.json::<serde_json::Value>().await.unwrap()["code"],
        "invalid_token_permission"
    );

    // 创建（write 权限 + 未来到期时间）。
    let expires = time::OffsetDateTime::now_utc() + time::Duration::days(30);
    let created = client()
        .post(format!("{base}/api/v2/admin/api-tokens"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "note": "ci",
            "permission": "write",
            "expires_at": expires.format(&time::format_description::well_known::Rfc3339).unwrap()
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let body: serde_json::Value = created.json().await.unwrap();
    let data = &body["data"];
    let token_id = data["id"].as_i64().unwrap();
    let plain = data["token"].as_str().unwrap().to_string();
    assert_eq!(plain.len(), 32);
    assert_eq!(data["permission"], "write");
    assert_eq!(data["enabled"], true);
    assert_eq!(data["expired"], false);
    let prefix: String = plain.chars().take(8).collect();
    assert_eq!(data["token_prefix"], format!("{prefix}…"));
    assert!(
        data["expires_at"].as_str().is_some(),
        "到期时间回显 RFC3339"
    );

    // 明文 Bearer 立即可用（读 admin 列表）。
    let bearer = client()
        .get(format!("{base}/api/v2/admin/servers"))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {plain}"))
        .send()
        .await
        .unwrap();
    assert_eq!(bearer.status(), 200);

    // 列表不带明文；详情回明文。
    let list = client()
        .get(format!("{base}/api/v2/admin/api-tokens"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = list.json().await.unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert!(body["data"][0].get("token").is_none());
    let detail = client()
        .get(format!("{base}/api/v2/admin/api-tokens/{token_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(
        detail.json::<serde_json::Value>().await.unwrap()["data"]["token"],
        plain
    );

    // 禁用后 Bearer 立即失效。
    let patched = client()
        .patch(format!("{base}/api/v2/admin/api-tokens/{token_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"enabled": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(patched.status(), 200);
    assert_eq!(
        patched.json::<serde_json::Value>().await.unwrap()["data"]["enabled"],
        false
    );
    let rejected = client()
        .get(format!("{base}/api/v2/admin/servers"))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {plain}"))
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 401);

    // 重新启用 → 恢复；删除 → 204 + 失效 + 404。
    let _ = client()
        .patch(format!("{base}/api/v2/admin/api-tokens/{token_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"enabled": true}))
        .send()
        .await
        .unwrap();
    let restored = client()
        .get(format!("{base}/api/v2/admin/servers"))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {plain}"))
        .send()
        .await
        .unwrap();
    assert_eq!(restored.status(), 200);

    let deleted = client()
        .delete(format!("{base}/api/v2/admin/api-tokens/{token_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);
    let rejected = client()
        .get(format!("{base}/api/v2/admin/servers"))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {plain}"))
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 401);
    let missing = client()
        .get(format!("{base}/api/v2/admin/api-tokens/{token_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    assert_eq!(
        missing.json::<serde_json::Value>().await.unwrap()["code"],
        "token_not_found"
    );
    let _ = member;

    cancel.cancel();
    let _ = server.await;
}
