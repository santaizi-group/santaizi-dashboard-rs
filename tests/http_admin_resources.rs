//! Admin typed 资源集成测试：monitors / notifications / alert-rules CRUD、
//! 校验失败形态、probe-capabilities / script-commands 静态端点与
//! install / upgrade preview。

mod common;

use common::*;

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

/// 服务监控 CRUD 与校验。
#[tokio::test]
async fn monitors_crud_and_validation() {
    let fixture = setup_primary("res-monitors", false).await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, csrf) = member_headers(&fixture, port, "kate").await;
    let url = format!("{base}/api/v2/admin/monitors");

    // 校验失败形态。
    for (body, code) in [
        (
            serde_json::json!({"name": "m", "type": "grpc", "target": "https://x",
                "scope": {"mode": "all"}}),
            "invalid_monitor",
        ),
        (
            serde_json::json!({"name": "m", "type": "http", "target": "https://x",
                "interval_seconds": 1, "scope": {"mode": "all"}}),
            "invalid_monitor",
        ),
        (
            serde_json::json!({"name": "m", "type": "http", "target": "https://x",
                "scope": {"mode": "include", "server_ids": []}}),
            "invalid_monitor",
        ),
        (
            serde_json::json!({"name": "m", "type": "http", "target": "https://x",
                "scope": {"mode": "wat"}}),
            "invalid_monitor",
        ),
    ] {
        let response = client()
            .post(&url)
            .header(reqwest::header::COOKIE, &cookie_header)
            .header("X-CSRF-Token", &csrf)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400, "body {body}");
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["code"],
            code
        );
    }

    // 创建：默认间隔 30、scope exclude 归一。
    let created = client()
        .post(&url)
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "blog", "type": "HTTP", "target": "https://blog.example",
            "scope": {"mode": "exclude", "server_ids": [7]},
            "notify": true, "notification_tag": "default", "show_in_service": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let data = created.json::<serde_json::Value>().await.unwrap()["data"].clone();
    let monitor_id = data["id"].as_i64().unwrap();
    assert_eq!(data["type"], "http", "类型归一化小写");
    assert_eq!(data["interval_seconds"], 30, "缺省 30s");
    assert_eq!(
        data["scope"],
        serde_json::json!({"mode": "exclude", "server_ids": [7]})
    );

    // 更新：include scope。
    let updated = client()
        .patch(format!("{url}/{monitor_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "blog2", "type": "icmp", "target": "blog.example",
            "interval_seconds": 60, "scope": {"mode": "include", "server_ids": [7, 8]},
            "latency_notify": true, "min_latency_ms": 10, "max_latency_ms": 100
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(updated.status(), 200);
    let data = updated.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data["name"], "blog2");
    assert_eq!(data["interval_seconds"], 60);
    assert_eq!(
        data["scope"],
        serde_json::json!({"mode": "include", "server_ids": [7, 8]})
    );

    // 列表 + 搜索。
    let list = client()
        .get(format!("{url}?q=blog"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(
        list.json::<serde_json::Value>().await.unwrap()["meta"]["total"],
        1
    );

    // 删除 → 204 + 404。
    let deleted = client()
        .delete(format!("{url}/{monitor_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);
    let missing = client()
        .get(format!("{url}/{monitor_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);

    cancel.cancel();
    let _ = server.await;
}

/// 通知渠道 CRUD：headers 键值对、method/type 归一化、测试端点 501。
#[tokio::test]
async fn notifications_crud() {
    let fixture = setup_primary("res-notify", false).await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, csrf) = member_headers(&fixture, port, "leo").await;
    let url = format!("{base}/api/v2/admin/notifications");

    // URL 必须绝对。
    let bad = client()
        .post(&url)
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "n", "tag": "default", "url": "/relative",
            "method": "get", "request_type": "json"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);

    let created = client()
        .post(&url)
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "telegram", "tag": "default",
            "url": "https://example.invalid/hook",
            "method": "POST", "request_type": "form",
            "headers": [{"key": "b", "value": "2"}, {"key": "a", "value": "1"}],
            "body": "key=value", "verify_tls": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let data = created.json::<serde_json::Value>().await.unwrap()["data"].clone();
    let notification_id = data["id"].as_i64().unwrap();
    assert_eq!(data["method"], "post");
    assert_eq!(data["request_type"], "form");
    assert_eq!(data["verify_tls"], true);
    assert_eq!(
        data["headers"],
        serde_json::json!([{"key": "a", "value": "1"}, {"key": "b", "value": "2"}]),
        "headers 按键排序回显"
    );

    // 测试端点：SSRF 拒内网 URL → 502 notification_test_failed（引擎已接线）。
    let test = client()
        .post(format!("{url}/{notification_id}/test"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(test.status(), 502);
    assert_eq!(
        test.json::<serde_json::Value>().await.unwrap()["code"],
        "notification_test_failed"
    );

    let deleted = client()
        .delete(format!("{url}/{notification_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);

    cancel.cancel();
    let _ = server.await;
}

/// 告警规则 CRUD：条件校验与 scope 序列化。
#[tokio::test]
async fn alert_rules_crud() {
    let fixture = setup_primary("res-alerts", false).await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, csrf) = member_headers(&fixture, port, "mallory").await;
    let url = format!("{base}/api/v2/admin/alert-rules");

    // cpu 条件缺 min/max → 400。
    let bad = client()
        .post(&url)
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "r", "trigger_mode": "always", "notification_tag": "default",
            "conditions": [{"type": "cpu", "duration_seconds": 300,
                "scope": {"mode": "all"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);

    let created = client()
        .post(&url)
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "cpu-hot", "enabled": true, "trigger_mode": "always",
            "notification_tag": "default",
            "conditions": [
                {"type": "CPU", "min": 80, "max": 100, "duration_seconds": 300,
                 "scope": {"mode": "exclude", "server_ids": [7]}},
                {"type": "offline", "duration_seconds": 30,
                 "scope": {"mode": "include", "server_ids": [7]}}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let data = created.json::<serde_json::Value>().await.unwrap()["data"].clone();
    let rule_id = data["id"].as_i64().unwrap();
    assert_eq!(data["enabled"], true);
    assert_eq!(data["trigger_mode"], "always");
    let conditions = data["conditions"].as_array().unwrap();
    assert_eq!(conditions.len(), 2);
    assert_eq!(conditions[0]["type"], "cpu", "条件类型归一化小写");
    assert_eq!(conditions[0]["min"], serde_json::json!(80.0));
    assert_eq!(
        conditions[0]["scope"],
        serde_json::json!({"mode": "exclude", "server_ids": [7]})
    );
    assert_eq!(conditions[1]["type"], "offline");
    assert_eq!(
        conditions[1]["scope"],
        serde_json::json!({"mode": "include", "server_ids": [7]})
    );

    // 更新为 once 触发 + 禁用。
    let updated = client()
        .patch(format!("{url}/{rule_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "cpu-hot", "enabled": false, "trigger_mode": "once",
            "notification_tag": "default",
            "conditions": [{"type": "memory", "max": 90, "duration_seconds": 600,
                "scope": {"mode": "all"}}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(updated.status(), 200);
    let data = updated.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data["enabled"], false);
    assert_eq!(data["trigger_mode"], "once");
    assert_eq!(data["conditions"][0]["min"], serde_json::Value::Null);

    let deleted = client()
        .delete(format!("{url}/{rule_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);

    cancel.cancel();
    let _ = server.await;
}

/// 静态端点：probe-capabilities / script-commands / install-preview /
/// upgrade-preview。
#[tokio::test]
async fn static_endpoints_and_previews() {
    let fixture = setup_primary("res-static", false).await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, csrf) = member_headers(&fixture, port, "nancy").await;

    let capabilities = client()
        .get(format!("{base}/api/v2/admin/probe-capabilities"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let data = capabilities.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(
        data["required"],
        serde_json::json!(["heartbeat", "identity"])
    );
    assert_eq!(data["optional"].as_array().unwrap().len(), 14);
    assert!(data["presets"]["standard_cloud"]["cpu"].as_bool().unwrap());
    assert!(
        !data["presets"]["standard_cloud"]["temperature"]
            .as_bool()
            .unwrap()
    );
    assert!(
        data["presets"]["standard_physical"]["gpu"]
            .as_bool()
            .unwrap()
    );

    let scripts = client()
        .get(format!("{base}/api/v2/admin/script-commands"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let data = scripts.json::<serde_json::Value>().await.unwrap()["data"].clone();
    let ids: Vec<&str> = data["commands"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"dashboard_install"));
    assert!(ids.contains(&"agent_upgrade_linux"));
    assert!(ids.contains(&"agent_uninstall_windows"));

    // install-preview：go / linux，命令带主机、端口与密钥。
    let install = client()
        .post(format!(
            "{base}/api/v2/admin/servers/{SERVER_ID}/install-preview"
        ))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "platform": "linux",
            "clean_install": true,
            "options": {"cpu": true, "ip_report": true},
            "ip_report_config": {"country_code": "CN", "prefer_ipv6": true}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(install.status(), 200);
    let data = install.json::<serde_json::Value>().await.unwrap()["data"].clone();
    let command = data["command"].as_str().unwrap();
    assert!(command.contains(CLIENT_SECRET), "命令内嵌服务器密钥");
    assert!(command.contains("--clean-install"));
    assert!(command.contains("--country-code"));
    assert!(
        !command.contains("--disable-cpu"),
        "cpu 显式启用不打禁用旗标"
    );
    assert!(
        command.contains("--disable-memory"),
        "未选 memory 打禁用旗标"
    );
    assert_eq!(data["implementation"], "go");

    // rust 实现仅 linux。
    let rust_windows = client()
        .post(format!(
            "{base}/api/v2/admin/servers/{SERVER_ID}/install-preview"
        ))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"platform": "windows", "implementation": "rust"}))
        .send()
        .await
        .unwrap();
    assert_eq!(rust_windows.status(), 400);

    // upgrade-preview。
    let upgrade = client()
        .post(format!(
            "{base}/api/v2/admin/servers/{SERVER_ID}/upgrade-preview"
        ))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"platform": "linux"}))
        .send()
        .await
        .unwrap();
    assert_eq!(upgrade.status(), 200);
    let command = upgrade.json::<serde_json::Value>().await.unwrap()["data"]["command"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(command.starts_with("curl -fsSL '"));

    cancel.cancel();
    let _ = server.await;
}
