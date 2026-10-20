//! 从端管理：创建后可见、令牌轮换使旧令牌失效、范围写入、安装预览。

mod common;

use common::*;
use santaizi_dashboard::telemetry::credential;

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

async fn json_ok(response: reqwest::Response, expected: u16) -> serde_json::Value {
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status.as_u16(), expected, "{body}");
    serde_json::from_str(&body).unwrap()
}

#[tokio::test]
async fn collector_create_rotate_scope_and_install_preview() {
    let fixture = setup_primary("http-collectors", false).await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie, csrf) = member_headers(&fixture, port, "cole").await;

    let created = json_ok(
        client()
            .post(format!("{base}/api/v2/admin/telemetry/collectors"))
            .header(reqwest::header::COOKIE, &cookie)
            .header("X-CSRF-Token", &csrf)
            .json(&serde_json::json!({
                "name": "边缘",
                "kind": "observer",
                "address": "edge.example:5556",
                "scopes": [{"type": "all", "value": ""}]
            }))
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let id = created["data"]["collector"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let old_token = created["data"]["registration_token"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(old_token.starts_with("stc_"));

    let listed = json_ok(
        client()
            .get(format!("{base}/api/v2/admin/telemetry/collectors"))
            .header(reqwest::header::COOKIE, &cookie)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert!(
        listed["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == id),
        "创建的从端应出现在列表"
    );

    let rotated = json_ok(
        client()
            .post(format!(
                "{base}/api/v2/admin/telemetry/collectors/{id}/rotate-token"
            ))
            .header(reqwest::header::COOKIE, &cookie)
            .header("X-CSRF-Token", &csrf)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let new_token = rotated["data"]["registration_token"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(old_token, new_token);
    let hash: Vec<u8> = fixture
        .db
        .read(|conn| {
            conn.query_row(
                "SELECT token_hash FROM collectors WHERE collector_uuid = ?1",
                [&id],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .unwrap();
    assert!(
        !credential::registration_token_matches(&old_token, &hash),
        "旧令牌不能再登记"
    );
    assert!(credential::registration_token_matches(&new_token, &hash));

    let scoped = json_ok(
        client()
            .put(format!(
                "{base}/api/v2/admin/telemetry/collectors/{id}/scope"
            ))
            .header(reqwest::header::COOKIE, &cookie)
            .header("X-CSRF-Token", &csrf)
            .json(&serde_json::json!({
                "scopes": [{"type": "tag", "value": "tag1"}]
            }))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(scoped["data"]["scopes"][0]["type"], "tag");
    assert_eq!(scoped["data"]["scopes"][0]["value"], "tag1");

    let preview = json_ok(
        client()
            .post(format!(
                "{base}/api/v2/admin/telemetry/collectors/{id}/install-preview"
            ))
            .header(reqwest::header::COOKIE, &cookie)
            .header("X-CSRF-Token", &csrf)
            .json(&serde_json::json!({
                "primary_endpoint": "panel.example:5555"
            }))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let command = preview["data"]["command"].as_str().unwrap();
    assert!(command.contains("panel.example:5555"), "{command}");
    assert!(command.contains(&new_token), "{command}");

    cancel.cancel();
    let _ = server.await;
}
