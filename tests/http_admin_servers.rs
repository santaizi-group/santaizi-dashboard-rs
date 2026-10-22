//! Admin 服务器端点集成测试：CRUD / 批量分组 / 分组重命名 / 密钥重置 /
//! 可用性重置 / credential / traffic-policies CRUD 与 Replace 语义 / 权限门。

mod common;

use common::*;

/// 会话三元组：cookie 值、csrf cookie 对、csrf 明文。
async fn session_parts(port: u16, token: &str) -> (String, String, String) {
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
        format!("santaizi-dashboard={token}"),
        format!("santaizi-dashboard-csrf={csrf}"),
        csrf,
    )
}

/// 带会话与 CSRF 头的构造器：返回 (cookie 头, csrf 头)。
async fn member_headers(fixture: &PrimaryFixture, port: u16, login: &str) -> (String, String) {
    let token = insert_user(&fixture.db, login, true);
    let (cookie, csrf_cookie, csrf) = session_parts(port, &token).await;
    (format!("{cookie}; {csrf_cookie}"), csrf)
}

fn admin_db_seed(fixture: &PrimaryFixture, id: i64, name: &str, tag: &str) {
    let ciphertext =
        santaizi_dashboard::secret::encrypt_secret(&format!("seed-secret-{id}")).unwrap();
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) \
                 VALUES (?1, ?2, ?3, ?4, ?1, 0)",
                rusqlite::params![id, name, tag, &ciphertext],
            )?;
            Ok(())
        })
        .unwrap();
}

/// 认证门：匿名 401；read-only Token 读 200、写 403。
#[tokio::test]
async fn admin_servers_auth_gate() {
    let fixture = setup_primary("admin-gate", false).await;
    let member = insert_user(&fixture.db, "alice", true);
    let user_id = user_id_by_login(&fixture.db, "alice");
    let read_token = insert_api_token(&fixture.db, user_id, "read");
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);

    let anon = client()
        .get(format!("{base}/api/v2/admin/servers"))
        .send()
        .await
        .unwrap();
    assert_eq!(anon.status(), 401);
    assert_eq!(
        anon.json::<serde_json::Value>().await.unwrap()["code"],
        "authentication_required"
    );

    let bearer_read = client()
        .get(format!("{base}/api/v2/admin/servers"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {read_token}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(bearer_read.status(), 200);

    let (_, _, csrf) = session_parts(port, &member).await;
    let denied = client()
        .post(format!("{base}/api/v2/admin/servers"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {read_token}"),
        )
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"name": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 403);
    assert_eq!(
        denied.json::<serde_json::Value>().await.unwrap()["code"],
        "read_only_api_token"
    );

    cancel.cancel();
    let _ = server.await;
}

/// CRUD 全流程：建号（含明文密钥一次性返回 + 索引热更新）→ 列表搜索 →
/// 详情 → 更新（探测开关 keep 语义）→ display-index → credential → 删除。
#[tokio::test]
async fn server_crud_round_trip() {
    let fixture = setup_primary("admin-crud", false).await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, csrf) = member_headers(&fixture, port, "carol").await;

    // 建号。
    let created = client()
        .post(format!("{base}/api/v2/admin/servers"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "web-1", "tag": "prod", "note": "primary",
            "public_note": {"text": "hi"}, "monitoring_options": {"cpu": true},
            "display_index": 5, "hide_for_guest": true,
            "probe_target": "example.com", "probe_enable_icmp": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let body: serde_json::Value = created.json().await.unwrap();
    let data = &body["data"];
    let server_id = data["id"].as_i64().unwrap();
    let secret_plain = data["secret"].as_str().unwrap().to_string();
    assert_eq!(secret_plain.len(), 18);
    assert_eq!(data["name"], "web-1");
    assert_eq!(data["tag"], "prod");
    assert_eq!(data["public_note"], serde_json::json!({"text": "hi"}));
    assert_eq!(data["monitoring_options"], serde_json::json!({"cpu": true}));
    assert_eq!(data["display_index"], 5);
    assert_eq!(data["hide_for_guest"], true);
    assert_eq!(data["probe_target"], "example.com");
    assert_eq!(data["probe_enable_icmp"], false, "显式关闭");
    assert_eq!(data["probe_enable_tcp"], true, "未提供建号回落 true");
    assert_eq!(data["online"], false);
    assert_eq!(data["telemetry"]["host"], "unknown");
    assert_eq!(data["traffic_summaries"], serde_json::json!([]));
    assert_eq!(data["traffic_policies"], serde_json::json!([]));
    assert_eq!(
        fixture.state.secrets().server_id(&secret_plain),
        Some(server_id),
        "建号即热更新秘密索引"
    );

    // 列表 + 搜索；列表行不回明文密钥。
    let list = client()
        .get(format!(
            "{base}/api/v2/admin/servers?q=web&sort=name&order=asc"
        ))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(list.status(), 200);
    let body: serde_json::Value = list.json().await.unwrap();
    assert_eq!(body["meta"]["total"], 1);
    assert_eq!(body["data"][0]["name"], "web-1");
    assert!(body["data"][0].get("secret").is_none());

    // LIKE 通配符必须转义：`%` 不能退化成「匹配一切」。
    for (query, expected) in [("%", 0), ("_", 0), ("w_b", 0), ("web", 1)] {
        let hits = client()
            .get(format!("{base}/api/v2/admin/servers?q={query}"))
            .header(reqwest::header::COOKIE, &cookie_header)
            .send()
            .await
            .unwrap();
        assert_eq!(hits.status(), 200);
        let body: serde_json::Value = hits.json().await.unwrap();
        assert_eq!(body["meta"]["total"], expected, "q={query} 的通配符未转义");
    }

    // 更新：显式关 TCP；ICMP 缺省保留 false（Go boolOrKeep）。
    let updated = client()
        .patch(format!("{base}/api/v2/admin/servers/{server_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "web-2", "tag": "prod", "probe_enable_tcp": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(updated.status(), 200);
    let body: serde_json::Value = updated.json().await.unwrap();
    assert_eq!(body["data"]["name"], "web-2");
    assert_eq!(body["data"]["probe_enable_icmp"], false);
    assert_eq!(body["data"]["probe_enable_tcp"], false);
    assert!(body["data"].get("secret").is_none(), "更新不回明文");

    // display-index。
    let moved = client()
        .patch(format!(
            "{base}/api/v2/admin/servers/{server_id}/display-index"
        ))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"display_index": 9}))
        .send()
        .await
        .unwrap();
    assert_eq!(moved.status(), 200);
    assert_eq!(
        moved.json::<serde_json::Value>().await.unwrap()["data"]["display_index"],
        9
    );

    // credential：明文与建号一致，host/port 来自配置。
    let credential = client()
        .get(format!(
            "{base}/api/v2/admin/servers/{server_id}/credential"
        ))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(credential.status(), 200);
    let body: serde_json::Value = credential.json().await.unwrap();
    assert_eq!(body["data"]["secret"], secret_plain);
    assert_eq!(body["data"]["grpc_host"], "127.0.0.1");
    assert!(body["data"].get("grpc_port").is_some());

    // 重置密钥：新明文生效、旧明文失效。
    let reset = client()
        .post(format!(
            "{base}/api/v2/admin/servers/{server_id}/reset-secret"
        ))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(reset.status(), 200);
    let new_secret = reset.json::<serde_json::Value>().await.unwrap()["data"]["secret"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(new_secret, secret_plain);
    assert_eq!(
        fixture.state.secrets().server_id(&new_secret),
        Some(server_id)
    );
    assert_eq!(fixture.state.secrets().server_id(&secret_plain), None);

    // 预置一条当前绑定 + 一条传输量：验证删除时的连带清理与「只终结不删」。
    let node = [0x0Cu8; 16];
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason, created_at) VALUES (?1, ?2, '0', NULL, 1, 'legacy', CURRENT_TIMESTAMP)",
                rusqlite::params![server_id, node],
            )?;
            tx.execute(
                "INSERT INTO transfers (server_id, \"in\", \"out\") VALUES (?1, 1, 2)",
                [server_id],
            )?;
            Ok(())
        })
        .unwrap();

    // 删除：204 + 索引清理 + 404。
    let deleted = client()
        .delete(format!("{base}/api/v2/admin/servers/{server_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);
    assert_eq!(fixture.state.secrets().server_id(&new_secret), None);
    let missing = client()
        .get(format!("{base}/api/v2/admin/servers/{server_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);

    // 关联数据一并清掉：不得留下引用已删 server_id 的孤儿行
    // （重新占用同一 id 时会读到上一任的传输量与运行态）。
    let orphans: Vec<(String, i64)> = fixture
        .db
        .read(|conn| {
            let tables = [
                "monitor_histories",
                "transfers",
                "server_runtimes",
                "server_offline_histories",
            ];
            let mut out = Vec::new();
            for table in tables {
                let count: i64 = conn.query_row(
                    &format!("SELECT COUNT(*) FROM {table} WHERE server_id = ?1"),
                    [server_id],
                    |row| row.get(0),
                )?;
                out.push((table.to_string(), count));
            }
            Ok(out)
        })
        .unwrap();
    for (table, count) in orphans {
        assert_eq!(count, 0, "{table} 应随服务器一起删除");
    }
    // 绑定只终结不删：历史观测与可用性证据保持不可变。
    let (binding_current, binding_rows, binding_to): (i64, i64, i64) = fixture
        .db
        .read(|conn| {
            Ok((
                conn.query_row(
                    "SELECT COUNT(*) FROM server_node_bindings WHERE server_id = ?1 AND current = 1",
                    [server_id],
                    |row| row.get(0),
                )?,
                conn.query_row(
                    "SELECT COUNT(*) FROM server_node_bindings WHERE server_id = ?1",
                    [server_id],
                    |row| row.get(0),
                )?,
                conn.query_row(
                    "SELECT COUNT(*) FROM server_node_bindings WHERE server_id = ?1 AND valid_to IS NOT NULL",
                    [server_id],
                    |row| row.get(0),
                )?,
            ))
        })
        .unwrap();
    assert_eq!(binding_current, 0, "当前绑定必须被终结");
    assert_eq!(binding_rows, 1, "历史绑定行不得删除");
    assert_eq!(binding_to, 1, "终结绑定必须写入 valid_to");

    cancel.cancel();
    let _ = server.await;
}

/// 批量分组、分组清单与重命名（含 404 与 from==to 短路）。
#[tokio::test]
async fn batch_group_and_rename() {
    let fixture = setup_primary("admin-batch", false).await;
    admin_db_seed(&fixture, 11, "node-a", "alpha");
    admin_db_seed(&fixture, 12, "node-b", "beta");
    fixture.state.secrets().rebuild(&fixture.db).unwrap();
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, csrf) = member_headers(&fixture, port, "dave").await;

    let groups = client()
        .get(format!("{base}/api/v2/admin/server-groups"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = groups.json().await.unwrap();
    let names: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["alpha", "beta", "tag1"],
        "含夹具种子服务器的 tag1"
    );

    let batch = client()
        .post(format!("{base}/api/v2/admin/servers/batch/group"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"ids": [11, 12], "group": "gamma"}))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 200);
    assert_eq!(
        batch.json::<serde_json::Value>().await.unwrap()["data"]["updated"],
        2
    );

    let renamed = client()
        .post(format!("{base}/api/v2/admin/server-groups/rename"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"from": "gamma", "to": "delta"}))
        .send()
        .await
        .unwrap();
    assert_eq!(renamed.status(), 200);
    assert_eq!(
        renamed.json::<serde_json::Value>().await.unwrap()["data"]["updated"],
        2
    );

    let missing = client()
        .post(format!("{base}/api/v2/admin/server-groups/rename"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"from": "nope", "to": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    assert_eq!(
        missing.json::<serde_json::Value>().await.unwrap()["code"],
        "group_not_found"
    );

    let same = client()
        .post(format!("{base}/api/v2/admin/server-groups/rename"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"from": "delta", "to": "delta"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        same.json::<serde_json::Value>().await.unwrap()["data"]["updated"],
        0
    );

    // 批量删除：204 + 数据库无行。
    let wiped = client()
        .post(format!("{base}/api/v2/admin/servers/batch/delete"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"ids": [11, 12]}))
        .send()
        .await
        .unwrap();
    assert_eq!(wiped.status(), 204);
    let left: i64 = fixture
        .db
        .read(|conn| {
            Ok(conn.query_row("SELECT COUNT(*) FROM servers", [], |row| {
                row.get::<_, i64>(0)
            })?)
        })
        .unwrap();
    assert_eq!(left, 1, "批量删除只清 11/12，夹具种子服务器 7 保留");

    cancel.cancel();
    let _ = server.await;
}

/// 可用性重置：硬删离线历史并复位运行态。
#[tokio::test]
async fn reset_availability_clears_histories() {
    let fixture = setup_primary("admin-reset-avail", false).await;
    use santaizi_dashboard::util::time as util_time;
    let now = util_time::now_nanos();
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO server_runtimes (server_id, status, protocol, last_seen_at, last_received_at, current_offline_id) \
                 VALUES (?1, 'offline', 'v2', ?2, ?3, 42)",
                rusqlite::params![
                    SERVER_ID,
                    util_time::unix_nanos_to_sqlite(now - 3_600_000_000_000),
                    now - 3_600_000_000_000
                ],
            )?;
            tx.execute(
                "INSERT INTO server_offline_histories (server_id, started_at, ended_at, status) \
                 VALUES (?1, ?2, NULL, 'open')",
                rusqlite::params![SERVER_ID, util_time::unix_nanos_to_sqlite(now - 3_600_000_000_000)],
            )?;
            Ok(())
        })
        .unwrap();
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, csrf) = member_headers(&fixture, port, "erin").await;

    let response = client()
        .post(format!(
            "{base}/api/v2/admin/servers/{SERVER_ID}/reset-availability"
        ))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["data"]["deleted"],
        1
    );
    let (histories, status): (i64, String) = fixture
        .db
        .read(|conn| {
            Ok((
                conn.query_row(
                    "SELECT COUNT(*) FROM server_offline_histories WHERE server_id = ?1",
                    [SERVER_ID],
                    |row| row.get(0),
                )?,
                conn.query_row(
                    "SELECT status FROM server_runtimes WHERE server_id = ?1",
                    [SERVER_ID],
                    |row| row.get(0),
                )?,
            ))
        })
        .unwrap();
    assert_eq!(histories, 0);
    assert_eq!(status, "online", "上报过的运行态重置后置 online");

    cancel.cancel();
    let _ = server.await;
}

/// 流量策略：校验、CRUD、usage、服务器保存时的整体替换（Replace）与历史序列。
#[tokio::test]
async fn traffic_policies_crud_and_history() {
    let fixture = setup_primary("admin-traffic", false).await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, csrf) = member_headers(&fixture, port, "frank").await;
    let policies_url = format!("{base}/api/v2/admin/servers/{SERVER_ID}/traffic-policies");

    // 校验失败形态。
    let invalid = client()
        .post(&policies_url)
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "bad", "direction": "sideways", "mode": "cumulative",
            "quota_bytes": 1000, "warning_percent": 80, "enabled": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), 400);
    assert_eq!(
        invalid.json::<serde_json::Value>().await.unwrap()["code"],
        "invalid_traffic_policy"
    );
    let zero_quota = client()
        .post(&policies_url)
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "bad", "direction": "total", "mode": "cumulative",
            "quota_bytes": 0, "warning_percent": 80, "enabled": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(zero_quota.status(), 400);

    // 建号 + cumulative 归一（interval=1、unit=""）。
    let created = client()
        .post(&policies_url)
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "monthly", "direction": "Total", "mode": "CUMULATIVE",
            "cycle_start": "2026-01-01T00:00:00Z", "cycle_interval": 3,
            "cycle_unit": "day", "quota_bytes": 1000, "warning_percent": 80,
            "enabled": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let body: serde_json::Value = created.json().await.unwrap();
    let policy_id = body["data"]["id"].as_i64().unwrap();
    assert_eq!(body["data"]["direction"], "total", "方向归一化小写");
    assert_eq!(body["data"]["mode"], "cumulative");
    assert_eq!(
        body["data"]["cycle_interval"], 1,
        "cumulative 强制 interval=1"
    );
    assert_eq!(body["data"]["cycle_unit"], "");

    // recurring 缺 cycle_start → 400。
    let bad_recurring = client()
        .post(&policies_url)
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "daily", "direction": "total", "mode": "recurring",
            "quota_bytes": 1000, "warning_percent": 80, "enabled": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_recurring.status(), 400);

    // 列表带 usage（无绑定 → used 0）。
    let list = client()
        .get(&policies_url)
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = list.json().await.unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"][0]["usage"]["used_bytes"], 0);

    // 更新 + usage 端点。
    let updated = client()
        .patch(format!("{policies_url}/{policy_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "monthly-2", "direction": "outbound", "mode": "cumulative",
            "quota_bytes": 2000, "warning_percent": 70, "enabled": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(updated.status(), 200);
    assert_eq!(
        updated.json::<serde_json::Value>().await.unwrap()["data"]["name"],
        "monthly-2"
    );
    let usage = client()
        .get(format!("{policies_url}/{policy_id}/usage"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(usage.status(), 200);
    let body: serde_json::Value = usage.json().await.unwrap();
    assert_eq!(body["data"]["policy_id"], policy_id);
    assert_eq!(body["data"]["status"], "normal");

    // 服务器保存的整体替换：提交 2 条 → 旧策略删除、新策略建号。
    let saved = client()
        .patch(format!("{base}/api/v2/admin/servers/{SERVER_ID}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "node-7",
            "traffic_policies": [
                {"name": "keep", "direction": "total", "mode": "cumulative",
                 "quota_bytes": 10, "warning_percent": 50, "enabled": true},
                {"name": "extra", "direction": "inbound", "mode": "cumulative",
                 "quota_bytes": 20, "warning_percent": 50, "enabled": true}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(saved.status(), 200);
    let body: serde_json::Value = saved.json().await.unwrap();
    let policies = body["data"]["traffic_policies"].as_array().unwrap();
    assert_eq!(policies.len(), 2, "旧策略被替换删除");

    // 历史序列：24 个小时桶 + 至少一个日桶。
    let history = client()
        .get(format!(
            "{base}/api/v2/admin/servers/{SERVER_ID}/traffic-history"
        ))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(history.status(), 200);
    let body: serde_json::Value = history.json().await.unwrap();
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["hourly"].as_array().unwrap().len(), 24);
    assert!(!items[0]["daily"].as_array().unwrap().is_empty());

    // 删除策略。
    let keep_id = policies[0]["id"].as_i64().unwrap();
    let deleted = client()
        .delete(format!("{policies_url}/{keep_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);
    let missing = client()
        .get(format!("{policies_url}/{keep_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);

    cancel.cancel();
    let _ = server.await;
}

/// 备份导出/导入：导出带明文密钥与策略、preview 识别 create/unchanged、
/// 导入建号（密钥复用 + 索引热更）、动作覆盖校验。
#[tokio::test]
async fn backup_export_import_round_trip() {
    let fixture = setup_primary("admin-backup", false).await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, csrf) = member_headers(&fixture, port, "tina").await;

    // 建一台带策略的服务器。
    let created = client()
        .post(format!("{base}/api/v2/admin/servers"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({
            "name": "backup-me", "tag": "prod", "display_index": 3,
            "traffic_policies": [
                {"name": "p1", "direction": "total", "mode": "cumulative",
                 "quota_bytes": 500, "warning_percent": 80, "enabled": true}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let data = created.json::<serde_json::Value>().await.unwrap()["data"].clone();
    let original_secret = data["secret"].as_str().unwrap().to_string();

    // 导出。
    let export = client()
        .get(format!("{base}/api/v2/admin/servers/export"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(export.status(), 200);
    let mut document = export.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(document["format"], "santaizi.servers.v1");
    let servers = document["servers"].as_array().unwrap().len();
    assert_eq!(servers, 2, "种子 node-7 + backup-me");

    // 预览：追加一台新机器，node-7 未改动应判 unchanged。
    let new_server = serde_json::json!({
        "name": "imported-1", "tag": "new", "display_index": 1,
        "secret": "brand-new-secret-1",
        "traffic_policies": []
    });
    document["servers"]
        .as_array_mut()
        .unwrap()
        .push(new_server.clone());
    let preview = client()
        .post(format!("{base}/api/v2/admin/servers/import/preview"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&document)
        .send()
        .await
        .unwrap();
    if preview.status() != 200 {
        let body = preview.json::<serde_json::Value>().await.unwrap();
        panic!("preview failed: {body}");
    }
    let items = preview.json::<serde_json::Value>().await.unwrap()["data"]["items"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(items.len(), 3);
    let imported = items
        .iter()
        .find(|item| item["name"] == "imported-1")
        .unwrap();
    assert_eq!(imported["match"], "create");
    let node7 = items.iter().find(|item| item["name"] == "node-7").unwrap();
    assert_eq!(node7["match"], "unchanged", "种子服务器未改动");
    let backup_me = items
        .iter()
        .find(|item| item["name"] == "backup-me")
        .unwrap();
    assert_eq!(backup_me["match"], "unchanged");

    // 导入：三行动作全覆盖（create + skip + skip）。
    let actions = items
        .iter()
        .map(|item| {
            serde_json::json!({
                "index": item["index"],
                "action": if item["name"] == "imported-1" { "create" } else { "skip" },
            })
        })
        .collect::<Vec<_>>();
    let imported = client()
        .post(format!("{base}/api/v2/admin/servers/import"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({ "document": document, "actions": actions }))
        .send()
        .await
        .unwrap();
    assert_eq!(imported.status(), 200);
    let data = imported.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data["created"], 1);
    assert_eq!(data["skipped"], 2);
    assert_eq!(
        data["secrets_reused"], 1,
        "Go 语义：新密钥无冲突即沿用（reused）"
    );
    assert_eq!(data["secrets_regenerated"], 0, "无冲突不重生成");

    // 新服务器存在且密钥可解析（秘密索引热更新）。
    let found = fixture.state.secrets().server_id("brand-new-secret-1");
    assert!(found.is_some(), "导入即热更秘密索引");

    // 动作覆盖不全 → 400。
    let incomplete = client()
        .post(format!("{base}/api/v2/admin/servers/import"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(
            &serde_json::json!({"document": {"format": "santaizi.servers.v1", "servers": [
                {"name": "x"}
            ]}}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(incomplete.status(), 400);
    assert_eq!(
        incomplete.json::<serde_json::Value>().await.unwrap()["code"],
        "server_import_conflict"
    );
    let _ = original_secret;

    cancel.cancel();
    let _ = server.await;
}
