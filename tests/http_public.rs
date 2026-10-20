//! Public v2 端点集成测试：bootstrap / 查看密码 / 公开服务器列表与详情。
//!
//! 覆盖访问门槛（无密码 / 查看密码 / 会员 / Bearer）、`-vp` Cookie 签发与
//! 校验、快照行形态（遥测共识、host 脱敏、token_full 展开）与排序。

mod common;

use common::*;
use prost::Message as _;
use santaizi_dashboard::proto;

/// 固定接收时刻：2026-10-15T13:40:00Z（断言 RFC3339 回显）。
const RECEIVED_AT_NANOS: i64 = 1_792_071_600_000_000_000;

/// 给夹具种子服务器（id=SERVER_ID）写一条 V2 运行态 + 已封窗可用性桶。
fn seed_runtime(fixture: &PrimaryFixture, host_ip: &str) {
    use santaizi_dashboard::util::time as util_time;
    let state_payload = proto::State {
        cpu: 42.5,
        mem_used: 2048,
        uptime: 1000,
        ..Default::default()
    }
    .encode_to_vec();
    let host_payload = proto::Host {
        platform: "debian".into(),
        platform_version: "12".into(),
        cpu: vec!["cpu0".into()],
        mem_total: 8192,
        disk_total: 100,
        arch: "amd64".into(),
        virtualization: "kvm".into(),
        boot_time: 123,
        ip: host_ip.into(),
        country_code: "CN".into(),
        version: "v1.0.0".into(),
        ..Default::default()
    }
    .encode_to_vec();
    let now = util_time::now_nanos();
    let bucket_start = now - 60_000_000_000;
    let window_end = now - 30_000_000_000;
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO server_runtimes (server_id, status, protocol, current_node_uuid, \
             host_state, connectivity_state, last_collected_at, last_received_at, \
             state_payload, host_payload) \
             VALUES (?1, 'online', 'v2', ?2, 'online', 'full', ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    SERVER_ID,
                    node(),
                    RECEIVED_AT_NANOS,
                    RECEIVED_AT_NANOS,
                    state_payload,
                    host_payload
                ],
            )?;
            tx.execute(
            "INSERT INTO availability_buckets (node_uuid, bucket_start, window_end, resolution, \
             host_state, connectivity_state, expected_observers, healthy_observers, \
             seen_observers, finalized) \
             VALUES (?1, ?2, ?3, '30s', 'online', 'full', 3, 3, 2, 1)",
            rusqlite::params![node(), bucket_start, window_end],
        )?;
            Ok(())
        })
        .unwrap();
}

/// 匿名 bootstrap：站点默认态 + CSRF 令牌。
#[tokio::test]
async fn bootstrap_anonymous_reports_site_state() {
    let fixture = setup_primary("public-boot", false).await;
    let (port, cancel, server) = spawn_http(&fixture).await;

    let response = client()
        .get(format!("{}/api/v2/public/bootstrap", base_url(port)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let csrf = cookie_value(&response, "santaizi-dashboard-csrf").expect("csrf cookie");
    let body: serde_json::Value = response.json().await.unwrap();
    let data = &body["data"];
    assert_eq!(data["brand"], "Santaizi Monitoring");
    assert_eq!(data["theme"], "server-status");
    assert_eq!(data["requires_view_password"], false);
    assert_eq!(data["view_password_verified"], false);
    assert_eq!(data["authenticated"], false);
    assert_eq!(data["show_availability"], false);
    assert_eq!(data["allow_frontend_theme_switch"], true);
    assert_eq!(data["logo_url"], "/static/logo.svg");
    assert_eq!(data["csrf_token"], serde_json::json!(csrf));
    assert_eq!(data["version"], santaizi_dashboard::VERSION);

    cancel.cancel();
    let _ = server.await;
}

/// 查看密码：配了就要过门；错误密码 403；正确密码签 `-vp` Cookie 并放行。
#[tokio::test]
async fn view_password_gates_public_endpoints() {
    let fixture = setup_primary_with("public-vp", false, |conf| {
        conf.site.view_password = "s3cret".into();
    })
    .await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);

    // 无密码会话：bootstrap 提示需要，servers 直接 403。
    let bootstrap = client()
        .get(format!("{base}/api/v2/public/bootstrap"))
        .send()
        .await
        .unwrap();
    let data = &bootstrap.json::<serde_json::Value>().await.unwrap()["data"];
    assert_eq!(data["requires_view_password"], true);
    assert_eq!(data["view_password_verified"], false);

    let gated = client()
        .get(format!("{base}/api/v2/public/servers"))
        .send()
        .await
        .unwrap();
    assert_eq!(gated.status(), 403);
    assert_eq!(
        gated.headers().get(reqwest::header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );
    assert_eq!(
        gated.json::<serde_json::Value>().await.unwrap()["code"],
        "view_password_required"
    );

    // 现取 CSRF（Cookie + 头配对）。
    let warmup = client()
        .get(format!("{base}/api/v2/public/bootstrap"))
        .send()
        .await
        .unwrap();
    let csrf = cookie_value(&warmup, "santaizi-dashboard-csrf").expect("csrf cookie");

    // 错误密码 → 403 invalid_view_password。
    let wrong = client()
        .post(format!("{base}/api/v2/public/view-password/session"))
        .header(
            reqwest::header::COOKIE,
            format!("santaizi-dashboard-csrf={csrf}"),
        )
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"password": "nope"}))
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), 403);
    assert_eq!(
        wrong.json::<serde_json::Value>().await.unwrap()["code"],
        "invalid_view_password"
    );

    // 缺 password 字段 → 400 invalid_request。
    let malformed = client()
        .post(format!("{base}/api/v2/public/view-password/session"))
        .header(
            reqwest::header::COOKIE,
            format!("santaizi-dashboard-csrf={csrf}"),
        )
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), 400);
    assert_eq!(
        malformed.json::<serde_json::Value>().await.unwrap()["code"],
        "invalid_request"
    );

    // 正确密码 → 204 + `-vp` Cookie。
    let ok = client()
        .post(format!("{base}/api/v2/public/view-password/session"))
        .header(
            reqwest::header::COOKIE,
            format!("santaizi-dashboard-csrf={csrf}"),
        )
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"password": "s3cret"}))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 204);
    let vp = cookie_value(&ok, "santaizi-dashboard-vp").expect("vp cookie");
    assert!(!vp.is_empty());

    // 带上 `-vp`：bootstrap 标记已验证，servers 放行。
    let verified = client()
        .get(format!("{base}/api/v2/public/bootstrap"))
        .header(
            reqwest::header::COOKIE,
            format!("santaizi-dashboard-vp={vp}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        verified.json::<serde_json::Value>().await.unwrap()["data"]["view_password_verified"],
        true
    );
    let servers = client()
        .get(format!("{base}/api/v2/public/servers"))
        .header(
            reqwest::header::COOKIE,
            format!("santaizi-dashboard-vp={vp}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(servers.status(), 200);

    cancel.cancel();
    let _ = server.await;
}

/// 快照行形态：遥测取已封窗共识桶、host 脱敏、online 与 last_active。
#[tokio::test]
async fn public_servers_snapshot_shapes_row() {
    let fixture = setup_primary("public-list", false).await;
    seed_runtime(&fixture, "10.0.0.7");
    let (port, cancel, server) = spawn_http(&fixture).await;

    let response = client()
        .get(format!("{}/api/v2/public/servers", base_url(port)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    let row = &items[0];
    assert_eq!(row["id"], 7);
    assert_eq!(row["name"], "node-7");
    assert_eq!(row["tag"], "tag1");
    assert_eq!(row["public_note"], serde_json::json!({}));
    assert_eq!(row["hide_for_guest"], false);
    assert_eq!(row["online"], true);
    assert_eq!(row["last_active"], "2026-10-15T13:40:00Z");
    assert_eq!(row["telemetry"]["host"], "online");
    assert_eq!(row["telemetry"]["connectivity"], "full");
    assert_eq!(row["telemetry"]["available"], true);
    assert_eq!(row["telemetry"]["coverage"], "2/3");
    assert_eq!(row["state"]["CPU"], 42.5);
    assert_eq!(row["state"]["MemUsed"], 2048);
    assert_eq!(row["host"]["Platform"], "debian");
    assert_eq!(row["host"]["MemTotal"], 8192);
    assert!(row["host"].get("ip").is_none(), "匿名不暴露 IP");
    assert!(row.get("note").is_none(), "匿名不带管理字段");
    assert_eq!(body["meta"]["page"], 1);
    assert_eq!(body["meta"]["page_size"], 1);
    assert_eq!(body["meta"]["total"], 1);

    cancel.cancel();
    let _ = server.await;
}

/// 详情端点：命中与 404（含坏 ID）。
#[tokio::test]
async fn public_server_detail_and_not_found() {
    let fixture = setup_primary("public-detail", false).await;
    seed_runtime(&fixture, "10.0.0.7");
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);

    let hit = client()
        .get(format!("{base}/api/v2/public/servers/7"))
        .send()
        .await
        .unwrap();
    assert_eq!(hit.status(), 200);
    assert_eq!(
        hit.json::<serde_json::Value>().await.unwrap()["data"]["id"],
        7
    );

    let missing = client()
        .get(format!("{base}/api/v2/public/servers/999"))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    assert_eq!(
        missing.json::<serde_json::Value>().await.unwrap()["code"],
        "server_not_found"
    );

    cancel.cancel();
    let _ = server.await;
}

/// Bearer Token：host 展开 IP/ipv4，并附带 note 与 monitoring_options。
#[tokio::test]
async fn public_servers_bearer_reveals_ip_and_admin_fields() {
    let fixture = setup_primary("public-bearer", false).await;
    seed_runtime(&fixture, "10.0.0.7");
    insert_user(&fixture.db, "grace", false);
    let user_id = user_id_by_login(&fixture.db, "grace");
    let plain = insert_api_token(&fixture.db, user_id, "write");
    let (port, cancel, server) = spawn_http(&fixture).await;

    let response = client()
        .get(format!("{}/api/v2/public/servers", base_url(port)))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {plain}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    let row = &body["data"][0];
    assert_eq!(row["host"]["ip"], "10.0.0.7");
    assert_eq!(row["host"]["ipv4"], "10.0.0.7");
    assert_eq!(row["note"], "");
    assert_eq!(row["monitoring_options"], serde_json::json!({}));

    cancel.cancel();
    let _ = server.await;
}

/// `hide_for_guest`：匿名过滤、会员可见（Go ForGuest / 全量双列表）。
#[tokio::test]
async fn hidden_server_filtered_for_guest_only() {
    let fixture = setup_primary("public-hidden", false).await;
    let ciphertext = santaizi_dashboard::secret::encrypt_secret("secret-hidden").unwrap();
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) \
                 VALUES (8, 'ghost', 'tag2', ?, 2, 1)",
                rusqlite::params![&ciphertext],
            )?;
            Ok(())
        })
        .unwrap();
    let member = insert_user(&fixture.db, "heidi", true);
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);

    let anon = client()
        .get(format!("{base}/api/v2/public/servers"))
        .send()
        .await
        .unwrap();
    let items = anon.json::<serde_json::Value>().await.unwrap()["data"].clone();
    let ids: Vec<i64> = items
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, vec![7], "匿名只见未隐藏的种子服务器");

    let member_response = client()
        .get(format!("{base}/api/v2/public/servers"))
        .header(
            reqwest::header::COOKIE,
            format!("santaizi-dashboard={member}"),
        )
        .send()
        .await
        .unwrap();
    let items = member_response.json::<serde_json::Value>().await.unwrap()["data"].clone();
    let ids: Vec<i64> = items
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, vec![8, 7], "display_index DESC，隐藏服务器靠前");

    cancel.cancel();
    let _ = server.await;
}

/// 可用性摘要种子：server_runtimes 带 last_seen_at（已上报）+ 一段已结束的
/// 1 小时离线历史。
fn seed_availability_summary(fixture: &PrimaryFixture) {
    use santaizi_dashboard::util::time as util_time;
    let now = util_time::now_nanos();
    let hour = 3_600_000_000_000;
    fixture
        .db
        .write(|tx| {
            // last_seen_at 是 datetime 列；last_received_at 是 INTEGER 纳秒列。
            tx.execute(
                "INSERT INTO server_runtimes (server_id, status, protocol, last_seen_at, last_received_at) \
                 VALUES (?1, 'online', 'v2', ?2, ?3)",
                rusqlite::params![
                    SERVER_ID,
                    util_time::unix_nanos_to_sqlite(now),
                    now
                ],
            )?;
            tx.execute(
                "INSERT INTO server_offline_histories (server_id, started_at, ended_at, status) \
                 VALUES (?1, ?2, ?3, 'closed')",
                rusqlite::params![
                    SERVER_ID,
                    util_time::unix_nanos_to_sqlite(now - 2 * hour),
                    util_time::unix_nanos_to_sqlite(now - hour),
                ],
            )?;
            Ok(())
        })
        .unwrap();
}

/// `/servers/:id/availability`：匿名 403（前台可用性关闭）、会员统计、
/// days 缺省/非法回落 30、未知服务器 404。
#[tokio::test]
async fn server_availability_summary_and_gates() {
    let fixture = setup_primary("public-avail", false).await;
    seed_availability_summary(&fixture);
    let member = insert_user(&fixture.db, "avery", false);
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let auth = format!("santaizi-dashboard={member}");

    // 匿名：默认 show_availability_to_guest=false → 403。
    let anon = client()
        .get(format!("{base}/api/v2/public/servers/7/availability"))
        .send()
        .await
        .unwrap();
    assert_eq!(anon.status(), 403);
    assert_eq!(
        anon.json::<serde_json::Value>().await.unwrap()["code"],
        "availability_hidden"
    );

    // 会员：30 天窗（缺省）含一段 1h 离线 → 99.86%。
    let response = client()
        .get(format!("{base}/api/v2/public/servers/7/availability"))
        .header(reqwest::header::COOKIE, &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let data = response.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data["server_id"], 7);
    assert_eq!(data["days"], 30);
    assert_eq!(data["offline_count"], 1);
    assert_eq!(data["total_offline_seconds"], 3600);
    assert_eq!(data["longest_offline_seconds"], 3600);
    assert_eq!(
        data["availability_percent"],
        serde_json::json!(99.86),
        "30 天窗 1h 离线：(1-3600/2592000)*100 向下取整"
    );

    // days=7 → 99.40%。
    let week = client()
        .get(format!(
            "{base}/api/v2/public/servers/7/availability?days=7"
        ))
        .header(reqwest::header::COOKIE, &auth)
        .send()
        .await
        .unwrap();
    let data = week.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data["days"], 7);
    assert_eq!(data["availability_percent"], serde_json::json!(99.40));

    // 缺省/非法/负数/零一律回落 30（Go DefaultQuery 语义）。
    for query in ["?days=abc", "?days=-3", "?days=0"] {
        let fallback = client()
            .get(format!(
                "{base}/api/v2/public/servers/7/availability{query}"
            ))
            .header(reqwest::header::COOKIE, &auth)
            .send()
            .await
            .unwrap();
        assert_eq!(
            fallback.json::<serde_json::Value>().await.unwrap()["data"]["days"],
            30,
            "days{query} 应回落 30"
        );
    }

    // 未知服务器 → 404。
    let missing = client()
        .get(format!("{base}/api/v2/public/servers/999/availability"))
        .header(reqwest::header::COOKIE, &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    assert_eq!(
        missing.json::<serde_json::Value>().await.unwrap()["code"],
        "server_not_found"
    );

    cancel.cancel();
    let _ = server.await;
}

/// 指标端点种子：node 绑定 + 两条 1m（一条无 average 验证速率回落）+
/// 一条 1h rollup，全部落在最近窗口内。
fn seed_metric_rollups(fixture: &PrimaryFixture) -> (i64, i64, i64) {
    use santaizi_dashboard::util::time as util_time;
    let now = util_time::now_nanos();
    let minute = 60_000_000_000;
    let hour = 60 * minute;
    let m0 = now / minute * minute - 2 * minute;
    let m1 = m0 + minute;
    let h0 = now / hour * hour - hour;
    let rows: [(String, i64, i64, Option<proto::State>, u64, u64); 3] = [
        (
            "1m".into(),
            m0,
            m0 + minute,
            Some(proto::State {
                cpu: 42.5,
                mem_used: 2048,
                net_in_speed: 300,
                net_out_speed: 500,
                process_count: 12,
                tcp_conn_count: 30,
                udp_conn_count: 4,
                ..Default::default()
            }),
            3000,
            7000,
        ),
        ("1m".into(), m1, m1 + minute, None, 1200, 0),
        (
            "1h".into(),
            h0,
            h0 + hour,
            Some(proto::State {
                cpu: 10.0,
                net_in_speed: 90_000,
                ..Default::default()
            }),
            1_000_000,
            2_000_000,
        ),
    ];
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason, created_at) \
                 VALUES (?1, ?2, 0, NULL, 1, 'r', CURRENT_TIMESTAMP)",
                rusqlite::params![SERVER_ID, node()],
            )?;
            for (resolution, start, end, average, net_in, net_out) in rows {
                let payload = proto::StateRollupPayload {
                    window_start_unix_nano: start,
                    window_end_unix_nano: end,
                    sample_count: 1,
                    average,
                    net_in_total: net_in,
                    net_out_total: net_out,
                    ..Default::default()
                }
                .encode_to_vec();
                tx.execute(
                    "INSERT INTO state_rollups (node_uuid, resolution, window_start, window_end, sample_count, payload, net_in_total, net_out_total) \
                     VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, ?7)",
                    rusqlite::params![node(), resolution, start, end, payload, net_in as i64, net_out as i64],
                )?;
            }
            Ok(())
        })
        .unwrap();
    (m0, m1, h0)
}

/// `/metrics/:id`：1m/1h 窗口解码（含无 average 的速率回落）、无效 hours 400、
/// 坏 start 400、未知服务器 404。
#[tokio::test]
async fn public_metrics_windows_and_validation() {
    use santaizi_dashboard::util::time as util_time;
    let fixture = setup_primary("public-metrics", false).await;
    let (m0, m1, _h0) = seed_metric_rollups(&fixture);
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);

    // 缺省 1m 分辨率、24h 窗口 → 两条 1m 行。
    let response = client()
        .get(format!("{base}/api/v2/public/metrics/7?hours=1"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    let points = body["data"].as_array().unwrap();
    assert_eq!(points.len(), 2);
    let p0 = &points[0];
    assert_eq!(
        p0["window_start"].as_str().unwrap(),
        util_time::unix_nanos_to_rfc3339(m0)
    );
    assert_eq!(p0["cpu"], 42.5);
    assert_eq!(p0["mem_used"], 2048);
    assert_eq!(p0["net_in_speed"], 300, "avg 优先");
    assert_eq!(p0["net_out_speed"], 500);
    assert_eq!(p0["net_in_total"], 3000);
    assert_eq!(p0["net_out_total"], 7000);
    assert_eq!(p0["process_count"], 12);
    assert_eq!(p0["tcp_conn_count"], 30);
    assert_eq!(p0["udp_conn_count"], 4);
    // 第二条无 average：net_in 速率按总量/窗秒回落（1200/60 = 20）。
    assert_eq!(
        points[1]["window_start"].as_str().unwrap(),
        util_time::unix_nanos_to_rfc3339(m1)
    );
    assert_eq!(points[1]["net_in_speed"], 20);
    assert_eq!(points[1]["net_out_speed"], 0);
    assert_eq!(points[1]["cpu"], serde_json::json!(0.0));
    assert_eq!(body["meta"]["total"], 2);

    // 1h 分辨率 + 2h 窗口 → 一条小时行。
    let hourly = client()
        .get(format!(
            "{base}/api/v2/public/metrics/7?resolution=1h&hours=2"
        ))
        .send()
        .await
        .unwrap();
    let points = hourly.json::<serde_json::Value>().await.unwrap()["data"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(points.len(), 1);
    assert_eq!(points[0]["net_in_total"], 1_000_000);
    assert_eq!(points[0]["net_out_total"], 2_000_000);
    assert_eq!(points[0]["net_in_speed"], 90_000);

    // 校验形态。
    let invalid_hours = client()
        .get(format!("{base}/api/v2/public/metrics/7?hours=0.4"))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_hours.status(), 400);
    assert_eq!(
        invalid_hours.json::<serde_json::Value>().await.unwrap()["code"],
        "invalid_hours"
    );
    let bad_start = client()
        .get(format!(
            "{base}/api/v2/public/metrics/7?start=x&end=2026-10-16T00:00:00Z"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_start.status(), 400);
    assert_eq!(
        bad_start.json::<serde_json::Value>().await.unwrap()["code"],
        "invalid_start"
    );
    let missing = client()
        .get(format!("{base}/api/v2/public/metrics/999"))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    assert_eq!(
        missing.json::<serde_json::Value>().await.unwrap()["code"],
        "server_not_found"
    );

    cancel.cancel();
    let _ = server.await;
}

/// 周期流量种子：node 绑定 + 一个整点 1h rollup（in 300 / out 700）+
/// 三条策略（可算 / 参数非法 / 停用）。
fn seed_cycle_transfer(fixture: &PrimaryFixture) -> i64 {
    use santaizi_dashboard::util::time as util_time;
    let now = util_time::now_nanos();
    let hour = 3_600_000_000_000;
    let window_start = now / hour * hour - hour;
    let payload = proto::StateRollupPayload {
        window_start_unix_nano: window_start,
        window_end_unix_nano: window_start + hour,
        sample_count: 1,
        net_in_total: 300,
        net_out_total: 700,
        ..Default::default()
    }
    .encode_to_vec();
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason, created_at) \
                 VALUES (?1, ?2, 0, NULL, 1, 'r', CURRENT_TIMESTAMP)",
                rusqlite::params![SERVER_ID, node()],
            )?;
            tx.execute(
                "INSERT INTO state_rollups (node_uuid, resolution, window_start, window_end, sample_count, payload, net_in_total, net_out_total) \
                 VALUES (?1, '1h', ?2, ?3, 1, ?4, 300, 700)",
                rusqlite::params![
                    node(),
                    window_start,
                    window_start + hour,
                    payload
                ],
            )?;
            // 可算：cumulative，窗口起点 = 该整点，quota 1250 → 80% warning。
            tx.execute(
                "INSERT INTO traffic_policies (created_at, server_id, name, direction, mode, cycle_start, cycle_interval, cycle_unit, quota_bytes, warning_percent, enabled) \
                 VALUES (?1, ?2, 'monthly', 'total', 'cumulative', ?3, 1, '', 1250, 80, 1)",
                rusqlite::params![
                    util_time::unix_nanos_to_sqlite(window_start),
                    SERVER_ID,
                    util_time::unix_nanos_to_sqlite(window_start),
                ],
            )?;
            // recurring 缺 cycle_start → 只回基础字段。
            tx.execute(
                "INSERT INTO traffic_policies (created_at, server_id, name, direction, mode, cycle_interval, cycle_unit, quota_bytes, warning_percent, enabled) \
                 VALUES (?1, ?2, 'broken', 'total', 'recurring', 1, 'day', 1000, 80, 1)",
                rusqlite::params![util_time::unix_nanos_to_sqlite(window_start), SERVER_ID],
            )?;
            // 停用：不出现。
            tx.execute(
                "INSERT INTO traffic_policies (created_at, server_id, name, direction, mode, cycle_start, cycle_interval, cycle_unit, quota_bytes, warning_percent, enabled) \
                 VALUES (?1, ?2, 'off', 'total', 'cumulative', ?3, 1, '', 1000, 80, 0)",
                rusqlite::params![
                    util_time::unix_nanos_to_sqlite(window_start),
                    SERVER_ID,
                    util_time::unix_nanos_to_sqlite(window_start),
                ],
            )?;
            Ok(())
        })
        .unwrap();
    window_start
}

/// `/cycle-transfer`：用量字段、非法策略只回基础字段、server_id 过滤与校验。
#[tokio::test]
async fn cycle_transfer_lists_and_computes_usage() {
    use santaizi_dashboard::util::time as util_time;
    let fixture = setup_primary("public-cycle", false).await;
    let window_start = seed_cycle_transfer(&fixture);
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);

    let response = client()
        .get(format!("{base}/api/v2/public/cycle-transfer"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 2, "停用策略不出现");
    assert_eq!(body["meta"]["total"], 2);

    let computed = &items[0];
    assert_eq!(computed["policy_id"], 1);
    assert_eq!(computed["server_id"], 7);
    assert_eq!(computed["name"], "monthly");
    assert_eq!(computed["direction"], "total");
    assert_eq!(computed["mode"], "cumulative");
    assert_eq!(computed["cycle_unit"], "");
    assert_eq!(
        computed["window_start"].as_str().unwrap(),
        util_time::unix_nanos_to_rfc3339(window_start)
    );
    assert!(computed.get("window_end").is_none(), "cumulative 无窗末");
    assert_eq!(computed["next_reset_at"], serde_json::Value::Null);
    assert_eq!(computed["used_bytes"], 1000, "整点 1h 行 in+out");
    assert_eq!(computed["quota_bytes"], 1250);
    assert_eq!(computed["usage_percent"], serde_json::json!(80.0));
    assert_eq!(computed["status"], "warning");
    assert_eq!(computed["warning_percent"], serde_json::json!(80.0));
    assert_eq!(computed["warning_bytes"], 1000);
    assert_eq!(computed["remaining_bytes"], 250);

    let broken = &items[1];
    assert_eq!(broken["policy_id"], 2);
    assert_eq!(broken["name"], "broken");
    assert!(
        broken.get("used_bytes").is_none(),
        "非法策略不带 usage 字段（Go Calculate 报错静默省略）"
    );

    // server_id 过滤与校验。
    let empty = client()
        .get(format!("{base}/api/v2/public/cycle-transfer?server_id=999"))
        .send()
        .await
        .unwrap();
    let items = empty.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(items.as_array().unwrap().len(), 0);
    let invalid = client()
        .get(format!("{base}/api/v2/public/cycle-transfer?server_id=abc"))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), 400);
    assert_eq!(
        invalid.json::<serde_json::Value>().await.unwrap()["code"],
        "invalid_server_id"
    );

    cancel.cancel();
    let _ = server.await;
}
