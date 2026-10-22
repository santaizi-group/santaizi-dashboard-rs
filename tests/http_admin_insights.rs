//! Admin 读侧集成测试：summary / telemetry datasets / connections /
//! probes summary / offline-history / monitors history / admin 可用性桶 /
//! database 门。

mod common;

use common::*;
use prost::Message as _;
use serde_json::Value;

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

/// 种子：绑定 + 开放指派（primary + collector）+ agent 运行态（含 sink）+
/// collector 运行态（在线）+ 开放 incident + 数据丢失 + 告警。
fn seed_telemetry(fixture: &PrimaryFixture, now: i64) {
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason, created_at) VALUES (?1, ?2, 0, NULL, 1, 'r', CURRENT_TIMESTAMP)",
                rusqlite::params![SERVER_ID, node()],
            )?;
            tx.execute(
                "INSERT INTO observer_assignments (node_uuid, observer_id, valid_from, valid_to, config_version, generation, created_at) VALUES (?1, 'primary', ?2, 0, 1, 1, CURRENT_TIMESTAMP)",
                rusqlite::params![node(), now],
            )?;
            tx.execute(
                "INSERT INTO observer_assignments (node_uuid, observer_id, valid_from, valid_to, config_version, generation, created_at) VALUES (?1, 'collector-edge', ?2, 0, 1, 1, CURRENT_TIMESTAMP)",
                rusqlite::params![node(), now],
            )?;
            tx.execute(
                "INSERT INTO agent_telemetry_runtimes (node_uuid, wal_pressure, wal_bytes, pending_events, sink_cursors, clock_untrusted, updated_at) VALUES (?1, 1, 2048, 3, ?2, 0, CURRENT_TIMESTAMP)",
                rusqlite::params![
                    node(),
                    santaizi_dashboard::proto::AgentRuntime {
                        wal_pressure: santaizi_dashboard::proto::WalPressure::Healthy as i32,
                        wal_bytes: 2048,
                        pending_events: 3,
                        sinks: vec![
                            santaizi_dashboard::proto::SinkRuntime {
                                endpoint_id: "primary".into(),
                                connected: true,
                                // 超过 i64::MAX 的序列号：旧代码 `as i64` 会回绕成
                                // 负数，JSON 里于是出现负数序号。
                                ack_through: u64::MAX,
                                pending_events: u64::MAX - 1,
                                last_rtt_ms: 1.5,
                                rtt_sampled_at_unix_nano: now,
                                ..Default::default()
                            },
                            santaizi_dashboard::proto::SinkRuntime {
                                endpoint_id: "collector-edge".into(),
                                connected: true,
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    }
                    .encode_to_vec(),
                ],
            )?;
            tx.execute(
                "INSERT OR IGNORE INTO collector_runtimes (collector_uuid, status, last_seen, updated_at) VALUES ('collector-edge', 'online', ?1, CURRENT_TIMESTAMP)",
                [now],
            )?;
            tx.execute(
                "INSERT INTO availability_incidents (node_uuid, initial_classification, current_classification, revision, started_at, ended_at, observer_evidence) VALUES (?1, 'offline', 'offline', 1, ?2, 0, ?3)",
                rusqlite::params![
                    node(),
                    now - 600_000_000_000,
                    r#"[{"observer_id":"primary","healthy":true,"seen":false}]"#.as_bytes(),
                ],
            )?;
            tx.execute(
                "INSERT INTO telemetry_data_losses (fact_id, component_id, occurred_at, reason, lost_records, detail, created_at) VALUES (?1, 'comp-1', ?2, 2, 7, 'spool overflow', CURRENT_TIMESTAMP)",
                rusqlite::params![other_node(), now],
            )?;
            tx.execute(
                "INSERT INTO telemetry_alerts (dedup_key, alert_type, severity, node_uuid, occurred_at, message, notified, created_at) VALUES ('k-1', 'connectivity', 'warning', ?1, ?2, 'path down', 1, CURRENT_TIMESTAMP)",
                rusqlite::params![node(), now],
            )?;
            Ok(())
        })
        .unwrap();
}

/// summary / telemetry overview / 六 dataset 形态与 404。
#[tokio::test]
async fn summary_and_telemetry_datasets() {
    let fixture = setup_primary("insights-summary", false).await;
    let now = santaizi_dashboard::util::time::now_nanos();
    seed_telemetry(&fixture, now);
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, _csrf) = member_headers(&fixture, port, "olga").await;

    let response = client()
        .get(format!("{base}/api/v2/admin/summary"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let data = response.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data["total_servers"], 1);
    assert_eq!(data["active_incidents"], 1);
    assert_eq!(data["data_loss"], 1);
    assert_eq!(data["telemetry_alerts"], 1);
    assert_eq!(
        data["paths_assigned"], 2,
        "primary + collector 两条开放指派"
    );
    assert_eq!(data["active_collectors"], 1, "collector 90s 内有心跳");

    let overview = client()
        .get(format!("{base}/api/v2/admin/telemetry/overview"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let data = overview.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data["collectors"], 1);
    assert_eq!(data["agents"], 1);
    assert_eq!(data["active_assignments"], 2);
    assert_eq!(data["data_loss"], 1);
    assert_eq!(data["alerts"], 1);

    // incidents：证据标注 primary kind。
    let incidents = client()
        .get(format!("{base}/api/v2/admin/telemetry/incidents?open=1"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = incidents.json().await.unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"][0]["current_classification"], "offline");
    assert_eq!(
        body["data"][0]["observer_evidence"][0]["observer_kind"],
        "primary"
    );
    assert_eq!(body["data"][0]["observer_evidence"][0]["seen"], false);

    // data-loss：reason 标签。
    let losses = client()
        .get(format!("{base}/api/v2/admin/telemetry/data-loss"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let data = losses.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data[0]["reason"], "hard_limit_data_loss");
    assert_eq!(data[0]["lost_records"], 7);

    // alerts 与 assignments。
    let alerts = client()
        .get(format!("{base}/api/v2/admin/telemetry/alerts"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(
        alerts.json::<serde_json::Value>().await.unwrap()["data"][0]["alert_type"],
        "connectivity"
    );
    let assignments = client()
        .get(format!("{base}/api/v2/admin/telemetry/assignments"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let data = assignments.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data.as_array().unwrap().len(), 2);

    // agents：sink 展开。
    let agents = client()
        .get(format!("{base}/api/v2/admin/telemetry/agents"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let data = agents.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data[0]["wal_pressure"], "healthy");
    assert_eq!(data[0]["sinks"].as_array().unwrap().len(), 2);
    assert_eq!(data[0]["sinks"][0]["endpoint_id"], "primary");
    assert_eq!(data[0]["sinks"][0]["connected"], true);

    // 未知 dataset → 404。
    let unknown = client()
        .get(format!("{base}/api/v2/admin/telemetry/wat"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status(), 404);
    assert_eq!(
        unknown.json::<serde_json::Value>().await.unwrap()["code"],
        "dataset_not_found"
    );

    cancel.cancel();
    let _ = server.await;
}

/// connections 三端点：summary 计数、paths 的 sink 握手语义（collector 在线
/// 保留 RTT、离线清零）、latency 过滤校验。
#[tokio::test]
async fn connections_endpoints() {
    let fixture = setup_primary("insights-conn", false).await;
    let now = santaizi_dashboard::util::time::now_nanos();
    seed_telemetry(&fixture, now);
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, _csrf) = member_headers(&fixture, port, "peter").await;

    let summary = client()
        .get(format!("{base}/api/v2/admin/connections/summary"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let data = summary.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data["collectors_total"], 1);
    assert_eq!(data["collectors_online"], 1);
    assert_eq!(data["paths_assigned"], 2);
    assert_eq!(data["paths_connected"], 2, "collector 在线 → 两条都握手");
    assert_eq!(data["paths_seen"], 0, "无 path 桶");

    let paths = client()
        .get(format!("{base}/api/v2/admin/connections/paths?server_id=7"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = paths.json().await.unwrap();
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    let primary = items
        .iter()
        .find(|item| item["observer_id"] == "primary")
        .unwrap();
    assert_eq!(primary["server_name"], "node-7");
    assert_eq!(primary["observer_name"], "主面板");
    assert_eq!(
        primary["sink"]["ack_through"],
        serde_json::json!(u64::MAX),
        "uint64 序列号不得被回绕成负数"
    );
    assert_eq!(
        primary["sink"]["pending_events"],
        serde_json::json!(u64::MAX - 1)
    );
    assert_eq!(primary["sink"]["last_rtt_ms"], serde_json::json!(1.5));

    // collector 离线（心跳 10 分钟前）→ 非 primary sink 清零。
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "UPDATE collector_runtimes SET last_seen = ?1 WHERE collector_uuid = 'collector-edge'",
                [now - 600_000_000_000],
            )?;
            Ok(())
        })
        .unwrap();
    let paths = client()
        .get(format!("{base}/api/v2/admin/connections/paths"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let items = paths.json::<serde_json::Value>().await.unwrap()["data"].clone();
    for item in items.as_array().unwrap() {
        if item["observer_id"] == "collector-edge" {
            assert_eq!(item["sink"]["connected"], false, "离线从端 sink 清零");
            assert_eq!(item["sink"]["last_rtt_ms"], Value::Null);
        } else {
            assert_eq!(item["sink"]["connected"], true, "primary 免握手门槛");
        }
    }

    // latency：kind 白名单。
    let bad = client()
        .get(format!("{base}/api/v2/admin/connections/latency?kind=wat"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    let empty = client()
        .get(format!("{base}/api/v2/admin/connections/latency"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(
        empty.json::<serde_json::Value>().await.unwrap()["data"]
            .as_array()
            .unwrap()
            .len(),
        0
    );

    // 两个不同 node 各一分钟桶：无过滤时 `server_id` 必须是行的真值，
    // 不得回显过滤参数（旧实现无过滤时恒为 0，有过滤时恒为过滤值）。
    let other_node = [0x0Au8; 16];
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) VALUES (8, 'node-8', 'tag2', X'00', 2, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason, created_at) VALUES (8, ?1, 0, NULL, 1, 'r', CURRENT_TIMESTAMP)",
                [&other_node],
            )?;
            tx.execute(
                "INSERT INTO connection_latency_buckets (kind, collector_uuid, node_uuid, observer_id, bucket_start, min_ms, max_ms, sum_ms, count, updated_at) \
                 VALUES ('heartbeat', 'collector-edge', ?1, 'collector-edge', ?2, 1.0, 2.0, 3.0, 2, CURRENT_TIMESTAMP)",
                rusqlite::params![node(), now - 60_000_000_000i64],
            )?;
            tx.execute(
                "INSERT INTO connection_latency_buckets (kind, collector_uuid, node_uuid, observer_id, bucket_start, min_ms, max_ms, sum_ms, count, updated_at) \
                 VALUES ('heartbeat', 'collector-edge', ?1, 'collector-edge', ?2, 5.0, 6.0, 11.0, 2, CURRENT_TIMESTAMP)",
                rusqlite::params![other_node, now - 60_000_000_000i64],
            )?;
            Ok(())
        })
        .unwrap();
    let listed = client()
        .get(format!("{base}/api/v2/admin/connections/latency"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let items = listed.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(items.as_array().unwrap().len(), 2);
    let mut seen: Vec<i64> = items
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["server_id"].as_i64().unwrap())
        .collect();
    seen.sort_unstable();
    assert_eq!(seen, vec![7, 8], "server_id 必须是行的真值而非过滤参数");
    for item in items.as_array().unwrap() {
        if item["server_id"] == 7 {
            assert_eq!(item["server_name"], "node-7");
        } else {
            assert_eq!(item["server_name"], "node-8");
        }
    }

    cancel.cancel();
    let _ = server.await;
}

/// 探测样本分页：动态 WHERE 的占位符必须真正绑定（P0 回归）。
#[tokio::test]
async fn probe_samples_filtered_pagination() {
    let fixture = setup_primary("insights-probe-samples", false).await;
    insert_user(&fixture.db, "vera", true);
    let now = santaizi_dashboard::util::time::now_nanos();
    let bucket = |offset: i64, kind: &'static str| {
        (
            "probe-1",
            SERVER_ID,
            kind,
            22i64,
            now - offset,
            1.0f64,
            2.0f64,
            3.0f64,
            2i64,
            0.0f64,
            2i64,
            0i64,
        )
    };
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO collectors (collector_uuid, name, address, token_hash, token_ciphertext, kind, tcp_ports, revoked, deleted) VALUES ('probe-1', 'Probe1', 'p:5556', X'00', X'00', 'probe', '22', 0, 0)",
                [],
            )?;
            for (index, kind) in ["icmp", "tcp"].iter().enumerate() {
                let row = bucket(index as i64 * 60_000_000_000i64, kind);
                tx.execute(
                    "INSERT INTO probe_sample_buckets \
                     (collector_uuid, server_id, kind, port, bucket_start, min_ms, max_ms, sum_ms, count, loss_sum, success_count, fail_count, updated_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, CURRENT_TIMESTAMP)",
                    rusqlite::params![row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9, row.10, row.11],
                )?;
            }
            Ok(())
        })
        .unwrap();
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, _csrf) = member_headers(&fixture, port, "vera").await;

    // 无过滤：全部过滤条件都为空，历史上是唯一「能跑」的路径。
    let all = client()
        .get(format!("{base}/api/v2/admin/probes/samples"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(all.status(), 200);
    assert_eq!(
        all.json::<serde_json::Value>().await.unwrap()["data"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    // 任一过滤条件都会触发 `InvalidParameterCount`（P0）。
    for (query, expected) in [
        ("?collector_id=probe-1".to_string(), 2usize),
        ("?collector_id=nope".to_string(), 0),
        ("?kind=icmp".to_string(), 1),
        ("?kind=tcp".to_string(), 1),
        (format!("?server_id={SERVER_ID}"), 2),
        ("?server_id=999".to_string(), 0),
        ("?port=22".to_string(), 2),
        ("?port=23".to_string(), 0),
        (
            format!("?collector_id=probe-1&kind=icmp&server_id={SERVER_ID}&port=22"),
            1,
        ),
    ] {
        let filtered = client()
            .get(format!("{base}/api/v2/admin/probes/samples{query}"))
            .header(reqwest::header::COOKIE, &cookie_header)
            .send()
            .await
            .unwrap();
        assert_eq!(filtered.status(), 200, "query {query} 不得 500");
        let data = filtered.json::<serde_json::Value>().await.unwrap()["data"].clone();
        assert_eq!(
            data.as_array().unwrap().len(),
            expected,
            "query {query} 行数不符"
        );
    }

    // 分页参数同样走绑定：offset 越界返回空而不是报错。
    let paged = client()
        .get(format!(
            "{base}/api/v2/admin/probes/samples?page=2&page_size=1"
        ))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(paged.status(), 200);
    let data = paged.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data.as_array().unwrap().len(), 1);
    assert_eq!(data[0]["server_name"], "node-7");

    cancel.cancel();
    let _ = server.await;
}

/// probes summary：探测从端计数与指派笛卡尔积。
#[tokio::test]
async fn probes_summary_endpoint() {
    let fixture = setup_primary("insights-probes", false).await;
    let member = insert_user(&fixture.db, "quinn", true);
    let _ = member;
    let user_id = user_id_by_login(&fixture.db, "quinn");
    let write_token = insert_api_token(&fixture.db, user_id, "write");
    let now = santaizi_dashboard::util::time::now_nanos();
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO collectors (collector_uuid, name, address, token_hash, token_ciphertext, kind, tcp_ports, revoked, deleted) VALUES ('probe-1', 'Probe1', 'p:5556', X'00', X'00', 'probe', '22', 0, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO collector_scopes (collector_uuid, scope_type, scope_value) VALUES ('probe-1', 'all', '')",
                [],
            )?;
            tx.execute(
                "INSERT INTO collector_runtimes (collector_uuid, status, last_seen, updated_at) VALUES ('probe-1', 'online', ?1, CURRENT_TIMESTAMP)",
                [now],
            )?;
            Ok(())
        })
        .unwrap();
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, _csrf) = member_headers(&fixture, port, "quinn").await;

    let response = client()
        .get(format!("{base}/api/v2/admin/probes/summary"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let data = response.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data["collectors_total"], 1);
    assert_eq!(data["collectors_online"], 1);
    assert_eq!(data["paths_assigned"], 1, "scope all × 种子服务器");
    assert_eq!(
        data["paths_no_target"], 1,
        "种子服务器没有探测目标，也没有最近地址"
    );

    // 路由任务入队：Bearer 免 CSRF。
    let queued = client()
        .post(format!("{base}/api/v2/admin/probes/route"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {write_token}"),
        )
        .json(&serde_json::json!({
            "collector_id": "probe-1", "server_id": SERVER_ID, "protocol": "icmp"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(queued.status(), 202);
    let data = queued.json::<serde_json::Value>().await.unwrap()["data"].clone();
    assert_eq!(data["protocol"], "icmp");
    assert_eq!(data["status"], "pending");

    cancel.cancel();
    let _ = server.await;
}

/// offline-history 三端点与 monitors history 游标。
#[tokio::test]
async fn offline_history_and_monitor_history() {
    let fixture = setup_primary("insights-offline", false).await;
    let now = santaizi_dashboard::util::time::now_nanos();
    use santaizi_dashboard::util::time as util_time;
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO server_offline_histories (server_id, started_at, ended_at, status) VALUES (?1, ?2, ?3, 'closed')",
                rusqlite::params![
                    SERVER_ID,
                    util_time::unix_nanos_to_sqlite(now - 7_200_000_000_000i64),
                    util_time::unix_nanos_to_sqlite(now - 3_600_000_000_000i64),
                ],
            )?;
            tx.execute(
                "INSERT INTO monitor_histories (monitor_id, server_id, avg_delay, up, down, created_at) VALUES (1, ?1, 12.5, 3, 1, ?2)",
                rusqlite::params![SERVER_ID, util_time::unix_nanos_to_sqlite(now)],
            )?;
            tx.execute(
                "INSERT INTO monitor_histories (monitor_id, server_id, avg_delay, up, down, created_at) VALUES (1, ?1, 13.5, 2, 1, ?2)",
                rusqlite::params![SERVER_ID, util_time::unix_nanos_to_sqlite(now - 60_000_000_000i64)],
            )?;
            Ok(())
        })
        .unwrap();
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, csrf) = member_headers(&fixture, port, "ruth").await;

    // server_id 必填。
    let missing = client()
        .get(format!("{base}/api/v2/admin/offline-history"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 400);

    let list = client()
        .get(format!(
            "{base}/api/v2/admin/offline-history?server_id={SERVER_ID}"
        ))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = list.json().await.unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"][0]["status"], "closed");
    let history_id = body["data"][0]["id"].as_i64().unwrap();

    // 单条删除 → 204 → 再删 404。
    let deleted = client()
        .delete(format!("{base}/api/v2/admin/offline-history/{history_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);
    let again = client()
        .delete(format!("{base}/api/v2/admin/offline-history/{history_id}"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(again.status(), 404);

    // 清理：只删已结束区间（重种一条已结束的）。
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO server_offline_histories (server_id, started_at, ended_at, status) \
                 VALUES (?1, ?2, ?3, 'closed')",
                rusqlite::params![
                    SERVER_ID,
                    util_time::unix_nanos_to_sqlite(now - 7_200_000_000_000i64),
                    util_time::unix_nanos_to_sqlite(now - 3_600_000_000_000i64),
                ],
            )?;
            Ok(())
        })
        .unwrap();
    let cleanup = client()
        .post(format!("{base}/api/v2/admin/offline-history/cleanup"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"server_id": SERVER_ID, "before": util_time::unix_nanos_to_rfc3339(util_time::now_nanos())}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        cleanup.json::<serde_json::Value>().await.unwrap()["data"]["deleted"],
        1
    );

    // monitors history：游标分页。
    let history = client()
        .get(format!("{base}/api/v2/admin/monitors/1/history?limit=1"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    if history.status() != 200 {
        let code = history.json::<serde_json::Value>().await.unwrap();
        panic!("monitor history status != 200: {code}");
    }
    let body: serde_json::Value = history.json().await.unwrap();
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0]["avg_delay"],
        serde_json::json!(13.5),
        "id DESC 最新在前（后插 id 更大）"
    );
    let cursor = body["meta"]["next_cursor"].as_str().unwrap().to_string();
    assert!(!cursor.is_empty(), "还有下一页");

    let second = client()
        .get(format!(
            "{base}/api/v2/admin/monitors/1/history?limit=1&cursor={cursor}"
        ))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = second.json().await.unwrap();
    assert_eq!(body["data"][0]["avg_delay"], serde_json::json!(12.5));
    assert!(
        body["meta"].get("next_cursor").is_none()
            || body["meta"]["next_cursor"] == serde_json::json!(""),
        "末页无游标"
    );

    cancel.cancel();
    let _ = server.await;
}

/// admin 可用性桶列表（游标）+ database 门（夹具无维护器 → 500）。
#[tokio::test]
async fn admin_availability_and_database_gate() {
    let fixture = setup_primary("insights-avail", false).await;
    let now = santaizi_dashboard::util::time::now_nanos();
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason, created_at) VALUES (?1, ?2, 0, NULL, 1, 'r', CURRENT_TIMESTAMP)",
                rusqlite::params![SERVER_ID, node()],
            )?;
            for index in 0..3i64 {
                tx.execute(
                    "INSERT INTO availability_buckets (node_uuid, bucket_start, window_end, resolution, host_state, connectivity_state, expected_observers, healthy_observers, seen_observers, finalized, observer_summary) VALUES (?1, ?2, ?3, '30s', 'online', 'full', 1, 1, 1, 1, ?4)",
                    rusqlite::params![
                        node(),
                        now - (index + 1) * 60_000_000_000,
                        now - index * 60_000_000_000,
                        br#"[{"observer_id":"primary","healthy":true,"seen":true}]"#.as_slice(),
                    ],
                )?;
            }
            Ok(())
        })
        .unwrap();
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let (cookie_header, _csrf) = member_headers(&fixture, port, "sara").await;

    let response = client()
        .get(format!(
            "{base}/api/v2/admin/servers/{SERVER_ID}/availability?limit=2"
        ))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = response.json().await.unwrap();
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 2, "limit 截断");
    assert_eq!(items[0]["connectivity"], "full");
    assert_eq!(items[0]["available"], serde_json::json!(true));
    assert_eq!(items[0]["coverage"], "1/1");
    assert_eq!(items[0]["observer_evidence"][0]["observer_kind"], "primary");
    let cursor = body["meta"]["next_cursor"].as_str().unwrap().to_string();
    assert!(!cursor.is_empty());

    let next = client()
        .get(format!(
            "{base}/api/v2/admin/servers/{SERVER_ID}/availability?limit=2&cursor={cursor}"
        ))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = next.json().await.unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 1, "游标后剩 1 条");

    // database：夹具未接维护器 → 500 problem。
    let database = client()
        .get(format!("{base}/api/v2/admin/database"))
        .header(reqwest::header::COOKIE, &cookie_header)
        .send()
        .await
        .unwrap();
    assert_eq!(database.status(), 500);
    assert_eq!(
        database.json::<serde_json::Value>().await.unwrap()["code"],
        "database_maintenance_unavailable"
    );

    cancel.cancel();
    let _ = server.await;
}
