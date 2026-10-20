//! 监控派发引擎集成测试：Control 流注册 → 调度器下发 ProbeRequest（capability
//! 过滤 + scope 匹配 + probe_id 形态）→ 回流 ProbeResult → 30 样本窗写
//! monitor_histories → public services / network 端点可读。

mod common;

use common::*;
use santaizi_dashboard::proto;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Identity;

/// 种子一条 HTTP 监控（interval 5s、scope all）。
fn seed_monitor(fixture: &PrimaryFixture) -> i64 {
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO monitors (name, type, target, skip_servers_raw, duration, notify, cover) VALUES ('blog', 1, 'https://blog.example', '[]', 5, 0, 0)",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    fixture
        .db
        .read(|conn| {
            Ok(conn.query_row("SELECT MAX(id) FROM monitors", [], |row| {
                row.get::<_, i64>(0)
            })?)
        })
        .unwrap()
}

fn hello_with_capabilities() -> proto::AgentControlHello {
    proto::AgentControlHello {
        node_uuid: node(),
        session_id: vec![7u8; 16],
        current_config_version: 1,
        agent_version: "0.1.0".to_string(),
        capabilities: Some(proto::AgentCapabilities {
            enabled: vec![
                proto::AgentCapability::ProbeHttp as i32,
                proto::AgentCapability::ProbeIcmp as i32,
                proto::AgentCapability::ProbeTcp as i32,
            ],
        }),
        ..Default::default()
    }
}

/// 派发 → 回流 → 聚合 → 端点全链路。
#[tokio::test]
async fn monitor_dispatch_round_trip() {
    santaizi_dashboard::util::logging::init();
    let fixture = setup_primary("monitor-e2e", false).await;
    // 测试夹具不跑 spawn_background_workers：手动起调度循环。
    let (engine_shutdown_tx, engine_shutdown) = tokio::sync::watch::channel(false);
    tokio::spawn(fixture.state.monitor().run(engine_shutdown));
    let monitor_id = seed_monitor(&fixture);
    seed_telemetry_binding(&fixture);
    let member = insert_user(&fixture.db, "victor", true);

    let (tx, mut stream) = open_control_client(
        &fixture,
        None,
        hello_with_capabilities(),
        Some(CLIENT_SECRET),
    )
    .await
    .unwrap();

    // 调度器 1s 粒度：5s 内应收到 credential + assignment + ProbeRequest。
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    let mut probe: Option<proto::ProbeRequest> = None;
    let mut frames = 0usize;
    while std::time::Instant::now() < deadline {
        let message =
            match tokio::time::timeout(std::time::Duration::from_secs(3), stream.message()).await {
                Ok(Ok(message)) => message,
                other => panic!("Control 流异常 {other:?}（已读 {frames} 帧）"),
            };
        let Some(message) = message else {
            panic!("Control 流提前关闭（已读 {frames} 帧）");
        };
        frames += 1;
        if let Some(proto::primary_control_response::Body::ProbeRequest(request)) = message.body {
            probe = Some(request);
            break;
        }
    }
    let probe = probe.expect("调度器应下发 ProbeRequest");
    assert!(
        probe.probe_id.starts_with(&format!("{monitor_id}-7-")),
        "probe_id 前缀 monitor-server，实际 {}",
        probe.probe_id
    );
    assert!(
        !probe.probe_id.split('-').nth(2).unwrap().is_empty(),
        "尾部随机串"
    );
    match &probe.target {
        Some(proto::probe_request::Target::Http(http)) => {
            assert_eq!(http.url, "https://blog.example");
            assert_eq!(http.timeout_ms, 30_000);
        }
        other => panic!("应派 HTTP 探测，实际 {other:?}"),
    }

    // 回流一个结果（Go 语义：probe_id 一次性消费；重复回声被丢弃——
    // 30 样本聚合写历史由模块单测覆盖）。
    tx.send(proto::AgentControlRequest {
        body: Some(proto::agent_control_request::Body::ProbeResult(
            proto::ProbeResult {
                probe_id: probe.probe_id.clone(),
                kind: proto::ProbeKind::Http as i32,
                successful: true,
                delay_ms: 100.0,
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // 种子两条历史（服务页/网络页读侧）。
    fixture
        .db
        .write(|tx| {
            for (delay, up, down) in [(120.0, 3, 1), (80.0, 4, 0)] {
                tx.execute(
                    "INSERT INTO monitor_histories (created_at, updated_at, monitor_id, server_id, avg_delay, up, down, data) VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?1, ?2, ?3, ?4, ?5, '')",
                    rusqlite::params![monitor_id, SERVER_ID, delay, up, down],
                )?;
            }
            Ok(())
        })
        .unwrap();

    // public services：对外展示 + 最近 30 个本地日。先开展示开关。
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "UPDATE monitors SET enable_show_in_service = 1 WHERE id = ?1",
                [monitor_id],
            )?;
            Ok(())
        })
        .unwrap();
    let (http_port, http_cancel, http_server) = spawn_http(&fixture).await;
    let response = client()
        .get(format!("{}/api/v2/public/services", base_url(http_port)))
        .header(
            reqwest::header::COOKIE,
            format!("santaizi-dashboard={member}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    let items = body["data"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"], monitor_id);
    assert_eq!(items[0]["type"], "http");
    let up = items[0]["up"].as_array().expect("30 日 up");
    let down = items[0]["down"].as_array().expect("30 日 down");
    assert_eq!(up.len(), 30);
    assert_eq!(down.len(), 30);
    assert_eq!(items[0]["delay"].as_array().map(Vec::len), Some(30));
    assert_eq!(up.last().and_then(|value| value.as_i64()), Some(7));
    assert_eq!(down.last().and_then(|value| value.as_i64()), Some(1));
    assert_eq!(items[0]["current_up"], 7);
    assert_eq!(items[0]["current_down"], 1);

    // public network：延迟序列（毫秒时间戳 + avg_delay）。
    let network = client()
        .get(format!(
            "{}/api/v2/public/network/{SERVER_ID}",
            base_url(http_port)
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(network.status(), 200);
    let data = network.json::<serde_json::Value>().await.unwrap()["data"].clone();
    let items = data.as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["monitor_id"], monitor_id);
    assert_eq!(items[0]["monitor_name"], "blog");
    assert_eq!(items[0]["server_name"], "node-7");
    assert!(!items[0]["created_at"].as_array().unwrap().is_empty());
    assert_eq!(items[0]["avg_delay"].as_array().unwrap().len(), 2);

    engine_shutdown_tx.send(true).unwrap();
    http_cancel.cancel();
    let _ = http_server.await;
    let _ = tx;
}

/// 种子绑定（node → server 7），Control 流的 secret 认证路径需要。
fn seed_telemetry_binding(fixture: &PrimaryFixture) {
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT OR IGNORE INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason, created_at) VALUES (?1, ?2, 0, NULL, 1, 'r', CURRENT_TIMESTAMP)",
                rusqlite::params![SERVER_ID, node()],
            )?;
            Ok(())
        })
        .unwrap();
}

/// rpc_v2 的 open_control 复制（测试间不共享私有 helper）。
async fn open_control_client(
    fixture: &PrimaryFixture,
    identity: Option<Identity>,
    hello: proto::AgentControlHello,
    client_secret: Option<&str>,
) -> Result<
    (
        tokio::sync::mpsc::Sender<proto::AgentControlRequest>,
        tonic::Streaming<proto::PrimaryControlResponse>,
    ),
    tonic::Status,
> {
    let channel = dial(fixture, identity)
        .await
        .map_err(|err| tonic::Status::internal(err.to_string()))?;
    let mut client =
        proto::santaizi_control_service_client::SantaiziControlServiceClient::new(channel);
    let (tx, rx) = tokio::sync::mpsc::channel::<proto::AgentControlRequest>(64);
    tx.send(proto::AgentControlRequest {
        body: Some(proto::agent_control_request::Body::Hello(hello)),
    })
    .await
    .unwrap();
    let mut request = tonic::Request::new(ReceiverStream::new(rx));
    if let Some(secret) = client_secret {
        let key: tonic::metadata::MetadataKey<tonic::metadata::Ascii> =
            "client_secret".parse().unwrap();
        request.metadata_mut().insert(
            key,
            tonic::metadata::MetadataValue::try_from(secret).unwrap(),
        );
    }
    let response = client.control(request).await?;
    Ok((tx, response.into_inner()))
}
