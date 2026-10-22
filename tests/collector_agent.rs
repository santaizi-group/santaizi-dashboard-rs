//! Collector 从端 agent 服务集成测试：真实主端 + 真实从端 Runtime + 真实
//! TCP/TLS 探针流。
//!
//! 覆盖端到端路径（探针 Ingest → 从端出库 → replication → 主端落库）与
//! 拒绝路径（endpoint 身份不符、节点未指派、probe 从端、GetStatus 授权）。
//! 授权缓存经共享夹具预置（免去等待 Sync 下发），ingest/GetStatus 路径
//! 不依赖注册完成。

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use santaizi_dashboard::collector::store;
use santaizi_dashboard::config::Config;
use santaizi_dashboard::proto;
use santaizi_dashboard::rpc;
use santaizi_dashboard::telemetry::store::Store;
use santaizi_dashboard::util::time as util_time;
use tokio::net::TcpListener;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Code, Request, Status};

/// 轮询等待闭包产出 `Some`；DB 写在后台任务中，需给提交留时间。
async fn wait_until<T>(timeout: Duration, mut produce: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(value) = produce() {
            return Some(value);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn session() -> Vec<u8> {
    vec![0x43; 16]
}

fn agent_event(sequence: u64) -> proto::TelemetryEvent {
    let event_id = Store::event_id(&node(), &session(), sequence).expect("event id");
    proto::TelemetryEvent {
        event_id: event_id.to_vec(),
        node_uuid: node(),
        session_id: session(),
        sequence,
        event_type: proto::TelemetryEventType::Host as i32,
        priority: proto::TelemetryPriority::P1Important as i32,
        collected_at_unix_nano: util_time::now_nanos(),
        protocol_version: 2,
        source_protocol: proto::SourceProtocol::SantaiziV2 as i32,
        reliability: proto::Reliability::ReliableReplay as i32,
        payload: Some(proto::telemetry_event::Payload::Host(proto::Host {
            ip: "10.8.8.8".to_string(),
            version: "0.1.0".to_string(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn agent_batch(sequences: &[u64]) -> proto::TelemetryBatch {
    proto::TelemetryBatch {
        records: sequences
            .iter()
            .map(|sequence| proto::TelemetryRecord {
                record: Some(proto::telemetry_record::Record::Event(agent_event(
                    *sequence,
                ))),
            })
            .collect(),
    }
}

/// 本地签一份探针凭证（探针侧由 Control 下发；这里直接签）。
fn sign_credential(fixture: &PrimaryFixture, node_uuid: &[u8]) -> proto::SignedAgentCredential {
    let now = util_time::now_nanos() / 1_000_000_000;
    fixture
        .state
        .signer()
        .sign(node_uuid, 1, now, 3600)
        .expect("sign credential")
}

fn agent_hello(
    endpoint_id: String,
    credential: proto::SignedAgentCredential,
) -> proto::TelemetryRequest {
    proto::TelemetryRequest {
        body: Some(proto::telemetry_request::Body::Hello(
            proto::TelemetryHello {
                node_uuid: node(),
                endpoint_id,
                assignment_generation: 1,
                credential: Some(credential),
                protocol_version: "2".to_string(),
                ..Default::default()
            },
        )),
    }
}

/// 拨从端 agent 服务：TLS 下可带 agent 客户端证书；服务端证书与主端同一把
/// （自签 fixture），CA 用主端证书。
async fn dial_collector_agent(
    fixture: &PrimaryFixture,
    port: u16,
    identity: Option<Identity>,
) -> anyhow::Result<Channel> {
    let endpoint = if fixture.state.conf().grpc_tls.enabled {
        let mut tls = ClientTlsConfig::new()
            .domain_name("localhost")
            .ca_certificate(Certificate::from_pem(fixture.server_cert_pem.clone()));
        if let Some(identity) = identity {
            tls = tls.identity(identity);
        }
        Endpoint::from_shared(format!("https://localhost:{port}"))?.tls_config(tls)?
    } else {
        Endpoint::from_shared(format!("http://127.0.0.1:{port}"))?
    };
    endpoint
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
        .map_err(|err| anyhow::anyhow!("dial collector agent {port}: {err}"))
}

/// 起从端 agent 服务（与 `collector::serve` 同一入口）；返回端口与取消令牌。
async fn spawn_agent_service(
    runtime: Arc<santaizi_dashboard::collector::runtime::Runtime>,
    fixture: &PrimaryFixture,
    tls: bool,
) -> (u16, CancellationToken, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind agent port");
    let port = listener.local_addr().expect("local addr").port();
    let mut conf = Config::default();
    conf.mode = "collector".to_string();
    conf.grpc_tls.enabled = tls;
    if tls {
        conf.grpc_tls.cert_file = fixture
            .dir
            .join("server.crt")
            .to_string_lossy()
            .into_owned();
        conf.grpc_tls.key_file = fixture
            .dir
            .join("server.key")
            .to_string_lossy()
            .into_owned();
    }
    let cancel = CancellationToken::new();
    let server_cancel = cancel.clone();
    let server = tokio::spawn(async move {
        let _ =
            rpc::collector_agent::serve_collector_agent(runtime, listener, server_cancel, &conf)
                .await;
    });
    (port, cancel, server)
}

/// 开 Ingest 双工流；首帧先入缓冲，与服务端先读 hello 的行为配对。
async fn open_agent_ingest(
    fixture: &PrimaryFixture,
    port: u16,
    identity: Option<Identity>,
    hello: proto::TelemetryRequest,
) -> Result<
    (
        tokio::sync::mpsc::Sender<proto::TelemetryRequest>,
        tonic::Streaming<proto::TelemetryResponse>,
    ),
    Status,
> {
    let channel = dial_collector_agent(fixture, port, identity)
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    let mut client =
        proto::santaizi_telemetry_service_client::SantaiziTelemetryServiceClient::new(channel);
    let (tx, rx) = tokio::sync::mpsc::channel::<proto::TelemetryRequest>(8);
    tx.send(hello)
        .await
        .map_err(|err| Status::internal(format!("buffer ingest hello: {err}")))?;
    let stream = client
        .ingest(Request::new(ReceiverStream::new(rx)))
        .await?
        .into_inner();
    Ok((tx, stream))
}

fn outbox_pending(db: &santaizi_dashboard::db::Db) -> i64 {
    db.read(|conn| Ok(conn.query_row("SELECT COUNT(*) FROM collector_outboxes", [], |r| r.get(0))?))
        .unwrap()
}

fn primary_event_count(db: &santaizi_dashboard::db::Db, event_id: &[u8]) -> i64 {
    db.read(|conn| {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM telemetry_events WHERE event_id = ?1",
            [event_id],
            |r| r.get(0),
        )?)
    })
    .unwrap()
}

/// 端到端：探针经 mTLS（agent 证书）Ingest 到从端 → 出库 → replication
/// 复制到主端 → ack 清理。
#[tokio::test]
async fn agent_ingest_replicates_to_primary() {
    let fixture = setup_primary("agent-e2e", true).await;
    let (enrolled, key) = enroll_with_key(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    let (runtime, db, dir) = setup_collector_runtime(
        &fixture,
        "agent-e2e-edge",
        true,
        true,
        None,
        Some(observer_authorization(&fixture, &node())),
        "",
    )
    .await;
    let worker = runtime.start();

    let (port, cancel, server) = spawn_agent_service(Arc::clone(&runtime), &fixture, true).await;

    let (tx, mut stream) = open_agent_ingest(
        &fixture,
        port,
        Some(identity_from(&enrolled.certificate_pem, &key)),
        agent_hello(
            COLLECTOR_UUID.to_string(),
            sign_credential(&fixture, &node()),
        ),
    )
    .await
    .unwrap();

    tx.send(proto::TelemetryRequest {
        body: Some(proto::telemetry_request::Body::Batch(agent_batch(&[
            1, 2, 3,
        ]))),
    })
    .await
    .unwrap();
    let response = stream.message().await.unwrap().unwrap();
    assert_eq!(response.acks.len(), 1);
    assert_eq!(response.acks[0].ack_through, 3);
    assert_eq!(response.acks[0].node_uuid, node());
    assert_eq!(response.acks[0].session_id, session());

    // ping → pong（流保持可用）。
    tx.send(proto::TelemetryRequest {
        body: Some(proto::telemetry_request::Body::Ping(
            proto::TelemetryPing {},
        )),
    })
    .await
    .unwrap();
    assert!(stream.message().await.unwrap().unwrap().pong.is_some());

    // 出库有条目，随后被复制循环冲走并清理。
    assert!(
        wait_until(Duration::from_secs(10), || {
            (outbox_pending(&db) > 0).then_some(())
        })
        .await
        .is_some(),
        "ingested batch should enqueue outbox rows"
    );
    let first = Store::event_id(&node(), &session(), 1).expect("event id");
    let third = Store::event_id(&node(), &session(), 3).expect("event id");
    let replicated = wait_until(Duration::from_secs(15), || {
        let count =
            primary_event_count(&fixture.db, &first) + primary_event_count(&fixture.db, &third);
        (count >= 2).then_some(count)
    })
    .await;
    assert_eq!(replicated, Some(2), "ingested events missing on primary");
    let drained = wait_until(Duration::from_secs(10), || {
        (outbox_pending(&db) == 0).then_some(())
    })
    .await;
    assert!(drained.is_some(), "outbox should be cleared after ack");

    drop(tx);
    cancel.cancel();
    let _ = server.await;
    runtime.cancel_token().cancel();
    assert!(worker.await.unwrap().is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}

/// hello 的 endpoint_id 不是本从端 UUID → PermissionDenied。
#[tokio::test]
async fn agent_ingest_rejects_endpoint_mismatch() {
    let fixture = setup_primary("agent-mismatch", true).await;
    let (enrolled, key) = enroll_with_key(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    let (runtime, _db, dir) = setup_collector_runtime(
        &fixture,
        "agent-mismatch-edge",
        true,
        true,
        None,
        Some(observer_authorization(&fixture, &node())),
        "",
    )
    .await;

    let (port, cancel, server) = spawn_agent_service(Arc::clone(&runtime), &fixture, true).await;

    let (_tx, mut stream) = open_agent_ingest(
        &fixture,
        port,
        Some(identity_from(&enrolled.certificate_pem, &key)),
        agent_hello(
            "not-this-collector".to_string(),
            sign_credential(&fixture, &node()),
        ),
    )
    .await
    .unwrap();
    let err = stream.message().await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);

    cancel.cancel();
    let _ = server.await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// 节点未被指派到本从端（授权缓存无指派）→ PermissionDenied。
#[tokio::test]
async fn agent_ingest_rejects_unauthorized_node() {
    let fixture = setup_primary("agent-unauth", true).await;
    let (enrolled, key) = enroll_with_key(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    // 预置授权但不带指派：缓存可用、节点不在指派表。Agent CA 必须随授权
    // 落下：TLS 开时无 CA 池 tonic 不会请求客户端证书，ingest 根本到不了
    // 授权检查。
    let mut authorization = observer_authorization(&fixture, &node());
    authorization.assignments.clear();
    let (runtime, _db, dir) = setup_collector_runtime(
        &fixture,
        "agent-unauth-edge",
        true,
        true,
        None,
        Some(authorization),
        "",
    )
    .await;

    let (port, cancel, server) = spawn_agent_service(Arc::clone(&runtime), &fixture, true).await;

    let (_tx, mut stream) = open_agent_ingest(
        &fixture,
        port,
        Some(identity_from(&enrolled.certificate_pem, &key)),
        agent_hello(
            COLLECTOR_UUID.to_string(),
            sign_credential(&fixture, &node()),
        ),
    )
    .await
    .unwrap();
    let err = stream.message().await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);

    cancel.cancel();
    let _ = server.await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// GetStatus：授权串常量时间比对，错误拒、正确回运行态。
#[tokio::test]
async fn get_status_requires_authorization() {
    let fixture = setup_primary("agent-status", true).await;
    let (runtime, _db, dir) = setup_collector_runtime(
        &fixture,
        "agent-status-edge",
        true,
        true,
        None,
        Some(observer_authorization(&fixture, &node())),
        "status-secret",
    )
    .await;

    let (port, cancel, server) = spawn_agent_service(Arc::clone(&runtime), &fixture, true).await;

    let channel = dial_collector_agent(&fixture, port, None).await.unwrap();
    let mut client =
        proto::santaizi_collector_service_client::SantaiziCollectorServiceClient::new(channel);

    let err = client
        .get_status(Request::new(proto::CollectorStatusRequest {
            authorization: "wrong".to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);

    let status = client
        .get_status(Request::new(proto::CollectorStatusRequest {
            authorization: "status-secret".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.protocol_version, "2");
    assert_eq!(status.replication_cursor, 0);
    assert_eq!(status.connected_agents, 0);

    cancel.cancel();
    let _ = server.await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// probe 从端不接受 agent ingest（FailedPrecondition，先于后续校验）。
#[tokio::test]
async fn probe_collector_rejects_agent_ingest() {
    let fixture = setup_primary("agent-probe", false).await;
    let (runtime, _db, dir) = setup_collector_runtime(
        &fixture,
        "agent-probe-edge",
        false,
        false,
        None,
        Some(proto::CollectorAuthorizationConfig {
            config_version: 1,
            primary_public_key: vec![0xab; 32],
            kind: proto::CollectorKind::Probe as i32,
            ..Default::default()
        }),
        "",
    )
    .await;
    assert!(runtime.is_probe(), "fixture should restore probe kind");

    // 明文从端：无 agent CA 池也能拨；probe 拒绝先于 hello 之后的校验。
    let (port, cancel, server) = spawn_agent_service(Arc::clone(&runtime), &fixture, false).await;

    let (_tx, mut stream) = open_agent_ingest(
        &fixture,
        port,
        None,
        agent_hello(
            COLLECTOR_UUID.to_string(),
            sign_credential(&fixture, &node()),
        ),
    )
    .await
    .unwrap();
    let err = stream.message().await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    cancel.cancel();
    let _ = server.await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// 摄入后复制循环被 wake 提前唤醒（不必等 2s 空转）。
#[tokio::test]
async fn agent_ingest_wakes_replication() {
    let fixture = setup_primary("agent-wake", false).await;
    // 明文 fixture 不过 enroll：绑定 + 指派直写数据库。
    // 指派不能省——主端复制路径按它校验节点归属。
    common::bind_node_with_assignment(&fixture, &node(), COLLECTOR_UUID);
    let (runtime, db, dir) = setup_collector_runtime(
        &fixture,
        "agent-wake-edge",
        false,
        false,
        None,
        Some(observer_authorization(&fixture, &node())),
        "",
    )
    .await;
    let worker = runtime.start();

    let (port, cancel, server) = spawn_agent_service(Arc::clone(&runtime), &fixture, false).await;

    let (tx, mut stream) = open_agent_ingest(
        &fixture,
        port,
        None,
        agent_hello(
            COLLECTOR_UUID.to_string(),
            sign_credential(&fixture, &node()),
        ),
    )
    .await
    .unwrap();
    tx.send(proto::TelemetryRequest {
        body: Some(proto::telemetry_request::Body::Batch(agent_batch(&[1]))),
    })
    .await
    .unwrap();
    let response = stream.message().await.unwrap().unwrap();
    assert_eq!(response.acks[0].ack_through, 1);
    drop(tx);

    // wake 语义：出库在摄入后极短时间内被冲空（远小于 2s 空转）。
    let drained = wait_until(Duration::from_millis(900), || {
        (outbox_pending(&db) == 0).then_some(())
    })
    .await;
    assert!(
        drained.is_some(),
        "replication should be woken by ingest promptly"
    );

    cancel.cancel();
    let _ = server.await;
    runtime.cancel_token().cancel();
    assert!(worker.await.unwrap().is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}

/// 摄入批次落本地事实表（IngestResult 经流回执体现；事实表由 store 单测覆盖，
/// 这里只确认流路径不回错误、出库入队计数正确）。
#[tokio::test]
async fn agent_ingest_enqueues_outbox_rows() {
    let fixture = setup_primary("agent-outbox", false).await;
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason) \
                 VALUES (?1, ?2, 0, NULL, 1, 'test')",
                rusqlite::params![SERVER_ID, node()],
            )?;
            Ok(())
        })
        .unwrap();
    let (runtime, db, dir) = setup_collector_runtime(
        &fixture,
        "agent-outbox-edge",
        false,
        false,
        None,
        Some(observer_authorization(&fixture, &node())),
        "",
    )
    .await;

    let (port, cancel, server) = spawn_agent_service(Arc::clone(&runtime), &fixture, false).await;

    let (tx, mut stream) = open_agent_ingest(
        &fixture,
        port,
        None,
        agent_hello(
            COLLECTOR_UUID.to_string(),
            sign_credential(&fixture, &node()),
        ),
    )
    .await
    .unwrap();
    // 序列从 1 连续：可靠游标逐点推进，有洞即停（ack 停在洞前）。
    tx.send(proto::TelemetryRequest {
        body: Some(proto::telemetry_request::Body::Batch(agent_batch(&[1, 2]))),
    })
    .await
    .unwrap();
    let response = stream.message().await.unwrap().unwrap();
    assert_eq!(response.acks[0].ack_through, 2);
    drop(tx);

    // 两个事件各带一条自动观测 = 4 条出库（不起复制循环，条目留存）。
    let pending = wait_until(Duration::from_secs(5), || {
        let count = outbox_pending(&db);
        (count >= 4).then_some(count)
    })
    .await;
    assert_eq!(pending, Some(4));

    cancel.cancel();
    let _ = server.await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// 本地事实表随摄入落库（复用 store 层断言，防流路径回归）。
#[tokio::test]
async fn agent_ingest_writes_local_facts() {
    let fixture = setup_primary("agent-facts", false).await;
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason) \
                 VALUES (?1, ?2, 0, NULL, 1, 'test')",
                rusqlite::params![SERVER_ID, node()],
            )?;
            Ok(())
        })
        .unwrap();
    let (runtime, db, dir) = setup_collector_runtime(
        &fixture,
        "agent-facts-edge",
        false,
        false,
        None,
        Some(observer_authorization(&fixture, &node())),
        "",
    )
    .await;

    let (port, cancel, server) = spawn_agent_service(Arc::clone(&runtime), &fixture, false).await;

    let (tx, mut stream) = open_agent_ingest(
        &fixture,
        port,
        None,
        agent_hello(
            COLLECTOR_UUID.to_string(),
            sign_credential(&fixture, &node()),
        ),
    )
    .await
    .unwrap();
    tx.send(proto::TelemetryRequest {
        body: Some(proto::telemetry_request::Body::Batch(agent_batch(&[1]))),
    })
    .await
    .unwrap();
    assert_eq!(
        stream.message().await.unwrap().unwrap().acks[0].ack_through,
        1
    );
    drop(tx);

    let facts = wait_until(Duration::from_secs(5), || {
        db.read(|conn| {
            Ok(conn
                .query_row(
                    "SELECT (SELECT COUNT(*) FROM collector_stored_events), \
                     (SELECT COUNT(*) FROM collector_stored_observations)",
                    [],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .ok())
        })
        .ok()
        .flatten()
        .filter(|(events, observations)| *events >= 1 && *observations >= 1)
    })
    .await;
    assert_eq!(
        facts,
        Some((1, 1)),
        "event + auto observation should be stored"
    );
    let _ = store::OUTBOX_EVENT;

    cancel.cancel();
    let _ = server.await;
    let _ = std::fs::remove_dir_all(&dir);
}
