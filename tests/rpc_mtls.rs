//! 主端 gRPC mTLS 集成测试：对齐 Go `service/rpc/mtls_integration_test.go`。
//!
//! 每个用例自建一座临时主端（独立数据目录 / PKI / SQLite / 端口），经真实
//! TCP + TLS 拨号，覆盖 Enrollment 认证与换绑、collector 注册、Sync 配置
//! 推送与运行态回执，以及认证层对错证书、缺证书的拒绝。

mod common;

use santaizi_dashboard::pki;
use santaizi_dashboard::pki::uri;
use santaizi_dashboard::proto;
use santaizi_dashboard::repo::bindings;
use santaizi_dashboard::telemetry::store::Store;
use time::OffsetDateTime;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Identity;
use tonic::{Code, Request, Status};

use common::*;

#[tokio::test]
async fn enroll_wrong_secret_is_unauthenticated() {
    // TLS 开启但拨号不带客户端证书：单独验证 client_secret 校验路径。
    let fixture = setup_primary("wrong-secret", true).await;
    let err = enroll(&fixture, &node(), Some("wrong")).await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn enroll_issues_agent_certificate_and_binds_node() {
    // enroll 准入前提是 TLS 连接（单向即可，客户端证书可选）。
    let fixture = setup_primary("issue-cert", true).await;
    let response = enroll(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    let identity = device_identity(&response.certificate_pem);
    assert_eq!(identity.kind, uri::DeviceKind::Agent);
    assert_eq!(identity.node_uuid, node());
    let agent_ca_pem =
        String::from_utf8_lossy(&fixture.state.pki().expect("pki").agent.cert_pem).into_owned();
    assert_eq!(response.ca_certificate_pem, agent_ca_pem);
    assert!(response.expires_at_unix > response.not_before_unix);
    let server_id = fixture
        .db
        .read(|conn| bindings::server_id_from_node_uuid(conn, &node()))
        .unwrap();
    assert_eq!(server_id, Some(SERVER_ID));
}

#[tokio::test]
async fn enroll_requires_tls() {
    let fixture = setup_primary("enroll-tls", false).await;
    let err = enroll(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn enroll_rebinds_when_offline() {
    let fixture = setup_primary("rebind", true).await;
    enroll(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    enroll(&fixture, &other_node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    let rebound = fixture
        .db
        .read(|conn| bindings::server_id_from_node_uuid(conn, &other_node()))
        .unwrap();
    assert_eq!(rebound, Some(SERVER_ID));
    let closed = fixture
        .db
        .read(|conn| bindings::server_id_from_node_uuid(conn, &node()))
        .unwrap();
    assert_eq!(closed, None);
}

#[tokio::test]
async fn renew_issues_new_certificate() {
    let fixture = setup_primary("renew", true).await;
    let (first, first_key) = enroll_with_key(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    let (csr_der, _) = device_csr(&uri::encode_agent_uri(&node()));
    let channel = dial(
        &fixture,
        Some(identity_from(&first.certificate_pem, &first_key)),
    )
    .await
    .unwrap();
    let mut client =
        proto::santaizi_enrollment_service_client::SantaiziEnrollmentServiceClient::new(channel);
    let renewed = client
        .renew(Request::new(proto::AgentRenewRequest {
            node_uuid: node(),
            csr_der,
            agent_version: "test".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!renewed.certificate_pem.is_empty());
    assert_ne!(renewed.certificate_pem, first.certificate_pem);
    assert_eq!(device_identity(&renewed.certificate_pem).node_uuid, node());
}

#[tokio::test]
async fn renew_rejects_uuid_mismatch() {
    let fixture = setup_primary("renew-mismatch", true).await;
    let (first, first_key) = enroll_with_key(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    let (csr_der, _) = device_csr(&uri::encode_agent_uri(&other_node()));
    let channel = dial(
        &fixture,
        Some(identity_from(&first.certificate_pem, &first_key)),
    )
    .await
    .unwrap();
    let mut client =
        proto::santaizi_enrollment_service_client::SantaiziEnrollmentServiceClient::new(channel);
    let err = client
        .renew(Request::new(proto::AgentRenewRequest {
            node_uuid: other_node(),
            csr_der,
            agent_version: "test".to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn strict_ingest_without_certificate_is_unauthenticated() {
    let fixture = setup_primary("strict-ingest", true).await;
    let channel = dial(&fixture, None).await.unwrap();
    let mut client =
        proto::santaizi_telemetry_service_client::SantaiziTelemetryServiceClient::new(channel);
    let (tx, rx) = tokio::sync::mpsc::channel::<proto::TelemetryRequest>(4);
    let mut stream = match client.ingest(Request::new(ReceiverStream::new(rx))).await {
        Ok(response) => response.into_inner(),
        Err(status) => {
            assert_eq!(status.code(), Code::Unauthenticated);
            return;
        }
    };
    let _ = tx
        .send(proto::TelemetryRequest {
            body: Some(proto::telemetry_request::Body::Ping(
                proto::TelemetryPing {},
            )),
        })
        .await;
    let err = stream.message().await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn collector_register_with_csr() {
    let fixture = setup_primary("collector-register", true).await;
    let (response, _key) = register_collector(&fixture).await.unwrap();
    assert_eq!(response.collector_uuid, COLLECTOR_UUID);
    assert_eq!(response.config_version, 1);
    assert_eq!(
        response.primary_public_key,
        fixture.state.signer().public_key()
    );
    assert_eq!(response.key_id, fixture.state.signer().key_id());
    assert!(!response.collector_certificate_pem.is_empty());
    assert!(!response.collector_ca_certificate_pem.is_empty());
    assert!(!response.agent_ca_certificate_pem.is_empty());
    let identity = device_identity(&response.collector_certificate_pem);
    assert_eq!(identity.kind, uri::DeviceKind::Collector);
    assert_eq!(identity.collector_uuid, COLLECTOR_UUID);
}

#[tokio::test]
async fn collector_wrong_token_is_unauthenticated() {
    let fixture = setup_primary("collector-token", false).await;
    let channel = dial(&fixture, None).await.unwrap();
    let mut client =
        proto::santaizi_collector_service_client::SantaiziCollectorServiceClient::new(channel);
    let err = client
        .register(Request::new(proto::RegisterCollectorRequest {
            registration_token: "stc_deadbeef".to_string(),
            protocol_version: "2".to_string(),
            csr_der: Vec::new(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn agent_certificate_on_collector_sync_is_permission_denied() {
    let fixture = setup_primary("agent-on-sync", true).await;
    let (enrolled, key) = enroll_with_key(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    let err = sync_first_error(
        &fixture,
        identity_from(&enrolled.certificate_pem, &key),
        Some(proto::CollectorSyncHello {
            collector_uuid: COLLECTOR_UUID.to_string(),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(err.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn collector_sync_hello_uuid_mismatch_is_permission_denied() {
    let fixture = setup_primary("hello-mismatch", true).await;
    let (registered, key) = register_collector(&fixture).await.unwrap();
    let err = sync_first_error(
        &fixture,
        identity_from(&registered.collector_certificate_pem, &key),
        Some(proto::CollectorSyncHello {
            collector_uuid: "other-collector".to_string(),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(err.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn collector_sync_without_hello_is_invalid_argument() {
    let fixture = setup_primary("sync-no-hello", true).await;
    let (registered, key) = register_collector(&fixture).await.unwrap();
    let err = sync_first_error(
        &fixture,
        identity_from(&registered.collector_certificate_pem, &key),
        None,
    )
    .await;
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn collector_sync_pushes_config_and_acks_runtime() {
    let fixture = setup_primary("sync-runtime", true).await;
    // enroll 换绑后按 collector 作用域生成指派，并推高其配置版本。
    enroll(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    let (registered, key) = register_collector(&fixture).await.unwrap();
    let identity = identity_from(&registered.collector_certificate_pem, &key);

    // 首帧须先入缓冲再拨号：服务端处理器会阻塞等待 hello。
    let (sender, mut stream) = open_sync(
        &fixture,
        identity,
        Some(proto::CollectorSyncRequest {
            body: Some(proto::collector_sync_request::Body::Hello(
                proto::CollectorSyncHello {
                    collector_uuid: COLLECTOR_UUID.to_string(),
                    current_config_version: 1,
                    ..Default::default()
                },
            )),
        }),
    )
    .await
    .unwrap();
    let tx = sender.expect("hello sender");

    let config = match stream.message().await.unwrap().unwrap().body {
        Some(proto::collector_sync_response::Body::Config(config)) => config,
        other => panic!("expected collector config, got {other:?}"),
    };
    assert_eq!(config.config_version, 2);
    assert_eq!(config.kind, proto::CollectorKind::Observer as i32);
    assert!(config.probe.is_some());
    assert_eq!(
        config.primary_public_key,
        fixture.state.signer().public_key()
    );
    assert_eq!(config.assignments.len(), 1);
    assert_eq!(config.assignments[0].node_uuid, node());
    assert_eq!(config.assignments[0].observer_id, COLLECTOR_UUID);
    let agent_ca_pem =
        String::from_utf8_lossy(&fixture.state.pki().expect("pki").agent.cert_pem).into_owned();
    assert_eq!(config.agent_ca_certificate_pem, agent_ca_pem);

    tx.send(proto::CollectorSyncRequest {
        body: Some(proto::collector_sync_request::Body::Runtime(
            proto::CollectorRuntime {
                collector_uuid: COLLECTOR_UUID.to_string(),
                spool_size: 42,
                protocol_version: "2".to_string(),
                software_version: "0.1.0".to_string(),
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();
    let ack = stream.message().await.unwrap().unwrap();
    assert!(matches!(
        ack.body,
        Some(proto::collector_sync_response::Body::Accepted(true))
    ));

    let runtime = fixture
        .db
        .read(|conn| {
            let row = conn
                .query_row(
                    "SELECT status, spool_size, software_version FROM collector_runtimes WHERE collector_uuid = ?",
                    [COLLECTOR_UUID],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .map_err(anyhow::Error::from)?;
            Ok(row)
        })
        .unwrap();
    assert_eq!(runtime, ("online".to_string(), 42, "0.1.0".to_string()));
}

/// 开 Replicate 双工流：collector 证书身份；首帧先入缓冲。
async fn open_replicate(
    fixture: &PrimaryFixture,
    identity: Identity,
    first: proto::ReplicationBatch,
) -> Result<
    (
        tokio::sync::mpsc::Sender<proto::ReplicationBatch>,
        tonic::codec::Streaming<proto::ReplicationAck>,
    ),
    Status,
> {
    let channel = dial(fixture, Some(identity))
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    let mut client =
        proto::santaizi_replication_service_client::SantaiziReplicationServiceClient::new(channel);
    let (tx, rx) = tokio::sync::mpsc::channel::<proto::ReplicationBatch>(4);
    tx.send(first).await.expect("buffer first batch");
    let stream = client
        .replicate(Request::new(ReceiverStream::new(rx)))
        .await?
        .into_inner();
    Ok((tx, stream))
}

/// 复制批次里的合法 HOST 事件（event_id 由 node/session/sequence 派生）。
fn replicate_event(sequence: u64) -> proto::TelemetryEvent {
    let now = santaizi_dashboard::util::time::now_nanos();
    let event_id = Store::event_id(&node(), &replicate_session(), sequence).expect("event id");
    proto::TelemetryEvent {
        event_id: event_id.to_vec(),
        node_uuid: node(),
        session_id: replicate_session(),
        sequence,
        event_type: proto::TelemetryEventType::Host as i32,
        priority: proto::TelemetryPriority::P1Important as i32,
        collected_at_unix_nano: now,
        protocol_version: 2,
        source_protocol: proto::SourceProtocol::SantaiziV2 as i32,
        reliability: proto::Reliability::ReliableReplay as i32,
        payload: Some(proto::telemetry_event::Payload::Host(proto::Host {
            ip: "10.9.9.9".to_string(),
            version: "0.1.0".to_string(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn replicate_session() -> Vec<u8> {
    vec![0x23; 16]
}

fn replication_batch(batch_sequence: u64, spool_through_id: u64) -> proto::ReplicationBatch {
    let event = replicate_event(1);
    let now = santaizi_dashboard::util::time::now_nanos();
    proto::ReplicationBatch {
        collector_uuid: COLLECTOR_UUID.to_string(),
        replication_session: vec![0x24; 16],
        batch_sequence,
        spool_through_id,
        events: vec![event.clone()],
        observations: vec![proto::TelemetryObservation {
            event_id: event.event_id,
            observer_id: COLLECTOR_UUID.to_string(),
            received_at_unix_nano: now,
            ..Default::default()
        }],
        runtime: Some(proto::CollectorRuntime {
            collector_uuid: COLLECTOR_UUID.to_string(),
            sampled_at_unix_nano: now,
            spool_size: 42,
            protocol_version: "2".to_string(),
            replication_rtt_ms: 12.5,
            replication_rtt_sampled_at_unix_nano: now,
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn replicate_commits_batch_and_acks() {
    let fixture = setup_primary("replicate", true).await;
    // enroll 换绑生成节点绑定与指派：复制事件要能刷运行态。
    enroll(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    let (registered, key) = register_collector(&fixture).await.unwrap();
    let (tx, mut stream) = open_replicate(
        &fixture,
        identity_from(&registered.collector_certificate_pem, &key),
        replication_batch(1, 42),
    )
    .await
    .unwrap();

    let ack = stream.message().await.unwrap().unwrap();
    assert_eq!(ack.collector_uuid, COLLECTOR_UUID);
    assert_eq!(ack.batch_sequence, 1);
    assert_eq!(ack.committed_spool_through_id, 42);
    assert!(ack.error.is_empty());

    let event_id = Store::event_id(&node(), &replicate_session(), 1).expect("event id");
    let events: i64 = fixture
        .db
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM telemetry_events WHERE event_id = ?",
                [event_id.as_slice()],
                |r| r.get(0),
            )?)
        })
        .unwrap();
    assert_eq!(events, 1);
    let receipts: i64 = fixture
        .db
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM collector_replication_receipts \
                 WHERE collector_uuid = ? AND batch_sequence = 1",
                [COLLECTOR_UUID],
                |r| r.get(0),
            )?)
        })
        .unwrap();
    assert_eq!(receipts, 1);
    // 复制路径 upsert 带 last_sync（同步流心跳不刷新该列）。
    let last_sync: i64 = fixture
        .db
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT last_sync FROM collector_runtimes WHERE collector_uuid = ?",
                [COLLECTOR_UUID],
                |r| r.get(0),
            )?)
        })
        .unwrap();
    assert!(last_sync > 0);
    // 复制 RTT 时延样本落桶（record_collector_latency 在回执前完成）。
    let latency: i64 = fixture
        .db
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM connection_latency_buckets WHERE kind = 'collector_replication'",
                [],
                |r| r.get(0),
            )?)
        })
        .unwrap();
    assert_eq!(latency, 1);

    // 重复投递同一 (collector, session, sequence)：回执去重，不重放。
    tx.send(replication_batch(1, 42)).await.unwrap();
    let ack = stream.message().await.unwrap().unwrap();
    assert_eq!(ack.committed_spool_through_id, 42);
    let events: i64 = fixture
        .db
        .read(|conn| Ok(conn.query_row("SELECT COUNT(*) FROM telemetry_events", [], |r| r.get(0))?))
        .unwrap();
    assert_eq!(events, 1);

    drop(tx);
    assert!(stream.message().await.unwrap().is_none());
}

#[tokio::test]
async fn replicate_identity_mismatch_is_permission_denied() {
    let fixture = setup_primary("replicate-mismatch", true).await;
    let (registered, key) = register_collector(&fixture).await.unwrap();
    let (tx, mut stream) = open_replicate(
        &fixture,
        identity_from(&registered.collector_certificate_pem, &key),
        proto::ReplicationBatch {
            collector_uuid: "someone-else".to_string(),
            replication_session: vec![0x24; 16],
            batch_sequence: 1,
            spool_through_id: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let err = stream.message().await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
    drop(tx);
}

#[tokio::test]
async fn wrong_ca_client_certificate_is_rejected() {
    let fixture = setup_primary("wrong-ca", true).await;
    // 先正常 enroll 建立绑定：若服务端不校验 CA，下面的续签就会得逞。
    enroll(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();

    let other_dir = temp_dir("wrong-ca-other");
    let other = pki::load_or_create(other_dir.to_str().expect("other pki dir")).unwrap();
    let (csr_der, key_pem) = device_csr(&uri::encode_agent_uri(&node()));
    let (cert_pem, _, _) =
        pki::sign_agent_csr(&other.agent, &csr_der, &node(), OffsetDateTime::now_utc()).unwrap();
    let identity = identity_from(&String::from_utf8_lossy(&cert_pem), &key_pem);

    match dial(&fixture, Some(identity)).await {
        // rustls 对 optional 模式下给出的客户端证书同样验签：可能在拨号阶段即被拒绝，
        // 也可能握手放行后由服务端在证书校验失败时中止连接——tonic 会把后者呈现为
        // 首个 RPC 上的 transport error（fatal alert），而非干净的 gRPC 状态。
        Err(_) => {}
        Ok(channel) => {
            let mut client =
                proto::santaizi_enrollment_service_client::SantaiziEnrollmentServiceClient::new(
                    channel,
                );
            let (csr_der, _) = device_csr(&uri::encode_agent_uri(&node()));
            match client
                .renew(Request::new(proto::AgentRenewRequest {
                    node_uuid: node(),
                    csr_der,
                    agent_version: "test".to_string(),
                }))
                .await
            {
                Err(err) => assert!(
                    err.code() == Code::Unknown
                        || matches!(err.code(), Code::Unauthenticated | Code::PermissionDenied),
                    "unexpected renew rejection: {err}"
                ),
                Ok(response) => {
                    panic!("renew succeeded with a certificate from an untrusted CA: {response:?}")
                }
            }
        }
    }
    let _ = std::fs::remove_dir_all(&other_dir);
}
