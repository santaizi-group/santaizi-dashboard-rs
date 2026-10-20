//! 主端 v2 热路径集成测试：对齐 Go `service/rpc/v2_integration_test.go`。
//!
//! 每个用例自建一座临时主端（独立数据目录 / PKI / SQLite / 端口），经真实
//! TCP + TLS 拨号，覆盖 Control 的凭证/指派下发与运行态回执，以及 Ingest
//! 的批次 ack、快照、ping 与认证拒绝路径。

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rcgen::string::Ia5String;
use rcgen::{CertificateParams, KeyPair, PKCS_ED25519, SanType};
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Code, Request, Status};

use santaizi_dashboard::config::Config;
use santaizi_dashboard::db::{self, Db};
use santaizi_dashboard::pki;
use santaizi_dashboard::pki::uri;
use santaizi_dashboard::proto;
use santaizi_dashboard::repo::bindings;
use santaizi_dashboard::rpc;
use santaizi_dashboard::secret;
use santaizi_dashboard::state::AppState;
use santaizi_dashboard::telemetry::credential;
use santaizi_dashboard::telemetry::store::Store;

const CLIENT_SECRET: &str = "test-client-secret";
const SERVER_ID: i64 = 7;

fn node() -> Vec<u8> {
    vec![0x31; 16]
}

fn other_node() -> Vec<u8> {
    vec![0x32; 16]
}

fn session() -> Vec<u8> {
    vec![0x33; 16]
}

static SEQ: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "santaizi-v2-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// 进程级共享业务密钥：并行 fixture 共用同一把密钥。
fn shared_secret_key_path() -> &'static str {
    static PATH: OnceLock<String> = OnceLock::new();
    PATH.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("santaizi-v2-shared-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create shared dir");
        let path = dir.join("business-secrets.key");
        if !path.exists() {
            std::fs::write(&path, [0x5a_u8; 32]).expect("write shared secret key");
        }
        path.to_string_lossy().into_owned()
    })
}

struct PrimaryFixture {
    dir: PathBuf,
    db: Db,
    state: Arc<AppState>,
    port: u16,
    server_cert_pem: Vec<u8>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for PrimaryFixture {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// 建主端；`tls_enabled` 决定传输加密，`require_agent_mtls` 决定
/// agent 方法是否强制客户端证书（Ingest 单测只要 TLS、不要强制 mTLS）。
async fn setup_primary(tag: &str, tls_enabled: bool, require_agent_mtls: bool) -> PrimaryFixture {
    santaizi_dashboard::util::tls::install_crypto_provider();
    let dir = temp_dir(tag);
    let db = db::migrate::open_primary(dir.join("panel.db").to_str().expect("db path"))
        .expect("open primary db");

    let (server_cert_pem, server_key_pem) = pki::tls::new_self_signed_server_certificate(
        &["localhost".to_string()],
        OffsetDateTime::now_utc(),
        Duration::from_secs(24 * 60 * 60),
    )
    .expect("server certificate");
    let cert_file = dir.join("server.crt");
    let key_file = dir.join("server.key");
    std::fs::write(&cert_file, &server_cert_pem).expect("write server cert");
    std::fs::write(&key_file, &server_key_pem).expect("write server key");

    let mut conf = Config::default();
    conf.mode = "primary".to_string();
    conf.grpc_host = "127.0.0.1".to_string();
    conf.grpc_port = 0;
    conf.telemetry.data_dir = dir.to_string_lossy().into_owned();
    conf.telemetry.signing_key_path = dir
        .join("telemetry-signing.key")
        .to_string_lossy()
        .into_owned();
    conf.telemetry.secret_key_path = shared_secret_key_path().to_string();
    conf.grpc_tls.enabled = tls_enabled;
    conf.grpc_tls.cert_file = cert_file.to_string_lossy().into_owned();
    conf.grpc_tls.key_file = key_file.to_string_lossy().into_owned();
    conf.grpc_tls.require_agent_mtls = require_agent_mtls;
    conf.grpc_tls.require_collector_mtls = require_agent_mtls;

    let state = AppState::primary(conf, db.clone())
        .await
        .expect("app state");

    let ciphertext = secret::encrypt_secret(CLIENT_SECRET).expect("encrypt client secret");
    let seed_db = db.clone();
    tokio::task::spawn_blocking(move || {
        seed_db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) VALUES (?, 'node-7', 'tag1', ?, 1, 0)",
                rusqlite::params![SERVER_ID, &ciphertext],
            )?;
            Ok(())
        })
    })
    .await
    .expect("seed join")
    .expect("seed db");
    state.secrets().rebuild(&db).expect("rebuild secret index");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind grpc");
    let port = listener.local_addr().expect("local addr").port();
    let server_state = Arc::clone(&state);
    let server = tokio::spawn(async move {
        let _ = rpc::serve_primary(server_state, listener).await;
    });

    PrimaryFixture {
        dir,
        db,
        state,
        port,
        server_cert_pem,
        server,
    }
}

async fn dial(fixture: &PrimaryFixture, identity: Option<Identity>) -> anyhow::Result<Channel> {
    let endpoint = if fixture.state.conf().grpc_tls.enabled {
        let mut tls = ClientTlsConfig::new()
            .domain_name("localhost")
            .ca_certificate(Certificate::from_pem(fixture.server_cert_pem.clone()));
        if let Some(identity) = identity {
            tls = tls.identity(identity);
        }
        Endpoint::from_shared(format!("https://localhost:{}", fixture.port))?.tls_config(tls)?
    } else {
        Endpoint::from_shared(format!("http://127.0.0.1:{}", fixture.port))?
    };
    endpoint
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
        .map_err(|err| anyhow::anyhow!("dial {}: {err}", fixture.port))
}

fn device_csr(uri: &str) -> (Vec<u8>, String) {
    let key_pair = KeyPair::generate_for(&PKCS_ED25519).expect("generate key");
    let mut params = CertificateParams::default();
    params.subject_alt_names = vec![SanType::URI(
        Ia5String::try_from(uri.to_string()).expect("uri san"),
    )];
    let csr = params.serialize_request(&key_pair).expect("serialize csr");
    (csr.der().to_vec(), key_pair.serialize_pem())
}

fn identity_from(cert_pem: &str, key_pem: &str) -> Identity {
    Identity::from_pem(cert_pem.as_bytes(), key_pem.as_bytes())
}

/// Enroll 并返回 `(响应, 首份 CSR 的私钥 PEM)`：enroll 会把节点绑到服务器。
async fn enroll_with_key(
    fixture: &PrimaryFixture,
    node_uuid: &[u8],
    client_secret: Option<&str>,
) -> Result<(proto::AgentEnrollResponse, String), Status> {
    let (csr_der, key_pem) = device_csr(&uri::encode_agent_uri(node_uuid));
    let channel = dial(fixture, None)
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    let mut client =
        proto::santaizi_enrollment_service_client::SantaiziEnrollmentServiceClient::new(channel);
    let mut request = Request::new(proto::AgentEnrollRequest {
        node_uuid: node_uuid.to_vec(),
        csr_der,
        agent_version: "test".to_string(),
    });
    if let Some(client_secret) = client_secret {
        let key: tonic::metadata::MetadataKey<tonic::metadata::Ascii> =
            "client_secret".parse().expect("client secret key");
        request.metadata_mut().insert(
            key,
            MetadataValue::try_from(client_secret).expect("client secret value"),
        );
    }
    let response = client.enroll(request).await?.into_inner();
    Ok((response, key_pem))
}

fn host_event(sequence: u64) -> proto::TelemetryEvent {
    let now = santaizi_dashboard::util::time::now_nanos();
    let event_id = Store::event_id(&node(), &session(), sequence).expect("event id");
    proto::TelemetryEvent {
        event_id: event_id.to_vec(),
        node_uuid: node(),
        session_id: session(),
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

fn telemetry_batch(sequences: &[u64]) -> proto::TelemetryBatch {
    proto::TelemetryBatch {
        records: sequences
            .iter()
            .map(|sequence| proto::TelemetryRecord {
                record: Some(proto::telemetry_record::Record::Event(host_event(
                    *sequence,
                ))),
            })
            .collect(),
    }
}

/// 用主端签名器签一份合法凭证（探针侧由 Control 下发，这里直接本地签）。
fn sign_credential(fixture: &PrimaryFixture, node_uuid: &[u8]) -> proto::SignedAgentCredential {
    let now = santaizi_dashboard::util::time::now_nanos() / 1_000_000_000;
    fixture
        .state
        .signer()
        .sign(node_uuid, 1, now, 3600)
        .expect("sign credential")
}

fn ingest_hello(credential: proto::SignedAgentCredential) -> proto::TelemetryRequest {
    proto::TelemetryRequest {
        body: Some(proto::telemetry_request::Body::Hello(
            proto::TelemetryHello {
                node_uuid: node(),
                endpoint_id: "primary".to_string(),
                assignment_generation: 1,
                credential: Some(credential),
                protocol_version: "2".to_string(),
                ..Default::default()
            },
        )),
    }
}

/// 开 Ingest 双工流；首帧先入缓冲，与服务端先读 hello 的行为配对。
async fn open_ingest(
    fixture: &PrimaryFixture,
    identity: Option<Identity>,
    hello: proto::TelemetryRequest,
) -> Result<
    (
        tokio::sync::mpsc::Sender<proto::TelemetryRequest>,
        tonic::Streaming<proto::TelemetryResponse>,
    ),
    Status,
> {
    let channel = dial(fixture, identity)
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

/// 开 Control 双工流；hello 先入缓冲。
async fn open_control(
    fixture: &PrimaryFixture,
    identity: Option<Identity>,
    hello: proto::AgentControlHello,
    client_secret: Option<&str>,
) -> Result<
    (
        tokio::sync::mpsc::Sender<proto::AgentControlRequest>,
        tonic::Streaming<proto::PrimaryControlResponse>,
    ),
    Status,
> {
    let channel = dial(fixture, identity)
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    let mut client =
        proto::santaizi_control_service_client::SantaiziControlServiceClient::new(channel);
    let (tx, rx) = tokio::sync::mpsc::channel::<proto::AgentControlRequest>(8);
    tx.send(proto::AgentControlRequest {
        body: Some(proto::agent_control_request::Body::Hello(hello)),
    })
    .await
    .map_err(|err| Status::internal(format!("buffer control hello: {err}")))?;
    let mut request = Request::new(ReceiverStream::new(rx));
    if let Some(client_secret) = client_secret {
        let key: tonic::metadata::MetadataKey<tonic::metadata::Ascii> =
            "client_secret".parse().expect("client secret key");
        request.metadata_mut().insert(
            key,
            MetadataValue::try_from(client_secret).expect("client secret value"),
        );
    }
    let stream = client.control(request).await?.into_inner();
    Ok((tx, stream))
}

fn control_hello() -> proto::AgentControlHello {
    proto::AgentControlHello {
        node_uuid: node(),
        session_id: session(),
        current_config_version: 1,
        agent_version: "0.1.0".to_string(),
        ..Default::default()
    }
}

#[tokio::test]
async fn control_certificate_path_issues_credential_and_assignment() {
    let fixture = setup_primary("control-cert", true, true).await;
    let (enrolled, key) = enroll_with_key(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    let (tx, mut stream) = open_control(
        &fixture,
        Some(identity_from(&enrolled.certificate_pem, &key)),
        control_hello(),
        None,
    )
    .await
    .unwrap();

    // 首帧必须是凭证，且能被主端签名器验签。
    let credential = match stream.message().await.unwrap().unwrap().body {
        Some(proto::primary_control_response::Body::Credential(credential)) => credential,
        other => panic!("expected credential, got {other:?}"),
    };
    let now = santaizi_dashboard::util::time::now_nanos() / 1_000_000_000;
    let verification = credential::verify_credential(
        &fixture.state.signer().public_key(),
        &fixture.state.signer().key_id(),
        &credential,
        now,
        0,
        false,
    )
    .expect("credential verifies");
    assert_eq!(verification.claims.node_uuid, node());
    assert_eq!(verification.claims.capability, "telemetry");

    // 第二帧是端点指派：primary 恒在。
    let assignment = match stream.message().await.unwrap().unwrap().body {
        Some(proto::primary_control_response::Body::Assignment(assignment)) => assignment,
        other => panic!("expected assignment, got {other:?}"),
    };
    assert_eq!(assignment.config_version, 1);
    assert_eq!(assignment.endpoints.len(), 1);
    assert_eq!(assignment.endpoints[0].endpoint_id, "primary");
    assert_eq!(
        assignment.endpoints[0].kind,
        proto::EndpointKind::Primary as i32
    );

    // 运行态帧落库。
    tx.send(proto::AgentControlRequest {
        body: Some(proto::agent_control_request::Body::Runtime(
            proto::AgentRuntime {
                wal_pressure: proto::WalPressure::Healthy as i32,
                wal_bytes: 2048,
                pending_events: 3,
                protocol_version: "2".to_string(),
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();
    // 探测回执暂不派发：不应报错，流保持可用。
    tx.send(proto::AgentControlRequest {
        body: Some(proto::agent_control_request::Body::ProbeResult(
            proto::ProbeResult {
                probe_id: "probe-1".to_string(),
                successful: true,
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();
    drop(tx);
    assert!(
        stream.message().await.unwrap().is_none(),
        "stream should end cleanly"
    );

    let runtime = fixture
        .db
        .read(|conn| {
            let row = conn
                .query_row(
                    "SELECT wal_bytes, pending_events, agent_version FROM agent_telemetry_runtimes WHERE node_uuid = ?",
                    [&node()],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .map_err(anyhow::Error::from)?;
            Ok(row)
        })
        .unwrap();
    assert_eq!(runtime, (2048, 3, "0.1.0".to_string()));
}

#[tokio::test]
async fn control_client_secret_fallback_issues_credential() {
    let fixture = setup_primary("control-secret", false, false).await;
    // TLS 关闭时 enroll 要求 TLS 被拒：绑定改由直写数据库建立。
    let err = enroll_with_key(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
    fixture
        .db
        .write(|tx| {
            bindings::bind_server_node_for_protocol(tx, SERVER_ID, &node(), 1, true)?;
            Ok(())
        })
        .unwrap();

    let (tx, mut stream) = open_control(&fixture, None, control_hello(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    let credential = match stream.message().await.unwrap().unwrap().body {
        Some(proto::primary_control_response::Body::Credential(credential)) => credential,
        other => panic!("expected credential, got {other:?}"),
    };
    assert!(!credential.claims.is_empty());
    assert!(matches!(
        stream.message().await.unwrap().unwrap().body,
        Some(proto::primary_control_response::Body::Assignment(_))
    ));
    drop(tx);

    // 错误秘密：认证失败经流返回。
    let (_tx, mut stream) = open_control(&fixture, None, control_hello(), Some("wrong-secret"))
        .await
        .unwrap();
    let err = stream.message().await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn control_certificate_uuid_mismatch_is_permission_denied() {
    let fixture = setup_primary("control-mismatch", true, true).await;
    let (enrolled, key) = enroll_with_key(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    let mut hello = control_hello();
    hello.node_uuid = other_node();
    let (_tx, mut stream) = open_control(
        &fixture,
        Some(identity_from(&enrolled.certificate_pem, &key)),
        hello,
        None,
    )
    .await
    .unwrap();
    let err = stream.message().await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn control_invalid_hello_is_invalid_argument() {
    let fixture = setup_primary("control-invalid-hello", false, false).await;
    fixture
        .db
        .write(|tx| {
            bindings::bind_server_node_for_protocol(tx, SERVER_ID, &node(), 1, true)?;
            Ok(())
        })
        .unwrap();
    let mut hello = control_hello();
    hello.node_uuid = vec![0x01; 15];
    let (_tx, mut stream) = open_control(&fixture, None, hello, Some(CLIENT_SECRET))
        .await
        .unwrap();
    let err = stream.message().await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn ingest_batch_acks_and_flips_runtime_online() {
    let fixture = setup_primary("ingest-batch", true, false).await;
    // TLS 单向（无客户端证书）+ require_agent_mtls=false：Ingest 只验凭证。
    let (enrolled, _key) = enroll_with_key(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    assert!(!enrolled.certificate_pem.is_empty());
    let credential = sign_credential(&fixture, &node());
    let (tx, mut stream) = open_ingest(&fixture, None, ingest_hello(credential))
        .await
        .unwrap();

    tx.send(proto::TelemetryRequest {
        body: Some(proto::telemetry_request::Body::Batch(telemetry_batch(&[
            1, 2,
        ]))),
    })
    .await
    .unwrap();
    let response = stream.message().await.unwrap().unwrap();
    assert_eq!(response.acks.len(), 1);
    assert_eq!(response.acks[0].ack_through, 2);
    assert_eq!(response.acks[0].node_uuid, node());
    assert_eq!(response.acks[0].session_id, session());

    // ping → pong。
    tx.send(proto::TelemetryRequest {
        body: Some(proto::telemetry_request::Body::Ping(
            proto::TelemetryPing {},
        )),
    })
    .await
    .unwrap();
    let pong = stream.message().await.unwrap().unwrap();
    assert!(pong.pong.is_some());

    // 快照节点不匹配 hello → PermissionDenied。
    tx.send(proto::TelemetryRequest {
        body: Some(proto::telemetry_request::Body::RealtimeSnapshot(
            proto::RealtimeSnapshot {
                node_uuid: other_node(),
                session_id: session(),
                latest_sequence: 3,
                collected_at_unix_nano: santaizi_dashboard::util::time::now_nanos(),
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();
    let err = stream.message().await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
    drop(tx);

    // 落库：两条事件证据 + 运行态翻在线。
    let (events, status, last_ip): (i64, String, String) = fixture
        .db
        .read(|conn| {
            let events = conn.query_row("SELECT COUNT(*) FROM telemetry_events", [], |r| {
                r.get::<_, i64>(0)
            })?;
            let row = conn.query_row(
                "SELECT status, last_ip FROM server_runtimes WHERE server_id = ?",
                [SERVER_ID],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )?;
            Ok((events, row.0, row.1))
        })
        .unwrap();
    assert_eq!(events, 2);
    assert_eq!(status, "online");
    assert_eq!(last_ip, "10.9.9.9");
}

#[tokio::test]
async fn ingest_rejects_bad_hello_and_empty_batch() {
    let fixture = setup_primary("ingest-reject", true, false).await;
    let credential = sign_credential(&fixture, &node());

    // endpoint_id 不是 primary。
    let mut hello = ingest_hello(credential.clone());
    if let Some(proto::telemetry_request::Body::Hello(hello)) = &mut hello.body {
        hello.endpoint_id = "collector".to_string();
    }
    let (_tx, mut stream) = open_ingest(&fixture, None, hello).await.unwrap();
    let err = stream.message().await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    // 凭证签给别的节点。
    let mut hello = ingest_hello(sign_credential(&fixture, &other_node()));
    if let Some(proto::telemetry_request::Body::Hello(hello)) = &mut hello.body {
        hello.node_uuid = node();
    }
    let (_tx, mut stream) = open_ingest(&fixture, None, hello).await.unwrap();
    let err = stream.message().await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);

    // 空批次。
    let (tx, mut stream) = open_ingest(&fixture, None, ingest_hello(credential))
        .await
        .unwrap();
    tx.send(proto::TelemetryRequest {
        body: Some(proto::telemetry_request::Body::Batch(
            proto::TelemetryBatch {
                records: Vec::new(),
            },
        )),
    })
    .await
    .unwrap();
    let err = stream.message().await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn ingest_tampered_credential_is_unauthenticated() {
    let fixture = setup_primary("ingest-tamper", true, false).await;
    let mut credential = sign_credential(&fixture, &node());
    credential.signature[0] ^= 0xff;
    let (_tx, mut stream) = open_ingest(&fixture, None, ingest_hello(credential))
        .await
        .unwrap();
    let err = stream.message().await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
}
