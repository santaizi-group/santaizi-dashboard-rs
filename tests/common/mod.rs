//! 主端集成测试共享夹具：一座真实 TCP/TLS 临时主端 + 设备 CSR/证书辅助。
//!
//! 供 `rpc_mtls.rs`、`collector_sync.rs` 等集成测试复用；每个用例仍自建
//! 独立 fixture（独立数据目录 / PKI / SQLite / 端口），互不干扰。
//! 各测试二进制只用到其中一部分辅助函数，故整体允许 dead_code。
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rcgen::string::Ia5String;
use rcgen::{CertificateParams, KeyPair, PKCS_ED25519, SanType};
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Request, Status};

use santaizi_dashboard::config::Config;
use santaizi_dashboard::db::{self, Db};
use santaizi_dashboard::pki;
use santaizi_dashboard::pki::uri;
use santaizi_dashboard::proto;
use santaizi_dashboard::rpc;
use santaizi_dashboard::secret;
use santaizi_dashboard::state::AppState;
use santaizi_dashboard::telemetry::credential;

pub const CLIENT_SECRET: &str = "test-client-secret";
pub const SERVER_ID: i64 = 7;
pub const COLLECTOR_UUID: &str = "collector-edge";

pub fn node() -> Vec<u8> {
    vec![0x21; 16]
}

pub fn other_node() -> Vec<u8> {
    vec![0x22; 16]
}

static SEQ: AtomicU64 = AtomicU64::new(0);

pub fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "santaizi-mtls-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// 进程级共享业务密钥：并行 fixture 共用同一把密钥，
/// 避免互相覆盖进程级密钥导致对方密文解不开。
pub fn shared_secret_key_path() -> &'static str {
    static PATH: OnceLock<String> = OnceLock::new();
    PATH.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("santaizi-mtls-shared-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create shared dir");
        let path = dir.join("business-secrets.key");
        if !path.exists() {
            std::fs::write(&path, [0x5a_u8; 32]).expect("write shared secret key");
        }
        path.to_string_lossy().into_owned()
    })
}

pub struct PrimaryFixture {
    pub dir: PathBuf,
    pub db: Db,
    pub state: Arc<AppState>,
    pub port: u16,
    pub server_cert_pem: Vec<u8>,
    pub token: String,
    pub server: tokio::task::JoinHandle<()>,
}

impl Drop for PrimaryFixture {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub async fn setup_primary(tag: &str, require_mtls: bool) -> PrimaryFixture {
    setup_primary_with(tag, require_mtls, |_conf| {}).await
}

/// 带配置覆写的夹具：闭子在 `AppState` 构建前修改 `Config`
/// （查看密码、主题、语言等 HTTP 面用例用）。
pub async fn setup_primary_with(
    tag: &str,
    require_mtls: bool,
    override_conf: impl FnOnce(&mut Config),
) -> PrimaryFixture {
    // rustls 提供者须在客户端拨号前装配（测试不经 `run()` 入口）。
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
    conf.grpc_tls.enabled = require_mtls;
    conf.grpc_tls.cert_file = cert_file.to_string_lossy().into_owned();
    conf.grpc_tls.key_file = key_file.to_string_lossy().into_owned();
    conf.grpc_tls.require_agent_mtls = require_mtls;
    conf.grpc_tls.require_collector_mtls = require_mtls;
    override_conf(&mut conf);

    let state = AppState::primary(conf, db.clone())
        .await
        .expect("app state");

    let ciphertext = secret::encrypt_secret(CLIENT_SECRET).expect("encrypt client secret");
    let (token, token_hash) = credential::new_registration_token().expect("registration token");
    let seed_db = db.clone();
    tokio::task::spawn_blocking(move || {
        seed_db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) VALUES (?, 'node-7', 'tag1', ?, 1, 0)",
                rusqlite::params![SERVER_ID, &ciphertext],
            )?;
            tx.execute(
                "INSERT INTO collectors (collector_uuid, name, address, token_hash, token_ciphertext, kind, tcp_ports, revoked, deleted) VALUES (?, 'Edge', 'edge:5556', ?, X'00', 'observer', '', 0, 0)",
                rusqlite::params![COLLECTOR_UUID, &token_hash.to_vec()],
            )?;
            // 作用域 all：enroll 换绑时为该 collector 生成指派，Sync 配置才有内容。
            tx.execute(
                "INSERT INTO collector_scopes (collector_uuid, scope_type, scope_value) VALUES (?, 'all', '')",
                rusqlite::params![COLLECTOR_UUID],
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
        token,
        server,
    }
}

/// 写一个带有效会话令牌的用户，返回令牌明文。
pub fn insert_user(db: &santaizi_dashboard::db::Db, login: &str, super_admin: bool) -> String {
    let token = format!("session-token-{login}");
    let expires = santaizi_dashboard::util::time::now_nanos() / 1_000_000_000 + 3600;
    db.write(|tx| {
        tx.execute(
            "INSERT INTO users (login, name, token, token_expired, super_admin) \
             VALUES (?1, ?1, ?2, datetime(?3, 'unixepoch'), ?4)",
            rusqlite::params![login, token, expires, super_admin as i64],
        )?;
        Ok(())
    })
    .unwrap();
    token
}

/// 写一条 API Token，返回呈现给 Bearer 头的明文。
pub fn insert_api_token(db: &santaizi_dashboard::db::Db, user_id: i64, permission: &str) -> String {
    use sha2::{Digest, Sha256};
    let plain = format!("api-token-{user_id}-{permission}");
    let ciphertext = santaizi_dashboard::secret::encrypt_secret(&plain).expect("encrypt token");
    let hash = Sha256::digest(plain.as_bytes()).to_vec();
    db.write(|tx| {
        tx.execute(
            "INSERT INTO api_tokens (user_id, token_ciphertext, token_hash, permission, enabled) \
             VALUES (?1, ?2, ?3, ?4, 1)",
            rusqlite::params![user_id, ciphertext, hash, permission],
        )?;
        Ok(())
    })
    .unwrap();
    plain
}

pub fn user_id_by_login(db: &santaizi_dashboard::db::Db, login: &str) -> i64 {
    db.read(|conn| {
        Ok(
            conn.query_row("SELECT id FROM users WHERE login = ?1", [login], |r| {
                r.get::<_, i64>(0)
            })?,
        )
    })
    .unwrap()
}

/// 起真实 HTTP 服务（`api::serve_http` 同一入口），返回端口与取消令牌。
pub async fn spawn_http(
    fixture: &PrimaryFixture,
) -> (u16, CancellationToken, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind http port");
    let port = listener.local_addr().expect("local addr").port();
    let cancel = CancellationToken::new();
    let server_cancel = cancel.clone();
    let state = Arc::clone(&fixture.state);
    let server = tokio::spawn(async move {
        let _ = santaizi_dashboard::api::serve_http(state, listener, server_cancel).await;
    });
    (port, cancel, server)
}

pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("http client")
}

pub fn base_url(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

/// 从 Set-Cookie 头里取指定 Cookie 的值。
pub fn cookie_value(response: &reqwest::Response, name: &str) -> Option<String> {
    response
        .headers()
        .get_all(reqwest::header::SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .find_map(|raw| {
            let (key, value) = raw.split_once('=')?;
            (key == name).then(|| value.split(';').next().unwrap_or("").to_string())
        })
}

/// 拨号：带客户端身份走 mTLS；TLS 开启但不带身份走单向 TLS；TLS 关闭走明文。
pub async fn dial(fixture: &PrimaryFixture, identity: Option<Identity>) -> anyhow::Result<Channel> {
    let tls_enabled = fixture.state.conf().grpc_tls.enabled;
    let endpoint = if tls_enabled {
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

/// 生成带设备 URI SAN 的 CSR；返回 `(DER, 私钥 PEM)`。
pub fn device_csr(uri: &str) -> (Vec<u8>, String) {
    let key_pair = KeyPair::generate_for(&PKCS_ED25519).expect("generate key");
    let mut params = CertificateParams::default();
    params.subject_alt_names = vec![SanType::URI(
        Ia5String::try_from(uri.to_string()).expect("uri san"),
    )];
    let csr = params.serialize_request(&key_pair).expect("serialize csr");
    (csr.der().to_vec(), key_pair.serialize_pem())
}

pub fn device_identity(cert_pem: &str) -> uri::DeviceIdentity {
    let der = pki::ca::cert_der_from_pem(cert_pem.as_bytes()).expect("cert der");
    uri::device_identity_from_der(&der).expect("device identity")
}

pub fn identity_from(cert_pem: &str, key_pem: &str) -> Identity {
    Identity::from_pem(cert_pem.as_bytes(), key_pem.as_bytes())
}

/// Enroll 并返回 `(响应, 首份 CSR 的私钥 PEM)`，供后续 mTLS 拨号使用。
pub async fn enroll_with_key(
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

pub async fn enroll(
    fixture: &PrimaryFixture,
    node_uuid: &[u8],
    client_secret: Option<&str>,
) -> Result<proto::AgentEnrollResponse, Status> {
    enroll_with_key(fixture, node_uuid, client_secret)
        .await
        .map(|(response, _)| response)
}

/// 注册 collector 并返回 `(响应, CSR 的私钥 PEM)`。
pub async fn register_collector(
    fixture: &PrimaryFixture,
) -> Result<(proto::RegisterCollectorResponse, String), Status> {
    let (csr_der, key_pem) = device_csr(&uri::encode_collector_uri("pending"));
    let channel = dial(fixture, None)
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    let mut client =
        proto::santaizi_collector_service_client::SantaiziCollectorServiceClient::new(channel);
    let response = client
        .register(Request::new(proto::RegisterCollectorRequest {
            registration_token: fixture.token.clone(),
            protocol_version: "2".to_string(),
            csr_der,
        }))
        .await?
        .into_inner();
    Ok((response, key_pem))
}

/// 建从端库 + Runtime（不启动循环）；`insecure` 对应 `primary_insecure_tls`
/// （自签主端证书下 Go 既定的连通路径）；`runtime_resend` 可缩短运行态帧间隔；
/// `pre_authorization` 在 Runtime 构造前写入授权缓存（agent 服务测试免去
/// 等待 Sync 下发）；`status_authorization` 进 CollectorModeConfig（GetStatus）。
pub async fn setup_collector_runtime(
    fixture: &PrimaryFixture,
    tag: &str,
    primary_tls: bool,
    insecure: bool,
    runtime_resend: Option<Duration>,
    pre_authorization: Option<proto::CollectorAuthorizationConfig>,
    status_authorization: &str,
) -> (
    Arc<santaizi_dashboard::collector::runtime::Runtime>,
    Db,
    PathBuf,
) {
    let dir = temp_dir(tag);
    let db_path = dir.join("collector.db");
    let db = db::migrate::open_collector(db_path.to_str().expect("collector db path"))
        .expect("open collector db");
    if let Some(config) = pre_authorization {
        db.write(|tx| {
            santaizi_dashboard::collector::store::save_authorization(tx, COLLECTOR_UUID, &config, 1)
        })
        .expect("seed collector authorization");
    }
    let config = santaizi_dashboard::config::CollectorModeConfig {
        primary_endpoint: format!("localhost:{}", fixture.port),
        primary_tls,
        primary_insecure_tls: insecure,
        registration_token: fixture.token.clone(),
        database_path: db_path.to_string_lossy().into_owned(),
        status_authorization: status_authorization.to_string(),
        ..Default::default()
    };
    let mut runtime = santaizi_dashboard::collector::runtime::Runtime::new(
        CancellationToken::new(),
        db.clone(),
        db_path.to_str().expect("collector db path"),
        config,
    )
    .await
    .expect("collector runtime");
    if let Some(interval) = runtime_resend {
        runtime = runtime.with_runtime_resend(interval);
    }
    (Arc::new(runtime), db, dir)
}

/// 便捷构造：observer 从端授权缓存（含 `node` 指派），主端公钥来自签名器。
pub fn observer_authorization(
    fixture: &PrimaryFixture,
    node_uuid: &[u8],
) -> proto::CollectorAuthorizationConfig {
    let now = santaizi_dashboard::util::time::now_nanos();
    proto::CollectorAuthorizationConfig {
        config_version: 1,
        primary_public_key: fixture.state.signer().public_key(),
        key_id: fixture.state.signer().key_id(),
        kind: proto::CollectorKind::Observer as i32,
        assignments: vec![proto::NodeAssignment {
            node_uuid: node_uuid.to_vec(),
            observer_id: COLLECTOR_UUID.to_string(),
            valid_from_unix_nano: now,
            valid_to_unix_nano: 0,
            generation: 1,
            config_version: 1,
        }],
        agent_ca_certificate_pem: fixture
            .state
            .pki()
            .map(|bundle| String::from_utf8_lossy(&bundle.agent.cert_pem).into_owned())
            .unwrap_or_default(),
        ..Default::default()
    }
}

/// 打开 collector Sync 双工流；返回 `(发送端, 响应流)`。
///
/// tonic 的流式调用要等服务端响应头才返回，而服务端处理器先等 hello 帧——
/// 因此首帧必须先入 mpsc 缓冲再拨号；`None` 表示直接半关闭请求流。
pub async fn open_sync(
    fixture: &PrimaryFixture,
    identity: Identity,
    initial: Option<proto::CollectorSyncRequest>,
) -> Result<
    (
        Option<tokio::sync::mpsc::Sender<proto::CollectorSyncRequest>>,
        tonic::Streaming<proto::CollectorSyncResponse>,
    ),
    Status,
> {
    let channel = dial(fixture, Some(identity))
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    let mut client =
        proto::santaizi_collector_service_client::SantaiziCollectorServiceClient::new(channel);
    let (tx, rx) = tokio::sync::mpsc::channel::<proto::CollectorSyncRequest>(8);
    let sender = match initial {
        Some(initial) => {
            tx.send(initial)
                .await
                .map_err(|err| Status::internal(format!("buffer sync frame: {err}")))?;
            Some(tx)
        }
        None => {
            drop(tx);
            None
        }
    };
    let stream = client
        .sync(Request::new(ReceiverStream::new(rx)))
        .await?
        .into_inner();
    Ok((sender, stream))
}

/// 开流并发送 hello（`None` 表示直接半关闭）后取回首个错误。
/// 认证层的拒绝可能出现在开流或首条消息两个阶段。
pub async fn sync_first_error(
    fixture: &PrimaryFixture,
    identity: Identity,
    hello: Option<proto::CollectorSyncHello>,
) -> Status {
    let initial = hello.map(|hello| proto::CollectorSyncRequest {
        body: Some(proto::collector_sync_request::Body::Hello(hello)),
    });
    let (_sender, mut stream) = match open_sync(fixture, identity, initial).await {
        Ok(stream) => stream,
        Err(status) => return status,
    };
    match stream.message().await {
        Ok(Some(_)) => panic!("expected a stream error"),
        Ok(None) => panic!("stream closed without an error"),
        Err(status) => status,
    }
}
