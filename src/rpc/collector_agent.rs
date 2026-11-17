//! 从端 agent 服务：对齐 Go `service/collector/runtime.go` 的 `Ingest` /
//! `GetStatus` 与 `cmd/dashboard/rpc/collector.go` 的 `ServeCollectorRPC`。
//!
//! 从端在 `grpcport` 上监听 `SantaiziTelemetryService`（Ingest）与
//! `SantaiziCollectorService`（GetStatus）：探针把可靠批次投递给所在从端，
//! 落本地事实表并入队出库，由 replication 循环复制到主端；运维经
//! `status_authorization` 查询从端运行态。Register / Sync / RenewCollector
//! 是从端→主端方向，这里不实现（tonic trait 无默认体，显式 unimplemented）。
//!
//! 与 Go 的实现对齐：tonic 的 `ServerTlsConfig` 接收静态 CA 池，因此通过监听
//! `Runtime` 的 `agent-ca.crt` watch 通道，在首次注册落盘或运行期轮换 Agent CA
//! 时热重建 tonic `Server` 实例（复用同一个 `TcpListener` 分发连接），无需人工重启从端。

use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

use subtle::ConstantTimeEq;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

use crate::collector::runtime::Runtime;
use crate::collector::store;
use crate::config::Config;
use crate::pki::uri::UriError;
use crate::proto;
use crate::rpc::auth;
use crate::rpc::support::{blocking_read, blocking_write};
use crate::util::time as util_time;

/// 响应通道容量：acks / pong 顺序送达，吸收突发批次足够。
const INGEST_RESPONSE_CAPACITY: usize = 32;

/// 从端 agent 服务处理器：Ingest 流 + GetStatus。
#[derive(Clone)]
pub struct AgentServiceHandler {
    runtime: Arc<Runtime>,
    /// 凭证宽限期（Go `NewRuntime` 的 `grace`，来自 `telemetry.credential_grace_days`）。
    credential_grace_secs: i64,
}

impl AgentServiceHandler {
    pub fn new(runtime: Arc<Runtime>, credential_grace_secs: i64) -> Self {
        Self {
            runtime,
            credential_grace_secs,
        }
    }

    /// `Runtime.Ingest`：hello 校验 → 逐帧处理 batch / ping；快照不 ACK。
    async fn run_ingest(
        &self,
        peer: Option<StdResult<crate::pki::uri::DeviceIdentity, UriError>>,
        mut stream: Streaming<proto::TelemetryRequest>,
        tx: mpsc::Sender<StdResult<proto::TelemetryResponse, Status>>,
    ) -> StdResult<(), Status> {
        if self.runtime.is_probe() {
            return Err(Status::failed_precondition(
                "probe collector does not accept agent ingest",
            ));
        }
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("collector telemetry hello is required"))?;
        let hello = match &first.body {
            Some(proto::telemetry_request::Body::Hello(hello)) => hello.clone(),
            _ => {
                return Err(Status::invalid_argument(
                    "collector telemetry hello is required",
                ));
            }
        };
        if hello.node_uuid.len() != 16 {
            return Err(Status::invalid_argument(
                "collector telemetry hello is required",
            ));
        }
        let collector_uuid = self.runtime.collector_uuid();
        if collector_uuid.is_empty() || hello.endpoint_id != collector_uuid {
            return Err(Status::permission_denied(
                "telemetry endpoint identity mismatch",
            ));
        }
        let authorized = blocking_read(self.runtime.db(), {
            let node_uuid = hello.node_uuid.clone();
            move |conn| store::is_node_authorized(conn, &node_uuid, util_time::now_nanos())
        })
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
        if !authorized {
            return Err(Status::permission_denied(
                "node is not assigned to this collector",
            ));
        }
        let cache = blocking_read(self.runtime.db(), store::authorization)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .ok_or_else(|| Status::unavailable("collector authorization cache is unavailable"))?;
        let now_unix = util_time::now_nanos() / 1_000_000_000;
        let verification = crate::telemetry::credential::verify_credential(
            &cache.primary_public_key,
            &cache.key_id,
            hello
                .credential
                .as_ref()
                .ok_or_else(|| Status::unauthenticated("telemetry credential is required"))?,
            now_unix,
            self.credential_grace_secs,
            authorized,
        )
        .map_err(|err| Status::unauthenticated(err.to_string()))?;
        if verification.claims.node_uuid != hello.node_uuid {
            return Err(Status::permission_denied("credential node mismatch"));
        }
        match_ingest_certificate(
            peer.as_ref(),
            &hello.node_uuid,
            &verification.claims.node_uuid,
        )?;

        self.runtime.connected_agent_connected();
        let _guard = ConnectedAgentGuard {
            runtime: Arc::clone(&self.runtime),
        };

        while let Some(request) = stream.message().await? {
            let received_at_nanos = util_time::now_nanos();
            match request.body {
                Some(proto::telemetry_request::Body::Batch(batch)) => {
                    let result = self
                        .ingest_batch(batch, &collector_uuid, received_at_nanos)
                        .await?;
                    if result.enqueued > 0 {
                        self.runtime.wake_replication();
                    }
                    send_response(
                        &tx,
                        proto::TelemetryResponse {
                            acks: result.acks,
                            ..Default::default()
                        },
                    )
                    .await?;
                }
                Some(proto::telemetry_request::Body::Ping(_)) => {
                    send_response(
                        &tx,
                        proto::TelemetryResponse {
                            pong: Some(proto::TelemetryPong {}),
                            ..Default::default()
                        },
                    )
                    .await?;
                }
                // 实时快照刻意不 ACK、不推进可靠游标：探针与主端保持直连
                // realtime 路径（Go 同）。
                Some(proto::telemetry_request::Body::RealtimeSnapshot(_)) => {}
                _ => return Err(Status::invalid_argument("unexpected telemetry request")),
            }
        }
        Ok(())
    }

    /// `Store.Ingest`：批次落本地事实表并入队出库（单事务）。
    async fn ingest_batch(
        &self,
        batch: proto::TelemetryBatch,
        collector_uuid: &str,
        received_at_nanos: i64,
    ) -> StdResult<store::IngestResult, Status> {
        let collector_uuid = collector_uuid.to_string();
        blocking_write(self.runtime.db(), move |tx| {
            store::ingest(tx, &batch, &collector_uuid, received_at_nanos)
        })
        .await
        .map_err(|err| Status::invalid_argument(err.to_string()))
    }

    /// `Runtime.GetStatus`：`status_authorization` 常量时间比对后回运行态。
    async fn get_status(
        &self,
        request: Request<proto::CollectorStatusRequest>,
    ) -> StdResult<Response<proto::CollectorStatus>, Status> {
        let expected = &self.runtime.config().status_authorization;
        if expected.is_empty()
            || !constant_time_eq(
                request.get_ref().authorization.as_bytes(),
                expected.as_bytes(),
            )
        {
            return Err(Status::unauthenticated(
                "collector status authorization failed",
            ));
        }
        let snapshot = self
            .runtime
            .runtime_snapshot()
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(proto::CollectorStatus {
            connected_agents: snapshot.connected_agents,
            spool_size: snapshot.spool_size,
            pending_records: snapshot.pending_records,
            oldest_pending_unix_nano: snapshot.oldest_pending_unix_nano,
            replication_cursor: snapshot.replication_cursor,
            last_primary_seen_unix_nano: snapshot.last_primary_seen_unix_nano,
            protocol_version: snapshot.protocol_version,
        }))
    }
}

/// 连接计数守卫：流退出（含错误路径）时递减（Go `defer connectedAgents.Add(^uint64(0))`）。
struct ConnectedAgentGuard {
    runtime: Arc<Runtime>,
}

impl Drop for ConnectedAgentGuard {
    fn drop(&mut self) {
        self.runtime.connected_agent_disconnected();
    }
}

/// `matchIngestCertificate`：无证书且不强制时放行；有证书则 kind 必须是
/// agent 且 UUID 同时匹配 hello 与凭证。
fn match_ingest_certificate(
    peer: Option<&StdResult<crate::pki::uri::DeviceIdentity, UriError>>,
    hello_uuid: &[u8],
    credential_uuid: &[u8],
) -> StdResult<(), Status> {
    let Some(peer) = peer else {
        return Ok(());
    };
    let identity = peer
        .as_ref()
        .map_err(|err| Status::permission_denied(err.to_string()))?;
    if identity.kind != crate::pki::uri::DeviceKind::Agent {
        return Err(Status::permission_denied(
            "ingest requires an agent certificate",
        ));
    }
    if identity.node_uuid != hello_uuid || identity.node_uuid != credential_uuid {
        return Err(Status::permission_denied(
            "certificate UUID does not match hello or credential",
        ));
    }
    Ok(())
}

/// 常量时间字节比较（Go `subtle.ConstantTimeCompare`）。
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.ct_eq(right).into()
}

async fn send_response(
    tx: &mpsc::Sender<StdResult<proto::TelemetryResponse, Status>>,
    response: proto::TelemetryResponse,
) -> StdResult<(), Status> {
    tx.send(Ok(response))
        .await
        .map_err(|err| Status::internal(format!("send telemetry response: {err}")))
}

#[tonic::async_trait]
impl proto::santaizi_telemetry_service_server::SantaiziTelemetryService for AgentServiceHandler {
    type IngestStream = ReceiverStream<StdResult<proto::TelemetryResponse, Status>>;

    async fn ingest(
        &self,
        request: Request<Streaming<proto::TelemetryRequest>>,
    ) -> StdResult<Response<Self::IngestStream>, Status> {
        // 对端身份须在 `into_inner` 前取出：TLS 扩展挂在 Request 上。
        let peer = auth::peer_device_identity_result(request.extensions());
        let stream = request.into_inner();
        let (tx, rx) =
            mpsc::channel::<StdResult<proto::TelemetryResponse, Status>>(INGEST_RESPONSE_CAPACITY);
        // hello 校验放进任务：响应头随流立即返回，与 Go `SendHeader(nil)` 等价，
        // 避免 tonic 客户端因等服务端首帧而阻塞在开流调用上。
        let handler = self.clone();
        tokio::spawn(async move {
            if let Err(status) = handler.run_ingest(peer, stream, tx.clone()).await {
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tonic::async_trait]
impl proto::santaizi_collector_service_server::SantaiziCollectorService for AgentServiceHandler {
    type SyncStream = ReceiverStream<StdResult<proto::CollectorSyncResponse, Status>>;

    /// 从端→主端方向的方法在从端侧不服务。
    async fn register(
        &self,
        _request: Request<proto::RegisterCollectorRequest>,
    ) -> StdResult<Response<proto::RegisterCollectorResponse>, Status> {
        Err(Status::unimplemented(
            "collector register is served by the primary",
        ))
    }

    async fn sync(
        &self,
        _request: Request<Streaming<proto::CollectorSyncRequest>>,
    ) -> StdResult<Response<Self::SyncStream>, Status> {
        Err(Status::unimplemented(
            "collector sync is served by the primary",
        ))
    }

    async fn get_status(
        &self,
        request: Request<proto::CollectorStatusRequest>,
    ) -> StdResult<Response<proto::CollectorStatus>, Status> {
        self.get_status(request).await
    }

    async fn renew_collector(
        &self,
        _request: Request<proto::CollectorRenewRequest>,
    ) -> StdResult<Response<proto::CollectorEnrollResponse>, Status> {
        Err(Status::unimplemented(
            "collector renew is served by the primary",
        ))
    }
}

/// keepalive：与主端同一组参数（Go `grpcKeepaliveOptions`）。
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// `ServeCollectorRPC`：从端 agent 服务监听。`force_agent_ingest` 对应 Go
/// `SetForceAgentIngest(GRPCTLS.Enabled)`：TLS 开启时 Ingest 必须带 agent
/// 证书。监听 `Runtime` 的 `agent-ca.crt` 变化并在无中断复用 `TcpListener` 的前提下
/// 热重建 tonic `Server` 实例；`cancel` 触发即优雅退出。
pub async fn serve_collector_agent(
    runtime: Arc<Runtime>,
    listener: tokio::net::TcpListener,
    cancel: CancellationToken,
    conf: &Config,
) -> anyhow::Result<()> {
    crate::util::tls::install_crypto_provider();
    let policy = auth::DeviceAuthPolicy {
        require_agent_mtls: false,
        require_collector_mtls: false,
        force_agent_ingest: conf.grpc_tls.enabled,
    };
    let grace_secs = (conf.telemetry.credential_grace_days.clamp(0, 3650) * 86_400) as i64;
    let listen_addr = listener.local_addr()?;

    if !conf.grpc_tls.enabled {
        let handler = AgentServiceHandler::new(Arc::clone(&runtime), grace_secs);
        let telemetry =
            proto::santaizi_telemetry_service_server::SantaiziTelemetryServiceServer::new(
                handler.clone(),
            );
        let collector =
            proto::santaizi_collector_service_server::SantaiziCollectorServiceServer::new(handler);
        let (_health_reporter, health_service) = tonic_health::server::health_reporter();

        tracing::info!(
            listen = %listen_addr,
            tls = false,
            "collector agent 服务启动"
        );
        Server::builder()
            .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
            .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
            .layer(auth::DeviceAuthLayer::new(policy))
            .add_service(telemetry)
            .add_service(collector)
            .add_service(health_service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                cancel.cancelled().await;
            })
            .await
            .map_err(|err| anyhow::anyhow!("serve collector agent: {err}"))?;
        tracing::info!("collector agent 服务退出");
        return Ok(());
    }

    let mut ca_rx = runtime.subscribe_agent_ca();
    // 先标记已见快照再读取磁盘 TLS 配置：即便并发更新发生在两者之间，下一轮 changed() 也会立即捕获。
    let _ = ca_rx.borrow_and_update();
    let initial_tls = collector_agent_tls_config(&runtime, conf)?;
    let (mut conn_tx, mut gen_cancel, mut gen_task) = spawn_collector_agent_generation(
        Arc::clone(&runtime),
        policy.clone(),
        grace_secs,
        initial_tls,
    )?;

    tracing::info!(
        listen = %listen_addr,
        tls = true,
        "collector agent 服务启动"
    );
    let mut retiring: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                drop(conn_tx);
                gen_cancel.cancel();
                break;
            }
            changed = ca_rx.changed() => {
                if changed.is_err() {
                    continue;
                }
                match collector_agent_tls_config(&runtime, conf) {
                    Ok(next_tls) => match spawn_collector_agent_generation(
                        Arc::clone(&runtime),
                        policy.clone(),
                        grace_secs,
                        next_tls,
                    ) {
                        Ok((next_tx, next_cancel, next_task)) => {
                            tracing::info!(
                                listen = %listen_addr,
                                "agent-ca.crt 已更新，热重建 collector agent TLS 服务"
                            );
                            let old_tx = std::mem::replace(&mut conn_tx, next_tx);
                            let old_cancel = std::mem::replace(&mut gen_cancel, next_cancel);
                            let mut old_task = std::mem::replace(&mut gen_task, next_task);
                            drop(old_tx);
                            old_cancel.cancel();
                            retiring.retain(|handle| !handle.is_finished());
                            retiring.push(tokio::spawn(async move {
                                if tokio::time::timeout(Duration::from_secs(5), &mut old_task)
                                    .await
                                    .is_err()
                                {
                                    old_task.abort();
                                }
                            }));
                        }
                        Err(err) => {
                            tracing::warn!("热重建 collector agent TLS 实例失败，保留当前实例: {err:#}");
                        }
                    },
                    Err(err) => {
                        tracing::warn!("重载 collector agent TLS 配置失败，保留当前实例: {err:#}");
                    }
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _peer)) => {
                        let _ = conn_tx.send(Ok(stream)).await;
                    }
                    Err(err) => {
                        tracing::warn!("collector agent accept error: {err}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }
    }
    if tokio::time::timeout(Duration::from_secs(5), &mut gen_task)
        .await
        .is_err()
    {
        gen_task.abort();
    }
    for handle in retiring {
        let _ = handle.await;
    }
    tracing::info!("collector agent 服务退出");
    Ok(())
}

type ConnSender = mpsc::Sender<std::io::Result<tokio::net::TcpStream>>;

fn spawn_collector_agent_generation(
    runtime: Arc<Runtime>,
    policy: auth::DeviceAuthPolicy,
    grace_secs: i64,
    tls_config: ServerTlsConfig,
) -> anyhow::Result<(ConnSender, CancellationToken, tokio::task::JoinHandle<()>)> {
    let mut builder = Server::builder()
        .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .layer(auth::DeviceAuthLayer::new(policy))
        .tls_config(tls_config)?;

    let handler = AgentServiceHandler::new(runtime, grace_secs);
    let telemetry = proto::santaizi_telemetry_service_server::SantaiziTelemetryServiceServer::new(
        handler.clone(),
    );
    let collector =
        proto::santaizi_collector_service_server::SantaiziCollectorServiceServer::new(handler);
    let (_health_reporter, health_service) = tonic_health::server::health_reporter();

    let (conn_tx, conn_rx) = mpsc::channel::<std::io::Result<tokio::net::TcpStream>>(64);
    let gen_cancel = CancellationToken::new();
    let shutdown_token = gen_cancel.clone();
    let task = tokio::spawn(async move {
        if let Err(err) = builder
            .add_service(telemetry)
            .add_service(collector)
            .add_service(health_service)
            .serve_with_incoming_shutdown(ReceiverStream::new(conn_rx), async move {
                shutdown_token.cancelled().await;
            })
            .await
        {
            tracing::warn!("collector agent TLS generation exited with error: {err}");
        }
    });
    Ok((conn_tx, gen_cancel, task))
}

/// `collectorServerOptions` 的 TLS 部分：服务端证书 + 客户端 CA 池
/// （`grpc_tls.client_ca_file` ‖ 主端下发的 Agent CA），证书可选
/// （`VerifyClientCertIfGiven` 语义）。Agent CA 为启动时快照，见模块注释。
fn collector_agent_tls_config(runtime: &Runtime, conf: &Config) -> anyhow::Result<ServerTlsConfig> {
    let tls = &conf.grpc_tls;
    let cert_pem = std::fs::read(&tls.cert_file)
        .map_err(|err| anyhow::anyhow!("read grpc_tls.cert_file {}: {err}", tls.cert_file))?;
    let key_pem = std::fs::read(&tls.key_file)
        .map_err(|err| anyhow::anyhow!("read grpc_tls.key_file {}: {err}", tls.key_file))?;
    let mut ca_pem = Vec::new();
    if let Ok(agent_ca) = runtime.client_store().load_agent_ca()
        && !agent_ca.is_empty()
    {
        ca_pem.extend_from_slice(&agent_ca);
    }
    if !tls.client_ca_file.is_empty() {
        let extra = std::fs::read(&tls.client_ca_file).map_err(|err| {
            anyhow::anyhow!("read grpc_tls.client_ca_file {}: {err}", tls.client_ca_file)
        })?;
        ca_pem.extend_from_slice(&extra);
    }
    let mut config = ServerTlsConfig::new()
        .identity(Identity::from_pem(cert_pem, key_pem))
        .client_auth_optional(true);
    if !ca_pem.is_empty() {
        config = config.client_ca_root(Certificate::from_pem(ca_pem));
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CollectorModeConfig;
    use rcgen::CertifiedKey;
    use time::OffsetDateTime;
    use tonic::Code;
    use tonic::transport::{ClientTlsConfig, Endpoint};

    #[tokio::test]
    async fn hot_reloads_agent_ca_without_restarting_listener() {
        crate::util::tls::install_crypto_provider();
        let dir = std::env::temp_dir().join(format!(
            "santaizi-collector-ca-reload-{}-{}",
            std::process::id(),
            util_time::now_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        // 生成自签服务端证书（SAN: localhost）与主端 PKI Bundle（含 Agent CA）。
        let CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let server_cert_pem = cert.pem();
        let server_key_pem = signing_key.serialize_pem();
        let cert_path = dir.join("server.crt");
        let key_path = dir.join("server.key");
        std::fs::write(&cert_path, &server_cert_pem).unwrap();
        std::fs::write(&key_path, &server_key_pem).unwrap();

        let pki_bundle =
            crate::pki::load_or_create(&dir.join("primary-pki").to_string_lossy()).unwrap();
        let signer = crate::telemetry::credential::Signer::load_or_create(
            &dir.join("telemetry.key").to_string_lossy(),
        )
        .unwrap();
        let node_uuid = vec![0x55u8; 16];
        let agent_key = crate::pki::client::generate_key().unwrap();
        let agent_csr = crate::pki::client::create_csr(
            &agent_key,
            &crate::pki::uri::encode_agent_uri(&node_uuid),
        )
        .unwrap();
        let (agent_cert_pem, _, _) = crate::pki::sign_agent_csr(
            &pki_bundle.agent,
            &agent_csr,
            &node_uuid,
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        let agent_identity = Identity::from_pem(agent_cert_pem, agent_key.serialize_pem());

        let db_path = dir.join("collector.db");
        let db = crate::db::migrate::open_collector(db_path.to_str().unwrap()).unwrap();
        // 初始授权缓存不带 agent_ca_certificate_pem，模拟首次启动本地尚无 agent-ca.crt。
        let collector_uuid = "col-hot-reload".to_string();
        let auth_cfg = proto::CollectorAuthorizationConfig {
            config_version: 1,
            primary_public_key: signer.public_key(),
            key_id: signer.key_id(),
            kind: proto::CollectorKind::Observer as i32,
            assignments: vec![proto::NodeAssignment {
                node_uuid: node_uuid.clone(),
                observer_id: collector_uuid.clone(),
                valid_from_unix_nano: 0,
                valid_to_unix_nano: 0,
                generation: 1,
                config_version: 1,
            }],
            ..Default::default()
        };
        let now_nanos = util_time::now_nanos();
        db.write(|tx| store::save_authorization(tx, &collector_uuid, &auth_cfg, now_nanos))
            .unwrap();

        let cancel = CancellationToken::new();
        let runtime = Arc::new(
            Runtime::new(
                cancel.clone(),
                db,
                db_path.to_str().unwrap(),
                CollectorModeConfig {
                    primary_endpoint: "127.0.0.1:9".into(),
                    registration_token: "stc_test".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
        assert!(runtime.client_store().load_agent_ca().unwrap().is_empty());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut conf = Config {
            mode: "collector".into(),
            ..Default::default()
        };
        conf.grpc_tls.enabled = true;
        conf.grpc_tls.cert_file = cert_path.to_string_lossy().into_owned();
        conf.grpc_tls.key_file = key_path.to_string_lossy().into_owned();

        let srv_cancel = cancel.clone();
        let srv_runtime = Arc::clone(&runtime);
        let srv_task = tokio::spawn(async move {
            serve_collector_agent(srv_runtime, listener, srv_cancel, &conf)
                .await
                .unwrap();
        });

        let credential = signer
            .sign(&node_uuid, 1, now_nanos / 1_000_000_000, 3600)
            .unwrap();
        let try_ingest = |agent_id: Identity| {
            let server_cert_pem = server_cert_pem.clone();
            let collector_uuid = collector_uuid.clone();
            let node_uuid = node_uuid.clone();
            let credential = credential.clone();
            async move {
                let tls = ClientTlsConfig::new()
                    .domain_name("localhost")
                    .ca_certificate(Certificate::from_pem(server_cert_pem))
                    .identity(agent_id);
                let ch = Endpoint::from_shared(format!("https://localhost:{port}"))
                    .unwrap()
                    .tls_config(tls)
                    .unwrap()
                    .connect()
                    .await
                    .map_err(|e| Status::unavailable(e.to_string()))?;
                let mut cli =
                    proto::santaizi_telemetry_service_client::SantaiziTelemetryServiceClient::new(
                        ch,
                    );
                let (tx, rx) = mpsc::channel(4);
                tx.send(proto::TelemetryRequest {
                    body: Some(proto::telemetry_request::Body::Hello(
                        proto::TelemetryHello {
                            node_uuid,
                            endpoint_id: collector_uuid,
                            assignment_generation: 1,
                            credential: Some(credential),
                            protocol_version: "2".into(),
                            ..Default::default()
                        },
                    )),
                })
                .await
                .unwrap();
                tx.send(proto::TelemetryRequest {
                    body: Some(proto::telemetry_request::Body::Ping(
                        proto::TelemetryPing {},
                    )),
                })
                .await
                .unwrap();
                let mut stream = cli
                    .ingest(Request::new(ReceiverStream::new(rx)))
                    .await?
                    .into_inner();
                stream
                    .message()
                    .await?
                    .ok_or_else(|| Status::unavailable("closed"))
            }
        };

        // 1) 热重载前：无 agent-ca.crt，服务端未请求客户端证书，Ingest 被 DeviceAuthLayer 拒为 Unauthenticated。
        let before = try_ingest(agent_identity.clone()).await.unwrap_err();
        assert_eq!(before.code(), Code::Unauthenticated);

        // 2) Runtime 落盘并广播新的 agent-ca.crt，无需重启 serve_collector_agent 即热重建 TLS 实例。
        runtime.update_agent_ca(&pki_bundle.agent.cert_pem).unwrap();
        let mut succeeded = false;
        for _ in 0..25 {
            tokio::time::sleep(Duration::from_millis(40)).await;
            if let Ok(resp) = try_ingest(agent_identity.clone()).await
                && resp.pong.is_some()
            {
                succeeded = true;
                break;
            }
        }
        assert!(
            succeeded,
            "热重载 agent-ca.crt 后探针 mTLS Ingest 应自动恢复成功"
        );

        cancel.cancel();
        srv_task.await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
