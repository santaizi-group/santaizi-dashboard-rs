//! gRPC 服务端框架：keepalive、mTLS 与设备认证层组装。
//!
//! 对齐 Go `cmd/dashboard/rpc/rpc.go` 的 `grpcServerOptions` /
//! `grpcKeepaliveOptions`：keepalive 60s/20s、空闲也发 ping；TLS 开启时
//! 客户端证书「给了就验」（`VerifyClientCertIfGiven` 的 rustls 等价物
//! `allow_unauthenticated`），信任池 = 双 CA + 可选外部 CA 文件。

use std::sync::Arc;
use std::time::Duration;

use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

use crate::state::AppState;
use crate::{proto, proto::santaizi_enrollment_service_server};

pub mod auth;
pub mod collector;
pub mod collector_agent;
pub mod control;
pub mod enroll;
pub mod ingest;
pub mod support;

/// keepalive：对齐 Go `keepalive.ServerParameters{Time: 60s, Timeout: 20s}`。
/// tonic 0.14 服务端未暴露 `PermitWithoutStream`（空闲也发 ping）开关，
/// 且没有 `MinTime` 强制策略，两项均暂缺。
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// 主端 gRPC 服务：Enrollment + Collector + Replication + Health。
pub async fn serve_primary(
    state: Arc<AppState>,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<()> {
    // ring 与 aws-lc-rs 同图，rustls 无法自动选择提供者，见 `util::tls`。
    crate::util::tls::install_crypto_provider();
    let policy = auth::DeviceAuthPolicy {
        require_agent_mtls: state.conf().grpc_tls.require_agent_mtls,
        require_collector_mtls: state.conf().grpc_tls.require_collector_mtls,
        force_agent_ingest: false,
    };
    let mut builder = Server::builder()
        .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .layer(auth::DeviceAuthLayer::new(policy));
    if state.conf().grpc_tls.enabled {
        builder = builder.tls_config(server_tls_config(&state)?)?;
    }

    let enrollment = santaizi_enrollment_service_server::SantaiziEnrollmentServiceServer::new(
        enroll::EnrollmentHandler::new(Arc::clone(&state)),
    );
    let collector_handler = collector::CollectorHandler::new(Arc::clone(&state));
    let collector = proto::santaizi_collector_service_server::SantaiziCollectorServiceServer::new(
        collector_handler.clone(),
    );
    let replication =
        proto::santaizi_replication_service_server::SantaiziReplicationServiceServer::new(
            collector_handler,
        );
    let (_health_reporter, health_service) = tonic_health::server::health_reporter();
    let telemetry = proto::santaizi_telemetry_service_server::SantaiziTelemetryServiceServer::new(
        ingest::IngestHandler::new(Arc::clone(&state)),
    );
    let control = proto::santaizi_control_service_server::SantaiziControlServiceServer::new(
        control::ControlHandler::new(Arc::clone(&state)),
    );

    tracing::info!(
        listen = %listener.local_addr()?,
        tls = state.conf().grpc_tls.enabled,
        "gRPC 服务启动"
    );
    builder
        .add_service(enrollment)
        .add_service(collector)
        .add_service(replication)
        .add_service(telemetry)
        .add_service(control)
        .add_service(health_service)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown_signal())
        .await
        .map_err(|err| anyhow::anyhow!("serve gRPC: {err}"))?;
    tracing::info!("gRPC 服务退出");
    Ok(())
}

/// `GRPCServerTLS`：服务端证书 + 双 CA（及外部 CA 文件）信任池，
/// 客户端证书可选（`VerifyClientCertIfGiven` 语义）。
fn server_tls_config(state: &AppState) -> anyhow::Result<ServerTlsConfig> {
    let tls = &state.conf().grpc_tls;
    let cert_pem = std::fs::read(&tls.cert_file)
        .map_err(|err| anyhow::anyhow!("read grpc_tls.cert_file {}: {err}", tls.cert_file))?;
    let key_pem = std::fs::read(&tls.key_file)
        .map_err(|err| anyhow::anyhow!("read grpc_tls.key_file {}: {err}", tls.key_file))?;
    let mut ca_pem = Vec::new();
    if let Some(bundle) = state.pki() {
        ca_pem.extend_from_slice(&bundle.agent.cert_pem);
        ca_pem.extend_from_slice(&bundle.collector.cert_pem);
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

/// 关闭信号：Ctrl-C / SIGTERM（Go `signal.NotifyContext` 等价物）。
/// 退出日志由调用方按各自模式打印。
pub(crate) async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
