//! 三太子（Santaizi）面板 Rust 实现。
//!
//! 上游版权：Nezha Monitoring（Apache-2.0），见 NOTICE。

use std::sync::Arc;

pub mod api;
pub mod availability;
pub mod collector;
pub mod config;
pub mod db;
pub mod geoip;
pub mod monitor;
pub mod notify;
pub mod pki;
pub mod proto;
pub mod repo;
pub mod rpc;
pub mod secret;
pub mod state;
pub mod telemetry;
pub mod traffic_eval;
pub mod util;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// 进程级错误别名。
pub type Result<T> = std::result::Result<T, anyhow::Error>;

/// 按 `mode` 分发主面板 / 从端。
///
/// 主端：打开（并迁移）主库 → 装载 PKI/签名器/秘密索引 → 起 gRPC 服务
/// 与 HTTP API（`/api/v2/*` + 内嵌前端）。
/// 从端：打开（并迁移）从端库 → 起 collector Runtime 的 Sync 循环。
pub async fn run(conf: config::Config, db_path: &str) -> Result<()> {
    tracing::info!(
        version = VERSION,
        mode = %conf.mode,
        http_port = conf.http_port,
        grpc_port = conf.grpc_port,
        db = db_path,
        "santaizi-dashboard 启动"
    );
    // ring 与 aws-lc-rs 同图，rustls 无法自动选择提供者；进程级装配一次。
    crate::util::tls::install_crypto_provider();
    // GeoIP：外部指定 → 内嵌 → 数据目录缓存 → 自愈下载（主端/从端都要查跳点与 HOST 国家码）。
    let data_dir = std::path::Path::new(db_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    geoip::bootstrap(conf.geoip_db_path().as_deref(), data_dir);
    match conf.mode.as_str() {
        "collector" => collector::serve(&conf, db_path).await,
        _ => {
            let db = db::migrate::open_primary(db_path)?;
            let state = state::AppState::primary(conf, db).await?;
            // gRPC 恒绑全接口：`grpchost` 只是对外通告地址（安装命令/凭据下发用），
            // 不拿来 bind——域名或公网 IP 在容器里没有对应网卡，会 EADDRNOTAVAIL。
            let grpc_listener =
                tokio::net::TcpListener::bind(("0.0.0.0", state.conf().grpc_port)).await?;
            let http_listener =
                tokio::net::TcpListener::bind(("0.0.0.0", state.conf().http_port)).await?;
            let shutdown = spawn_background_workers(&state, db_path);
            let http_cancel = tokio_util::sync::CancellationToken::new();
            let http_state = Arc::clone(&state);
            let http_cancel_worker = http_cancel.clone();
            let http_server = tokio::spawn(async move {
                api::serve_http(http_state, http_listener, http_cancel_worker).await
            });
            let served = rpc::serve_primary(state, grpc_listener).await;
            http_cancel.cancel();
            let _ = http_server.await;
            let _ = shutdown.send(true);
            served
        }
    }
}

/// 后台 worker：可用性共识引擎 + V2 离线检测 + STATE rollup + 数据库维护。返回停机信号
/// 发送端——gRPC 服务退出即通知它们收尾（发送端丢弃同样会令 receiver 唤醒）。
fn spawn_background_workers(
    state: &std::sync::Arc<state::AppState>,
    db_path: &str,
) -> tokio::sync::watch::Sender<bool> {
    let (shutdown, worker_shutdown) = tokio::sync::watch::channel(false);
    let engine = std::sync::Arc::new(availability::engine::Engine::new(
        state.db().clone(),
        state.conf().telemetry.availability_bucket_seconds,
        state.conf().telemetry.min_observers,
    ));
    tokio::spawn(engine.run(worker_shutdown.clone()));
    tokio::spawn(state.notifier().run_flusher(worker_shutdown.clone()));
    tokio::spawn(state.monitor().run(worker_shutdown.clone()));
    tokio::spawn(crate::traffic_eval::run(
        state.db().clone(),
        state.conf_handle(),
        state.notifier(),
        worker_shutdown.clone(),
    ));
    tokio::spawn(telemetry::alert_sentinel::run(
        state.db().clone(),
        state.notifier(),
        worker_shutdown.clone(),
    ));
    tokio::spawn(telemetry::telemetry_alerts::run(
        state.db().clone(),
        state.conf_handle(),
        state.notifier(),
        worker_shutdown.clone(),
    ));
    if state.conf().enable_offline_history {
        let detector = std::sync::Arc::new(
            telemetry::offline_detector::V2OfflineDetector::with_notifier(
                state.db().clone(),
                &state.conf(),
                Some(state.notifier()),
            ),
        );
        tokio::spawn(detector.run(worker_shutdown.clone()));
    } else {
        tracing::info!("离线历史未启用，跳过 V2 离线检测");
    }
    let mut rollup_worker: Option<std::sync::Arc<telemetry::rollup::RollupWorker>> = None;
    if state.conf().rollup.enabled {
        let worker = std::sync::Arc::new(telemetry::rollup::RollupWorker::new(
            state.db().clone(),
            telemetry::rollup::RetentionPolicy::from_config(&state.conf().retention),
            state.telemetry_store().shared_states(),
        ));
        rollup_worker = Some(std::sync::Arc::clone(&worker));
        tokio::spawn(worker.run(worker_shutdown.clone()));
    } else {
        tracing::info!("rollup 未启用，跳过 STATE 聚合");
    }
    if let Err(err) = state.db().write(|tx| {
        telemetry::housekeeping::init_server_runtimes(tx)?;
        if state.conf().enable_offline_history {
            let threshold = state.conf().offline_threshold_seconds.max(10);
            telemetry::housekeeping::reconcile_offline_histories(
                tx,
                threshold,
                crate::util::time::now_nanos(),
            )?;
        }
        Ok(())
    }) {
        tracing::warn!("运行态骨架与离线历史修复失败: {err:#}");
    }
    tokio::spawn(telemetry::housekeeping::run_offline_retention(
        state.db().clone(),
        state.conf_handle(),
        worker_shutdown.clone(),
    ));
    let maintainer = std::sync::Arc::new(telemetry::maintenance::DatabaseMaintainer::new(
        state.db().clone(),
        db_path,
        telemetry::rollup::RetentionPolicy::from_config(&state.conf().retention),
        rollup_worker,
    ));
    state.set_maintainer(Arc::clone(&maintainer));
    tokio::spawn(maintainer.run(worker_shutdown));
    shutdown
}
