//! Collector 从端：对齐 Go `service/collector` + `cmd/dashboard/main.go` 的
//! `runCollector`。
//!
//! 本刀落地 Sync 循环基础（注册 / 证书 / hello / 运行态 / 授权缓存）与
//! Replication 复制循环（spool 出库 → 双工流 → ack 游标提交），spool
//! 摄入 store 层（agent 批次 → 本地事实表 + 出库入队 + 会话 ack），以及
//! collector 侧 agent 服务（Ingest / GetStatus，TLS 下 agent CA 认证），以及
//! 探测型从端的 ICMP/TCP 探活、nexttrace MTR 与路由任务。

pub mod netprobe;
pub mod nexttrace;
pub mod probe;
pub mod runtime;
pub mod store;

use crate::config::Config;
use crate::{Result, VERSION};

/// 以从端模式运行：打开从端库 → 建 Runtime → 起 Sync/Replication 循环 →
/// 起 agent 服务（Ingest / GetStatus）→ 等关闭信号。
///
/// 从端库路径取 `collector.database_path`；未配置时回退到 CLI `--db`
/// （Go 要求该配置项非空，这里放宽为回退，避免误配即拒绝启动）。
/// agent 服务监听 `grpc_host:grpc_port`（Go `ServeCollectorRPC` 同）。
pub async fn serve(conf: &Config, db_path: &str) -> Result<()> {
    let collector_path = if conf.collector.database_path.is_empty() {
        db_path
    } else {
        conf.collector.database_path.as_str()
    };
    let db = crate::db::migrate::open_collector(collector_path)?;
    let cancel = tokio_util::sync::CancellationToken::new();
    let runtime = std::sync::Arc::new(
        runtime::Runtime::new(cancel.clone(), db, collector_path, conf.collector.clone()).await?,
    );
    let mut worker = runtime.start();
    let listener = tokio::net::TcpListener::bind((conf.grpc_host.as_str(), conf.grpc_port))
        .await
        .map_err(|err| {
            anyhow::anyhow!(
                "bind collector agent {}:{}: {err}",
                conf.grpc_host,
                conf.grpc_port
            )
        })?;
    let server_cancel = cancel.clone();
    let server_conf = conf.clone();
    let server_runtime = std::sync::Arc::clone(&runtime);
    let mut server = tokio::spawn(async move {
        crate::rpc::collector_agent::serve_collector_agent(
            server_runtime,
            listener,
            server_cancel,
            &server_conf,
        )
        .await
    });
    tracing::info!(
        version = VERSION,
        primary = %conf.collector.primary_endpoint,
        db = collector_path,
        "collector 从端启动"
    );
    // 关闭信号、agent 服务退出、Sync/Replication worker 退出三者任一先达即收尾。
    let outcome = tokio::select! {
        _ = crate::rpc::shutdown_signal() => None,
        joined = &mut server => Some(joined
            .map_err(|err| anyhow::anyhow!("collector agent service failed: {err}"))
            .and_then(|inner| inner)),
        joined = &mut worker => Some(joined
            .map_err(|err| anyhow::anyhow!("collector sync/replication worker failed: {err}"))
            .and_then(|inner| inner)),
    };
    cancel.cancel();
    if let Some(outcome) = outcome {
        let _ = server.await;
        let _ = worker.await;
        return outcome;
    }
    tracing::info!("收到关闭信号，collector 从端退出");
    let server = server
        .await
        .map_err(|err| anyhow::anyhow!("collector agent service failed: {err}"))?;
    let worker = worker
        .await
        .map_err(|err| anyhow::anyhow!("collector sync/replication worker failed: {err}"))?;
    server?;
    worker?;
    Ok(())
}
