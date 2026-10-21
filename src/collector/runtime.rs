//! Collector 从端运行时：与主端的 Sync 双工循环 + Replication 复制循环。
//! 对齐 Go `service/collector/runtime.go` 的 `syncLoop` / `syncOnce` /
//! `registerWithPrimary` / `ensureCollectorCertificate` / `runtimeSnapshot` /
//! `replicationLoop` / `replicationOnce` / `flushOutbox` / `replicateBatch`。
//!
//! 注册、证书、运行态帧、授权缓存与复制循环之外，探测型从端每秒调度
//! ICMP/TCP，并把 nexttrace 的 MTR / 路由结果经 Sync 的 `ProbeSamples` 上报。
//!
//! 与 Go 的一处有意偏差：hello 携带注册令牌的条件。Go 仅在
//! `legacyPrimary || !hasCollectorCert()` 时带令牌，而主端同步流的拨号在
//! 注册完成前不携带客户端证书（TLS 握手先于 Register 发生），导致明文 +
//! 现代主端的组合下永远无法通过身份识别。这里把条件改为「本连接拿不出
//! 客户端证书时带令牌」，与主端 `identifyCollector` 的令牌路径一致。
//! replication 的令牌条件同理：Go 只看盘上证书，明文 + 已签发证书的组合
//! 下反而丢令牌；这里按「本连接能否出示证书身份」判定。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::Rng;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

use crate::collector::probe::ProbeScheduler;
use crate::collector::store;
use crate::config::CollectorModeConfig;
use crate::db::Db;
use crate::pki::ca::DEFAULT_RENEW_WINDOW;
use crate::pki::client::{self, ClientBundle};
use crate::pki::uri;
use crate::proto;
use crate::rpc::support::{blocking_read, blocking_write};
use crate::telemetry::probes;
use crate::util::time as util_time;
use crate::util::tls::SkipServerVerify;
use crate::{Result, VERSION};

/// 线协议版本（Go `collectorProtocolVersion`）。
const PROTOCOL_VERSION: &str = "2";
/// 主端不可达后的重连间隔（Go `syncLoop` 10s）。
const SYNC_RETRY: Duration = Duration::from_secs(10);
/// 运行态帧重发间隔（Go `syncOnce` 15s ticker）。
const RUNTIME_RESEND: Duration = Duration::from_secs(15);
/// 证书续期失败退避：1s 起翻倍，上限 10s（Go `ensureCollectorCertificate`）。
const RENEW_BACKOFF_MIN: Duration = Duration::from_secs(1);
const RENEW_BACKOFF_MAX: Duration = Duration::from_secs(10);
/// 拨号超时：与 Rust 探针拨主端一致；Go 用 grpc 默认值。
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
/// 单批复制记录数上限（Go `replicationBatchSize`）。
const REPLICATION_BATCH_SIZE: i64 = 512;
/// 出库为空时的空闲等待（Go `replicationIdleWait`）；摄入经 `wake` 通知提前唤醒。
const REPLICATION_IDLE_WAIT: Duration = Duration::from_secs(2);
/// 复制重连退避：1s → 2s → 5s → 10s（Go `replicationRetryMin/Max` + `nextReplicationRetry`）。
const REPLICATION_RETRY_MIN: Duration = Duration::from_secs(1);
const REPLICATION_RETRY_MAX: Duration = Duration::from_secs(10);
/// probe 从端不落 spool：复制循环空转等待（Go `replicationLoop` 15s）。
const PROBE_REPLICATION_IDLE: Duration = Duration::from_secs(15);
/// 证书被主端拒绝后的作废冷却：同一证书问题最多每 30s 重注册一次，
/// 避免中间件等场景下高频丢弃证书。
const IDENTITY_RESET_COOLDOWN: Duration = Duration::from_secs(30);
/// 主端在 TLS 层拒绝从端客户端证书时发出的 fatal alert
/// （rustls `AlertDescription` 的 Display 形态）。客户端侧校验失败不会
/// 以 "received fatal alert" 呈现，双重条件避免误判。
const CLIENT_CERT_REJECTED_ALERTS: [&str; 5] = [
    "UnknownCA",
    "BadCertificate",
    "CertificateUnknown",
    "CertificateExpired",
    "CertificateRevoked",
];

type CollectorClient =
    proto::santaizi_collector_service_client::SantaiziCollectorServiceClient<Channel>;
type ReplicationClient =
    proto::santaizi_replication_service_client::SantaiziReplicationServiceClient<Channel>;
type ReplicationSink = mpsc::Sender<proto::ReplicationBatch>;
type ReplicationStream = tonic::Streaming<proto::ReplicationAck>;

/// 进程内可变状态（短临界区，无 await；Go 用 `sync.RWMutex` 保护同名字段）。
#[derive(Default)]
struct SyncState {
    collector_uuid: String,
    kind: String,
    legacy_primary: bool,
    sync_sent_at: Option<Instant>,
    heartbeat_rtt_ms: f64,
    heartbeat_rtt_at: i64,
    renew_backoff: Duration,
    next_renew: Option<Instant>,
    /// 下一批批次号（Go `nextBatchSequence`，从 1 起）。
    next_batch_sequence: u64,
    /// 最近一次主端确认的 spool 游标（Go `lastReplicationAck`）。
    last_replication_ack: u64,
    replication_rtt_ms: f64,
    replication_rtt_at: i64,
    /// 最近一次作废被拒证书的时间（冷却控制重注册频率）。
    identity_reset_at: Option<Instant>,
}

pub struct Runtime {
    db: Db,
    db_path: String,
    config: CollectorModeConfig,
    client_store: client::ClientStore,
    cancel: CancellationToken,
    runtime_resend: Duration,
    /// 进程级复制会话 ID（Go `replicationSession`，每进程 16 随机字节）。
    replication_session: Vec<u8>,
    state: Mutex<SyncState>,
    /// 当前接入的探针流数（Go `connectedAgents`）。
    connected_agents: AtomicU64,
    /// 摄入入队后唤醒复制循环（Go `replicateWake` 非阻塞 channel）。
    wake: Notify,
    probe: Arc<ProbeScheduler>,
}

impl Runtime {
    /// 对齐 Go `NewRuntime`：校验主端端点与注册令牌，建客户端 PKI 目录，
    /// 并从授权缓存恢复 UUID / kind（Agent CA 同步落客户端存储）。
    pub async fn new(
        cancel: CancellationToken,
        db: Db,
        db_path: &str,
        config: CollectorModeConfig,
    ) -> Result<Self> {
        if config.primary_endpoint.is_empty() || config.registration_token.is_empty() {
            anyhow::bail!("collector primary endpoint and registration token are required");
        }
        let pki_dir = std::path::Path::new(db_path)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("pki");
        let client_store = client::ClientStore::new(&pki_dir.to_string_lossy())?;
        let mut state = SyncState {
            next_batch_sequence: 1,
            ..SyncState::default()
        };
        let cache = blocking_read(&db, store::authorization).await?;
        if let Some(cache) = cache {
            state.collector_uuid = cache.collector_uuid;
            state.kind = probes::normalize_collector_kind(&cache.kind);
            if !cache.agent_ca_certificate_pem.is_empty() {
                let _ = client_store.save_agent_ca(cache.agent_ca_certificate_pem.as_bytes());
            }
        }
        let mut replication_session = vec![0u8; 16];
        rand::rng().fill_bytes(&mut replication_session);
        Ok(Self {
            db,
            db_path: db_path.to_string(),
            config,
            client_store,
            cancel,
            runtime_resend: RUNTIME_RESEND,
            replication_session,
            state: Mutex::new(state),
            connected_agents: AtomicU64::new(0),
            wake: Notify::new(),
            probe: Arc::new(ProbeScheduler::new()),
        })
    }

    /// 测试专用：缩短运行态帧重发间隔。
    pub fn with_runtime_resend(mut self, interval: Duration) -> Self {
        self.runtime_resend = interval;
        self
    }

    pub fn collector_uuid(&self) -> String {
        self.lock().collector_uuid.clone()
    }

    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn db_path(&self) -> &str {
        &self.db_path
    }

    pub fn config(&self) -> &CollectorModeConfig {
        &self.config
    }

    pub fn client_store(&self) -> &client::ClientStore {
        &self.client_store
    }

    pub fn connected_agents(&self) -> u64 {
        self.connected_agents.load(Ordering::Relaxed)
    }

    pub fn connected_agent_connected(&self) {
        self.connected_agents.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connected_agent_disconnected(&self) {
        // 饱和减法：guard 重复 drop 也不会回绕。
        let _ = self
            .connected_agents
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_sub(1)
            });
    }

    pub fn is_probe(&self) -> bool {
        probes::is_probe_kind(&self.lock().kind)
    }

    /// `wakeReplication`：摄入入队后提前触发下一轮冲刷；无线等待时
    /// `Notify` 保留一次许可，不会丢唤醒（Go 无缓冲 channel 丢弃亦无害）。
    pub fn wake_replication(&self) {
        self.wake.notify_one();
    }

    /// 取消令牌：供嵌入方与测试控制 Sync 循环退出（Go `Runtime.Stop` 等价物）。
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// 启动 Sync、Replication 与探测循环。取消时三个循环干净退出并返回 `Ok`。
    pub fn start(self: &std::sync::Arc<Self>) -> tokio::task::JoinHandle<Result<()>> {
        let runtime = std::sync::Arc::clone(self);
        tokio::spawn(async move {
            let sync = {
                let runtime = std::sync::Arc::clone(&runtime);
                tokio::spawn(async move { runtime.sync_loop().await })
            };
            let replication = {
                let runtime = std::sync::Arc::clone(&runtime);
                tokio::spawn(async move { runtime.replication_loop().await })
            };
            let probe = {
                let runtime = std::sync::Arc::clone(&runtime);
                tokio::spawn(async move { runtime.probe_loop().await })
            };
            let (sync, replication, probe) = tokio::join!(sync, replication, probe);
            sync.map_err(|err| anyhow::anyhow!("collector sync worker failed: {err}"))??;
            replication
                .map_err(|err| anyhow::anyhow!("collector replication worker failed: {err}"))??;
            probe.map_err(|err| anyhow::anyhow!("collector probe worker failed: {err}"))?;
            Ok(())
        })
    }

    async fn probe_loop(self: &std::sync::Arc<Self>) {
        let is_probe = {
            let runtime = std::sync::Arc::clone(self);
            Arc::new(move || runtime.is_probe()) as Arc<dyn Fn() -> bool + Send + Sync>
        };
        self.probe
            .clone()
            .run(
                self.cancel.clone(),
                self.db.clone(),
                self.config.nexttrace_path.clone(),
                is_probe,
            )
            .await;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SyncState> {
        self.state.lock().unwrap_or_else(|err| err.into_inner())
    }

    async fn sync_loop(self: &std::sync::Arc<Self>) -> Result<()> {
        loop {
            if self.cancel.is_cancelled() {
                return Ok(());
            }
            match self.sync_once().await {
                Ok(()) => {}
                Err(err) => {
                    if self.cancel.is_cancelled() {
                        return Ok(());
                    }
                    if self.recover_rejected_certificate(&err) {
                        // 证书已作废：立即重拨重注册，不等 SYNC_RETRY。
                        continue;
                    }
                    tracing::warn!("collector sync disconnected: {err:#}");
                }
            }
            if !sleep_or_cancel(&self.cancel, SYNC_RETRY).await {
                return Ok(());
            }
        }
    }

    /// 主端在 TLS 层不承认从端客户端证书时的自愈：丢弃盘上证书，下一轮
    /// 不带证书拨号并经注册令牌重新签发。触发场景：主端 CA 轮换 / 主端
    /// 重装（数据目录重建）/ 从端数据目录复用自其它主端——此时证书还在
    /// 盘上，`dial` 每次都带上它，握手即被拒，注册请求永远到不了主端，
    /// 只能死循环重试（Go 同场景需手工删 PKI 目录）。
    /// 返回 `true` 表示已作废，调用方立即重试。
    fn recover_rejected_certificate(&self, err: &anyhow::Error) -> bool {
        let Some(alert) = primary_rejected_client_certificate(err) else {
            return false;
        };
        let has_identity = match self.client_store.identity() {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(err) => {
                tracing::warn!("collector client certificate unreadable: {err:#}");
                false
            }
        };
        if !has_identity {
            // 没有证书还被拒：不是证书问题，走常规重试与日志。
            return false;
        }
        if self.config.registration_token.trim().is_empty() {
            tracing::error!(
                "主端拒绝了从端证书（{alert}），且未配置注册令牌，无法自动重新注册；请删除 {} 后重启从端",
                self.client_store.dir().display()
            );
            return false;
        }
        {
            let mut state = self.lock();
            if let Some(at) = state.identity_reset_at
                && Instant::now().duration_since(at) < IDENTITY_RESET_COOLDOWN
            {
                return false;
            }
            state.identity_reset_at = Some(Instant::now());
        }
        if let Err(err) = self.client_store.discard_identity() {
            tracing::error!("作废被主端拒绝的从端证书失败: {err:#}");
            return false;
        }
        tracing::warn!("主端拒绝了从端证书（{alert}）；已丢弃旧证书，将用注册令牌重新注册");
        true
    }

    /// 一轮同步：拨号 → 证书就绪 → 注册 → hello → 收发循环。
    /// 流断开或取消时返回，由 `sync_loop` 决定重连。
    pub async fn sync_once(self: &std::sync::Arc<Self>) -> Result<()> {
        let channel = self.dial().await?;
        let mut client = CollectorClient::new(channel);
        self.ensure_collector_certificate(&mut client).await?;
        if self.collector_uuid().is_empty()
            || (!self.client_store.has_bundle()? && !self.lock().legacy_primary)
        {
            self.register_with_primary(&mut client).await?;
        }

        let (tx, rx) = mpsc::channel::<proto::CollectorSyncRequest>(8);
        let hello = self.hello().await?;
        tx.send(proto::CollectorSyncRequest {
            body: Some(proto::collector_sync_request::Body::Hello(hello)),
        })
        .await?;
        let mut stream = client
            .sync(Request::new(ReceiverStream::new(rx)))
            .await?
            .into_inner();
        self.mark_sync_sent();

        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + self.runtime_resend,
            self.runtime_resend,
        );
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                message = stream.message() => match message? {
                    Some(response) => {
                        self.note_sync_rtt();
                        self.handle_response(response).await?;
                    }
                    None => anyhow::bail!("collector sync stream closed by primary"),
                },
                _ = ticker.tick() => {
                    let sent = Instant::now();
                    let runtime = self.runtime_snapshot().await?;
                    tx.send(proto::CollectorSyncRequest {
                        body: Some(proto::collector_sync_request::Body::Runtime(runtime)),
                    })
                    .await?;
                    self.mark_sync_sent_at(sent);
                    self.flush_probe_samples(&tx).await?;
                }
                _ = self.probe.wake().notified() => {
                    self.flush_probe_samples(&tx).await?;
                }
            }
        }
    }

    /// `replicationLoop`：复制循环。probe 从端不复制；断开按退避重连。
    async fn replication_loop(self: &std::sync::Arc<Self>) -> Result<()> {
        let mut retry = REPLICATION_RETRY_MIN;
        loop {
            if self.cancel.is_cancelled() {
                return Ok(());
            }
            if self.is_probe() {
                if !sleep_or_cancel(&self.cancel, PROBE_REPLICATION_IDLE).await {
                    return Ok(());
                }
                continue;
            }
            let outcome = self.replication_once().await;
            match outcome {
                Ok(()) => retry = REPLICATION_RETRY_MIN,
                Err(err) => {
                    if !self.cancel.is_cancelled() {
                        tracing::warn!("collector replication disconnected: {err:#}");
                    }
                }
            }
            if !sleep_or_cancel(&self.cancel, retry).await {
                return Ok(());
            }
            retry = next_replication_retry(retry);
        }
    }

    /// `replicationOnce`：拨号 → 开 `Replicate` 双工流 → 反复冲刷出库。
    /// 流断开或取消时返回，由 `replication_loop` 决定重连。
    pub async fn replication_once(self: &std::sync::Arc<Self>) -> Result<()> {
        if self.collector_uuid().is_empty() {
            anyhow::bail!("collector is not registered");
        }
        let channel = self.dial().await?;
        let mut client = ReplicationClient::new(channel);
        let (tx, rx) = mpsc::channel::<proto::ReplicationBatch>(8);
        let mut request = Request::new(ReceiverStream::new(rx));
        if self.replication_with_token()? {
            // 主端按证书或 `collector_token` metadata 识别从端；拿不出证书
            // 身份时带令牌（语义同 hello，见模块注释）。
            request.metadata_mut().insert(
                "collector_token",
                self.config
                    .registration_token
                    .parse()
                    .map_err(|_| anyhow::anyhow!("invalid collector registration token"))?,
            );
        }
        let mut stream = client.replicate(request).await?.into_inner();
        loop {
            if self.cancel.is_cancelled() {
                return Ok(());
            }
            if self.flush_outbox(&tx, &mut stream).await? > 0 {
                continue;
            }
            // 空转：idle 到点或摄入唤醒即进入下一轮冲刷（Go `replicateWake`）。
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(REPLICATION_IDLE_WAIT) => {}
            }
        }
    }

    /// `flushOutbox`：持续冲刷直到出库取空；返回本轮批数。
    async fn flush_outbox(
        self: &std::sync::Arc<Self>,
        sink: &ReplicationSink,
        stream: &mut ReplicationStream,
    ) -> Result<usize> {
        let mut flushed = 0;
        while self.replicate_batch(sink, stream).await? {
            flushed += 1;
        }
        Ok(flushed)
    }

    /// `replicateBatch`：读一页出库 → 组帧 → 等 ack → 校验身份 → 提交游标。
    /// 返回 `false` 表示出库已空。
    async fn replicate_batch(
        self: &std::sync::Arc<Self>,
        sink: &ReplicationSink,
        stream: &mut ReplicationStream,
    ) -> Result<bool> {
        let (collector_uuid, session, batch_sequence) = {
            let state = self.lock();
            (
                state.collector_uuid.clone(),
                self.replication_session.clone(),
                state.next_batch_sequence,
            )
        };
        let outbox = blocking_read(&self.db, move |conn| {
            store::read_outbox(conn, REPLICATION_BATCH_SIZE)
        })
        .await?;
        if outbox.is_empty() {
            return Ok(false);
        }
        let batch = proto::ReplicationBatch {
            collector_uuid: collector_uuid.clone(),
            replication_session: session.clone(),
            batch_sequence,
            spool_through_id: outbox.through,
            events: outbox.events,
            observations: outbox.observations,
            gaps: outbox.gaps,
            health: outbox.health,
            data_loss: outbox.data_loss,
            runtime: Some(self.runtime_snapshot().await?),
        };
        let sent = Instant::now();
        sink.send(batch).await?;
        let ack = stream
            .message()
            .await?
            .ok_or_else(|| anyhow::anyhow!("replication stream closed by primary"))?;
        if !ack.error.is_empty() {
            anyhow::bail!("primary rejected replication batch: {}", ack.error);
        }
        if ack.collector_uuid != collector_uuid
            || ack.replication_session != session
            || ack.batch_sequence != batch_sequence
        {
            anyhow::bail!("replication ACK identity mismatch");
        }
        let through = ack.committed_spool_through_id;
        blocking_write(&self.db, move |tx| {
            store::commit_replication_ack(tx, through)
        })
        .await?;
        self.note_replication_rtt(sent);
        let mut state = self.lock();
        state.last_replication_ack = through;
        state.next_batch_sequence += 1;
        Ok(true)
    }

    /// `noteReplicationRTT`：一批 send→ack 的往返时延。
    fn note_replication_rtt(&self, sent: Instant) {
        let mut state = self.lock();
        state.replication_rtt_ms = duration_millis(sent.elapsed());
        state.replication_rtt_at = util_time::now_nanos();
    }

    /// `replicationDialOptions` 的令牌条件：本连接能否出示 collector 证书。
    fn replication_with_token(&self) -> Result<bool> {
        let legacy = self.lock().legacy_primary;
        let has_identity = self.config.primary_tls && self.client_store.identity()?.is_some();
        Ok(!has_identity || legacy)
    }

    /// 组装 hello：UUID、当前配置版本、运行态快照，必要时带注册令牌。
    async fn hello(&self) -> Result<proto::CollectorSyncHello> {
        let cache = blocking_read(&self.db, store::authorization)
            .await?
            .ok_or_else(|| anyhow::anyhow!("collector authorization cache is unavailable"))?;
        // 令牌语义见模块注释：本连接没有客户端证书时才需要令牌认证。
        let has_identity = self.config.primary_tls && self.client_store.identity()?.is_some();
        let registration_token = if self.lock().legacy_primary || !has_identity {
            self.config.registration_token.clone()
        } else {
            String::new()
        };
        Ok(proto::CollectorSyncHello {
            collector_uuid: self.collector_uuid(),
            registration_token,
            current_config_version: cache.config_version,
            runtime: Some(self.runtime_snapshot().await?),
        })
    }

    /// `runtimeSnapshot`：outbox 统计 + 授权缓存 + 进程内 RTT。
    pub async fn runtime_snapshot(&self) -> Result<proto::CollectorRuntime> {
        let (stats, cache) = blocking_read(&self.db, |conn| {
            Ok((store::runtime_stats(conn)?, store::authorization(conn)?))
        })
        .await?;
        let state = self.lock();
        let probe = probes::is_probe_kind(&state.kind);
        let mut runtime = proto::CollectorRuntime {
            collector_uuid: state.collector_uuid.clone(),
            sampled_at_unix_nano: util_time::now_nanos(),
            spool_size: stats.spool_bytes,
            pending_records: stats.pending,
            oldest_pending_unix_nano: stats.oldest_pending_nano,
            replication_cursor: state.last_replication_ack,
            connected_agents: self.connected_agents.load(Ordering::Relaxed),
            protocol_version: PROTOCOL_VERSION.to_string(),
            software_version: VERSION.to_string(),
            last_primary_seen_unix_nano: cache
                .as_ref()
                .map(|cache| cache.last_primary_seen_nano)
                .unwrap_or(0),
            heartbeat_rtt_ms: state.heartbeat_rtt_ms,
            heartbeat_rtt_sampled_at_unix_nano: state.heartbeat_rtt_at,
            replication_rtt_ms: state.replication_rtt_ms,
            replication_rtt_sampled_at_unix_nano: state.replication_rtt_at,
        };
        if probe {
            // probe 从端不落 spool：统计与复制字段归零（Go 同）。
            runtime.spool_size = 0;
            runtime.pending_records = 0;
            runtime.oldest_pending_unix_nano = 0;
            runtime.replication_cursor = 0;
            runtime.replication_rtt_ms = 0.0;
            runtime.replication_rtt_sampled_at_unix_nano = 0;
        }
        Ok(runtime)
    }

    async fn flush_probe_samples(
        &self,
        tx: &mpsc::Sender<proto::CollectorSyncRequest>,
    ) -> Result<()> {
        let Some(batch) = self.probe.take_samples() else {
            return Ok(());
        };
        tx.send(proto::CollectorSyncRequest {
            body: Some(proto::collector_sync_request::Body::ProbeSamples(batch)),
        })
        .await?;
        Ok(())
    }

    /// 处理主端下发帧：授权配置落库 + kind/Agent CA 同步；accepted 刷可见时间。
    async fn handle_response(&self, response: proto::CollectorSyncResponse) -> Result<()> {
        match response.body {
            Some(proto::collector_sync_response::Body::Config(config)) => {
                let collector_uuid = self.collector_uuid();
                let seen_at = util_time::now_nanos();
                let config_clone = config.clone();
                blocking_write(&self.db, move |tx| {
                    store::save_authorization(tx, &collector_uuid, &config_clone, seen_at)
                })
                .await?;
                let kind = if config.kind == proto::CollectorKind::Probe as i32 {
                    probes::COLLECTOR_KIND_PROBE
                } else {
                    probes::COLLECTOR_KIND_OBSERVER
                };
                self.lock().kind = kind.to_string();
                if !config.agent_ca_certificate_pem.is_empty() {
                    let _ = self
                        .client_store
                        .save_agent_ca(config.agent_ca_certificate_pem.as_bytes());
                }
            }
            Some(proto::collector_sync_response::Body::RouteJobs(batch)) => {
                let targets = blocking_read(&self.db, store::list_probe_targets).await?;
                self.probe.enqueue_jobs(&batch.jobs, &targets);
            }
            Some(proto::collector_sync_response::Body::Accepted(true)) => {
                let seen_at = util_time::now_nanos();
                blocking_write(&self.db, move |tx| store::touch_primary_seen(tx, seen_at)).await?;
            }
            Some(proto::collector_sync_response::Body::Accepted(false)) | None => {}
        }
        Ok(())
    }

    /// `registerWithPrimary`：新密钥 + CSR → 注册 → 授权缓存 + 证书包落盘。
    async fn register_with_primary(&self, client: &mut CollectorClient) -> Result<()> {
        let key = client::generate_key()?;
        let csr = client::create_csr(&key, &uri::encode_collector_uri("pending"))?;
        let response = client
            .register(Request::new(proto::RegisterCollectorRequest {
                registration_token: self.config.registration_token.clone(),
                protocol_version: PROTOCOL_VERSION.to_string(),
                csr_der: csr,
            }))
            .await?
            .into_inner();
        if response.collector_uuid.is_empty() {
            anyhow::bail!("collector registration returned an empty uuid");
        }
        let collector_uuid = response.collector_uuid.clone();
        let uuid_for_db = collector_uuid.clone();
        let config = proto::CollectorAuthorizationConfig {
            config_version: response.config_version,
            primary_public_key: response.primary_public_key.clone(),
            key_id: response.key_id.clone(),
            agent_ca_certificate_pem: response.agent_ca_certificate_pem.clone(),
            ..Default::default()
        };
        let seen_at = util_time::now_nanos();
        blocking_write(&self.db, move |tx| {
            store::save_authorization(tx, &uuid_for_db, &config, seen_at)
        })
        .await?;
        if !response.agent_ca_certificate_pem.is_empty() {
            let _ = self
                .client_store
                .save_agent_ca(response.agent_ca_certificate_pem.as_bytes());
        }
        if response.collector_certificate_pem.is_empty() {
            // 旧主端不签 collector 证书：全程走注册令牌（Go `legacyPrimary`）。
            self.lock().legacy_primary = true;
        } else {
            let bundle = ClientBundle::new(
                key.serialize_pem().into_bytes(),
                response.collector_certificate_pem.as_bytes().to_vec(),
                response.collector_ca_certificate_pem.as_bytes().to_vec(),
            )?;
            self.client_store.save(&bundle)?;
            self.lock().legacy_primary = false;
        }
        self.lock().collector_uuid = collector_uuid;
        Ok(())
    }

    /// `ensureCollectorCertificate`：临近过期时续期；失败按退避保留旧证书。
    async fn ensure_collector_certificate(&self, client: &mut CollectorClient) -> Result<()> {
        if self.lock().legacy_primary {
            return Ok(());
        }
        let bundle = match self.client_store.load()? {
            // 未注册：交由 registerWithPrimary。
            None => return Ok(()),
            Some(bundle) => bundle,
        };
        let now = ::time::OffsetDateTime::now_utc();
        if bundle.expired(now) && !self.config.registration_token.is_empty() {
            // 已过期且带令牌：重新注册（hasCollectorCert 为假）。
            return Ok(());
        }
        if !bundle.needs_renew(now, DEFAULT_RENEW_WINDOW) {
            return Ok(());
        }
        if let Some(next_renew) = self.lock().next_renew
            && Instant::now() < next_renew
        {
            return Ok(());
        }

        let key = client::generate_key()?;
        let collector_uuid = self.collector_uuid();
        let csr = client::create_csr(&key, &uri::encode_collector_uri(&collector_uuid))?;
        let response = match client
            .renew_collector(Request::new(proto::CollectorRenewRequest {
                collector_uuid,
                csr_der: csr,
            }))
            .await
        {
            Ok(response) => response.into_inner(),
            Err(err) => {
                // 旧证书仍有效：退避后下轮再试，不断流（Go 同）。
                if !bundle.expired(now) {
                    let mut state = self.lock();
                    let backoff = if state.renew_backoff.is_zero() {
                        RENEW_BACKOFF_MIN
                    } else {
                        (state.renew_backoff * 2).min(RENEW_BACKOFF_MAX)
                    };
                    state.renew_backoff = backoff;
                    state.next_renew = Some(Instant::now() + backoff);
                    drop(state);
                    tracing::warn!(
                        "collector certificate renew failed, keeping existing cert: {err}"
                    );
                    return Ok(());
                }
                return Err(Status::internal(err.to_string()).into());
            }
        };
        let bundle = ClientBundle::new(
            key.serialize_pem().into_bytes(),
            response.certificate_pem.as_bytes().to_vec(),
            response.ca_certificate_pem.as_bytes().to_vec(),
        )?;
        self.client_store.save(&bundle)?;
        if !response.agent_ca_certificate_pem.is_empty() {
            let _ = self
                .client_store
                .save_agent_ca(response.agent_ca_certificate_pem.as_bytes());
        }
        let mut state = self.lock();
        state.renew_backoff = Duration::ZERO;
        state.next_renew = None;
        Ok(())
    }

    /// 拨主端：每轮新连接（Go `grpc.NewClient` per attempt）。TLS 时带
    /// 客户端证书（若已注册）；`primary_insecure_tls` 跳过服务端校验。
    ///
    /// 信任池对齐 Go `transportCredentials`：webpki 系统根 + 注册下发的
    /// 主端 CA（`ca.crt`）叠加；自签主端证书两者都不覆盖，Go 同样要求
    /// 运营方开启 `primary_insecure_tls`。
    async fn dial(&self) -> Result<Channel> {
        let endpoint = self.config.primary_endpoint.trim().to_string();
        let scheme = if self.config.primary_tls {
            "https"
        } else {
            "http"
        };
        let mut builder = Endpoint::from_shared(format!("{scheme}://{endpoint}"))
            .map_err(|err| anyhow::anyhow!("collector primary endpoint {endpoint}: {err}"))?
            .connect_timeout(DIAL_TIMEOUT)
            .http2_keep_alive_interval(Duration::from_secs(60))
            .keep_alive_timeout(Duration::from_secs(20))
            .keep_alive_while_idle(true)
            .tcp_keepalive(Some(Duration::from_secs(60)));
        if self.config.primary_tls {
            let mut tls = ClientTlsConfig::new().domain_name(host_of(&endpoint));
            if let Some(identity) = self.client_store.identity()? {
                tls = tls.identity(identity);
            }
            builder = if self.config.primary_insecure_tls {
                tracing::warn!("primary_insecure_tls 已开启：跳过主端证书校验，仅用于测试");
                builder.tls_config_with_verifier(tls, std::sync::Arc::new(SkipServerVerify))?
            } else {
                let mut verified = tls.with_webpki_roots();
                if let Some(ca_pem) = self.client_store.ca_pem()? {
                    verified = verified.ca_certificate(Certificate::from_pem(ca_pem));
                }
                builder.tls_config(verified)?
            };
        }
        builder
            .connect()
            .await
            .map_err(|err| anyhow::anyhow!("dial primary {endpoint}: {err}"))
    }

    fn mark_sync_sent(&self) {
        self.mark_sync_sent_at(Instant::now());
    }

    fn mark_sync_sent_at(&self, at: Instant) {
        self.lock().sync_sent_at = Some(at);
    }

    /// `noteSyncRTT`：任一响应到达即结算上一帧至今的 RTT。
    fn note_sync_rtt(&self) {
        let mut state = self.lock();
        let Some(sent) = state.sync_sent_at.take() else {
            return;
        };
        state.heartbeat_rtt_ms = duration_millis(sent.elapsed());
        state.heartbeat_rtt_at = util_time::now_nanos();
    }
}

/// `waitContext`：可取消的睡眠；取消返回 `false`。
async fn sleep_or_cancel(cancel: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => false,
        _ = tokio::time::sleep(duration) => true,
    }
}

/// `nextReplicationRetry`：1s → 2s → 5s → 10s 到顶。
fn next_replication_retry(current: Duration) -> Duration {
    if current < Duration::from_secs(2) {
        Duration::from_secs(2)
    } else if current < Duration::from_secs(5) {
        Duration::from_secs(5)
    } else {
        REPLICATION_RETRY_MAX
    }
}

/// 端点 `host:port` → 主机名（去端口与 IPv6 方括号），用作 TLS SNI/校验域名。
fn host_of(endpoint: &str) -> String {
    let host = endpoint.rsplit_once(':').map_or(endpoint, |(host, _)| host);
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .to_string()
}

/// 错误链是否表示「主端拒绝了从端客户端证书」；命中返回 alert 名。
/// tonic 把 TLS 握手失败包成 `transport error: connection error:
/// received fatal alert: <AlertDescription>`，客户端自身校验失败不会
/// 以 "received fatal alert" 呈现，故双重条件防误判。
fn primary_rejected_client_certificate(err: &anyhow::Error) -> Option<&'static str> {
    let chain = format!("{err:#}");
    if !chain.contains("received fatal alert") {
        return None;
    }
    CLIENT_CERT_REJECTED_ALERTS
        .iter()
        .copied()
        .find(|alert| chain.contains(alert))
}

/// `durationMilliseconds`：负值夹到 0。
fn duration_millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_of_strips_port_and_brackets() {
        assert_eq!(host_of("panel.example.com:5556"), "panel.example.com");
        assert_eq!(host_of("127.0.0.1:18081"), "127.0.0.1");
        assert_eq!(host_of("[::1]:5556"), "::1");
        assert_eq!(host_of("panel.example.com"), "panel.example.com");
    }

    #[test]
    fn detects_primary_client_certificate_rejection() {
        // 形态对齐实战日志：tonic transport error 包 hyper 的 fatal alert。
        let err = anyhow::anyhow!(
            "dial primary main.example.in:18181: transport error: connection error: received fatal alert: UnknownCA"
        );
        assert_eq!(primary_rejected_client_certificate(&err), Some("UnknownCA"));
        let wrapped = Err::<(), _>(err)
            .map_err(|err| anyhow::anyhow!("collector sync worker failed: {err}"))
            .unwrap_err();
        assert_eq!(
            primary_rejected_client_certificate(&wrapped),
            Some("UnknownCA")
        );
    }

    #[test]
    fn ignores_non_certificate_alerts_and_local_errors() {
        for message in [
            // 客户端自身校验服务端证书失败：不呈 "received fatal alert"。
            "dial primary: transport error: connection error: invalid peer certificate: UnknownIssuer",
            // 与服务端证书无关的握手告警。
            "dial primary: transport error: connection error: received fatal alert: HandshakeFailure",
            "dial primary: transport error: connection error: received fatal alert: ProtocolVersion",
            // 纯传输/超时类错误。
            "dial primary: transport error: connection error: Connection reset by peer",
            "collector sync stream closed by primary",
        ] {
            let err = anyhow::anyhow!(message.to_string());
            assert_eq!(
                primary_rejected_client_certificate(&err),
                None,
                "should not treat as cert rejection: {message}"
            );
        }
    }

    #[tokio::test]
    async fn new_requires_endpoint_and_token() {
        let cancel = CancellationToken::new();
        let dir =
            std::env::temp_dir().join(format!("santaizi-collector-runtime-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db =
            crate::db::migrate::open_collector(dir.join("collector.db").to_str().unwrap()).unwrap();
        let config = CollectorModeConfig::default();
        assert!(
            Runtime::new(
                cancel.clone(),
                db.clone(),
                dir.join("collector.db").to_str().unwrap(),
                config
            )
            .await
            .is_err()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
