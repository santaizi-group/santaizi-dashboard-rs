//! 探测型从端的探活循环。
//!
//! 每秒看一次缓存目标，把工作投进**有界**队列，由固定数量的 worker 执行：
//! 连通性（ICMP/TCP）2 个 worker；周期 MTR、周期路由与手动路由共用**一个**
//! nexttrace 执行器——同时最多一个 nexttrace 子进程，与目标数无关。
//!
//! 队列满时周期工作不推进 due、不造假样本，下一轮重投（去重键已挡住重复
//! 投递）；手动路由任务回带 `job_id` 的失败样本，让主端当场结单。
//! 周期时间在**执行结束**后结算，排队期间不会补跑，也不会因缺二进制紧循环。

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use crate::collector::netprobe::{self, TcpResult};
use crate::collector::nexttrace::{self, TraceOpts};
use crate::collector::store::{self, CachedProbeTarget};
use crate::db::Db;
use crate::proto;
use crate::rpc::support::blocking_read;
use crate::telemetry::probes;

const SAMPLE_QUEUE_LIMIT: usize = 256;
/// 每类等待队列的容量：固定值，不随目标数放大。
const QUEUE_CAPACITY: usize = 32;
/// 连通性 worker 数：ICMP/TCP 是短任务，两个足够喂饱一台小内存主机。
const CONNECTIVITY_WORKERS: usize = 2;
/// 手动作业结果缓存：主端每个运行态帧都会重发 pending job（见主端
/// `send_pending_route_jobs`），重发直接重投缓存样本，不再起子进程。
const MANUAL_RESULT_LIMIT: usize = 256;
const MANUAL_RESULT_TTL: Duration = Duration::from_secs(15 * 60);
/// 运行态帧在 Sync 通道里的预留槽：样本不得把缓冲占满，否则心跳要等在
/// 一批探测样本后面。
const RUNTIME_SLOT_RESERVE: usize = 1;
const DROP_LOG_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_MTR_INTERVAL: Duration = Duration::from_secs(300);

/// 手动路由任务。`job_id > 0` 表示主端下发的按需任务，`0` 表示周期任务。
pub struct RouteWork {
    pub target: CachedProbeTarget,
    pub stamp: TargetStamp,
    pub protocol: String,
    pub port: u32,
    pub job_id: u64,
}

struct ConnWork {
    target: CachedProbeTarget,
    stamp: TargetStamp,
}

struct MtrWork {
    target: CachedProbeTarget,
    stamp: TargetStamp,
    /// 本轮连通性的 TCP 结果：MTR 的 TCP 追踪复用成功的端口。
    tcp: Vec<TcpResult>,
}

/// 目标代际：目标被删除、关闭协议或换地址时，旧的排队/在飞工作作废。
/// 用内容指纹而不是单调计数——配置改回原值就该重新探测。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TargetStamp {
    fingerprint: u64,
}

fn stamp_of(target: &CachedProbeTarget) -> TargetStamp {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    target.ipv4.hash(&mut hasher);
    target.ipv6.hash(&mut hasher);
    target.hostname.hash(&mut hasher);
    target.tcp_ports.hash(&mut hasher);
    target.enable_icmp.hash(&mut hasher);
    target.enable_tcp.hash(&mut hasher);
    target.enable_mtr.hash(&mut hasher);
    target.enable_ipv4.hash(&mut hasher);
    target.enable_ipv6.hash(&mut hasher);
    target.interval_sec.hash(&mut hasher);
    target.mtr_interval_sec.hash(&mut hasher);
    target.mtr_probes.hash(&mut hasher);
    target.route_interval_sec.hash(&mut hasher);
    TargetStamp {
        fingerprint: hasher.finish(),
    }
}

/// 目标仍在且代际一致。
fn is_current(
    server_id: u64,
    stamp: &TargetStamp,
    states: &Mutex<HashMap<u64, ProbeState>>,
) -> bool {
    let map = states.lock().unwrap_or_else(|err| err.into_inner());
    map.get(&server_id)
        .is_some_and(|state| state.stamp == *stamp)
}

/// 工作是否已过期：目标没了，或配置换过一代。
fn is_stale(server_id: u64, stamp: &TargetStamp, states: &Mutex<HashMap<u64, ProbeState>>) -> bool {
    !is_current(server_id, stamp, states)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum BusyKey {
    Connectivity(u64),
    Mtr(u64),
    Route(u64, String),
    /// 手动作业按 job_id 去重，不与周期路由共用键——否则同目标的周期路由
    /// 会把手动任务吞掉，主端只能等 10 分钟 TTL。
    Manual(u64),
}

/// 排队/运行中去重。键在**投递成功时**插入、在**执行结束时**移除，
/// 因此同一目标同一协议永远只有一份工作。
type BusySet = Mutex<HashSet<BusyKey>>;

/// RAII：正常返回、错误、panic、取消（future 被 drop）都会清键。
struct BusyGuard {
    busy: Arc<BusySet>,
    key: Option<BusyKey>,
}

impl BusyGuard {
    fn new(busy: Arc<BusySet>, key: BusyKey) -> Self {
        Self {
            busy,
            key: Some(key),
        }
    }
}

impl Drop for BusyGuard {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.busy
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .remove(&key);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Offer {
    Queued,
    /// 同键已有工作（排队或运行中）：本次触发被它覆盖。
    Busy,
    Full,
}

#[derive(Default)]
struct ManualResults {
    by_job: HashMap<u64, (Instant, proto::ProbeSample)>,
    order: VecDeque<u64>,
}

impl ManualResults {
    fn prune(&mut self, now: Instant) {
        while let Some(front) = self.order.front().copied() {
            let expired = self
                .by_job
                .get(&front)
                .is_none_or(|(at, _)| now.duration_since(*at) >= MANUAL_RESULT_TTL);
            if !expired {
                break;
            }
            self.order.pop_front();
            self.by_job.remove(&front);
        }
    }

    fn insert(&mut self, job_id: u64, sample: proto::ProbeSample, now: Instant) {
        self.prune(now);
        self.by_job.insert(job_id, (now, sample));
        if let Some(position) = self.order.iter().position(|id| *id == job_id) {
            self.order.remove(position);
        }
        self.order.push_back(job_id);
        while self.order.len() > MANUAL_RESULT_LIMIT {
            if let Some(evicted) = self.order.pop_front() {
                self.by_job.remove(&evicted);
            }
        }
    }

    fn get(&mut self, job_id: u64, now: Instant) -> Option<proto::ProbeSample> {
        self.prune(now);
        self.by_job.get(&job_id).map(|(_, sample)| sample.clone())
    }
}

/// 探测执行体：生产用真实网络与 nexttrace，测试用可闸门的假执行体。
///
/// 返回 boxed future 而不是 `async fn`：trait 要能当 `dyn` 用，且 future 必须
/// `Send`（worker 跑在 tokio 任务里）。
pub trait ProbeExec: Send + Sync + 'static {
    fn connectivity<'a>(
        &'a self,
        target: &'a CachedProbeTarget,
    ) -> Pin<Box<dyn Future<Output = ConnectivityOutcome> + Send + 'a>>;

    fn trace<'a>(
        &'a self,
        target: &'a CachedProbeTarget,
        protocol: &'a str,
        port: u32,
    ) -> Pin<Box<dyn Future<Output = TraceOutcome> + Send + 'a>>;
}

/// 连通性执行结果：样本 + 是否可达 + TCP 明细（MTR 复用端口）。
type ConnectivityOutcome = (proto::ProbeSample, bool, Vec<TcpResult>);
type TraceOutcome = Result<nexttrace::TraceResult, String>;

pub(crate) struct RealExec {
    path: String,
    cancel: CancellationToken,
}

impl RealExec {
    /// 生产执行体：真实网络探测 + nexttrace 子进程。
    pub fn shared(path: String, cancel: CancellationToken) -> Arc<dyn ProbeExec> {
        Arc::new(Self { path, cancel })
    }
}

impl ProbeExec for RealExec {
    fn connectivity<'a>(
        &'a self,
        target: &'a CachedProbeTarget,
    ) -> Pin<Box<dyn Future<Output = ConnectivityOutcome> + Send + 'a>> {
        Box::pin(run_connectivity(target))
    }

    fn trace<'a>(
        &'a self,
        target: &'a CachedProbeTarget,
        protocol: &'a str,
        port: u32,
    ) -> Pin<Box<dyn Future<Output = TraceOutcome> + Send + 'a>> {
        Box::pin(async move {
            let Some((host, ipv4)) = trace_host(target) else {
                return Err("无探测目标".to_string());
            };
            let bin = nexttrace::resolve_binary(&self.path)?;
            nexttrace::run_cancellable(
                &bin,
                &TraceOpts {
                    target: host,
                    tcp: protocol == "tcp",
                    port,
                    ipv4,
                    ipv6: !ipv4,
                },
                &self.cancel,
            )
            .await
        })
    }
}

#[derive(Default)]
struct ProbeState {
    stamp: TargetStamp,
    last_cycle: Option<Instant>,
    last_mtr: Option<Instant>,
    last_route_icmp: Option<Instant>,
    last_route_tcp: Option<Instant>,
    /// 最近一次连通性结果（连通 worker 写，探测循环读）。
    reachable: Option<bool>,
    /// 已据以判断翻转的可达状态。
    flip_basis: Option<bool>,
    /// 最近一次 TCP 结果：MTR 的 TCP 追踪复用成功的端口。
    last_tcp: Vec<TcpResult>,
}

pub struct ProbeScheduler {
    pending: Mutex<Vec<proto::ProbeSample>>,
    wake: Notify,
    conn_tx: Vec<mpsc::Sender<ConnWork>>,
    conn_rx: Mutex<Vec<mpsc::Receiver<ConnWork>>>,
    mtr_tx: mpsc::Sender<MtrWork>,
    mtr_rx: Mutex<Option<mpsc::Receiver<MtrWork>>>,
    route_tx: mpsc::Sender<RouteWork>,
    route_rx: Mutex<Option<mpsc::Receiver<RouteWork>>>,
    manual_tx: mpsc::Sender<RouteWork>,
    manual_rx: Mutex<Option<mpsc::Receiver<RouteWork>>>,
    busy: Arc<BusySet>,
    /// 连通性队列轮转游标：队列满时每轮换一个目标先投，低 server_id 不长期占位。
    conn_cursor: AtomicUsize,
    manual_results: Mutex<ManualResults>,
    drop_log_at: Mutex<Option<Instant>>,
}

impl Default for ProbeScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl ProbeScheduler {
    pub fn new() -> Self {
        let mut conn_tx = Vec::with_capacity(CONNECTIVITY_WORKERS);
        let mut conn_rx = Vec::with_capacity(CONNECTIVITY_WORKERS);
        for _ in 0..CONNECTIVITY_WORKERS {
            let (tx, rx) = mpsc::channel(QUEUE_CAPACITY / CONNECTIVITY_WORKERS);
            conn_tx.push(tx);
            conn_rx.push(rx);
        }
        let (mtr_tx, mtr_rx) = mpsc::channel(QUEUE_CAPACITY);
        let (route_tx, route_rx) = mpsc::channel(QUEUE_CAPACITY);
        let (manual_tx, manual_rx) = mpsc::channel(QUEUE_CAPACITY);
        Self {
            pending: Mutex::new(Vec::new()),
            wake: Notify::new(),
            conn_tx,
            conn_rx: Mutex::new(conn_rx),
            mtr_tx,
            mtr_rx: Mutex::new(Some(mtr_rx)),
            route_tx,
            route_rx: Mutex::new(Some(route_rx)),
            manual_tx,
            manual_rx: Mutex::new(Some(manual_rx)),
            busy: Arc::new(Mutex::new(HashSet::new())),
            conn_cursor: AtomicUsize::new(0),
            manual_results: Mutex::new(ManualResults::default()),
            drop_log_at: Mutex::new(None),
        }
    }

    pub fn wake(&self) -> &Notify {
        &self.wake
    }

    pub fn queue_samples(&self, mut samples: Vec<proto::ProbeSample>) {
        if samples.is_empty() {
            return;
        }
        let mut pending = self.pending.lock().unwrap_or_else(|err| err.into_inner());
        pending.append(&mut samples);
        let dropped = pending.len().saturating_sub(SAMPLE_QUEUE_LIMIT);
        if dropped > 0 {
            pending.drain(0..dropped);
        }
        drop(pending);
        if dropped > 0 {
            self.note_drops(dropped);
        }
        self.wake.notify_one();
    }

    /// 单次上报的样本数上限：取队列上限，主端 `telemetry.probe_batch_size`
    /// 默认 512，256 留一倍余量。再大也会被待发队列截断，且只会把主端事务拉长。
    pub const MAX_BATCH: usize = SAMPLE_QUEUE_LIMIT;

    pub fn take_samples(&self) -> Option<proto::ProbeSampleBatch> {
        let mut pending = self.pending.lock().unwrap_or_else(|err| err.into_inner());
        if pending.is_empty() {
            return None;
        }
        let take = pending.len().min(Self::MAX_BATCH);
        Some(proto::ProbeSampleBatch {
            samples: pending.drain(..take).collect(),
        })
    }

    /// 把没发出去的样本按原顺序放回队首。
    pub fn restore_samples(&self, mut samples: Vec<proto::ProbeSample>) {
        if samples.is_empty() {
            return;
        }
        let mut pending = self.pending.lock().unwrap_or_else(|err| err.into_inner());
        samples.append(&mut *pending);
        *pending = samples;
        let excess = pending.len().saturating_sub(SAMPLE_QUEUE_LIMIT);
        if excess > 0 {
            pending.drain(0..excess);
        }
    }

    pub fn has_samples(&self) -> bool {
        !self
            .pending
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .is_empty()
    }

    /// 取一批样本并**非阻塞**投递。Sync 通道要为运行态帧留槽：样本占满缓冲，
    /// 心跳就得等在一批探测样本后面（面板据此判离线）。投不出去就整批退回。
    pub fn flush_samples(&self, tx: &mpsc::Sender<proto::CollectorSyncRequest>) -> SampleFlush {
        let Some(batch) = self.take_samples() else {
            return SampleFlush::Sent;
        };
        if tx.capacity() <= RUNTIME_SLOT_RESERVE {
            self.restore_samples(batch.samples);
            return SampleFlush::Deferred;
        }
        match tx.try_send(proto::CollectorSyncRequest {
            body: Some(proto::collector_sync_request::Body::ProbeSamples(batch)),
        }) {
            Ok(()) => SampleFlush::Sent,
            Err(mpsc::error::TrySendError::Full(request)) => {
                if let Some(proto::collector_sync_request::Body::ProbeSamples(batch)) = request.body
                {
                    self.restore_samples(batch.samples);
                }
                SampleFlush::Deferred
            }
            Err(mpsc::error::TrySendError::Closed(request)) => {
                if let Some(proto::collector_sync_request::Body::ProbeSamples(batch)) = request.body
                {
                    self.restore_samples(batch.samples);
                }
                SampleFlush::Closed
            }
        }
    }

    fn note_drops(&self, dropped: usize) {
        let now = Instant::now();
        let mut last = self
            .drop_log_at
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if last.is_none_or(|at| now.duration_since(at) >= DROP_LOG_INTERVAL) {
            *last = Some(now);
            drop(last);
            tracing::warn!("探测样本队列已满（{SAMPLE_QUEUE_LIMIT}），丢弃 {dropped} 条最旧样本");
        }
    }

    fn offer_connectivity(&self, work: ConnWork) -> Offer {
        let key = BusyKey::Connectivity(work.target.server_id);
        let mut busy = self.busy.lock().unwrap_or_else(|err| err.into_inner());
        if !busy.insert(key.clone()) {
            return Offer::Busy;
        }
        let channels = self.conn_tx.len();
        let start = self.conn_cursor.fetch_add(1, Ordering::Relaxed) % channels;
        let mut pending = work;
        for step in 0..channels {
            let index = (start + step) % channels;
            match self.conn_tx[index].try_send(pending) {
                Ok(()) => return Offer::Queued,
                Err(mpsc::error::TrySendError::Full(returned)) => pending = returned,
                Err(mpsc::error::TrySendError::Closed(_)) => break,
            }
        }
        busy.remove(&key);
        Offer::Full
    }

    fn offer_mtr(&self, work: MtrWork) -> Offer {
        let key = BusyKey::Mtr(work.target.server_id);
        self.offer_one(&self.mtr_tx, work, key)
    }

    fn offer_route(&self, work: RouteWork) -> Offer {
        let key = BusyKey::Route(work.target.server_id, work.protocol.clone());
        self.offer_one(&self.route_tx, work, key)
    }

    fn offer_manual(&self, work: RouteWork) -> Offer {
        let key = BusyKey::Manual(work.job_id);
        self.offer_one(&self.manual_tx, work, key)
    }

    fn offer_one<T>(&self, tx: &mpsc::Sender<T>, work: T, key: BusyKey) -> Offer {
        let mut busy = self.busy.lock().unwrap_or_else(|err| err.into_inner());
        if !busy.insert(key.clone()) {
            return Offer::Busy;
        }
        match tx.try_send(work) {
            Ok(()) => Offer::Queued,
            Err(mpsc::error::TrySendError::Full(_)) => {
                busy.remove(&key);
                Offer::Full
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                busy.remove(&key);
                Offer::Full
            }
        }
    }

    fn remember_manual(&self, job_id: u64, sample: proto::ProbeSample) {
        self.manual_results
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .insert(job_id, sample, Instant::now());
    }

    fn cached_manual(&self, job_id: u64) -> Option<proto::ProbeSample> {
        self.manual_results
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .get(job_id, Instant::now())
    }

    pub fn enqueue_jobs(&self, jobs: &[proto::ProbeRouteJob], targets: &[CachedProbeTarget]) {
        let mut samples = Vec::new();
        for job in jobs {
            // job_id 为 0 的任务主端永远结不了单（finish_route_job 要求 id>0），
            // 跑它只会白占一个 nexttrace 进程。
            if job.job_id == 0 {
                continue;
            }
            let Some(target) = targets
                .iter()
                .find(|target| target.server_id == job.server_id)
            else {
                continue;
            };
            let protocol = job.protocol.trim().to_ascii_lowercase();
            if !accept_route(target, &protocol) {
                continue;
            }
            // 主端每个运行态帧都会重发 pending job：执行过的直接重投缓存样本，
            // 不再起一次子进程。
            if let Some(sample) = self.cached_manual(job.job_id) {
                samples.push(sample);
                continue;
            }
            let mut port = job.port;
            if protocol == "tcp" && port == 0 {
                port = first_port(&target.tcp_ports);
            }
            // 失败样本只在队满时才会用上：主端每个运行态帧都会重发 pending
            // job，不能在 Queued/Busy 路径上白分配一份。
            let fail_protocol = protocol.clone();
            let work = RouteWork {
                target: target.clone(),
                stamp: stamp_of(target),
                protocol,
                port,
                job_id: job.job_id,
            };
            match self.offer_manual(work) {
                // 已排队/运行中：主端下一帧还会再发，届时要么投缓存样本要么继续等。
                Offer::Queued | Offer::Busy => {}
                Offer::Full => {
                    // 队列满：立刻回失败样本让主端结单，而不是让任务挂到 TTL。
                    let rejected = failed_route_sample(
                        target.server_id,
                        &fail_protocol,
                        port,
                        job.job_id,
                        "从端探测队列已满，请稍后重试",
                    );
                    self.remember_manual(job.job_id, rejected.clone());
                    samples.push(rejected);
                }
            }
        }
        self.queue_samples(samples);
    }

    pub async fn run(
        self: Arc<Self>,
        cancel: CancellationToken,
        db: Db,
        exec: Arc<dyn ProbeExec>,
        is_probe: Arc<dyn Fn() -> bool + Send + Sync>,
    ) {
        let conn_rx: Vec<mpsc::Receiver<ConnWork>> = {
            let mut taken = self.conn_rx.lock().unwrap_or_else(|err| err.into_inner());
            std::mem::take(&mut *taken)
        };
        let mtr_rx = self
            .mtr_rx
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .take();
        let route_rx = self
            .route_rx
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .take();
        let manual_rx = self
            .manual_rx
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .take();
        let states: Arc<Mutex<HashMap<u64, ProbeState>>> = Arc::new(Mutex::new(HashMap::new()));

        for rx in conn_rx {
            let scheduler = Arc::clone(&self);
            let cancel = cancel.clone();
            let states = Arc::clone(&states);
            let exec = Arc::clone(&exec);
            tokio::spawn(async move {
                scheduler
                    .connectivity_worker(cancel, states, exec, rx)
                    .await
            });
        }
        if let (Some(mtr_rx), Some(route_rx), Some(manual_rx)) = (mtr_rx, route_rx, manual_rx) {
            let scheduler = Arc::clone(&self);
            let cancel_for_executor = cancel.clone();
            let states_for_executor = Arc::clone(&states);
            tokio::spawn(async move {
                scheduler
                    .trace_executor(
                        cancel_for_executor,
                        states_for_executor,
                        exec,
                        mtr_rx,
                        route_rx,
                        manual_rx,
                    )
                    .await
            });
        }

        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = ticker.tick() => {
                    if is_probe() {
                        self.tick(&db, &states).await;
                    }
                }
            }
        }
    }

    async fn connectivity_worker(
        self: Arc<Self>,
        cancel: CancellationToken,
        states: Arc<Mutex<HashMap<u64, ProbeState>>>,
        exec: Arc<dyn ProbeExec>,
        mut rx: mpsc::Receiver<ConnWork>,
    ) {
        while let Some(work) = rx.recv().await {
            if cancel.is_cancelled() {
                return;
            }
            let server_id = work.target.server_id;
            let _guard = BusyGuard::new(Arc::clone(&self.busy), BusyKey::Connectivity(server_id));
            if is_stale(server_id, &work.stamp, &states) {
                continue;
            }
            let (sample, reachable, tcp) = exec.connectivity(&work.target).await;
            let current = {
                let mut map = states.lock().unwrap_or_else(|err| err.into_inner());
                match map.get_mut(&server_id) {
                    // 执行期间目标换代：旧结果不写新代状态，也不产样本。
                    Some(state) if state.stamp == work.stamp => {
                        // 周期时间在执行结束后结算：排队再久也不会补跑。
                        state.last_cycle = Some(Instant::now());
                        state.reachable = Some(reachable);
                        state.last_tcp = tcp;
                        true
                    }
                    _ => false,
                }
            };
            if !current {
                continue;
            }
            self.queue_samples(vec![sample]);
            self.wake.notify_one();
        }
    }

    async fn trace_executor(
        self: Arc<Self>,
        cancel: CancellationToken,
        states: Arc<Mutex<HashMap<u64, ProbeState>>>,
        exec: Arc<dyn ProbeExec>,
        mut mtr_rx: mpsc::Receiver<MtrWork>,
        mut route_rx: mpsc::Receiver<RouteWork>,
        mut manual_rx: mpsc::Receiver<RouteWork>,
    ) {
        // MTR 与周期路由交替：两类同时积压时谁都不会被饿死。
        let mut prefer_mtr = true;
        loop {
            if cancel.is_cancelled() {
                return;
            }
            let task = next_trace_task(&mut manual_rx, &mut mtr_rx, &mut route_rx, &mut prefer_mtr);
            let Some(task) = task else {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    work = manual_rx.recv() => {
                        if let Some(work) = work {
                            self.run_trace(TraceTask::Trace(work, true), &states, &exec).await;
                        }
                    }
                    work = mtr_rx.recv() => {
                        if let Some(work) = work {
                            prefer_mtr = false;
                            self.run_trace(TraceTask::Mtr(work), &states, &exec).await;
                        }
                    }
                    work = route_rx.recv() => {
                        if let Some(work) = work {
                            prefer_mtr = true;
                            self.run_trace(TraceTask::Trace(work, false), &states, &exec).await;
                        }
                    }
                }
                continue;
            };
            self.run_trace(task, &states, &exec).await;
        }
    }

    async fn run_trace(
        self: &Arc<Self>,
        task: TraceTask,
        states: &Arc<Mutex<HashMap<u64, ProbeState>>>,
        exec: &Arc<dyn ProbeExec>,
    ) {
        match task {
            TraceTask::Mtr(work) => {
                let server_id = work.target.server_id;
                let _guard = BusyGuard::new(Arc::clone(&self.busy), BusyKey::Mtr(server_id));
                if is_stale(server_id, &work.stamp, states) {
                    return;
                }
                let sample = run_mtr_with(exec.as_ref(), &work.target, &work.tcp).await;
                // 结算与新鲜度在同一把锁里判断：执行期间换代就不结算新代计时、不产样本。
                if !settle_mtr(states, server_id, &work.stamp) {
                    return;
                }
                self.queue_samples(vec![sample]);
                self.wake.notify_one();
            }
            TraceTask::Trace(work, manual) => {
                let server_id = work.target.server_id;
                let key = if manual {
                    BusyKey::Manual(work.job_id)
                } else {
                    BusyKey::Route(server_id, work.protocol.clone())
                };
                let _guard = BusyGuard::new(Arc::clone(&self.busy), key);
                if is_stale(server_id, &work.stamp, states) {
                    if manual {
                        // 目标已变更：回失败样本让主端结单，别让它挂到 TTL。
                        self.fail_manual_job(&work, "探测目标已变更，任务作废");
                    }
                    return;
                }
                let sample = exec_route_with(exec.as_ref(), &work).await;
                if manual {
                    // 执行期间目标换代：结果已无意义，仍要回失败样本让主端结单。
                    if !is_current(server_id, &work.stamp, states) {
                        self.fail_manual_job(&work, "探测目标已变更，任务作废");
                        return;
                    }
                    self.remember_manual(work.job_id, sample.clone());
                } else if !settle_route(states, server_id, &work.protocol, &work.stamp) {
                    // 周期路由执行期间换代：旧结果作废，新代按自己的间隔重排。
                    return;
                }
                self.queue_samples(vec![sample]);
                self.wake.notify_one();
            }
        }
    }

    /// 手动作业作废：回带 `job_id` 的失败样本并缓存，主端下一帧重发时不再起进程。
    fn fail_manual_job(&self, work: &RouteWork, error: &str) {
        let sample = failed_route_sample(
            work.target.server_id,
            &work.protocol,
            work.port,
            work.job_id,
            error,
        );
        self.remember_manual(work.job_id, sample.clone());
        self.queue_samples(vec![sample]);
        self.wake.notify_one();
    }

    async fn tick(self: &Arc<Self>, db: &Db, states: &Arc<Mutex<HashMap<u64, ProbeState>>>) {
        let targets = match blocking_read(db, store::list_probe_targets).await {
            Ok(targets) => targets,
            Err(err) => {
                tracing::warn!("load probe targets: {err:#}");
                return;
            }
        };
        if targets.is_empty() {
            states.lock().unwrap_or_else(|err| err.into_inner()).clear();
            return;
        }
        let now = Instant::now();
        let live: HashSet<u64> = targets.iter().map(|target| target.server_id).collect();
        // 边判断边投递：队列满后不再为后面的目标克隆 work。每秒扫一遍目标，
        // 73 个目标同时到期时这里省掉的是几十次无用的 `CachedProbeTarget` 克隆。
        // states 锁内只做非阻塞 try_send，不会和 worker 的加锁顺序冲突。
        let mut connectivity_open = true;
        let mut routes_open = true;
        let mut mtr_open = true;
        {
            let mut map = states.lock().unwrap_or_else(|err| err.into_inner());
            map.retain(|id, _| live.contains(id));
            // 轮转起点：队列满时每轮换一个目标先投。
            let offset = self.conn_cursor.fetch_add(1, Ordering::Relaxed) % targets.len();
            for target in targets[offset..].iter().chain(targets[..offset].iter()) {
                let stamp = stamp_of(target);
                let state = map.entry(target.server_id).or_default();
                state.stamp = stamp;
                if !target.enable_mtr {
                    state.last_mtr = None;
                }
                let interval = if target.interval_sec == 0 {
                    DEFAULT_INTERVAL
                } else {
                    Duration::from_secs(target.interval_sec)
                };
                if connectivity_open && due(state.last_cycle, now, interval) {
                    let work = ConnWork {
                        target: target.clone(),
                        stamp,
                    };
                    if self.offer_connectivity(work) == Offer::Full {
                        connectivity_open = false;
                    }
                }
                let mtr_every = if target.mtr_interval_sec == 0 {
                    DEFAULT_MTR_INTERVAL
                } else {
                    Duration::from_secs(target.mtr_interval_sec)
                };
                // 可达状态翻转：首次观察不算翻转（没有可比的前值）。
                let flipped = match (state.reachable, state.flip_basis) {
                    (Some(current), Some(basis)) if current != basis => {
                        state.flip_basis = Some(current);
                        true
                    }
                    (Some(current), None) => {
                        state.flip_basis = Some(current);
                        false
                    }
                    _ => false,
                };
                // MTR 只在开关打开时触发：关掉后翻转不得再拉起追踪。
                if mtr_open && target.enable_mtr && (due(state.last_mtr, now, mtr_every) || flipped)
                {
                    let work = MtrWork {
                        target: target.clone(),
                        stamp,
                        tcp: state.last_tcp.clone(),
                    };
                    if self.offer_mtr(work) == Offer::Full {
                        mtr_open = false;
                    }
                }
                let route_every = route_interval(target);
                if routes_open && target.enable_icmp && due(state.last_route_icmp, now, route_every)
                {
                    let work = RouteWork {
                        target: target.clone(),
                        stamp,
                        protocol: "icmp".into(),
                        port: 0,
                        job_id: 0,
                    };
                    if self.offer_route(work) == Offer::Full {
                        routes_open = false;
                    }
                }
                if routes_open && target.enable_tcp && due(state.last_route_tcp, now, route_every) {
                    let port = first_port(&target.tcp_ports);
                    if port > 0 {
                        let work = RouteWork {
                            target: target.clone(),
                            stamp,
                            protocol: "tcp".into(),
                            port,
                            job_id: 0,
                        };
                        if self.offer_route(work) == Offer::Full {
                            routes_open = false;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleFlush {
    Sent,
    /// 通道要为运行态帧留槽，整批退回，下轮再发。
    Deferred,
    Closed,
}

enum TraceTask {
    Mtr(MtrWork),
    /// `bool` = 是否主端下发的手动作业。
    Trace(RouteWork, bool),
}

/// 取下一项追踪工作：手动作业优先；无手动作业时 MTR 与周期路由交替。
fn next_trace_task(
    manual: &mut mpsc::Receiver<RouteWork>,
    mtr: &mut mpsc::Receiver<MtrWork>,
    route: &mut mpsc::Receiver<RouteWork>,
    prefer_mtr: &mut bool,
) -> Option<TraceTask> {
    if let Ok(work) = manual.try_recv() {
        return Some(TraceTask::Trace(work, true));
    }
    for mtr_first in [*prefer_mtr, !*prefer_mtr] {
        if mtr_first {
            if let Ok(work) = mtr.try_recv() {
                *prefer_mtr = false;
                return Some(TraceTask::Mtr(work));
            }
        } else if let Ok(work) = route.try_recv() {
            *prefer_mtr = true;
            return Some(TraceTask::Trace(work, false));
        }
    }
    None
}

fn due(last: Option<Instant>, now: Instant, every: Duration) -> bool {
    last.is_none_or(|last| now.saturating_duration_since(last) >= every)
}

fn route_interval(target: &CachedProbeTarget) -> Duration {
    if target.route_interval_sec == 0 {
        Duration::from_secs(86_400)
    } else {
        Duration::from_secs(target.route_interval_sec)
    }
}

/// 结算 MTR 周期。返回 `false` 表示执行期间目标换代：不计入新代计时。
fn settle_mtr(
    states: &Mutex<HashMap<u64, ProbeState>>,
    server_id: u64,
    stamp: &TargetStamp,
) -> bool {
    let mut map = states.lock().unwrap_or_else(|err| err.into_inner());
    match map.get_mut(&server_id) {
        Some(state) if state.stamp == *stamp => {
            state.last_mtr = Some(Instant::now());
            true
        }
        _ => false,
    }
}

/// 结算周期路由；`false` 的含义同 [`settle_mtr`]。
fn settle_route(
    states: &Mutex<HashMap<u64, ProbeState>>,
    server_id: u64,
    protocol: &str,
    stamp: &TargetStamp,
) -> bool {
    let mut map = states.lock().unwrap_or_else(|err| err.into_inner());
    let Some(state) = map
        .get_mut(&server_id)
        .filter(|state| state.stamp == *stamp)
    else {
        return false;
    };
    let slot = if protocol == "tcp" {
        &mut state.last_route_tcp
    } else {
        &mut state.last_route_icmp
    };
    *slot = Some(Instant::now());
    true
}

fn accept_route(target: &CachedProbeTarget, protocol: &str) -> bool {
    match protocol {
        "icmp" => target.enable_icmp,
        "tcp" => target.enable_tcp,
        _ => false,
    }
}

fn first_port(ports: &str) -> u32 {
    probes::parse_ports(ports).first().copied().unwrap_or(0)
}

async fn run_connectivity(
    target: &CachedProbeTarget,
) -> (proto::ProbeSample, bool, Vec<TcpResult>) {
    let dests = netprobe::destinations(
        &target.ipv4,
        &target.ipv6,
        &target.hostname,
        target.enable_ipv4,
        target.enable_ipv6,
    );
    if dests.is_empty() {
        // 没有可探测地址不是成功：主端要能看到「无探测目标」，MTR/路由同一口径。
        return (
            proto::ProbeSample {
                server_id: target.server_id,
                sampled_at_unix_nano: crate::util::time::now_nanos(),
                last_error: "无探测目标".to_string(),
                ..Default::default()
            },
            false,
            Vec::new(),
        );
    }
    let sampled_at = crate::util::time::now_nanos();
    let mut sample = proto::ProbeSample {
        server_id: target.server_id,
        sampled_at_unix_nano: sampled_at,
        ..Default::default()
    };
    let mut icmp = None;
    if target.enable_icmp {
        let mut results = Vec::with_capacity(dests.len());
        for dest in &dests {
            results.push(netprobe::icmp_ping(&dest.host, dest.ipv4).await);
        }
        let merged = netprobe::merge_icmp(&results);
        icmp = Some(netprobe::IcmpResult {
            ok: merged.ok,
            rtt_ms: merged.rtt_ms,
            loss: merged.loss,
            sent: merged.sent,
            received: merged.received,
            error: merged.error.clone(),
        });
        sample.icmp = Some(proto::ProbeIcmpSample {
            ok: merged.ok,
            rtt_ms: merged.rtt_ms,
            loss: merged.loss,
            packets_sent: merged.sent,
            packets_received: merged.received,
            error: merged.error,
        });
    }
    let mut tcp_results = Vec::new();
    if target.enable_tcp {
        let ports = probes::parse_ports(&target.tcp_ports);
        for dest in &dests {
            for port in &ports {
                tcp_results.push(netprobe::tcp_probe(&dest.host, dest.ipv4, *port).await);
            }
        }
        let merged = netprobe::merge_tcp_by_port(std::mem::take(&mut tcp_results));
        tcp_results = merged.clone();
        sample.tcp = merged
            .into_iter()
            .map(|item| proto::ProbeTcpSample {
                port: item.port,
                ok: item.ok,
                rtt_ms: item.rtt_ms,
                error: item.error,
            })
            .collect();
    }
    let has_connectivity = target.enable_icmp || target.enable_tcp;
    let mut reachable = true;
    if has_connectivity {
        reachable =
            icmp.as_ref().is_some_and(|item| item.ok) || tcp_results.iter().any(|item| item.ok);
        sample.last_error = netprobe::display_error(icmp.as_ref(), &tcp_results);
    }
    (sample, reachable, tcp_results)
}

async fn run_mtr_with(
    exec: &dyn ProbeExec,
    target: &CachedProbeTarget,
    tcp: &[TcpResult],
) -> proto::ProbeSample {
    let sampled_at = crate::util::time::now_nanos();
    let mut sample = proto::ProbeSample {
        server_id: target.server_id,
        sampled_at_unix_nano: sampled_at,
        ..Default::default()
    };
    if trace_host(target).is_none() {
        sample.last_error = "无探测目标".to_string();
        return sample;
    }
    if target.enable_icmp {
        match exec.trace(target, "icmp", 0).await {
            Ok(result) => {
                sample.mtr = Some(mtr_proto(&result, "icmp", 0, sampled_at));
            }
            // `ProbeMTRTrace` 没有 error 字段（proto 未定义），失败原因落在
            // 样本的 last_error 上，主端与面板按同一口径展示。
            Err(err) if sample.last_error.is_empty() => sample.last_error = err,
            Err(_) => {}
        }
    }
    if target.enable_tcp {
        let port = tcp_trace_port(tcp, &target.tcp_ports);
        if port > 0 {
            match exec.trace(target, "tcp", port).await {
                Ok(result) => {
                    sample.mtr_tcp = Some(mtr_proto(&result, "tcp", port, sampled_at));
                }
                Err(err) if sample.last_error.is_empty() => sample.last_error = err,
                Err(_) => {}
            }
        }
    }
    sample
}

async fn exec_route_with(exec: &dyn ProbeExec, work: &RouteWork) -> proto::ProbeSample {
    let mut sample = proto::ProbeSample {
        server_id: work.target.server_id,
        sampled_at_unix_nano: crate::util::time::now_nanos(),
        ..Default::default()
    };
    let mut trace = proto::ProbeRouteTrace {
        sampled_at_unix_nano: sample.sampled_at_unix_nano,
        protocol: work.protocol.clone(),
        port: work.port,
        job_id: work.job_id,
        ..Default::default()
    };
    let Some((host, _)) = trace_host(&work.target) else {
        trace.error = "无探测目标".to_string();
        assign_route(&mut sample, &work.protocol, trace);
        return sample;
    };
    trace.destination = host;
    let result = match exec.trace(&work.target, &work.protocol, work.port).await {
        Ok(result) => result,
        Err(err) => {
            trace.error = err;
            assign_route(&mut sample, &work.protocol, trace);
            return sample;
        }
    };
    if !result.destination.is_empty() {
        trace.destination = result.destination;
    }
    for hop in &result.hops {
        trace.hops.push(route_hop(hop));
    }
    assign_route(&mut sample, &work.protocol, trace);
    sample
}

/// 跳点 → proto。nexttrace 自带 geo 时直接用；为空则回退本地 GeoIP
/// （主端原样落库，不做补算），这是 Go 版就有的口径。
fn route_hop(hop: &nexttrace::Hop) -> proto::ProbeRouteHop {
    let mut country = hop.country.clone();
    let mut asn = hop.asn.clone();
    if country.is_empty() {
        country = crate::geoip::lookup_code_from_addr(Some(&hop.address));
    }
    if asn.is_empty()
        && let Ok(ip) = hop.address.parse()
        && let Some(found) = crate::geoip::lookup_asn(ip)
    {
        asn = found;
    }
    proto::ProbeRouteHop {
        ttl: hop.ttl,
        address: hop.address.clone(),
        hostname: hop.hostname.clone(),
        rtt_ms: hop.rtt_ms,
        loss: hop.loss,
        sent: hop.sent,
        asn,
        country,
        province: hop.province.clone(),
        city: hop.city.clone(),
        owner: hop.owner.clone(),
    }
}

/// 队满/作废时回给主端的失败样本：字段必须齐，主端才能据此把任务标记为失败。
fn failed_route_sample(
    server_id: u64,
    protocol: &str,
    port: u32,
    job_id: u64,
    error: &str,
) -> proto::ProbeSample {
    let mut sample = proto::ProbeSample {
        server_id,
        sampled_at_unix_nano: crate::util::time::now_nanos(),
        ..Default::default()
    };
    let trace = proto::ProbeRouteTrace {
        sampled_at_unix_nano: sample.sampled_at_unix_nano,
        protocol: protocol.to_string(),
        port,
        job_id,
        error: error.to_string(),
        ..Default::default()
    };
    assign_route(&mut sample, protocol, trace);
    sample
}

fn mtr_proto(
    result: &nexttrace::TraceResult,
    protocol: &str,
    port: u32,
    sampled_at: i64,
) -> proto::ProbeMtrTrace {
    proto::ProbeMtrTrace {
        sampled_at_unix_nano: sampled_at,
        destination: result.destination.clone(),
        hops: result
            .hops
            .iter()
            .map(|hop| proto::ProbeMtrHop {
                ttl: hop.ttl,
                address: hop.address.clone(),
                loss: hop.loss,
                avg_ms: hop.rtt_ms,
                // 实际发包数来自 nexttrace 的 attempts；配置的 mtr_probes 是
                // 自研 MTR 的口径，不能拿来填这里（否则 sent 与 loss 对不上）。
                sent: hop.sent,
            })
            .collect(),
        protocol: protocol.to_string(),
        port,
    }
}

fn trace_host(target: &CachedProbeTarget) -> Option<(String, bool)> {
    let dests = netprobe::destinations(
        &target.ipv4,
        &target.ipv6,
        &target.hostname,
        target.enable_ipv4,
        target.enable_ipv6,
    );
    let dest = dests
        .iter()
        .find(|dest| dest.ipv4)
        .or_else(|| dests.first())?;
    Some((dest.host.clone(), dest.ipv4))
}

fn tcp_trace_port(results: &[TcpResult], ports: &str) -> u32 {
    if let Some(port) = results
        .iter()
        .find(|item| item.ok && item.port > 0)
        .map(|item| item.port)
    {
        return port;
    }
    if let Some(port) = results
        .iter()
        .find(|item| item.port > 0)
        .map(|item| item.port)
    {
        return port;
    }
    first_port(ports)
}

fn assign_route(sample: &mut proto::ProbeSample, protocol: &str, trace: proto::ProbeRouteTrace) {
    if protocol == "tcp" {
        sample.route_tcp = Some(trace);
    } else {
        sample.route_icmp = Some(trace);
    }
}

#[cfg(test)]
mod batch_cap_tests {
    use super::*;

    fn sample(server_id: u64) -> proto::ProbeSample {
        proto::ProbeSample {
            server_id,
            sampled_at_unix_nano: 1,
            ..Default::default()
        }
    }

    /// 单批上报必须设上限：探测目标多时一轮能积出任意多的样本，
    /// 不设上限等于让主端为一个完全由对端决定的巨型批次开长事务。
    /// 超额的留在队列里，下一批再走——不得丢。
    #[test]
    fn take_samples_caps_the_batch_and_keeps_the_rest() {
        let engine = ProbeScheduler::new();
        let total = ProbeScheduler::MAX_BATCH * 2 + 7;
        {
            let mut pending = engine.pending.lock().unwrap_or_else(|err| err.into_inner());
            for id in 0..total {
                pending.push(sample(id as u64));
            }
        }
        let first = engine.take_samples().expect("first batch");
        assert_eq!(first.samples.len(), ProbeScheduler::MAX_BATCH);
        let second = engine.take_samples().expect("second batch");
        assert_eq!(second.samples.len(), ProbeScheduler::MAX_BATCH);
        let third = engine.take_samples().expect("third batch");
        assert_eq!(third.samples.len(), 7);
        assert!(engine.take_samples().is_none(), "取空后不得再产空批");
    }

    /// 空队列不得产出空批次（主端会把空批当 InvalidArgument）。
    #[test]
    fn take_samples_on_empty_queue_returns_none() {
        let engine = ProbeScheduler::new();
        assert!(engine.take_samples().is_none());
    }

    /// 从端单批不得超过主端默认上限：主端默认 512，从端取队列上限 256。
    #[test]
    fn max_batch_stays_within_the_primary_default() {
        let conf = crate::config::Config::default();
        assert!(
            ProbeScheduler::MAX_BATCH <= conf.telemetry.probe_batch_size,
            "从端单批 {} 超过主端默认上限 {}",
            ProbeScheduler::MAX_BATCH,
            conf.telemetry.probe_batch_size
        );
    }

    /// 样本发送不得占满 Sync 通道：运行态帧要能随时插进去。
    #[tokio::test]
    async fn flush_samples_keeps_a_slot_for_the_runtime_frame() {
        let engine = ProbeScheduler::new();
        // 攒够 9 批：8 槽的通道必须只吃得下 7 批样本。
        let total = ProbeScheduler::MAX_BATCH * 9;
        {
            let mut pending = engine.pending.lock().unwrap_or_else(|err| err.into_inner());
            for id in 0..total {
                pending.push(sample(id as u64));
            }
        }
        let (tx, mut rx) = mpsc::channel::<proto::CollectorSyncRequest>(8);
        let mut sent = 0;
        while engine.has_samples() {
            match engine.flush_samples(&tx) {
                SampleFlush::Sent => sent += 1,
                SampleFlush::Deferred => break,
                SampleFlush::Closed => panic!("通道不该关闭"),
            }
        }
        assert_eq!(sent, 7, "8 槽里必须给运行态帧留 1 槽");
        assert!(tx.capacity() >= 1, "至少留一个空槽");
        assert_eq!(rx.recv().await.map(|_| ()), Some(()));
    }

    /// 投不出去的样本必须原样退回，顺序不变、不丢。
    #[tokio::test]
    async fn deferred_samples_are_restored_in_order() {
        let engine = ProbeScheduler::new();
        engine.queue_samples((0..5).map(sample).collect());
        // 容量 1：永远不足以在预留槽之外再发一批。
        let (tx, _rx) = mpsc::channel::<proto::CollectorSyncRequest>(1);
        assert_eq!(engine.flush_samples(&tx), SampleFlush::Deferred);
        let batch = engine.take_samples().expect("样本必须回来");
        let ids: Vec<u64> = batch.samples.iter().map(|item| item.server_id).collect();
        assert_eq!(ids, vec![0, 1, 2, 3, 4]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(server_id: u64) -> CachedProbeTarget {
        CachedProbeTarget {
            server_id,
            server_name: format!("edge-{server_id}"),
            ipv4: "203.0.113.1".into(),
            ipv6: String::new(),
            hostname: String::new(),
            tcp_ports: "443".into(),
            enable_icmp: true,
            enable_tcp: true,
            enable_mtr: true,
            enable_ipv4: true,
            enable_ipv6: false,
            interval_sec: 30,
            mtr_interval_sec: 300,
            mtr_probes: 3,
            route_interval_sec: 86_400,
        }
    }

    fn route_work(server_id: u64, protocol: &str, job_id: u64) -> RouteWork {
        RouteWork {
            target: target(server_id),
            stamp: stamp_of(&target(server_id)),
            protocol: protocol.to_string(),
            port: 443,
            job_id,
        }
    }

    /// MTR 开关关掉后，可达状态翻转不得再拉起追踪。
    #[test]
    fn flip_without_mtr_gate_does_not_trigger() {
        let state = ProbeState {
            reachable: Some(false),
            flip_basis: Some(true),
            ..ProbeState::default()
        };
        let mut t = target(1);
        t.enable_mtr = false;
        // 关 MTR 时状态里 last_mtr 会被清空（视为首次），但开关仍要拦住触发。
        let every = Duration::from_secs(300);
        let now = Instant::now();
        let flipped = state.reachable != state.flip_basis;
        assert!(flipped, "前置条件：可达状态确实翻转了");
        assert!(
            !(t.enable_mtr && (due(state.last_mtr, now, every) || flipped)),
            "关掉 MTR 后翻转不该触发追踪"
        );
    }

    /// 同键排队中重复触发只保留一份工作。
    #[test]
    fn duplicate_triggers_collapse_into_one_work() {
        let engine = ProbeScheduler::new();
        let work = ConnWork {
            target: target(1),
            stamp: stamp_of(&target(1)),
        };
        assert_eq!(engine.offer_connectivity(work), Offer::Queued);
        let again = ConnWork {
            target: target(1),
            stamp: stamp_of(&target(1)),
        };
        assert_eq!(engine.offer_connectivity(again), Offer::Busy);
        // 执行结束（guard drop）后同一目标又能进队。
        engine
            .busy
            .lock()
            .unwrap()
            .remove(&BusyKey::Connectivity(1));
        let third = ConnWork {
            target: target(1),
            stamp: stamp_of(&target(1)),
        };
        assert_eq!(engine.offer_connectivity(third), Offer::Queued);
    }

    /// 周期路由与手动作业即使目标/协议相同也不互吞。
    #[test]
    fn manual_and_periodic_routes_do_not_share_a_key() {
        let engine = ProbeScheduler::new();
        assert_eq!(engine.offer_route(route_work(1, "icmp", 0)), Offer::Queued);
        assert_eq!(
            engine.offer_manual(route_work(1, "icmp", 42)),
            Offer::Queued
        );
        // 同一个 job_id 重复下发只算一份。
        assert_eq!(engine.offer_manual(route_work(1, "icmp", 42)), Offer::Busy);
        // 另一个 job_id 不被吞。
        assert_eq!(
            engine.offer_manual(route_work(1, "icmp", 43)),
            Offer::Queued
        );
    }

    /// 手动队列满：立刻回带 job_id 的失败样本，主端据此结单。
    #[test]
    fn manual_queue_full_returns_a_failed_sample() {
        let engine = ProbeScheduler::new();
        for job_id in 1..=QUEUE_CAPACITY {
            assert_eq!(
                engine.offer_manual(route_work(1, "icmp", job_id as u64)),
                Offer::Queued
            );
        }
        let jobs = vec![proto::ProbeRouteJob {
            job_id: QUEUE_CAPACITY as u64 + 1,
            server_id: 1,
            protocol: "icmp".into(),
            port: 0,
        }];
        engine.enqueue_jobs(&jobs, &[target(1)]);
        let batch = engine.take_samples().expect("失败样本");
        let trace = batch.samples[0].route_icmp.as_ref().expect("route_icmp");
        assert_eq!(trace.job_id, QUEUE_CAPACITY as u64 + 1);
        assert!(!trace.error.is_empty(), "必须带错误，主端才标记 failed");
        // 重复下发同一 job：重投缓存样本，不再占队列。
        engine.enqueue_jobs(&jobs, &[target(1)]);
        let again = engine.take_samples().expect("缓存样本");
        assert_eq!(again.samples.len(), 1);
        assert_eq!(
            again.samples[0].route_icmp.as_ref().map(|t| t.job_id),
            Some(QUEUE_CAPACITY as u64 + 1)
        );
    }

    /// 执行过的手动作业，主端重发时只重投缓存样本。
    #[test]
    fn finished_manual_job_is_replayed_from_cache() {
        let engine = ProbeScheduler::new();
        engine.remember_manual(99, failed_route_sample(7, "tcp", 443, 99, "done"));
        let jobs = vec![proto::ProbeRouteJob {
            job_id: 99,
            server_id: 7,
            protocol: "tcp".into(),
            port: 443,
        }];
        engine.enqueue_jobs(&jobs, &[target(7)]);
        let batch = engine.take_samples().expect("缓存样本");
        let trace = batch.samples[0].route_tcp.as_ref().expect("route_tcp");
        assert_eq!(trace.job_id, 99);
    }

    /// 目标代际：配置换代后，旧排队工作作废。
    #[test]
    fn stale_work_is_dropped_after_the_target_changes() {
        let states: Mutex<HashMap<u64, ProbeState>> = Mutex::new(HashMap::new());
        let original = target(3);
        let stamp = stamp_of(&original);
        states.lock().unwrap().insert(
            3,
            ProbeState {
                stamp,
                ..ProbeState::default()
            },
        );
        assert!(!is_stale(3, &stamp, &states));
        let mut changed = original.clone();
        changed.ipv4 = "198.51.100.9".into();
        states.lock().unwrap().get_mut(&3).unwrap().stamp = stamp_of(&changed);
        assert!(is_stale(3, &stamp, &states));
        // 目标被删掉也算过期。
        states.lock().unwrap().clear();
        assert!(is_stale(3, &stamp, &states));
    }

    /// MTR 与周期路由交替：两类同时积压时都不被饿死。
    #[test]
    fn mtr_and_periodic_routes_alternate() {
        let (manual_tx, mut manual_rx) = mpsc::channel::<RouteWork>(4);
        let (mtr_tx, mut mtr_rx) = mpsc::channel::<MtrWork>(4);
        let (route_tx, mut route_rx) = mpsc::channel::<RouteWork>(4);
        let _ = manual_tx;
        for id in 1..=2u64 {
            mtr_tx
                .try_send(MtrWork {
                    target: target(id),
                    stamp: stamp_of(&target(id)),
                    tcp: Vec::new(),
                })
                .unwrap();
        }
        for id in 1..=2u64 {
            route_tx.try_send(route_work(id, "icmp", 0)).unwrap();
        }
        let mut prefer_mtr = true;
        let mut order = Vec::new();
        for _ in 0..4 {
            match next_trace_task(&mut manual_rx, &mut mtr_rx, &mut route_rx, &mut prefer_mtr) {
                Some(TraceTask::Mtr(work)) => order.push(format!("mtr:{}", work.target.server_id)),
                Some(TraceTask::Trace(work, _)) => {
                    order.push(format!("route:{}", work.target.server_id))
                }
                None => order.push("none".to_string()),
            }
        }
        assert_eq!(order, vec!["mtr:1", "route:1", "mtr:2", "route:2"]);
    }

    /// 手动作业优先于周期追踪。
    #[test]
    fn manual_jobs_go_first() {
        let (manual_tx, mut manual_rx) = mpsc::channel::<RouteWork>(4);
        let (_mtr_tx, mut mtr_rx) = mpsc::channel::<MtrWork>(4);
        let (_route_tx, mut route_rx) = mpsc::channel::<RouteWork>(4);
        manual_tx.try_send(route_work(5, "icmp", 1)).unwrap();
        let mut prefer_mtr = true;
        let task = next_trace_task(&mut manual_rx, &mut mtr_rx, &mut route_rx, &mut prefer_mtr)
            .expect("manual work");
        match task {
            TraceTask::Trace(work, manual) => {
                assert!(manual);
                assert_eq!(work.job_id, 1);
            }
            TraceTask::Mtr(_) => panic!("手动任务必须优先"),
        }
    }

    /// 周期时间在执行结束后结算：排队再久也不会立刻补跑。
    #[test]
    fn periodic_timers_settle_at_execution_end() {
        let states: Mutex<HashMap<u64, ProbeState>> = Mutex::new(HashMap::new());
        let stamp = TargetStamp { fingerprint: 7 };
        states.lock().unwrap().insert(
            9,
            ProbeState {
                stamp,
                ..ProbeState::default()
            },
        );
        let before = Instant::now();
        assert!(settle_mtr(&states, 9, &stamp));
        assert!(settle_route(&states, 9, "tcp", &stamp));
        let state = states.lock().unwrap();
        let state = state.get(&9).unwrap();
        assert!(state.last_mtr.is_some_and(|at| at >= before));
        assert!(state.last_route_tcp.is_some_and(|at| at >= before));
        assert!(state.last_route_icmp.is_none(), "只结算对应协议");
    }

    /// 执行期间换代：旧结果不得结算新代的计时。
    #[test]
    fn stale_execution_does_not_settle_the_new_generation() {
        let states: Mutex<HashMap<u64, ProbeState>> = Mutex::new(HashMap::new());
        let old = TargetStamp { fingerprint: 1 };
        let new = TargetStamp { fingerprint: 2 };
        states.lock().unwrap().insert(
            9,
            ProbeState {
                stamp: new,
                ..ProbeState::default()
            },
        );
        assert!(!settle_mtr(&states, 9, &old));
        assert!(!settle_route(&states, 9, "icmp", &old));
        let state = states.lock().unwrap();
        let state = state.get(&9).unwrap();
        assert!(state.last_mtr.is_none());
        assert!(state.last_route_icmp.is_none());
        assert_eq!(state.stamp, new);
    }

    /// 没有可探测地址：不得静默当成功，样本要带「无探测目标」且不可达。
    #[tokio::test]
    async fn target_without_destinations_is_not_success() {
        let mut t = target(11);
        t.ipv4.clear();
        t.ipv6.clear();
        t.hostname.clear();
        let (sample, reachable, tcp) = run_connectivity(&t).await;
        assert!(!reachable);
        assert_eq!(sample.last_error, "无探测目标");
        assert!(sample.icmp.is_none());
        assert!(sample.tcp.is_empty());
        assert!(tcp.is_empty());
    }

    /// MTR 每跳 `sent` 取 nexttrace 实际 attempts，不拿配置的 mtr_probes 冒充。
    #[test]
    fn mtr_hop_sent_uses_actual_attempts() {
        let result = nexttrace::TraceResult {
            destination: "203.0.113.1".into(),
            hops: vec![nexttrace::Hop {
                ttl: 1,
                address: "203.0.113.9".into(),
                hostname: String::new(),
                rtt_ms: 1.5,
                loss: 0.5,
                sent: 3,
                asn: String::new(),
                country: String::new(),
                province: String::new(),
                city: String::new(),
                owner: String::new(),
            }],
        };
        let trace = mtr_proto(&result, "icmp", 0, 1);
        assert_eq!(trace.hops.len(), 1);
        assert_eq!(trace.hops[0].sent, 3, "sent 必须与 loss 的实际分母一致");
    }

    /// 路由跳点缺 geo 时回退本地 GeoIP（Go 版口径），已有值不覆盖。
    #[test]
    fn route_hop_falls_back_to_local_geoip() {
        let _guard = crate::geoip::test_guard();
        crate::geoip::reset_for_test();
        crate::geoip::install_country_for_test("US", [8, 8, 8, 0], 24);
        let base = nexttrace::Hop {
            ttl: 2,
            address: "8.8.8.8".into(),
            hostname: String::new(),
            rtt_ms: 2.0,
            loss: 0.0,
            sent: 3,
            asn: "AS15169".into(),
            country: String::new(),
            province: String::new(),
            city: String::new(),
            owner: String::new(),
        };
        let hop = route_hop(&base);
        assert_eq!(hop.country, "us", "空国家码按本地库补");
        assert_eq!(hop.asn, "AS15169", "已有 ASN 不覆盖");
        let explicit = nexttrace::Hop {
            country: "CN".into(),
            ..base.clone()
        };
        assert_eq!(route_hop(&explicit).country, "CN", "已有国家码不覆盖");
        crate::geoip::reset_for_test();
    }

    /// 缺 nexttrace 时样本带明确错误，不当成成功。
    #[tokio::test]
    async fn missing_nexttrace_is_reported_on_the_route_sample() {
        struct MissingNexttrace;
        impl ProbeExec for MissingNexttrace {
            fn connectivity<'a>(
                &'a self,
                _target: &'a CachedProbeTarget,
            ) -> Pin<Box<dyn Future<Output = (proto::ProbeSample, bool, Vec<TcpResult>)> + Send + 'a>>
            {
                Box::pin(async { (proto::ProbeSample::default(), true, Vec::new()) })
            }

            fn trace<'a>(
                &'a self,
                _target: &'a CachedProbeTarget,
                _protocol: &str,
                _port: u32,
            ) -> Pin<Box<dyn Future<Output = Result<nexttrace::TraceResult, String>> + Send + 'a>>
            {
                Box::pin(async { Err(nexttrace::NOT_FOUND.to_string()) })
            }
        }
        let work = route_work(7, "icmp", 9);
        let sample = exec_route_with(&MissingNexttrace, &work).await;
        let trace = sample.route_icmp.expect("route");
        assert!(
            trace.error.contains(nexttrace::NOT_FOUND),
            "{}",
            trace.error
        );
        assert!(trace.hops.is_empty());
        assert_eq!(trace.job_id, 9);
    }
}

/// 风暴回归：复盘里 73 个目标同时到期打爆 1.9G 宿主机的场景。
/// 假执行体用信号量当闸门——把在飞任务全部按住，才能量到「同时在飞」的上限。
#[cfg(test)]
mod storm_tests {
    use super::*;

    const STORM_TARGETS: u64 = 73;

    #[derive(Default)]
    struct Counters {
        conn_live: AtomicUsize,
        conn_peak: AtomicUsize,
        conn_calls: AtomicUsize,
        trace_live: AtomicUsize,
        trace_peak: AtomicUsize,
        trace_calls: AtomicUsize,
    }

    struct GatedExec {
        counters: Arc<Counters>,
        conn_gate: Arc<tokio::sync::Semaphore>,
        trace_gate: Arc<tokio::sync::Semaphore>,
    }

    impl ProbeExec for GatedExec {
        fn connectivity<'a>(
            &'a self,
            target: &'a CachedProbeTarget,
        ) -> Pin<Box<dyn Future<Output = (proto::ProbeSample, bool, Vec<TcpResult>)> + Send + 'a>>
        {
            let counters = Arc::clone(&self.counters);
            let gate = Arc::clone(&self.conn_gate);
            let server_id = target.server_id;
            Box::pin(async move {
                let live = counters.conn_live.fetch_add(1, Ordering::SeqCst) + 1;
                counters.conn_peak.fetch_max(live, Ordering::SeqCst);
                counters.conn_calls.fetch_add(1, Ordering::SeqCst);
                let _permit = gate.acquire().await;
                counters.conn_live.fetch_sub(1, Ordering::SeqCst);
                (
                    proto::ProbeSample {
                        server_id,
                        sampled_at_unix_nano: 1,
                        ..Default::default()
                    },
                    true,
                    Vec::new(),
                )
            })
        }

        fn trace<'a>(
            &'a self,
            target: &'a CachedProbeTarget,
            protocol: &'a str,
            port: u32,
        ) -> Pin<Box<dyn Future<Output = Result<nexttrace::TraceResult, String>> + Send + 'a>>
        {
            let counters = Arc::clone(&self.counters);
            let gate = Arc::clone(&self.trace_gate);
            let destination = target.ipv4.clone();
            Box::pin(async move {
                let live = counters.trace_live.fetch_add(1, Ordering::SeqCst) + 1;
                counters.trace_peak.fetch_max(live, Ordering::SeqCst);
                counters.trace_calls.fetch_add(1, Ordering::SeqCst);
                let _permit = gate.acquire().await;
                counters.trace_live.fetch_sub(1, Ordering::SeqCst);
                let _ = (protocol, port);
                Ok(nexttrace::TraceResult {
                    destination,
                    hops: vec![nexttrace::Hop {
                        ttl: 1,
                        address: "203.0.113.1".into(),
                        hostname: String::new(),
                        rtt_ms: 1.0,
                        loss: 0.0,
                        sent: 1,
                        asn: String::new(),
                        country: String::new(),
                        province: String::new(),
                        city: String::new(),
                        owner: String::new(),
                    }],
                })
            })
        }
    }

    fn seed_targets(db: &Db, count: u64, enable_mtr: bool) {
        let config = proto::CollectorAuthorizationConfig {
            primary_public_key: vec![0x11; 32],
            config_version: 1,
            kind: proto::CollectorKind::Probe as i32,
            targets: (1..=count)
                .map(|id| proto::ProbeTarget {
                    server_id: id,
                    server_name: format!("edge-{id}"),
                    ipv4: format!("203.0.113.{id}"),
                    ipv6: String::new(),
                    hostname: String::new(),
                    tcp_ports: vec![443],
                    enable_icmp: true,
                    enable_tcp: true,
                    enable_mtr,
                    interval_seconds: 30,
                    mtr_interval_seconds: 300,
                    route_interval_seconds: 86_400,
                    mtr_probes: 3,
                })
                .collect(),
            ..Default::default()
        };
        db.write(|tx| store::save_authorization(tx, "collector-storm", &config, 1))
            .unwrap();
    }

    fn storm_db(name: &str) -> Db {
        let dir =
            std::env::temp_dir().join(format!("santaizi-probe-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        crate::db::migrate::open_collector(&dir.join("collector.db").to_string_lossy()).unwrap()
    }

    fn busy_of(busy: &Arc<BusySet>, matches: impl Fn(&BusyKey) -> bool) -> usize {
        busy.lock()
            .unwrap_or_else(|err| err.into_inner())
            .iter()
            .filter(|key| matches(key))
            .count()
    }

    async fn wait_until(mut condition: impl FnMut() -> bool, limit: Duration) -> bool {
        let deadline = Instant::now() + limit;
        while Instant::now() < deadline {
            if condition() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        condition()
    }

    fn spawn_scheduler(
        engine: &Arc<ProbeScheduler>,
        db: Db,
        exec: Arc<GatedExec>,
        cancel: &CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let scheduler = Arc::clone(engine);
        let is_probe = Arc::new(|| true) as Arc<dyn Fn() -> bool + Send + Sync>;
        let cancel = cancel.clone();
        let exec: Arc<dyn ProbeExec> = exec;
        tokio::spawn(async move {
            scheduler.run(cancel, db, exec, is_probe).await;
        })
    }

    /// 复盘场景：73 个双协议目标同时到期。nexttrace 执行体与连通性 worker
    /// 都被闸门按住，此时数到的就是「同时在飞」的硬上限。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn seventy_three_targets_keep_concurrency_bounded() {
        let db = storm_db("storm");
        seed_targets(&db, STORM_TARGETS, true);
        let counters = Arc::new(Counters::default());
        let engine = Arc::new(ProbeScheduler::new());
        let exec = Arc::new(GatedExec {
            counters: Arc::clone(&counters),
            conn_gate: Arc::new(tokio::sync::Semaphore::new(0)),
            trace_gate: Arc::new(tokio::sync::Semaphore::new(0)),
        });
        let conn_gate = Arc::clone(&exec.conn_gate);
        let trace_gate = Arc::clone(&exec.trace_gate);
        let cancel = CancellationToken::new();
        let running = spawn_scheduler(&engine, db, exec, &cancel);

        assert!(
            wait_until(
                || {
                    busy_of(&engine.busy, |key| matches!(key, BusyKey::Connectivity(_)))
                        >= QUEUE_CAPACITY
                },
                Duration::from_secs(6)
            )
            .await,
            "首轮应把连通性队列填满"
        );
        assert!(
            wait_until(
                || counters.trace_calls.load(Ordering::SeqCst) >= 1,
                Duration::from_secs(3)
            )
            .await,
            "追踪执行体应已取走一项工作"
        );
        // 两个 worker 都进场再量峰值：直接断言 peak==2 依赖调度时序，会偶发。
        assert!(
            wait_until(
                || counters.conn_peak.load(Ordering::SeqCst) >= CONNECTIVITY_WORKERS,
                Duration::from_secs(3)
            )
            .await,
            "两个连通性 worker 都应进场，实际 {}",
            counters.conn_peak.load(Ordering::SeqCst)
        );

        // 闸门全关：下面看到的就是上限本身。
        assert_eq!(
            counters.conn_peak.load(Ordering::SeqCst),
            CONNECTIVITY_WORKERS,
            "连通性在飞数超过 worker 数"
        );
        assert_eq!(
            counters.trace_peak.load(Ordering::SeqCst),
            1,
            "同时只能有一个 nexttrace 子进程"
        );
        // 未完成工作 = 等待中的 + worker 手里攥着的。队列容量之外只多出固定几个
        // 在飞项，上界与目标数无关——这正是复盘里缺的那条约束。
        let conn_outstanding = busy_of(&engine.busy, |key| matches!(key, BusyKey::Connectivity(_)));
        assert!(
            conn_outstanding <= QUEUE_CAPACITY + CONNECTIVITY_WORKERS,
            "连通性未完成工作 {conn_outstanding} 超过有界上限"
        );
        let mtr_outstanding = busy_of(&engine.busy, |key| matches!(key, BusyKey::Mtr(_)));
        assert!(
            mtr_outstanding <= QUEUE_CAPACITY + 1,
            "MTR 未完成工作 {mtr_outstanding} 超过有界上限"
        );
        let route_outstanding = busy_of(&engine.busy, |key| matches!(key, BusyKey::Route(_, _)));
        assert!(
            route_outstanding <= QUEUE_CAPACITY + 1,
            "路由未完成工作 {route_outstanding} 超过有界上限"
        );

        // 放行：队列排空后 ticker 继续补位，73 个目标最终都被探到。
        conn_gate.add_permits(1024);
        trace_gate.add_permits(1024);
        assert!(
            wait_until(
                || counters.conn_calls.load(Ordering::SeqCst) >= STORM_TARGETS as usize,
                Duration::from_secs(30)
            )
            .await,
            "73 个目标都应被探到，实际 {}",
            counters.conn_calls.load(Ordering::SeqCst)
        );
        assert!(counters.conn_peak.load(Ordering::SeqCst) <= CONNECTIVITY_WORKERS);
        assert!(counters.trace_peak.load(Ordering::SeqCst) <= 1);

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), running).await;
    }

    /// MTR 开关关掉：目标照样探活、照样跑周期路由，但绝不出 MTR 样本
    /// （可达状态翻转也不拉起追踪）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn mtr_disabled_targets_never_produce_mtr_samples() {
        let db = storm_db("mtr-off");
        seed_targets(&db, 6, false);
        let counters = Arc::new(Counters::default());
        let engine = Arc::new(ProbeScheduler::new());
        let exec = Arc::new(GatedExec {
            counters: Arc::clone(&counters),
            conn_gate: Arc::new(tokio::sync::Semaphore::new(64)),
            trace_gate: Arc::new(tokio::sync::Semaphore::new(64)),
        });
        let cancel = CancellationToken::new();
        let running = spawn_scheduler(&engine, db, exec, &cancel);
        assert!(
            wait_until(
                || counters.conn_calls.load(Ordering::SeqCst) >= 6,
                Duration::from_secs(10)
            )
            .await,
            "连通性应照旧跑"
        );
        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), running).await;

        let mut mtr_samples = 0;
        let mut route_samples = 0;
        while let Some(batch) = engine.take_samples() {
            for sample in batch.samples {
                if sample.mtr.is_some() || sample.mtr_tcp.is_some() {
                    mtr_samples += 1;
                }
                if sample.route_icmp.is_some() || sample.route_tcp.is_some() {
                    route_samples += 1;
                }
            }
        }
        assert_eq!(mtr_samples, 0, "关掉 MTR 后不得产出 MTR 样本");
        assert!(route_samples > 0, "周期路由照旧");
    }

    /// 取消时即便 worker 卡在执行体里，探测循环本身也要立刻返回。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancel_returns_from_run_even_while_workers_are_blocked() {
        let db = storm_db("cancel");
        seed_targets(&db, 4, true);
        let engine = Arc::new(ProbeScheduler::new());
        let exec = Arc::new(GatedExec {
            counters: Arc::new(Counters::default()),
            conn_gate: Arc::new(tokio::sync::Semaphore::new(0)),
            trace_gate: Arc::new(tokio::sync::Semaphore::new(0)),
        });
        let conn_gate = Arc::clone(&exec.conn_gate);
        let trace_gate = Arc::clone(&exec.trace_gate);
        let cancel = CancellationToken::new();
        let running = spawn_scheduler(&engine, db, exec, &cancel);
        assert!(
            wait_until(
                || busy_of(&engine.busy, |key| matches!(key, BusyKey::Connectivity(_))) > 0,
                Duration::from_secs(6)
            )
            .await,
            "应先有工作进场"
        );
        cancel.cancel();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), running)
                .await
                .is_ok(),
            "取消后 run 必须立刻返回"
        );
        // 放行卡住的执行体，避免测试进程退出时还挂着未完成的任务。
        conn_gate.add_permits(64);
        trace_gate.add_permits(64);
    }
}
