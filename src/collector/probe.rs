//! 探测型从端的探活循环。
//!
//! 每秒看一次缓存目标：ICMP/TCP 按间隔探活；周期 MTR 与路由任务都调用 nexttrace。
//! 二进制缺失时样本带明确错误。结果先入队，由 Sync 循环随运行态帧发出。

use std::collections::HashSet;
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
const ROUTE_QUEUE: usize = 32;
const DEFAULT_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_MTR_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub struct RouteWork {
    pub target: CachedProbeTarget,
    pub protocol: String,
    pub port: u32,
    pub job_id: u64,
}

struct ProbeState {
    last_cycle: Option<Instant>,
    last_mtr: Option<Instant>,
    last_route_icmp: Option<Instant>,
    last_route_tcp: Option<Instant>,
    last_reachable: Arc<Mutex<Option<bool>>>,
}

pub struct ProbeScheduler {
    pending: Mutex<Vec<proto::ProbeSample>>,
    wake: Notify,
    route_tx: mpsc::Sender<RouteWork>,
    route_rx: Mutex<Option<mpsc::Receiver<RouteWork>>>,
    inflight: Mutex<HashSet<String>>,
}

impl Default for ProbeScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl ProbeScheduler {
    pub fn new() -> Self {
        let (route_tx, route_rx) = mpsc::channel(ROUTE_QUEUE);
        Self {
            pending: Mutex::new(Vec::new()),
            wake: Notify::new(),
            route_tx,
            route_rx: Mutex::new(Some(route_rx)),
            inflight: Mutex::new(HashSet::new()),
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
        if pending.len() > SAMPLE_QUEUE_LIMIT {
            let drop_count = pending.len() - SAMPLE_QUEUE_LIMIT;
            pending.drain(0..drop_count);
        }
        drop(pending);
        self.wake.notify_one();
    }

    /// 单次上报的样本数上限：与主端 `telemetry.probe_batch_size` 默认值对齐。
    /// 探测目标多时一轮会积出远超一批的样本，若不设上限，主端那一侧就要为一个
    /// 完全由对端决定的巨型批次分配内存并开长事务。超额的留在队列里下一批再走。
    pub const MAX_BATCH: usize = 512;

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

    pub fn enqueue_jobs(&self, jobs: &[proto::ProbeRouteJob], targets: &[CachedProbeTarget]) {
        for job in jobs {
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
            let mut port = job.port;
            if protocol == "tcp" && port == 0 {
                port = first_port(&target.tcp_ports);
            }
            self.enqueue_route(RouteWork {
                target: target.clone(),
                protocol,
                port,
                job_id: job.job_id,
            });
        }
    }

    pub async fn run(
        self: Arc<Self>,
        cancel: CancellationToken,
        db: Db,
        nexttrace_path: String,
        is_probe: Arc<dyn Fn() -> bool + Send + Sync>,
    ) {
        let mut route_rx = self
            .route_rx
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .take();
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut states: std::collections::HashMap<u64, ProbeState> =
            std::collections::HashMap::new();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = ticker.tick() => {
                    if !is_probe() {
                        continue;
                    }
                    self.tick(&db, &nexttrace_path, &mut states).await;
                }
                work = recv_route(&mut route_rx) => {
                    let Some(work) = work else {
                        continue;
                    };
                    let scheduler = Arc::clone(&self);
                    let path = nexttrace_path.clone();
                    let key = route_key(work.target.server_id, &work.protocol);
                    tokio::spawn(async move {
                        let sample = exec_route(&path, &work).await;
                        scheduler.queue_samples(vec![sample]);
                        scheduler
                            .inflight
                            .lock()
                            .unwrap_or_else(|err| err.into_inner())
                            .remove(&key);
                    });
                }
            }
        }
    }

    async fn tick(
        self: &Arc<Self>,
        db: &Db,
        nexttrace_path: &str,
        states: &mut std::collections::HashMap<u64, ProbeState>,
    ) {
        let targets = match blocking_read(db, store::list_probe_targets).await {
            Ok(targets) => targets,
            Err(err) => {
                tracing::warn!("load probe targets: {err:#}");
                return;
            }
        };
        let live: HashSet<u64> = targets.iter().map(|target| target.server_id).collect();
        states.retain(|id, _| live.contains(id));
        let now = Instant::now();
        for target in &targets {
            let state = states
                .entry(target.server_id)
                .or_insert_with(|| ProbeState {
                    last_cycle: None,
                    last_mtr: None,
                    last_route_icmp: None,
                    last_route_tcp: None,
                    last_reachable: Arc::new(Mutex::new(None)),
                });
            let interval = if target.interval_sec == 0 {
                DEFAULT_INTERVAL
            } else {
                Duration::from_secs(target.interval_sec)
            };
            if state
                .last_cycle
                .is_none_or(|last| now.saturating_duration_since(last) >= interval)
            {
                state.last_cycle = Some(now);
                let mtr_every = if target.mtr_interval_sec == 0 {
                    DEFAULT_MTR_INTERVAL
                } else {
                    Duration::from_secs(target.mtr_interval_sec)
                };
                let mtr_due = target.enable_mtr
                    && state
                        .last_mtr
                        .is_none_or(|last| now.saturating_duration_since(last) >= mtr_every);
                if mtr_due {
                    state.last_mtr = Some(now);
                }
                let scheduler = Arc::clone(self);
                let target = target.clone();
                let path = nexttrace_path.to_string();
                let reachable_slot = Arc::clone(&state.last_reachable);
                tokio::spawn(async move {
                    let (sample, reachable, tcp) = run_connectivity(&target).await;
                    let flipped = {
                        let mut slot = reachable_slot.lock().unwrap_or_else(|err| err.into_inner());
                        let flipped = slot.is_some_and(|previous| previous != reachable);
                        *slot = Some(reachable);
                        flipped
                    };
                    scheduler.queue_samples(vec![sample]);
                    if mtr_due || flipped {
                        let follow = run_mtr(&path, &target, &tcp).await;
                        scheduler.queue_samples(vec![follow]);
                    }
                });
            }
            self.schedule_route(target, state, now);
        }
    }

    fn schedule_route(&self, target: &CachedProbeTarget, state: &mut ProbeState, now: Instant) {
        let interval = probes::normalize_route_interval_sec(
            i64::try_from(target.route_interval_sec).unwrap_or(i64::MAX),
        );
        let every = Duration::from_secs(interval.max(0) as u64);
        if target.enable_icmp
            && due(state.last_route_icmp, now, every)
            && self.enqueue_route(RouteWork {
                target: target.clone(),
                protocol: "icmp".into(),
                port: 0,
                job_id: 0,
            })
        {
            state.last_route_icmp = Some(now);
        }
        if target.enable_tcp && due(state.last_route_tcp, now, every) {
            let port = first_port(&target.tcp_ports);
            if port > 0
                && self.enqueue_route(RouteWork {
                    target: target.clone(),
                    protocol: "tcp".into(),
                    port,
                    job_id: 0,
                })
            {
                state.last_route_tcp = Some(now);
            }
        }
    }

    fn enqueue_route(&self, work: RouteWork) -> bool {
        let key = route_key(work.target.server_id, &work.protocol);
        {
            let mut inflight = self.inflight.lock().unwrap_or_else(|err| err.into_inner());
            if !inflight.insert(key.clone()) {
                return false;
            }
        }
        if self.route_tx.try_send(work).is_err() {
            self.inflight
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .remove(&key);
            return false;
        }
        true
    }
}

fn due(last: Option<Instant>, now: Instant, every: Duration) -> bool {
    last.is_none_or(|last| now.saturating_duration_since(last) >= every)
}

fn route_key(server_id: u64, protocol: &str) -> String {
    format!("{protocol}/{server_id}")
}

fn accept_route(target: &CachedProbeTarget, protocol: &str) -> bool {
    match protocol {
        "icmp" => target.enable_icmp,
        "tcp" => target.enable_tcp,
        _ => false,
    }
}

fn first_port(raw: &str) -> u32 {
    probes::parse_ports(raw).into_iter().next().unwrap_or(0)
}

async fn recv_route(rx: &mut Option<mpsc::Receiver<RouteWork>>) -> Option<RouteWork> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
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
    let mut sample = proto::ProbeSample {
        server_id: target.server_id,
        sampled_at_unix_nano: crate::util::time::now_nanos(),
        ..Default::default()
    };
    if dests.is_empty() {
        sample.last_error = "无探测目标".into();
        return (sample, false, Vec::new());
    }
    let mut icmp_results = Vec::new();
    if target.enable_icmp {
        for dest in &dests {
            icmp_results.push(netprobe::icmp_ping(&dest.host, dest.ipv4).await);
        }
    }
    let icmp = if icmp_results.is_empty() {
        None
    } else {
        Some(netprobe::merge_icmp(&icmp_results))
    };
    if let Some(icmp) = &icmp {
        sample.icmp = Some(proto::ProbeIcmpSample {
            ok: icmp.ok,
            rtt_ms: icmp.rtt_ms,
            loss: icmp.loss,
            packets_sent: icmp.sent,
            packets_received: icmp.received,
            error: icmp.error.clone(),
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
        tcp_results = netprobe::merge_tcp_by_port(tcp_results);
        for result in &tcp_results {
            sample.tcp.push(proto::ProbeTcpSample {
                port: result.port,
                ok: result.ok,
                rtt_ms: result.rtt_ms,
                error: result.error.clone(),
            });
        }
    }
    let has_connectivity = target.enable_icmp || target.enable_tcp;
    let mut reachable =
        icmp.as_ref().is_some_and(|item| item.ok) || tcp_results.iter().any(|item| item.ok);
    if !has_connectivity {
        reachable = true;
    } else {
        sample.last_error = netprobe::display_error(icmp.as_ref(), &tcp_results);
    }
    (sample, reachable, tcp_results)
}

async fn run_mtr(
    nexttrace_path: &str,
    target: &CachedProbeTarget,
    tcp: &[TcpResult],
) -> proto::ProbeSample {
    let mut sample = proto::ProbeSample {
        server_id: target.server_id,
        sampled_at_unix_nano: crate::util::time::now_nanos(),
        ..Default::default()
    };
    let Some((host, ipv4)) = trace_host(target) else {
        sample.last_error = "无探测目标".into();
        return sample;
    };
    let sampled_at = sample.sampled_at_unix_nano;
    if target.enable_icmp {
        match trace_with_nexttrace(nexttrace_path, &host, ipv4, false, 0).await {
            Ok(result) => sample.mtr = Some(mtr_proto(&result, "icmp", 0, sampled_at)),
            Err(err) => sample.last_error = err,
        }
    }
    if target.enable_tcp {
        let port = tcp_trace_port(tcp, &target.tcp_ports);
        if port > 0 {
            match trace_with_nexttrace(nexttrace_path, &host, ipv4, true, port).await {
                Ok(result) => sample.mtr_tcp = Some(mtr_proto(&result, "tcp", port, sampled_at)),
                Err(err) if sample.last_error.is_empty() => sample.last_error = err,
                Err(_) => {}
            }
        }
    }
    sample
}

async fn exec_route(nexttrace_path: &str, work: &RouteWork) -> proto::ProbeSample {
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
    let Some((host, ipv4)) = trace_host(&work.target) else {
        trace.error = "无探测目标".into();
        assign_route(&mut sample, &work.protocol, trace);
        return sample;
    };
    trace.destination = host.clone();
    match trace_with_nexttrace(
        nexttrace_path,
        &host,
        ipv4,
        work.protocol == "tcp",
        work.port,
    )
    .await
    {
        Ok(result) => {
            if !result.destination.is_empty() {
                trace.destination = result.destination;
            }
            trace.hops = result
                .hops
                .into_iter()
                .map(|hop| {
                    let mut country = hop.country;
                    let mut asn = hop.asn;
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
                        address: hop.address,
                        hostname: hop.hostname,
                        rtt_ms: hop.rtt_ms,
                        loss: hop.loss,
                        sent: hop.sent,
                        asn,
                        country,
                        province: hop.province,
                        city: hop.city,
                        owner: hop.owner,
                    }
                })
                .collect();
        }
        Err(err) => trace.error = err,
    }
    assign_route(&mut sample, &work.protocol, trace);
    sample
}

async fn trace_with_nexttrace(
    configured: &str,
    host: &str,
    ipv4: bool,
    tcp: bool,
    port: u32,
) -> Result<nexttrace::TraceResult, String> {
    let bin = nexttrace::resolve_binary(configured)?;
    nexttrace::run(
        &bin,
        &TraceOpts {
            target: host.to_string(),
            tcp,
            port,
            ipv4,
            ipv6: !ipv4,
        },
    )
    .await
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

    /// 从端上限必须与主端 `telemetry.probe_batch_size` 默认值一致：
    /// 默认配置下自家从端永远不被自己的上限挡住，上限只拦异常批次。
    #[test]
    fn max_batch_matches_the_primary_default() {
        let conf = crate::config::Config::default();
        assert_eq!(
            ProbeScheduler::MAX_BATCH,
            conf.telemetry.probe_batch_size,
            "从端探测批次上限与主端默认值漂移了"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn missing_nexttrace_is_reported_on_the_route_sample() {
        let target = CachedProbeTarget {
            server_id: 7,
            server_name: "edge".into(),
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
        };
        let sample = exec_route(
            "/tmp/does-not-exist-nexttrace",
            &RouteWork {
                target,
                protocol: "icmp".into(),
                port: 0,
                job_id: 9,
            },
        )
        .await;
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
