//! 监控派发引擎（Phase 6，对齐 Go `service/singleton/servicesentinel.go` 的
//! 派发半边 + `service/rpc/control.go` 的 DispatchMonitor/pendingProbes）。
//!
//! 调度器按 `monitors.duration` 周期把 ProbeRequest 派给已连接、具备对应
//! capability 且在 scope 内的 agent Control 流；probe_id 登记 pending
//! （2 分钟过期）；结果回流反查后按 30 样本窗聚合写 `monitor_histories`。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use crate::config::ConfigHandle;
use crate::db::Db;
use crate::proto;
use crate::repo::typed_resources as repo;
use crate::rpc::support::{blocking_read, blocking_write};
use crate::util::time as util_time;

/// pending 生存期（Go `2 * time.Minute`）。
const PENDING_TTL: Duration = Duration::from_secs(120);
/// 聚合窗口样本数（Go：每 30 个样本写一条 MonitorHistory）。
const WINDOW_SAMPLES: u32 = 30;
/// 单目标超时（对齐 Go：HTTP 30s / ICMP 20s+5 次 / TCP 10s）。
const HTTP_TIMEOUT_MS: u32 = 30_000;
const ICMP_COUNT: u32 = 5;
const ICMP_TIMEOUT_MS: u32 = 20_000;
const TCP_TIMEOUT_MS: u32 = 10_000;

#[derive(Debug, Clone)]
struct PendingProbe {
    monitor_id: i64,
    server_id: i64,
    expires: Instant,
}

/// 每条 (monitor, server) 的聚合状态：30 样本窗计数。
#[derive(Default)]
struct SampleState {
    window_count: u32,
    window_up: u32,
    /// 只累计成功样本（均值分母同）。
    window_delay_sum: f64,
    window_delay_count: u32,
}

impl SampleState {
    /// 记一个样本；窗满 30 条时返回聚合结果**并复位窗口**（保留默认 30 供旧接口与单测复用）。
    ///
    /// 不复位 = `window_count >= 30` 从此恒真，之后每个样本都写一行
    /// `monitor_histories`：历史行爆炸，且公开站 30 天曲线与可用率被
    /// 单点样本反复污染。Go `servicesentinel.go` 在 `ts.count ==
    /// Conf.AvgPingCount` 时写库并 `ts.count = 0`，同一语义。
    fn record(&mut self, successful: bool, delay_ms: f64) -> Option<(f64, u32, u32)> {
        self.record_with_window(successful, delay_ms, WINDOW_SAMPLES)
    }

    /// 按配置窗口大小（`avg_ping_count.clamp(1, 30)`）累计样本并复位。
    fn record_with_window(
        &mut self,
        successful: bool,
        delay_ms: f64,
        window_samples: u32,
    ) -> Option<(f64, u32, u32)> {
        let target_window = window_samples.clamp(1, WINDOW_SAMPLES);
        self.window_count += 1;
        if successful {
            self.window_up += 1;
            self.window_delay_sum += delay_ms;
            self.window_delay_count += 1;
        }
        if self.window_count < target_window {
            return None;
        }
        let avg = if self.window_delay_count > 0 {
            self.window_delay_sum / self.window_delay_count as f64
        } else {
            0.0
        };
        let summary = (avg, self.window_up, self.window_count - self.window_up);
        // 出窗即复位：下一批样本重新累计。
        self.window_count = 0;
        self.window_up = 0;
        self.window_delay_sum = 0.0;
        self.window_delay_count = 0;
        Some(summary)
    }
}

const STATUS_UNKNOWN: u8 = 0;
const STATUS_GOOD: u8 = 1;
const STATUS_LOW_AVAILABILITY: u8 = 2;
const STATUS_DOWN: u8 = 3;

/// 单项服务监控跨探测节点的最近 30 条结果滑窗，用于计算实时可用率与状态切换告警。
#[derive(Default)]
struct MonitorRuntimeTracker {
    recent: std::collections::VecDeque<(bool, f64)>,
    last_status: u8,
}

impl MonitorRuntimeTracker {
    fn push(&mut self, successful: bool, delay_ms: f64) -> (u8, u64, u64) {
        if self.recent.len() >= WINDOW_SAMPLES as usize {
            self.recent.pop_front();
        }
        self.recent.push_back((successful, delay_ms));
        let total = self.recent.len() as u64;
        let up = self.recent.iter().filter(|(ok, _)| *ok).count() as u64;
        let down = total.saturating_sub(up);
        let up_percent = (up * 100).checked_div(total).unwrap_or(0);
        let status = if up_percent > 95 {
            STATUS_GOOD
        } else if up_percent > 80 {
            STATUS_LOW_AVAILABILITY
        } else {
            STATUS_DOWN
        };
        (status, up_percent, down)
    }

    fn observe(&mut self, successful: bool, delay_ms: f64) -> Option<(u8, u8, u64)> {
        let prev_status = self.last_status;
        let (next_status, up_percent, down) = self.push(successful, delay_ms);
        if prev_status == STATUS_UNKNOWN {
            // 冷启动至少观察 2 个样本，且进入非健康状态至少需要 2 个失败样本，
            // 避免重启瞬间单次网络抖动误报 [服务故障] 或后续误报孤立 [服务恢复]。
            if self.recent.len() < 2 {
                return None;
            }
            if next_status != STATUS_GOOD && down < 2 {
                return None;
            }
            self.last_status = next_status;
            return if next_status != STATUS_GOOD {
                Some((prev_status, next_status, up_percent))
            } else {
                None
            };
        }
        if next_status != prev_status {
            self.last_status = next_status;
            Some((prev_status, next_status, up_percent))
        } else {
            None
        }
    }
}

#[derive(Default)]
struct EngineState {
    /// server_id → Control 流响应发送端 + capabilities。
    ///
    /// 会话带 hello 的 node_uuid：同一 server 上换节点接管 Control 流必须
    /// 能被判出来（Go `enroll.go:66` 正是拿它与重绑节点比对，不同即
    /// FailedPrecondition），否则旧探针的响应端被静默替换、派发错人。
    sessions: HashMap<i64, ControlSession>,
    pending: HashMap<String, PendingProbe>,
    /// monitor_id → 下次可派发时刻（按 duration 周期）。
    next_dispatch: HashMap<i64, Instant>,
    /// monitor_id → 上次生效的 `(duration, target)`，供缩短周期或更换目标时立即重算与清缓存。
    monitor_configs: HashMap<i64, (i64, String)>,
    samples: HashMap<(i64, i64), SampleState>,
    trackers: HashMap<i64, MonitorRuntimeTracker>,
    /// (monitor_id, server_id) → 证书 `(issuer, expires_at_unix)`，按探测节点隔离避免多地域 CDN 误报。
    ssl_certs: HashMap<(i64, i64), (String, i64)>,
}

use std::result::Result as StdResult;

/// Control 流会话：响应发送端 + hello 声明的探测 capabilities + 节点身份。
type ControlSession = (
    mpsc::Sender<StdResult<proto::PrimaryControlResponse, tonic::Status>>,
    Vec<i32>,
    Vec<u8>,
);

/// 监控派发引擎。挂在 `AppState`；Control 流注册会话、回流结果。
pub struct MonitorEngine {
    db: Db,
    conf: ConfigHandle,
    notifier: Arc<crate::notify::Notifier>,
    state: Mutex<EngineState>,
}

/// Control 流的一帧派发产物（供 control_loop 发送）。
pub struct DispatchItem {
    pub server_id: i64,
    pub request: proto::ProbeRequest,
}

impl MonitorEngine {
    pub fn new(db: Db, conf: ConfigHandle, notifier: Arc<crate::notify::Notifier>) -> Self {
        Self {
            db,
            conf,
            notifier,
            state: Mutex::new(EngineState::default()),
        }
    }

    /// Control 流建流注册（capabilities 与 node_uuid 来自 hello）。
    pub fn register_session(
        &self,
        server_id: i64,
        node_uuid: Vec<u8>,
        capabilities: Vec<i32>,
        tx: mpsc::Sender<StdResult<proto::PrimaryControlResponse, tonic::Status>>,
    ) {
        self.state
            .lock()
            .expect("monitor lock")
            .sessions
            .insert(server_id, (tx, capabilities, node_uuid));
    }

    /// 注销必须带节点身份：只摘自己那一段。
    ///
    /// 旧探针断流收尾时若按 server_id 无条件删，会把已在同一 server 上建流的
    /// 新会话一起摘掉——探测派发从此静默中断（这正是审查记的
    /// `control.rs:212-219`「注册/注销无会话标识」）。
    pub fn unregister_session(&self, server_id: i64, node_uuid: &[u8]) {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        match state.sessions.get(&server_id) {
            Some((_, _, live)) if live.as_slice() == node_uuid => {
                state.sessions.remove(&server_id);
            }
            // 已经是别的节点的会话了：不能替别人收尾。
            _ => {}
        }
    }

    /// 该 server 上是否已有**别的节点**占着 Control 流。
    ///
    /// Go `enroll.go:66`：server 已绑到别的节点、且那个节点的 Control 流还活着，
    /// 重绑一律 FailedPrecondition——不能让离线探针凭秘密偷走在线节点的绑定。
    /// 只比 node_uuid，空键视为不同（秘密认证路径带不出节点身份）。
    pub fn session_node_conflict(&self, server_id: i64, node_uuid: &[u8]) -> bool {
        let state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        match state.sessions.get(&server_id) {
            Some((_, _, live)) => live.as_slice() != node_uuid,
            None => false,
        }
    }

    /// ProbeResult 回流：反查 pending → 样本入窗 → 满 30 条写历史（调用方
    /// 需在 async 上下文里对返回值落库）。返回 Some 表示命中派发且窗口已满。
    #[allow(clippy::type_complexity)]
    pub fn handle_result(
        &self,
        server_id: i64,
        result: &proto::ProbeResult,
    ) -> Option<(i64, i64, f64, f64, u32, u32)> {
        // 返回 (monitor_id, server_id, 样本延迟, 窗口均值, up, down)。
        let now = Instant::now();
        let pending = {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            match state.pending.remove(&result.probe_id) {
                Some(entry) if entry.server_id == server_id && entry.expires > now => entry,
                _ => return None,
            }
        };
        let successful = result.successful;
        let delay = result.delay_ms.max(0.0);
        let monitor_id = pending.monitor_id;

        // 样本入窗；满 30 条返回聚合结果由调用方异步落库 + 触发延迟告警。
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        let entry = state.samples.entry((monitor_id, server_id)).or_default();
        entry
            .record(successful, delay)
            .map(|(avg_delay, up, down)| (monitor_id, server_id, delay, avg_delay, up, down))
    }

    /// 完整处理单次 `ProbeResult`：反查 pending → 按配置样本窗落历史（含错误/证书摘要）→
    /// 逐样本延迟越界/恢复解静默 → 服务可用率状态切换通知 → HTTPS 证书到期/变更通知。
    pub async fn process_probe_result(&self, server_id: i64, result: &proto::ProbeResult) {
        let now_mono = Instant::now();
        let (location, window_samples) = {
            let conf = self.conf.read().expect("config lock");
            (
                conf.location.clone(),
                conf.avg_ping_count.clamp(1, WINDOW_SAMPLES as i32) as u32,
            )
        };
        let data_text = probe_result_message(result, &location);
        let successful = result.successful;
        let delay = if result.delay_ms.is_finite() {
            result.delay_ms.max(0.0)
        } else {
            0.0
        };

        let (monitor_id, window_summary, status_transition, ssl_eval) = {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            let pending = match state.pending.remove(&result.probe_id) {
                Some(entry) if entry.server_id == server_id && entry.expires > now_mono => entry,
                _ => return,
            };
            let monitor_id = pending.monitor_id;
            let window = state
                .samples
                .entry((monitor_id, server_id))
                .or_default()
                .record_with_window(successful, delay, window_samples);

            let transition = state
                .trackers
                .entry(monitor_id)
                .or_default()
                .observe(successful, delay);

            let ssl_eval = if let Some(proto::probe_result::Detail::Http(http)) = &result.detail {
                // 颁发机构是远端证书串：限长 + 去控制字符后再比较/落库/进静音键。
                let issuer = sanitize_cert_text(&http.tls_issuer);
                if !issuer.is_empty() && http.tls_expires_at_unix > 0 {
                    let prev_cert = state.ssl_certs.get(&(monitor_id, server_id)).cloned();
                    let changed = cert_change(&prev_cert, &issuer, http.tls_expires_at_unix);
                    if changed.is_some() || prev_cert.is_none() {
                        state.ssl_certs.insert(
                            (monitor_id, server_id),
                            (issuer.clone(), http.tls_expires_at_unix),
                        );
                    }
                    Some((issuer, http.tls_expires_at_unix, changed))
                } else {
                    None
                }
            } else {
                None
            };

            (monitor_id, window, transition, ssl_eval)
        };

        if let Some((avg_delay, up, down)) = window_summary {
            self.write_history_with_data(monitor_id, server_id, avg_delay, up, down, &data_text)
                .await;
        }

        let Some((monitor, reporter_name)) =
            self.load_monitor_and_server(monitor_id, server_id).await
        else {
            return;
        };
        let tag = crate::notify::normalize_tag(&monitor.notification_tag).to_string();
        let reporter_label = if reporter_name.trim().is_empty() {
            format!("#{server_id}")
        } else {
            reporter_name.trim().to_string()
        };
        let monitor_label = if monitor.name.trim().is_empty() {
            format!("#{monitor_id}")
        } else {
            monitor.name.trim().to_string()
        };

        // 1) 逐样本延迟告警与恢复解静默
        if monitor.latency_notify && successful && delay > 0.0 {
            let min_label = format!("bf::slmin-{monitor_id}-{server_id}");
            let max_label = format!("bf::slmax-{monitor_id}-{server_id}");
            if monitor.max_latency > 0.0 && delay > monitor.max_latency {
                let msg = format!(
                    "[延迟异常] {monitor_label}（探测节点：{reporter_label}）\n当前延迟：{delay:.0}ms（超上限 {:.0}ms）",
                    monitor.max_latency
                );
                self.notifier
                    .send(&tag, &msg, Some(max_label), Some(server_id))
                    .await;
            } else if monitor.min_latency > 0.0 && delay < monitor.min_latency {
                let msg = format!(
                    "[延迟异常] {monitor_label}（探测节点：{reporter_label}）\n当前延迟：{delay:.0}ms（低于下限 {:.0}ms）",
                    monitor.min_latency
                );
                self.notifier
                    .send(&tag, &msg, Some(min_label), Some(server_id))
                    .await;
            } else {
                self.notifier.unmute(&tag, &min_label);
                self.notifier.unmute(&tag, &max_label);
            }
        }

        let down_mute = format!("bf::ssc-down-{monitor_id}");
        let up_mute = format!("bf::ssc-up-{monitor_id}");
        if let Some((_prev, next_status, _)) = status_transition {
            // 即使 notify 暂时关闭，状态翻转也应同步解除反向静音，避免重新开启后被旧静音吞掉。
            if next_status == STATUS_GOOD {
                self.notifier.unmute(&tag, &down_mute);
            } else {
                self.notifier.unmute(&tag, &up_mute);
            }
        }

        if !monitor.notify {
            return;
        }

        // 2) 服务可用率状态切换告警与恢复（对称双键：波动/故障共用 down_mute，恢复用 up_mute）
        if let Some((_prev, next_status, up_percent)) = status_transition {
            let (state_mute, msg) = match next_status {
                STATUS_GOOD => (
                    up_mute,
                    format!(
                        "[服务恢复] {monitor_label}\n目标：{}\n可用率：{up_percent}%（探测节点：{reporter_label}，延迟 {delay:.0}ms）",
                        monitor.target
                    ),
                ),
                STATUS_LOW_AVAILABILITY => {
                    let detail = if data_text.is_empty() {
                        "部分探测失败"
                    } else {
                        data_text.as_str()
                    };
                    (
                        down_mute,
                        format!(
                            "[服务波动] {monitor_label}\n目标：{}\n可用率：{up_percent}%（探测节点：{reporter_label}）\n详情：{detail}",
                            monitor.target
                        ),
                    )
                }
                _ => {
                    let detail = if data_text.is_empty() {
                        "探测失败"
                    } else {
                        data_text.as_str()
                    };
                    (
                        down_mute,
                        format!(
                            "[服务故障] {monitor_label}\n目标：{}\n可用率：{up_percent}%（探测节点：{reporter_label}）\n详情：{detail}",
                            monitor.target
                        ),
                    )
                }
            };
            self.notifier
                .send(&tag, &msg, Some(state_mute), Some(server_id))
                .await;
        }

        // 3) HTTPS 证书到期预警（< 7 天）与证书变更通知
        if let Some((issuer, expires_unix, changed)) = ssl_eval {
            let now_unix = util_time::now_nanos() / 1_000_000_000;
            let remaining_secs = expires_unix - now_unix;
            let exp_str = util_time::format_unix_secs_in_location(expires_unix, &location);
            if remaining_secs < 7 * 86_400 {
                let exp_mute = format!("bf::sssl-exp-{monitor_id}-{expires_unix}");
                let msg = if remaining_secs <= 0 {
                    format!(
                        "[SSL证书已过期] {monitor_label}\n目标：{}\n颁发机构：{issuer}\n过期时间：{exp_str}",
                        monitor.target
                    )
                } else {
                    let days = remaining_secs as f64 / 86_400.0;
                    format!(
                        "[SSL证书即将到期] {monitor_label}\n目标：{}\n颁发机构：{issuer}\n到期时间：{exp_str}（剩余 {days:.1} 天）",
                        monitor.target
                    )
                };
                self.notifier
                    .send(&tag, &msg, Some(exp_mute), Some(server_id))
                    .await;
            }
            if let Some((old_issuer, old_exp)) = changed {
                let old_exp_str = util_time::format_unix_secs_in_location(old_exp, &location);
                let chg_mute = format!("bf::sssl-chg-{monitor_id}-{issuer}-{expires_unix}");
                let msg = format!(
                    "[SSL证书变更] {monitor_label}\n目标：{}\n原证书：{old_issuer}（到期 {old_exp_str}）\n新证书：{issuer}（到期 {exp_str}）",
                    monitor.target
                );
                self.notifier
                    .send(&tag, &msg, Some(chg_mute), Some(server_id))
                    .await;
            }
        }
    }

    /// 窗口满后的落库 + 延迟告警（保留供测试直接调用）。
    pub async fn finalize_window(
        &self,
        monitor_id: i64,
        server_id: i64,
        sample_delay: f64,
        avg_delay: f64,
        up: u32,
        down: u32,
    ) {
        self.write_history_with_data(monitor_id, server_id, avg_delay, up, down, "")
            .await;
        let Some((monitor, server_name)) =
            self.load_monitor_and_server(monitor_id, server_id).await
        else {
            return;
        };
        if monitor.latency_notify {
            let min = monitor.min_latency;
            let max = monitor.max_latency;
            let tag = crate::notify::normalize_tag(&monitor.notification_tag).to_string();
            let label = format!("monitor-latency-{monitor_id}-{server_id}");
            let breached = (max > 0.0 && sample_delay > max)
                || (min > 0.0 && sample_delay > 0.0 && sample_delay < min);
            if breached {
                let monitor_name = if monitor.name.trim().is_empty() {
                    format!("#{monitor_id}")
                } else {
                    monitor.name.trim().to_string()
                };
                let message = format!(
                    "[延迟异常] {monitor_name}（探测节点：{server_name}）\n当前延迟：{sample_delay:.0}ms（允许 {min:.0}-{max:.0}ms）"
                );
                let notifier = Arc::clone(&self.notifier);
                tokio::spawn(async move {
                    notifier
                        .send(&tag, &message, Some(label), Some(server_id))
                        .await;
                });
            } else {
                self.notifier.unmute(&tag, &label);
            }
        }
    }

    async fn write_history_with_data(
        &self,
        monitor_id: i64,
        server_id: i64,
        avg_delay: f64,
        up: u32,
        down: u32,
        data: &str,
    ) {
        let data = data.to_string();
        let _ = blocking_write(&self.db, move |tx| {
            tx.execute(
                "INSERT INTO monitor_histories (created_at, updated_at, monitor_id, server_id, avg_delay, up, down, data) \
                 VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![monitor_id, server_id, avg_delay, up as i64, down as i64, data],
            )?;
            Ok(())
        })
        .await;
    }

    async fn load_monitor_and_server(
        &self,
        monitor_id: i64,
        server_id: i64,
    ) -> Option<(repo::MonitorRow, String)> {
        blocking_read(&self.db, move |conn| {
            use rusqlite::OptionalExtension as _;
            let monitor = conn
                .query_row(
                    "SELECT id, COALESCE(name, ''), COALESCE(type, 0), COALESCE(target, ''), \
                     COALESCE(skip_servers_raw, '[]'), COALESCE(duration, 30), COALESCE(notify, 0), \
                     COALESCE(notification_tag, ''), COALESCE(cover, 0), COALESCE(enable_show_in_service, 0), \
                     COALESCE(min_latency, 0), COALESCE(max_latency, 0), COALESCE(latency_notify, 0), NULL, NULL \
                     FROM monitors WHERE id = ?1 AND deleted_at IS NULL",
                    [monitor_id],
                    monitor_row_lite,
                )
                .optional()?;
            let Some(monitor) = monitor else {
                return Ok(None);
            };
            let server_name: String = conn
                .query_row(
                    "SELECT COALESCE(name, '') FROM servers WHERE id = ?1",
                    [server_id],
                    |row| row.get(0),
                )
                .optional()?
                .unwrap_or_default();
            Ok(Some((monitor, server_name)))
        })
        .await
        .ok()
        .flatten()
    }

    /// 调度循环：1s 粒度扫描到期 monitor 并派发；顺带清理过期 pending。
    pub async fn run(self: Arc<Self>, shutdown: tokio::sync::watch::Receiver<bool>) {
        tracing::info!("监控派发循环启动");
        let mut shutdown = shutdown;
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let items = self.dispatch_due().await;
                    // 先克隆发送端再逐个 await（std Mutex 不跨 await）。
                    let senders: Vec<(i64, mpsc::Sender<StdResult<proto::PrimaryControlResponse, tonic::Status>>)> = {
                        let state = self.state.lock().unwrap_or_else(|err| err.into_inner());
                        state
                            .sessions
                            .iter()
                            .map(|(id, (tx, _, _))| (*id, tx.clone()))
                            .collect()
                    };
                    for item in items {
                        if let Some((_, tx)) = senders.iter().find(|(id, _)| *id == item.server_id) {
                            let response = proto::PrimaryControlResponse {
                                body: Some(proto::primary_control_response::Body::ProbeRequest(
                                    item.request,
                                )),
                            };
                            if tx.send(Ok(response)).await.is_err() {
                                tracing::warn!(server_id = item.server_id, "probe 派发失败：流已关闭");
                            }
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    /// 扫描到期 monitor，构造派发项并登记 pending。
    async fn dispatch_due(&self) -> Vec<DispatchItem> {
        let now_mono = Instant::now();
        // 会话快照（id + capability）。
        let sessions: Vec<(i64, Vec<i32>)> = {
            let state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            state
                .sessions
                .iter()
                .map(|(id, (_, caps, _))| (*id, caps.clone()))
                .collect()
        };
        if sessions.is_empty() {
            return Vec::new();
        }
        let monitors: Vec<repo::MonitorRow> = match blocking_read(&self.db, move |conn| {
            let mut stmt = conn.prepare(
                // cover 必须读真实列：include（ignore_all=1）语义靠它区分，硬编码 0 会反转覆盖范围。
                "SELECT id, COALESCE(name, ''), COALESCE(type, 0), COALESCE(target, ''), \
                 COALESCE(skip_servers_raw, '[]'), COALESCE(duration, 30), COALESCE(notify, 0), \
                 COALESCE(notification_tag, ''), COALESCE(cover, 0), COALESCE(enable_show_in_service, 0), \
                 COALESCE(min_latency, 0), COALESCE(max_latency, 0), COALESCE(latency_notify, 0), NULL, NULL \
                 FROM monitors WHERE deleted_at IS NULL",
            )?;
            let mapped = stmt.query_map([], monitor_row_lite)?;
            let mut out = Vec::new();
            for row in mapped {
                out.push(row?);
            }
            Ok(out)
        })
        .await
        {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(error = %err, "监控列表查询失败");
                return Vec::new();
            }
        };

        let mut items = Vec::new();
        {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            let live_ids: std::collections::HashSet<i64> =
                monitors.iter().map(|monitor| monitor.id).collect();
            // 清理已删除监控项的内存状态与过期 pending。
            state.next_dispatch.retain(|id, _| live_ids.contains(id));
            state.monitor_configs.retain(|id, _| live_ids.contains(id));
            state.trackers.retain(|id, _| live_ids.contains(id));
            state.samples.retain(|(id, _), _| live_ids.contains(id));
            state.ssl_certs.retain(|(id, _), _| live_ids.contains(id));
            state.pending.retain(|_, pending| {
                pending.expires > now_mono && live_ids.contains(&pending.monitor_id)
            });
            tracing::debug!(
                monitors = monitors.len(),
                sessions = sessions.len(),
                "dispatch sweep"
            );
            for monitor in monitors {
                let effective_duration = monitor.duration.max(5);
                let normalized_target = monitor.target.trim().to_string();
                if let Some((prev_duration, prev_target)) =
                    state.monitor_configs.get(&monitor.id).cloned()
                {
                    if prev_target != normalized_target {
                        // 目标变更：立即重新派发并清空旧目标的证书/滑窗/在飞 pending，避免误报 [SSL证书变更]。
                        state.next_dispatch.remove(&monitor.id);
                        state.ssl_certs.retain(|(id, _), _| *id != monitor.id);
                        state.trackers.remove(&monitor.id);
                        state.samples.retain(|(id, _), _| *id != monitor.id);
                        state
                            .pending
                            .retain(|_, pending| pending.monitor_id != monitor.id);
                    } else if effective_duration < prev_duration {
                        let shrink =
                            Duration::from_secs((prev_duration - effective_duration) as u64);
                        if let Some(next) = state.next_dispatch.get_mut(&monitor.id) {
                            *next = next.checked_sub(shrink).unwrap_or(now_mono);
                        }
                    }
                }
                state
                    .monitor_configs
                    .insert(monitor.id, (effective_duration, normalized_target));

                let next = state
                    .next_dispatch
                    .get(&monitor.id)
                    .copied()
                    .unwrap_or(now_mono - Duration::from_secs(1));
                if now_mono < next {
                    continue;
                }
                state.next_dispatch.insert(
                    monitor.id,
                    now_mono + Duration::from_secs(effective_duration as u64),
                );
                let Some(capability) = monitor_capability(monitor.monitor_type) else {
                    continue;
                };
                let Some(request_template) = probe_request_for_monitor(&monitor, 0) else {
                    continue;
                };
                let scope_ids = skip_server_ids(&monitor.skip_servers_raw);
                for (server_id, caps) in &sessions {
                    let in_scope = if monitor.cover == repo::MONITOR_COVER_IGNORE_ALL {
                        scope_ids.contains(server_id)
                    } else {
                        !scope_ids.contains(server_id)
                    };
                    if !in_scope || !caps.contains(&capability) {
                        continue;
                    }
                    let probe_id = format!("{}-{}-{}", monitor.id, server_id, random_hex8());
                    let mut request = request_template.clone();
                    request.probe_id = probe_id.clone();
                    state.pending.insert(
                        probe_id,
                        PendingProbe {
                            monitor_id: monitor.id,
                            server_id: *server_id,
                            expires: now_mono + PENDING_TTL,
                        },
                    );
                    tracing::debug!(server_id = *server_id, probe_id = %request.probe_id, "dispatch item");
                    items.push(DispatchItem {
                        server_id: *server_id,
                        request,
                    });
                }
            }
        }
        items
    }
}

fn probe_result_message(result: &proto::ProbeResult, location: &str) -> String {
    let err = result.error.trim();
    if !err.is_empty() {
        return err.chars().take(512).collect();
    }
    if let Some(proto::probe_result::Detail::Http(http)) = &result.detail {
        let issuer = sanitize_cert_text(&http.tls_issuer);
        if !issuer.is_empty() && http.tls_expires_at_unix > 0 {
            return format!(
                "{issuer}|{}",
                util_time::format_unix_secs_in_location(http.tls_expires_at_unix, location)
            );
        }
        if http.status_code > 0 {
            return format!("HTTP {}", http.status_code);
        }
    }
    String::new()
}

/// 证书变更判定：颁发机构或到期时间任一变化都算变更。
/// 同 CA 续签只改到期时间——旧实现要求两者同时变，整类漏报。
fn cert_change(
    previous: &Option<(String, i64)>,
    issuer: &str,
    expires_at_unix: i64,
) -> Option<(String, i64)> {
    match previous {
        Some((old_issuer, old_exp)) if old_issuer != issuer || *old_exp != expires_at_unix => {
            Some((old_issuer.clone(), *old_exp))
        }
        _ => None,
    }
}

/// 远端证书/探针文本进正文与静音键前的清洗：去控制字符、限长。
/// `data_text` 会整条写进 `monitor_histories.data` 并常驻样本窗，
/// 超长颁发机构串（或伪造换行）会同时污染库、内存与 `mutes` 键名。
const MAX_CERT_TEXT: usize = 128;

fn sanitize_cert_text(raw: &str) -> String {
    raw.trim()
        .chars()
        .filter(|ch| !ch.is_control())
        .take(MAX_CERT_TEXT)
        .collect()
}

fn monitor_row_lite(row: &rusqlite::Row<'_>) -> rusqlite::Result<repo::MonitorRow> {
    Ok(repo::MonitorRow {
        id: row.get(0)?,
        name: row.get(1)?,
        monitor_type: row.get(2)?,
        target: row.get(3)?,
        skip_servers_raw: row.get(4)?,
        duration: row.get(5)?,
        notify: row.get::<_, i64>(6)? != 0,
        notification_tag: row.get(7)?,
        // 覆盖范围语义的唯一来源：include（ignore_all）靠它识别，硬编码会反转派发范围。
        cover: row.get(8)?,
        enable_show_in_service: row.get::<_, i64>(9)? != 0,
        min_latency: row.get(10)?,
        max_latency: row.get(11)?,
        latency_notify: row.get::<_, i64>(12)? != 0,
        created_at_nanos: None,
        updated_at_nanos: None,
    })
}

fn random_hex8() -> String {
    let mut bytes = [0u8; 8];
    rand::Rng::fill_bytes(&mut rand::rng(), &mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Go `monitorCapability`：monitor 类型 → agent capability 枚举值。
fn monitor_capability(monitor_type: i64) -> Option<i32> {
    match monitor_type {
        repo::MONITOR_TYPE_HTTP => Some(proto::AgentCapability::ProbeHttp as i32),
        repo::MONITOR_TYPE_ICMP => Some(proto::AgentCapability::ProbeIcmp as i32),
        repo::MONITOR_TYPE_TCP => Some(proto::AgentCapability::ProbeTcp as i32),
        _ => None,
    }
}

/// Go `probeRequestForMonitor`（超时参数对齐：30s/20s+5 次/10s）。
fn probe_request_for_monitor(
    monitor: &repo::MonitorRow,
    probe_id_seed: i64,
) -> Option<proto::ProbeRequest> {
    let target = monitor.target.trim();
    if target.is_empty() {
        return None;
    }
    let _ = probe_id_seed;
    let mut request = proto::ProbeRequest {
        probe_id: String::new(),
        ..Default::default()
    };
    match monitor.monitor_type {
        repo::MONITOR_TYPE_HTTP => {
            request.target = Some(proto::probe_request::Target::Http(
                proto::HttpProbeRequest {
                    url: target.to_string(),
                    timeout_ms: HTTP_TIMEOUT_MS,
                },
            ));
        }
        repo::MONITOR_TYPE_ICMP => {
            request.target = Some(proto::probe_request::Target::Icmp(
                proto::IcmpProbeRequest {
                    host: target.to_string(),
                    count: ICMP_COUNT,
                    timeout_ms: ICMP_TIMEOUT_MS,
                },
            ));
        }
        repo::MONITOR_TYPE_TCP => {
            let (host, port_text) = target.rsplit_once(':')?;
            let port: u64 = port_text.parse().ok()?;
            if port == 0 || port > 65535 {
                return None;
            }
            request.target = Some(proto::probe_request::Target::Tcp(proto::TcpProbeRequest {
                host: host.to_string(),
                port: port as u32,
                timeout_ms: TCP_TIMEOUT_MS,
            }));
        }
        _ => return None,
    }
    Some(request)
}

/// `InitSkipServers` 读侧：skip_servers_raw → id 列表。
fn skip_server_ids(raw: &str) -> Vec<i64> {
    serde_json::from_str::<Vec<i64>>(raw).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 远端证书串进正文/静音键前必须去控制字符并限长：
    /// 伪造换行会污染库与内存，超长颁发机构会让 `mutes` 键无界增长。
    #[test]
    fn cert_text_is_sanitized_for_message_and_mute_key() {
        assert_eq!(sanitize_cert_text("  R3  "), "R3");
        assert_eq!(sanitize_cert_text("R3\r\nX-Evil: 1"), "R3X-Evil: 1");
        assert_eq!(sanitize_cert_text(&"A".repeat(500)).len(), MAX_CERT_TEXT);

        let result = proto::ProbeResult {
            error: String::new(),
            detail: Some(proto::probe_result::Detail::Http(proto::HttpProbeResult {
                tls_issuer: format!("R9\r\n{}", "B".repeat(400)),
                tls_expires_at_unix: 1_800_000_000,
                ..Default::default()
            })),
            ..Default::default()
        };
        let message = probe_result_message(&result, "UTC");
        let (issuer, _) = message.split_once('|').expect("证书正文带到期时间");
        assert!(!issuer.contains('\n'), "正文不得跨行");
        assert_eq!(issuer.len(), MAX_CERT_TEXT, "颁发机构截断到 128 字符");
        // 静音键由正文拼出，长度因此有界。
        assert!(message.len() < MAX_CERT_TEXT + 64);
    }

    /// 同 CA 续签只改到期时间：只比颁发机构会整类漏报。
    #[test]
    fn cert_change_detects_same_ca_renewal() {
        assert_eq!(cert_change(&None, "R3", 100), None);
        assert_eq!(cert_change(&Some(("R3".into(), 100)), "R3", 100), None);
        assert_eq!(
            cert_change(&Some(("R3".into(), 100)), "R3", 200),
            Some(("R3".into(), 100)),
            "同 CA 续签必须报警"
        );
        assert_eq!(
            cert_change(&Some(("R3".into(), 100)), "R9", 100),
            Some(("R3".into(), 100)),
            "换 CA 必须报警"
        );
    }

    fn engine(tag: &str) -> (MonitorEngine, String) {
        let path =
            std::env::temp_dir().join(format!("santaizi-monitor-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let db = crate::db::migrate::open_primary(path.to_str().unwrap()).unwrap();
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) VALUES (7, 'node-7', 'tag1', X'11', 1, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason, created_at) VALUES (7, X'21212121212121212121212121212121', 0, NULL, 1, 'r', CURRENT_TIMESTAMP)",
                [],
            )?;
            tx.execute(
                "INSERT INTO monitors (name, type, target, skip_servers_raw, duration, cover) \
                 VALUES ('blog', 1, 'https://blog.example', '[]', 5, 0)",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        let conf = crate::config::new_config_handle(crate::config::Config::default());
        let notifier = Arc::new(crate::notify::Notifier::new(db.clone(), conf.clone()));
        (
            MonitorEngine::new(db, conf, notifier),
            path.to_string_lossy().into_owned(),
        )
    }

    #[test]
    fn window_aggregates_and_writes_history() {
        let engine = engine("window");
        let _path_guard = engine.1.clone();
        let engine = engine.0;
        // 登记 30 条 pending（每次派发一条），逐一回流。
        {
            let mut state = engine.state.lock().unwrap_or_else(|err| err.into_inner());
            for index in 0..30i64 {
                state.pending.insert(
                    format!("p{index}"),
                    PendingProbe {
                        monitor_id: 1,
                        server_id: 7,
                        expires: Instant::now() + PENDING_TTL,
                    },
                );
            }
        }
        let mut final_window = None;
        for index in 0..30i64 {
            let result = proto::ProbeResult {
                probe_id: format!("p{index}"),
                kind: proto::ProbeKind::Http as i32,
                successful: index != 4,
                delay_ms: 100.0 + index as f64,
                ..Default::default()
            };
            if let Some(window) = engine.handle_result(7, &result) {
                final_window = Some(window);
            }
        }
        let (monitor_id, server_id, sample_delay, avg_delay, up, down) =
            final_window.expect("第 30 个样本应触发窗口");

        assert_eq!((monitor_id, server_id), (1, 7));
        assert_eq!(up, 29, "index 4 失败");
        assert_eq!(down, 1);
        // 样本 104 缺席的 29 个 100..130 均值。
        let expected: f64 = (100..130)
            .filter(|value| *value != 104)
            .map(|value| value as f64)
            .sum::<f64>()
            / 29.0;
        assert!((avg_delay - expected).abs() < 0.01);
        assert_eq!(sample_delay, 129.0);

        // 落库（async——用 block_on）。
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(engine.finalize_window(
                monitor_id,
                server_id,
                sample_delay,
                avg_delay,
                up,
                down,
            ));
        let (_count, stored_up): (i64, i64) = engine
            .db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*), COALESCE(SUM(up), 0) FROM monitor_histories WHERE monitor_id = 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?)
            })
            .unwrap();
        assert_eq!(_count, 1);
        assert_eq!(stored_up, 29);
        // 清理临时库
        std::fs::remove_file(format!("{}{}", std::env::temp_dir().display(), "")).ok();
        let _ = _path_guard;
    }

    #[test]
    fn dispatch_respects_monitor_cover() {
        // include（cover=1）只派列表内；all（cover=0）派列表外。回归：派发查询曾把
        // cover 硬编码 0，include 被反转成「列表外全派、列表内不派」。
        let path =
            std::env::temp_dir().join(format!("santaizi-monitor-cover-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let _path_guard = path.to_string_lossy().into_owned();
        let db = crate::db::migrate::open_primary(path.to_str().unwrap()).unwrap();
        db.write(|tx| {
            tx.execute(
                "INSERT INTO monitors (name, type, target, skip_servers_raw, duration, cover) \
                 VALUES ('only-7', 1, 'https://a.example', '[7]', 30, 1)",
                [],
            )?;
            tx.execute(
                "INSERT INTO monitors (name, type, target, skip_servers_raw, duration, cover) \
                 VALUES ('all-but-7', 1, 'https://b.example', '[7]', 30, 0)",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        let conf = crate::config::new_config_handle(crate::config::Config::default());
        let notifier = Arc::new(crate::notify::Notifier::new(db.clone(), conf.clone()));
        let engine = MonitorEngine::new(db, conf, notifier);
        let capability = proto::AgentCapability::ProbeHttp as i32;
        let (tx7, _rx7) = mpsc::channel(4);
        let (tx8, _rx8) = mpsc::channel(4);
        engine.register_session(7, vec![0x11; 16], vec![capability], tx7);
        engine.register_session(8, vec![0x12; 16], vec![capability], tx8);

        let items = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(engine.dispatch_due());
        // probe_id 形如 "{monitor_id}-{server_id}-{hex}"，首段即 monitor_id。
        let mut targets: Vec<(i64, i64)> = items
            .into_iter()
            .map(|item| {
                (
                    item.request
                        .probe_id
                        .split('-')
                        .next()
                        .and_then(|raw| raw.parse::<i64>().ok())
                        .unwrap_or(0),
                    item.server_id,
                )
            })
            .collect();
        targets.sort_unstable();
        assert_eq!(
            targets,
            vec![(1, 7), (2, 8)],
            "include 只派列表内(7)，all 排除列表(8)"
        );
    }

    #[test]
    fn pending_is_one_shot_and_expired_dropped() {
        let engine = engine("pending");
        let _path = engine.1;
        let engine = engine.0;
        {
            let mut state = engine.state.lock().unwrap_or_else(|err| err.into_inner());
            state.pending.insert(
                "live".into(),
                PendingProbe {
                    monitor_id: 1,
                    server_id: 7,
                    expires: Instant::now() + PENDING_TTL,
                },
            );
            state.pending.insert(
                "dead".into(),
                PendingProbe {
                    monitor_id: 1,
                    server_id: 7,
                    expires: Instant::now() - Duration::from_secs(1),
                },
            );
        }
        let hit = engine.handle_result(
            7,
            &proto::ProbeResult {
                probe_id: "dead".into(),
                successful: true,
                ..Default::default()
            },
        );
        assert!(hit.is_none(), "过期 pending 不命中");
        let hit = engine.handle_result(
            7,
            &proto::ProbeResult {
                probe_id: "live".into(),
                successful: true,
                delay_ms: 5.0,
                ..Default::default()
            },
        );
        assert!(hit.is_none(), "窗口未满只入样本");
        // pending 一次性：同 id 二次回流不命中。
        let second = engine.handle_result(
            7,
            &proto::ProbeResult {
                probe_id: "live".into(),
                successful: true,
                delay_ms: 5.0,
                ..Default::default()
            },
        );
        assert!(second.is_none(), "pending 已消费");
        // 非 pending 的回声同样丢弃。
        let echo = engine.handle_result(
            7,
            &proto::ProbeResult {
                probe_id: "ghost".into(),
                successful: true,
                ..Default::default()
            },
        );
        assert!(echo.is_none());
    }
}

/// Control 流会话必须记住节点身份：否则换节点接管同一个 server 时，
/// 旧探针的响应端被静默替换、派发错人，且重绑检查无从判起。
#[cfg(test)]
mod session_identity_tests {
    use super::*;

    /// 会话注册/冲突判定只碰内存态，不需要库；但 `MonitorEngine::new` 要 Db，
    /// 这里开一个空的即可（临时文件路径按 tag 唯一化，避免并行测试互踩）。
    fn engine(tag: &str) -> MonitorEngine {
        let path =
            std::env::temp_dir().join(format!("santaizi-monitor-id-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let db = crate::db::migrate::open_primary(path.to_str().unwrap()).unwrap();
        let conf = crate::config::new_config_handle(crate::config::Config::default());
        let notifier = Arc::new(crate::notify::Notifier::new(db.clone(), conf.clone()));
        MonitorEngine::new(db, conf, notifier)
    }

    fn channel() -> mpsc::Sender<StdResult<proto::PrimaryControlResponse, tonic::Status>> {
        let (tx, _rx) = mpsc::channel(1);
        tx
    }

    /// 同节点重连不是冲突：探针断线重连是常态，不能因此拒绝建流。
    #[test]
    fn same_node_reconnect_is_not_a_conflict() {
        let engine = engine("same-node");
        let node = vec![0x11; 16];
        engine.register_session(7, node.clone(), vec![], channel());
        assert!(
            !engine.session_node_conflict(7, &node),
            "同一节点重连不得被判成冲突"
        );
    }

    /// 换节点接管才报冲突：这正是 Go `enroll.go:66` 要挡的场景。
    #[test]
    fn a_different_node_taking_over_is_a_conflict() {
        let engine = engine("other-node");
        engine.register_session(7, vec![0x11; 16], vec![], channel());
        assert!(
            engine.session_node_conflict(7, &[0x22; 16]),
            "别的节点接管在线 server 的 Control 流必须报冲突"
        );
    }

    /// 没有在线会话时无从冲突；注销后同样恢复可绑。
    #[test]
    fn no_live_session_means_no_conflict() {
        let engine = engine("no-session");
        assert!(!engine.session_node_conflict(7, &[0x22; 16]));
        engine.register_session(7, vec![0x11; 16], vec![], channel());
        assert!(engine.session_node_conflict(7, &[0x22; 16]));
        engine.unregister_session(7, &[0x22; 16]);
        assert!(
            engine.session_node_conflict(7, &[0x22; 16]),
            "别的节点来注销不能摘掉在线会话"
        );
        engine.unregister_session(7, &[0x11; 16]);
        assert!(
            !engine.session_node_conflict(7, &[0x22; 16]),
            "本节点断流注销后该 server 应可被别的节点重绑"
        );
    }

    /// hello 长度已在建流前校验，会话里存的必然是 16 字节；
    /// 这里守住「短键永不等于真节点」——比对按字节走，不按长度猜。
    #[test]
    fn a_short_identity_never_matches_a_real_node() {
        let engine = engine("short-node");
        engine.register_session(7, vec![0x11; 16], vec![], channel());
        assert!(engine.session_node_conflict(7, &[0x11; 8]));
        assert!(engine.session_node_conflict(7, &[]));
        assert!(!engine.session_node_conflict(7, &[0x11; 16]));
    }
}

#[cfg(test)]
mod sample_window_tests {
    use super::{SampleState, WINDOW_SAMPLES};

    #[test]
    fn window_emits_once_per_thirty_samples() {
        let mut state = SampleState::default();
        let mut emissions = 0;
        // 90 个样本 = 恰好 3 批；不复位的实现会在第 31 个起每个样本都出结果。
        for index in 0..90 {
            let successful = index % 3 != 0;
            if state.record(successful, 10.0).is_some() {
                emissions += 1;
            }
        }
        assert_eq!(emissions, 3, "90 个样本必须只出 3 条聚合，不得逐样本落库");
    }

    #[test]
    fn window_resets_after_emitting() {
        let mut state = SampleState::default();
        // 前 29 个不出窗。
        for _ in 0..WINDOW_SAMPLES - 1 {
            assert!(state.record(true, 10.0).is_none());
        }
        // 第 30 个出窗并复位。
        assert!(state.record(true, 10.0).is_some());
        // 复位后重新计满 30 才再出。
        for _ in 0..WINDOW_SAMPLES - 1 {
            assert!(state.record(true, 10.0).is_none(), "复位后不得立即再出窗");
        }
        assert!(state.record(true, 10.0).is_some());
    }

    #[test]
    fn summary_reflects_only_the_emitted_window() {
        let mut state = SampleState::default();
        // 第 1 批：30 个全失败（延迟不计入均值）。
        for _ in 0..WINDOW_SAMPLES {
            state.record(false, 0.0);
        }
        // 第 2 批：30 个全成功，延迟 100ms。
        for _ in 0..WINDOW_SAMPLES - 1 {
            assert!(state.record(true, 100.0).is_none());
        }
        let (avg, up, down) = state.record(true, 100.0).expect("第 30 个样本应出窗");
        assert_eq!(up, WINDOW_SAMPLES, "上一批的失败样本不得漏进本窗 up 计数");
        assert_eq!(down, 0);
        assert_eq!(avg, 100.0);
    }

    #[tokio::test]
    async fn process_probe_result_uses_avg_ping_count_and_symmetric_mutes() {
        use super::*;
        let path = std::env::temp_dir().join(format!(
            "santaizi-monitor-avgping-{}-{}",
            std::process::id(),
            util_time::now_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        let db = crate::db::migrate::open_primary(path.to_str().unwrap()).unwrap();
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, secret_ciphertext) VALUES (7, 'node-7', X'01'), (8, 'node-8', X'02')",
                [],
            )?;
            tx.execute(
                "INSERT INTO monitors (id, name, type, target, skip_servers_raw, duration, notify, notification_tag, cover) \
                 VALUES (1, 'cdn', 1, 'https://cdn.example', '[]', 300, 1, 'ops', 0)",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        let raw_conf = crate::config::Config {
            avg_ping_count: 2,
            ..Default::default()
        };
        let conf = crate::config::new_config_handle(raw_conf);
        let notifier = Arc::new(crate::notify::Notifier::new(db.clone(), conf.clone()));
        let engine = MonitorEngine::new(db.clone(), conf, Arc::clone(&notifier));

        let enqueue = |probe_id: &str, server_id: i64| {
            engine.state.lock().unwrap().pending.insert(
                probe_id.to_string(),
                PendingProbe {
                    monitor_id: 1,
                    server_id,
                    expires: Instant::now() + PENDING_TTL,
                },
            );
        };

        // 1) 冷启动单次失败抖动不得误报 [服务故障]；默认 avg_ping_count=2 时第 2 个样本即落库。
        enqueue("p1", 7);
        engine
            .process_probe_result(
                7,
                &proto::ProbeResult {
                    probe_id: "p1".into(),
                    successful: false,
                    error: "blip".into(),
                    ..Default::default()
                },
            )
            .await;
        assert!(
            !notifier.is_muted("ops", "bf::ssc-down-1"),
            "冷启动单次失败不得触发故障告警"
        );
        enqueue("p2", 7);
        engine
            .process_probe_result(
                7,
                &proto::ProbeResult {
                    probe_id: "p2".into(),
                    successful: true,
                    delay_ms: 40.0,
                    detail: Some(proto::probe_result::Detail::Http(proto::HttpProbeResult {
                        tls_issuer: "Let's Encrypt R10".into(),
                        tls_expires_at_unix: 4_000_000_000,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .await;
        let history_rows: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM monitor_histories WHERE monitor_id = 1",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(history_rows, 1, "avg_ping_count=2 满 2 个样本应立即写历史");

        // 2) 多地域节点（server 8）返回不同 CDN 边缘证书时，按 (monitor_id, server_id) 隔离缓存，不得误报证书变更。
        enqueue("p3", 8);
        engine
            .process_probe_result(
                8,
                &proto::ProbeResult {
                    probe_id: "p3".into(),
                    successful: true,
                    delay_ms: 50.0,
                    detail: Some(proto::probe_result::Detail::Http(proto::HttpProbeResult {
                        tls_issuer: "DigiCert Global G2".into(),
                        tls_expires_at_unix: 4_100_000_000,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .await;
        assert!(
            !notifier.is_muted("ops", "bf::sssl-chg-1-DigiCert Global G2-4100000000"),
            "不同探测节点观测到不同边缘证书不得误报证书变更"
        );

        // 3) 连续两次失败进入故障态后，在 LOW_AVAILABILITY <-> DOWN 间震荡不得清掉 down_mute。
        for i in 0..2 {
            let pid = format!("down-{i}");
            enqueue(&pid, 7);
            engine
                .process_probe_result(
                    7,
                    &proto::ProbeResult {
                        probe_id: pid,
                        successful: false,
                        error: "down".into(),
                        ..Default::default()
                    },
                )
                .await;
        }
        assert!(
            notifier.is_muted("ops", "bf::ssc-down-1"),
            "连续 2 个失败样本进入故障态应置故障静音"
        );

        // 4) 缩短 duration 与修改 target 时，dispatch_due 立即重算并清理旧状态；删除监控后清理内存。
        let cap = proto::AgentCapability::ProbeHttp as i32;
        let (tx7, _rx7) = mpsc::channel(8);
        engine.register_session(7, vec![0x11; 16], vec![cap], tx7);
        let first_items = engine.dispatch_due().await;
        assert_eq!(first_items.len(), 1);
        assert!(engine.dispatch_due().await.is_empty(), "300s 周期未到期");
        db.write(|tx| {
            tx.execute(
                "UPDATE monitors SET target = 'https://new-cdn.example', duration = 5 WHERE id = 1",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        let retarget_items = engine.dispatch_due().await;
        assert_eq!(retarget_items.len(), 1, "修改 target 后应立即重新派发");
        assert!(
            engine.state.lock().unwrap().ssl_certs.is_empty(),
            "修改 target 后应清空旧证书缓存"
        );
        db.write(|tx| {
            tx.execute("DELETE FROM monitors WHERE id = 1", [])?;
            Ok(())
        })
        .unwrap();
        let _ = engine.dispatch_due().await;
        {
            let st = engine.state.lock().unwrap();
            assert!(!st.next_dispatch.contains_key(&1));
            assert!(!st.monitor_configs.contains_key(&1));
            assert!(!st.trackers.contains_key(&1));
        }
        let _ = std::fs::remove_file(&path);
    }
}
