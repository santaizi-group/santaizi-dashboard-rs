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
use crate::rpc::support::blocking_read;

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
    /// 记一个样本；窗满 30 条时返回聚合结果**并复位窗口**。
    ///
    /// 不复位 = `window_count >= 30` 从此恒真，之后每个样本都写一行
    /// `monitor_histories`：历史行爆炸，且公开站 30 天曲线与可用率被
    /// 单点样本反复污染。Go `servicesentinel.go` 在 `ts.count ==
    /// Conf.AvgPingCount` 时写库并 `ts.count = 0`，同一语义。
    fn record(&mut self, successful: bool, delay_ms: f64) -> Option<(f64, u32, u32)> {
        self.window_count += 1;
        if successful {
            self.window_up += 1;
            self.window_delay_sum += delay_ms;
            self.window_delay_count += 1;
        }
        if self.window_count < WINDOW_SAMPLES {
            return None;
        }
        let avg = if self.window_delay_count > 0 {
            self.window_delay_sum / self.window_delay_count as f64
        } else {
            0.0
        };
        let summary = (avg, self.window_up, self.window_count - self.window_up);
        // 出窗即复位：下一批 30 个样本重新累计。
        self.window_count = 0;
        self.window_up = 0;
        self.window_delay_sum = 0.0;
        self.window_delay_count = 0;
        Some(summary)
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
    samples: HashMap<(i64, i64), SampleState>,
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

    /// 窗口满后的落库 + 延迟告警（Control 回流处理里调用）。
    pub async fn finalize_window(
        &self,
        monitor_id: i64,
        server_id: i64,
        sample_delay: f64,
        avg_delay: f64,
        up: u32,
        down: u32,
    ) {
        let latency_notify = self
            .write_history(monitor_id, server_id, avg_delay, up, down)
            .await;
        if latency_notify {
            let (min, max) = self.latency_bounds(monitor_id).await;
            let breached = (max > 0.0 && sample_delay > max)
                || (min > 0.0 && sample_delay > 0.0 && sample_delay < min);
            if breached {
                let name = self.server_name(server_id).await;
                let message = format!(
                    "[延迟异常] {name}\n监控 #{monitor_id} 延迟 {sample_delay:.0}ms（允许 {min:.0}-{max:.0}ms）"
                );
                let notifier = Arc::clone(&self.notifier);
                let label = format!("monitor-latency-{monitor_id}-{server_id}");
                tokio::spawn(async move {
                    notifier
                        .send("default", &message, Some(label), Some(server_id))
                        .await;
                });
            }
        }
    }

    async fn write_history(
        &self,
        monitor_id: i64,
        server_id: i64,
        avg_delay: f64,
        up: u32,
        down: u32,
    ) -> bool {
        blocking_read(&self.db, move |conn| {
            conn.execute(
                "INSERT INTO monitor_histories (created_at, updated_at, monitor_id, server_id,                      avg_delay, up, down, data)                      VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?1, ?2, ?3, ?4, ?5, '')",
                rusqlite::params![monitor_id, server_id, avg_delay, up as i64, down as i64],
            )?;
            let latency_notify: bool = conn
                .query_row(
                    "SELECT COALESCE(latency_notify, 0) FROM monitors WHERE id = ?1 AND deleted_at IS NULL",
                    [monitor_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap_or(0)
                != 0;
            Ok(latency_notify)
        })
            .await
            .unwrap_or(false)
    }

    async fn latency_bounds(&self, monitor_id: i64) -> (f64, f64) {
        blocking_read(&self.db, move |conn| {
            Ok(conn
                .query_row(
                    "SELECT COALESCE(min_latency, 0), COALESCE(max_latency, 0) FROM monitors WHERE id = ?1",
                    [monitor_id],
                    |row| Ok((row.get::<_, f64>(0)?, row.get::<_, f64>(1)?)),
                )?)
        })
        .await
        .unwrap_or((0.0, 0.0))
    }

    async fn server_name(&self, server_id: i64) -> String {
        blocking_read(&self.db, move |conn| {
            Ok(conn
                .query_row(
                    "SELECT COALESCE(name, '') FROM servers WHERE id = ?1",
                    [server_id],
                    |row| row.get(0),
                )
                .unwrap_or_default())
        })
        .await
        .unwrap_or_default()
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
        let monitors: Vec<repo::MonitorRow> = blocking_read(&self.db, move |conn| {
            let mut stmt = conn.prepare(
                // cover 必须读真实列：include（ignore_all=1）语义靠它区分，硬编码 0 会反转覆盖范围。
                "SELECT id, COALESCE(name, ''), COALESCE(type, 0), COALESCE(target, ''), \
                 COALESCE(skip_servers_raw, '[]'), COALESCE(duration, 30), 0, '', COALESCE(cover, 0), \
                 0, 0, 0, 0, NULL, NULL \
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
        .unwrap_or_else(|err| {
            tracing::warn!(error = %err, "监控列表查询失败");
            Vec::new()
        });
        let avg_ping = self.conf.read().expect("config lock").avg_ping_count.max(1);
        let _ = avg_ping;

        let mut items = Vec::new();
        {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            // 清过期 pending。
            state
                .pending
                .retain(|_, pending| pending.expires > now_mono);
            tracing::info!(
                monitors = monitors.len(),
                sessions = sessions.len(),
                "dispatch sweep"
            );
            for monitor in monitors {
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
                    now_mono + Duration::from_secs(monitor.duration.max(5) as u64),
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
                    tracing::info!(server_id = *server_id, probe_id = %request.probe_id, "dispatch item");
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

fn monitor_row_lite(row: &rusqlite::Row<'_>) -> rusqlite::Result<repo::MonitorRow> {
    Ok(repo::MonitorRow {
        id: row.get(0)?,
        name: row.get(1)?,
        monitor_type: row.get(2)?,
        target: row.get(3)?,
        skip_servers_raw: row.get(4)?,
        duration: row.get(5)?,
        notify: false,
        notification_tag: String::new(),
        // 覆盖范围语义的唯一来源：include（ignore_all）靠它识别，硬编码会反转派发范围。
        cover: row.get(8)?,
        enable_show_in_service: false,
        min_latency: 0.0,
        max_latency: 0.0,
        latency_notify: false,
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
}
