//! STATE rollup 聚合 worker 与保留策略：对齐 Go `service/telemetry/rollup.go`。
//!
//! 三级聚合：内存缓冲样本 → 1m 行；已落库的 STATE 事件 → 1m 行（补录路径）；
//! 1m 行 → 1h 行。窗口边界一律取整到分/小时，未结束的窗口不聚合。
//! 保留策略（retention drain）属 maintenance，另行移植。

use std::collections::BTreeMap;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;
use rusqlite::{Connection, Transaction};

use crate::db::Db;
use crate::proto;
use crate::telemetry::state_buffer::{StateSample, StateSampleBuffer};
use crate::util::time::now_nanos;

/// 1m 聚合行（agent 上报的 STATE_ROLLUP 与本 worker 的产物共用）。
pub const RESOLUTION_1M: &str = "1m";
/// 1h 聚合行：仅由本 worker 从 1m 行卷出。
pub const RESOLUTION_1H: &str = "1h";

const MINUTE_NANOS: i64 = 60_000_000_000;
const HOUR_NANOS: i64 = 60 * MINUTE_NANOS;
const ROLLUP_INTERVAL: Duration = Duration::from_secs(60);

const DEFAULT_BATCH: usize = 5000;
const DEFAULT_MAX_RUNTIME: Duration = Duration::from_secs(20);
const DEFAULT_RECEIPT_NANOS: i64 = 7 * 24 * HOUR_NANOS;
const DEFAULT_EVIDENCE_NANOS: i64 = 48 * HOUR_NANOS;
const DEFAULT_COMPACT_MIN_BYTES: i64 = 64 << 20;

const DEFAULT_STATE_RAW_NANOS: i64 = 6 * HOUR_NANOS;
const DEFAULT_STATE_ONE_MINUTE_NANOS: i64 = 30 * 24 * HOUR_NANOS;
const DEFAULT_STATE_ONE_HOUR_NANOS: i64 = 365 * 24 * HOUR_NANOS;
const DEFAULT_OBSERVATION_NANOS: i64 = 30 * 24 * HOUR_NANOS;
const DEFAULT_LIFECYCLE_NANOS: i64 = 10 * 365 * 24 * HOUR_NANOS;

/// 相邻样本计数差上限（1 PiB）：超过视为损坏证据，不计入总量。
const MAX_PLAUSIBLE_DELTA: u64 = 1 << 50;

/// 保留策略。时长一律 i64 Unix 纳秒，与全库时间表示一致。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub state_raw: i64,
    pub state_one_minute: i64,
    pub state_one_hour: i64,
    pub observation: i64,
    pub evidence: i64,
    pub lifecycle: i64,
    pub receipt: i64,
    pub batch_size: usize,
    pub max_runtime: Duration,
    pub compact_min_bytes: i64,
    pub auto_compact: bool,
}

impl RetentionPolicy {
    /// `PolicyFromConfig`：配置值原样搬来，零值交给 `normalized` 兜底。
    pub fn from_config(config: &crate::config::RetentionConfig) -> Self {
        Self {
            state_raw: config.state_raw_hours as i64 * HOUR_NANOS,
            state_one_minute: config.state_one_minute_days as i64 * 24 * HOUR_NANOS,
            state_one_hour: config.state_one_hour_days as i64 * 24 * HOUR_NANOS,
            observation: config.observation_days as i64 * 24 * HOUR_NANOS,
            evidence: config.evidence_hours as i64 * HOUR_NANOS,
            lifecycle: config.lifecycle_days as i64 * 24 * HOUR_NANOS,
            receipt: config.receipt_days as i64 * 24 * HOUR_NANOS,
            batch_size: config.batch_size,
            max_runtime: Duration::from_millis(config.max_runtime_ms),
            compact_min_bytes: config.compact_min_bytes,
            auto_compact: config.auto_compact(),
        }
        .normalized()
    }

    /// `NormalizeRetentionPolicy`：零值回落默认；Go 用 <=0 判定，这里沿用。
    pub fn normalized(mut self) -> Self {
        if self.state_raw <= 0 {
            self.state_raw = DEFAULT_STATE_RAW_NANOS;
        }
        if self.state_one_minute <= 0 {
            self.state_one_minute = DEFAULT_STATE_ONE_MINUTE_NANOS;
        }
        if self.state_one_hour <= 0 {
            self.state_one_hour = DEFAULT_STATE_ONE_HOUR_NANOS;
        }
        if self.observation <= 0 {
            self.observation = DEFAULT_OBSERVATION_NANOS;
        }
        if self.evidence <= 0 {
            self.evidence = DEFAULT_EVIDENCE_NANOS;
        }
        if self.lifecycle <= 0 {
            self.lifecycle = DEFAULT_LIFECYCLE_NANOS;
        }
        if self.receipt <= 0 {
            self.receipt = DEFAULT_RECEIPT_NANOS;
        }
        if self.batch_size == 0 {
            self.batch_size = DEFAULT_BATCH;
        }
        if self.max_runtime.is_zero() {
            self.max_runtime = DEFAULT_MAX_RUNTIME;
        }
        if self.compact_min_bytes <= 0 {
            self.compact_min_bytes = DEFAULT_COMPACT_MIN_BYTES;
        }
        self
    }
}

/// `state_rollups` 行。payload 供 hourly 聚合与前台指标点解码；
/// window/总量列冗余于 SQL，供流量策略直接 SUM 而不必逐行解码。
#[derive(Debug, Clone)]
pub struct StateRollupRow {
    pub window_start: i64,
    pub window_end: i64,
    pub net_in_total: u64,
    pub net_out_total: u64,
    pub payload: Vec<u8>,
}

/// 前台指标点用的窗口行（Go `decodePublicMetricPoints` 的输入）。
pub fn list_window(
    conn: &Connection,
    node_uuid: &[u8],
    resolution: &str,
    from_nanos: i64,
    to_nanos: i64,
) -> anyhow::Result<Vec<StateRollupRow>> {
    let mut stmt = conn.prepare(
        "SELECT window_start, window_end, net_in_total, net_out_total, payload \
         FROM state_rollups \
         WHERE node_uuid = ? AND resolution = ? AND window_start >= ? AND window_start < ? \
         ORDER BY window_start ASC",
    )?;
    let rows = stmt
        .query_map(
            rusqlite::params![node_uuid, resolution, from_nanos, to_nanos],
            |row| {
                Ok(StateRollupRow {
                    window_start: row.get(0)?,
                    window_end: row.get(1)?,
                    net_in_total: row.get::<_, i64>(2)?.max(0) as u64,
                    net_out_total: row.get::<_, i64>(3)?.max(0) as u64,
                    payload: row.get(4)?,
                })
            },
        )?
        .collect::<StdResult<Vec<_>, _>>()?;
    Ok(rows)
}

/// 流量方向（Go `model.TrafficDirection*`）。
pub const TRAFFIC_DIRECTION_INBOUND: &str = "inbound";
pub const TRAFFIC_DIRECTION_OUTBOUND: &str = "outbound";
pub const TRAFFIC_DIRECTION_TOTAL: &str = "total";

/// `sumRollupUsage`：窗口内按方向汇总流量。
/// 完整小时走 1h 行，两端不足一小时的部分走 1m 行（Go 同款两段式求和），
/// 求和过程 saturating——脏数据下封顶 u64::MAX 而不是回绕成小值。
pub fn sum_direction_usage(
    conn: &Connection,
    node_uuid: &[u8],
    direction: &str,
    start_nanos: i64,
    end_nanos: i64,
) -> anyhow::Result<u64> {
    if end_nanos <= start_nanos {
        return Ok(0);
    }
    let first_full_hour = ceil_to_hour(start_nanos);
    let last_full_hour_end = floor_to_hour(end_nanos);
    let column = match direction {
        TRAFFIC_DIRECTION_OUTBOUND => "net_out_total",
        TRAFFIC_DIRECTION_TOTAL => "net_in_total + net_out_total",
        _ => "net_in_total",
    };
    if first_full_hour < last_full_hour_end {
        let hourly = sum_rollup_column(
            conn,
            node_uuid,
            RESOLUTION_1H,
            column,
            first_full_hour,
            last_full_hour_end,
        )?;
        let leading = sum_rollup_column(
            conn,
            node_uuid,
            RESOLUTION_1M,
            column,
            start_nanos,
            first_full_hour,
        )?;
        let trailing = sum_rollup_column(
            conn,
            node_uuid,
            RESOLUTION_1M,
            column,
            last_full_hour_end,
            end_nanos,
        )?;
        let edges = leading.saturating_add(trailing);
        Ok(hourly.saturating_add(edges))
    } else {
        sum_rollup_column(
            conn,
            node_uuid,
            RESOLUTION_1M,
            column,
            start_nanos,
            end_nanos,
        )
    }
}

fn sum_rollup_column(
    conn: &Connection,
    node_uuid: &[u8],
    resolution: &str,
    column: &str,
    from_nanos: i64,
    to_nanos: i64,
) -> anyhow::Result<u64> {
    if to_nanos <= from_nanos {
        return Ok(0);
    }
    // column 只来自本模块的三个常量分支，不拼接外部输入。
    let sql = format!(
        "SELECT COALESCE(SUM({column}), 0) FROM state_rollups \
         WHERE node_uuid = ? AND resolution = ? AND window_start >= ? AND window_start < ?"
    );
    let total: i64 = conn.query_row(
        &sql,
        rusqlite::params![node_uuid, resolution, from_nanos, to_nanos],
        |row| row.get(0),
    )?;
    Ok(total.max(0) as u64)
}

fn ceil_to_hour(nanos: i64) -> i64 {
    let truncated = floor_to_hour(nanos);
    if truncated < nanos {
        truncated + HOUR_NANOS
    } else {
        truncated
    }
}

fn floor_to_hour(nanos: i64) -> i64 {
    nanos.div_euclid(HOUR_NANOS) * HOUR_NANOS
}

pub struct RollupWorker {
    db: Db,
    retention: RetentionPolicy,
    states: Arc<StateSampleBuffer>,
}

impl RollupWorker {
    pub fn new(db: Db, policy: RetentionPolicy, states: Arc<StateSampleBuffer>) -> Self {
        Self {
            db,
            retention: policy.normalized(),
            states,
        }
    }

    pub fn retention(&self) -> &RetentionPolicy {
        &self.retention
    }

    /// `RollupPending`：以 now 为基准卷「刚过去的一分钟」与「刚过去的一小时」。
    /// 分钟边界未到的窗口不碰——卷到一半的窗口会在下一轮以完整样本重算。
    pub fn rollup_pending(&self, now_unix_nano: i64) -> anyhow::Result<()> {
        let minute_end = now_unix_nano / MINUTE_NANOS * MINUTE_NANOS;
        self.rollup_memory_before(minute_end)?;
        self.rollup_raw_window(minute_end - MINUTE_NANOS, minute_end)?;
        let hour_end = now_unix_nano / HOUR_NANOS * HOUR_NANOS;
        self.rollup_hour_window(hour_end - HOUR_NANOS, hour_end)
    }

    /// `rollupMemoryBefore`：消费内存缓冲里早于 cutoff 的样本，按 (节点, 分钟) 分组聚合。
    fn rollup_memory_before(&self, cutoff_nanos: i64) -> anyhow::Result<()> {
        let samples = self.states.take_before(cutoff_nanos);
        if samples.is_empty() {
            return Ok(());
        }
        let mut grouped: BTreeMap<(Vec<u8>, i64), Vec<StateSample>> = BTreeMap::new();
        for sample in samples {
            let start = sample.collected_at_nanos / MINUTE_NANOS * MINUTE_NANOS;
            grouped
                .entry((sample.node_uuid.clone(), start))
                .or_default()
                .push(sample);
        }
        for ((node_uuid, start), mut window) in grouped {
            // Go 显式按 collected_at 排序；缓冲内是 HashMap，顺序不确定。
            window.sort_by_key(|sample| sample.collected_at_nanos);
            let states: Vec<proto::State> = window.into_iter().map(|sample| sample.state).collect();
            let Some(payload) = aggregate_states(&states, start, start + MINUTE_NANOS) else {
                continue;
            };
            self.upsert_rollup(&node_uuid, RESOLUTION_1M, &payload)?;
        }
        Ok(())
    }

    /// `rollupRawWindow`：补录路径——把窗口内已落库的 STATE 事件再卷成 1m 行。
    /// 当前摄入策略不落 STATE 事件，此路径只对历史/迁移数据生效。
    fn rollup_raw_window(&self, start_nanos: i64, end_nanos: i64) -> anyhow::Result<()> {
        let nodes = self.db.read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT node_uuid FROM telemetry_events \
                 WHERE event_type = ? AND collected_at >= ? AND collected_at < ? AND payload_retained = 1",
            )?;
            let rows = stmt
                .query_map(
                    rusqlite::params![
                        proto::TelemetryEventType::State as i32,
                        start_nanos,
                        end_nanos
                    ],
                    |row| row.get::<_, Vec<u8>>(0),
                )?
                .collect::<StdResult<Vec<_>, _>>()?;
            Ok(rows)
        })?;
        for node_uuid in nodes {
            let payloads = self.db.read(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT payload FROM telemetry_events \
                     WHERE node_uuid = ? AND event_type = ? AND collected_at >= ? AND collected_at < ? AND payload_retained = 1 \
                     ORDER BY collected_at ASC",
                )?;
                let rows = stmt
                    .query_map(
                        rusqlite::params![
                            node_uuid,
                            proto::TelemetryEventType::State as i32,
                            start_nanos,
                            end_nanos
                        ],
                        |row| row.get::<_, Vec<u8>>(0),
                    )?
                    .collect::<StdResult<Vec<_>, _>>()?;
                Ok(rows)
            })?;
            let mut states = Vec::with_capacity(payloads.len());
            for payload in payloads {
                // Go 对解码失败直接报错并中止整窗；此处保持同一严格度。
                let event = proto::TelemetryEvent::decode(payload.as_slice())
                    .map_err(|err| anyhow::anyhow!("decode telemetry event: {err}"))?;
                if let Some(proto::telemetry_event::Payload::State(state)) = event.payload {
                    states.push(state);
                }
            }
            let Some(payload) = aggregate_states(&states, start_nanos, end_nanos) else {
                continue;
            };
            self.upsert_rollup(&node_uuid, RESOLUTION_1M, &payload)?;
        }
        Ok(())
    }

    /// `rollupHourWindow`：把窗口内的 1m 行卷成 1h 行。
    /// 极值取各行 min/max 的并，均值按 sample_count 加权，总量直接求和。
    fn rollup_hour_window(&self, start_nanos: i64, end_nanos: i64) -> anyhow::Result<()> {
        let nodes = self.db.read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT node_uuid FROM state_rollups \
                 WHERE resolution = ? AND window_start >= ? AND window_start < ?",
            )?;
            let rows = stmt
                .query_map(
                    rusqlite::params![RESOLUTION_1M, start_nanos, end_nanos],
                    |row| row.get::<_, Vec<u8>>(0),
                )?
                .collect::<StdResult<Vec<_>, _>>()?;
            Ok(rows)
        })?;
        for node_uuid in nodes {
            let rows = self.db.read(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT window_start, window_end, net_in_total, net_out_total, payload \
                     FROM state_rollups \
                     WHERE node_uuid = ? AND resolution = ? AND window_start >= ? AND window_start < ? \
                     ORDER BY window_start ASC",
                )?;
                let rows = stmt
                    .query_map(
                        rusqlite::params![node_uuid, RESOLUTION_1M, start_nanos, end_nanos],
                        |row| {
                            Ok(StateRollupRow {
                                window_start: row.get(0)?,
                                window_end: row.get(1)?,
                                net_in_total: row.get::<_, i64>(2)?.max(0) as u64,
                                net_out_total: row.get::<_, i64>(3)?.max(0) as u64,
                                payload: row.get(4)?,
                            })
                        },
                    )?
                    .collect::<StdResult<Vec<_>, _>>()?;
                Ok(rows)
            })?;
            let Some(payload) = aggregate_rollups(&rows, start_nanos, end_nanos) else {
                continue;
            };
            self.upsert_rollup(&node_uuid, RESOLUTION_1H, &payload)?;
        }
        Ok(())
    }

    fn upsert_rollup(
        &self,
        node_uuid: &[u8],
        resolution: &str,
        payload: &proto::StateRollupPayload,
    ) -> anyhow::Result<()> {
        self.db.write(|tx| {
            upsert_state_rollup(tx, node_uuid, resolution, payload)?;
            Ok(())
        })
    }

    /// `RollupWorker.Run`：启动即卷一次，随后每分钟一轮。
    pub async fn run(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let worker = Arc::clone(&self);
        let initial = tokio::task::spawn_blocking(move || worker.rollup_pending(now_nanos())).await;
        report_tick("startup", initial);

        let mut ticker = tokio::time::interval(ROLLUP_INTERVAL);
        ticker.tick().await; // 首轮已由启动时的一次性执行覆盖
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let worker = Arc::clone(&self);
                    let tick = tokio::task::spawn_blocking(move || worker.rollup_pending(now_nanos())).await;
                    report_tick("rollup", tick);
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        tracing::info!("rollup worker 退出");
                        return;
                    }
                }
            }
        }
    }
}

/// 摄入侧与 rollup 侧共用的 upsert；冲突列与 Go `upsertStateRollup` 一致。
pub(crate) fn upsert_state_rollup(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    resolution: &str,
    rollup: &proto::StateRollupPayload,
) -> anyhow::Result<()> {
    if rollup.window_start_unix_nano == 0 {
        return Ok(());
    }
    let encoded = prost::Message::encode_to_vec(rollup);
    // 晚到的 StateRollup（补传/乱序）不得覆盖已经卷好的窗口：那会用一份
    // 样本更少的局部聚合换掉完整聚合，且 `rollup_hour_window` 下一轮还会
    // 把这个被污染的分钟值卷进小时桶，错误就此固化。
    // 只接受「样本数不劣于已存行」的更新；相等时保留先到者（在时限内到达的
    // 通常是按轮次正常聚合出来的那份）。
    tx.execute(
        "INSERT INTO state_rollups (node_uuid, resolution, window_start, window_end, sample_count, payload, net_in_total, net_out_total, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, CURRENT_TIMESTAMP) \
         ON CONFLICT (node_uuid, resolution, window_start) DO UPDATE SET \
         window_end = ?4, sample_count = ?5, payload = ?6, net_in_total = ?7, net_out_total = ?8 \
         WHERE excluded.sample_count >= state_rollups.sample_count",
        rusqlite::params![
            node_uuid,
            resolution,
            rollup.window_start_unix_nano,
            rollup.window_end_unix_nano,
            rollup.sample_count,
            encoded,
            rollup.net_in_total,
            rollup.net_out_total,
        ],
    )?;
    Ok(())
}

/// `aggregateStates`：min/avg/max + 相邻样本计数差。
/// min/max 由 states[0] 克隆而来，只动 Go 明列出的字段——
/// gpu 与 temperatures 因此保留首见样本值（Go 行为，勿「顺手」聚合）。
fn aggregate_states(
    states: &[proto::State],
    start_nanos: i64,
    end_nanos: i64,
) -> Option<proto::StateRollupPayload> {
    if states.is_empty() {
        return None;
    }
    let mut minimum = states[0].clone();
    let mut maximum = states[0].clone();
    let mut average = proto::State::default();
    for state in states {
        minimum.cpu = minimum.cpu.min(state.cpu);
        minimum.mem_used = minimum.mem_used.min(state.mem_used);
        minimum.swap_used = minimum.swap_used.min(state.swap_used);
        minimum.disk_used = minimum.disk_used.min(state.disk_used);
        minimum.load1 = minimum.load1.min(state.load1);
        minimum.load5 = minimum.load5.min(state.load5);
        minimum.load15 = minimum.load15.min(state.load15);
        minimum.tcp_conn_count = minimum.tcp_conn_count.min(state.tcp_conn_count);
        minimum.udp_conn_count = minimum.udp_conn_count.min(state.udp_conn_count);
        minimum.process_count = minimum.process_count.min(state.process_count);
        minimum.net_in_speed = minimum.net_in_speed.min(state.net_in_speed);
        minimum.net_out_speed = minimum.net_out_speed.min(state.net_out_speed);
        maximum.cpu = maximum.cpu.max(state.cpu);
        maximum.mem_used = maximum.mem_used.max(state.mem_used);
        maximum.swap_used = maximum.swap_used.max(state.swap_used);
        maximum.disk_used = maximum.disk_used.max(state.disk_used);
        maximum.load1 = maximum.load1.max(state.load1);
        maximum.load5 = maximum.load5.max(state.load5);
        maximum.load15 = maximum.load15.max(state.load15);
        maximum.tcp_conn_count = maximum.tcp_conn_count.max(state.tcp_conn_count);
        maximum.udp_conn_count = maximum.udp_conn_count.max(state.udp_conn_count);
        maximum.process_count = maximum.process_count.max(state.process_count);
        maximum.net_in_speed = maximum.net_in_speed.max(state.net_in_speed);
        maximum.net_out_speed = maximum.net_out_speed.max(state.net_out_speed);
        average.cpu += state.cpu;
        average.mem_used += state.mem_used;
        average.swap_used += state.swap_used;
        average.disk_used += state.disk_used;
        average.load1 += state.load1;
        average.load5 += state.load5;
        average.load15 += state.load15;
        average.tcp_conn_count += state.tcp_conn_count;
        average.udp_conn_count += state.udp_conn_count;
        average.process_count += state.process_count;
        average.net_in_speed += state.net_in_speed;
        average.net_out_speed += state.net_out_speed;
    }
    let count = states.len() as u64;
    // 计数类字段是整数均值（Go 同样整数除）；cpu/load 是浮点。
    average.cpu /= count as f64;
    average.mem_used /= count;
    average.swap_used /= count;
    average.disk_used /= count;
    average.load1 /= count as f64;
    average.load5 /= count as f64;
    average.load15 /= count as f64;
    average.tcp_conn_count /= count;
    average.udp_conn_count /= count;
    average.process_count /= count;
    average.net_in_speed /= count;
    average.net_out_speed /= count;
    let (net_in, net_out) = counter_deltas(states);
    Some(proto::StateRollupPayload {
        window_start_unix_nano: start_nanos,
        window_end_unix_nano: end_nanos,
        sample_count: states.len() as u32,
        minimum: Some(minimum),
        average: Some(average),
        maximum: Some(maximum),
        net_in_total: net_in,
        net_out_total: net_out,
    })
}

/// `aggregateRollups`：1m 行 → 1h 行。
/// 无 average 的行整行跳过；有 min/max 的行才参与极值合并；
/// 最终极值被各行合并结果覆盖（Go 语义）。
fn aggregate_rollups(
    rows: &[StateRollupRow],
    start_nanos: i64,
    end_nanos: i64,
) -> Option<proto::StateRollupPayload> {
    let mut states: Vec<proto::State> = Vec::new();
    let mut net_in_total = 0u64;
    let mut net_out_total = 0u64;
    let mut minimum: Option<proto::State> = None;
    let mut maximum: Option<proto::State> = None;
    for row in rows {
        let Ok(payload) = proto::StateRollupPayload::decode(row.payload.as_slice()) else {
            continue;
        };
        let Some(average) = payload.average.as_ref() else {
            continue;
        };
        if let (Some(candidate_minimum), Some(candidate_maximum)) =
            (payload.minimum.as_ref(), payload.maximum.as_ref())
        {
            match (minimum.as_mut(), maximum.as_mut()) {
                (Some(current_minimum), Some(current_maximum)) => merge_state_extrema(
                    current_minimum,
                    current_maximum,
                    candidate_minimum,
                    candidate_maximum,
                ),
                _ => {
                    minimum = Some(candidate_minimum.clone());
                    maximum = Some(candidate_maximum.clone());
                }
            }
        }
        for _ in 0..payload.sample_count {
            states.push(average.clone());
        }
        net_in_total += payload.net_in_total;
        net_out_total += payload.net_out_total;
    }
    let mut result = aggregate_states(&states, start_nanos, end_nanos)?;
    if let (Some(minimum), Some(maximum)) = (minimum, maximum) {
        result.minimum = Some(minimum);
        result.maximum = Some(maximum);
    }
    result.net_in_total = net_in_total;
    result.net_out_total = net_out_total;
    Some(result)
}

/// `mergeStateExtrema`：把候选极值并进当前极值。
fn merge_state_extrema(
    minimum: &mut proto::State,
    maximum: &mut proto::State,
    candidate_minimum: &proto::State,
    candidate_maximum: &proto::State,
) {
    minimum.cpu = minimum.cpu.min(candidate_minimum.cpu);
    minimum.mem_used = minimum.mem_used.min(candidate_minimum.mem_used);
    minimum.swap_used = minimum.swap_used.min(candidate_minimum.swap_used);
    minimum.disk_used = minimum.disk_used.min(candidate_minimum.disk_used);
    minimum.load1 = minimum.load1.min(candidate_minimum.load1);
    minimum.load5 = minimum.load5.min(candidate_minimum.load5);
    minimum.load15 = minimum.load15.min(candidate_minimum.load15);
    minimum.tcp_conn_count = minimum.tcp_conn_count.min(candidate_minimum.tcp_conn_count);
    minimum.udp_conn_count = minimum.udp_conn_count.min(candidate_minimum.udp_conn_count);
    minimum.process_count = minimum.process_count.min(candidate_minimum.process_count);
    minimum.net_in_speed = minimum.net_in_speed.min(candidate_minimum.net_in_speed);
    minimum.net_out_speed = minimum.net_out_speed.min(candidate_minimum.net_out_speed);
    maximum.cpu = maximum.cpu.max(candidate_maximum.cpu);
    maximum.mem_used = maximum.mem_used.max(candidate_maximum.mem_used);
    maximum.swap_used = maximum.swap_used.max(candidate_maximum.swap_used);
    maximum.disk_used = maximum.disk_used.max(candidate_maximum.disk_used);
    maximum.load1 = maximum.load1.max(candidate_maximum.load1);
    maximum.load5 = maximum.load5.max(candidate_maximum.load5);
    maximum.load15 = maximum.load15.max(candidate_maximum.load15);
    maximum.tcp_conn_count = maximum.tcp_conn_count.max(candidate_maximum.tcp_conn_count);
    maximum.udp_conn_count = maximum.udp_conn_count.max(candidate_maximum.udp_conn_count);
    maximum.process_count = maximum.process_count.max(candidate_maximum.process_count);
    maximum.net_in_speed = maximum.net_in_speed.max(candidate_maximum.net_in_speed);
    maximum.net_out_speed = maximum.net_out_speed.max(candidate_maximum.net_out_speed);
}

/// `counterDeltas`：相邻样本的 net_in/net_out 计数差之和。
/// 探针重启（uptime 回退）不计——计数器归零，差值无意义。
fn counter_deltas(states: &[proto::State]) -> (u64, u64) {
    let mut inbound = 0u64;
    let mut outbound = 0u64;
    for index in 1..states.len() {
        let previous = &states[index - 1];
        let current = &states[index];
        let continuous_boot = current.uptime >= previous.uptime;
        inbound += safe_counter_delta(
            previous.net_in_transfer,
            current.net_in_transfer,
            continuous_boot,
        );
        outbound += safe_counter_delta(
            previous.net_out_transfer,
            current.net_out_transfer,
            continuous_boot,
        );
    }
    (inbound, outbound)
}

/// `safeCounterDelta`：非连续启动 → 0；超大跳变 → 0（损坏证据）；
/// 仅接受 2^32 邻域内的 u64 回绕。
fn safe_counter_delta(previous: u64, current: u64, continuous_boot: bool) -> u64 {
    if !continuous_boot {
        return 0;
    }
    if current >= previous {
        let delta = current - previous;
        return if delta > MAX_PLAUSIBLE_DELTA {
            0
        } else {
            delta
        };
    }
    if previous > u64::MAX - (1 << 32) {
        let delta = u64::MAX - previous + current + 1;
        if delta <= MAX_PLAUSIBLE_DELTA {
            return delta;
        }
    }
    0
}

fn report_tick(kind: &str, result: StdResult<anyhow::Result<()>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::warn!(tick = kind, "rollup 执行失败: {err:#}"),
        Err(err) => tracing::warn!(tick = kind, "rollup 任务崩溃: {err}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;
    use rusqlite::OptionalExtension;

    const NODE: [u8; 16] = [9u8; 16];
    const MINUTE: i64 = MINUTE_NANOS;
    const HOUR: i64 = HOUR_NANOS;
    /// 2026-10-16T00:00:00Z，整分整小时，免去时区与取整歧义。
    const BASE: i64 = 1_792_108_800_000_000_000;

    fn state(cpu: f64, mem_used: u64, uptime: u64, net_in: u64, net_out: u64) -> proto::State {
        proto::State {
            cpu,
            mem_used,
            uptime,
            net_in_transfer: net_in,
            net_out_transfer: net_out,
            ..Default::default()
        }
    }

    fn worker(name: &str) -> (RollupWorker, Arc<StateSampleBuffer>, String) {
        let dir =
            std::env::temp_dir().join(format!("santaizi-rollup-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("panel.db").to_string_lossy().into_owned();
        let db = migrate::open_primary(&path).unwrap();
        let states = Arc::new(StateSampleBuffer::new());
        let worker = RollupWorker::new(
            db,
            RetentionPolicy::default().normalized(),
            Arc::clone(&states),
        );
        (worker, states, path)
    }

    fn rollup_rows(worker: &RollupWorker, resolution: &str) -> Vec<(i64, i64, u32, u64, u64)> {
        worker
            .db
            .read(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT window_start, window_end, sample_count, net_in_total, net_out_total \
                     FROM state_rollups WHERE resolution = ? ORDER BY window_start ASC",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![resolution], |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?.max(0) as u32,
                            row.get::<_, i64>(3)?.max(0) as u64,
                            row.get::<_, i64>(4)?.max(0) as u64,
                        ))
                    })?
                    .collect::<StdResult<Vec<_>, _>>()?;
                Ok(rows)
            })
            .unwrap()
    }

    fn rollup_payload(
        worker: &RollupWorker,
        resolution: &str,
    ) -> Option<proto::StateRollupPayload> {
        worker
            .db
            .read(|conn| {
                let payload: Option<Vec<u8>> = conn
                    .query_row(
                        "SELECT payload FROM state_rollups WHERE resolution = ? ORDER BY window_start ASC LIMIT 1",
                        rusqlite::params![resolution],
                        |row| row.get(0),
                    )
                    .optional()?;
                Ok(payload.map(|bytes| {
                    proto::StateRollupPayload::decode(bytes.as_slice()).unwrap()
                }))
            })
            .unwrap()
    }

    #[test]
    fn aggregate_states_min_avg_max_and_totals() {
        let states = vec![
            state(10.0, 100, 1000, 0, 0),
            state(30.0, 300, 1001, 500, 200),
            state(20.0, 200, 1002, 1500, 600),
        ];
        let payload = aggregate_states(&states, BASE, BASE + MINUTE).unwrap();
        assert_eq!(payload.window_start_unix_nano, BASE);
        assert_eq!(payload.window_end_unix_nano, BASE + MINUTE);
        assert_eq!(payload.sample_count, 3);
        let minimum = payload.minimum.unwrap();
        let average = payload.average.unwrap();
        let maximum = payload.maximum.unwrap();
        assert_eq!(minimum.cpu, 10.0);
        assert_eq!(minimum.mem_used, 100);
        assert_eq!(maximum.cpu, 30.0);
        assert_eq!(maximum.mem_used, 300);
        assert_eq!(average.cpu, 20.0);
        assert_eq!(average.mem_used, 200); // (100+300+200)/3 整数除
        // 连续启动，计数差直接求和。
        assert_eq!(payload.net_in_total, 1500);
        assert_eq!(payload.net_out_total, 600);
    }

    #[test]
    fn counter_deltas_ignore_reboot_and_corrupt_jumps() {
        // uptime 回退 = 重启，整段不计。
        let reboot = vec![state(1.0, 1, 500, 100, 100), state(1.0, 1, 100, 900, 900)];
        assert_eq!(counter_deltas(&reboot), (0, 0));
        // 超过 1 PiB 的跳变视为损坏。
        let corrupt = vec![state(1.0, 1, 100, 0, 0), state(1.0, 1, 101, 1 << 51, 0)];
        assert_eq!(counter_deltas(&corrupt), (0, 0));
        // u64 回绕在 2^32 邻域内被接受。
        let wrapped = vec![
            state(1.0, 1, 100, u64::MAX - 10, 0),
            state(1.0, 1, 101, 5, 0),
        ];
        assert_eq!(counter_deltas(&wrapped), (16, 0));
        // 远离 2^32 邻域的回绕拒绝。
        let far = vec![
            state(1.0, 1, 100, u64::MAX / 2, 0),
            state(1.0, 1, 101, 5, 0),
        ];
        assert_eq!(counter_deltas(&far), (0, 0));
    }

    #[test]
    fn aggregate_states_keeps_first_sample_gpu_and_temperatures() {
        // Go 语义：min/max 克隆 states[0]，只动明列字段；average 从零开始。
        // gpu 与 temperatures 因此保留首见样本，average 中为零。
        let mut first = state(50.0, 100, 10, 0, 0);
        first.gpu = 0.75;
        first.temperatures = vec![proto::StateSensorTemperature {
            name: "core".into(),
            temperature: 42.0,
        }];
        let second = state(10.0, 50, 11, 0, 0);
        let payload = aggregate_states(&[first.clone(), second], BASE, BASE + MINUTE).unwrap();
        let minimum = payload.minimum.unwrap();
        let average = payload.average.unwrap();
        assert_eq!(minimum.gpu, 0.75);
        assert_eq!(minimum.temperatures.len(), 1);
        assert_eq!(average.gpu, 0.0);
        assert!(average.temperatures.is_empty());
    }

    fn rollup_row(payload: &proto::StateRollupPayload) -> StateRollupRow {
        StateRollupRow {
            window_start: payload.window_start_unix_nano,
            window_end: payload.window_end_unix_nano,
            net_in_total: payload.net_in_total,
            net_out_total: payload.net_out_total,
            payload: prost::Message::encode_to_vec(payload),
        }
    }

    #[test]
    fn aggregate_rollups_merges_extrema_and_weighted_average() {
        let first = aggregate_states(
            &[state(10.0, 100, 1, 0, 0), state(20.0, 200, 2, 1000, 400)],
            BASE,
            BASE + MINUTE,
        )
        .unwrap();
        let second = aggregate_states(
            &[
                state(30.0, 50, 3, 5000, 100),
                state(40.0, 300, 4, 6000, 300),
            ],
            BASE + MINUTE,
            BASE + 2 * MINUTE,
        )
        .unwrap();
        let rows = vec![rollup_row(&first), rollup_row(&second)];
        let payload = aggregate_rollups(&rows, BASE, BASE + HOUR).unwrap();
        // 极值取并：cpu min 10 / max 40，mem min 50 / max 300。
        let minimum = payload.minimum.unwrap();
        let maximum = payload.maximum.unwrap();
        assert_eq!(minimum.cpu, 10.0);
        assert_eq!(minimum.mem_used, 50);
        assert_eq!(maximum.cpu, 40.0);
        assert_eq!(maximum.mem_used, 300);
        // 均值按 sample_count 加权：每行 2 个样本，平均为 (10+20+30+40)/4。
        let average = payload.average.unwrap();
        assert_eq!(average.cpu, 25.0);
        assert_eq!(payload.sample_count, 4);
        // 总量直接求和，不经 counter_deltas。
        assert_eq!(payload.net_in_total, 1000 + 1000); // first(1000) + second(1000)
        assert_eq!(payload.net_out_total, 400 + 200);
    }

    #[test]
    fn aggregate_rollups_skips_rows_without_average() {
        let mut broken = proto::StateRollupPayload {
            window_start_unix_nano: BASE,
            window_end_unix_nano: BASE + MINUTE,
            sample_count: 3,
            net_in_total: 999,
            ..Default::default()
        };
        broken.average = None;
        let rows = vec![rollup_row(&broken)];
        assert!(aggregate_rollups(&rows, BASE, BASE + HOUR).is_none());
    }

    #[test]
    fn list_window_returns_rows_in_range_with_columns() {
        let (worker, _states, _path) = worker("window");
        worker
            .db
            .write(|tx| {
                upsert_state_rollup(
                    tx,
                    &NODE,
                    RESOLUTION_1M,
                    &make_rollup(BASE, BASE + MINUTE, 10, 1),
                )?;
                upsert_state_rollup(
                    tx,
                    &NODE,
                    RESOLUTION_1M,
                    &make_rollup(BASE + MINUTE, BASE + 2 * MINUTE, 20, 2),
                )?;
                // 窗外与异分辨率各一行，必须被过滤。
                upsert_state_rollup(
                    tx,
                    &NODE,
                    RESOLUTION_1M,
                    &make_rollup(BASE + 2 * MINUTE, BASE + 3 * MINUTE, 30, 3),
                )?;
                upsert_state_rollup(
                    tx,
                    &NODE,
                    RESOLUTION_1H,
                    &make_rollup(BASE, BASE + HOUR, 999, 9),
                )?;
                Ok(())
            })
            .unwrap();

        let rows = worker
            .db
            .read(|conn| list_window(conn, &NODE, RESOLUTION_1M, BASE, BASE + 2 * MINUTE))
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].window_start, BASE);
        assert_eq!(rows[0].net_in_total, 10);
        assert_eq!(rows[0].net_out_total, 1);
        assert_eq!(rows[1].window_start, BASE + MINUTE);
        assert_eq!(rows[1].net_in_total, 20);
        // payload 仍在，供前台指标点解码。
        let decoded = proto::StateRollupPayload::decode(rows[0].payload.as_slice()).unwrap();
        assert_eq!(decoded.net_in_total, 10);
    }

    #[test]
    fn sum_direction_usage_mixes_hour_and_minute_rows() {
        let (worker, _states, _path) = worker("usage");
        worker
            .db
            .write(|tx| {
                // 两个完整小时窗。
                upsert_state_rollup(
                    tx,
                    &NODE,
                    RESOLUTION_1H,
                    &make_rollup(BASE, BASE + HOUR, 1000, 100),
                )?;
                upsert_state_rollup(
                    tx,
                    &NODE,
                    RESOLUTION_1H,
                    &make_rollup(BASE + HOUR, BASE + 2 * HOUR, 2000, 200),
                )?;
                // 分钟窗：跨小时边界的窗口必须拼边缘。
                for (offset, net_in, net_out) in [
                    (10 * MINUTE, 10u64, 1u64),
                    (30 * MINUTE, 20, 2),
                    (HOUR + 30 * MINUTE, 30, 3),
                    (2 * HOUR, 40, 4),
                ] {
                    upsert_state_rollup(
                        tx,
                        &NODE,
                        RESOLUTION_1M,
                        &make_rollup(BASE + offset, BASE + offset + MINUTE, net_in, net_out),
                    )?;
                }
                Ok(())
            })
            .unwrap();

        let read = |direction: &str, from: i64, to: i64| {
            worker
                .db
                .read(move |conn| sum_direction_usage(conn, &NODE, direction, from, to))
                .unwrap()
        };

        // [BASE+30m, BASE+1h+30m)：两端都不含完整小时 → 纯 1m 求和；
        // 端点恰好压在 end 上的 m3 行（window_start == end）不计入。
        assert_eq!(
            read(
                TRAFFIC_DIRECTION_INBOUND,
                BASE + 30 * MINUTE,
                BASE + HOUR + 30 * MINUTE
            ),
            20
        );
        assert_eq!(
            read(
                TRAFFIC_DIRECTION_OUTBOUND,
                BASE + 30 * MINUTE,
                BASE + HOUR + 30 * MINUTE
            ),
            2
        );
        // [BASE+10m, BASE+2h+10m)：中间整小时走 1h 行，两端不足一小时走 1m 行。
        assert_eq!(
            read(
                TRAFFIC_DIRECTION_INBOUND,
                BASE + 10 * MINUTE,
                BASE + 2 * HOUR + 10 * MINUTE
            ),
            2000 + 30 + 40
        );
        assert_eq!(
            read(
                TRAFFIC_DIRECTION_OUTBOUND,
                BASE + 10 * MINUTE,
                BASE + 2 * HOUR + 10 * MINUTE
            ),
            200 + 3 + 4
        );
        assert_eq!(
            read(
                TRAFFIC_DIRECTION_TOTAL,
                BASE + 10 * MINUTE,
                BASE + 2 * HOUR + 10 * MINUTE
            ),
            2070 + 207
        );
        // 空窗与逆向窗为 0。
        assert_eq!(read(TRAFFIC_DIRECTION_INBOUND, BASE, BASE), 0);
        assert_eq!(read(TRAFFIC_DIRECTION_INBOUND, BASE + HOUR, BASE), 0);
        assert_eq!(
            read(TRAFFIC_DIRECTION_INBOUND, BASE + 3 * HOUR, BASE + 4 * HOUR),
            0
        );
    }

    fn make_rollup(start: i64, end: i64, net_in: u64, net_out: u64) -> proto::StateRollupPayload {
        proto::StateRollupPayload {
            window_start_unix_nano: start,
            window_end_unix_nano: end,
            sample_count: 1,
            net_in_total: net_in,
            net_out_total: net_out,
            ..Default::default()
        }
    }

    #[test]
    fn policy_normalization_defaults() {
        let policy = RetentionPolicy::default().normalized();
        assert_eq!(policy.state_raw, 6 * HOUR_NANOS);
        assert_eq!(policy.state_one_minute, 30 * 24 * HOUR_NANOS);
        assert_eq!(policy.state_one_hour, 365 * 24 * HOUR_NANOS);
        assert_eq!(policy.observation, 30 * 24 * HOUR_NANOS);
        assert_eq!(policy.evidence, 48 * HOUR_NANOS);
        assert_eq!(policy.lifecycle, 10 * 365 * 24 * HOUR_NANOS);
        assert_eq!(policy.receipt, 7 * 24 * HOUR_NANOS);
        assert_eq!(policy.batch_size, DEFAULT_BATCH);
        assert_eq!(policy.max_runtime, DEFAULT_MAX_RUNTIME);
        assert_eq!(policy.compact_min_bytes, DEFAULT_COMPACT_MIN_BYTES);
        // auto_compact 由配置侧 BoolOrTrue 决定，规范化不插手（Go 同）。
        assert!(!policy.auto_compact);
        assert!(
            RetentionPolicy {
                auto_compact: true,
                ..RetentionPolicy::default()
            }
            .normalized()
            .auto_compact
        );
    }

    #[test]
    fn rollup_memory_before_groups_by_minute() {
        let (worker, states, _path) = worker("memory");
        // event_id 必须互异：缓冲按 event_id 去重（Go 同）。
        states.add(&[1u8; 16], &NODE, BASE + 10, &state(10.0, 100, 1, 0, 0));
        states.add(&[2u8; 16], &NODE, BASE + 20, &state(30.0, 300, 2, 500, 200));
        states.add(
            &[3u8; 16],
            &NODE,
            BASE + MINUTE + 5,
            &state(20.0, 200, 3, 100, 100),
        );
        // 晚于 cutoff 的样本必须留在缓冲里。
        states.add(
            &[4u8; 16],
            &NODE,
            BASE + 2 * MINUTE,
            &state(99.0, 999, 4, 0, 0),
        );

        worker.rollup_memory_before(BASE + 2 * MINUTE).unwrap();

        let rows = rollup_rows(&worker, RESOLUTION_1M);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (BASE, BASE + MINUTE, 2, 500, 200));
        // 单样本窗没有相邻对，计数差为 0。
        assert_eq!(rows[1], (BASE + MINUTE, BASE + 2 * MINUTE, 1, 0, 0));
        assert_eq!(states.len(), 1); // 只剩晚于 cutoff 的样本
        let payload = rollup_payload(&worker, RESOLUTION_1M).unwrap();
        assert_eq!(payload.minimum.unwrap().cpu, 10.0);
    }

    #[test]
    fn rollup_raw_window_reads_persisted_state_events() {
        let (worker, _states, _path) = worker("raw");
        // 当前摄入策略不落 STATE 事件；这里直接插一行模拟历史/迁移数据。
        let event = proto::TelemetryEvent {
            event_id: vec![1u8; 16],
            node_uuid: NODE.to_vec(),
            session_id: vec![2u8; 16],
            sequence: 1,
            event_type: proto::TelemetryEventType::State as i32,
            priority: proto::TelemetryPriority::P1Important as i32,
            collected_at_unix_nano: BASE + 10,
            payload: Some(proto::telemetry_event::Payload::State(state(
                25.0, 250, 1, 0, 0,
            ))),
            ..Default::default()
        };
        worker
            .db
            .write(|tx| {
                tx.execute(
                    "INSERT INTO telemetry_events (event_id, node_uuid, session_id, sequence, event_type, priority, collected_at, \
                     agent_uptime_nano, session_elapsed_nano, clock_untrusted, source_protocol, reliability, payload, payload_retained, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, 0, 0, 0, 0, ?8, 1, CURRENT_TIMESTAMP)",
                    rusqlite::params![
                        event.event_id,
                        event.node_uuid,
                        event.session_id,
                        event.sequence as i64,
                        event.event_type,
                        event.priority,
                        event.collected_at_unix_nano,
                        prost::Message::encode_to_vec(&event),
                    ],
                )?;
                Ok(())
            })
            .unwrap();

        worker.rollup_raw_window(BASE, BASE + MINUTE).unwrap();

        let rows = rollup_rows(&worker, RESOLUTION_1M);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, BASE);
        assert_eq!(rows[0].2, 1);
        let payload = rollup_payload(&worker, RESOLUTION_1M).unwrap();
        assert_eq!(payload.average.unwrap().mem_used, 250);
    }

    #[test]
    fn rollup_hour_window_builds_one_hour_from_minutes() {
        let (worker, _states, _path) = worker("hour");
        for index in 0..3i64 {
            let start = BASE + index * MINUTE;
            let payload = aggregate_states(
                &[state(
                    10.0 * (index + 1) as f64,
                    100 * (index + 1) as u64,
                    1,
                    0,
                    0,
                )],
                start,
                start + MINUTE,
            )
            .unwrap();
            worker
                .db
                .write(|tx| {
                    upsert_state_rollup(tx, &NODE, RESOLUTION_1M, &payload)?;
                    Ok(())
                })
                .unwrap();
        }

        worker.rollup_hour_window(BASE, BASE + HOUR).unwrap();

        let rows = rollup_rows(&worker, RESOLUTION_1H);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], (BASE, BASE + HOUR, 3, 0, 0));
        let payload = rollup_payload(&worker, RESOLUTION_1H).unwrap();
        let minimum = payload.minimum.unwrap();
        let maximum = payload.maximum.unwrap();
        assert_eq!(minimum.cpu, 10.0);
        assert_eq!(maximum.cpu, 30.0);
        assert_eq!(payload.average.unwrap().cpu, 20.0);
    }

    #[test]
    fn rollup_pending_rolls_all_three_levels() {
        let (worker, states, _path) = worker("pending");
        // 上一分钟的缓冲样本 → 1m 行。
        states.add(
            &[5u8; 16],
            &NODE,
            BASE + MINUTE - 10,
            &state(15.0, 150, 1, 0, 0),
        );
        // 上一小时的 1m 行（两个样本，计数差 700/300）→ 1h 行。
        let minute_payload = aggregate_states(
            &[state(25.0, 250, 2, 0, 0), state(25.0, 250, 3, 700, 300)],
            BASE - HOUR + MINUTE,
            BASE - HOUR + 2 * MINUTE,
        )
        .unwrap();
        worker
            .db
            .write(|tx| {
                upsert_state_rollup(tx, &NODE, RESOLUTION_1M, &minute_payload)?;
                Ok(())
            })
            .unwrap();

        // now 取 BASE 之后 1 分钟又 1 纳秒：刚过去的分钟 = [BASE, BASE+MINUTE)。
        worker.rollup_pending(BASE + MINUTE + 1).unwrap();

        // 1m 行共两条：缓冲卷出的 [BASE, BASE+MINUTE) 与预置的旧行。
        let minute_rows = rollup_rows(&worker, RESOLUTION_1M);
        assert_eq!(minute_rows.len(), 2);
        assert_eq!(minute_rows[1], (BASE, BASE + MINUTE, 1, 0, 0));
        // now 落在 BASE+MINUTE，小时窗是 [BASE-HOUR, BASE)，只含预置的 1m 行。
        let hour_rows = rollup_rows(&worker, RESOLUTION_1H);
        assert_eq!(hour_rows.len(), 1);
        assert_eq!(hour_rows[0].0, BASE - HOUR);
        assert_eq!(hour_rows[0].2, 2);
        assert_eq!(hour_rows[0].3, 700);
        assert_eq!(hour_rows[0].4, 300);
    }
}

#[cfg(test)]
mod late_state_rollup_tests {
    use super::*;
    use crate::db::migrate;
    use std::sync::Arc;

    const NODE: [u8; 16] = [0x5a; 16];
    const MINUTE: i64 = 60 * 1_000_000_000;
    /// 2026-10-16T00:00:00Z。
    const BASE: i64 = 1_792_108_800_000_000_000;

    fn worker(name: &str) -> RollupWorker {
        let dir = std::env::temp_dir().join(format!(
            "santaizi-rollup-late-{name}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("panel.db").to_string_lossy().into_owned();
        RollupWorker::new(
            migrate::open_primary(&path).unwrap(),
            RetentionPolicy::default().normalized(),
            Arc::new(StateSampleBuffer::new()),
        )
    }

    fn payload(start: i64, end: i64, samples: u32, net_in: u64) -> proto::StateRollupPayload {
        proto::StateRollupPayload {
            window_start_unix_nano: start,
            window_end_unix_nano: end,
            sample_count: samples,
            net_in_total: net_in,
            net_out_total: net_in / 2,
            ..Default::default()
        }
    }

    fn row(worker: &RollupWorker, start: i64) -> (u32, u64) {
        worker
            .db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT sample_count, net_in_total FROM state_rollups WHERE window_start = ?1",
                    [start],
                    |r| Ok((r.get::<_, u32>(0)?, r.get::<_, u64>(1)?)),
                )?)
            })
            .unwrap()
    }

    /// 晚到（样本更少）的 StateRollup 不得覆盖已卷好的窗口。
    #[test]
    fn late_partial_rollup_does_not_clobber_complete_window() {
        let worker = worker("clobber");
        worker
            .db
            .write(|tx| {
                upsert_state_rollup(
                    tx,
                    &NODE,
                    RESOLUTION_1M,
                    &payload(BASE, BASE + MINUTE, 60, 600),
                )?;
                // 乱序补传：只覆盖了窗口的一小段。
                upsert_state_rollup(
                    tx,
                    &NODE,
                    RESOLUTION_1M,
                    &payload(BASE, BASE + MINUTE, 3, 30),
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(row(&worker, BASE), (60, 600), "完整聚合必须保留");
    }

    /// 更完整的重算（样本数不劣）应当生效——否则窗口永远停在首次的劣化值。
    #[test]
    fn fuller_rollup_still_updates() {
        let worker = worker("fuller");
        worker
            .db
            .write(|tx| {
                upsert_state_rollup(
                    tx,
                    &NODE,
                    RESOLUTION_1M,
                    &payload(BASE, BASE + MINUTE, 3, 30),
                )?;
                upsert_state_rollup(
                    tx,
                    &NODE,
                    RESOLUTION_1M,
                    &payload(BASE, BASE + MINUTE, 60, 600),
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(row(&worker, BASE), (60, 600), "更完整的重算应生效");
    }

    /// 样本数相等时放行：同窗口重投（复制重试）携带的是同一份数据，
    /// 覆盖无差别；真正的劣化——样本更少——才被挡住。
    #[test]
    fn equal_sample_count_is_allowed_through() {
        let worker = worker("equal");
        worker
            .db
            .write(|tx| {
                upsert_state_rollup(
                    tx,
                    &NODE,
                    RESOLUTION_1M,
                    &payload(BASE, BASE + MINUTE, 10, 100),
                )?;
                upsert_state_rollup(
                    tx,
                    &NODE,
                    RESOLUTION_1M,
                    &payload(BASE, BASE + MINUTE, 10, 999),
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(row(&worker, BASE), (10, 999), "相等样本数应放行");
    }
}
