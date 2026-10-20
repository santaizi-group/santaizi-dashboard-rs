//! 可用性共识引擎：观测点健康/证据桶 → `availability_buckets` + 事件。
//!
//! 移植 Go `service/availability/engine.go`。核心不变量：**窗口未结束时不得判离线**。
//! 健康行常先于证据落桶（健康 ticker 每个桶为全部已指派节点入队重算），
//! 桶刚开头的 `seen=0` 只是「尚未看到」而不是「确认失联」。

use std::sync::Arc;
use std::time::Duration;

use rusqlite::{OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

use crate::db::Db;
use crate::repo::bindings::{self, PRIMARY_OBSERVER_ID};
use crate::util::time::now_nanos;

pub const HOST_STATE_UNKNOWN: &str = "unknown";
pub const HOST_STATE_ONLINE: &str = "online";
pub const HOST_STATE_OFFLINE: &str = "offline";

pub const CONNECTIVITY_UNKNOWN: &str = "unknown";
pub const CONNECTIVITY_FULL: &str = "full";
pub const CONNECTIVITY_PARTIAL: &str = "partial";
pub const CONNECTIVITY_UNAVAILABLE: &str = "unavailable";

pub const RESOLUTION_RAW: &str = "30s";
pub const RESOLUTION_SPAN: &str = "span";

const CLASSIFICATION_HOST_OFFLINE: &str = "HOST_OFFLINE";
const CLASSIFICATION_CONNECTIVITY_DEGRADED: &str = "CONNECTIVITY_DEGRADED";
const CLASSIFICATION_EVIDENCE_UNKNOWN: &str = "EVIDENCE_UNKNOWN";
const CLASSIFICATION_HEALTHY: &str = "HEALTHY";

const DEFAULT_BUCKET_SECONDS: u64 = 30;
const PROCESS_INTERVAL: Duration = Duration::from_secs(2);
const DEFAULT_QUEUE_LIMIT: usize = 200;

/// 观测点证据行：落 `availability_buckets.observer_summary` 的 JSON 数组元素。
/// 字段名与 Go `observerEvidence` 一致（面板前端与通知文案直接消费）。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObserverEvidence {
    pub observer_id: String,
    pub healthy: bool,
    pub seen: bool,
}

/// `availability_buckets` 行。
#[derive(Debug, Clone, Default)]
pub struct AvailabilityBucketRow {
    pub node_uuid: Vec<u8>,
    pub bucket_start: i64,
    pub window_end: i64,
    pub resolution: String,
    pub host_state: String,
    pub connectivity_state: String,
    pub expected_observers: u32,
    pub healthy_observers: u32,
    pub seen_observers: u32,
    pub observer_summary: Vec<u8>,
    pub revision: u64,
    pub finalized: bool,
    pub recalculated_at: i64,
}

/// `availability_incidents` 行。
#[derive(Debug, Clone, Default)]
struct IncidentRow {
    id: i64,
    current_classification: String,
    revision: u64,
    started_at: i64,
    ended_at: i64,
    observer_evidence: Vec<u8>,
}

pub struct Engine {
    db: Db,
    bucket_nanos: i64,
    minimum: u32,
}

impl Engine {
    pub fn new(db: Db, bucket_seconds: u64, minimum_observers: u32) -> Self {
        let seconds = if bucket_seconds == 0 {
            DEFAULT_BUCKET_SECONDS
        } else {
            bucket_seconds
        };
        Self {
            db,
            bucket_nanos: seconds as i64 * 1_000_000_000,
            minimum: minimum_observers.max(1),
        }
    }

    pub fn bucket_nanos(&self) -> i64 {
        self.bucket_nanos
    }

    /// `RecordPrimaryHealth`：主面板自报健康，并为当前/上一桶入队重算。
    /// 上一桶此刻已封窗，这是它获得终结重算的机会，否则证据只落在上一桶的
    /// 节点会一直停在 unknown。
    pub fn record_primary_health(&self, sampled_at_nanos: i64) -> anyhow::Result<()> {
        let bucket_start = sampled_at_nanos / self.bucket_nanos * self.bucket_nanos;
        self.db.write(|tx| {
            tx.execute(
                "INSERT INTO observer_health_buckets (observer_id, bucket_start, healthy, process_session, revision, updated_at) \
                 VALUES (?1, ?2, 1, 'primary', 1, CURRENT_TIMESTAMP) \
                 ON CONFLICT (observer_id, bucket_start) DO UPDATE SET healthy = 1, process_session = 'primary', updated_at = CURRENT_TIMESTAMP",
                rusqlite::params![PRIMARY_OBSERVER_ID, bucket_start],
            )?;
            let nodes = assigned_nodes_for_observer(tx, PRIMARY_OBSERVER_ID, bucket_start, self.bucket_nanos)?;
            for node_uuid in nodes {
                // 当前桶随健康行即时重算；上一桶此刻已封窗，见方法注释。
                for bucket in [bucket_start, bucket_start - self.bucket_nanos] {
                    if bucket <= 0 {
                        continue;
                    }
                    enqueue_recompute(tx, &node_uuid, bucket, "primary_health")?;
                }
            }
            Ok(())
        })
    }

    /// `ProcessQueue`：按桶起点升序消费重算队列。BUSY 时留下该行并停止本轮，
    /// 不把已持有的锁乘以队列长度。
    pub fn process_queue(&self, limit: usize) -> anyhow::Result<()> {
        let limit = if limit == 0 {
            DEFAULT_QUEUE_LIMIT
        } else {
            limit
        };
        let queued: Vec<(Vec<u8>, i64)> = self.db.read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT node_uuid, bucket_start FROM availability_recompute_queues \
                 ORDER BY bucket_start ASC LIMIT ?1",
            )?;
            let rows = stmt
                .query_map(rusqlite::params![limit as i64], |row| {
                    Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
                })?
                .collect::<Result<_, _>>()?;
            Ok(rows)
        })?;
        for (node_uuid, bucket_start) in queued {
            match self.recompute(&node_uuid, bucket_start, now_nanos()) {
                Ok(()) => {}
                Err(err) if crate::db::pool::is_busy_error(&err) => return Ok(()),
                Err(err) => return Err(err),
            }
            match self.db.write(|tx| {
                tx.execute(
                    "DELETE FROM availability_recompute_queues WHERE node_uuid = ? AND bucket_start = ?",
                    rusqlite::params![node_uuid, bucket_start],
                )?;
                Ok(())
            }) {
                Ok(()) => {}
                Err(err) if crate::db::pool::is_busy_error(&err) => return Ok(()),
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    /// `Recompute`：单桶共识重算。
    pub fn recompute(
        &self,
        node_uuid: &[u8],
        bucket_start: i64,
        recalculated_at_nanos: i64,
    ) -> anyhow::Result<()> {
        if node_uuid.len() != 16 || bucket_start <= 0 {
            anyhow::bail!("invalid availability bucket identity");
        }
        self.db.write(|tx| {
            let observers = observers_for_node(tx, node_uuid, bucket_start, self.bucket_nanos)?;
            let mut healthy_set = std::collections::HashSet::new();
            let mut seen_set = std::collections::HashSet::new();
            if !observers.is_empty() {
                let placeholders = question_marks(observers.len());
                let health_sql = format!(
                    "SELECT observer_id, healthy FROM observer_health_buckets \
                     WHERE bucket_start = ?1 AND observer_id IN ({placeholders})"
                );
                let mut params: Vec<&dyn rusqlite::ToSql> = vec![&bucket_start];
                for observer in &observers {
                    params.push(observer);
                }
                let mut stmt = tx.prepare(&health_sql)?;
                let rows = stmt.query_map(params.as_slice(), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? != 0))
                })?;
                for row in rows {
                    let (observer_id, healthy) = row?;
                    if healthy {
                        healthy_set.insert(observer_id);
                    }
                }
                drop(stmt);

                let path_sql = format!(
                    "SELECT observer_id, seen FROM observer_path_buckets \
                     WHERE node_uuid = ?1 AND bucket_start = ?2 AND observer_id IN ({placeholders})"
                );
                let mut params: Vec<&dyn rusqlite::ToSql> = vec![&node_uuid, &bucket_start];
                for observer in &observers {
                    params.push(observer);
                }
                let mut stmt = tx.prepare(&path_sql)?;
                let rows = stmt.query_map(params.as_slice(), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? != 0))
                })?;
                for row in rows {
                    let (observer_id, seen) = row?;
                    if seen {
                        seen_set.insert(observer_id);
                    }
                }
            }
            let mut evidence = Vec::with_capacity(observers.len());
            let (mut healthy, mut seen) = (0u32, 0u32);
            for observer in &observers {
                let is_healthy = healthy_set.contains(observer);
                // 观测点不健康时它的「没看到」不算证据。
                let is_seen = is_healthy && seen_set.contains(observer);
                if is_healthy {
                    healthy += 1;
                }
                if is_seen {
                    seen += 1;
                }
                evidence.push(ObserverEvidence {
                    observer_id: observer.clone(),
                    healthy: is_healthy,
                    seen: is_seen,
                });
            }
            let window_closed = bucket_start + self.bucket_nanos <= recalculated_at_nanos;
            let (host_state, connectivity) =
                classify(healthy, seen, self.minimum, window_closed);
            let summary = serde_json::to_vec(&evidence)?;

            let existing = load_bucket(tx, node_uuid, bucket_start)?;
            if let Some(row) = &existing
                && row.resolution == RESOLUTION_SPAN
            {
                return close_recovered_incident(tx, row, recalculated_at_nanos);
            }
            // 长记录按小时合并且互不重叠。结束时间晚于该桶的第一条 span，
            // 是唯一可能覆盖它的记录；开始时间在内存里判断，避免主键上的
            // 范围条件把查询计划带离窗口索引。
            let covering = tx
                .query_row(
                    "SELECT node_uuid, bucket_start, window_end, resolution, host_state \
                     FROM availability_buckets \
                     WHERE node_uuid = ? AND resolution = ? AND window_end > ? \
                     ORDER BY window_end ASC LIMIT 1",
                    rusqlite::params![node_uuid, RESOLUTION_SPAN, bucket_start],
                    |row| {
                        Ok(AvailabilityBucketRow {
                            node_uuid: row.get(0)?,
                            bucket_start: row.get(1)?,
                            window_end: row.get(2)?,
                            resolution: row.get(3)?,
                            host_state: row.get(4)?,
                            ..AvailabilityBucketRow::default()
                        })
                    },
                )
                .optional()?;
            if let Some(row) = &covering
                && row.bucket_start <= bucket_start
            {
                return close_recovered_incident(tx, row, recalculated_at_nanos);
            }

            let mut revision = 1u64;
            let mut changed = true;
            if let Some(row) = &existing {
                revision = row.revision;
                changed = row.host_state != host_state
                    || row.connectivity_state != connectivity
                    || row.expected_observers != observers.len() as u32
                    || row.healthy_observers != healthy
                    || row.seen_observers != seen
                    || row.observer_summary != summary;
                if changed {
                    revision += 1;
                }
            }
            let finalized = bucket_start + self.bucket_nanos
                < recalculated_at_nanos - self.bucket_nanos;
            let row = AvailabilityBucketRow {
                node_uuid: node_uuid.to_vec(),
                bucket_start,
                window_end: bucket_start + self.bucket_nanos,
                resolution: RESOLUTION_RAW.to_string(),
                host_state: host_state.to_string(),
                connectivity_state: connectivity.to_string(),
                expected_observers: observers.len() as u32,
                healthy_observers: healthy,
                seen_observers: seen,
                observer_summary: summary,
                revision,
                finalized,
                recalculated_at: recalculated_at_nanos,
            };
            tx.execute(
                "INSERT INTO availability_buckets (node_uuid, bucket_start, window_end, resolution, \
                 host_state, connectivity_state, expected_observers, healthy_observers, seen_observers, \
                 observer_summary, revision, finalized, recalculated_at, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
                 ON CONFLICT (node_uuid, bucket_start) DO UPDATE SET \
                 window_end = ?3, resolution = ?4, host_state = ?5, connectivity_state = ?6, \
                 expected_observers = ?7, healthy_observers = ?8, seen_observers = ?9, \
                 observer_summary = ?10, revision = ?11, finalized = ?12, recalculated_at = ?13, \
                 updated_at = CURRENT_TIMESTAMP",
                rusqlite::params![
                    row.node_uuid,
                    row.bucket_start,
                    row.window_end,
                    row.resolution,
                    row.host_state,
                    row.connectivity_state,
                    row.expected_observers as i64,
                    row.healthy_observers as i64,
                    row.seen_observers as i64,
                    row.observer_summary,
                    row.revision as i64,
                    row.finalized as i32,
                    row.recalculated_at,
                ],
            )?;
            if changed {
                revise_incident(tx, &row, self.bucket_nanos, recalculated_at_nanos)?;
            }
            Ok(())
        })
    }

    /// `CloseStaleIncidents`：关闭已不再描述当前主机的开放事件。
    pub fn close_stale_incidents(&self, now_nanos: i64) -> anyhow::Result<()> {
        let open: Vec<Vec<u8>> = self.db.read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT node_uuid FROM availability_incidents WHERE ended_at = 0",
            )?;
            let rows = stmt
                .query_map([], |row| row.get::<_, Vec<u8>>(0))?
                .collect::<Result<_, _>>()?;
            Ok(rows)
        })?;
        for node_uuid in open {
            self.close_stale_node(&node_uuid, now_nanos)?;
        }
        Ok(())
    }

    /// `closeStaleNode`：恢复/未知的最新桶在该桶起点关闭；已不是任何人当前
    /// 身份的节点在绑定终点关闭。
    fn close_stale_node(&self, node_uuid: &[u8], now_nanos: i64) -> anyhow::Result<()> {
        let current: i64 = self.db.read(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM server_node_bindings WHERE node_uuid = ? AND current = 1",
                [node_uuid],
                |row| row.get::<_, i64>(0),
            )
            .map_err(Into::into)
        })?;
        if current == 0 {
            let ended = self.db.read(|conn| {
                conn.query_row(
                    "SELECT COALESCE(valid_to, 0) FROM server_node_bindings \
                     WHERE node_uuid = ? ORDER BY valid_to DESC, valid_from DESC LIMIT 1",
                    [node_uuid],
                    |row| row.get::<_, Option<i64>>(0),
                )
                .optional()
                .map_err(Into::into)
            })?;
            let ended = match ended {
                Some(Some(valid_to)) if valid_to > 0 => valid_to,
                _ => now_nanos,
            };
            return self.db.write(|tx| {
                bindings::close_open_incidents(tx, node_uuid, ended)?;
                Ok(())
            });
        }
        let Some(bucket) = self.latest_closed_bucket(node_uuid, now_nanos)? else {
            return Ok(());
        };
        match incident_classification(&bucket) {
            CLASSIFICATION_HEALTHY | CLASSIFICATION_EVIDENCE_UNKNOWN => self.db.write(|tx| {
                close_recovered_incident(tx, &bucket, now_nanos)?;
                Ok(())
            }),
            _ => Ok(()),
        }
    }

    /// `LatestClosedAvailabilityBucket`：`window_end <= now` 的最新桶。
    /// 没有已结束的桶时返回 None；进行中的桶可能还没有证据，不能当作当前共识。
    pub fn latest_closed_bucket(
        &self,
        node_uuid: &[u8],
        now_nanos: i64,
    ) -> anyhow::Result<Option<AvailabilityBucketRow>> {
        self.db.read(|conn| {
            conn.query_row(
                "SELECT node_uuid, bucket_start, window_end, resolution, host_state, \
                 connectivity_state, expected_observers, healthy_observers, seen_observers, \
                 observer_summary, revision, finalized, recalculated_at \
                 FROM availability_buckets \
                 WHERE node_uuid = ? AND window_end <= ? \
                 ORDER BY window_end DESC, bucket_start DESC LIMIT 1",
                rusqlite::params![node_uuid, now_nanos],
                |row| {
                    Ok(AvailabilityBucketRow {
                        node_uuid: row.get(0)?,
                        bucket_start: row.get(1)?,
                        window_end: row.get(2)?,
                        resolution: row.get(3)?,
                        host_state: row.get(4)?,
                        connectivity_state: row.get(5)?,
                        expected_observers: row.get::<_, i64>(6)?.max(0) as u32,
                        healthy_observers: row.get::<_, i64>(7)?.max(0) as u32,
                        seen_observers: row.get::<_, i64>(8)?.max(0) as u32,
                        observer_summary: row.get::<_, Option<Vec<u8>>>(9)?.unwrap_or_default(),
                        revision: row.get::<_, i64>(10)?.max(0) as u64,
                        finalized: row.get::<_, i64>(11)? != 0,
                        recalculated_at: row.get::<_, Option<i64>>(12)?.unwrap_or(0),
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
    }

    /// `Engine.Run`：进程队列 + 主面板健康两个 ticker。
    pub async fn run(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let engine = Arc::clone(&self);
        let initial = tokio::task::spawn_blocking(move || {
            engine.record_primary_health(now_nanos())?;
            engine.close_stale_incidents(now_nanos())
        })
        .await;
        report_tick("startup", initial);

        let mut process = tokio::time::interval(PROCESS_INTERVAL);
        process.tick().await; // 首帧已由启动时的一次性执行覆盖
        let mut health = tokio::time::interval(Duration::from_nanos(self.bucket_nanos as u64));
        health.tick().await;
        loop {
            tokio::select! {
                _ = process.tick() => {
                    let engine = Arc::clone(&self);
                    let tick = tokio::task::spawn_blocking(move || {
                        let queued = engine.process_queue(DEFAULT_QUEUE_LIMIT);
                        let stale = queued.is_ok().then(|| engine.close_stale_incidents(now_nanos()));
                        queued.and(stale.unwrap_or(Ok(())))
                    })
                    .await;
                    report_tick("process", tick);
                }
                _ = health.tick() => {
                    let engine = Arc::clone(&self);
                    let tick = tokio::task::spawn_blocking(move || {
                        engine.record_primary_health(now_nanos())
                    })
                    .await;
                    report_tick("health", tick);
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        tracing::info!("可用性引擎退出");
                        return;
                    }
                }
            }
        }
    }
}

fn report_tick(kind: &str, result: Result<anyhow::Result<()>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::warn!(tick = kind, "可用性引擎执行失败: {err:#}"),
        Err(err) => tracing::warn!(tick = kind, "可用性引擎任务崩溃: {err}"),
    }
}

/// `classify`：seen>0 即在线；否则只有窗口已结束且健康观测点达标才判离线。
fn classify(
    healthy: u32,
    seen: u32,
    minimum: u32,
    window_closed: bool,
) -> (&'static str, &'static str) {
    if seen > 0 {
        if seen == healthy {
            (HOST_STATE_ONLINE, CONNECTIVITY_FULL)
        } else {
            (HOST_STATE_ONLINE, CONNECTIVITY_PARTIAL)
        }
    } else if window_closed && healthy >= minimum {
        (HOST_STATE_OFFLINE, CONNECTIVITY_UNAVAILABLE)
    } else {
        (HOST_STATE_UNKNOWN, CONNECTIVITY_UNKNOWN)
    }
}

fn incident_classification(bucket: &AvailabilityBucketRow) -> &'static str {
    if bucket.host_state == HOST_STATE_OFFLINE {
        CLASSIFICATION_HOST_OFFLINE
    } else if bucket.connectivity_state == CONNECTIVITY_PARTIAL {
        CLASSIFICATION_CONNECTIVITY_DEGRADED
    } else if bucket.host_state == HOST_STATE_UNKNOWN {
        CLASSIFICATION_EVIDENCE_UNKNOWN
    } else {
        CLASSIFICATION_HEALTHY
    }
}

/// `closeRecoveredIncident`：恢复/未知不是事件，只关闭仍开着的事件。
fn close_recovered_incident(
    tx: &Transaction<'_>,
    bucket: &AvailabilityBucketRow,
    _recalculated_at_nanos: i64,
) -> anyhow::Result<()> {
    match incident_classification(bucket) {
        CLASSIFICATION_HEALTHY | CLASSIFICATION_EVIDENCE_UNKNOWN => {
            bindings::close_open_incidents(tx, &bucket.node_uuid, bucket.bucket_start)
        }
        _ => Ok(()),
    }
}

/// `reviseIncident`：把桶的分类落成事件（开/延/关）。
fn revise_incident(
    tx: &Transaction<'_>,
    bucket: &AvailabilityBucketRow,
    bucket_nanos: i64,
    recalculated_at_nanos: i64,
) -> anyhow::Result<()> {
    let classification = incident_classification(bucket);
    let mut bucket_end = bucket.window_end;
    if bucket_end <= bucket.bucket_start {
        bucket_end = bucket.bucket_start + bucket_nanos;
    }
    let incident = lookup_incident(tx, &bucket.node_uuid, bucket.bucket_start, bucket_end)?;
    // HEALTHY 与 EVIDENCE_UNKNOWN 都不是事件：unknown（含窗口未完结的空证据桶）
    // 只关闭仍开着的事件，绝不新开，否则每个新桶都会制造一张无人消费的事件行。
    if classification == CLASSIFICATION_HEALTHY || classification == CLASSIFICATION_EVIDENCE_UNKNOWN
    {
        let Some(incident) = incident else {
            return Ok(());
        };
        if incident.started_at == bucket.bucket_start {
            return apply_incident_update(
                tx,
                &incident,
                classification,
                bucket,
                bucket_end,
                recalculated_at_nanos,
            );
        }
        if incident.ended_at == 0 || incident.ended_at > bucket.bucket_start {
            tx.execute(
                "UPDATE availability_incidents SET ended_at = ?, recalculated_at = ? WHERE id = ?",
                rusqlite::params![
                    bucket.bucket_start.to_string(),
                    recalculated_at_nanos.to_string(),
                    incident.id
                ],
            )?;
        }
        return Ok(());
    }
    let incident = match incident {
        Some(incident) => Some(incident),
        None => lookup_extendable_incident(tx, &bucket.node_uuid, bucket.bucket_start)?,
    };
    let Some(incident) = incident else {
        return create_incident(tx, bucket, classification, recalculated_at_nanos);
    };
    apply_incident_update(
        tx,
        &incident,
        classification,
        bucket,
        0,
        recalculated_at_nanos,
    )
}

fn lookup_incident(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    bucket_start: i64,
    bucket_end: i64,
) -> anyhow::Result<Option<IncidentRow>> {
    if let Some(incident) = find_incident(
        tx,
        "node_uuid = ? AND started_at = ?",
        rusqlite::params![node_uuid, bucket_start],
    )? {
        return Ok(Some(incident));
    }
    find_incident(
        tx,
        "node_uuid = ? AND started_at < ? AND (ended_at = 0 OR ended_at IS NULL OR ended_at >= ?)",
        rusqlite::params![node_uuid, bucket_start, bucket_end],
    )
}

fn lookup_extendable_incident(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    bucket_start: i64,
) -> anyhow::Result<Option<IncidentRow>> {
    if let Some(incident) = find_incident(
        tx,
        "node_uuid = ? AND (ended_at = 0 OR ended_at IS NULL)",
        rusqlite::params![node_uuid],
    )? {
        return Ok(Some(incident));
    }
    find_incident(
        tx,
        "node_uuid = ? AND ended_at = ? AND current_classification != ?",
        rusqlite::params![node_uuid, bucket_start, CLASSIFICATION_HEALTHY],
    )
}

fn find_incident(
    tx: &Transaction<'_>,
    where_clause: &str,
    params: impl rusqlite::Params,
) -> anyhow::Result<Option<IncidentRow>> {
    let sql = format!(
        "SELECT id, current_classification, revision, started_at, \
         COALESCE(ended_at, 0), observer_evidence \
         FROM availability_incidents WHERE {where_clause} ORDER BY started_at DESC LIMIT 1"
    );
    tx.query_row(&sql, params, |row| {
        Ok(IncidentRow {
            id: row.get(0)?,
            current_classification: row.get(1)?,
            revision: row.get::<_, i64>(2)?.max(0) as u64,
            started_at: row.get(3)?,
            ended_at: row.get::<_, Option<i64>>(4)?.unwrap_or(0),
            observer_evidence: row.get::<_, Option<Vec<u8>>>(5)?.unwrap_or_default(),
        })
    })
    .optional()
    .map_err(Into::into)
}

fn create_incident(
    tx: &Transaction<'_>,
    bucket: &AvailabilityBucketRow,
    classification: &str,
    recalculated_at_nanos: i64,
) -> anyhow::Result<()> {
    tx.execute(
        "INSERT INTO availability_incidents (node_uuid, initial_classification, current_classification, \
         revision, started_at, ended_at, recalculated_at, reason, observer_evidence, created_at, updated_at) \
         VALUES (?1, ?2, ?2, 1, ?3, 0, ?4, 'availability evidence', ?5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        rusqlite::params![
            bucket.node_uuid,
            classification,
            bucket.bucket_start.to_string(),
            recalculated_at_nanos.to_string(),
            bucket.observer_summary,
        ],
    )?;
    let incident_id = tx.last_insert_rowid();
    tx.execute(
        "INSERT INTO incident_revisions (incident_id, revision, classification, reason, evidence, recalculated_at, created_at) \
         VALUES (?1, 1, ?2, 'availability evidence', ?3, ?4, CURRENT_TIMESTAMP)",
        rusqlite::params![
            incident_id,
            classification,
            bucket.observer_summary,
            recalculated_at_nanos.to_string(),
        ],
    )?;
    Ok(())
}

fn apply_incident_update(
    tx: &Transaction<'_>,
    incident: &IncidentRow,
    classification: &str,
    bucket: &AvailabilityBucketRow,
    ended_at: i64,
    recalculated_at_nanos: i64,
) -> anyhow::Result<()> {
    let same_class = incident.current_classification == classification;
    let same_evidence = incident.observer_evidence == bucket.observer_summary;
    if same_class && same_evidence && incident.ended_at == ended_at {
        return Ok(());
    }
    if same_class && same_evidence {
        tx.execute(
            "UPDATE availability_incidents SET ended_at = ?, recalculated_at = ? WHERE id = ?",
            rusqlite::params![
                ended_at.to_string(),
                recalculated_at_nanos.to_string(),
                incident.id
            ],
        )?;
        return Ok(());
    }
    let revision = incident.revision + 1;
    tx.execute(
        "UPDATE availability_incidents SET revision = ?, current_classification = ?, ended_at = ?, \
         recalculated_at = ?, observer_evidence = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        rusqlite::params![
            revision as i64,
            classification,
            ended_at.to_string(),
            recalculated_at_nanos.to_string(),
            bucket.observer_summary,
            incident.id,
        ],
    )?;
    // 迟到的证据纠正与常规证据都要留修订痕，reason 只是区分来源。
    let reason = if same_class {
        "availability evidence"
    } else {
        "late evidence correction"
    };
    tx.execute(
        "INSERT INTO incident_revisions (incident_id, revision, classification, reason, evidence, recalculated_at, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, CURRENT_TIMESTAMP)",
        rusqlite::params![
            incident.id,
            revision as i64,
            classification,
            reason,
            bucket.observer_summary,
            recalculated_at_nanos.to_string(),
        ],
    )?;
    Ok(())
}

/// 某观测点在该桶时刻已指派的节点集合。
fn assigned_nodes_for_observer(
    tx: &Transaction<'_>,
    observer_id: &str,
    bucket_start: i64,
    bucket_nanos: i64,
) -> anyhow::Result<Vec<Vec<u8>>> {
    let mut stmt = tx.prepare(
        "SELECT DISTINCT node_uuid FROM observer_assignments \
         WHERE observer_id = ? AND valid_from < ? AND (valid_to = 0 OR valid_to > ?)",
    )?;
    let rows = stmt
        .query_map(
            rusqlite::params![observer_id, bucket_start + bucket_nanos, bucket_start],
            |row| row.get::<_, Vec<u8>>(0),
        )?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

/// 某节点在该桶时刻已指派的观测点集合（排序保证证据行稳定）。
fn observers_for_node(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    bucket_start: i64,
    bucket_nanos: i64,
) -> anyhow::Result<Vec<String>> {
    let mut stmt = tx.prepare(
        "SELECT DISTINCT observer_id FROM observer_assignments \
         WHERE node_uuid = ? AND valid_from < ? AND (valid_to = 0 OR valid_to > ?) \
         ORDER BY observer_id ASC",
    )?;
    let mut rows: Vec<String> = stmt
        .query_map(
            rusqlite::params![node_uuid, bucket_start + bucket_nanos, bucket_start],
            |row| row.get::<_, String>(0),
        )?
        .collect::<Result<_, _>>()?;
    // Go 用 map + sort.Strings；DISTINCT 已去重，这里只补排序稳定性。
    rows.sort();
    rows.dedup();
    Ok(rows)
}

fn load_bucket(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    bucket_start: i64,
) -> anyhow::Result<Option<AvailabilityBucketRow>> {
    tx.query_row(
        "SELECT node_uuid, bucket_start, window_end, resolution, host_state, \
         connectivity_state, expected_observers, healthy_observers, seen_observers, \
         observer_summary, revision, finalized, recalculated_at \
         FROM availability_buckets WHERE node_uuid = ? AND bucket_start = ?",
        rusqlite::params![node_uuid, bucket_start],
        |row| {
            Ok(AvailabilityBucketRow {
                node_uuid: row.get(0)?,
                bucket_start: row.get(1)?,
                window_end: row.get(2)?,
                resolution: row.get(3)?,
                host_state: row.get(4)?,
                connectivity_state: row.get(5)?,
                expected_observers: row.get::<_, i64>(6)?.max(0) as u32,
                healthy_observers: row.get::<_, i64>(7)?.max(0) as u32,
                seen_observers: row.get::<_, i64>(8)?.max(0) as u32,
                observer_summary: row.get::<_, Option<Vec<u8>>>(9)?.unwrap_or_default(),
                revision: row.get::<_, i64>(10)?.max(0) as u64,
                finalized: row.get::<_, i64>(11)? != 0,
                recalculated_at: row.get::<_, Option<i64>>(12)?.unwrap_or(0),
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn enqueue_recompute(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    bucket_start: i64,
    reason: &str,
) -> anyhow::Result<()> {
    if bucket_start <= 0 {
        return Ok(());
    }
    tx.execute(
        "INSERT INTO availability_recompute_queues (node_uuid, bucket_start, reason, created_at, updated_at) \
         VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT (node_uuid, bucket_start) DO UPDATE SET reason = ?3, updated_at = CURRENT_TIMESTAMP",
        rusqlite::params![node_uuid, bucket_start, reason],
    )?;
    Ok(())
}

fn question_marks(count: usize) -> String {
    let mut marks = String::with_capacity(count * 2);
    for index in 0..count {
        if index > 0 {
            marks.push(',');
        }
        marks.push('?');
    }
    marks
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;

    const NODE: [u8; 16] = [7u8; 16];
    const BUCKET: i64 = 30_000_000_000;

    fn engine(name: &str) -> (Engine, String) {
        let dir =
            std::env::temp_dir().join(format!("santaizi-avail-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("panel.db").to_string_lossy().into_owned();
        let db = migrate::open_primary(&path).unwrap();
        (Engine::new(db, 30, 1), path)
    }

    fn bind(engine: &Engine, node: &[u8; 16], now: i64) {
        engine
            .db
            .write(|tx| {
                tx.execute(
                    "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) \
                     VALUES (1, 'n', 't', X'00', 1, 0)",
                    [],
                )?;
                bindings::bind_server_node_for_protocol(tx, 1, node, now, true)?;
                // 主面板每个桶都自报健康：没有健康行的观测点不参与共识。
                tx.execute(
                    "INSERT INTO observer_health_buckets (observer_id, bucket_start, healthy, revision) \
                     VALUES (?1, ?2, 1, 1) \
                     ON CONFLICT (observer_id, bucket_start) DO UPDATE SET healthy = 1",
                    rusqlite::params![PRIMARY_OBSERVER_ID, now / BUCKET * BUCKET],
                )?;
                Ok(())
            })
            .unwrap();
    }

    fn observe(engine: &Engine, observer: &str, node: &[u8; 16], bucket: i64, at: i64) {
        engine
            .db
            .write(|tx| {
                tx.execute(
                    "INSERT INTO observer_health_buckets (observer_id, bucket_start, healthy, revision) \
                     VALUES (?1, ?2, 1, 1) \
                     ON CONFLICT (observer_id, bucket_start) DO UPDATE SET healthy = 1",
                    rusqlite::params![observer, bucket],
                )?;
                tx.execute(
                    "INSERT INTO observer_path_buckets (node_uuid, observer_id, bucket_start, seen, last_seen_at, revision) \
                     VALUES (?1, ?2, ?3, 1, ?4, 1) \
                     ON CONFLICT (node_uuid, observer_id, bucket_start) DO UPDATE SET seen = 1, last_seen_at = ?4",
                    rusqlite::params![node, observer, bucket, at],
                )?;
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn closed_window_without_evidence_is_offline() {
        let (engine, path) = engine("closed-window");
        bind(&engine, &NODE, BUCKET);
        let now = BUCKET + BUCKET + 1;
        engine.recompute(&NODE, BUCKET, now).unwrap();
        let bucket = engine
            .latest_closed_bucket(&NODE, now)
            .unwrap()
            .expect("bucket");
        assert_eq!(bucket.host_state, HOST_STATE_OFFLINE);
        assert_eq!(bucket.connectivity_state, CONNECTIVITY_UNAVAILABLE);
        assert_eq!(bucket.expected_observers, 1);
        assert_eq!(bucket.seen_observers, 0);
        let incident = engine
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM availability_incidents WHERE node_uuid = ? AND ended_at = 0",
                    [&NODE],
                    |row| row.get::<_, i64>(0),
                )
                    .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(incident, 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_window_without_evidence_stays_unknown() {
        let (engine, path) = engine("open-window");
        bind(&engine, &NODE, BUCKET);
        // 窗口尚未结束：seen=0 只是「尚未看到」。
        engine.recompute(&NODE, BUCKET, BUCKET + 1).unwrap();
        let bucket = engine
            .latest_closed_bucket(&NODE, BUCKET + BUCKET + 1)
            .unwrap()
            .expect("bucket");
        assert_eq!(bucket.host_state, HOST_STATE_UNKNOWN);
        assert_eq!(bucket.connectivity_state, CONNECTIVITY_UNKNOWN);
        let incidents: i64 = engine
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM availability_incidents WHERE node_uuid = ?",
                    [&NODE],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(incidents, 0);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn seen_by_primary_is_online_full() {
        let (engine, path) = engine("seen");
        bind(&engine, &NODE, BUCKET);
        observe(&engine, PRIMARY_OBSERVER_ID, &NODE, BUCKET, BUCKET + 5);
        engine.recompute(&NODE, BUCKET, BUCKET + 1).unwrap();
        let bucket = engine
            .latest_closed_bucket(&NODE, BUCKET + BUCKET)
            .unwrap()
            .expect("bucket");
        assert_eq!(bucket.host_state, HOST_STATE_ONLINE);
        assert_eq!(bucket.connectivity_state, CONNECTIVITY_FULL);
        let summary: Vec<ObserverEvidence> =
            serde_json::from_slice(&bucket.observer_summary).unwrap();
        assert_eq!(summary.len(), 1);
        assert!(summary[0].healthy && summary[0].seen);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unhealthy_observer_does_not_offline_the_host() {
        let (engine, path) = engine("unhealthy");
        bind(&engine, &NODE, BUCKET);
        engine
            .db
            .write(|tx| {
                tx.execute(
                    "UPDATE observer_health_buckets SET healthy = 0 \
                     WHERE observer_id = 'primary' AND bucket_start = ?1",
                    [BUCKET],
                )?;
                Ok(())
            })
            .unwrap();
        engine
            .recompute(&NODE, BUCKET, BUCKET + BUCKET + 1)
            .unwrap();
        let bucket = engine
            .latest_closed_bucket(&NODE, BUCKET + BUCKET + 1)
            .unwrap()
            .expect("bucket");
        assert_eq!(bucket.host_state, HOST_STATE_UNKNOWN);
        assert_eq!(bucket.healthy_observers, 0);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn recovery_closes_open_incident_at_healthy_bucket_start() {
        let (engine, path) = engine("recovery");
        bind(&engine, &NODE, BUCKET);
        let next = BUCKET + BUCKET;
        let now = next + 1;
        engine.recompute(&NODE, BUCKET, now).unwrap();
        observe(&engine, PRIMARY_OBSERVER_ID, &NODE, next, next + 5);
        engine.recompute(&NODE, next, now).unwrap();
        let incident = engine
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT COALESCE(ended_at, 0) FROM availability_incidents WHERE node_uuid = ?",
                    [&NODE],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        // 恢复桶自身健康：事件关闭到恢复桶的起点（Go 同序）。
        assert_eq!(incident, next);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn queue_is_consumed_in_bucket_order() {
        let (engine, path) = engine("queue");
        bind(&engine, &NODE, BUCKET);
        let later = BUCKET + BUCKET;
        engine
            .db
            .write(|tx| {
                enqueue_recompute(tx, &NODE, later, "observation")?;
                enqueue_recompute(tx, &NODE, BUCKET, "observation")?;
                Ok(())
            })
            .unwrap();
        engine.process_queue(200).unwrap();
        let left: i64 = engine
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM availability_recompute_queues",
                    [],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(left, 0);
        let buckets: i64 = engine
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM availability_buckets WHERE node_uuid = ?",
                    [&NODE],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(buckets, 2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn primary_health_enqueues_current_and_previous_bucket() {
        let (engine, path) = engine("primary-health");
        bind(&engine, &NODE, BUCKET);
        engine.record_primary_health(BUCKET + 10).unwrap();
        let queued: Vec<i64> = engine
            .db
            .read(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT bucket_start FROM availability_recompute_queues ORDER BY bucket_start",
                )?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, i64>(0))?
                    .collect::<Result<_, _>>()?;
                Ok(rows)
            })
            .unwrap();
        assert_eq!(queued, vec![BUCKET]);
        let healthy: i64 = engine
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT healthy FROM observer_health_buckets WHERE observer_id = 'primary'",
                    [],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(healthy, 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn stale_incident_closes_when_binding_is_gone() {
        let (engine, path) = engine("stale-binding");
        bind(&engine, &NODE, BUCKET);
        let now = BUCKET + BUCKET + 1;
        engine.recompute(&NODE, BUCKET, now).unwrap();
        engine
            .db
            .write(|tx| {
                tx.execute(
                    "UPDATE server_node_bindings SET current = 0, valid_to = ? WHERE node_uuid = ?",
                    rusqlite::params![(BUCKET + BUCKET).to_string(), NODE],
                )?;
                Ok(())
            })
            .unwrap();
        engine.close_stale_incidents(now).unwrap();
        let ended: i64 = engine
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT COALESCE(ended_at, 0) FROM availability_incidents WHERE node_uuid = ?",
                    [&NODE],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(ended, BUCKET + BUCKET);
        let _ = std::fs::remove_file(&path);
    }
}
