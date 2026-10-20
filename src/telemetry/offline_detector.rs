//! V2 离线检测：只认可用性桶共识。
//!
//! 移植 Go `singleton.detectV2OfflineServers` / `inspectV2Consensus` /
//! `createV2OfflineHistoryTx`（`offline_detector_v2.go`）。V1 的 LastSeenAt
//! 超时判定不移植：本仓只接 V2 协议。共识 = 全部已分配且健康的观测点都看不到，
//! 且持续达到阈值；只在**已封窗**的桶上进行（进行中的桶 seen=0 不代表失联）。

use std::time::{Duration, Instant};

use rusqlite::{OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

use crate::availability::engine::HOST_STATE_OFFLINE;
use crate::config::Config;
use crate::db::Db;
use crate::telemetry::offline::{self, OfflineHistoryPolicy, RuntimeOfflineFields};
use crate::util::time::now_nanos;

const V2_PROTOCOL: &str = "v2";
const STATUS_RECOVERING: &str = "recovering";
const STATUS_OFFLINE: &str = "offline";
const STATUS_ONLINE: &str = "online";

const MIN_THRESHOLD_SECONDS: u64 = 10;
const MIN_INTERVAL_SECONDS: u64 = 5;

/// 共识结论。`latest_offline=false` 时其余字段无意义。
#[derive(Debug, Clone, Default)]
pub struct V2Consensus {
    pub latest_offline: bool,
    pub since_nanos: i64,
    pub last_seen_nanos: i64,
    pub observer_summary: Vec<u8>,
    pub duration_nanos: i64,
}

/// 观测点证据行（`observer_summary` JSON 的元素），字段与 Go `v2ObserverEvidence` 一致。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObserverEvidenceRow {
    pub observer_id: String,
    pub healthy: bool,
    pub seen: bool,
}

/// 一次确认恢复（通知消费）。
#[derive(Debug, Clone)]
pub struct RecoveryNotice {
    pub server_id: i64,
    pub offline_seconds: i64,
    pub last_ip: String,
    pub recovered_ip: String,
}

/// 一次确认离线。通知（Phase 6）消费它。
#[derive(Debug, Clone)]
pub struct OfflineNotice {
    pub server_id: i64,
    pub history_id: i64,
    pub started_at_nanos: i64,
    pub last_seen_nanos: i64,
    pub observer_summary: Vec<u8>,
}

/// `server_runtimes` 候选行：检测只需要协议/身份/离线指针/恢复三件套。
#[derive(Debug, Clone)]
struct RuntimeCandidate {
    server_id: i64,
    status: String,
    protocol: String,
    current_node_uuid: Vec<u8>,
    current_offline_id: i64,
    last_boot_time: u64,
    last_uptime: u64,
    last_ip: String,
}

pub struct V2OfflineDetector {
    db: Db,
    notifier: Option<std::sync::Arc<crate::notify::Notifier>>,
    policy: OfflineHistoryPolicy,
    bucket_nanos: i64,
    threshold_nanos: i64,
    interval: Duration,
    started_at: Instant,
}

impl V2OfflineDetector {
    pub fn from_conf(db: Db, conf: &Config) -> Self {
        Self::with_notifier(db, conf, None)
    }

    /// 带通知引擎的构造（生产接线用）。
    pub fn with_notifier(
        db: Db,
        conf: &Config,
        notifier: Option<std::sync::Arc<crate::notify::Notifier>>,
    ) -> Self {
        let bucket_seconds = conf.telemetry.availability_bucket_seconds.max(1);
        let threshold_seconds = conf.offline_threshold_seconds.max(MIN_THRESHOLD_SECONDS);
        let interval_seconds = conf
            .offline_check_interval_seconds
            .max(MIN_INTERVAL_SECONDS);
        Self {
            db,
            notifier,
            policy: OfflineHistoryPolicy::from_conf(conf),
            bucket_nanos: bucket_seconds as i64 * 1_000_000_000,
            threshold_nanos: threshold_seconds as i64 * 1_000_000_000,
            interval: Duration::from_secs(interval_seconds),
            started_at: Instant::now(),
        }
    }

    /// `detectV2OfflineServers`：扫描 V2 运行态，按共识开/关离线历史。
    /// 返回（离线通知, 恢复通知）。
    pub fn detect(
        &self,
        now_nanos: i64,
        threshold_nanos: i64,
        bucket_nanos: i64,
    ) -> anyhow::Result<(Vec<OfflineNotice>, Vec<RecoveryNotice>)> {
        if !self.policy.enabled {
            return Ok((Vec::new(), Vec::new()));
        }
        // 阈值不得低于一个桶：否则桶本身还没封窗就要求判离线。
        let threshold = threshold_nanos.max(bucket_nanos);
        let limit = consensus_limit(threshold, bucket_nanos);
        let runtimes = self.db.read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT server_id, status, protocol, current_node_uuid, current_offline_id, \
                 last_boot_time, last_uptime, last_ip FROM server_runtimes WHERE protocol = ?",
            )?;
            let rows = stmt
                .query_map([V2_PROTOCOL], |row| {
                    Ok(RuntimeCandidate {
                        server_id: row.get(0)?,
                        status: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                        protocol: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                        current_node_uuid: row.get::<_, Option<Vec<u8>>>(3)?.unwrap_or_default(),
                        current_offline_id: row.get::<_, Option<i64>>(4)?.unwrap_or(0),
                        last_boot_time: row.get::<_, Option<i64>>(5)?.unwrap_or(0).max(0) as u64,
                        last_uptime: row.get::<_, Option<i64>>(6)?.unwrap_or(0).max(0) as u64,
                        last_ip: row.get::<_, Option<String>>(7)?.unwrap_or_default(),
                    })
                })?
                .collect::<Result<Vec<RuntimeCandidate>, _>>()?;
            Ok(rows)
        })?;
        let mut notices = Vec::new();
        let mut recoveries = Vec::new();
        for runtime in runtimes {
            if runtime.status == STATUS_RECOVERING || runtime.current_node_uuid.len() != 16 {
                continue;
            }
            let verdict =
                self.inspect_consensus(&runtime.current_node_uuid, now_nanos, bucket_nanos, limit)?;
            if verdict.latest_offline {
                if runtime.current_offline_id == 0
                    && verdict.duration_nanos >= threshold
                    && let Some(notice) =
                        self.create_history(runtime.server_id, now_nanos, threshold, bucket_nanos)?
                {
                    notices.push(notice);
                }
                continue;
            }
            if runtime.current_offline_id != 0
                && let Some(recovery) = self.close_history(runtime.server_id, now_nanos)?
            {
                recoveries.push(recovery);
            }
        }
        Ok((notices, recoveries))
    }

    /// `inspectV2Consensus`：在已封窗桶上找最近一段持续 offline 的起点。
    pub fn inspect_consensus(
        &self,
        node_uuid: &[u8],
        now_nanos: i64,
        bucket_nanos: i64,
        limit: usize,
    ) -> anyhow::Result<V2Consensus> {
        self.db
            .read(|conn| inspect_consensus_tx(conn, node_uuid, now_nanos, bucket_nanos, limit))
    }

    /// `createV2OfflineHistoryTx`：事务内二次校验运行态与共识后开记录。
    pub fn create_history(
        &self,
        server_id: i64,
        now_nanos: i64,
        threshold_nanos: i64,
        bucket_nanos: i64,
    ) -> anyhow::Result<Option<OfflineNotice>> {
        self.db.write(|tx| {
            let Some(current) = load_runtime_tx(tx, server_id)? else {
                return Ok(None);
            };
            if current.protocol != V2_PROTOCOL
                || current.current_offline_id != 0
                || current.status == STATUS_RECOVERING
                || current.current_node_uuid.len() != 16
            {
                return Ok(None);
            }
            let limit = consensus_limit(threshold_nanos, bucket_nanos);
            let fresh = inspect_consensus_tx(
                tx,
                &current.current_node_uuid,
                now_nanos,
                bucket_nanos,
                limit,
            )?;
            if !fresh.latest_offline || fresh.duration_nanos < threshold_nanos {
                return Ok(None);
            }
            let last_seen = if fresh.last_seen_nanos > 0 {
                fresh.last_seen_nanos
            } else {
                fresh.since_nanos
            };
            tx.execute(
                "INSERT INTO server_offline_histories (server_id, started_at, detected_at, status, \
                 reason, threshold_seconds, last_seen_at, last_boot_time, last_uptime, last_ip, \
                 created_at, updated_at) \
                 VALUES (?1, ?2, ?3, 'open', ?4, ?5, ?6, ?7, ?8, ?9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                rusqlite::params![
                    server_id,
                    crate::util::time::unix_nanos_to_sqlite(fresh.since_nanos),
                    crate::util::time::unix_nanos_to_sqlite(now_nanos),
                    offline::OFFLINE_REASON_UNKNOWN,
                    threshold_nanos / 1_000_000_000,
                    crate::util::time::unix_nanos_to_sqlite(last_seen),
                    current.last_boot_time as i64,
                    current.last_uptime as i64,
                    current.last_ip,
                ],
            )?;
            let history_id = tx.last_insert_rowid();
            tx.execute(
                "UPDATE server_runtimes SET status = ?1, last_offline_at = ?2, current_offline_id = ?3, \
                 updated_at = CURRENT_TIMESTAMP WHERE server_id = ?4",
                rusqlite::params![
                    STATUS_OFFLINE,
                    crate::util::time::unix_nanos_to_sqlite(now_nanos),
                    history_id,
                    server_id,
                ],
            )?;
            Ok(Some(OfflineNotice {
                server_id,
                history_id,
                started_at_nanos: fresh.since_nanos,
                last_seen_nanos: last_seen,
                observer_summary: fresh.observer_summary,
            }))
        })
    }

    /// `CloseOfflineHistory(rt, nil, nil, now)`：共识恢复后关闭记录并把运行态置回在线。
    fn close_history(
        &self,
        server_id: i64,
        now_nanos: i64,
    ) -> anyhow::Result<Option<RecoveryNotice>> {
        self.db.write(|tx| {
            let Some(current) = load_runtime_tx(tx, server_id)? else {
                return Ok(None);
            };
            if current.current_offline_id == 0 {
                return Ok(None);
            }
            let started_text: Option<String> = tx
                .query_row(
                    "SELECT started_at FROM server_offline_histories WHERE id = ?1",
                    [current.current_offline_id],
                    |row| row.get(0),
                )
                .unwrap_or(None);
            let offline_started_at: i64 = started_text
                .as_deref()
                .and_then(crate::util::time::sqlite_to_unix_nanos)
                .unwrap_or(0);
            let closed = offline::close_offline_history_tx(
                tx,
                server_id,
                &RuntimeOfflineFields {
                    current_offline_id: current.current_offline_id,
                    last_boot_time: current.last_boot_time,
                    last_uptime: current.last_uptime,
                    last_ip: current.last_ip.clone(),
                },
                None,
                None,
                now_nanos,
                self.policy,
            )?;
            if closed.is_some() {
                tx.execute(
                    "UPDATE server_runtimes SET status = ?1, last_online_at = ?2, \
                     updated_at = CURRENT_TIMESTAMP WHERE server_id = ?3",
                    rusqlite::params![
                        STATUS_ONLINE,
                        crate::util::time::unix_nanos_to_sqlite(now_nanos),
                        server_id,
                    ],
                )?;
                return Ok(Some(RecoveryNotice {
                    server_id,
                    offline_seconds: (now_nanos - offline_started_at).max(0) / 1_000_000_000,
                    last_ip: current.last_ip.clone(),
                    recovered_ip: String::new(),
                }));
            }
            Ok(None)
        })
    }

    /// `offlineDetectorLoop`：按间隔检测；启动宽限期内不判离线，
    /// 避免重启瞬间把全部服务器误判为失联。
    pub async fn run(self: std::sync::Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.tick().await; // 首帧即启动时刻，落在宽限期内，直接跳过
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if self.started_at.elapsed() <= self.interval * 2 {
                        continue;
                    }
                    let detector = std::sync::Arc::clone(&self);
                    let outcome = tokio::task::spawn_blocking(move || {
                        detector.detect(now_nanos(), detector.threshold_nanos, detector.bucket_nanos)
                    })
                    .await;
                    match outcome {
                        Ok(Ok((notices, recoveries))) => {
                            for notice in notices {
                                tracing::info!(
                                    server_id = notice.server_id,
                                    history_id = notice.history_id,
                                    "V2 共识判定离线"
                                );
                                if let Some(notifier) = &self.notifier {
                                    notifier
                                        .notify_offline(
                                            notice.server_id,
                                            notice.started_at_nanos,
                                            notice.last_seen_nanos,
                                            String::new(),
                                        )
                                        .await;
                                }
                            }
                            for recovery in recoveries {
                                tracing::info!(server_id = recovery.server_id, "V2 共识恢复");
                                if let Some(notifier) = &self.notifier {
                                    notifier
                                        .notify_recovery(
                                            recovery.server_id,
                                            recovery.offline_seconds,
                                            recovery.last_ip.clone(),
                                            recovery.recovered_ip.clone(),
                                        )
                                        .await;
                                }
                            }
                        }
                        Ok(Err(err)) => tracing::warn!("V2 离线检测失败: {err:#}"),
                        Err(err) => tracing::warn!("V2 离线检测任务崩溃: {err}"),
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        tracing::info!("V2 离线检测退出");
                        return;
                    }
                }
            }
        }
    }
}

/// `v2OfflineThreshold` + `limit`：至少看 3 个桶，覆盖阈值跨度。
fn consensus_limit(threshold_nanos: i64, bucket_nanos: i64) -> usize {
    if bucket_nanos <= 0 {
        return 3;
    }
    ((threshold_nanos / bucket_nanos) as usize + 2).max(3)
}

fn inspect_consensus_tx(
    conn: &rusqlite::Connection,
    node_uuid: &[u8],
    now_nanos: i64,
    bucket_nanos: i64,
    limit: usize,
) -> anyhow::Result<V2Consensus> {
    // 只在已封窗桶上做共识：进行中的桶 seen=0 不代表失联。
    let mut stmt = conn.prepare(
        "SELECT bucket_start, host_state, observer_summary FROM availability_buckets \
         WHERE node_uuid = ? AND window_end <= ? \
         ORDER BY window_end DESC, bucket_start DESC LIMIT ?",
    )?;
    let buckets: Vec<(i64, String, Vec<u8>)> = stmt
        .query_map(
            rusqlite::params![node_uuid, now_nanos, limit as i64],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?.unwrap_or_default(),
                ))
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    let Some(latest) = buckets.first() else {
        return Ok(V2Consensus::default());
    };
    if latest.1 != HOST_STATE_OFFLINE {
        return Ok(V2Consensus::default());
    }
    let mut since = latest.0;
    for bucket in &buckets {
        if bucket.1 != HOST_STATE_OFFLINE {
            break;
        }
        since = bucket.0;
    }
    let last_seen = last_seen_before_bucket(conn, node_uuid, since, bucket_nanos)?;
    Ok(V2Consensus {
        latest_offline: true,
        since_nanos: since,
        last_seen_nanos: last_seen,
        observer_summary: latest.2.clone(),
        duration_nanos: now_nanos - since,
    })
}

/// `lastSeenBeforeBucket`：离线起点前最后一次上报时刻。
/// 先看上一桶的 path 证据，再回退到更早的任意桶，都没有则用起点本身。
fn last_seen_before_bucket(
    conn: &rusqlite::Connection,
    node_uuid: &[u8],
    since_nanos: i64,
    bucket_nanos: i64,
) -> anyhow::Result<i64> {
    let previous_start = since_nanos - bucket_nanos;
    let exact = max_last_seen(
        conn,
        "SELECT COALESCE(MAX(last_seen_at), 0) FROM observer_path_buckets \
         WHERE node_uuid = ? AND bucket_start = ? AND seen = 1",
        rusqlite::params![node_uuid, previous_start],
    )?;
    if exact > 0 {
        return Ok(exact);
    }
    let fallback = max_last_seen(
        conn,
        "SELECT COALESCE(MAX(last_seen_at), 0) FROM observer_path_buckets \
         WHERE node_uuid = ? AND bucket_start < ? AND seen = 1",
        rusqlite::params![node_uuid, since_nanos],
    )?;
    if fallback > 0 {
        return Ok(fallback);
    }
    Ok(since_nanos)
}

fn max_last_seen(
    conn: &rusqlite::Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> anyhow::Result<i64> {
    conn.query_row(sql, params, |row| row.get::<_, Option<i64>>(0))
        .optional()
        .map(|value| value.flatten().unwrap_or(0))
        .map_err(Into::into)
}

fn load_runtime_tx(
    tx: &Transaction<'_>,
    server_id: i64,
) -> anyhow::Result<Option<RuntimeCandidate>> {
    tx.query_row(
        "SELECT server_id, status, protocol, current_node_uuid, current_offline_id, \
         last_boot_time, last_uptime, last_ip FROM server_runtimes WHERE server_id = ?",
        [server_id],
        |row| {
            Ok(RuntimeCandidate {
                server_id: row.get(0)?,
                status: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                protocol: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                current_node_uuid: row.get::<_, Option<Vec<u8>>>(3)?.unwrap_or_default(),
                current_offline_id: row.get::<_, Option<i64>>(4)?.unwrap_or(0),
                last_boot_time: row.get::<_, Option<i64>>(5)?.unwrap_or(0).max(0) as u64,
                last_uptime: row.get::<_, Option<i64>>(6)?.unwrap_or(0).max(0) as u64,
                last_ip: row.get::<_, Option<String>>(7)?.unwrap_or_default(),
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::availability::engine::{RESOLUTION_RAW, RESOLUTION_SPAN};
    use crate::db::migrate;

    const NODE: [u8; 16] = [5u8; 16];
    const BUCKET: i64 = 30_000_000_000;

    fn detector(name: &str) -> (V2OfflineDetector, String) {
        let dir =
            std::env::temp_dir().join(format!("santaizi-v2off-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("panel.db").to_string_lossy().into_owned();
        let db = migrate::open_primary(&path).unwrap();
        let conf = Config {
            enable_offline_history: true,
            offline_threshold_seconds: 60,
            offline_check_interval_seconds: 10,
            offline_merge_gap_seconds: 0,
            ..Config::default()
        };
        (V2OfflineDetector::from_conf(db, &conf), path)
    }

    fn seed_runtime(detector: &V2OfflineDetector, status: &str, node: Option<&[u8; 16]>) {
        detector
            .db
            .write(|tx| {
                tx.execute(
                    "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) \
                     VALUES (1, 'n', 't', X'00', 1, 0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO server_runtimes (server_id, status, protocol, current_node_uuid, \
                     current_offline_id, last_boot_time, last_uptime, last_ip, last_seen_at, \
                     last_collected_at, last_received_at, created_at, updated_at) \
                     VALUES (1, ?1, 'v2', ?2, 0, 111, 222, '10.0.0.1', ?3, ?3, ?3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                    rusqlite::params![
                        status,
                        node.map(|node| node.to_vec()),
                        crate::util::time::unix_nanos_to_sqlite(BUCKET + 5),
                    ],
                )?;
                Ok(())
            })
            .unwrap();
    }

    fn seed_offline_bucket(detector: &V2OfflineDetector, bucket: i64, summary: &[u8]) {
        detector
            .db
            .write(|tx| {
                tx.execute(
                    "INSERT INTO availability_buckets (node_uuid, bucket_start, window_end, resolution, \
                     host_state, connectivity_state, expected_observers, healthy_observers, seen_observers, \
                     observer_summary, revision, finalized, recalculated_at, created_at, updated_at) \
                     VALUES (?1, ?2, ?3, ?4, 'offline', 'unavailable', 1, 1, 0, ?5, 1, 1, ?6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                    rusqlite::params![
                        NODE.to_vec(),
                        bucket,
                        bucket + BUCKET,
                        RESOLUTION_RAW,
                        summary,
                        bucket + BUCKET,
                    ],
                )?;
                Ok(())
            })
            .unwrap();
    }

    fn seed_path_seen(detector: &V2OfflineDetector, bucket: i64, last_seen: i64) {
        detector
            .db
            .write(|tx| {
                tx.execute(
                    "INSERT INTO observer_path_buckets (node_uuid, observer_id, bucket_start, seen, last_seen_at, revision) \
                     VALUES (?1, 'primary', ?2, 1, ?3, 1)",
                    rusqlite::params![NODE.to_vec(), bucket, last_seen],
                )?;
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn consensus_offline_past_threshold_opens_history() {
        let (detector, path) = detector("opens");
        seed_runtime(&detector, "online", Some(&NODE));
        let summary = br#"[{"observer_id":"primary","healthy":true,"seen":false}]"#.to_vec();
        seed_offline_bucket(&detector, BUCKET, &summary);
        seed_path_seen(&detector, BUCKET - BUCKET, BUCKET + 5);
        let now = BUCKET + BUCKET + BUCKET;
        let (notices, recoveries) = detector.detect(now, 60_000_000_000, BUCKET).unwrap();
        assert_eq!(notices.len(), 1);
        assert!(recoveries.is_empty());
        assert_eq!(notices[0].started_at_nanos, BUCKET);
        assert_eq!(notices[0].last_seen_nanos, BUCKET + 5);

        let (status, offline_id, started_at): (String, i64, String) = detector
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT server_runtimes.status, server_runtimes.current_offline_id, started_at \
                     FROM server_runtimes \
                     JOIN server_offline_histories ON server_offline_histories.id = server_runtimes.current_offline_id \
                     WHERE server_runtimes.server_id = 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                    .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(status, "offline");
        assert_eq!(offline_id, notices[0].history_id);
        assert_eq!(started_at, crate::util::time::unix_nanos_to_sqlite(BUCKET));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn offline_shorter_than_threshold_opens_nothing() {
        let (detector, path) = detector("short");
        seed_runtime(&detector, "online", Some(&NODE));
        seed_offline_bucket(&detector, BUCKET + BUCKET + BUCKET, b"[]");
        let now = BUCKET + BUCKET + BUCKET + BUCKET + 1;
        let (notices, recoveries) = detector.detect(now, 60_000_000_000, BUCKET).unwrap();
        assert!(notices.is_empty() && recoveries.is_empty());
        let histories: i64 = detector
            .db
            .read(|conn| {
                conn.query_row("SELECT COUNT(*) FROM server_offline_histories", [], |row| {
                    row.get(0)
                })
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(histories, 0);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn online_latest_bucket_closes_open_history() {
        let (detector, path) = detector("closes");
        seed_runtime(&detector, "offline", Some(&NODE));
        detector
            .db
            .write(|tx| {
                tx.execute(
                    "INSERT INTO server_offline_histories (server_id, started_at, detected_at, status, \
                     reason, threshold_seconds, last_seen_at, created_at, updated_at) \
                     VALUES (1, ?1, ?1, 'open', 'unknown', 60, ?1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                    [crate::util::time::unix_nanos_to_sqlite(BUCKET)],
                )?;
                tx.execute(
                    "UPDATE server_runtimes SET current_offline_id = last_insert_rowid() WHERE server_id = 1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        // 最新已封窗桶是在线：共识恢复。
        detector
            .db
            .write(|tx| {
                tx.execute(
                    "INSERT INTO availability_buckets (node_uuid, bucket_start, window_end, resolution, \
                     host_state, connectivity_state, expected_observers, healthy_observers, seen_observers, \
                     observer_summary, revision, finalized, recalculated_at, created_at, updated_at) \
                     VALUES (?1, ?2, ?3, ?4, 'online', 'full', 1, 1, 1, ?5, 1, 1, ?6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                    rusqlite::params![
                        NODE.to_vec(),
                        BUCKET + BUCKET,
                        BUCKET + BUCKET + BUCKET,
                        RESOLUTION_RAW,
                        b"[{\"observer_id\":\"primary\",\"healthy\":true,\"seen\":true}]".to_vec(),
                        BUCKET + BUCKET + BUCKET,
                    ],
                )?;
                Ok(())
            })
            .unwrap();
        let (notices, recoveries) = detector
            .detect(BUCKET + BUCKET + BUCKET + 1, 60_000_000_000, BUCKET)
            .unwrap();
        assert!(notices.is_empty());
        assert_eq!(
            recoveries.len(),
            1,
            "最新桶在线应关闭离线历史并产出恢复通知"
        );
        assert_eq!(recoveries[0].server_id, 1);
        let (status, offline_id): (String, Option<i64>) = detector
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT status, current_offline_id FROM server_runtimes WHERE server_id = 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(status, "online");
        assert_eq!(offline_id, None);
        let ended_at: Option<String> = detector
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT ended_at FROM server_offline_histories WHERE server_id = 1",
                    [],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(
            ended_at,
            Some(crate::util::time::unix_nanos_to_sqlite(
                BUCKET + BUCKET + BUCKET + 1
            ))
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn span_row_preserves_long_offline_run() {
        let (detector, path) = detector("span");
        seed_runtime(&detector, "online", Some(&NODE));
        // 长离线段被压缩成 span（起点在段首、终点更晚）。已封窗且 host_state=offline，
        // 与 Go 同序：它就是当前共识，起点取段首。
        detector
            .db
            .write(|tx| {
                tx.execute(
                    "INSERT INTO availability_buckets (node_uuid, bucket_start, window_end, resolution, \
                     host_state, connectivity_state, expected_observers, healthy_observers, seen_observers, \
                     observer_summary, revision, finalized, recalculated_at, created_at, updated_at) \
                     VALUES (?1, ?2, ?3, ?4, 'offline', 'unavailable', 1, 1, 0, ?5, 1, 1, ?6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                    rusqlite::params![
                        NODE.to_vec(),
                        0,
                        BUCKET + BUCKET + BUCKET + BUCKET,
                        RESOLUTION_SPAN,
                        b"[]".to_vec(),
                        BUCKET + BUCKET,
                    ],
                )?;
                Ok(())
            })
            .unwrap();
        let now = BUCKET + BUCKET + BUCKET + BUCKET + BUCKET;
        let verdict = detector.inspect_consensus(&NODE, now, BUCKET, 3).unwrap();
        assert!(verdict.latest_offline);
        assert_eq!(verdict.since_nanos, 0);
        assert_eq!(verdict.duration_nanos, now);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn recovering_runtime_is_skipped() {
        let (detector, path) = detector("recovering");
        seed_runtime(&detector, "recovering", Some(&NODE));
        seed_offline_bucket(&detector, BUCKET, b"[]");
        let (notices, recoveries) = detector
            .detect(BUCKET + BUCKET + BUCKET, 60_000_000_000, BUCKET)
            .unwrap();
        assert!(notices.is_empty() && recoveries.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn disabled_history_policy_detects_nothing() {
        let (mut detector, path) = detector("disabled");
        detector.policy = OfflineHistoryPolicy::disabled();
        seed_runtime(&detector, "online", Some(&NODE));
        seed_offline_bucket(&detector, BUCKET, b"[]");
        let (notices, recoveries) = detector
            .detect(BUCKET + BUCKET + BUCKET, 60_000_000_000, BUCKET)
            .unwrap();
        assert!(notices.is_empty() && recoveries.is_empty());
        let _ = std::fs::remove_file(&path);
    }
}
