//! 离线检测：主端收报超时（report gap）。
//!
//! 判定只看 `server_runtimes.last_seen_at`（主端接收钟）：超过离线阈值没有再收到
//! 任何上报即离线；恢复只看是否收到比离线前更新的上报。可用性桶不参与离线状态与
//! 通知，只服务可用率 / 观测点展示。
//!
//! 会话状态在进程内按 server_id 维护；启动时用运行态 `status='offline'` 回填，
//! 避免面板重启把仍离线的机器再通知一遍。

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::config::ConfigHandle;
use crate::db::Db;
use crate::telemetry::offline::{self, OfflineHistoryPolicy, RuntimeOfflineFields};
use crate::util::time::{now_nanos, sqlite_to_unix_nanos, unix_nanos_to_sqlite};

const MIN_THRESHOLD_SECONDS: u64 = 10;
const MIN_INTERVAL_SECONDS: u64 = 5;
const STATUS_OFFLINE: &str = "offline";
const STATUS_ONLINE: &str = "online";

/// 一次确认离线（通知消费）。
#[derive(Debug, Clone)]
pub struct OfflineNotice {
    pub server_id: i64,
    pub history_id: i64,
    pub started_at_nanos: i64,
    pub last_seen_nanos: i64,
    pub threshold_seconds: u64,
    pub last_ip: String,
}

/// 一次确认恢复（通知消费）。
#[derive(Debug, Clone)]
pub struct RecoveryNotice {
    pub server_id: i64,
    pub recovered_at_nanos: i64,
    pub offline_seconds: i64,
    pub last_ip: String,
    pub recovered_ip: String,
    pub reason: &'static str,
}

#[derive(Debug, Clone)]
struct Episode {
    started_at_nanos: i64,
    last_seen_nanos: i64,
    last_boot_time: u64,
    last_uptime: u64,
    last_ip: String,
}

/// `server_runtimes` 候选行：检测只需要身份、收报钟与离线指针。
#[derive(Debug, Clone)]
struct RuntimeRow {
    server_id: i64,
    last_seen_nanos: i64,
    last_ip: String,
    last_boot_time: u64,
    last_uptime: u64,
    current_offline_id: i64,
}

pub struct OfflineDetector {
    db: Db,
    conf: ConfigHandle,
    notifier: Option<Arc<crate::notify::Notifier>>,
    episodes: Mutex<HashMap<i64, Episode>>,
    started_at: Instant,
}

impl OfflineDetector {
    pub fn new(db: Db, conf: ConfigHandle, notifier: Option<Arc<crate::notify::Notifier>>) -> Self {
        Self {
            db,
            conf,
            notifier,
            episodes: Mutex::new(HashMap::new()),
            started_at: Instant::now(),
        }
    }

    /// 扫描运行态，按收报间隔开/关离线历史。返回（离线通知, 恢复通知）。
    pub fn detect(
        &self,
        now_nanos: i64,
        threshold_nanos: i64,
        policy: OfflineHistoryPolicy,
    ) -> anyhow::Result<(Vec<OfflineNotice>, Vec<RecoveryNotice>)> {
        let threshold_seconds = ((threshold_nanos / 1_000_000_000).max(1)) as u64;
        let runtimes = self.db.read(load_runtimes)?;
        let mut notices = Vec::new();
        let mut recoveries = Vec::new();
        let mut live = HashSet::with_capacity(runtimes.len());
        let mut episodes = self.episodes.lock().unwrap_or_else(|err| err.into_inner());
        for runtime in &runtimes {
            live.insert(runtime.server_id);
            // 从未上报（last_seen 为空）不是离线：新建主机、装好探针但还没连上。
            if runtime.last_seen_nanos <= 0 {
                episodes.remove(&runtime.server_id);
                continue;
            }
            if let Some(episode) = episodes.get(&runtime.server_id) {
                // 只有收到比离线前更新的上报才算恢复；阈值上调不应把仍在停报的机器判回在线。
                if runtime.last_seen_nanos > episode.last_seen_nanos {
                    let episode = episodes.remove(&runtime.server_id).expect("episode exists");
                    if policy.enabled && runtime.current_offline_id > 0 {
                        self.close_history(runtime.server_id, runtime.last_seen_nanos, policy)?;
                    }
                    let reason = offline::detect_offline_reason(
                        episode.last_boot_time,
                        runtime.last_boot_time,
                        episode.last_uptime,
                        runtime.last_uptime,
                    );
                    recoveries.push(RecoveryNotice {
                        server_id: runtime.server_id,
                        recovered_at_nanos: runtime.last_seen_nanos,
                        offline_seconds: ((runtime.last_seen_nanos - episode.started_at_nanos)
                            .max(0)
                            / 1_000_000_000),
                        last_ip: episode.last_ip,
                        recovered_ip: runtime.last_ip.clone(),
                        reason,
                    });
                }
                continue;
            }
            if now_nanos.saturating_sub(runtime.last_seen_nanos) < threshold_nanos {
                continue;
            }
            let started_at_nanos = runtime
                .last_seen_nanos
                .saturating_add(threshold_nanos)
                .min(now_nanos);
            let Some(history_id) = self.open_episode(
                runtime,
                started_at_nanos,
                now_nanos,
                threshold_seconds,
                policy,
            )?
            else {
                // 读运行态与开记录之间收到了新上报：下一轮按在线处理。
                continue;
            };
            episodes.insert(
                runtime.server_id,
                Episode {
                    started_at_nanos,
                    last_seen_nanos: runtime.last_seen_nanos,
                    last_boot_time: runtime.last_boot_time,
                    last_uptime: runtime.last_uptime,
                    last_ip: runtime.last_ip.clone(),
                },
            );
            notices.push(OfflineNotice {
                server_id: runtime.server_id,
                history_id,
                started_at_nanos,
                last_seen_nanos: runtime.last_seen_nanos,
                threshold_seconds,
                last_ip: runtime.last_ip.clone(),
            });
        }
        episodes.retain(|server_id, _| live.contains(server_id));
        drop(episodes);
        Ok((notices, recoveries))
    }

    /// 开一次离线：历史开关打开时落 `server_offline_histories` 并回填运行态指针；
    /// 关闭时只用运行态 `status` / `last_offline_at` 记住这次离线。
    /// 返回 `None` 表示运行态在读与写之间已收到新上报。
    fn open_episode(
        &self,
        runtime: &RuntimeRow,
        started_at_nanos: i64,
        now_nanos: i64,
        threshold_seconds: u64,
        policy: OfflineHistoryPolicy,
    ) -> anyhow::Result<Option<i64>> {
        let server_id = runtime.server_id;
        let expected_last_seen = runtime.last_seen_nanos;
        self.db.write(|tx| {
            let Some(current) = load_runtime_tx(tx, server_id)? else {
                return Ok(None);
            };
            if current.last_seen_nanos != expected_last_seen || current.last_seen_nanos <= 0 {
                return Ok(None);
            }
            if !policy.enabled {
                let started = unix_nanos_to_sqlite(started_at_nanos);
                let updated = tx.execute(
                    "UPDATE server_runtimes SET status = ?1, last_offline_at = ?2, \
                     updated_at = CURRENT_TIMESTAMP WHERE server_id = ?3 AND status IS NOT ?1",
                    params![STATUS_OFFLINE, started, server_id],
                )?;
                return Ok((updated > 0).then_some(0));
            }
            if current.current_offline_id > 0 {
                return Ok(Some(current.current_offline_id));
            }
            tx.execute(
                "INSERT INTO server_offline_histories (server_id, started_at, detected_at, status, \
                 reason, threshold_seconds, last_seen_at, last_boot_time, last_uptime, last_ip, \
                 created_at, updated_at) \
                 VALUES (?1, ?2, ?3, 'open', ?4, ?5, ?6, ?7, ?8, ?9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                params![
                    server_id,
                    unix_nanos_to_sqlite(started_at_nanos),
                    unix_nanos_to_sqlite(now_nanos),
                    offline::OFFLINE_REASON_UNKNOWN,
                    threshold_seconds as i64,
                    unix_nanos_to_sqlite(current.last_seen_nanos),
                    current.last_boot_time as i64,
                    current.last_uptime as i64,
                    current.last_ip,
                ],
            )?;
            let history_id = tx.last_insert_rowid();
            tx.execute(
                "UPDATE server_runtimes SET status = ?1, last_offline_at = ?2, current_offline_id = ?3, \
                 updated_at = CURRENT_TIMESTAMP WHERE server_id = ?4",
                params![
                    STATUS_OFFLINE,
                    unix_nanos_to_sqlite(started_at_nanos),
                    history_id,
                    server_id,
                ],
            )?;
            Ok(Some(history_id))
        })
    }

    /// 报告路径通常已在同一事务里关闭历史；这里兜底指针仍在的情况（例如进程中断）。
    fn close_history(
        &self,
        server_id: i64,
        now_nanos: i64,
        policy: OfflineHistoryPolicy,
    ) -> anyhow::Result<()> {
        self.db.write(|tx| {
            let Some(current) = load_runtime_tx(tx, server_id)? else {
                return Ok(());
            };
            if current.current_offline_id <= 0 {
                return Ok(());
            }
            offline::close_offline_history_tx(
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
                policy,
            )?;
            tx.execute(
                "UPDATE server_runtimes SET status = ?1, last_online_at = ?2, updated_at = CURRENT_TIMESTAMP \
                 WHERE server_id = ?3",
                params![STATUS_ONLINE, unix_nanos_to_sqlite(now_nanos), server_id],
            )?;
            Ok(())
        })
    }

    /// 启动回填：已在离线中的机器不重复通知，恢复时仍能收尾。
    fn seed_episodes(&self) -> anyhow::Result<()> {
        let threshold_seconds = self
            .conf
            .read()
            .expect("config lock")
            .offline_threshold_seconds
            .max(MIN_THRESHOLD_SECONDS);
        let threshold_nanos = threshold_seconds as i64 * 1_000_000_000;
        let rows = self.db.read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT server_id, last_seen_at, last_ip, last_boot_time, last_uptime, \
                 last_offline_at FROM server_runtimes \
                 WHERE status = ?1 AND last_seen_at IS NOT NULL",
            )?;
            let rows = stmt
                .query_map([STATUS_OFFLINE], |row| {
                    let last_seen: Option<String> = row.get(1)?;
                    let last_offline_at: Option<String> = row.get(5)?;
                    Ok((
                        row.get::<_, i64>(0)?,
                        last_seen.as_deref().and_then(sqlite_to_unix_nanos),
                        row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                        row.get::<_, Option<i64>>(3)?.unwrap_or(0).max(0) as u64,
                        row.get::<_, Option<i64>>(4)?.unwrap_or(0).max(0) as u64,
                        last_offline_at.as_deref().and_then(sqlite_to_unix_nanos),
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })?;
        let mut episodes = self.episodes.lock().unwrap_or_else(|err| err.into_inner());
        for (server_id, last_seen, last_ip, last_boot_time, last_uptime, started) in rows {
            let Some(last_seen_nanos) = last_seen else {
                continue;
            };
            if last_seen_nanos <= 0 {
                continue;
            }
            episodes.insert(
                server_id,
                Episode {
                    started_at_nanos: started
                        .unwrap_or_else(|| last_seen_nanos.saturating_add(threshold_nanos)),
                    last_seen_nanos,
                    last_boot_time,
                    last_uptime,
                    last_ip,
                },
            );
        }
        Ok(())
    }

    /// 检测循环：配置里的间隔/阈值/开关每轮重读，改设置不需要重启面板。
    pub async fn run(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        if let Err(err) = self.seed_episodes() {
            tracing::warn!("离线检测回填失败: {err:#}");
        }
        loop {
            let (interval_seconds, threshold_seconds, enabled) = {
                let conf = self.conf.read().expect("config lock");
                (
                    conf.offline_check_interval_seconds
                        .max(MIN_INTERVAL_SECONDS),
                    conf.offline_threshold_seconds.max(MIN_THRESHOLD_SECONDS),
                    conf.enable_offline_history
                        || conf.enable_offline_notification
                        || conf.enable_recovery_notification,
                )
            };
            let interval = Duration::from_secs(interval_seconds);
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    if !enabled || self.started_at.elapsed() <= interval * 2 {
                        continue;
                    }
                    let policy = OfflineHistoryPolicy::from_conf(&self.conf.read().expect("config lock"));
                    let threshold_nanos = threshold_seconds as i64 * 1_000_000_000;
                    let detector = Arc::clone(&self);
                    let outcome = tokio::task::spawn_blocking(move || {
                        detector.detect(now_nanos(), threshold_nanos, policy)
                    })
                    .await;
                    match outcome {
                        Ok(Ok((notices, recoveries))) => {
                            for notice in notices {
                                tracing::info!(
                                    server_id = notice.server_id,
                                    history_id = notice.history_id,
                                    last_seen_nanos = notice.last_seen_nanos,
                                    "收报超时判定离线"
                                );
                                if let Some(notifier) = &self.notifier {
                                    notifier
                                        .notify_offline(
                                            notice.server_id,
                                            notice.started_at_nanos,
                                            notice.last_seen_nanos,
                                            notice.threshold_seconds,
                                            notice.last_ip,
                                        )
                                        .await;
                                }
                            }
                            for recovery in recoveries {
                                tracing::info!(
                                    server_id = recovery.server_id,
                                    "收到上报，离线恢复"
                                );
                                if let Some(notifier) = &self.notifier {
                                    notifier
                                        .notify_recovery(
                                            recovery.server_id,
                                            recovery.recovered_at_nanos,
                                            recovery.offline_seconds,
                                            recovery.last_ip,
                                            recovery.recovered_ip,
                                            recovery.reason,
                                        )
                                        .await;
                                }
                            }
                        }
                        Ok(Err(err)) => tracing::warn!("离线检测失败: {err:#}"),
                        Err(err) => tracing::warn!("离线检测任务崩溃: {err}"),
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        tracing::info!("离线检测退出");
                        return;
                    }
                }
            }
        }
    }
}

fn load_runtimes(conn: &Connection) -> anyhow::Result<Vec<RuntimeRow>> {
    let mut stmt = conn.prepare(
        "SELECT r.server_id, r.last_seen_at, COALESCE(r.last_ip, ''), \
         COALESCE(r.last_boot_time, 0), COALESCE(r.last_uptime, 0), COALESCE(r.current_offline_id, 0) \
         FROM server_runtimes r JOIN servers s ON s.id = r.server_id \
         WHERE s.deleted_at IS NULL",
    )?;
    let rows = stmt
        .query_map([], |row| {
            let last_seen: Option<String> = row.get(1)?;
            Ok(RuntimeRow {
                server_id: row.get(0)?,
                last_seen_nanos: last_seen
                    .as_deref()
                    .and_then(sqlite_to_unix_nanos)
                    .unwrap_or(0),
                last_ip: row.get(2)?,
                last_boot_time: row.get::<_, i64>(3)?.max(0) as u64,
                last_uptime: row.get::<_, i64>(4)?.max(0) as u64,
                current_offline_id: row.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn load_runtime_tx(tx: &Transaction<'_>, server_id: i64) -> anyhow::Result<Option<RuntimeRow>> {
    tx.query_row(
        "SELECT r.server_id, r.last_seen_at, COALESCE(r.last_ip, ''), \
         COALESCE(r.last_boot_time, 0), COALESCE(r.last_uptime, 0), COALESCE(r.current_offline_id, 0) \
         FROM server_runtimes r WHERE r.server_id = ?",
        [server_id],
        |row| {
            let last_seen: Option<String> = row.get(1)?;
            Ok(RuntimeRow {
                server_id: row.get(0)?,
                last_seen_nanos: last_seen
                    .as_deref()
                    .and_then(sqlite_to_unix_nanos)
                    .unwrap_or(0),
                last_ip: row.get(2)?,
                last_boot_time: row.get::<_, i64>(3)?.max(0) as u64,
                last_uptime: row.get::<_, i64>(4)?.max(0) as u64,
                current_offline_id: row.get(5)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, new_config_handle};
    use crate::db::migrate;

    const SERVER_ID: i64 = 1;
    const THRESHOLD: i64 = 60_000_000_000;

    fn detector(name: &str) -> (OfflineDetector, String) {
        let dir =
            std::env::temp_dir().join(format!("santaizi-offline-{name}-{}", std::process::id()));
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
        (
            OfflineDetector::new(db, new_config_handle(conf), None),
            path,
        )
    }

    fn policy() -> OfflineHistoryPolicy {
        OfflineHistoryPolicy {
            enabled: true,
            merge_gap_seconds: 0,
        }
    }

    fn seed_runtime(detector: &OfflineDetector, now: i64, last_seen_ago: i64) {
        detector
            .db
            .write(|tx| {
                tx.execute(
                    "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) \
                     VALUES (?1, 'node-a', 't', X'00', 1, 0)",
                    [SERVER_ID],
                )?;
                tx.execute(
                    "INSERT INTO server_runtimes (server_id, status, protocol, last_seen_at, last_ip, \
                     last_boot_time, last_uptime, created_at, updated_at) \
                     VALUES (?1, 'online', 'v2', ?2, '10.0.0.1', 111, 222, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                    params![SERVER_ID, unix_nanos_to_sqlite(now - last_seen_ago)],
                )?;
                Ok(())
            })
            .unwrap();
    }

    fn bump_last_seen(detector: &OfflineDetector, at: i64) {
        detector
            .db
            .write(|tx| {
                tx.execute(
                    "UPDATE server_runtimes SET last_seen_at = ?1, last_online_at = ?1, status = 'online' \
                     WHERE server_id = ?2",
                    params![unix_nanos_to_sqlite(at), SERVER_ID],
                )?;
                Ok(())
            })
            .unwrap();
    }

    fn runtime_state(detector: &OfflineDetector) -> (String, Option<i64>) {
        detector
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT COALESCE(status, ''), current_offline_id FROM server_runtimes WHERE server_id = ?1",
                    [SERVER_ID],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(Into::into)
            })
            .unwrap()
    }

    fn history_rows(detector: &OfflineDetector) -> i64 {
        detector
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM server_offline_histories WHERE server_id = ?1",
                    [SERVER_ID],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap()
    }

    #[test]
    fn silence_past_threshold_opens_history_once() {
        let (detector, path) = detector("opens");
        let now = 1_700_000_000_000_000_000i64;
        seed_runtime(&detector, now, 90_000_000_000);
        let (notices, recoveries) = detector.detect(now, THRESHOLD, policy()).unwrap();
        assert_eq!(notices.len(), 1);
        assert!(recoveries.is_empty());
        assert_eq!(
            notices[0].started_at_nanos,
            now - 90_000_000_000 + THRESHOLD
        );
        assert_eq!(notices[0].last_seen_nanos, now - 90_000_000_000);
        assert_eq!(notices[0].threshold_seconds, 60);
        assert_eq!(notices[0].last_ip, "10.0.0.1");
        assert_eq!(
            runtime_state(&detector),
            ("offline".into(), Some(notices[0].history_id))
        );
        assert_eq!(history_rows(&detector), 1);

        // 下一轮仍是同一段离线：不重复开记录、不重复通知。
        let (notices, _) = detector
            .detect(now + 10_000_000_000, THRESHOLD, policy())
            .unwrap();
        assert!(notices.is_empty());
        assert_eq!(history_rows(&detector), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fresh_report_stays_online() {
        let (detector, path) = detector("fresh");
        let now = 1_700_000_000_000_000_000i64;
        seed_runtime(&detector, now, 30_000_000_000);
        let (notices, recoveries) = detector.detect(now, THRESHOLD, policy()).unwrap();
        assert!(notices.is_empty() && recoveries.is_empty());
        assert_eq!(history_rows(&detector), 0);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn never_reported_is_not_offline() {
        let (detector, path) = detector("never");
        let now = 1_700_000_000_000_000_000i64;
        detector
            .db
            .write(|tx| {
                tx.execute(
                    "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) \
                     VALUES (?1, 'node-a', 't', X'00', 1, 0)",
                    [SERVER_ID],
                )?;
                tx.execute(
                    "INSERT INTO server_runtimes (server_id, status) VALUES (?1, 'unknown')",
                    [SERVER_ID],
                )?;
                Ok(())
            })
            .unwrap();
        let (notices, recoveries) = detector.detect(now, THRESHOLD, policy()).unwrap();
        assert!(notices.is_empty() && recoveries.is_empty());
        assert_eq!(history_rows(&detector), 0);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn report_after_offline_closes_history_and_notifies_recovery() {
        let (detector, path) = detector("recovers");
        let now = 1_700_000_000_000_000_000i64;
        seed_runtime(&detector, now, 90_000_000_000);
        let (notices, _) = detector.detect(now, THRESHOLD, policy()).unwrap();
        assert_eq!(notices.len(), 1);

        // 探针续报：last_seen 前进；运行态由上报路径翻回 online。
        let recovered_at = now + 30_000_000_000;
        bump_last_seen(&detector, recovered_at);
        let (notices, recoveries) = detector.detect(recovered_at, THRESHOLD, policy()).unwrap();
        assert!(notices.is_empty());
        assert_eq!(recoveries.len(), 1);
        assert_eq!(recoveries[0].recovered_at_nanos, recovered_at);
        assert_eq!(recoveries[0].offline_seconds, 60);
        assert_eq!(recoveries[0].last_ip, "10.0.0.1");
        assert_eq!(runtime_state(&detector), ("online".into(), None));
        let closed: i64 = detector
            .db
            .read(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM server_offline_histories WHERE status = 'closed'",
                    [],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(closed, 1);

        // 再跑一轮不重复发恢复。
        let (_, recoveries) = detector
            .detect(recovered_at + 5_000_000_000, THRESHOLD, policy())
            .unwrap();
        assert!(recoveries.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn history_disabled_still_tracks_episode_in_runtime() {
        let (detector, path) = detector("no-history");
        let now = 1_700_000_000_000_000_000i64;
        seed_runtime(&detector, now, 90_000_000_000);
        let disabled = OfflineHistoryPolicy::disabled();
        let (notices, _) = detector.detect(now, THRESHOLD, disabled).unwrap();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].history_id, 0);
        assert_eq!(history_rows(&detector), 0);
        assert_eq!(runtime_state(&detector).0, "offline");

        bump_last_seen(&detector, now + 10_000_000_000);
        let (_, recoveries) = detector
            .detect(now + 10_000_000_000, THRESHOLD, disabled)
            .unwrap();
        assert_eq!(recoveries.len(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn raising_threshold_does_not_resurrect_a_silent_host() {
        let (detector, path) = detector("threshold");
        let now = 1_700_000_000_000_000_000i64;
        seed_runtime(&detector, now, 90_000_000_000);
        let (notices, _) = detector.detect(now, THRESHOLD, policy()).unwrap();
        assert_eq!(notices.len(), 1);
        // 阈值改到 10 分钟，但这条机器没有新上报：仍是同一段离线，不判恢复。
        let (notices, recoveries) = detector.detect(now, 600_000_000_000, policy()).unwrap();
        assert!(notices.is_empty() && recoveries.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn seeded_episode_does_not_re_notify_after_restart() {
        let (detector, path) = detector("seeded");
        let now = 1_700_000_000_000_000_000i64;
        seed_runtime(&detector, now, 90_000_000_000);
        let (notices, _) = detector.detect(now, THRESHOLD, policy()).unwrap();
        assert_eq!(notices.len(), 1);
        detector.episodes.lock().unwrap().clear();
        detector.seed_episodes().unwrap();
        let (notices, recoveries) = detector
            .detect(now + 10_000_000_000, THRESHOLD, policy())
            .unwrap();
        assert!(notices.is_empty() && recoveries.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reboot_reason_is_reported_on_recovery() {
        let (detector, path) = detector("reboot");
        let now = 1_700_000_000_000_000_000i64;
        seed_runtime(&detector, now, 90_000_000_000);
        detector.detect(now, THRESHOLD, policy()).unwrap();
        detector
            .db
            .write(|tx| {
                tx.execute(
                    "UPDATE server_runtimes SET last_seen_at = ?1, last_boot_time = 999, last_uptime = 5 \
                     WHERE server_id = ?2",
                    params![unix_nanos_to_sqlite(now + 10_000_000_000), SERVER_ID],
                )?;
                Ok(())
            })
            .unwrap();
        let (_, recoveries) = detector
            .detect(now + 10_000_000_000, THRESHOLD, policy())
            .unwrap();
        assert_eq!(recoveries.len(), 1);
        assert_eq!(recoveries[0].reason, offline::OFFLINE_REASON_MACHINE_REBOOT);
        let _ = std::fs::remove_file(&path);
    }
}
