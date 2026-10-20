//! 离线历史收口：对齐 Go `closeV2OfflineHistory` / `closeOfflineHistoryTx` /
//! `tryMergeWithPrevious`（`service/singleton/offline_detector.go`）。
//!
//! 探针恢复上报时在**同一写事务**内关闭进行中的离线记录：原因判定比较
//! 「离线前」运行态与「本次恢复」上报，因此必须在覆盖 LastBootTime /
//! LastUptime / LastIP 之前调用。短抖动合并把间隔 ≤ merge gap 的两次离线
//! 并成一条；离线/恢复通知属 Phase 6。

use rusqlite::{OptionalExtension, Transaction};

use crate::proto;
use crate::util::time::{sqlite_to_unix_nanos, unix_nanos_to_sqlite};

pub const OFFLINE_REASON_UNKNOWN: &str = "unknown";
pub const OFFLINE_REASON_NETWORK_DISCONNECT: &str = "network_disconnect";
pub const OFFLINE_REASON_MACHINE_REBOOT: &str = "machine_reboot";

const STATUS_OPEN: &str = "open";

/// 离线历史配置快照（主端 Config 离线段）。
#[derive(Debug, Clone, Copy)]
pub struct OfflineHistoryPolicy {
    pub enabled: bool,
    pub merge_gap_seconds: u64,
}

impl OfflineHistoryPolicy {
    pub fn from_conf(conf: &crate::config::Config) -> Self {
        Self {
            enabled: conf.enable_offline_history,
            merge_gap_seconds: conf.offline_merge_gap_seconds,
        }
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            merge_gap_seconds: 0,
        }
    }
}

/// 运行态中的「离线前」快照：关闭判定的比较基准。
pub struct RuntimeOfflineFields {
    pub current_offline_id: i64,
    pub last_boot_time: u64,
    pub last_uptime: u64,
    pub last_ip: String,
}

/// 被关闭（或被合并延展）的离线记录，供 Phase 6 通知使用。
pub struct ClosedOfflineHistory {
    pub id: i64,
    pub server_id: i64,
    pub started_at_nanos: i64,
    pub last_seen_at_nanos: i64,
    pub ended_at_nanos: i64,
    pub duration_seconds: u64,
    pub reason: &'static str,
    pub recovered_boot_time: u64,
    pub recovered_uptime: u64,
    pub recovered_ip: String,
}

/// `DetectOfflineReason`：任一钟/运行时缺失 → unknown；BootTime 前进或
/// Uptime 回退 → 机器重启；否则 → 网络断线。
pub fn detect_offline_reason(
    last_boot_time: u64,
    recovered_boot_time: u64,
    last_uptime: u64,
    recovered_uptime: u64,
) -> &'static str {
    if last_boot_time == 0 || recovered_boot_time == 0 || last_uptime == 0 || recovered_uptime == 0
    {
        return OFFLINE_REASON_UNKNOWN;
    }
    if recovered_boot_time < last_boot_time {
        return OFFLINE_REASON_UNKNOWN;
    }
    if recovered_boot_time > last_boot_time || recovered_uptime < last_uptime {
        return OFFLINE_REASON_MACHINE_REBOOT;
    }
    OFFLINE_REASON_NETWORK_DISCONNECT
}

/// `durationSecondsBetween`：不足半秒舍、半秒起进（对齐 Go 取整）。
pub fn duration_seconds_between(start_nanos: i64, end_nanos: i64) -> u64 {
    if end_nanos <= start_nanos {
        return 0;
    }
    let delta = end_nanos - start_nanos;
    let mut seconds = (delta / 1_000_000_000) as u64;
    if delta % 1_000_000_000 >= 500_000_000 {
        seconds += 1;
    }
    seconds
}

/// `closeV2OfflineHistory`：关闭运行态指针指向的进行中离线记录。
/// 记录缺失或已关闭时仅修正运行态指针；返回本次生效的记录（合并后为延展后的上一条）。
pub fn close_offline_history_tx(
    tx: &Transaction<'_>,
    server_id: i64,
    fields: &RuntimeOfflineFields,
    state: Option<&proto::State>,
    host: Option<&proto::Host>,
    now_nanos: i64,
    policy: OfflineHistoryPolicy,
) -> anyhow::Result<Option<ClosedOfflineHistory>> {
    if !policy.enabled || fields.current_offline_id <= 0 {
        return Ok(None);
    }
    struct OpenRow {
        started_at_nanos: i64,
        last_seen_at_nanos: i64,
        last_boot_time: u64,
        last_uptime: u64,
        status: String,
    }
    let history = tx
        .query_row(
            "SELECT started_at, last_seen_at, last_boot_time, last_uptime, status \
             FROM server_offline_histories WHERE id = ?",
            [fields.current_offline_id],
            |row| {
                Ok(OpenRow {
                    started_at_nanos: datetime_nanos(row, 0)?,
                    last_seen_at_nanos: datetime_nanos(row, 1)?,
                    last_boot_time: row.get::<_, Option<i64>>(2)?.unwrap_or(0).max(0) as u64,
                    last_uptime: row.get::<_, Option<i64>>(3)?.unwrap_or(0).max(0) as u64,
                    status: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                })
            },
        )
        .optional()?;
    // 记录缺失或已关闭：只修正运行态指针，不造收尾。
    let Some(history) = history.filter(|row| row.status == STATUS_OPEN) else {
        clear_runtime_offline_id(tx, server_id)?;
        return Ok(None);
    };

    let recovered_boot_time = host
        .map(|host| host.boot_time)
        .unwrap_or(fields.last_boot_time);
    let recovered_uptime = state
        .map(|state| state.uptime)
        .unwrap_or(fields.last_uptime);
    let recovered_ip = host
        .map(|host| host.ip.trim().to_string())
        .filter(|ip| !ip.is_empty())
        .unwrap_or_else(|| fields.last_ip.clone());
    let reason = detect_offline_reason(
        history.last_boot_time,
        recovered_boot_time,
        history.last_uptime,
        recovered_uptime,
    );
    let duration_seconds = duration_seconds_between(history.started_at_nanos, now_nanos);
    tx.execute(
        "UPDATE server_offline_histories SET ended_at = ?1, recovered_at = ?1, duration_seconds = ?2, \
         reason = ?3, status = 'closed', recovered_boot_time = ?4, recovered_uptime = ?5, recovered_ip = ?6, \
         updated_at = CURRENT_TIMESTAMP WHERE id = ?7",
        rusqlite::params![
            unix_nanos_to_sqlite(now_nanos),
            duration_seconds as i64,
            reason,
            recovered_boot_time as i64,
            recovered_uptime as i64,
            recovered_ip,
            fields.current_offline_id,
        ],
    )?;
    clear_runtime_offline_id(tx, server_id)?;

    let closed = ClosedOfflineHistory {
        id: fields.current_offline_id,
        server_id,
        started_at_nanos: history.started_at_nanos,
        last_seen_at_nanos: history.last_seen_at_nanos,
        ended_at_nanos: now_nanos,
        duration_seconds,
        reason,
        recovered_boot_time,
        recovered_uptime,
        recovered_ip,
    };
    if policy.merge_gap_seconds > 0
        && let Some(merged) = try_merge_with_previous_tx(tx, &closed, policy.merge_gap_seconds)?
    {
        return Ok(Some(merged));
    }
    Ok(Some(closed))
}

/// 运行态指针清零：可信帧即在线，`current_offline_id` 不得残留。
fn clear_runtime_offline_id(tx: &Transaction<'_>, server_id: i64) -> anyhow::Result<()> {
    tx.execute(
        "UPDATE server_runtimes SET current_offline_id = NULL, updated_at = CURRENT_TIMESTAMP \
         WHERE server_id = ? AND current_offline_id IS NOT NULL",
        [server_id],
    )?;
    Ok(())
}

/// `tryMergeWithPrevious`：与上一条已关闭记录的「真实在线时间」
/// （本次离线前最后上报 − 上一条恢复点）≤ merge gap 时并入：
/// 延展上一条到本次恢复点并删除当前记录；原因不同降级为 unknown。
fn try_merge_with_previous_tx(
    tx: &Transaction<'_>,
    current: &ClosedOfflineHistory,
    merge_gap_seconds: u64,
) -> anyhow::Result<Option<ClosedOfflineHistory>> {
    struct PrevRow {
        id: i64,
        started_at_nanos: i64,
        ended_at_nanos: i64,
        reason: String,
    }
    let prev = tx
        .query_row(
            "SELECT id, started_at, ended_at, reason FROM server_offline_histories \
             WHERE server_id = ? AND status = 'closed' AND id < ? ORDER BY id DESC LIMIT 1",
            rusqlite::params![current.server_id, current.id],
            |row| {
                Ok(PrevRow {
                    id: row.get(0)?,
                    started_at_nanos: datetime_nanos(row, 1)?,
                    ended_at_nanos: datetime_nanos(row, 2)?,
                    reason: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                })
            },
        )
        .optional()?;
    let Some(prev) = prev else { return Ok(None) };
    if prev.ended_at_nanos == 0 {
        return Ok(None);
    }
    let gap_nanos = merge_gap_seconds as i64 * 1_000_000_000;
    if current.last_seen_at_nanos - prev.ended_at_nanos > gap_nanos {
        return Ok(None);
    }
    let duration_seconds = duration_seconds_between(prev.started_at_nanos, current.ended_at_nanos);
    let reason: &'static str = if prev.reason == current.reason {
        current.reason
    } else {
        OFFLINE_REASON_UNKNOWN
    };
    tx.execute(
        "UPDATE server_offline_histories SET ended_at = ?1, recovered_at = ?1, duration_seconds = ?2, \
         reason = ?3, recovered_boot_time = ?4, recovered_uptime = ?5, recovered_ip = ?6, \
         updated_at = CURRENT_TIMESTAMP WHERE id = ?7",
        rusqlite::params![
            unix_nanos_to_sqlite(current.ended_at_nanos),
            duration_seconds as i64,
            reason,
            current.recovered_boot_time as i64,
            current.recovered_uptime as i64,
            current.recovered_ip,
            prev.id,
        ],
    )?;
    // Go 用 Unscoped 硬删：合并掉的短抖动不留在历史里。
    tx.execute(
        "DELETE FROM server_offline_histories WHERE id = ?",
        [current.id],
    )?;
    Ok(Some(ClosedOfflineHistory {
        id: prev.id,
        server_id: current.server_id,
        started_at_nanos: prev.started_at_nanos,
        last_seen_at_nanos: current.last_seen_at_nanos,
        ended_at_nanos: current.ended_at_nanos,
        duration_seconds,
        reason,
        recovered_boot_time: current.recovered_boot_time,
        recovered_uptime: current.recovered_uptime,
        recovered_ip: current.recovered_ip.clone(),
    }))
}

fn datetime_nanos(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<i64> {
    Ok(row
        .get::<_, Option<String>>(index)?
        .and_then(|text| sqlite_to_unix_nanos(&text))
        .unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;

    const NOW: i64 = 1_700_000_000_000_000_000;

    fn temp_db(tag: &str) -> String {
        let dir =
            std::env::temp_dir().join(format!("santaizi-offline-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("panel.db").to_string_lossy().into_owned()
    }

    fn seed(db: &crate::db::Db, current_offline_id: Option<i64>) {
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) \
                 VALUES (7, 'n', 't', X'00', 1, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason) \
                 VALUES (7, X'05000000000000000000000000000000', 1, NULL, 1, 'test')",
                [],
            )?;
            tx.execute(
                "INSERT INTO server_runtimes (server_id, status, last_collected_at, last_boot_time, last_uptime, last_ip, current_offline_id) \
                 VALUES (7, 'offline', 0, 100, 50, '10.0.0.1', ?)",
                [current_offline_id],
            )?;
            Ok(())
        })
        .unwrap();
    }

    fn seed_history(
        db: &crate::db::Db,
        id: i64,
        status: &str,
        started_nanos: i64,
        last_seen_nanos: i64,
    ) {
        db.write(|tx| {
            tx.execute(
                "INSERT INTO server_offline_histories (id, server_id, started_at, detected_at, status, reason, \
                 threshold_seconds, last_seen_at, last_boot_time, last_uptime, last_ip) \
                 VALUES (?, 7, ?, ?, ?, 'unknown', 30, ?, 100, 50, '10.0.0.1')",
                rusqlite::params![
                    id,
                    unix_nanos_to_sqlite(started_nanos),
                    unix_nanos_to_sqlite(started_nanos + 1_000_000_000),
                    status,
                    unix_nanos_to_sqlite(last_seen_nanos),
                ],
            )?;
            Ok(())
        })
        .unwrap();
    }

    fn runtime_offline_id(db: &crate::db::Db) -> Option<i64> {
        db.read(|conn| {
            Ok(conn.query_row(
                "SELECT current_offline_id FROM server_runtimes WHERE server_id = 7",
                [],
                |r| r.get(0),
            )?)
        })
        .unwrap()
    }

    fn history_row(
        db: &crate::db::Db,
        id: i64,
    ) -> (String, Option<String>, Option<i64>, String, Option<String>) {
        db.read(|conn| {
            Ok(conn.query_row(
                "SELECT status, ended_at, duration_seconds, reason, recovered_ip \
                 FROM server_offline_histories WHERE id = ?",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )?)
        })
        .unwrap()
    }

    fn fields(current_offline_id: i64) -> RuntimeOfflineFields {
        RuntimeOfflineFields {
            current_offline_id,
            last_boot_time: 100,
            last_uptime: 50,
            last_ip: "10.0.0.1".into(),
        }
    }

    fn enabled(merge_gap_seconds: u64) -> OfflineHistoryPolicy {
        OfflineHistoryPolicy {
            enabled: true,
            merge_gap_seconds,
        }
    }

    #[test]
    fn close_fills_recovery_fields_and_network_reason() {
        let path = temp_db("close");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db, Some(1));
        seed_history(&db, 1, "open", NOW - 60_000_000_000, NOW - 60_000_000_000);
        let host = proto::Host {
            ip: "10.0.0.2".into(),
            boot_time: 100,
            version: "0.1.0".into(),
            ..Default::default()
        };
        let state = proto::State {
            uptime: 60,
            ..Default::default()
        };
        let closed = db
            .write(|tx| {
                close_offline_history_tx(
                    tx,
                    7,
                    &fields(1),
                    Some(&state),
                    Some(&host),
                    NOW,
                    enabled(10),
                )
            })
            .unwrap()
            .expect("history closed");
        assert_eq!(closed.reason, OFFLINE_REASON_NETWORK_DISCONNECT);
        assert_eq!(closed.duration_seconds, 60);
        assert_eq!(closed.recovered_ip, "10.0.0.2");
        let (status, ended, duration, reason, recovered_ip) = history_row(&db, 1);
        assert_eq!(status, "closed");
        assert_eq!(ended, Some(unix_nanos_to_sqlite(NOW)));
        assert_eq!(duration, Some(60));
        assert_eq!(reason, OFFLINE_REASON_NETWORK_DISCONNECT);
        assert_eq!(recovered_ip, Some("10.0.0.2".to_string()));
        assert_eq!(runtime_offline_id(&db), None);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn close_detects_machine_reboot_on_boot_time_and_uptime_rollback() {
        // BootTime 前进 → 重启。
        let path = temp_db("reboot");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db, Some(1));
        seed_history(&db, 1, "open", NOW - 10_000_000_000, NOW - 10_000_000_000);
        let rebooted = proto::Host {
            ip: "10.0.0.2".into(),
            boot_time: 200,
            ..Default::default()
        };
        let closed = db
            .write(|tx| {
                close_offline_history_tx(tx, 7, &fields(1), None, Some(&rebooted), NOW, enabled(10))
            })
            .unwrap()
            .unwrap();
        assert_eq!(closed.reason, OFFLINE_REASON_MACHINE_REBOOT);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());

        // 同 BootTime 但 Uptime 回退 → 重启。
        let path = temp_db("rollback");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db, Some(1));
        seed_history(&db, 1, "open", NOW - 10_000_000_000, NOW - 10_000_000_000);
        let rolled = proto::Host {
            ip: "10.0.0.2".into(),
            boot_time: 100,
            ..Default::default()
        };
        let state = proto::State {
            uptime: 40,
            ..Default::default()
        };
        let closed = db
            .write(|tx| {
                close_offline_history_tx(
                    tx,
                    7,
                    &fields(1),
                    Some(&state),
                    Some(&rolled),
                    NOW,
                    enabled(10),
                )
            })
            .unwrap()
            .unwrap();
        assert_eq!(closed.reason, OFFLINE_REASON_MACHINE_REBOOT);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn close_without_host_or_state_carries_runtime_fields() {
        let path = temp_db("carry");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db, Some(1));
        seed_history(&db, 1, "open", NOW - 30_000_000_000, NOW - 30_000_000_000);
        let closed = db
            .write(|tx| close_offline_history_tx(tx, 7, &fields(1), None, None, NOW, enabled(10)))
            .unwrap()
            .unwrap();
        assert_eq!(closed.recovered_boot_time, 100);
        assert_eq!(closed.recovered_uptime, 50);
        assert_eq!(closed.recovered_ip, "10.0.0.1");
        assert_eq!(closed.duration_seconds, 30);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn close_skips_missing_or_already_closed_history() {
        let path = temp_db("skip");
        let db = migrate::open_primary(&path).unwrap();
        // 指针指向不存在的记录：仅修正运行态。
        seed(&db, Some(999));
        let closed = db
            .write(|tx| close_offline_history_tx(tx, 7, &fields(999), None, None, NOW, enabled(10)))
            .unwrap();
        assert!(closed.is_none());
        assert_eq!(runtime_offline_id(&db), None);

        // 已关闭：不重写收尾字段。
        seed_history(&db, 5, "closed", NOW - 30_000_000_000, NOW - 30_000_000_000);
        db.write(|tx| {
            tx.execute(
                "UPDATE server_offline_histories SET ended_at = ?1, recovered_at = ?1, duration_seconds = 99, \
                 reason = 'network_disconnect', recovered_ip = '10.9.9.9' WHERE id = 5",
                [unix_nanos_to_sqlite(NOW - 5_000_000_000)],
            )?;
            Ok(())
        })
        .unwrap();
        let closed = db
            .write(|tx| close_offline_history_tx(tx, 7, &fields(5), None, None, NOW, enabled(10)))
            .unwrap();
        assert!(closed.is_none());
        let (status, ended, duration, reason, recovered_ip) = history_row(&db, 5);
        assert_eq!(status, "closed");
        assert_eq!(ended, Some(unix_nanos_to_sqlite(NOW - 5_000_000_000)));
        assert_eq!(duration, Some(99));
        assert_eq!(reason, OFFLINE_REASON_NETWORK_DISCONNECT);
        assert_eq!(recovered_ip, Some("10.9.9.9".to_string()));
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn close_respects_disabled_policy() {
        let path = temp_db("disabled");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db, Some(1));
        seed_history(&db, 1, "open", NOW - 30_000_000_000, NOW - 30_000_000_000);
        let closed = db
            .write(|tx| {
                close_offline_history_tx(
                    tx,
                    7,
                    &fields(1),
                    None,
                    None,
                    NOW,
                    OfflineHistoryPolicy::disabled(),
                )
            })
            .unwrap();
        assert!(closed.is_none());
        let (status, ..) = history_row(&db, 1);
        assert_eq!(status, "open");
        assert_eq!(runtime_offline_id(&db), Some(1));
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn merge_extends_previous_when_gap_within_window() {
        let path = temp_db("merge");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db, Some(2));
        // 上一条：1000s 前起、990s 前恢复；本次：900s 前起，离线前最后上报 985s 前（在线 5s ≤ 10s）。
        seed_history(
            &db,
            1,
            "closed",
            NOW - 1_000_000_000_000,
            NOW - 990_000_000_000,
        );
        db.write(|tx| {
            tx.execute(
                "UPDATE server_offline_histories SET ended_at = ?1, recovered_at = ?1, duration_seconds = 10, \
                 reason = 'network_disconnect', status = 'closed' WHERE id = 1",
                [unix_nanos_to_sqlite(NOW - 990_000_000_000)],
            )?;
            Ok(())
        })
        .unwrap();
        seed_history(&db, 2, "open", NOW - 900_000_000_000, NOW - 985_000_000_000);
        let host = proto::Host {
            ip: "10.0.0.3".into(),
            boot_time: 100,
            ..Default::default()
        };
        let closed = db
            .write(|tx| {
                close_offline_history_tx(tx, 7, &fields(2), None, Some(&host), NOW, enabled(10))
            })
            .unwrap()
            .expect("merged into previous");
        assert_eq!(closed.id, 1);
        assert_eq!(closed.reason, OFFLINE_REASON_NETWORK_DISCONNECT);
        assert_eq!(closed.ended_at_nanos, NOW);
        assert_eq!(closed.duration_seconds, 1000); // 上一条起点 1000s 前 → 本次恢复点
        let (status, ended, duration, _, recovered_ip) = history_row(&db, 1);
        assert_eq!(status, "closed");
        assert_eq!(ended, Some(unix_nanos_to_sqlite(NOW)));
        assert_eq!(duration, Some(1000));
        assert_eq!(recovered_ip, Some("10.0.0.3".to_string()));
        let remaining: i64 = db
            .read(|conn| {
                Ok(
                    conn.query_row("SELECT COUNT(*) FROM server_offline_histories", [], |r| {
                        r.get(0)
                    })?,
                )
            })
            .unwrap();
        assert_eq!(remaining, 1);
        assert_eq!(runtime_offline_id(&db), None);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn merge_keeps_both_when_gap_exceeds_window() {
        let path = temp_db("merge-gap");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db, Some(2));
        seed_history(
            &db,
            1,
            "closed",
            NOW - 1_000_000_000_000,
            NOW - 990_000_000_000,
        );
        db.write(|tx| {
            tx.execute(
                "UPDATE server_offline_histories SET ended_at = ?1, recovered_at = ?1, duration_seconds = 10, \
                 reason = 'network_disconnect', status = 'closed' WHERE id = 1",
                [unix_nanos_to_sqlite(NOW - 990_000_000_000)],
            )?;
            Ok(())
        })
        .unwrap();
        // 在线 20s > merge gap 10s：不合并。
        seed_history(&db, 2, "open", NOW - 900_000_000_000, NOW - 970_000_000_000);
        let closed = db
            .write(|tx| close_offline_history_tx(tx, 7, &fields(2), None, None, NOW, enabled(10)))
            .unwrap()
            .expect("history closed");
        assert_eq!(closed.id, 2);
        let remaining: i64 = db
            .read(|conn| {
                Ok(
                    conn.query_row("SELECT COUNT(*) FROM server_offline_histories", [], |r| {
                        r.get(0)
                    })?,
                )
            })
            .unwrap();
        assert_eq!(remaining, 2);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn merge_downgrades_reason_when_reasons_differ() {
        let path = temp_db("merge-reason");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db, Some(2));
        seed_history(
            &db,
            1,
            "closed",
            NOW - 1_000_000_000_000,
            NOW - 990_000_000_000,
        );
        db.write(|tx| {
            tx.execute(
                "UPDATE server_offline_histories SET ended_at = ?1, recovered_at = ?1, duration_seconds = 10, \
                 reason = 'machine_reboot', status = 'closed' WHERE id = 1",
                [unix_nanos_to_sqlite(NOW - 990_000_000_000)],
            )?;
            Ok(())
        })
        .unwrap();
        seed_history(&db, 2, "open", NOW - 900_000_000_000, NOW - 985_000_000_000);
        let closed = db
            .write(|tx| close_offline_history_tx(tx, 7, &fields(2), None, None, NOW, enabled(10)))
            .unwrap()
            .unwrap();
        assert_eq!(closed.reason, OFFLINE_REASON_UNKNOWN);
        let (_, _, _, reason, _) = history_row(&db, 1);
        assert_eq!(reason, OFFLINE_REASON_UNKNOWN);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn duration_rounds_half_up() {
        assert_eq!(duration_seconds_between(0, 0), 0);
        assert_eq!(duration_seconds_between(100, 100), 0);
        assert_eq!(duration_seconds_between(0, 1_400_000_000), 1);
        assert_eq!(duration_seconds_between(0, 1_500_000_000), 2);
        assert_eq!(duration_seconds_between(0, 1_600_000_000), 2);
        assert_eq!(duration_seconds_between(0, -1), 0);
    }

    #[test]
    fn detect_reason_matrix() {
        assert_eq!(
            detect_offline_reason(0, 100, 50, 60),
            OFFLINE_REASON_UNKNOWN
        );
        assert_eq!(
            detect_offline_reason(100, 0, 50, 60),
            OFFLINE_REASON_UNKNOWN
        );
        assert_eq!(
            detect_offline_reason(100, 50, 0, 60),
            OFFLINE_REASON_UNKNOWN
        );
        assert_eq!(
            detect_offline_reason(100, 50, 50, 0),
            OFFLINE_REASON_UNKNOWN
        );
        assert_eq!(
            detect_offline_reason(100, 50, 50, 60),
            OFFLINE_REASON_UNKNOWN
        );
        assert_eq!(
            detect_offline_reason(100, 200, 50, 60),
            OFFLINE_REASON_MACHINE_REBOOT
        );
        assert_eq!(
            detect_offline_reason(100, 100, 50, 40),
            OFFLINE_REASON_MACHINE_REBOOT
        );
        assert_eq!(
            detect_offline_reason(100, 100, 50, 60),
            OFFLINE_REASON_NETWORK_DISCONNECT
        );
        assert_eq!(
            detect_offline_reason(100, 100, 50, 50),
            OFFLINE_REASON_NETWORK_DISCONNECT
        );
    }
}
