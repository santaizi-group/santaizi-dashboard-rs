//! 流量策略评估 worker（Phase 6，对齐 Go `service/traffic/policy.go`
//! `EvaluateAll` + `service/singleton/traffic_policy.go` 的 60s 周期）。
//!
//! 每 60s 对全部启用策略现算用量并写 `traffic_policy_states`：窗口翻转
//! （window_start/end 变化）时重置告警时间戳；warning/exceeded 首次越线
//! 经通知引擎推送（按策略 notification_tag）。

use std::sync::Arc;
use std::time::Duration;

use rusqlite::OptionalExtension as _;

use crate::db::Db;
use crate::repo::traffic;
use crate::rpc::support::blocking_read;
use crate::util::time as util_time;

/// 评估周期（Go `trafficEvalInterval`）。
const EVAL_INTERVAL: Duration = Duration::from_secs(60);

/// 一轮评估的结果（维护面/测试观测）。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EvaluateOutcome {
    pub evaluated: u64,
    pub warned: u64,
    pub exceeded: u64,
}

/// `EvaluateAll`：现算 → 窗口翻转重置 → 阈值告警 → upsert 状态。
pub fn evaluate_all(
    conn: &rusqlite::Connection,
    now_nanos: i64,
    mut notify: impl FnMut(&str, String, i64),
) -> anyhow::Result<EvaluateOutcome> {
    let mut outcome = EvaluateOutcome::default();
    for policy in traffic::list_enabled(conn, None)? {
        outcome.evaluated += 1;
        let Some(usage) = traffic::usage(conn, &policy, now_nanos).unwrap_or(None) else {
            continue;
        };
        let window_start = usage.window_start;
        let window_end = usage.window_end.unwrap_or(0);
        let state: Option<(i64, i64)> = conn
            .query_row(
                "SELECT window_start, window_end FROM traffic_policy_states WHERE policy_id = ?1",
                [policy.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let (warning_notified_at, exceeded_notified_at) = match state {
            Some((start, end)) if start == window_start && end == window_end => {
                let row: (Option<i64>, Option<i64>) = conn
                    .query_row(
                        "SELECT warning_notified_at, exceeded_notified_at \
                         FROM traffic_policy_states WHERE policy_id = ?1",
                        [policy.id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .unwrap_or((None, None));
                (row.0.unwrap_or(0), row.1.unwrap_or(0))
            }
            // 窗口翻转（或首评）：告警状态清零。
            _ => (0, 0),
        };
        let tag = if policy.notification_tag.is_empty() {
            "default".to_string()
        } else {
            policy.notification_tag.clone()
        };
        let mut new_warning = warning_notified_at;
        if usage.status == "warning" && warning_notified_at == 0 {
            notify(
                &tag,
                format!(
                    "{} traffic usage reached {:.1}%",
                    policy.name, usage.usage_percent
                ),
                policy.server_id,
            );
            new_warning = now_nanos;
            outcome.warned += 1;
        }
        let mut new_exceeded = exceeded_notified_at;
        if usage.status == "exceeded" && exceeded_notified_at == 0 {
            notify(
                &tag,
                format!("{} traffic quota exceeded", policy.name),
                policy.server_id,
            );
            new_exceeded = now_nanos;
            outcome.exceeded += 1;
        }
        conn.execute(
            "INSERT INTO traffic_policy_states (policy_id, window_start, window_end, used_bytes, \
                 warning_notified_at, exceeded_notified_at, last_evaluated_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, CURRENT_TIMESTAMP) \
                 ON CONFLICT(policy_id) DO UPDATE SET window_start = ?2, window_end = ?3, \
                 used_bytes = ?4, warning_notified_at = ?5, exceeded_notified_at = ?6, \
                 last_evaluated_at = ?7, updated_at = CURRENT_TIMESTAMP",
            rusqlite::params![
                policy.id,
                window_start,
                window_end,
                usage.used_bytes as i64,
                new_warning,
                new_exceeded,
                now_nanos,
            ],
        )?;
    }
    Ok(outcome)
}

/// 评估 worker：60s 周期 + 启动即评一轮；告警经注入的发送器进通知引擎。
pub async fn run(
    db: Db,
    conf: crate::config::ConfigHandle,
    notifier: Arc<crate::notify::Notifier>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut shutdown = shutdown;
    let mut ticker = tokio::time::interval(EVAL_INTERVAL);
    ticker.tick().await; // 首拍即启动时刻，直接评一轮。
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let plain_ip = conf.read().expect("config lock").enable_plain_ip_in_notification;
                let db = db.clone();
                let notifier = Arc::clone(&notifier);
                let _ = plain_ip;
                let now_nanos = util_time::now_nanos();
                // 一轮评估：收集告警 → 事务外逐条进通知引擎（async）。
                let pending = blocking_read(&db, move |conn| {
                    let mut pending = Vec::new();
                    let outcome = evaluate_all(conn, now_nanos, |tag, message, server_id| {
                        pending.push((tag.to_string(), message, server_id));
                    })?;
                    Ok((outcome, pending))
                })
                .await;
                match pending {
                    Ok((outcome, messages)) => {
                        if outcome.warned + outcome.exceeded > 0 {
                            tracing::info!(
                                evaluated = outcome.evaluated,
                                warned = outcome.warned,
                                exceeded = outcome.exceeded,
                                "流量策略评估完成"
                            );
                        }
                        for (tag, message, server_id) in messages {
                            notifier.send(&tag, &message, None, Some(server_id)).await;
                        }
                    }
                    Err(err) => tracing::warn!(error = %err, "流量策略评估失败"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;
    use prost::Message as _;
    use std::sync::Mutex;

    fn seed_db(tag: &str) -> (Db, String) {
        let path = std::env::temp_dir().join(format!(
            "santaizi-traffic-eval-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let db = migrate::open_primary(path.to_str().unwrap()).unwrap();
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext) VALUES (7, 'node-7', 't', X'11')",
                [],
            )?;
            tx.execute(
                "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason, created_at) \
                 VALUES (7, X'21212121212121212121212121212121', 0, NULL, 1, 'r', CURRENT_TIMESTAMP)",
                [],
            )?;
            // cumulative 策略：quota 100，当前用量由 rollup 求和（此处 0 → normal）。
            tx.execute(
                "INSERT INTO traffic_policies (created_at, server_id, name, direction, mode, cycle_start, quota_bytes, warning_percent, notification_tag, enabled) \
                 VALUES (CURRENT_TIMESTAMP, 7, 'monthly', 'total', 'cumulative', '2026-01-01 00:00:00+00:00', 100, 80, 'default', 1)",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        (db, path.to_string_lossy().into_owned())
    }

    #[test]
    fn evaluate_normal_writes_state_without_notifications() {
        let (db, path) = seed_db("normal");
        let now = util_time::now_nanos();
        let messages = Mutex::new(Vec::new());
        let outcome = db
            .read(|conn| {
                evaluate_all(conn, now, |tag, message, server_id| {
                    messages
                        .lock()
                        .unwrap()
                        .push((tag.to_string(), message, server_id));
                })
            })
            .unwrap();
        assert_eq!(outcome.evaluated, 1);
        assert_eq!(outcome.warned + outcome.exceeded, 0);
        assert!(messages.lock().unwrap().is_empty());
        let (used, status_warning): (i64, Option<i64>) = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT used_bytes, warning_notified_at FROM traffic_policy_states WHERE policy_id = 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?)
            })
            .unwrap();
        assert_eq!(used, 0);
        assert_eq!(status_warning, Some(0));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn evaluate_threshold_fires_once_then_mutes_until_window_flips() {
        let (db, path) = seed_db("threshold");
        // 用量 90/100 = 90% ≥ 80 warning，< 100 exceeded。
        db.write(|tx| {
            let payload = crate::proto::StateRollupPayload {
                window_start_unix_nano: util_time::now_nanos() - 3_600_000_000_000,
                window_end_unix_nano: util_time::now_nanos() - 2 * 3_600_000_000_000,
                sample_count: 1,
                net_in_total: 40,
                net_out_total: 50,
                ..Default::default()
            }
            .encode_to_vec();
            tx.execute(
                "INSERT INTO state_rollups (node_uuid, resolution, window_start, window_end, sample_count, payload, net_in_total, net_out_total) \
                 VALUES (X'21212121212121212121212121212121', '1h', ?1, ?2, 1, ?3, 40, 50)",
                rusqlite::params![
                    util_time::now_nanos() - 3_600_000_000_000,
                    util_time::now_nanos() - 2 * 3_600_000_000_000,
                    payload
                ],
            )?;
            Ok(())
        })
        .unwrap();
        let now = util_time::now_nanos();
        let messages = Mutex::new(Vec::new());
        let first = db
            .read(|conn| {
                evaluate_all(conn, now, |tag, message, server_id| {
                    messages
                        .lock()
                        .unwrap()
                        .push((tag.to_string(), message, server_id));
                })
            })
            .unwrap();
        assert_eq!(first.warned, 1, "首次越线告警");
        assert_eq!(messages.lock().unwrap().len(), 1);
        // 二轮：同窗口不重复告警。
        let second = db
            .read(|conn| {
                evaluate_all(conn, now + 1, |tag, message, server_id| {
                    messages
                        .lock()
                        .unwrap()
                        .push((tag.to_string(), message, server_id));
                })
            })
            .unwrap();
        assert_eq!(second.warned, 0, "同窗口告警只发一次");
        assert_eq!(messages.lock().unwrap().len(), 1);
        let _ = std::fs::remove_file(&path);
    }
}
