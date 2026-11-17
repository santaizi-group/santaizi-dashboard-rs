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
use crate::rpc::support::blocking_write;
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

/// `EvaluateAll`（兼容签名，默认按 `"UTC"` 评估）。
pub fn evaluate_all(
    conn: &rusqlite::Connection,
    now_nanos: i64,
    notify: impl FnMut(&str, String, i64),
) -> anyhow::Result<EvaluateOutcome> {
    evaluate_all_in_location(conn, now_nanos, "UTC", notify)
}

/// 按面板配置 `location` 评估所有启用策略：现算 → 窗口翻转重置 → 阈值告警 → upsert 状态。
pub fn evaluate_all_in_location(
    conn: &rusqlite::Connection,
    now_nanos: i64,
    location: &str,
    mut notify: impl FnMut(&str, String, i64),
) -> anyhow::Result<EvaluateOutcome> {
    let mut outcome = EvaluateOutcome::default();
    for policy in traffic::list_enabled(conn, None)? {
        outcome.evaluated += 1;
        let Some(usage) =
            traffic::usage_in_location(conn, &policy, now_nanos, location).unwrap_or(None)
        else {
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
        let tag = crate::notify::normalize_tag(&policy.notification_tag).to_string();
        let server_name: String = conn
            .query_row(
                "SELECT COALESCE(name, '') FROM servers WHERE id = ?1",
                [policy.server_id],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or_default();
        let host_label = if server_name.trim().is_empty() {
            format!("#{}", policy.server_id)
        } else {
            server_name.trim().to_string()
        };
        let dir_label = match policy.direction.as_str() {
            "inbound" | "in" => "入站",
            "outbound" | "out" => "出站",
            _ => "双向",
        };
        let used_human = util_time::format_bytes_human(usage.used_bytes as f64);
        let quota_human = util_time::format_bytes_human(policy.quota_bytes as f64);
        let mut new_warning = warning_notified_at;
        if usage.status == "warning" && warning_notified_at == 0 {
            notify(
                &tag,
                format!(
                    "[流量预警] {host_label} · {}\n{dir_label}流量已用：{used_human} / {quota_human}（{:.1}%，预警阈值 {:.0}%）",
                    policy.name, usage.usage_percent, policy.warning_percent
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
                format!(
                    "[流量超额] {host_label} · {}\n{dir_label}流量已用：{used_human} / {quota_human}（{:.1}%，已超出配额）",
                    policy.name, usage.usage_percent
                ),
                policy.server_id,
            );
            new_exceeded = now_nanos;
            if new_warning == 0 {
                new_warning = now_nanos;
            }
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
                let (plain_ip, location) = {
                    let guard = conf.read().expect("config lock");
                    (guard.enable_plain_ip_in_notification, guard.location.clone())
                };
                let db = db.clone();
                let notifier = Arc::clone(&notifier);
                let _ = plain_ip;
                let now_nanos = util_time::now_nanos();
                // 一轮评估：收集告警 → 事务外逐条进通知引擎（async）。
                let pending = blocking_write(&db, move |tx| {
                    let mut pending = Vec::new();
                    let outcome = evaluate_all_in_location(tx, now_nanos, &location, |tag, message, server_id| {
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

    #[test]
    fn direction_labels_and_quota_expansion_retrigger_alerts() {
        let (db, path) = seed_db("dir-expand");
        let now = util_time::now_nanos();
        db.write(|tx| {
            tx.execute(
                "UPDATE traffic_policies SET direction = 'outbound', quota_bytes = 50 WHERE id = 1",
                [],
            )?;
            let payload = crate::proto::StateRollupPayload {
                window_start_unix_nano: now - 3_600_000_000_000,
                window_end_unix_nano: now - 2 * 3_600_000_000_000,
                sample_count: 1,
                net_in_total: 10,
                net_out_total: 90,
                ..Default::default()
            }
            .encode_to_vec();
            tx.execute(
                "INSERT INTO state_rollups (node_uuid, resolution, window_start, window_end, sample_count, payload, net_in_total, net_out_total) \
                 VALUES (X'21212121212121212121212121212121', '1h', ?1, ?2, 1, ?3, 10, 90)",
                rusqlite::params![
                    now - 3_600_000_000_000,
                    now - 2 * 3_600_000_000_000,
                    payload
                ],
            )?;
            Ok(())
        })
        .unwrap();

        let messages = Mutex::new(Vec::new());
        let first = db
            .write(|tx| {
                evaluate_all_in_location(tx, now, "Asia/Shanghai", |tag, msg, sid| {
                    messages.lock().unwrap().push((tag.to_string(), msg, sid));
                })
            })
            .unwrap();
        assert_eq!(first.exceeded, 1);
        assert!(
            messages.lock().unwrap()[0].1.contains("出站流量已用"),
            "outbound 必须匹配为出站而非双向: {}",
            messages.lock().unwrap()[0].1
        );

        // 扩容配额至 100（90/100 = 90% warning）：应清零告警游标并在下轮正常触发预警通知。
        db.write(|tx| {
            traffic::update_policy(
                tx,
                7,
                1,
                &traffic::PolicyWrite {
                    name: "monthly".into(),
                    direction: "outbound".into(),
                    mode: "cumulative".into(),
                    cycle_start_nanos: util_time::sqlite_to_unix_nanos("2026-01-01 00:00:00+00:00"),
                    cycle_interval: 1,
                    cycle_unit: String::new(),
                    quota_bytes: 100,
                    warning_percent: 80.0,
                    notification_tag: "default".into(),
                    enabled: true,
                },
            )?;
            Ok(())
        })
        .unwrap();

        let after_expand = db
            .write(|tx| {
                evaluate_all_in_location(tx, now + 1, "Asia/Shanghai", |tag, msg, sid| {
                    messages.lock().unwrap().push((tag.to_string(), msg, sid));
                })
            })
            .unwrap();
        assert_eq!(after_expand.warned, 1, "配额扩容后再次越预警线应重新告警");
        let _ = std::fs::remove_file(&path);
    }
}
