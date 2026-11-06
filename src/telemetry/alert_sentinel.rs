//! 规则哨兵：对齐 Go `AlertSentinelStart` / `model.AlertRule.Check`。
//!
//! 检查历史只放在进程内。每轮从库重读规则；规则正文变化时丢掉该规则的历史。

use std::collections::HashMap;

use prost::Message as _;
use rusqlite::Connection;
use serde::Deserialize;

use crate::proto;

const CHECK_FAIL: u8 = 1;
const CHECK_PASS: u8 = 2;
const MODE_ALWAYS: i64 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleNotice {
    pub tag: String,
    pub message: String,
    pub mute_label: String,
    pub clear_label: String,
    pub server_id: i64,
}

#[derive(Default)]
pub struct SentinelMemory {
    points: HashMap<(i64, i64), Vec<Vec<Option<()>>>>,
    previous: HashMap<(i64, i64), u8>,
    fingerprint: HashMap<i64, String>,
}

#[derive(Debug, Deserialize)]
struct Rule {
    #[serde(default)]
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    min: f64,
    #[serde(default)]
    max: f64,
    #[serde(default)]
    duration: u64,
    #[serde(default)]
    cover: u64,
    #[serde(default)]
    ignore: HashMap<u64, bool>,
}

struct ServerSample {
    id: i64,
    name: String,
    last_ip: String,
    /// 主端最近一次收到该节点上报的秒级时间戳（0 = 从未上报）。
    last_seen_unix: i64,
    state: Option<proto::State>,
    host: Option<proto::Host>,
    /// 离线阈值（秒）：与列表 `online`、离线通知同一口径。
    offline_threshold_seconds: u64,
}

pub fn scan(
    conn: &Connection,
    memory: &mut SentinelMemory,
    now_unix: i64,
    offline_threshold_seconds: u64,
) -> anyhow::Result<Vec<RuleNotice>> {
    let rules = load_rules(conn)?;
    let live: HashMap<i64, String> = rules
        .iter()
        .map(|rule| (rule.id, rule.fingerprint.clone()))
        .collect();
    memory.fingerprint.retain(|id, _| live.contains_key(id));
    memory
        .points
        .retain(|(rule_id, _), _| live.contains_key(rule_id));
    memory
        .previous
        .retain(|(rule_id, _), _| live.contains_key(rule_id));
    for rule in &rules {
        if memory.fingerprint.get(&rule.id) != Some(&rule.fingerprint) {
            memory.points.retain(|(id, _), _| *id != rule.id);
            memory.previous.retain(|(id, _), _| *id != rule.id);
            memory.fingerprint.insert(rule.id, rule.fingerprint.clone());
        }
    }
    let servers = load_servers(conn, offline_threshold_seconds.max(10))?;
    let mut notices = Vec::new();
    for rule in rules {
        if !rule.enabled {
            continue;
        }
        let parsed = parse_rules(&rule.rules_raw);
        for server in &servers {
            let point: Vec<Option<()>> = parsed
                .iter()
                .map(|item| item.snapshot(server, now_unix))
                .collect();
            let history = memory.points.entry((rule.id, server.id)).or_default();
            history.push(point);
            let (max_keep, passed) = check(&parsed, history);
            if max_keep > 0 && history.len() > max_keep {
                let drop_count = history.len() - max_keep;
                history.drain(0..drop_count);
            }
            let prev = memory
                .previous
                .get(&(rule.id, server.id))
                .copied()
                .unwrap_or(0);
            if !passed {
                if rule.trigger_mode == MODE_ALWAYS || prev != CHECK_FAIL {
                    memory.previous.insert((rule.id, server.id), CHECK_FAIL);
                    notices.push(RuleNotice {
                        tag: rule.tag.clone(),
                        message: format!(
                            "[RuleIncident] {}({}) {}",
                            server.name, server.last_ip, rule.name
                        ),
                        // 对齐 Go `ServerIncident(alertId, serverId)` → `bf::sei-{rule}-{server}`。
                        mute_label: format!("bf::sei-{}-{}", rule.id, server.id),
                        clear_label: format!("bf::seir-{}-{}", rule.id, server.id),
                        server_id: server.id,
                    });
                }
            } else {
                if prev == CHECK_FAIL {
                    notices.push(RuleNotice {
                        tag: rule.tag.clone(),
                        message: format!(
                            "[RuleResolved] {}({}) {}",
                            server.name, server.last_ip, rule.name
                        ),
                        mute_label: format!("bf::seir-{}-{}", rule.id, server.id),
                        clear_label: format!("bf::sei-{}-{}", rule.id, server.id),
                        server_id: server.id,
                    });
                }
                memory.previous.insert((rule.id, server.id), CHECK_PASS);
            }
        }
    }
    Ok(notices)
}

/// 把哨兵里的占位词换成当前语言的「规则触发 / 规则恢复」。
/// 前缀不在聚合白名单里，规则通知直接投递，不会被 15 秒窗口吃掉。
pub fn localize_notice(notice: &RuleNotice, incident: &str, resolved: &str) -> RuleNotice {
    let mut notice = notice.clone();
    notice.message = notice
        .message
        .replacen("[RuleIncident]", &format!("[{incident}]"), 1)
        .replacen("[RuleResolved]", &format!("[{resolved}]"), 1);
    notice
}

struct StoredRule {
    id: i64,
    name: String,
    rules_raw: String,
    enabled: bool,
    trigger_mode: i64,
    tag: String,
    fingerprint: String,
}

fn load_rules(conn: &Connection) -> anyhow::Result<Vec<StoredRule>> {
    let mut stmt = conn.prepare(
        "SELECT id, COALESCE(name, ''), COALESCE(rules_raw, ''), enable, COALESCE(trigger_mode, 0), \
         COALESCE(notification_tag, '') FROM alert_rules WHERE deleted_at IS NULL",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, String>(5)?,
        ))
    })?;
    let mut raw = Vec::new();
    for row in rows {
        raw.push(row?);
    }
    drop(stmt);
    let mut rules = Vec::new();
    for (id, name, rules_raw, enable, trigger_mode, mut tag) in raw {
        if tag.trim().is_empty() {
            tag = "default".to_string();
            conn.execute(
                "UPDATE alert_rules SET notification_tag = 'default' WHERE id = ?1 AND COALESCE(notification_tag, '') = ''",
                [id],
            )?;
        }
        let fingerprint = format!("{rules_raw}\n{trigger_mode}\n{tag}");
        rules.push(StoredRule {
            id,
            name,
            rules_raw,
            enabled: enable == Some(1),
            trigger_mode,
            tag,
            fingerprint,
        });
    }
    Ok(rules)
}

impl ServerSample {
    /// 与列表 `online` / 离线通知同一口径：最近一次上报超过阈值即离线。
    /// 从未上报（`last_seen = 0`）不算离线，避免新建主机立刻报警。
    fn offline_now(&self, now_unix: i64) -> bool {
        if self.state.is_none() {
            return false;
        }
        self.last_seen_unix > 0
            && now_unix.saturating_sub(self.last_seen_unix) >= self.offline_threshold_seconds as i64
    }
}

fn load_servers(conn: &Connection, threshold_seconds: u64) -> anyhow::Result<Vec<ServerSample>> {
    let mut stmt = conn.prepare(
        "SELECT s.id, COALESCE(s.name, ''), COALESCE(r.last_ip, ''), r.last_seen_at, \
         r.state_payload, r.host_payload \
         FROM servers s \
         LEFT JOIN server_runtimes r ON r.server_id = s.id \
         WHERE s.deleted_at IS NULL",
    )?;
    let rows = stmt.query_map([], |row| {
        let state_blob: Option<Vec<u8>> = row.get(4)?;
        let host_blob: Option<Vec<u8>> = row.get(5)?;
        Ok(ServerSample {
            id: row.get(0)?,
            name: row.get(1)?,
            last_ip: row.get(2)?,
            last_seen_unix: last_seen_nanos(row, 3)? / 1_000_000_000,
            state: state_blob.and_then(|blob| proto::State::decode(blob.as_slice()).ok()),
            host: host_blob.and_then(|blob| proto::Host::decode(blob.as_slice()).ok()),
            offline_threshold_seconds: threshold_seconds,
        })
    })?;
    let mut servers = Vec::new();
    for row in rows {
        servers.push(row?);
    }
    Ok(servers)
}

/// `server_runtimes.last_seen_at` 是 `datetime` 列：生产写入 SQLite 文本日期
/// （`unix_nanos_to_sqlite`），旧数据/夹具可能直接写整数纳秒。两种都要读得出来，
/// 否则规则哨兵会在真实主机上每 3 秒报一次 `Invalid column type`。
fn last_seen_nanos(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<i64> {
    use rusqlite::types::ValueRef;
    Ok(match row.get_ref(index)? {
        ValueRef::Null => 0,
        ValueRef::Integer(value) => value,
        ValueRef::Real(value) => value as i64,
        ValueRef::Text(bytes) => std::str::from_utf8(bytes)
            .ok()
            .and_then(crate::util::time::sqlite_to_unix_nanos)
            .unwrap_or(0),
        ValueRef::Blob(_) => 0,
    })
}

fn parse_rules(raw: &str) -> Vec<Rule> {
    if raw.trim().is_empty() {
        return Vec::new();
    }
    serde_json::from_str(raw).unwrap_or_default()
}

impl Rule {
    fn snapshot(&self, server: &ServerSample, now_unix: i64) -> Option<()> {
        if self.cover == 0 && self.ignore.get(&u64::try_from(server.id).unwrap_or(0)) == Some(&true)
        {
            return None;
        }
        if self.cover == 1 && self.ignore.get(&u64::try_from(server.id).unwrap_or(0)) != Some(&true)
        {
            return None;
        }
        let Some(state) = &server.state else {
            return None;
        };
        let host = server.host.as_ref();
        let value = match self.kind.as_str() {
            "cpu" => state.cpu,
            "gpu" => state.gpu,
            "memory" => percentage(state.mem_used, host.map(|host| host.mem_total).unwrap_or(0)),
            "swap" => percentage(
                state.swap_used,
                host.map(|host| host.swap_total).unwrap_or(0),
            ),
            "disk" => percentage(
                state.disk_used,
                host.map(|host| host.disk_total).unwrap_or(0),
            ),
            "net_in_speed" => state.net_in_speed as f64,
            "net_out_speed" => state.net_out_speed as f64,
            "net_all_speed" => (state.net_in_speed + state.net_out_speed) as f64,
            "offline" => return server.offline_now(now_unix).then_some(()),
            "load1" => state.load1,
            "load5" => state.load5,
            "load15" => state.load15,
            "tcp_conn_count" => state.tcp_conn_count as f64,
            "udp_conn_count" => state.udp_conn_count as f64,
            "process_count" => state.process_count as f64,
            "temperature_max" => state
                .temperatures
                .iter()
                .map(|sensor| sensor.temperature)
                .filter(|value| *value != 0.0)
                .fold(None, |max: Option<f64>, value| {
                    Some(max.map_or(value, |current| current.max(value)))
                })
                .unwrap_or(0.0),
            _ => return None,
        };
        if (self.max > 0.0 && value > self.max) || (self.min > 0.0 && value < self.min) {
            Some(())
        } else {
            None
        }
    }
}

fn percentage(used: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        used as f64 * 100.0 / total as f64
    }
}

/// 哨兵每 3 秒采一次。界面把 `duration` 标成秒，评估却按采样次数用，
/// 填 60 要等约 180 秒。按秒换算，至少保留 1 次采样。
const SAMPLE_SECONDS: u64 = 3;

fn samples_for(duration_seconds: u64) -> usize {
    (duration_seconds / SAMPLE_SECONDS).max(1) as usize
}

fn check(rules: &[Rule], points: &[Vec<Option<()>>]) -> (usize, bool) {
    let mut max_keep = 0usize;
    let mut failed_rules = 0usize;
    for (index, rule) in rules.iter().enumerate() {
        let need = samples_for(rule.duration);
        max_keep = max_keep.max(need);
        if points.len() < need {
            continue;
        }
        let mut total = 0.0;
        let mut failed = 0.0;
        for point in points.iter().rev().take(need) {
            total += 1.0;
            if point.get(index).is_some_and(Option::is_some) {
                failed += 1.0;
            }
        }
        if total > 0.0 && failed / total > 0.7 {
            failed_rules += 1;
        }
    }
    (max_keep, failed_rules != rules.len())
}

pub async fn run(
    db: crate::db::Db,
    conf: crate::config::ConfigHandle,
    notifier: std::sync::Arc<crate::notify::Notifier>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut memory = SentinelMemory::default();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(3)) => {
                let threshold = conf.read().expect("config lock").offline_threshold_seconds;
                let scanned = db.read(|conn| scan(conn, &mut memory, crate::util::time::now_nanos() / 1_000_000_000, threshold));
                match scanned {
                    Ok(notices) => {
                        for notice in notices {
                            let notice = localize_notice(&notice, &notifier.t("RuleIncident"), &notifier.t("RuleResolved"));
                            notifier.unmute(&notice.tag, &notice.clear_label);
                            notifier
                                .send(&notice.tag, &notice.message, Some(notice.mute_label), Some(notice.server_id))
                                .await;
                        }
                    }
                    Err(err) => tracing::warn!("alert sentinel: {err:#}"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;
    use rusqlite::params;

    fn temp_db() -> crate::db::Db {
        let dir = std::env::temp_dir().join(format!(
            "santaizi-sentinel-{}-{}",
            std::process::id(),
            crate::util::time::now_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        migrate::open_primary(dir.join("panel.db").to_str().unwrap()).unwrap()
    }

    #[test]
    fn onetime_cpu_rule_alerts_once_then_resolves() {
        let db = temp_db();
        let state = proto::State {
            cpu: 95.0,
            ..Default::default()
        };
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, secret_ciphertext) VALUES (3, 'edge', X'00')",
                [],
            )?;
            tx.execute(
                "INSERT INTO server_runtimes (server_id, last_ip, last_seen_at, state_payload) VALUES (3, '203.0.113.9', ?1, ?2)",
                params![
                    crate::util::time::unix_nanos_to_sqlite(crate::util::time::now_nanos()),
                    state.encode_to_vec()
                ],
            )?;
            tx.execute(
                "INSERT INTO alert_rules (id, name, rules_raw, enable, trigger_mode, notification_tag) \
                 VALUES (4, 'cpu-high', '[{\"type\":\"cpu\",\"max\":80,\"duration\":1}]', 1, 1, 'ops')",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        let mut memory = SentinelMemory::default();
        let now = crate::util::time::now_nanos() / 1_000_000_000;
        let first = db.read(|conn| scan(conn, &mut memory, now, 30)).unwrap();
        assert_eq!(first.len(), 1);
        assert!(first[0].message.contains("[RuleIncident]"));
        assert!(first[0].message.contains("edge"));
        let second = db.read(|conn| scan(conn, &mut memory, now, 30)).unwrap();
        assert!(second.is_empty());
        db.write(|tx| {
            let calm = proto::State {
                cpu: 10.0,
                ..Default::default()
            };
            tx.execute(
                "UPDATE server_runtimes SET state_payload = ?1 WHERE server_id = 3",
                [calm.encode_to_vec()],
            )?;
            Ok(())
        })
        .unwrap();
        let resolved = db.read(|conn| scan(conn, &mut memory, now, 30)).unwrap();
        assert_eq!(resolved.len(), 1);
        assert!(resolved[0].message.contains("[RuleResolved]"));
    }

    /// 插一台有运行态的主机：`last_seen_ago_secs` 为 None 表示从未上报。
    /// 灌 state_payload：`Rule::snapshot` 要求 State 非空。
    fn seed_host(db: &crate::db::Db, last_seen_ago_secs: Option<i64>) {
        let now = crate::util::time::now_nanos();
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, secret_ciphertext) VALUES (9, 'node-a', X'00')",
                [],
            )?;
            let state = proto::State {
                cpu: 1.0,
                ..Default::default()
            };
            tx.execute(
                "INSERT INTO server_runtimes (server_id, protocol, last_ip, last_seen_at, state_payload) \
                 VALUES (9, 'v2', '203.0.113.1', ?1, ?2)",
                params![
                    last_seen_ago_secs.map(|secs| crate::util::time::unix_nanos_to_sqlite(
                        now - secs * 1_000_000_000
                    )),
                    state.encode_to_vec()
                ],
            )?;
            tx.execute(
                "INSERT INTO alert_rules (id, name, rules_raw, enable, trigger_mode, notification_tag) \
                 VALUES (10, 'offline', '[{\"type\":\"offline\"}]', 1, 1, 'ops')",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    }

    fn offline_notice_count(db: &crate::db::Db) -> usize {
        let mut memory = SentinelMemory::default();
        let now = crate::util::time::now_nanos() / 1_000_000_000;
        db.read(|conn| scan(conn, &mut memory, now, 30))
            .unwrap()
            .len()
    }

    #[test]
    fn offline_rule_fires_after_report_gap() {
        let db = temp_db();
        seed_host(&db, Some(60));
        assert_eq!(
            offline_notice_count(&db),
            1,
            "停报超过阈值必须触发 offline 规则"
        );
    }

    #[test]
    fn offline_rule_ignores_a_fresh_report() {
        let db = temp_db();
        seed_host(&db, Some(5));
        assert_eq!(offline_notice_count(&db), 0, "刚上报过的主机不算离线");
    }

    #[test]
    fn reads_last_seen_written_as_sqlite_datetime() {
        // 生产写入是 `datetime` 文本；读成 i64 会 Invalid column type，规则整条哑火。
        let db = temp_db();
        seed_host(&db, Some(60));
        let stored: String = db
            .read(|conn| {
                conn.query_row(
                    "SELECT last_seen_at FROM server_runtimes WHERE server_id = 9",
                    [],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert!(stored.contains('+'), "夹具应写文本日期：{stored}");
        assert_eq!(offline_notice_count(&db), 1);
    }

    #[test]
    fn offline_rule_ignores_never_reported_hosts() {
        let db = temp_db();
        seed_host(&db, None);
        assert_eq!(
            offline_notice_count(&db),
            0,
            "从未上报的新建主机不得被 offline 规则点名"
        );
    }

    #[test]
    fn offline_rule_does_not_follow_availability_buckets() {
        // 可用性桶说 offline：只要主端还在收报，规则不触发。
        let db = temp_db();
        seed_host(&db, Some(5));
        db.write(|tx| {
            tx.execute(
                "INSERT INTO availability_buckets (node_uuid, bucket_start, window_end, resolution, \
                 host_state, connectivity_state, expected_observers, healthy_observers, \
                 seen_observers, observer_summary, revision, finalized, recalculated_at) \
                 VALUES (X'09090909090909090909090909090909', 0, ?1, 'minute', 'offline', 'unavailable', \
                 1, 1, 0, X'5b5d', 1, 1, ?1)",
                [crate::util::time::now_nanos() - 60_000_000_000i64],
            )?;
            Ok(())
        })
        .unwrap();
        assert_eq!(offline_notice_count(&db), 0);

        // 桶说 online：主端已经停报，规则照样触发。
        db.write(|tx| {
            tx.execute("DELETE FROM availability_buckets", [])?;
            tx.execute(
                "INSERT INTO availability_buckets (node_uuid, bucket_start, window_end, resolution, \
                 host_state, connectivity_state, expected_observers, healthy_observers, \
                 seen_observers, observer_summary, revision, finalized, recalculated_at) \
                 VALUES (X'09090909090909090909090909090909', 0, ?1, 'minute', 'online', 'full', \
                 1, 1, 1, X'5b5d', 1, 1, ?1)",
                [crate::util::time::now_nanos() - 60_000_000_000i64],
            )?;
            tx.execute(
                "UPDATE server_runtimes SET last_seen_at = ?1 WHERE server_id = 9",
                [crate::util::time::now_nanos() - 60_000_000_000i64],
            )?;
            Ok(())
        })
        .unwrap();
        assert_eq!(offline_notice_count(&db), 1);
    }

    #[test]
    fn duration_is_seconds_not_sample_count() {
        // 界面写「60 秒」。按采样次数要等 60 轮（约 180 秒）；按秒只要 20 轮。
        let db = temp_db();
        seed_host(&db, Some(60));
        db.write(|tx| {
            tx.execute(
                "UPDATE alert_rules SET rules_raw = '[{\"type\":\"offline\",\"duration\":60}]' WHERE id = 10",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        let mut memory = SentinelMemory::default();
        let now = crate::util::time::now_nanos() / 1_000_000_000;
        let mut fired_at = None;
        for round in 1..=25 {
            let notices = db.read(|conn| scan(conn, &mut memory, now, 30)).unwrap();
            if !notices.is_empty() {
                fired_at = Some(round);
                break;
            }
        }
        assert_eq!(fired_at, Some(20), "60 秒应按 3 秒一轮换成 20 次采样");
    }
}
