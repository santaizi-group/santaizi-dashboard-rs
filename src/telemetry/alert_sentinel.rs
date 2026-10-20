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
    last_seen_unix: i64,
    state: Option<proto::State>,
    host: Option<proto::Host>,
}

pub fn scan(
    conn: &Connection,
    memory: &mut SentinelMemory,
    now_unix: i64,
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
    let servers = load_servers(conn)?;
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
                            "[{}] {}({}) {}",
                            "Incident", server.name, server.last_ip, rule.name
                        ),
                        mute_label: format!("bf::sei-{}-{}", server.id, rule.id),
                        clear_label: format!("bf::seir-{}-{}", server.id, rule.id),
                        server_id: server.id,
                    });
                }
            } else {
                if prev == CHECK_FAIL {
                    notices.push(RuleNotice {
                        tag: rule.tag.clone(),
                        message: format!(
                            "[{}] {}({}) {}",
                            "Resolved", server.name, server.last_ip, rule.name
                        ),
                        mute_label: format!("bf::seir-{}-{}", server.id, rule.id),
                        clear_label: format!("bf::sei-{}-{}", server.id, rule.id),
                        server_id: server.id,
                    });
                }
                memory.previous.insert((rule.id, server.id), CHECK_PASS);
            }
        }
    }
    Ok(notices)
}

/// 把哨兵里的占位词换成当前语言的「事件 / 恢复」。
pub fn localize_notice(notice: &RuleNotice, incident: &str, resolved: &str) -> RuleNotice {
    let mut notice = notice.clone();
    notice.message = notice
        .message
        .replacen("[Incident]", &format!("[{incident}]"), 1)
        .replacen("[Resolved]", &format!("[{resolved}]"), 1);
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

fn load_servers(conn: &Connection) -> anyhow::Result<Vec<ServerSample>> {
    let mut stmt = conn.prepare(
        "SELECT s.id, COALESCE(s.name, ''), COALESCE(r.last_ip, ''), COALESCE(r.last_seen_at, 0), \
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
            last_seen_unix: row.get::<_, i64>(3)? / 1_000_000_000,
            state: state_blob.and_then(|blob| proto::State::decode(blob.as_slice()).ok()),
            host: host_blob.and_then(|blob| proto::Host::decode(blob.as_slice()).ok()),
        })
    })?;
    let mut servers = Vec::new();
    for row in rows {
        servers.push(row?);
    }
    Ok(servers)
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
            "offline" => {
                if server.last_seen_unix == 0 || now_unix.saturating_sub(server.last_seen_unix) > 6
                {
                    return Some(());
                }
                return None;
            }
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

fn check(rules: &[Rule], points: &[Vec<Option<()>>]) -> (usize, bool) {
    let mut max_keep = 0usize;
    let mut failed_rules = 0usize;
    for (index, rule) in rules.iter().enumerate() {
        let need = (rule.duration as usize).max(1);
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
                let scanned = db.read(|conn| scan(conn, &mut memory, crate::util::time::now_nanos() / 1_000_000_000));
                match scanned {
                    Ok(notices) => {
                        for notice in notices {
                            let notice = localize_notice(&notice, &notifier.t("Incident"), &notifier.t("Resolved"));
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
                params![crate::util::time::now_nanos(), state.encode_to_vec()],
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
        let first = db.read(|conn| scan(conn, &mut memory, now)).unwrap();
        assert_eq!(first.len(), 1);
        assert!(first[0].message.contains("[Incident]"));
        assert!(first[0].message.contains("edge"));
        let second = db.read(|conn| scan(conn, &mut memory, now)).unwrap();
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
        let resolved = db.read(|conn| scan(conn, &mut memory, now)).unwrap();
        assert_eq!(resolved.len(), 1);
        assert!(resolved[0].message.contains("[Resolved]"));
    }
}
