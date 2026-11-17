//! 规则哨兵：对齐 Go `AlertSentinelStart` / `model.AlertRule.Check`。
//!
//! 检查历史只放在进程内。每轮从库重读规则；规则正文变化时丢掉该规则的历史。

use std::collections::{HashMap, HashSet};

use prost::Message as _;
use rusqlite::Connection;
use serde::Deserialize;

use crate::proto;
use crate::util::time as util_time;

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

#[derive(Debug, Clone)]
pub struct ScanOptions {
    pub offline_threshold_seconds: u64,
    pub plain_ip: bool,
    pub location: String,
    /// 启动宽限期内为 false：阻止原本非离线的主机在探针重连前误触发新的 `offline` 规则，
    /// 但允许启动前已离线（预热为 `CHECK_FAIL`）的主机正常触发恢复。
    pub allow_new_offline: bool,
}

impl ScanOptions {
    pub fn new(offline_threshold_seconds: u64) -> Self {
        Self {
            offline_threshold_seconds,
            plain_ip: false,
            location: "Asia/Shanghai".to_string(),
            allow_new_offline: true,
        }
    }
}

#[derive(Default)]
pub struct SentinelMemory {
    points: HashMap<(i64, i64), Vec<Vec<Option<f64>>>>,
    previous: HashMap<(i64, i64), u8>,
    started_at: HashMap<(i64, i64), i64>,
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
    runtime_status: String,
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
    scan_with_options(
        conn,
        memory,
        now_unix,
        &ScanOptions::new(offline_threshold_seconds),
    )
}

/// 面板启动时预热已处于离线状态的主机与 `offline` 规则，避免重启后对存量离线机器重复轰炸
/// `once` 规则告警，同时保证这些存量离线机器恢复时能正常发送 `[规则恢复]`。
pub fn seed_initial_memory(
    conn: &Connection,
    memory: &mut SentinelMemory,
    now_unix: i64,
    offline_threshold_seconds: u64,
) -> anyhow::Result<()> {
    let threshold = offline_threshold_seconds.max(10);
    let rules = load_rules(conn)?;
    let servers = load_servers(conn, threshold)?;
    for rule in &rules {
        memory.fingerprint.insert(rule.id, rule.fingerprint.clone());
        if !rule.enabled {
            continue;
        }
        let parsed = parse_rules(&rule.rules_raw);
        if parsed.is_empty() || !parsed.iter().all(|item| item.kind == "offline") {
            continue;
        }
        let max_keep = parsed
            .iter()
            .map(|item| samples_for(item.duration))
            .max()
            .unwrap_or(1);
        for server in &servers {
            if !server.runtime_status.eq_ignore_ascii_case("offline") {
                continue;
            }
            if parsed
                .iter()
                .all(|item| item.snapshot(server, now_unix).is_some())
            {
                let key = (rule.id, server.id);
                memory
                    .points
                    .insert(key, vec![vec![Some(0.0); parsed.len()]; max_keep]);
                memory.previous.insert(key, CHECK_FAIL);
                let start = if server.last_seen_unix > 0 && server.last_seen_unix <= now_unix {
                    server.last_seen_unix
                } else {
                    now_unix
                };
                memory.started_at.insert(key, start);
            }
        }
    }
    Ok(())
}

pub fn scan_with_options(
    conn: &Connection,
    memory: &mut SentinelMemory,
    now_unix: i64,
    options: &ScanOptions,
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
    memory
        .started_at
        .retain(|(rule_id, _), _| live.contains_key(rule_id));
    for rule in &rules {
        if memory.fingerprint.get(&rule.id) != Some(&rule.fingerprint) {
            memory.points.retain(|(id, _), _| *id != rule.id);
            memory.previous.retain(|(id, _), _| *id != rule.id);
            memory.started_at.retain(|(id, _), _| *id != rule.id);
            memory.fingerprint.insert(rule.id, rule.fingerprint.clone());
        }
    }
    let servers = load_servers(conn, options.offline_threshold_seconds.max(10))?;
    let live_servers: HashSet<i64> = servers.iter().map(|server| server.id).collect();
    memory
        .points
        .retain(|(_, server_id), _| live_servers.contains(server_id));
    memory
        .previous
        .retain(|(_, server_id), _| live_servers.contains(server_id));
    memory
        .started_at
        .retain(|(_, server_id), _| live_servers.contains(server_id));

    let mut notices = Vec::new();
    for rule in rules {
        if !rule.enabled {
            continue;
        }
        let parsed = parse_rules(&rule.rules_raw);
        if parsed.is_empty() {
            continue;
        }
        let all_offline = parsed.iter().all(|item| item.kind == "offline");
        for server in &servers {
            // 主机已停报离线时，`state_payload` 是死机前冻结的最后快照：
            // 非 `offline` 指标不得对冻结值继续做 `MODE_ALWAYS` 无限复报，也不得在死机期间误判恢复。
            if !all_offline && server.offline_now(now_unix) {
                continue;
            }
            let key = (rule.id, server.id);
            let prev = memory.previous.get(&key).copied().unwrap_or(0);
            let point: Vec<Option<f64>> = parsed
                .iter()
                .map(|item| {
                    if item.kind == "offline" && !options.allow_new_offline && prev != CHECK_FAIL {
                        return None;
                    }
                    item.snapshot(server, now_unix)
                })
                .collect();
            let history = memory.points.entry(key).or_default();
            // 离散 `offline` 条件一旦收到新鲜上报（`point[idx].is_none()`），立即清空该条件的历史故障点：
            // 1) 已触发告警的主机上线即可在当轮立刻发出 `[规则恢复]`，不必多等约 18 秒导致短暂恢复漏报；
            // 2) 未满持续时间的短暂抖动在恢复心跳后归零，不会把残留计数带入下一次断联。
            for (idx, item) in parsed.iter().enumerate() {
                if item.kind == "offline" && point.get(idx).is_some_and(Option::is_none) {
                    for past in history.iter_mut() {
                        if let Some(slot) = past.get_mut(idx) {
                            *slot = None;
                        }
                    }
                }
            }
            history.push(point);
            let (max_keep, passed) = check(&parsed, history);
            if max_keep > 0 && history.len() > max_keep {
                let drop_count = history.len() - max_keep;
                history.drain(0..drop_count);
            }
            if !passed {
                if prev != CHECK_FAIL {
                    let start = if all_offline
                        && server.last_seen_unix > 0
                        && server.last_seen_unix <= now_unix
                    {
                        server.last_seen_unix
                    } else {
                        now_unix
                    };
                    memory.started_at.insert(key, start);
                }
                if rule.trigger_mode == MODE_ALWAYS || prev != CHECK_FAIL {
                    memory.previous.insert(key, CHECK_FAIL);
                    notices.push(RuleNotice {
                        tag: rule.tag.clone(),
                        message: format_incident_message(
                            &rule, &parsed, history, server, now_unix, options,
                        ),
                        // 对齐 Go `ServerIncident(alertId, serverId)` → `bf::sei-{rule}-{server}`。
                        mute_label: format!("bf::sei-{}-{}", rule.id, server.id),
                        clear_label: format!("bf::seir-{}-{}", rule.id, server.id),
                        server_id: server.id,
                    });
                }
            } else {
                let started = memory.started_at.remove(&key);
                if prev == CHECK_FAIL {
                    notices.push(RuleNotice {
                        tag: rule.tag.clone(),
                        message: format_resolved_message(
                            &rule, &parsed, server, now_unix, started, options,
                        ),
                        mute_label: format!("bf::seir-{}-{}", rule.id, server.id),
                        clear_label: format!("bf::sei-{}-{}", rule.id, server.id),
                        server_id: server.id,
                    });
                }
                memory.previous.insert(key, CHECK_PASS);
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
        let enabled = enable == Some(1);
        let fingerprint = format!("{rules_raw}\n{trigger_mode}\n{tag}\n{}", u8::from(enabled));
        rules.push(StoredRule {
            id,
            name,
            rules_raw,
            enabled,
            trigger_mode,
            tag,
            fingerprint,
        });
    }
    Ok(rules)
}

impl ServerSample {
    /// 与列表 `online` / 离线通知同一口径：最近一次上报超过阈值即离线。
    /// 从未上报（`last_seen = 0`）不算离线，避免新建主机立刻报警；
    /// 即使 `state_payload` 为空（例如仅上报过 Host 元信息），只要曾上报并超时同样判离线。
    fn offline_now(&self, now_unix: i64) -> bool {
        self.last_seen_unix > 0
            && now_unix.saturating_sub(self.last_seen_unix) >= self.offline_threshold_seconds as i64
    }
}

fn load_servers(conn: &Connection, threshold_seconds: u64) -> anyhow::Result<Vec<ServerSample>> {
    let mut stmt = conn.prepare(
        "SELECT s.id, COALESCE(s.name, ''), COALESCE(r.last_ip, ''), COALESCE(r.status, ''), \
         r.last_seen_at, r.state_payload, r.host_payload \
         FROM servers s \
         LEFT JOIN server_runtimes r ON r.server_id = s.id \
         WHERE s.deleted_at IS NULL",
    )?;
    let rows = stmt.query_map([], |row| {
        let state_blob: Option<Vec<u8>> = row.get(5)?;
        let host_blob: Option<Vec<u8>> = row.get(6)?;
        Ok(ServerSample {
            id: row.get(0)?,
            name: row.get(1)?,
            last_ip: row.get(2)?,
            runtime_status: row.get(3)?,
            last_seen_unix: last_seen_nanos(row, 4)? / 1_000_000_000,
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
    fn covers_server(&self, server_id: i64) -> bool {
        let id = u64::try_from(server_id).unwrap_or(0);
        if self.cover == 0 && self.ignore.get(&id) == Some(&true) {
            return false;
        }
        if self.cover == 1 && self.ignore.get(&id) != Some(&true) {
            return false;
        }
        true
    }

    fn metric_value(&self, server: &ServerSample) -> Option<f64> {
        let state = server.state.as_ref()?;
        let host = server.host.as_ref();
        let value = match self.kind.as_str() {
            "cpu" => state.cpu,
            "gpu" => state.gpu,
            "memory" => {
                let total = host.map(|h| h.mem_total).unwrap_or(0);
                if total == 0 && self.min > 0.0 {
                    return None;
                }
                percentage(state.mem_used, total)
            }
            "swap" => {
                let total = host.map(|h| h.swap_total).unwrap_or(0);
                if total == 0 && self.min > 0.0 {
                    return None;
                }
                percentage(state.swap_used, total)
            }
            "disk" => {
                let total = host.map(|h| h.disk_total).unwrap_or(0);
                if total == 0 && self.min > 0.0 {
                    return None;
                }
                percentage(state.disk_used, total)
            }
            "net_in_speed" => state.net_in_speed as f64,
            "net_out_speed" => state.net_out_speed as f64,
            "net_all_speed" => state.net_in_speed.saturating_add(state.net_out_speed) as f64,
            "load1" => state.load1,
            "load5" => state.load5,
            "load15" => state.load15,
            "tcp_conn_count" => state.tcp_conn_count as f64,
            "udp_conn_count" => state.udp_conn_count as f64,
            "process_count" => state.process_count as f64,
            "temperature_max" => {
                let max_temp = state
                    .temperatures
                    .iter()
                    .map(|sensor| sensor.temperature)
                    .filter(|value| value.is_finite() && *value != 0.0)
                    .fold(None, |max: Option<f64>, value| {
                        Some(max.map_or(value, |current| current.max(value)))
                    });
                match max_temp {
                    Some(v) => v,
                    None if self.min > 0.0 => return None,
                    None => 0.0,
                }
            }
            _ => return None,
        };
        value.is_finite().then_some(value)
    }

    fn snapshot(&self, server: &ServerSample, now_unix: i64) -> Option<f64> {
        if !self.covers_server(server.id) {
            return None;
        }
        if self.kind == "offline" {
            return server.offline_now(now_unix).then_some(0.0);
        }
        let value = self.metric_value(server)?;
        if (self.max > 0.0 && value > self.max) || (self.min > 0.0 && value < self.min) {
            Some(value)
        } else {
            None
        }
    }
}

fn format_server_header(server: &ServerSample, plain_ip: bool) -> String {
    let name = if server.name.trim().is_empty() {
        format!("#{}", server.id)
    } else {
        server.name.trim().to_string()
    };
    let ip = crate::notify::desensitize_ip(server.last_ip.trim(), plain_ip);
    if ip.is_empty() {
        name
    } else {
        format!("{name} ({ip})")
    }
}

fn metric_label_zh(kind: &str) -> &str {
    match kind {
        "cpu" => "CPU",
        "gpu" => "GPU",
        "temperature_max" => "最高温度",
        "memory" => "内存",
        "swap" => "Swap",
        "disk" => "磁盘",
        "net_in_speed" => "入站速率",
        "net_out_speed" => "出站速率",
        "net_all_speed" => "总速率",
        "offline" => "离线",
        "load1" => "1分钟负载",
        "load5" => "5分钟负载",
        "load15" => "15分钟负载",
        "tcp_conn_count" => "TCP连接数",
        "udp_conn_count" => "UDP连接数",
        "process_count" => "进程数",
        other => other,
    }
}

fn format_metric_value(kind: &str, value: f64) -> String {
    match kind {
        "cpu" | "gpu" | "memory" | "swap" | "disk" => format!("{value:.1}%"),
        "temperature_max" => format!("{value:.1}°C"),
        "net_in_speed" | "net_out_speed" | "net_all_speed" => {
            format!("{}/s", util_time::format_bytes_human(value.max(0.0)))
        }
        "load1" | "load5" | "load15" => format!("{value:.2}"),
        "tcp_conn_count" | "udp_conn_count" | "process_count" => {
            format!("{}", value.max(0.0).round() as u64)
        }
        _ => format!("{value:.1}"),
    }
}

fn format_incident_message(
    rule: &StoredRule,
    parsed: &[Rule],
    history: &[Vec<Option<f64>>],
    server: &ServerSample,
    now_unix: i64,
    options: &ScanOptions,
) -> String {
    let header = format_server_header(server, options.plain_ip);
    let mut details = Vec::with_capacity(parsed.len());
    for (idx, item) in parsed.iter().enumerate() {
        if item.kind == "offline" {
            let gap = now_unix.saturating_sub(server.last_seen_unix).max(0) as u64;
            let seen =
                util_time::format_unix_secs_in_location(server.last_seen_unix, &options.location);
            details.push(format!(
                "离线（已停报 {}，最后上报 {seen}）",
                util_time::format_duration_zh(gap)
            ));
        } else if let Some(val) = item.metric_value(server) {
            let label = metric_label_zh(&item.kind);
            let cur = format_metric_value(&item.kind, val);
            let need = samples_for(item.duration);
            let window_breaches: Vec<f64> = history
                .iter()
                .rev()
                .take(need)
                .filter_map(|point| point.get(idx).and_then(|slot| *slot))
                .collect();
            if item.max > 0.0 && val > item.max {
                details.push(format!(
                    "{label} {cur}（超上限 {}）",
                    format_metric_value(&item.kind, item.max)
                ));
            } else if item.min > 0.0 && val < item.min {
                details.push(format!(
                    "{label} {cur}（低于下限 {}）",
                    format_metric_value(&item.kind, item.min)
                ));
            } else if item.max > 0.0
                && let Some(peak) = window_breaches
                    .iter()
                    .copied()
                    .filter(|v| *v > item.max)
                    .reduce(f64::max)
            {
                // 当前帧已回落，但滑窗内超 70% 样本越界：展示窗口峰值避免写出自相矛盾的「1.0%（超上限 80.0%）」。
                details.push(format!(
                    "{label} 窗口峰值 {}，当前 {cur}（滑窗触发，上限 {}）",
                    format_metric_value(&item.kind, peak),
                    format_metric_value(&item.kind, item.max)
                ));
            } else if item.min > 0.0
                && let Some(trough) = window_breaches
                    .iter()
                    .copied()
                    .filter(|v| *v < item.min)
                    .reduce(f64::min)
            {
                details.push(format!(
                    "{label} 窗口最低 {}，当前 {cur}（滑窗触发，下限 {}）",
                    format_metric_value(&item.kind, trough),
                    format_metric_value(&item.kind, item.min)
                ));
            } else if item.max > 0.0 {
                details.push(format!(
                    "{label} 当前 {cur}（滑窗触发，上限 {}）",
                    format_metric_value(&item.kind, item.max)
                ));
            } else if item.min > 0.0 {
                details.push(format!(
                    "{label} 当前 {cur}（滑窗触发，下限 {}）",
                    format_metric_value(&item.kind, item.min)
                ));
            } else {
                details.push(format!("{label} {cur}"));
            }
        } else {
            details.push(metric_label_zh(&item.kind).to_string());
        }
    }
    let at = util_time::format_unix_secs_in_location(now_unix, &options.location);
    format!(
        "[RuleIncident] {header} · {}\n触发详情：{}\n触发时间：{at}",
        rule.name,
        details.join("；")
    )
}

fn format_resolved_message(
    rule: &StoredRule,
    parsed: &[Rule],
    server: &ServerSample,
    now_unix: i64,
    started_at_unix: Option<i64>,
    options: &ScanOptions,
) -> String {
    let header = format_server_header(server, options.plain_ip);
    let mut current_parts = Vec::with_capacity(parsed.len());
    for item in parsed {
        if item.kind == "offline" {
            current_parts.push("主机已恢复上报".to_string());
        } else if let Some(val) = item.metric_value(server) {
            current_parts.push(format!(
                "{} {}",
                metric_label_zh(&item.kind),
                format_metric_value(&item.kind, val)
            ));
        }
    }
    let at = util_time::format_unix_secs_in_location(now_unix, &options.location);
    let mut out = format!("[RuleResolved] {header} · {}", rule.name);
    if !current_parts.is_empty() {
        out.push_str(&format!("\n当前状态：{}", current_parts.join("；")));
    }
    if let Some(start) = started_at_unix {
        let elapsed = now_unix.saturating_sub(start).max(0) as u64;
        if elapsed > 0 {
            out.push_str(&format!(
                "\n持续时长：{}",
                util_time::format_duration_zh(elapsed)
            ));
        }
    }
    out.push_str(&format!("\n恢复时间：{at}"));
    out
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

fn check(rules: &[Rule], points: &[Vec<Option<f64>>]) -> (usize, bool) {
    if rules.is_empty() {
        return (0, true);
    }
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
        let triggered = if rule.kind == "offline" {
            total > 0.0 && failed >= total
        } else {
            total > 0.0 && failed / total > 0.7
        };
        if triggered {
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
    let initial_threshold = conf
        .read()
        .expect("config lock")
        .offline_threshold_seconds
        .max(10);
    let boot_now_unix = util_time::now_nanos() / 1_000_000_000;
    if let Err(err) =
        db.read(|conn| seed_initial_memory(conn, &mut memory, boot_now_unix, initial_threshold))
    {
        tracing::warn!("alert sentinel seed: {err:#}");
    }
    let started = tokio::time::Instant::now();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(3)) => {
                let options = {
                    let c = conf.read().expect("config lock");
                    let threshold = c.offline_threshold_seconds.max(10);
                    let grace = std::time::Duration::from_secs(threshold);
                    ScanOptions {
                        offline_threshold_seconds: threshold,
                        plain_ip: c.enable_plain_ip_in_notification,
                        location: c.location.clone(),
                        allow_new_offline: started.elapsed() >= grace,
                    }
                };
                let now_unix = util_time::now_nanos() / 1_000_000_000;
                let scanned = db.read(|conn| scan_with_options(conn, &mut memory, now_unix, &options));
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

    #[test]
    fn offline_rule_recovers_on_first_heartbeat_and_refires_after_flap() {
        let db = temp_db();
        seed_host(&db, Some(60));
        db.write(|tx| {
            tx.execute(
                "UPDATE alert_rules SET name = '状态通知', rules_raw = '[{\"type\":\"offline\",\"duration\":60}]' WHERE id = 10",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        let mut memory = SentinelMemory::default();
        let now = crate::util::time::now_nanos() / 1_000_000_000;
        let mut fired = Vec::new();
        for _ in 1..=20 {
            fired = db.read(|conn| scan(conn, &mut memory, now, 30)).unwrap();
        }
        assert_eq!(fired.len(), 1);
        assert!(
            !fired[0].message.contains("203.0.113.1") && fired[0].message.contains("***.*.***.*"),
            "默认应脱敏 IP：{}",
            fired[0].message
        );
        assert!(fired[0].message.contains("离线（已停报"));

        // 主机短暂恢复心跳：第一轮扫描就必须立刻产出 `[RuleResolved]`，不能拖 18 秒漏掉恢复与再离线。
        db.write(|tx| {
            tx.execute(
                "UPDATE server_runtimes SET last_seen_at = ?1 WHERE server_id = 9",
                [crate::util::time::unix_nanos_to_sqlite(now * 1_000_000_000)],
            )?;
            Ok(())
        })
        .unwrap();
        let resolved = db
            .read(|conn| scan(conn, &mut memory, now + 3, 30))
            .unwrap();
        assert_eq!(resolved.len(), 1, "恢复心跳后首轮必须立刻发出恢复通知");
        assert!(resolved[0].message.contains("[RuleResolved]"));
        assert!(resolved[0].message.contains("主机已恢复上报"));
        assert!(resolved[0].message.contains("持续时长："));

        // 随后再次失联：新的 60 秒窗口满后必须能再次触发 `once` 规则。
        let flap_now = now + 90;
        let mut refired = Vec::new();
        for _ in 1..=20 {
            refired = db
                .read(|conn| scan(conn, &mut memory, flap_now, 30))
                .unwrap();
        }
        assert_eq!(refired.len(), 1, "再次离线满持续时间后必须重新触发");
        assert!(refired[0].message.contains("[RuleIncident]"));
    }

    #[test]
    fn startup_seed_prevents_restart_reblast_and_preserves_recovery() {
        let db = temp_db();
        seed_host(&db, Some(300));
        db.write(|tx| {
            tx.execute(
                "UPDATE server_runtimes SET status = 'offline', state_payload = NULL WHERE server_id = 9",
                [],
            )?;
            tx.execute(
                "UPDATE alert_rules SET rules_raw = '[{\"type\":\"offline\",\"duration\":60}]' WHERE id = 10",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        let mut memory = SentinelMemory::default();
        let now = crate::util::time::now_nanos() / 1_000_000_000;
        db.read(|conn| seed_initial_memory(conn, &mut memory, now, 30))
            .unwrap();

        // 存量离线主机在面板重启后不得再次轰炸 `[RuleIncident]`。
        for _ in 0..25 {
            let notices = db.read(|conn| scan(conn, &mut memory, now, 30)).unwrap();
            assert!(notices.is_empty(), "重启预热后的存量离线主机不得重复发告警");
        }

        // 一旦恢复上报（哪怕仍在启动宽限期内），应立即发出 `[RuleResolved]`。
        db.write(|tx| {
            tx.execute(
                "UPDATE server_runtimes SET status = 'online', last_seen_at = ?1 WHERE server_id = 9",
                [crate::util::time::unix_nanos_to_sqlite((now + 5) * 1_000_000_000)],
            )?;
            Ok(())
        })
        .unwrap();
        let mut grace_opts = ScanOptions::new(30);
        grace_opts.allow_new_offline = false;
        let resolved = db
            .read(|conn| scan_with_options(conn, &mut memory, now + 6, &grace_opts))
            .unwrap();
        assert_eq!(resolved.len(), 1);
        assert!(resolved[0].message.contains("[RuleResolved]"));
    }

    #[test]
    fn empty_rules_raw_and_empty_ip_are_handled_cleanly() {
        let db = temp_db();
        let now_nanos = crate::util::time::now_nanos();
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, secret_ciphertext) VALUES (11, 'no-ip-host', X'00')",
                [],
            )?;
            tx.execute(
                "INSERT INTO server_runtimes (server_id, last_ip, last_seen_at) VALUES (11, '', ?1)",
                [crate::util::time::unix_nanos_to_sqlite(now_nanos - 90_000_000_000)],
            )?;
            tx.execute(
                "INSERT INTO alert_rules (id, name, rules_raw, enable, trigger_mode, notification_tag) \
                 VALUES (20, 'empty-cond', '[]', 1, 1, 'default'), \
                        (21, 'offline-no-state', '[{\"type\":\"offline\",\"duration\":1}]', 1, 1, 'default')",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        let mut memory = SentinelMemory::default();
        let notices = db
            .read(|conn| scan(conn, &mut memory, now_nanos / 1_000_000_000, 30))
            .unwrap();
        assert_eq!(
            notices.len(),
            1,
            "空条件规则不得误报，无 state_payload 的超时主机必须可报离线"
        );
        assert!(notices[0].message.contains("no-ip-host · offline-no-state"));
        assert!(!notices[0].message.contains("()"), "空 IP 不得输出空括号");
    }

    #[test]
    fn reenable_clears_memory_offline_freezes_always_and_window_formats_peak() {
        let db = temp_db();
        let now_nanos = crate::util::time::now_nanos();
        let now = now_nanos / 1_000_000_000;
        let high_cpu = proto::State {
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
                    crate::util::time::unix_nanos_to_sqlite(now_nanos),
                    high_cpu.encode_to_vec()
                ],
            )?;
            // trigger_mode = 0 (MODE_ALWAYS)
            tx.execute(
                "INSERT INTO alert_rules (id, name, rules_raw, enable, trigger_mode, notification_tag) \
                 VALUES (4, 'cpu-always', '[{\"type\":\"cpu\",\"max\":80,\"duration\":1}]', 1, 0, 'ops')",
                [],
            )?;
            Ok(())
        })
        .unwrap();

        let mut memory = SentinelMemory::default();
        assert_eq!(
            db.read(|conn| scan(conn, &mut memory, now, 30))
                .unwrap()
                .len(),
            1
        );

        // 1) 主机停报离线期间（now + 60 >= threshold 30）：不得对冻结的 95% CPU 继续 MODE_ALWAYS 复报，也不得误报恢复。
        let while_dead = db
            .read(|conn| scan(conn, &mut memory, now + 60, 30))
            .unwrap();
        assert!(
            while_dead.is_empty(),
            "死机停报期间不得对冻结快照无限复报或误报恢复：{while_dead:?}"
        );

        // 2) 禁用规则再恢复低 CPU 后重新启用：fingerprint 包含 enabled，不得误发 [RuleResolved]。
        db.write(|tx| {
            let calm = proto::State {
                cpu: 5.0,
                ..Default::default()
            };
            tx.execute(
                "UPDATE server_runtimes SET last_seen_at = ?1, state_payload = ?2 WHERE server_id = 3",
                params![
                    crate::util::time::unix_nanos_to_sqlite((now + 65) * 1_000_000_000),
                    calm.encode_to_vec()
                ],
            )?;
            tx.execute("UPDATE alert_rules SET enable = 0 WHERE id = 4", [])?;
            Ok(())
        })
        .unwrap();
        assert!(
            db.read(|conn| scan(conn, &mut memory, now + 66, 30))
                .unwrap()
                .is_empty()
        );
        db.write(|tx| {
            tx.execute("UPDATE alert_rules SET enable = 1 WHERE id = 4", [])?;
            Ok(())
        })
        .unwrap();
        let after_reenable = db
            .read(|conn| scan(conn, &mut memory, now + 67, 30))
            .unwrap();
        assert!(
            after_reenable.is_empty(),
            "禁用再启用规则不得误发 [RuleResolved]：{after_reenable:?}"
        );

        // 3) 滑窗触发时当前帧已回落到 1.0%：文案展示窗口峰值与滑窗触发说明，不得写出「CPU 1.0%（超上限 80.0%）」。
        db.write(|tx| {
            tx.execute(
                "UPDATE alert_rules SET rules_raw = '[{\"type\":\"cpu\",\"max\":80,\"duration\":12}]', trigger_mode = 1 WHERE id = 4",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        let mut set_cpu = |cpu: f64, step: i64| {
            db.write(|tx| {
                let s = proto::State {
                    cpu,
                    ..Default::default()
                };
                tx.execute(
                    "UPDATE server_runtimes SET last_seen_at = ?1, state_payload = ?2 WHERE server_id = 3",
                    params![
                        crate::util::time::unix_nanos_to_sqlite((now + 100 + step) * 1_000_000_000),
                        s.encode_to_vec()
                    ],
                )?;
                Ok(())
            })
            .unwrap();
            db.read(|conn| scan(conn, &mut memory, now + 100 + step, 30))
                .unwrap()
        };
        assert!(set_cpu(92.0, 1).is_empty());
        assert!(set_cpu(96.0, 2).is_empty());
        assert!(set_cpu(90.0, 3).is_empty());
        // 第 4 帧回落到 1.0%，但 4 帧中 3 帧越界（75% > 70%），此时触发告警。
        let triggered = set_cpu(1.0, 4);
        assert_eq!(triggered.len(), 1);
        let msg = &triggered[0].message;
        assert!(
            msg.contains("窗口峰值 96.0%")
                && msg.contains("当前 1.0%")
                && !msg.contains("1.0%（超上限"),
            "回落帧滑窗触发文案不得自相矛盾：{msg}"
        );
    }
}
