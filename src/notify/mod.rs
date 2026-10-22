//! 通知引擎（Phase 6，对齐 Go `service/singleton/notification*.go` +
//! `model/notification.go`）：通用 Webhook 投递（占位符、SSRF 检查、
//! GET/POST × JSON/Form、自定义 header、VerifySSL）、防骚扰指数退避
//! （首报 15 分钟、翻倍封顶 24h）、按类 15 秒聚合窗口、l10n 文案。
//!
//! 触发点通过 [`Notifier::send`] 进入：带 muteLabel 的走防骚扰，
//! 命中聚合分类的进 15 秒窗口合并，其余直接投递。

// 行映射元组逐列镜像 SQL SELECT。
#![allow(clippy::type_complexity)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use prost::Message as _;
use serde_json::{Value, json};

use crate::config::ConfigHandle;
use crate::db::Db;
use crate::proto;
use crate::repo::typed_resources as repo;
use crate::rpc::support::blocking_read;
use crate::util::time as util_time;

/// 首次通知后的静音起点（Go `firstNotificationDelay`）。
const FIRST_MUTE: Duration = Duration::from_secs(15 * 60);
/// 静音上限（每天最多一次）。
const MAX_MUTE: Duration = Duration::from_secs(24 * 60 * 60);
/// 聚合窗口（Go `defaultNotificationAggregateWindow`）。
const AGG_WINDOW: Duration = Duration::from_secs(15);
/// Webhook 请求超时。
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);

// ---------- l10n ----------

/// 内嵌 l10n 资源（Go `resource.I18nFS` 等价物）。
#[derive(rust_embed::Embed)]
#[folder = "resource/l10n/"]
struct L10nFiles;

/// 极简 TOML 字典：`[Key] other = "文案"`；只取 `other`。
fn load_language_dict(language: &str) -> HashMap<String, String> {
    let file = L10nFiles::get(&format!("{language}.toml")).or_else(|| L10nFiles::get("zh-CN.toml"));
    let Some(file) = file else {
        return HashMap::new();
    };
    let parsed: toml::Value = toml::from_str(&String::from_utf8_lossy(&file.data))
        .unwrap_or(toml::Value::Table(Default::default()));
    let mut out = HashMap::new();
    if let toml::Value::Table(table) = parsed {
        for (key, value) in table {
            if let toml::Value::Table(entry) = value
                && let Some(other) = entry.get("other").and_then(toml::Value::as_str)
            {
                out.insert(key, other.to_string());
            }
        }
    }
    out
}

// ---------- 占位符 ----------

/// 占位符展开所需的服务器上下文（Go `model.Server` 的通知子集）。
pub struct ServerContext {
    pub id: i64,
    pub name: String,
    pub state: proto::State,
    pub host: proto::Host,
}

impl ServerContext {
    /// 从 `servers` + `server_runtimes` 载荷组装；无运行态返回 None。
    pub fn load(conn: &rusqlite::Connection, server_id: i64) -> anyhow::Result<Option<Self>> {
        use rusqlite::OptionalExtension as _;
        let row: Option<(String, Option<Vec<u8>>, Option<Vec<u8>>)> = conn
            .query_row(
                "SELECT COALESCE(s.name, ''), r.state_payload, r.host_payload \
                 FROM servers s LEFT JOIN server_runtimes r ON r.server_id = s.id \
                 WHERE s.id = ?1 AND s.deleted_at IS NULL",
                [server_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((name, state_payload, host_payload)) = row else {
            return Ok(None);
        };
        let state = state_payload
            .as_deref()
            .and_then(|payload| proto::State::decode(payload).ok())
            .unwrap_or_default();
        let host = host_payload
            .as_deref()
            .and_then(|payload| proto::Host::decode(payload).ok())
            .unwrap_or_default();
        Ok(Some(Self {
            id: server_id,
            name,
            state,
            host,
        }))
    }
}

/// `fmt.Sprintf("%f")` 的 Go 等价输出（6 位小数）。
fn go_f64(value: f64) -> String {
    format!("{value:.6}")
}

/// IP 打码（Go `IPDesensitize`：未开启明文 IP 时逐段替换为 `*`）。
fn desensitize(ip: &str, plain: bool) -> String {
    if plain || ip.is_empty() {
        return ip.to_string();
    }
    ip.chars()
        .map(|ch| if ch.is_ascii_digit() { '*' } else { ch })
        .collect()
}

/// `replaceParamsInString`：`#SANTAIZI#` / `#DATETIME#` / `#SERVER.*#`；
/// `mod_value` 作用于每个替换值（Go 的 mod 闭包，JSON 体传 json_escape）。
pub fn expand_placeholders(
    text: &str,
    message: &str,
    server: Option<&ServerContext>,
    now_nanos: i64,
    plain_ip: bool,
) -> String {
    expand_placeholders_with(text, message, server, now_nanos, plain_ip, |value| {
        value.to_string()
    })
}

pub fn expand_placeholders_with(
    text: &str,
    message: &str,
    server: Option<&ServerContext>,
    now_nanos: i64,
    plain_ip: bool,
    mod_value: impl Fn(&str) -> String,
) -> String {
    let mut out = text.replace("#SANTAIZI#", &mod_value(message));
    out = out.replace(
        "#DATETIME#",
        &mod_value(
            &util_time::unix_nanos_to_rfc3339(now_nanos)
                .replace('T', " ")
                .replace("Z", " +0000 UTC"),
        ),
    );
    let Some(server) = server else {
        return out;
    };
    let state = &server.state;
    let host = &server.host;
    let replacements = [
        ("#SERVER.NAME#", server.name.clone()),
        ("#SERVER.ID#", server.id.to_string()),
        ("#SERVER.CPU#", go_f64(state.cpu)),
        ("#SERVER.MEM#", state.mem_used.to_string()),
        ("#SERVER.SWAP#", state.swap_used.to_string()),
        ("#SERVER.DISK#", state.disk_used.to_string()),
        ("#SERVER.MEMUSED#", state.mem_used.to_string()),
        ("#SERVER.SWAPUSED#", state.swap_used.to_string()),
        ("#SERVER.DISKUSED#", state.disk_used.to_string()),
        ("#SERVER.MEMTOTAL#", host.mem_total.to_string()),
        ("#SERVER.SWAPTOTAL#", host.swap_total.to_string()),
        ("#SERVER.DISKTOTAL#", host.disk_total.to_string()),
        ("#SERVER.NETINSPEED#", state.net_in_speed.to_string()),
        ("#SERVER.NETOUTSPEED#", state.net_out_speed.to_string()),
        ("#SERVER.TRANSFERIN#", state.net_in_transfer.to_string()),
        ("#SERVER.NETINTRANSFER#", state.net_in_transfer.to_string()),
        ("#SERVER.TRANSFEROUT#", state.net_out_transfer.to_string()),
        (
            "#SERVER.NETOUTTRANSFER#",
            state.net_out_transfer.to_string(),
        ),
        ("#SERVER.LOAD1#", go_f64(state.load1)),
        ("#SERVER.LOAD5#", go_f64(state.load5)),
        ("#SERVER.LOAD15#", go_f64(state.load15)),
        ("#SERVER.TCPCONNCOUNT#", state.tcp_conn_count.to_string()),
        ("#SERVER.UDPCONNCOUNT#", state.udp_conn_count.to_string()),
    ];
    for (key, value) in replacements {
        out = out.replace(key, &mod_value(&value));
    }
    let ip = host.ip.trim();
    let (ipv4, ipv6, valid) = match ip.split_once('/') {
        Some((v4, v6)) => (v4.to_string(), v6.to_string(), v4.to_string()),
        None if ip.contains(':') => (String::new(), ip.to_string(), ip.to_string()),
        None if !ip.is_empty() => (ip.to_string(), String::new(), ip.to_string()),
        _ => (String::new(), String::new(), String::new()),
    };
    out = out.replace("#SERVER.IP#", &mod_value(&desensitize(&valid, plain_ip)));
    out = out.replace("#SERVER.IPV4#", &mod_value(&desensitize(&ipv4, plain_ip)));
    out = out.replace("#SERVER.IPV6#", &mod_value(&desensitize(&ipv6, plain_ip)));
    out
}

/// `utils.Json.Marshal(msg)` 去掉首尾引号：JSON 字符串转义。
fn json_escape(message: &str) -> String {
    let encoded = serde_json::to_string(message).unwrap_or_default();
    encoded[1..encoded.len() - 1].to_string()
}

// ---------- SSRF ----------

/// 把 IPv6 地址归一成「实际承载的 IPv4」：IPv4-mapped（`::ffff:127.0.0.1`）与
/// IPv4-compatible（`::127.0.0.1`）在 `Ipv6Addr` 的 `is_loopback()` 等判定上
/// 全部返回 false——直接按字面判会把整个 localhost 放过。
fn normalize_ip(ip: std::net::IpAddr) -> std::net::IpAddr {
    match ip {
        std::net::IpAddr::V4(v4) => {
            // IPv4-compatible 形态由解析层产出，这里只做映射归一。
            std::net::IpAddr::V4(v4)
        }
        std::net::IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .or_else(|| {
                // `::a.b.c.d`（IPv4-compatible，非 mapped）：仅当高 96 位全零。
                let segments = v6.segments();
                if segments[0..5].iter().all(|segment| *segment == 0) && segments[5] == 0 {
                    let octets = v6.octets();
                    Some(std::net::Ipv4Addr::new(
                        octets[12], octets[13], octets[14], octets[15],
                    ))
                } else {
                    None
                }
            })
            .map(std::net::IpAddr::V4)
            .unwrap_or(std::net::IpAddr::V6(v6)),
    }
}

fn v4_is_internal(v4: std::net::Ipv4Addr) -> bool {
    let octets = v4.octets();
    v4.is_loopback()
        || v4.is_private()
        || v4.is_link_local()
        || v4.is_broadcast()
        // `0.0.0.0` 在多数系统上等于本机，`is_private()` 不覆盖它。
        || v4.is_unspecified()
        // 100.64.0.0/10 运营商级 NAT：公网不可达，且常被云厂商拿来做元数据旁路。
        || (octets[0] == 100 && (octets[1] & 0xc0) == 0x40)
        // 192.0.0.0/24、198.18.0.0/15（基准测试）、240.0.0.0/4（保留）。
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
        || (octets[0] == 198 && (octets[1] & 0xfe) == 0x12)
        || octets[0] >= 240
}

fn ip_is_internal(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => v4_is_internal(v4),
        std::net::IpAddr::V6(v6) => {
            // 先按 IPv6 自身判定：`::1` 是 loopback，先归一成 IPv4 反而会丢掉这条。
            v6.is_loopback()
                || v6.is_unique_local()
                || v6.is_unicast_link_local()
                || v6.is_multicast()
                || v6.is_unspecified()
                // 再按它承载的 IPv4 判：`::ffff:127.0.0.1` / `::127.0.0.1`
                // 在前一组判定上全部返回 false，等于把整个 localhost 白送。
                || matches!(
                    normalize_ip(std::net::IpAddr::V6(v6)),
                    std::net::IpAddr::V4(v4) if v4_is_internal(v4)
                )
        }
    }
}

/// Go `CheckURLForSSRF`：IP 字面量直接判；域名解析后任一地址内部即拒。
///
/// 只校验首跳 URL 是不够的：HTTP 客户端默认跟随 30x，一个公网地址可以
/// 302 到 `http://169.254.169.254/`。真正的收口在 [`ssrf_redirect_policy`]——
/// 每一跳都过一遍同一套判定。
pub fn check_ssrf(raw_url: &str) -> Result<(), String> {
    let parsed = url::Url::parse(raw_url).map_err(|err| err.to_string())?;
    // 必须用 `host()` 而不是 `host_str()`：后者对 IPv6 主机带回括号
    // （`[::1]`），`parse::<IpAddr>()` 必然失败，于是整类 IPv6 字面量
    // 静默掉进 DNS 分支、`to_socket_addrs("[::1]", port)` 也失败，
    // 最终 `if let Ok(addrs)` 直接跳过——整个 IPv6 内网都不设防。
    let host = match parsed.host() {
        Some(url::Host::Ipv4(v4)) => {
            if v4_is_internal(v4) {
                return Err("内部地址被禁止".into());
            }
            return Ok(());
        }
        Some(url::Host::Ipv6(v6)) => {
            if ip_is_internal(std::net::IpAddr::V6(v6)) {
                return Err("内部地址被禁止".into());
            }
            return Ok(());
        }
        Some(url::Host::Domain(domain)) => domain.to_string(),
        None => return Err("URL 缺少主机".into()),
    };
    // 域名：解析（调用方处于 spawn_blocking 上下文）。
    let port = parsed.port_or_known_default().unwrap_or(80);
    if let Ok(addrs) = std::net::ToSocketAddrs::to_socket_addrs(&(host.as_str(), port)) {
        for addr in addrs {
            if ip_is_internal(addr.ip()) {
                return Err("内部地址被禁止".into());
            }
        }
    }
    Ok(())
}

/// reqwest 重定向策略：跟随前对**目标**再跑一次 SSRF 判定。
///
/// 首跳通过校验的公开 URL 完全可能 302 到内网/云元数据地址，默认
/// `Policy::default()`（最多 10 跳）会把请求原样带过去。这里改成自定义策略：
/// 目标内部 → 直接拒绝该跳（返回错误，不跟随）。
pub fn ssrf_redirect_policy() -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(|attempt| match check_ssrf(attempt.url().as_str()) {
        Ok(()) => attempt.follow(),
        Err(reason) => attempt.error(std::io::Error::other(format!(
            "redirect target rejected: {reason}"
        ))),
    })
}

// ---------- 渠道投递 ----------

/// 单渠道投递（阻塞；调用方放 `spawn_blocking`）。语义对齐 Go
/// `NotificationServerBundle.Send` + `reqBody`：GET 无 body；POST JSON 模板
/// 全占位符按 json_escape 展开；POST Form 按键值展开后 url 编码。
pub fn deliver_channel_blocking(
    channel: &repo::NotificationRow,
    message: &str,
    server: Option<&ServerContext>,
    now_nanos: i64,
) -> Result<(), String> {
    deliver_channel_with_policy(channel, message, server, now_nanos, false)
}

/// 测试缝隙：允许内网地址（本地 stub 集成测试用；生产恒走严格 SSRF）。
pub fn deliver_channel_allow_internal(
    channel: &repo::NotificationRow,
    message: &str,
    server: Option<&ServerContext>,
    now_nanos: i64,
) -> Result<(), String> {
    deliver_channel_with_policy(channel, message, server, now_nanos, true)
}

fn deliver_channel_with_policy(
    channel: &repo::NotificationRow,
    message: &str,
    server: Option<&ServerContext>,
    now_nanos: i64,
    allow_internal: bool,
) -> Result<(), String> {
    let plain_ip = true; // 占位符不做 IP 打码（打码由触发方文案处理，与 Go mod(nil) 一致）。
    let is_post = channel.request_method == repo::NOTIFY_METHOD_POST;
    let url = expand_placeholders(&channel.url, message, server, now_nanos, plain_ip);
    if !allow_internal {
        check_ssrf(&url)?;
    }
    let body = if !is_post || message.is_empty() {
        String::new()
    } else if channel.request_type == repo::NOTIFY_TYPE_FORM {
        let parsed: HashMap<String, String> =
            serde_json::from_str(&channel.request_body).unwrap_or_default();
        let mut encoded = String::new();
        for (index, (key, value)) in parsed.iter().enumerate() {
            let expanded = expand_placeholders(value, message, server, now_nanos, plain_ip);
            let pair = url::form_urlencoded::Serializer::new(String::new())
                .append_pair(key, &expanded)
                .finish();
            if index > 0 {
                encoded.push('&');
            }
            encoded.push_str(&pair);
        }
        encoded
    } else {
        expand_placeholders_with(
            &channel.request_body,
            message,
            server,
            now_nanos,
            plain_ip,
            json_escape,
        )
    };

    let client = reqwest::blocking::Client::builder()
        .timeout(HTTP_TIMEOUT)
        // 每一跳重定向都重新过 SSRF 判定（否则首跳公网地址可 302 到内网）。
        .redirect(ssrf_redirect_policy())
        // verify_ssl=0（默认）→ 跳过证书校验（Go HttpClientSkipTlsVerify）。
        .danger_accept_invalid_certs(channel.verify_ssl.map(|value| value == 0).unwrap_or(true))
        .build()
        .map_err(|err| err.to_string())?;
    let mut request = if is_post {
        let mut request = client.post(&url).body(body);
        request = request.header(
            "Content-Type",
            if channel.request_type == repo::NOTIFY_TYPE_FORM {
                "application/x-www-form-urlencoded"
            } else {
                "application/json"
            },
        );
        request
    } else {
        client.get(&url)
    };
    if !channel.request_header.is_empty()
        && let Ok(map) = serde_json::from_str::<HashMap<String, String>>(&channel.request_header)
    {
        for (key, value) in map {
            request = request.header(key, value);
        }
    }
    let response = request.send().map_err(|err| err.to_string())?;
    let status = response.status();
    if !(200..300).contains(&status.as_u16()) {
        let text = response.text().unwrap_or_default();
        return Err(format!("{}@{} {text}", status.as_u16(), status));
    }
    Ok(())
}

// ---------- 聚合分类 ----------

/// Go `classifyNotification`：命中聚合分类返回 (kind, names)。
pub fn classify(desc: &str) -> Option<(String, Vec<String>)> {
    let trimmed = desc.trim();
    if trimmed.is_empty() {
        return None;
    }
    let legacy = [
        ("HOST_OFFLINE", "离线", "node="),
        ("COLLECTOR_OFFLINE", "从端离线", "collector="),
        ("COLLECTOR_ONLINE", "从端上线", "collector="),
        ("CONNECTIVITY_DEGRADED", "连通降级", "node="),
        ("TELEMETRY_DATA_LOSS", "探测丢失", "component="),
    ];
    for (prefix, kind, field) in legacy {
        if trimmed.starts_with(prefix) || trimmed.contains(&format!("\n{prefix}")) {
            return Some((kind.to_string(), extract_legacy_field(trimmed, field)));
        }
    }
    let end = trimmed.find(']')?;
    if end <= 1 || !trimmed.starts_with('[') {
        return None;
    }
    let kind = trimmed[1..end].trim();
    match kind {
        "离线" | "从端离线" | "从端上线" | "恢复" | "连通降级" | "探测丢失" | "ProbeDown"
        | "ProbeUp" | "ProbeLatency" => Some((
            kind.to_string(),
            parse_aggregated_names(&trimmed[end + 1..]),
        )),
        _ => None,
    }
}

/// `node=x` / `collector=x` 行的值提取。
fn extract_legacy_field(desc: &str, field: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in desc.lines() {
        let line = line.trim();
        if let Some(value) = line.strip_prefix(field) {
            let value = value.trim();
            if !value.is_empty() {
                out.push(value.to_string());
            }
        }
    }
    out
}

/// `[kind] n1, n2` 的名字段。
fn parse_aggregated_names(rest: &str) -> Vec<String> {
    rest.split(',')
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect()
}

/// Go `formatAggregatedNotification`：`[kind] N 台/条` + 每行一个名字。
pub fn format_aggregated(kind: &str, names: &[String]) -> String {
    let unit = match kind {
        "离线" | "从端离线" | "从端上线" | "恢复" | "连通降级" => "台",
        _ => "条",
    };
    let mut out = format!("[{kind}] {} {unit}", names.len());
    for name in names {
        out.push('\n');
        out.push_str(name);
    }
    out
}

// ---------- Notifier ----------

#[derive(Clone)]
struct MuteState {
    duration: Duration,
    until: Instant,
}

#[derive(Default)]
struct NotifyState {
    mutes: HashMap<String, MuteState>,
    buckets: HashMap<String, (String, Vec<String>, String, Instant)>,
}

/// 进程级通知引擎（Go `SendNotification` + 聚合器 + 静音缓存的合体）。
pub struct Notifier {
    db: Db,
    conf: ConfigHandle,
    state: Mutex<NotifyState>,
    dict: Mutex<HashMap<String, String>>,
}

impl Notifier {
    pub fn new(db: Db, conf: ConfigHandle) -> Self {
        let dict = load_language_dict(&conf.read().expect("config lock").language);
        Self {
            db,
            conf,
            state: Mutex::new(NotifyState::default()),
            dict: Mutex::new(dict),
        }
    }

    /// l10n 文案（语言切换后由 settings 端点调 [`Self::reload_language`]）。
    pub fn t(&self, key: &str) -> String {
        self.dict
            .lock()
            .expect("l10n lock")
            .get(key)
            .cloned()
            .unwrap_or_else(|| key.to_string())
    }

    pub fn reload_language(&self) {
        *self.dict.lock().unwrap_or_else(|err| err.into_inner()) =
            load_language_dict(&self.conf.read().expect("config lock").language);
    }

    /// 清掉一条静音，让恢复通知或反向事件能立刻发出。
    pub fn unmute(&self, tag: &str, label: &str) {
        let mute_key = format!("{label}\x00{tag}");
        self.state
            .lock()
            .expect("notify lock")
            .mutes
            .remove(&mute_key);
    }

    /// Go `SendNotification`：防骚扰 → 聚合 → 投递。
    pub async fn send(
        &self,
        tag: &str,
        desc: &str,
        mute_label: Option<String>,
        server_id: Option<i64>,
    ) {
        if let Some(label) = &mute_label {
            let mute_key = format!("{label}\x00{tag}");
            let should_send = {
                let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
                match state.mutes.get(&mute_key) {
                    Some(entry) => {
                        if Instant::now() >= entry.until {
                            let mut duration = entry.duration * 2;
                            if duration > MAX_MUTE {
                                duration = MAX_MUTE;
                            }
                            state.mutes.insert(
                                mute_key.clone(),
                                MuteState {
                                    duration,
                                    until: Instant::now() + duration,
                                },
                            );
                            true
                        } else {
                            false
                        }
                    }
                    None => {
                        state.mutes.insert(
                            mute_key.clone(),
                            MuteState {
                                duration: FIRST_MUTE,
                                until: Instant::now() + FIRST_MUTE,
                            },
                        );
                        true
                    }
                }
            };
            if !should_send {
                return;
            }
        }
        if let Some((kind, names)) = classify(desc) {
            self.enqueue_aggregate(tag.to_string(), kind, names, desc.to_string());
            return;
        }
        self.deliver(tag, desc, server_id).await;
    }

    /// 聚合窗口入桶；同 (tag, kind) 首条启动计时。
    fn enqueue_aggregate(&self, tag: String, kind: String, names: Vec<String>, first: String) {
        let key = format!("{tag}\x00{kind}");
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        let (_, bucket_names, _, expires) = state.buckets.entry(key.clone()).or_insert_with(|| {
            (
                kind.clone(),
                Vec::new(),
                first.clone(),
                Instant::now() + AGG_WINDOW,
            )
        });
        let _ = kind;
        for name in names {
            let name = name.trim();
            if !name.is_empty() && !bucket_names.iter().any(|seen| seen == name) {
                bucket_names.push(name.to_string());
            }
        }
        let _ = expires;
    }

    /// 聚合清扫循环（spawn_background_workers 起一份）。
    pub async fn run_flusher(self: Arc<Self>, shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut shutdown = shutdown;
        let mut ticker = tokio::time::interval(Duration::from_millis(500));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let expired: Vec<(String, String, Vec<String>, String)> = {
                        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
                        let now = Instant::now();
                        let keys: Vec<String> = state
                            .buckets
                            .iter()
                            .filter(|entry| entry.1.3 <= now)
                            .map(|entry| entry.0.clone())
                            .collect();
                        keys.into_iter()
                            .filter_map(|key| {
                                state.buckets.remove(&key).map(|value| (key, value))
                            })
                            .map(|(key, value)| (key, value.0, value.1, value.2))
                            .collect()
                    };
                    for (key, kind, names, first) in expired {
                        let tag = key.split('\x00').next().unwrap_or(&key).to_string();
                        let desc = if names.len() > 1 {
                            format_aggregated(&kind, &names)
                        } else {
                            first
                        };
                        self.deliver(&tag, &desc, None).await;
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

    /// 按 tag 投递全部渠道（DB 读走 blocking_read，HTTP 放 spawn_blocking）。
    pub async fn deliver(&self, tag: &str, desc: &str, server_id: Option<i64>) {
        let tag = tag.to_string();
        let desc = desc.to_string();
        let now_nanos = util_time::now_nanos();
        let db = self.db.clone();
        let prepared = blocking_read(&db, move |conn| {
            let mut stmt = conn.prepare(
                "SELECT id, COALESCE(name, ''), COALESCE(tag, ''), COALESCE(url, ''), \
                 COALESCE(request_method, 1), COALESCE(request_type, 1), \
                 COALESCE(request_header, '{}'), COALESCE(request_body, ''), verify_ssl, \
                 created_at, updated_at \
                 FROM notifications WHERE tag = ?1 AND deleted_at IS NULL",
            )?;
            let mapped = stmt.query_map([&tag], |row| {
                Ok(repo::NotificationRow {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    tag: row.get(2)?,
                    url: row.get(3)?,
                    request_method: row.get(4)?,
                    request_type: row.get(5)?,
                    request_header: row.get(6)?,
                    request_body: row.get(7)?,
                    verify_ssl: row.get(8)?,
                    created_at_nanos: row
                        .get::<_, Option<String>>(9)?
                        .as_deref()
                        .and_then(util_time::sqlite_to_unix_nanos),
                    updated_at_nanos: row
                        .get::<_, Option<String>>(10)?
                        .as_deref()
                        .and_then(util_time::sqlite_to_unix_nanos),
                })
            })?;
            let mut channels = Vec::new();
            for row in mapped {
                channels.push(row?);
            }
            let server = match server_id {
                Some(id) => ServerContext::load(conn, id)?,
                None => None,
            };
            Ok((channels, server))
        })
        .await;
        let (channels, server) = match prepared {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(error = %err, "通知渠道读取失败");
                return;
            }
        };
        if channels.is_empty() {
            return;
        }
        let outcome = tokio::task::spawn_blocking(move || {
            for channel in &channels {
                if let Err(err) =
                    deliver_channel_blocking(channel, &desc, server.as_ref(), now_nanos)
                {
                    tracing::warn!(channel = %channel.name, error = %err, "通知发送失败");
                } else {
                    tracing::info!(channel = %channel.name, "通知发送成功");
                }
            }
        })
        .await;
        if let Err(err) = outcome {
            tracing::warn!(error = %err, "通知投递任务崩溃");
        }
    }

    /// 离线通知（Go `sendOfflineNotification`：tag default + mute server-offline-<id>）。
    pub async fn notify_offline(
        &self,
        server_id: i64,
        started_at_nanos: i64,
        last_seen_nanos: i64,
        last_ip: String,
    ) {
        if !self
            .conf
            .read()
            .expect("config lock")
            .enable_offline_notification
        {
            return;
        }
        let Some(message) = self
            .offline_message(server_id, started_at_nanos, last_seen_nanos, last_ip)
            .await
        else {
            return;
        };
        self.send(
            "default",
            &message,
            Some(format!("server-offline-{server_id}")),
            Some(server_id),
        )
        .await;
    }

    /// 恢复通知（Go `sendRecoveryNotification`）。
    pub async fn notify_recovery(
        &self,
        server_id: i64,
        offline_seconds: i64,
        last_ip: String,
        recovered_ip: String,
    ) {
        if !self
            .conf
            .read()
            .expect("config lock")
            .enable_recovery_notification
        {
            return;
        }
        let now_nanos = util_time::now_nanos();
        let plain = self
            .conf
            .read()
            .expect("config lock")
            .enable_plain_ip_in_notification;
        let name = self.server_name(server_id).await;
        let message = format!(
            "[恢复] {name}\n恢复时间：{}\n离线时长：{}s\n原因：原因未知\n离线前 IP：{}\n恢复后 IP：{}",
            util_time::unix_nanos_to_rfc3339(now_nanos).replace("T", " "),
            offline_seconds,
            desensitize(&last_ip, plain),
            desensitize(&recovered_ip, plain),
        );
        self.send(
            "default",
            &message,
            Some(format!("server-recovery-{server_id}")),
            Some(server_id),
        )
        .await;
    }

    async fn offline_message(
        &self,
        server_id: i64,
        started_at_nanos: i64,
        last_seen_nanos: i64,
        last_ip: String,
    ) -> Option<String> {
        let plain = self
            .conf
            .read()
            .expect("config lock")
            .enable_plain_ip_in_notification;
        let db = self.db.clone();
        let name = self.server_name(server_id).await;
        let duration = (util_time::now_nanos() - started_at_nanos) / 1_000_000_000;
        let _ = db;
        Some(format!(
            "[离线] {name}\n离线开始时间：{}\n最后上报：{}\n持续时间：{duration}s\n离线前 IP：{}",
            util_time::unix_nanos_to_rfc3339(started_at_nanos).replace("T", " "),
            util_time::unix_nanos_to_rfc3339(last_seen_nanos).replace("T", " "),
            desensitize(&last_ip, plain),
        ))
    }

    async fn server_name(&self, server_id: i64) -> String {
        let db = self.db.clone();
        blocking_read(&db, move |conn| {
            Ok(conn
                .query_row(
                    "SELECT COALESCE(name, '') FROM servers WHERE id = ?1",
                    [server_id],
                    |row| row.get(0),
                )
                .unwrap_or_default())
        })
        .await
        .unwrap_or_default()
    }

    /// 聚合窗口管理面（测试观测用）。
    pub fn pending_buckets(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .buckets
            .len()
    }
}

/// 测试辅助：构建 Value 形态的响应预览（保留给后续 admin 接口）。
#[allow(dead_code)]
pub fn channel_preview(channel: &repo::NotificationRow, message: &str) -> Value {
    json!({
        "id": channel.id,
        "name": channel.name,
        "url": expand_placeholders(&channel.url, message, None, util_time::now_nanos(), true),
    })
}

#[cfg(test)]
mod ssrf_tests {
    use super::{check_ssrf, ip_is_internal, normalize_ip};

    fn internal(ip: &str) -> bool {
        ip.parse::<std::net::IpAddr>()
            .map(ip_is_internal)
            .expect("parse ip")
    }

    #[test]
    fn literal_internal_addresses_are_rejected() {
        for host in [
            "127.0.0.1",
            "127.1.2.3",
            "10.0.0.1",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254", // 云元数据
            "0.0.0.0",         // 多数系统上等于本机
            "100.64.0.1",      // CGNAT
            "192.0.0.1",
            "198.18.0.1",
            "240.0.0.1",
            "255.255.255.255",
            "::1",
            "::",
            "fe80::1",
            "fc00::1",
            "ff02::1",
            // IPv4-mapped / IPv4-compatible：字面判定会整个放过。
            "::ffff:127.0.0.1",
            "::ffff:169.254.169.254",
            "::ffff:10.0.0.1",
            "::127.0.0.1",
        ] {
            assert!(internal(host), "{host} 应判为内部地址");
        }
    }

    #[test]
    fn public_addresses_are_allowed() {
        for host in ["1.1.1.1", "8.8.8.8", "93.184.216.34", "2606:4700::1111"] {
            assert!(!internal(host), "{host} 是公网地址，不应被拒");
        }
    }

    #[test]
    fn normalize_unwraps_mapped_and_compatible_v6() {
        assert_eq!(
            normalize_ip("::ffff:127.0.0.1".parse().unwrap()),
            "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
        );
        assert_eq!(
            normalize_ip("::10.0.0.1".parse().unwrap()),
            "10.0.0.1".parse::<std::net::IpAddr>().unwrap()
        );
        // 非映射地址保持原样。
        assert_eq!(
            normalize_ip("2606:4700::1111".parse().unwrap()),
            "2606:4700::1111".parse::<std::net::IpAddr>().unwrap()
        );
    }

    #[test]
    fn check_ssrf_rejects_internal_urls() {
        for url in [
            "http://127.0.0.1/hook",
            "http://0.0.0.0/hook",
            "http://169.254.169.254/latest/meta-data/",
            "http://[::ffff:127.0.0.1]/hook",
            "http://[::1]/hook",
            "http://localhost/hook",
        ] {
            assert!(check_ssrf(url).is_err(), "{url} 应被拒");
        }
    }

    /// 首跳公网地址 → 302 到内网：重定向策略必须把这一跳拦下来。
    /// （仅校验首跳 URL 的实现会在这里被绕过。）
    #[test]
    fn redirect_policy_blocks_internal_hop() {
        use std::io::{Read as _, Write as _};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming().take(4) {
                let Ok(mut stream) = stream else { continue };
                let mut buf = [0u8; 4096];
                let Ok(n) = stream.read(&mut buf) else {
                    continue;
                };
                let request = String::from_utf8_lossy(&buf[..n]).to_string();
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/")
                    .to_string();
                let body = if path == "/go" {
                    // 公开路径 302 到云元数据地址。
                    b"moved".to_vec()
                } else {
                    b"secret".to_vec()
                };
                let header = if path == "/go" {
                    format!(
                        "HTTP/1.1 302 Found\r\nLocation: http://169.254.169.254/latest/meta-data/\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                } else {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                };
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.write_all(&body);
            }
        });

        let client = reqwest::blocking::Client::builder()
            .redirect(super::ssrf_redirect_policy())
            .build()
            .unwrap();
        let error = client
            .get(format!("http://{addr}/go"))
            .send()
            .expect_err("302 到内网必须失败");
        // reqwest 把自定义策略错误包成 `error following redirect for url (…)`，
        // 真实原因在 source 链里。
        let mut chain = Vec::new();
        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        while let Some(err) = current {
            chain.push(err.to_string());
            current = err.source();
        }
        let joined = chain.join(" -> ");
        assert!(
            joined.contains("redirect target rejected"),
            "错误链应指明重定向目标被拒，实际：{joined}"
        );
    }

    #[test]
    fn check_ssrf_allows_public_urls() {
        assert!(check_ssrf("https://example.com/hook").is_ok());
        assert!(check_ssrf("https://1.1.1.1/hook").is_ok());
    }
}
