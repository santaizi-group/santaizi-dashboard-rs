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

use crate::config::{self, ConfigHandle};
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
        let row: Option<(String, Option<Vec<u8>>, Option<Vec<u8>>, String)> = conn
            .query_row(
                "SELECT COALESCE(s.name, ''), r.state_payload, r.host_payload, COALESCE(r.last_ip, '') \
                 FROM servers s LEFT JOIN server_runtimes r ON r.server_id = s.id \
                 WHERE s.id = ?1 AND s.deleted_at IS NULL",
                [server_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let Some((name, state_payload, host_payload, last_ip)) = row else {
            return Ok(None);
        };
        let state = state_payload
            .as_deref()
            .and_then(|payload| proto::State::decode(payload).ok())
            .unwrap_or_default();
        let mut host = host_payload
            .as_deref()
            .and_then(|payload| proto::Host::decode(payload).ok())
            .unwrap_or_default();
        if host.ip.trim().is_empty() && !last_ip.trim().is_empty() {
            host.ip = last_ip;
        }
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

/// 空通知组归一为 `default`，避免配置缺省或空串导致通知静默落空。
pub fn normalize_tag(tag: &str) -> &str {
    let trimmed = tag.trim();
    if trimmed.is_empty() {
        "default"
    } else {
        trimmed
    }
}

/// IP 打码（Go `IPDesensitize` 的保守形态）：未开启明文 IP 时逐位掩码。
/// IPv4 只掩数字；IPv6 额外掩十六进制字母——只掩数字会把 `a-f` 整段原样暴露。
pub fn desensitize_ip(ip: &str, plain: bool) -> String {
    let trimmed = ip.trim();
    if plain || trimmed.is_empty() {
        return trimmed.to_string();
    }
    let v6 = trimmed.contains(':');
    trimmed
        .chars()
        .map(|ch| {
            let masked = if v6 {
                ch.is_ascii_hexdigit()
            } else {
                ch.is_ascii_digit()
            };
            if masked { '*' } else { ch }
        })
        .collect()
}

fn desensitize(ip: &str, plain: bool) -> String {
    desensitize_ip(ip, plain)
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
    expand_placeholders_in_location(text, message, server, now_nanos, "UTC", plain_ip, mod_value)
}

/// 全部已知占位符键。均以 `#` 收尾，前缀匹配因此无歧义
/// （`#SERVER.MEM#` 不可能误吃 `#SERVER.MEMUSED#`）。
const PLACEHOLDER_KEYS: &[&str] = &[
    "#SANTAIZI#",
    "#DATETIME#",
    "#SERVER.NAME#",
    "#SERVER.ID#",
    "#SERVER.CPU#",
    "#SERVER.MEM#",
    "#SERVER.SWAP#",
    "#SERVER.DISK#",
    "#SERVER.MEMUSED#",
    "#SERVER.SWAPUSED#",
    "#SERVER.DISKUSED#",
    "#SERVER.MEMTOTAL#",
    "#SERVER.SWAPTOTAL#",
    "#SERVER.DISKTOTAL#",
    "#SERVER.NETINSPEED#",
    "#SERVER.NETOUTSPEED#",
    "#SERVER.TRANSFERIN#",
    "#SERVER.NETINTRANSFER#",
    "#SERVER.TRANSFEROUT#",
    "#SERVER.NETOUTTRANSFER#",
    "#SERVER.LOAD1#",
    "#SERVER.LOAD5#",
    "#SERVER.LOAD15#",
    "#SERVER.TCPCONNCOUNT#",
    "#SERVER.UDPCONNCOUNT#",
    "#SERVER.IP#",
    "#SERVER.IPV4#",
    "#SERVER.IPV6#",
];

pub fn expand_placeholders_in_location(
    text: &str,
    message: &str,
    server: Option<&ServerContext>,
    now_nanos: i64,
    location: &str,
    plain_ip: bool,
    mod_value: impl Fn(&str) -> String,
) -> String {
    let datetime_text = if location.eq_ignore_ascii_case("UTC") {
        util_time::unix_nanos_to_rfc3339(now_nanos)
            .replace('T', " ")
            .replace('Z', " +0000 UTC")
    } else {
        util_time::format_nanos_in_location(now_nanos, location)
    };
    let mut slots: Vec<(&'static str, String)> = Vec::with_capacity(PLACEHOLDER_KEYS.len());
    slots.push(("#SANTAIZI#", message.to_string()));
    slots.push(("#DATETIME#", datetime_text));
    if let Some(server) = server {
        slots.extend(server_slots(server, plain_ip));
    } else {
        slots.extend(empty_server_slots());
    }
    // 单遍扫描替换：替换值里再出现占位符字面量也不会被二次展开。
    // 通知正文混入远端可控文本（证书颁发机构、探针错误串），按序 `replace`
    // 会让 `#SERVER.IP#` 之类在外部文本里被二次展开并外泄主机属性。
    let mut out = String::with_capacity(text.len() + 64);
    let mut rest = text;
    while let Some(pos) = rest.find('#') {
        out.push_str(&rest[..pos]);
        let tail = &rest[pos..];
        match slots.iter().find(|(key, _)| tail.starts_with(key)) {
            Some((key, value)) => {
                out.push_str(&mod_value(value));
                rest = &tail[key.len()..];
            }
            None => {
                out.push('#');
                rest = &tail[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// URL 形态（GET Webhook / 带查询串的 POST）：值按查询分量百分号编码。
/// 证书颁发机构、探针错误串、IP 经 `#SANTAIZI#` 拼进 URL 时不编码，
/// 其中的 `&`、`=`、`#` 会篡改查询参数或截断后缀参数。
pub fn expand_placeholders_in_url(
    text: &str,
    message: &str,
    server: Option<&ServerContext>,
    now_nanos: i64,
    location: &str,
    plain_ip: bool,
) -> String {
    expand_placeholders_in_location(
        text,
        message,
        server,
        now_nanos,
        location,
        plain_ip,
        url_encode_query_value,
    )
}

/// RFC 3986 百分号编码：仅保留未保留字符 `A-Za-z0-9-._~`，其余字节（含空格）一律编码为 `%XX`。
/// 空格编码为 `%20` 而非 `+`，避免 Bark 等将 `#SANTAIZI#` 放入 URL Path 的渠道出现满屏 `+` 号，
/// 同时在 Query 参数中也能按标准解码为空格。
fn url_encode_query_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for &byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(byte as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(&mut out, "%{byte:02X}");
        }
    }
    out
}

/// 无 `ServerContext` 时（测试通知、聚合通知、从端通知）的 `#SERVER.*#` 兜底值：
/// 文本槽位给空串，数值槽位给 `"0"`，防止自定义 JSON 模板中未加引号的 `#SERVER.CPU#` 原样残留损坏 JSON 语法。
fn empty_server_slots() -> Vec<(&'static str, String)> {
    vec![
        ("#SERVER.NAME#", String::new()),
        ("#SERVER.ID#", "0".to_string()),
        ("#SERVER.CPU#", "0".to_string()),
        ("#SERVER.MEM#", "0".to_string()),
        ("#SERVER.SWAP#", "0".to_string()),
        ("#SERVER.DISK#", "0".to_string()),
        ("#SERVER.MEMUSED#", "0".to_string()),
        ("#SERVER.SWAPUSED#", "0".to_string()),
        ("#SERVER.DISKUSED#", "0".to_string()),
        ("#SERVER.MEMTOTAL#", "0".to_string()),
        ("#SERVER.SWAPTOTAL#", "0".to_string()),
        ("#SERVER.DISKTOTAL#", "0".to_string()),
        ("#SERVER.NETINSPEED#", "0".to_string()),
        ("#SERVER.NETOUTSPEED#", "0".to_string()),
        ("#SERVER.TRANSFERIN#", "0".to_string()),
        ("#SERVER.NETINTRANSFER#", "0".to_string()),
        ("#SERVER.TRANSFEROUT#", "0".to_string()),
        ("#SERVER.NETOUTTRANSFER#", "0".to_string()),
        ("#SERVER.LOAD1#", "0".to_string()),
        ("#SERVER.LOAD5#", "0".to_string()),
        ("#SERVER.LOAD15#", "0".to_string()),
        ("#SERVER.TCPCONNCOUNT#", "0".to_string()),
        ("#SERVER.UDPCONNCOUNT#", "0".to_string()),
        ("#SERVER.IP#", String::new()),
        ("#SERVER.IPV4#", String::new()),
        ("#SERVER.IPV6#", String::new()),
    ]
}

/// `#SERVER.*#` 的值表（无 server 时由调用方整表省略）。
fn server_slots(server: &ServerContext, plain_ip: bool) -> Vec<(&'static str, String)> {
    let state = &server.state;
    let host = &server.host;
    let mut slots = vec![
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
    let ip = host.ip.trim();
    let (ipv4, ipv6, valid) = match ip.split_once('/') {
        Some((v4, v6)) => (v4.to_string(), v6.to_string(), v4.to_string()),
        None if ip.contains(':') => (String::new(), ip.to_string(), ip.to_string()),
        None if !ip.is_empty() => (ip.to_string(), String::new(), ip.to_string()),
        _ => (String::new(), String::new(), String::new()),
    };
    slots.push(("#SERVER.IP#", desensitize(&valid, plain_ip)));
    slots.push(("#SERVER.IPV4#", desensitize(&ipv4, plain_ip)));
    slots.push(("#SERVER.IPV6#", desensitize(&ipv6, plain_ip)));
    slots
}

/// `utils.Json.Marshal(msg)` 去掉首尾引号：JSON 字符串转义。
fn json_escape(message: &str) -> String {
    let encoded = serde_json::to_string(message).unwrap_or_default();
    encoded[1..encoded.len() - 1].to_string()
}

/// Header 名/值清洗：剔除全部控制字符（含 `\r`/`\n`），空值返回 `None`。
fn sanitize_header_text(raw: &str) -> Option<String> {
    let cleaned: String = raw.chars().filter(|ch| !ch.is_control()).collect();
    if cleaned.trim().is_empty() {
        None
    } else {
        Some(cleaned)
    }
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

fn active_location() -> String {
    if let Some(lock) = ACTIVE_CONF.get()
        && let Ok(guard) = lock.read()
        && let Some(weak) = guard.as_ref()
        && let Some(handle) = weak.upgrade()
        && let Ok(conf) = handle.read()
        && !conf.location.trim().is_empty()
    {
        return conf.location.clone();
    }
    "Asia/Shanghai".to_string()
}

static ACTIVE_CONF: std::sync::OnceLock<
    std::sync::RwLock<Option<std::sync::Weak<std::sync::RwLock<Arc<config::Config>>>>>,
> = std::sync::OnceLock::new();

fn register_active_conf(conf: &ConfigHandle) {
    let slot = ACTIVE_CONF.get_or_init(|| std::sync::RwLock::new(None));
    if let Ok(mut guard) = slot.write() {
        *guard = Some(Arc::downgrade(conf));
    }
}

/// 单渠道投递（阻塞；调用方放 `spawn_blocking`）。语义对齐 Go
/// `NotificationServerBundle.Send` + `reqBody`：GET 无 body；POST JSON 模板
/// 全占位符按 json_escape 展开；POST Form 按键值展开后 url 编码。
/// 默认读取当前运行态配置时区（回退 `Asia/Shanghai`），避免测试通知硬编码 `"UTC"`。
pub fn deliver_channel_blocking(
    channel: &repo::NotificationRow,
    message: &str,
    server: Option<&ServerContext>,
    now_nanos: i64,
) -> Result<(), String> {
    let location = active_location();
    deliver_channel_with_options(channel, message, server, now_nanos, true, &location, false)
}

/// 带显式时区的单渠道阻塞投递入口。
pub fn deliver_channel_blocking_in_location(
    channel: &repo::NotificationRow,
    message: &str,
    server: Option<&ServerContext>,
    now_nanos: i64,
    location: &str,
) -> Result<(), String> {
    deliver_channel_with_options(channel, message, server, now_nanos, true, location, false)
}

/// 测试缝隙：允许内网地址（本地 stub 集成测试用；生产恒走严格 SSRF）。
pub fn deliver_channel_allow_internal(
    channel: &repo::NotificationRow,
    message: &str,
    server: Option<&ServerContext>,
    now_nanos: i64,
) -> Result<(), String> {
    let location = active_location();
    deliver_channel_with_options(channel, message, server, now_nanos, true, &location, true)
}

#[allow(clippy::too_many_arguments)]
pub fn deliver_channel_with_options(
    channel: &repo::NotificationRow,
    message: &str,
    server: Option<&ServerContext>,
    now_nanos: i64,
    plain_ip: bool,
    location: &str,
    allow_internal: bool,
) -> Result<(), String> {
    let is_post = channel.request_method == repo::NOTIFY_METHOD_POST;
    // URL 里的占位符值必须编码：GET Webhook 的查询串被外部文本注入参数会
    // 改变目标语义，甚至借 `#` 截断后缀参数。
    let url =
        expand_placeholders_in_url(&channel.url, message, server, now_nanos, location, plain_ip);
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
            let expanded = expand_placeholders_in_location(
                value,
                message,
                server,
                now_nanos,
                location,
                plain_ip,
                |v| v.to_string(),
            );
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
        expand_placeholders_in_location(
            &channel.request_body,
            message,
            server,
            now_nanos,
            location,
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
            let expanded = expand_placeholders_in_location(
                &value,
                message,
                server,
                now_nanos,
                location,
                plain_ip,
                |v| v.to_string(),
            );
            // Header 名与值都剔除控制字符：`#SANTAIZI#` 展开出多行正文时
            // `\r`/`\n` 会让整次请求在 Header 校验阶段静默失败。
            let Some(key) = sanitize_header_text(&key) else {
                continue;
            };
            let Some(value) = sanitize_header_text(&expanded) else {
                continue;
            };
            let Ok(name) = reqwest::header::HeaderName::from_bytes(key.as_bytes()) else {
                continue;
            };
            let Ok(value) = reqwest::header::HeaderValue::from_str(&value) else {
                continue;
            };
            request = request.header(name, value);
        }
    }
    let response = request.send().map_err(|err| err.to_string())?;
    let status = response.status();
    if !(200..300).contains(&status.as_u16()) {
        let text = response.text().unwrap_or_default();
        return Err(format!("{}@{} {text}", status.as_u16(), status));
    }
    let mut body_bytes = Vec::new();
    {
        use std::io::Read as _;
        let _ = std::io::Read::take(response, 64 * 1024).read_to_end(&mut body_bytes);
    }
    check_webhook_business_error(&body_bytes)?;
    Ok(())
}

/// 识别企微/钉钉（非零 `errcode`）与飞书等（非零且非 200 的整数 `code`）在 HTTP 2xx 下的业务失败响应。
/// 注：Bark 与 PushPlus 成功时返回 `{"code": 200, ...}`，故放行 `0` 与 `200`。
fn check_webhook_business_error(body: &[u8]) -> Result<(), String> {
    const MAX_JSON_CHECK_BYTES: usize = 16 * 1024;
    if body.is_empty() || body.len() > MAX_JSON_CHECK_BYTES {
        return Ok(());
    }
    let Ok(text) = std::str::from_utf8(body) else {
        return Ok(());
    };
    let trimmed = text.trim();
    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return Ok(());
    }
    let Ok(Value::Object(map)) = serde_json::from_str::<Value>(trimmed) else {
        return Ok(());
    };
    if let Some(errcode) = map.get("errcode").and_then(Value::as_i64)
        && errcode != 0
    {
        let detail = map
            .get("errmsg")
            .or_else(|| map.get("msg"))
            .or_else(|| map.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        return if detail.is_empty() {
            Err(format!("errcode={errcode}"))
        } else {
            Err(format!("errcode={errcode}: {detail}"))
        };
    }
    if let Some(code) = map.get("code").and_then(Value::as_i64)
        && code != 0
        && code != 200
    {
        let detail = map
            .get("msg")
            .or_else(|| map.get("errmsg"))
            .or_else(|| map.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        return if detail.is_empty() {
            Err(format!("code={code}"))
        } else {
            Err(format!("code={code}: {detail}"))
        };
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
        ("HOST_OFFLINE", "主机离线", "node="),
        ("COLLECTOR_OFFLINE", "从端离线", "collector="),
        ("COLLECTOR_ONLINE", "从端上线", "collector="),
        ("CONNECTIVITY_DEGRADED", "连通性降级", "node="),
        ("TELEMETRY_DATA_LOSS", "数据丢失", "component="),
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
    // 只有批量事件才进聚合窗口；规则通知（规则触发/规则恢复）直接投递。
    match kind {
        "主机离线" | "从端离线" | "从端上线" | "主机恢复" | "连通性降级" | "数据丢失"
        | "探测中断" | "探测恢复" | "延迟异常"
        // 旧版文案，已在队列里的仍需归并。
        | "离线" | "恢复" | "连通降级" | "探测丢失" | "ProbeDown" | "ProbeUp" | "ProbeLatency" => {
            Some((
                kind.to_string(),
                parse_aggregated_names(&trimmed[end + 1..]),
            ))
        }
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

/// `[kind]` 之后的名字段。两种形态：
/// - 单条事件：`name`，正文细节在后续行——只取首行，避免把「最后上报…」当名字；
/// - 已批量：`N 台/条` 头 + 每行一个名字——跳过表头取全部行。
fn parse_aggregated_names(rest: &str) -> Vec<String> {
    let mut lines = rest.lines().map(str::trim).filter(|line| !line.is_empty());
    let Some(first) = lines.next() else {
        return Vec::new();
    };
    let header = first
        .strip_suffix(" 台")
        .or_else(|| first.strip_suffix(" 条"));
    if header.is_some_and(|count| {
        !count.trim().is_empty() && count.trim().chars().all(|ch| ch.is_ascii_digit())
    }) {
        return lines.map(str::to_string).collect();
    }
    // 单条原始消息只对应单个实体名，不按英文逗号 `,` 切分，防止含逗号主机名（如 `HK, CN-01`）被拆成多台假聚合。
    vec![first.to_string()]
}

/// 离线原因的人类可读文案（`detect_offline_reason` 的三种取值）。
fn offline_reason_text(reason: &str) -> &'static str {
    match reason {
        "machine_reboot" => "机器重启",
        "network_disconnect" => "网络断线 / 上报中断",
        _ => "原因未知",
    }
}

/// Go `formatAggregatedNotification`：`[kind] N 台/条` + 每行一个名字。
pub fn format_aggregated(kind: &str, names: &[String]) -> String {
    let unit = match kind {
        "主机离线" | "从端离线" | "从端上线" | "主机恢复" | "连通性降级"
        // 旧版文案。
        | "离线" | "恢复" | "连通降级" => "台",
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

struct AggregateBucket {
    kind: String,
    names: Vec<String>,
    first: String,
    first_server_id: Option<i64>,
    expires: Instant,
}

#[derive(Default)]
struct NotifyState {
    mutes: HashMap<String, MuteState>,
    buckets: HashMap<String, AggregateBucket>,
}

/// 静音退避重置阈值：上次静音过期超过 1 小时后再次触发视为新一轮独立故障，退避重置回 15 分钟。
const MUTE_BACKOFF_RESET_AFTER: Duration = Duration::from_secs(60 * 60);
/// 静音条目驱逐阈值：过期超过 2 小时顺手清理，防止 `mutes` 随一次性键名无界增长。
const MUTE_EVICT_AFTER: Duration = Duration::from_secs(2 * 60 * 60);

fn evaluate_mute(mutes: &mut HashMap<String, MuteState>, mute_key: String, now: Instant) -> bool {
    mutes.retain(|_, entry| now.saturating_duration_since(entry.until) < MUTE_EVICT_AFTER);
    match mutes.get(&mute_key) {
        Some(entry) => {
            if now >= entry.until {
                let expired_for = now.saturating_duration_since(entry.until);
                let duration = if expired_for >= MUTE_BACKOFF_RESET_AFTER {
                    FIRST_MUTE
                } else {
                    (entry.duration * 2).min(MAX_MUTE)
                };
                mutes.insert(
                    mute_key,
                    MuteState {
                        duration,
                        until: now + duration,
                    },
                );
                true
            } else {
                false
            }
        }
        None => {
            mutes.insert(
                mute_key,
                MuteState {
                    duration: FIRST_MUTE,
                    until: now + FIRST_MUTE,
                },
            );
            true
        }
    }
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
        register_active_conf(&conf);
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
        let tag = normalize_tag(tag);
        let mute_key = format!("{label}\x00{tag}");
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .mutes
            .remove(&mute_key);
    }

    /// 查询某条静音键当前是否仍在静音窗口内（测试观测用）。
    pub fn is_muted(&self, tag: &str, label: &str) -> bool {
        let tag = normalize_tag(tag);
        let mute_key = format!("{label}\x00{tag}");
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .mutes
            .get(&mute_key)
            .is_some_and(|entry| Instant::now() < entry.until)
    }

    /// Go `SendNotification`：防骚扰 → 聚合 → 投递。
    pub async fn send(
        &self,
        tag: &str,
        desc: &str,
        mute_label: Option<String>,
        server_id: Option<i64>,
    ) {
        let _ = self.send_checked(tag, desc, mute_label, server_id).await;
    }

    /// 带返回值的发送入口：返回 `true` 表示已入聚合桶或直接投递，`false` 表示被防骚扰静音抑制。
    pub async fn send_checked(
        &self,
        tag: &str,
        desc: &str,
        mute_label: Option<String>,
        server_id: Option<i64>,
    ) -> bool {
        let tag = normalize_tag(tag);
        if let Some(label) = &mute_label {
            let mute_key = format!("{label}\x00{tag}");
            let should_send = {
                let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
                evaluate_mute(&mut state.mutes, mute_key, Instant::now())
            };
            if !should_send {
                return false;
            }
        }
        if let Some((kind, names)) = classify(desc) {
            self.enqueue_aggregate(tag.to_string(), kind, names, desc.to_string(), server_id);
            return true;
        }
        self.deliver(tag, desc, server_id).await;
        true
    }

    /// 聚合窗口入桶；同 (tag, kind) 首条启动计时，单条保留 `server_id` 以展开 `#SERVER.*#`。
    fn enqueue_aggregate(
        &self,
        tag: String,
        kind: String,
        names: Vec<String>,
        first: String,
        server_id: Option<i64>,
    ) {
        let key = format!("{tag}\x00{kind}");
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        let bucket = state.buckets.entry(key).or_insert_with(|| AggregateBucket {
            kind,
            names: Vec::new(),
            first,
            first_server_id: server_id,
            expires: Instant::now() + AGG_WINDOW,
        });
        for name in names {
            let name = name.trim();
            if !name.is_empty() && !bucket.names.iter().any(|seen| seen == name) {
                bucket.names.push(name.to_string());
            }
        }
    }

    async fn flush_buckets(&self, items: Vec<(String, AggregateBucket)>) {
        for (key, bucket) in items {
            let tag = key.split('\x00').next().unwrap_or(&key).to_string();
            let (desc, target_server) = if bucket.names.len() > 1 {
                (format_aggregated(&bucket.kind, &bucket.names), None)
            } else {
                (bucket.first, bucket.first_server_id)
            };
            self.deliver(&tag, &desc, target_server).await;
        }
    }

    /// 冲刷所有已到期的聚合桶。
    pub async fn flush_due(&self) {
        let expired: Vec<(String, AggregateBucket)> = {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            let now = Instant::now();
            let keys: Vec<String> = state
                .buckets
                .iter()
                .filter(|entry| entry.1.expires <= now)
                .map(|entry| entry.0.clone())
                .collect();
            keys.into_iter()
                .filter_map(|key| state.buckets.remove(&key).map(|bucket| (key, bucket)))
                .collect()
        };
        self.flush_buckets(expired).await;
    }

    /// 停机收尾冲刷：把队列里尚未满 15 秒窗口的待发聚合通知全部冲出，防止进程退出丢告警。
    pub async fn flush_all(&self) {
        let remaining: Vec<(String, AggregateBucket)> = {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            let mut entries: Vec<(String, AggregateBucket)> = state.buckets.drain().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            entries
        };
        self.flush_buckets(remaining).await;
    }

    /// 聚合清扫循环（spawn_background_workers 起一份）；停机信号到达时冲刷剩余桶再退出。
    pub async fn run_flusher(self: Arc<Self>, shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut shutdown = shutdown;
        let mut ticker = tokio::time::interval(Duration::from_millis(500));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.flush_due().await;
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.flush_all().await;
                        return;
                    }
                }
            }
        }
    }

    /// 按 tag 投递全部渠道（DB 读走 blocking_read，HTTP 放 spawn_blocking）。
    pub async fn deliver(&self, tag: &str, desc: &str, server_id: Option<i64>) {
        let tag = normalize_tag(tag).to_string();
        let desc = desc.to_string();
        let now_nanos = util_time::now_nanos();
        let (plain_ip, location) = {
            let conf = self.conf.read().expect("config lock");
            (conf.enable_plain_ip_in_notification, conf.location.clone())
        };
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
                if let Err(err) = deliver_channel_with_options(
                    channel,
                    &desc,
                    server.as_ref(),
                    now_nanos,
                    plain_ip,
                    &location,
                    false,
                ) {
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
    /// 进入离线态必须先解除该主机的恢复静音，保证下次恢复（以及恢复后的再次离线）成对送达。
    pub async fn notify_offline(
        &self,
        server_id: i64,
        started_at_nanos: i64,
        last_seen_nanos: i64,
        threshold_seconds: u64,
        last_ip: String,
    ) {
        self.unmute("default", &format!("server-recovery-{server_id}"));
        let (enabled, plain, location) = {
            let conf = self.conf.read().expect("config lock");
            (
                conf.enable_offline_notification,
                conf.enable_plain_ip_in_notification,
                conf.location.clone(),
            )
        };
        if !enabled {
            return;
        }
        let name = self.display_server_name(server_id).await;
        let ip_text = format_ip_or_unknown(&last_ip, plain);
        let message = format!(
            "[主机离线] {name}\n最后上报：{}\n离线开始：{}\n离线阈值：{}\n离线前 IP：{ip_text}",
            util_time::format_nanos_in_location(last_seen_nanos, &location),
            util_time::format_nanos_in_location(started_at_nanos, &location),
            util_time::format_duration_zh(threshold_seconds),
        );
        self.send(
            "default",
            &message,
            Some(format!("server-offline-{server_id}")),
            Some(server_id),
        )
        .await;
    }

    /// 恢复通知（Go `sendRecoveryNotification`）。
    /// 恢复必须先解除离线静音，即使恢复通知本身关闭也不能让离线静音残留到下一轮故障。
    pub async fn notify_recovery(
        &self,
        server_id: i64,
        recovered_at_nanos: i64,
        offline_seconds: i64,
        last_ip: String,
        recovered_ip: String,
        reason: &str,
    ) {
        self.unmute("default", &format!("server-offline-{server_id}"));
        let (enabled, plain, location) = {
            let conf = self.conf.read().expect("config lock");
            (
                conf.enable_recovery_notification,
                conf.enable_plain_ip_in_notification,
                conf.location.clone(),
            )
        };
        if !enabled {
            return;
        }
        let name = self.display_server_name(server_id).await;
        let duration_secs = u64::try_from(offline_seconds.max(0)).unwrap_or(0);
        let ip_lines = format_recovery_ip_lines(&last_ip, &recovered_ip, plain);
        let message = format!(
            "[主机恢复] {name}\n恢复时间：{}\n离线时长：{}\n原因：{}\n{ip_lines}",
            util_time::format_nanos_in_location(recovered_at_nanos, &location),
            util_time::format_duration_zh(duration_secs),
            offline_reason_text(reason),
        );
        self.send(
            "default",
            &message,
            Some(format!("server-recovery-{server_id}")),
            Some(server_id),
        )
        .await;
    }

    /// IP 变更通知（当 `enable_ip_change_notification` 打开且上报 IP 发生实质变化时触发）。
    /// 覆盖范围与忽略名单沿用 Go `Cover` / `IgnoredIPNotificationServerIDs` 语义：
    /// `COVER_ALL` 提醒未在名单内的主机，`COVER_IGNORE_ALL` 只提醒名单内的主机。
    pub async fn notify_ip_change(&self, server_id: i64, old_ip: &str, new_ip: &str) {
        let old_trimmed = old_ip.trim();
        let new_trimmed = new_ip.trim();
        if old_trimmed.is_empty() || new_trimmed.is_empty() || old_trimmed == new_trimmed {
            return;
        }
        let (enabled, tag, plain, location, in_scope) = {
            let conf = self.conf.read().expect("config lock");
            let ignored = u64::try_from(server_id)
                .ok()
                .is_some_and(|id| conf.ignored_ip_notification_server_ids.contains(&id));
            let in_scope = match conf.cover {
                config::COVER_IGNORE_ALL => ignored,
                // 未知取值按「提醒全部」处理，与 Go 默认的 `ConfigCoverAll` 一致。
                _ => !ignored,
            };
            (
                conf.enable_ip_change_notification,
                conf.ip_change_notification_tag.clone(),
                conf.enable_plain_ip_in_notification,
                conf.location.clone(),
                in_scope,
            )
        };
        if !enabled || !in_scope {
            return;
        }
        let name = self.display_server_name(server_id).await;
        let now = util_time::now_nanos();
        let message = format!(
            "[IP 变更] {name}\n原 IP：{}\n新 IP：{}\n时间：{}",
            desensitize_ip(old_trimmed, plain),
            desensitize_ip(new_trimmed, plain),
            util_time::format_nanos_in_location(now, &location),
        );
        // 静音键只带 server_id：掺入不可信 IP 既会被伪造值绕过 15 分钟防骚扰，
        // 又会让 `mutes` 键随上报值无限增长（该表从不驱逐过期项）。
        self.send(
            &tag,
            &message,
            Some(format!("ip-change-{server_id}")),
            Some(server_id),
        )
        .await;
    }

    async fn display_server_name(&self, server_id: i64) -> String {
        let name = self.server_name(server_id).await;
        let trimmed = name.trim();
        if trimmed.is_empty() {
            format!("#{server_id}")
        } else {
            trimmed.to_string()
        }
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

fn format_ip_or_unknown(ip: &str, plain: bool) -> String {
    let trimmed = ip.trim();
    if trimmed.is_empty() {
        "未知".to_string()
    } else {
        desensitize_ip(trimmed, plain)
    }
}

fn format_recovery_ip_lines(last_ip: &str, recovered_ip: &str, plain: bool) -> String {
    let old_ip = last_ip.trim();
    let new_ip = recovered_ip.trim();
    match (old_ip.is_empty(), new_ip.is_empty()) {
        (true, true) => "当前 IP：未知".to_string(),
        (false, true) => format!("当前 IP：{}", desensitize_ip(old_ip, plain)),
        (true, false) => format!("当前 IP：{}", desensitize_ip(new_ip, plain)),
        (false, false) if old_ip == new_ip => {
            format!("当前 IP：{}", desensitize_ip(new_ip, plain))
        }
        (false, false) => format!(
            "离线前 IP：{}\n恢复后 IP：{}",
            desensitize_ip(old_ip, plain),
            desensitize_ip(new_ip, plain)
        ),
    }
}

/// 测试辅助：构建 Value 形态的响应预览（保留给后续 admin 接口）。
#[allow(dead_code)]
pub fn channel_preview(channel: &repo::NotificationRow, message: &str) -> Value {
    json!({
        "id": channel.id,
        "name": channel.name,
        // 与真实投递同一套编码，预览才是「实际会发出去的 URL」。
        "url": expand_placeholders_in_url(&channel.url, message, None, util_time::now_nanos(), "UTC", true),
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

#[cfg(test)]
mod text_tests {
    use super::*;

    #[test]
    fn rich_offline_message_aggregates_by_host_name_only() {
        let message = "[主机离线] node-a\n最后上报：2026-11-06 10:00:00+00:00\n\
                       离线开始：2026-11-06 10:00:30+00:00\n离线阈值：30s\n离线前 IP：203.0.113.9";
        let (kind, names) = classify(message).expect("主机离线归入批量类");
        assert_eq!(kind, "主机离线");
        assert_eq!(names, vec!["node-a"], "细节行不得混进主机名");
        assert_eq!(format_aggregated(&kind, &names), "[主机离线] 1 台\nnode-a");
    }

    #[test]
    fn pre_bursted_message_drops_the_count_header() {
        let (kind, names) = classify("[主机离线] 2 台\nnode-a\nnode-b").expect("批量类");
        assert_eq!(kind, "主机离线");
        assert_eq!(names, vec!["node-a", "node-b"]);
        assert_eq!(
            format_aggregated(&kind, &names),
            "[主机离线] 2 台\nnode-a\nnode-b"
        );
    }

    #[test]
    fn recovery_reason_is_localized() {
        assert_eq!(offline_reason_text("machine_reboot"), "机器重启");
        assert_eq!(
            offline_reason_text("network_disconnect"),
            "网络断线 / 上报中断"
        );
        assert_eq!(offline_reason_text("unknown"), "原因未知");
    }

    #[test]
    fn recovery_ip_lines_avoid_redundancy_and_mask_properly() {
        assert_eq!(
            format_recovery_ip_lines("203.0.113.9", "203.0.113.9", false),
            "当前 IP：***.*.***.*"
        );
        assert_eq!(
            format_recovery_ip_lines("203.0.113.9", "198.51.100.2", true),
            "离线前 IP：203.0.113.9\n恢复后 IP：198.51.100.2"
        );
        assert_eq!(format_recovery_ip_lines("", "", true), "当前 IP：未知");
        assert_eq!(normalize_tag(""), "default");
        assert_eq!(normalize_tag("  ops "), "ops");
    }

    #[tokio::test]
    async fn offline_and_recovery_unmute_each_other_across_flaps() {
        let dir = std::env::temp_dir().join(format!(
            "santaizi-notify-flap-{}-{}",
            std::process::id(),
            util_time::now_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db = crate::db::migrate::open_primary(dir.join("panel.db").to_str().unwrap()).unwrap();
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, secret_ciphertext) VALUES (42, 'hk-edge', X'01')",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        let raw_conf = crate::config::Config {
            enable_offline_notification: true,
            enable_recovery_notification: false,
            ..Default::default()
        };
        let conf = crate::config::new_config_handle(raw_conf);
        let notifier = Notifier::new(db, conf.clone());

        let t0 = 1_792_071_600_i64 * 1_000_000_000;
        notifier
            .notify_offline(42, t0 + 30_000_000_000, t0, 30, "203.0.113.9".into())
            .await;
        assert!(
            notifier.is_muted("default", "server-offline-42"),
            "首次离线后进入防骚扰静音窗"
        );
        // 即使恢复通知关闭，主机恢复也必须解除离线静音，否则下一次真离线会被静默吞掉。
        notifier
            .notify_recovery(
                42,
                t0 + 90_000_000_000,
                60,
                "203.0.113.9".into(),
                "203.0.113.9".into(),
                "network_disconnect",
            )
            .await;
        assert!(
            !notifier.is_muted("default", "server-offline-42"),
            "恢复后离线静音必须被清除"
        );

        // 打开恢复通知：恢复 → 再次离线 → 恢复静音也必须同步清除。
        {
            let mut guard = conf.write().expect("config lock");
            let mut next = (**guard).clone();
            next.enable_recovery_notification = true;
            *guard = Arc::new(next);
        }
        notifier
            .notify_recovery(
                42,
                t0 + 120_000_000_000,
                90,
                "203.0.113.9".into(),
                "203.0.113.9".into(),
                "network_disconnect",
            )
            .await;
        assert!(notifier.is_muted("default", "server-recovery-42"));
        notifier
            .notify_offline(
                42,
                t0 + 180_000_000_000,
                t0 + 150_000_000_000,
                30,
                "203.0.113.9".into(),
            )
            .await;
        assert!(
            !notifier.is_muted("default", "server-recovery-42"),
            "再次离线后恢复静音必须被清除"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 单遍替换：外部文本里的占位符字面量不得被二次展开。
    /// 探针错误串/证书颁发机构经 `#SANTAIZI#` 进正文，按序 `replace` 会让
    /// `#SERVER.IP#` 在外部文本里生效并外泄主机属性。
    #[test]
    fn placeholders_are_expanded_once() {
        let server = ServerContext {
            id: 7,
            name: "edge".into(),
            state: proto::State::default(),
            host: proto::Host {
                ip: "203.0.113.9".into(),
                ..Default::default()
            },
        };
        let now = 1_792_071_600_i64 * 1_000_000_000;
        let text = "#SANTAIZI#|#SERVER.NAME#|#SERVER.IP#";
        let message = "error #SERVER.IP# injected";
        let out =
            expand_placeholders_in_location(text, message, Some(&server), now, "UTC", false, |v| {
                v.to_string()
            });
        assert_eq!(out, "error #SERVER.IP# injected|edge|***.*.***.*");
        // 正文里的 `#SERVER.NAME#` 保持字面量，未被二次展开。
        let out = expand_placeholders_in_location(
            "#SANTAIZI#",
            "boom #SERVER.NAME#",
            Some(&server),
            now,
            "UTC",
            false,
            |v| v.to_string(),
        );
        assert_eq!(out, "boom #SERVER.NAME#");
    }

    /// 未识别的 `#`（如 URL 片段）原样保留。
    #[test]
    fn unknown_hash_is_kept_verbatim() {
        let server = ServerContext {
            id: 7,
            name: "edge".into(),
            state: proto::State::default(),
            host: proto::Host::default(),
        };
        let out = expand_placeholders_in_location(
            "https://hook.test/x?a=1#frag-#SERVER.ID#",
            "",
            Some(&server),
            1_792_071_600_i64 * 1_000_000_000,
            "UTC",
            true,
            |v| v.to_string(),
        );
        assert_eq!(out, "https://hook.test/x?a=1#frag-7");
    }

    /// GET URL 的查询值必须编码：`&`/`=`/`#` 不得篡改或截断查询参数。
    #[test]
    fn url_values_are_percent_encoded() {
        let now = 1_792_071_600_i64 * 1_000_000_000;
        let out = expand_placeholders_in_url(
            "https://hook.test/x?a=1&b=#SANTAIZI#",
            "oops&evil=1#frag",
            None,
            now,
            "UTC",
            true,
        );
        assert_eq!(out, "https://hook.test/x?a=1&b=oops%26evil%3D1%23frag");
    }

    #[test]
    fn header_text_drops_control_characters() {
        assert_eq!(
            sanitize_header_text("a\r\nX-Evil: 1"),
            Some("aX-Evil: 1".into())
        );
        assert_eq!(sanitize_header_text("  \t "), None);
        assert_eq!(sanitize_header_text("plain"), Some("plain".into()));
    }

    /// IPv6 打码必须连十六进制字母一起掩：只掩数字会暴露 `a-f` 段。
    #[test]
    fn ipv6_desensitize_masks_hex_letters() {
        assert_eq!(desensitize_ip("2001:db8::1", false), "****:***::*");
        assert_eq!(desensitize_ip("203.0.113.9", false), "***.*.***.*");
        assert_eq!(desensitize_ip("2001:db8::1", true), "2001:db8::1");
    }

    /// IP 变更通知的覆盖范围：`COVER_ALL` 只放行名单外主机，
    /// `COVER_IGNORE_ALL` 只放行名单内主机（Go `Cover` + `IgnoredIPNotification` 语义）。
    /// 静音键只带 server_id——掺入不可信 IP 既能伪造值绕过防骚扰，
    /// 又会让 `mutes` 键随上报值无限增长。
    #[tokio::test]
    async fn ip_change_notification_respects_cover_and_ignore_list() {
        let dir = std::env::temp_dir().join(format!(
            "santaizi-notify-ipchange-{}-{}",
            std::process::id(),
            util_time::now_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db = crate::db::migrate::open_primary(dir.join("panel.db").to_str().unwrap()).unwrap();
        db.write(|tx| {
            for (index, id) in [41, 42, 43, 44].iter().enumerate() {
                // secret_ciphertext 有唯一索引，逐行给不同密文。
                tx.execute(
                    "INSERT INTO servers (id, name, secret_ciphertext) VALUES (?, ?, ?)",
                    rusqlite::params![id, format!("edge-{id}"), vec![index as u8 + 1]],
                )?;
            }
            Ok(())
        })
        .unwrap();

        let conf = crate::config::new_config_handle(crate::config::Config {
            enable_ip_change_notification: true,
            ip_change_notification_tag: "default".into(),
            ..Default::default()
        });
        // 单个 notifier 贯穿全程：静音表随实例走，才能区分「没发」与「发了被静音」。
        let notifier = Notifier::new(db.clone(), conf.clone());

        // 默认 cover=COVER_ALL、忽略名单为空 → 发送。
        notifier
            .notify_ip_change(41, "203.0.113.9", "198.51.100.2")
            .await;
        assert!(
            notifier.is_muted("default", "ip-change-41"),
            "发送后进入防骚扰静音窗"
        );
        // 伪造不同 IP 不得绕过：静音键不含 IP 值。
        notifier
            .notify_ip_change(41, "198.51.100.2", "192.0.2.7")
            .await;
        assert!(
            notifier.is_muted("default", "ip-change-41"),
            "静音键固定，换 IP 也绕不过 15 分钟窗"
        );
        assert!(
            !notifier.is_muted("default", "ip-change-41-192.0.2.7"),
            "不得再生成带 IP 的静音键"
        );

        // 忽略名单含 42 + COVER_ALL → 42 不发送，名单外主机照发。
        {
            let mut guard = conf.write().expect("config lock");
            let mut next = (**guard).clone();
            next.ignored_ip_notification = "42".into();
            // apply_defaults 会同步派生的忽略名单（update_ignored_ip_notification_ids）。
            next.apply_defaults();
            *guard = Arc::new(next);
        }
        notifier
            .notify_ip_change(42, "203.0.113.9", "198.51.100.2")
            .await;
        assert!(
            !notifier.is_muted("default", "ip-change-42"),
            "被忽略名单排除的主机不得触发 IP 变更通知"
        );
        notifier
            .notify_ip_change(43, "203.0.113.9", "198.51.100.2")
            .await;
        assert!(
            notifier.is_muted("default", "ip-change-43"),
            "名单外主机在 COVER_ALL 下照常提醒"
        );

        // 切到 COVER_IGNORE_ALL：只提醒名单内的 42，名单外的 44 不再提醒。
        {
            let mut guard = conf.write().expect("config lock");
            let mut next = (**guard).clone();
            next.cover = config::COVER_IGNORE_ALL;
            next.apply_defaults();
            *guard = Arc::new(next);
        }
        notifier
            .notify_ip_change(42, "203.0.113.9", "198.51.100.2")
            .await;
        assert!(
            notifier.is_muted("default", "ip-change-42"),
            "COVER_IGNORE_ALL 下名单内主机恢复提醒"
        );
        notifier
            .notify_ip_change(44, "203.0.113.9", "198.51.100.2")
            .await;
        assert!(
            !notifier.is_muted("default", "ip-change-44"),
            "COVER_IGNORE_ALL 下名单外主机不再提醒"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn bark_url_encodes_spaces_as_percent20_and_empty_server_slots_keep_json_valid() {
        let now = 1_792_071_600_i64 * 1_000_000_000;
        let bark = expand_placeholders_in_url(
            "https://api.day.app/key/#SANTAIZI#",
            "[主机离线] hk node 01",
            None,
            now,
            "Asia/Shanghai",
            true,
        );
        assert!(
            !bark.contains('+'),
            "URL Path 编码不得把空格转成 '+'：{bark}"
        );
        assert!(bark.contains("hk%20node%2001"));

        // server = None 时未加引号的数值占位符必须回退为 0，保证自定义 JSON 模板合法。
        let rendered = expand_placeholders_in_location(
            r##"{"msg":"#SANTAIZI#","server":"#SERVER.NAME#","ip":"#SERVER.IP#","cpu":#SERVER.CPU#,"mem":#SERVER.MEM#}"##,
            "这是测试消息",
            None,
            now,
            "Asia/Shanghai",
            true,
            json_escape,
        );
        let parsed: Value =
            serde_json::from_str(&rendered).expect("server=None 展开结果必须是合法 JSON");
        assert_eq!(parsed["msg"], "这是测试消息");
        assert_eq!(parsed["server"], "");
        assert_eq!(parsed["cpu"], 0);
        assert_eq!(parsed["mem"], 0);
    }

    #[test]
    fn comma_in_hostname_stays_single_item_and_business_errors_are_detected() {
        let msg = "[主机离线] HK, Zone-A\n最后上报：2026-11-06 10:00:00";
        let (kind, names) = classify(msg).expect("主机离线进入聚合");
        assert_eq!(kind, "主机离线");
        assert_eq!(
            names,
            vec!["HK, Zone-A"],
            "含英文逗号的单个服务器名不得被拆成多台"
        );

        // 企微/钉钉 errcode 与飞书 code 识别，同时放行 Bark/PushPlus 的 code=200。
        assert!(check_webhook_business_error(b"ok").is_ok());
        assert!(check_webhook_business_error(br#"{"errcode":0,"errmsg":"ok"}"#).is_ok());
        assert!(check_webhook_business_error(br#"{"code":0,"msg":"success"}"#).is_ok());
        assert!(
            check_webhook_business_error(br#"{"code":200,"message":"success"}"#).is_ok(),
            "Bark/PushPlus 的 code=200 属于成功响应"
        );
        let ding_err = check_webhook_business_error(
            br#"{"errcode":310000,"errmsg":"keywords not in content"}"#,
        )
        .unwrap_err();
        assert!(ding_err.contains("310000") && ding_err.contains("keywords not in content"));
        let feishu_err =
            check_webhook_business_error(br#"{"code":9499,"msg":"Bad Request"}"#).unwrap_err();
        assert!(feishu_err.contains("9499") && feishu_err.contains("Bad Request"));
    }

    #[tokio::test]
    async fn mute_resets_backoff_after_one_hour_evicts_old_entries_and_shutdown_flushes() {
        let mut mutes = HashMap::new();
        let t0 = Instant::now();
        assert!(evaluate_mute(&mut mutes, "k1".into(), t0));
        assert_eq!(mutes["k1"].duration, FIRST_MUTE);
        // 静音期内抑制。
        assert!(!evaluate_mute(
            &mut mutes,
            "k1".into(),
            t0 + Duration::from_secs(60)
        ));
        // 刚过 15m（未满 1h 冷却）：持续风暴翻倍为 30m。
        let t1 = t0 + FIRST_MUTE + Duration::from_secs(10);
        assert!(evaluate_mute(&mut mutes, "k1".into(), t1));
        assert_eq!(mutes["k1"].duration, FIRST_MUTE * 2);

        // 上次静音过期超过 1h 后再次触发：重置回初始 15m，不无限翻倍到 24h。
        let t2 = t1 + FIRST_MUTE * 2 + MUTE_BACKOFF_RESET_AFTER + Duration::from_secs(5);
        assert!(evaluate_mute(&mut mutes, "stale".into(), t0));
        assert!(evaluate_mute(&mut mutes, "k1".into(), t2));
        assert_eq!(mutes["k1"].duration, FIRST_MUTE);

        // 过期超过 2h 的旧键在下次 send 时顺手清理防泄漏。
        let t3 = t0 + FIRST_MUTE + MUTE_EVICT_AFTER + Duration::from_secs(10);
        let _ = evaluate_mute(&mut mutes, "fresh".into(), t3);
        assert!(
            !mutes.contains_key("stale"),
            "过期超过 2h 的静音项应被自动驱逐"
        );

        // run_flusher 收到停机信号时必须把未满 15s 的聚合桶冲刷完毕再退出。
        let dir = std::env::temp_dir().join(format!(
            "santaizi-notify-shutdown-{}-{}",
            std::process::id(),
            util_time::now_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db = crate::db::migrate::open_primary(dir.join("panel.db").to_str().unwrap()).unwrap();
        let conf = crate::config::new_config_handle(crate::config::Config::default());
        let notifier = Arc::new(Notifier::new(db, conf));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let flusher = tokio::spawn(Arc::clone(&notifier).run_flusher(shutdown_rx));
        notifier
            .send("default", "[主机离线] node-shutdown", None, None)
            .await;
        assert_eq!(notifier.pending_buckets(), 1);
        shutdown_tx.send(true).unwrap();
        flusher.await.unwrap();
        assert_eq!(notifier.pending_buckets(), 0, "停机退出前应冲空聚合队列");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
