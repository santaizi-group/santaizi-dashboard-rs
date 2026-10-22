//! nexttrace 独立进程：对齐 Go `pkg/nexttrace` 与 ADR-0017。
//!
//! 不链接 NTrace-core。显式路径必须是可执行的绝对路径；否则在 PATH 里找
//! `nexttrace` / `nexttrace-tiny`。找不到时返回「未找到 nexttrace」，调用方把
//! 该错误写进样本，不把空跳数当成成功。

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;

pub const NOT_FOUND: &str = "未找到 nexttrace";
const EXEC_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Debug, Clone, PartialEq)]
pub struct Hop {
    pub ttl: u32,
    pub address: String,
    pub hostname: String,
    pub rtt_ms: f64,
    pub loss: f64,
    pub sent: u32,
    pub asn: String,
    pub country: String,
    pub province: String,
    pub city: String,
    pub owner: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TraceResult {
    pub destination: String,
    pub hops: Vec<Hop>,
}

#[derive(Debug, Clone)]
pub struct TraceOpts {
    pub target: String,
    pub tcp: bool,
    pub port: u32,
    pub ipv4: bool,
    pub ipv6: bool,
}

pub fn resolve_binary(configured: &str) -> Result<PathBuf, String> {
    let configured = configured.trim();
    if !configured.is_empty() {
        let path = Path::new(configured);
        if !path.is_absolute() {
            return Err(format!("{NOT_FOUND}: 必须是绝对路径"));
        }
        if !is_executable(path) {
            return Err(NOT_FOUND.to_string());
        }
        return Ok(path.to_path_buf());
    }
    let path_env = std::env::var_os("PATH").unwrap_or_default();
    for dir in std::env::split_paths(&path_env) {
        for name in ["nexttrace", "nexttrace-tiny"] {
            let candidate = dir.join(name);
            if is_executable(&candidate) {
                return Ok(candidate);
            }
        }
    }
    Err(NOT_FOUND.to_string())
}

/// 目标名长度上限：与探测目标入库上限一致，避免把超长串塞进 argv。
const MAX_TARGET_LEN: usize = 253;

/// 目标名不得被 nexttrace 当成选项。
///
/// 目标来自探测任务（可由管理员经探测目标配置下发），若允许以 `-` 开头，
/// `nexttrace --json ... -x` 就会把 `-x` 当 flag 解释——轻则探测失败，
/// 重则借 nexttrace 自己的选项改变行为。这里在 spawn 前拦掉，而不是指望
/// nexttrace 支持 `--`（各版本行为不一）。
fn validate_target(target: &str) -> Result<&str, String> {
    let trimmed = target.trim();
    if trimmed.is_empty() {
        return Err("empty target".to_string());
    }
    if trimmed.len() > MAX_TARGET_LEN {
        return Err(format!("target too long: {} bytes", trimmed.len()));
    }
    if trimmed.starts_with('-') {
        return Err(format!("invalid target: {trimmed}"));
    }
    Ok(trimmed)
}

pub fn args(opts: &TraceOpts) -> Result<Vec<String>, String> {
    let mut args = vec![
        "--json".to_string(),
        "-M".to_string(),
        "--queries".to_string(),
        "3".to_string(),
        "--max-hops".to_string(),
        "30".to_string(),
        "--language".to_string(),
        "cn".to_string(),
    ];
    if opts.tcp {
        args.push("--tcp".to_string());
        if opts.port > 0 {
            args.push("--port".to_string());
            args.push(opts.port.to_string());
        }
    }
    if opts.ipv6 && !opts.ipv4 {
        args.push("--ipv6".to_string());
    } else if opts.ipv4 && !opts.ipv6 {
        args.push("--ipv4".to_string());
    }
    args.push(validate_target(&opts.target)?.to_string());
    Ok(args)
}

pub async fn run(bin: &Path, opts: &TraceOpts) -> Result<TraceResult, String> {
    validate_target(&opts.target)?;
    let mut command = tokio::process::Command::new(bin);
    command.args(args(opts)?).kill_on_drop(true);
    let output = match tokio::time::timeout(EXEC_TIMEOUT, command.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(err)) => return Err(compact_error(&err.to_string())),
        Err(_) => return Err("nexttrace timed out".to_string()),
    };
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut msg = stderr.trim().to_string();
        if msg.is_empty() {
            msg = stdout.trim().to_string();
        }
        if msg.is_empty() {
            msg = format!("nexttrace exit {}", output.status);
        }
        return Err(compact_error(&msg));
    }
    let mut parsed = parse_json(&output.stdout)?;
    if parsed.destination.is_empty() {
        parsed.destination = opts.target.clone();
    }
    Ok(parsed)
}

pub fn parse_json(raw: &[u8]) -> Result<TraceResult, String> {
    let raw = trim_ascii(raw);
    if raw.is_empty() {
        return Err("empty nexttrace json".to_string());
    }
    let envelope: Value =
        serde_json::from_slice(raw).map_err(|err| format!("nexttrace json: {err}"))?;
    let hops_raw = envelope.get("Hops").or_else(|| envelope.get("hops"));
    let Some(hops_raw) = hops_raw else {
        return Err("nexttrace json missing hops".to_string());
    };
    if let Some(result) = parse_attempt_hops(hops_raw)
        && !result.hops.is_empty()
    {
        let mut result = result;
        if result.destination.is_empty() {
            result.destination = json_string(envelope.get("resolved_ip"))
                .or_else(|| json_string(envelope.get("target")))
                .unwrap_or_default();
        }
        return Ok(result);
    }
    parse_cli_hops(hops_raw)
}

fn parse_cli_hops(hops_raw: &Value) -> Result<TraceResult, String> {
    let groups = hops_raw
        .as_array()
        .ok_or_else(|| "nexttrace hops: expected array".to_string())?;
    let mut result = TraceResult {
        destination: String::new(),
        hops: Vec::new(),
    };
    for (index, group) in groups.iter().enumerate() {
        let group = group
            .as_array()
            .cloned()
            .unwrap_or_else(|| vec![group.clone()]);
        let hop = merge_cli_group((index + 1) as u32, &group);
        if hop.ttl == 0 {
            continue;
        }
        result.hops.push(hop);
    }
    Ok(result)
}

fn merge_cli_group(ttl: u32, group: &[Value]) -> Hop {
    let mut hop = Hop {
        ttl,
        address: String::new(),
        hostname: String::new(),
        rtt_ms: 0.0,
        loss: 0.0,
        sent: 0,
        asn: String::new(),
        country: String::new(),
        province: String::new(),
        city: String::new(),
        owner: String::new(),
    };
    let mut rtts = Vec::new();
    let mut sent = 0u32;
    let mut ok = 0u32;
    for item in group {
        sent += 1;
        let success = item
            .get("Success")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let address = hop_address(item);
        if success || !address.is_empty() {
            ok += 1;
            if hop.address.is_empty() {
                hop.address = address;
            }
            if hop.hostname.is_empty() {
                hop.hostname = item
                    .get("Hostname")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
            }
            apply_geo(&mut hop, item.get("Geo"));
            let rtt = rtt_milliseconds(item.get("RTT"));
            if rtt > 0.0 {
                rtts.push(rtt);
            }
        }
        if let Some(item_ttl) = item.get("TTL").and_then(Value::as_u64)
            && item_ttl > 0
        {
            hop.ttl = item_ttl as u32;
        }
    }
    hop.sent = sent;
    if sent > 0 {
        hop.loss = f64::from(sent - ok) / f64::from(sent);
    }
    if !rtts.is_empty() {
        hop.rtt_ms = rtts.iter().sum::<f64>() / rtts.len() as f64;
    }
    hop
}

fn parse_attempt_hops(hops_raw: &Value) -> Option<TraceResult> {
    let hops = hops_raw.as_array()?;
    if hops.is_empty()
        || !hops
            .iter()
            .any(|hop| hop.get("attempts").is_some() || hop.get("address").is_some())
    {
        return None;
    }
    let mut result = TraceResult {
        destination: String::new(),
        hops: Vec::new(),
    };
    for (index, item) in hops.iter().enumerate() {
        if !item.is_object() {
            return None;
        }
        let ttl = item
            .get("ttl")
            .and_then(Value::as_u64)
            .filter(|ttl| *ttl > 0)
            .map(|ttl| ttl as u32)
            .unwrap_or((index + 1) as u32);
        let mut hop = Hop {
            ttl,
            address: first_non_empty(
                item.get("address").and_then(Value::as_str),
                item.get("ip").and_then(Value::as_str),
            ),
            hostname: item
                .get("hostname")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            rtt_ms: item.get("rtt_ms").and_then(Value::as_f64).unwrap_or(0.0),
            loss: 0.0,
            sent: 0,
            asn: String::new(),
            country: String::new(),
            province: String::new(),
            city: String::new(),
            owner: String::new(),
        };
        apply_geo(&mut hop, item.get("geo"));
        let attempts = item.get("attempts").and_then(Value::as_array);
        let mut sent = attempts.map(|items| items.len() as u32).unwrap_or(0);
        let mut ok_count = 0u32;
        let mut rtts = Vec::new();
        if let Some(attempts) = attempts {
            for sample in attempts {
                let success = sample
                    .get("success")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let ip = sample.get("ip").and_then(Value::as_str).unwrap_or("");
                if success || !ip.is_empty() {
                    ok_count += 1;
                }
                if hop.address.is_empty() {
                    hop.address = ip.to_string();
                }
                if hop.hostname.is_empty() {
                    hop.hostname = sample
                        .get("hostname")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                }
                apply_geo(&mut hop, sample.get("geo"));
                if let Some(rtt) = sample.get("rtt_ms").and_then(Value::as_f64)
                    && rtt > 0.0
                {
                    rtts.push(rtt);
                }
            }
        }
        if sent == 0 {
            sent = 1;
            if !hop.address.is_empty()
                || item
                    .get("success")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            {
                ok_count = 1;
            }
        }
        hop.sent = sent;
        hop.loss = f64::from(sent - ok_count) / f64::from(sent);
        if !rtts.is_empty() {
            hop.rtt_ms = rtts.iter().sum::<f64>() / rtts.len() as f64;
        }
        result.hops.push(hop);
    }
    Some(result)
}

fn apply_geo(hop: &mut Hop, geo: Option<&Value>) {
    let Some(geo) = geo else {
        return;
    };
    if hop.asn.is_empty() {
        hop.asn = json_scalar(geo.get("asnumber"))
            .or_else(|| json_scalar(geo.get("asn")))
            .unwrap_or_default();
    }
    if hop.country.is_empty() {
        hop.country = geo
            .get("country")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
    }
    if hop.province.is_empty() {
        hop.province = geo
            .get("prov")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
    }
    if hop.city.is_empty() {
        hop.city = geo
            .get("city")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
    }
    if hop.owner.is_empty() {
        hop.owner = first_non_empty(
            geo.get("owner").and_then(Value::as_str),
            geo.get("isp").and_then(Value::as_str),
        );
    }
}

fn hop_address(item: &Value) -> String {
    if let Some(ip) = item.get("IP").and_then(Value::as_str)
        && !ip.trim().is_empty()
    {
        return strip_port(ip);
    }
    if let Some(ip) = item.get("ip").and_then(Value::as_str)
        && !ip.trim().is_empty()
    {
        return strip_port(ip);
    }
    match item.get("Address") {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(value)) => strip_port(value),
        Some(Value::Object(obj)) => {
            for key in ["IP", "ip", "IPAddress"] {
                if let Some(Value::String(value)) = obj.get(key) {
                    return strip_port(value);
                }
            }
            String::new()
        }
        Some(other) => strip_port(&other.to_string()),
    }
}

fn strip_port(value: &str) -> String {
    let value = value.trim().trim_matches('"');
    if value.is_empty() {
        return String::new();
    }
    if let Some(rest) = value.strip_prefix('[')
        && let Some(end) = rest.find(']')
    {
        return rest[..end].to_string();
    }
    if value.matches(':').count() == 1
        && let Some((host, port)) = value.rsplit_once(':')
        && port.chars().all(|ch| ch.is_ascii_digit())
    {
        return host.to_string();
    }
    value.to_string()
}

fn rtt_milliseconds(raw: Option<&Value>) -> f64 {
    match raw {
        None | Some(Value::Null) => 0.0,
        Some(Value::Number(number)) => {
            let value = number.as_f64().unwrap_or(0.0);
            if value > 1_000_000.0 {
                value / 1_000_000.0
            } else {
                value
            }
        }
        Some(Value::String(value)) => value.trim_end_matches("ms").trim().parse().unwrap_or(0.0),
        Some(_) => 0.0,
    }
}

fn json_string(raw: Option<&Value>) -> Option<String> {
    raw.and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn json_scalar(raw: Option<&Value>) -> Option<String> {
    match raw {
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.trim().to_string()),
        Some(Value::Number(number)) => Some(number.to_string()),
        _ => None,
    }
}

fn first_non_empty(left: Option<&str>, right: Option<&str>) -> String {
    left.map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| right.map(str::trim).filter(|value| !value.is_empty()))
        .unwrap_or("")
        .to_string()
}

fn is_executable(path: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn compact_error(msg: &str) -> String {
    let msg = msg.split_whitespace().collect::<Vec<_>>().join(" ");
    if msg.is_empty() {
        return "nexttrace failed".to_string();
    }
    msg.chars().take(480).collect()
}

fn trim_ascii(raw: &[u8]) -> &[u8] {
    let start = raw
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(raw.len());
    let end = raw
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map(|index| index + 1)
        .unwrap_or(start);
    &raw[start..end]
}

#[cfg(test)]
mod target_guard_tests {
    use super::*;

    fn opts(target: &str) -> TraceOpts {
        TraceOpts {
            target: target.to_string(),
            tcp: false,
            port: 0,
            ipv4: true,
            ipv6: false,
        }
    }

    /// 目标名以 `-` 开头会被 nexttrace 当成选项，必须在 spawn 前拦掉。
    #[test]
    fn dash_prefixed_target_is_rejected() {
        for target in ["-x", "--help", "--json", "-"] {
            let err = args(&opts(target)).unwrap_err();
            assert!(
                err.contains("invalid target"),
                "target={target} 应被拒，实际：{err}"
            );
        }
    }

    /// 正常目标照旧放行，且仍是 argv 最后一项。
    #[test]
    fn ordinary_target_is_passed_through() {
        let built = args(&opts(" 1.1.1.1 ")).unwrap();
        assert_eq!(built.last().map(String::as_str), Some("1.1.1.1"));
        assert!(built.contains(&"--json".to_string()));
    }

    #[test]
    fn empty_and_oversized_targets_are_rejected() {
        assert!(args(&opts("   ")).unwrap_err().contains("empty target"));
        let long = "a".repeat(MAX_TARGET_LEN + 1);
        assert!(args(&opts(&long)).unwrap_err().contains("too long"));
    }

    /// `run` 入口同样先校验，不至于把坏目标交给子进程。
    #[tokio::test]
    async fn run_rejects_dash_prefixed_target_without_spawning() {
        let err = run(Path::new("/bin/true"), &opts("-x")).await.unwrap_err();
        assert!(err.contains("invalid target"), "实际：{err}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_binary_is_an_explicit_error() {
        let err = resolve_binary("/tmp/does-not-exist-nexttrace").unwrap_err();
        assert!(err.contains(NOT_FOUND));
        let err = resolve_binary("nexttrace").unwrap_err();
        assert!(err.contains("绝对路径"));
    }

    #[test]
    fn parses_cli_hop_groups() {
        let raw = br#"{
            "hops": [[
                {"Success": true, "Address": "203.0.113.1", "TTL": 1, "RTT": 12.5,
                 "Geo": {"asn": "AS1", "country": "CN", "prov": "SH", "city": "Shanghai", "isp": "Example"}}
            ]]
        }"#;
        let parsed = parse_json(raw).unwrap();
        assert_eq!(parsed.hops.len(), 1);
        assert_eq!(parsed.hops[0].address, "203.0.113.1");
        assert_eq!(parsed.hops[0].country, "CN");
        assert!((parsed.hops[0].rtt_ms - 12.5).abs() < 0.01);
        assert_eq!(parsed.hops[0].loss, 0.0);
    }
}
