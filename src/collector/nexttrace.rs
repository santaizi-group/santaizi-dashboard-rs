//! nexttrace 独立进程：对齐 Go `pkg/nexttrace` 与 ADR-0017。
//!
//! 不链接 NTrace-core。显式路径必须是可执行的绝对路径；否则在 PATH 里找
//! `nexttrace` / `nexttrace-tiny`。找不到时返回「未找到 nexttrace」，调用方把
//! 该错误写进样本，不把空跳数当成成功。

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

pub const NOT_FOUND: &str = "未找到 nexttrace";
const EXEC_TIMEOUT: Duration = Duration::from_secs(45);

/// stdout 上限：nexttrace `--json` 正常输出在几十 KB 量级；单跳 30 跳、每跳
/// 带 geo 字段也远低于此。超限说明目标不可信（或被中间设备灌了垃圾），
/// 直接判失败，不去解析被截断的 JSON。
pub const STDOUT_LIMIT: usize = 4 * 1024 * 1024;

/// stderr 上限：只用于拼错误信息，超限部分在读的过程中丢弃。
pub const STDERR_LIMIT: usize = 64 * 1024;

const CANCELLED: &str = "探测已取消";

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
    /// 显式追加 `-d ipinfo`。默认不追加，沿用 nexttrace 自身的数据源；
    /// nexttrace-tiny 等不支持该 provider 的版本会报错，由运营自行权衡。
    pub ipinfo: bool,
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
    if opts.ipinfo {
        // 数据源是 nexttrace 的显式选项：不写时用它自己的默认值。
        args.push("-d".to_string());
        args.push("ipinfo".to_string());
    }
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
    run_inner(bin, opts, &CancellationToken::new(), EXEC_TIMEOUT).await
}

/// 可取消的 `run`：取消令牌与 45s 超时谁先到都算结束，子进程都会被杀掉并回收，
/// 不会留下无人 wait 的僵尸进程；读管道任务同时 abort，不等孙进程放管道。
pub async fn run_cancellable(
    bin: &Path,
    opts: &TraceOpts,
    cancel: &CancellationToken,
) -> Result<TraceResult, String> {
    run_inner(bin, opts, cancel, EXEC_TIMEOUT).await
}

async fn run_inner(
    bin: &Path,
    opts: &TraceOpts,
    cancel: &CancellationToken,
    timeout: Duration,
) -> Result<TraceResult, String> {
    validate_target(&opts.target)?;
    let mut command = tokio::process::Command::new(bin);
    command
        .args(args(opts)?)
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => return Err(compact_error(&err.to_string())),
    };
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    // 两路输出必须并发读：只读一路会让另一路管道写满，把子进程堵死在 write 上，
    // 45s 超时就成了唯一的出口。
    let stdout_task = tokio::spawn(read_capped(stdout, STDOUT_LIMIT));
    let stderr_task = tokio::spawn(read_capped(stderr, STDERR_LIMIT));
    let mut status = None;
    let mut failure = None;
    tokio::select! {
        biased;
        _ = cancel.cancelled() => failure = Some(CANCELLED.to_string()),
        waited = tokio::time::timeout(timeout, child.wait()) => match waited {
            Ok(Ok(exit)) => status = Some(exit),
            Ok(Err(err)) => failure = Some(compact_error(&err.to_string())),
            Err(_) => failure = Some("nexttrace timed out".to_string()),
        },
    }
    if let Some(failure) = failure {
        // 超时/取消：显式杀掉并 wait 回收，不依赖 drop 的时机。结果已经作废，
        // 读管道任务直接 abort——孙进程可能仍持有 stdout，等 EOF 会把 45s
        // 超时拖成无限期，单执行器下等于卡住所有追踪。
        let _ = child.kill().await;
        let _ = child.wait().await;
        stdout_task.abort();
        stderr_task.abort();
        return Err(failure);
    }
    let Some(status) = status else {
        return Err("nexttrace 未返回退出状态".to_string());
    };
    let Some((stdout, stdout_overflow)) = join_reader(stdout_task).await else {
        return Err("nexttrace 输出读取超时".to_string());
    };
    let (stderr, _stderr_overflow) = join_reader(stderr_task).await.unwrap_or_default();
    if stdout_overflow {
        return Err(format!("nexttrace 输出超过 {STDOUT_LIMIT} 字节上限"));
    }
    if !status.success() {
        let mut msg = String::from_utf8_lossy(&stderr).trim().to_string();
        if msg.is_empty() {
            msg = String::from_utf8_lossy(&stdout).trim().to_string();
        }
        if msg.is_empty() {
            msg = format!("nexttrace exit {status}");
        }
        return Err(compact_error(&msg));
    }
    let mut parsed = parse_json(&stdout)?;
    if parsed.destination.is_empty() {
        parsed.destination = opts.target.clone();
    }
    Ok(parsed)
}

/// 读到上限后继续排空（丢弃超额内容），不让子进程堵在写满的管道上。
async fn read_capped<R: tokio::io::AsyncRead + Unpin>(
    reader: Option<R>,
    limit: usize,
) -> (Vec<u8>, bool) {
    let Some(mut reader) = reader else {
        return (Vec::new(), false);
    };
    let mut buf = Vec::with_capacity(limit.min(64 * 1024));
    let mut chunk = [0u8; 16 * 1024];
    let mut overflow = false;
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) => break,
            Ok(read) => {
                if buf.len() >= limit {
                    overflow = true;
                } else {
                    // 一次 read 就可能跨过上限：截下来的部分算溢出，
                    // 不能只按「已满」判断，否则超限一个字节也发现不了。
                    let room = limit - buf.len();
                    let keep = read.min(room);
                    buf.extend_from_slice(&chunk[..keep]);
                    if read > keep {
                        overflow = true;
                    }
                }
            }
            Err(_) => break,
        }
    }
    (buf, overflow)
}

/// 子进程已退出，正常路径等 EOF；万一有孙进程攥着管道，5s 后 abort，
/// 不能让一次追踪把整个执行器挂住。
const READER_JOIN_LIMIT: Duration = Duration::from_secs(5);

async fn join_reader(task: tokio::task::JoinHandle<(Vec<u8>, bool)>) -> Option<(Vec<u8>, bool)> {
    let abort = task.abort_handle();
    match tokio::time::timeout(READER_JOIN_LIMIT, task).await {
        Ok(Ok(value)) => Some(value),
        Ok(Err(_)) => None,
        Err(_) => {
            abort.abort();
            None
        }
    }
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
            ipinfo: false,
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

    /// 数据源开关只影响 argv：默认不带 `-d`，打开后成对追加且目标仍在最后。
    #[test]
    fn ipinfo_is_opt_in_and_keeps_target_last() {
        let mut base = opts("1.1.1.1");
        let plain = args(&base).unwrap();
        assert!(!plain.iter().any(|arg| arg == "-d" || arg == "ipinfo"));

        base.ipinfo = true;
        let built = args(&base).unwrap();
        let dash = built.iter().position(|arg| arg == "-d").expect("-d");
        assert_eq!(built.get(dash + 1).map(String::as_str), Some("ipinfo"));
        assert_eq!(built.last().map(String::as_str), Some("1.1.1.1"));
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

/// 子进程输出路径：上限、超时、取消。OOM 复盘里 nexttrace 的输出收集是
/// 父进程内存的第二来源（`Command::output` 无界收集两路输出）。
#[cfg(test)]
mod exec_tests {
    use super::*;

    const JSON: &str =
        r#"{"Hops":[[{"Success":true,"Address":"203.0.113.1","TTL":1,"RTT":12.5}]]}"#;

    fn opts(target: &str) -> TraceOpts {
        TraceOpts {
            target: target.to_string(),
            tcp: false,
            port: 0,
            ipv4: true,
            ipv6: false,
            ipinfo: false,
        }
    }

    fn fake_nexttrace(dir: &Path, body: &str) -> PathBuf {
        let path = dir.join("nexttrace");
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("write fake nexttrace");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod fake nexttrace");
        }
        path
    }

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("santaizi-nt-exec-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }

    /// 正常输出：能解析出跳点。
    #[tokio::test]
    async fn normal_output_is_parsed() {
        let dir = scratch("normal");
        let bin = fake_nexttrace(&dir, &format!("printf '%s' '{JSON}'"));
        let parsed = run(&bin, &opts("203.0.113.1")).await.expect("run");
        assert_eq!(parsed.hops.len(), 1);
        assert_eq!(parsed.hops[0].address, "203.0.113.1");
    }

    /// stdout 恰好等于上限：合法 JSON 加空白填充，必须放行（上限是含边界的）。
    #[tokio::test]
    async fn stdout_exactly_at_limit_is_accepted() {
        let dir = scratch("at-limit");
        let pad = STDOUT_LIMIT - JSON.len();
        let bin = fake_nexttrace(
            &dir,
            &format!("printf '%s' '{JSON}'\nyes '' | head -c {pad}"),
        );
        let parsed = run(&bin, &opts("203.0.113.1")).await.expect("run");
        assert_eq!(parsed.hops.len(), 1);
    }

    /// 超出一个字节：判失败，不解析被截断的 JSON。
    #[tokio::test]
    async fn stdout_over_limit_is_rejected() {
        let dir = scratch("over-limit");
        let pad = STDOUT_LIMIT - JSON.len() + 1;
        let bin = fake_nexttrace(
            &dir,
            &format!("printf '%s' '{JSON}'\nyes '' | head -c {pad}"),
        );
        let err = run(&bin, &opts("203.0.113.1")).await.unwrap_err();
        assert!(err.contains("超过"), "实际：{err}");
    }

    /// stderr 洪水不得影响成功路径：错误流有独立上限，且成功时只用于诊断。
    #[tokio::test]
    async fn stderr_flood_does_not_break_success() {
        let dir = scratch("stderr-flood");
        let bin = fake_nexttrace(
            &dir,
            &format!("head -c 1048576 /dev/zero >&2\nprintf '%s' '{JSON}'"),
        );
        let parsed = run(&bin, &opts("203.0.113.1")).await.expect("run");
        assert_eq!(parsed.hops.len(), 1);
    }

    /// 非零退出：错误来自 stderr，且被截到有限长度。
    #[tokio::test]
    async fn nonzero_exit_reports_stderr() {
        let dir = scratch("nonzero");
        let bin = fake_nexttrace(
            &dir,
            "{ printf 'boom '; yes xxxxxxxx | head -c 4096; } >&2\nexit 1",
        );
        let err = run(&bin, &opts("203.0.113.1")).await.unwrap_err();
        assert!(err.starts_with("boom"), "实际：{err}");
        assert!(err.len() < 1000, "错误必须被截断：{}", err.len());
    }

    /// 超时：杀掉并回收，报明确错误；之后同一条路径仍能正常跑。
    /// 子进程 `sleep` 被 shell 留在后台、仍攥着 stdout 时也必须立刻返回。
    #[tokio::test]
    async fn timeout_reaps_the_child_and_keeps_the_path_usable() {
        let dir = scratch("timeout");
        let hang = fake_nexttrace(&dir, "sleep 5");
        let started = std::time::Instant::now();
        let err = run_inner(
            &hang,
            &opts("203.0.113.1"),
            &CancellationToken::new(),
            Duration::from_millis(200),
        )
        .await
        .unwrap_err();
        assert!(err.contains("timed out"), "实际：{err}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "200ms 超时不得等孙进程放管道，实际 {:?}",
            started.elapsed()
        );
        let good = fake_nexttrace(&dir, &format!("printf '%s' '{JSON}'"));
        let parsed = run(&good, &opts("203.0.113.1")).await.expect("run");
        assert_eq!(parsed.hops.len(), 1);
    }

    /// 取消：立刻返回，不让子进程活过从端生命周期。
    #[tokio::test]
    async fn cancel_returns_promptly() {
        let dir = scratch("cancel");
        let hang = fake_nexttrace(&dir, "sleep 5");
        let cancel = CancellationToken::new();
        let bin = hang.clone();
        let target = opts("203.0.113.1");
        let token = cancel.clone();
        let handle = tokio::spawn(async move { run_cancellable(&bin, &target, &token).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = std::time::Instant::now();
        cancel.cancel();
        let err = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("取消后必须很快返回")
            .unwrap()
            .unwrap_err();
        assert!(err.contains(CANCELLED), "实际：{err}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "取消不得等孙进程放管道，实际 {:?}",
            started.elapsed()
        );
    }
}
