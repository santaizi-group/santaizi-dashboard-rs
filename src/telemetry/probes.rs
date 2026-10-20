//! 探针配置映射：对齐 Go `service/telemetry/probes.go` 的
//! `ProbeConfigFromCollector` / `ApplyProbeDefaults` / `ParsePorts` 与
//! `pkg/netprobe.ParsePorts`。
//!
//! collector 行 → `proto.ProbeConfig` 的纯映射。目标解析与样本摄入见 `probe_ingest`。

use crate::proto;
use crate::repo::collectors::CollectorRow;

pub const COLLECTOR_KIND_OBSERVER: &str = "observer";
pub const COLLECTOR_KIND_PROBE: &str = "probe";

const DEFAULT_PROBE_INTERVAL_SEC: i64 = 30;
const DEFAULT_MTR_INTERVAL_SEC: i64 = 300;
pub const DEFAULT_PROBE_TCP_PORTS: &str = "22,443";
const DEFAULT_PROBE_FAIL_THRESHOLD: i64 = 3;
const DEFAULT_ROUTE_INTERVAL_SEC: i64 = 86_400;
const ROUTE_INTERVAL_HOUR: i64 = 3_600;
const ROUTE_INTERVAL_DAY: i64 = 86_400;
const ROUTE_INTERVAL_WEEK: i64 = 604_800;
const DEFAULT_ROUTE_KEEP: i64 = 10;
const MIN_ROUTE_KEEP: i64 = 1;
const MAX_ROUTE_KEEP: i64 = 50;
const DEFAULT_MTR_PROBES: i64 = 10;
const MIN_MTR_PROBES: i64 = 1;
const MAX_MTR_PROBES: i64 = 10;

/// `NormalizeCollectorKind`：未知 kind 归一为空串，由调用方按 observer 兜底。
pub fn normalize_collector_kind(kind: &str) -> String {
    match kind.trim().to_ascii_lowercase().as_str() {
        "" | COLLECTOR_KIND_OBSERVER => COLLECTOR_KIND_OBSERVER.to_string(),
        COLLECTOR_KIND_PROBE => COLLECTOR_KIND_PROBE.to_string(),
        _ => String::new(),
    }
}

pub fn is_probe_kind(kind: &str) -> bool {
    normalize_collector_kind(kind) == COLLECTOR_KIND_PROBE
}

/// `netprobe.ParsePorts`：逗号分隔端口串 → 去重有序端口列表；非法与 0 丢弃。
pub fn parse_ports(raw: &str) -> Vec<u32> {
    let mut seen = std::collections::HashSet::new();
    let mut ports = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let Ok(value) = part.parse::<u32>() else {
            continue;
        };
        if value == 0 || value > u16::MAX as u32 {
            continue;
        }
        if seen.insert(value) {
            ports.push(value);
        }
    }
    ports
}

/// `NormalizeRouteIntervalSec`：只认小时/天/周三档，其余回落默认。
pub(crate) fn normalize_route_interval_sec(value: i64) -> i64 {
    match value {
        ROUTE_INTERVAL_HOUR | ROUTE_INTERVAL_DAY | ROUTE_INTERVAL_WEEK => value,
        _ => DEFAULT_ROUTE_INTERVAL_SEC,
    }
}

pub(crate) fn normalize_route_keep(value: i64) -> i64 {
    match value {
        0 => DEFAULT_ROUTE_KEEP,
        v if v < MIN_ROUTE_KEEP => MIN_ROUTE_KEEP,
        v if v > MAX_ROUTE_KEEP => MAX_ROUTE_KEEP,
        v => v,
    }
}

pub(crate) fn normalize_mtr_probes(value: i64) -> i64 {
    match value {
        0 => DEFAULT_MTR_PROBES,
        v if v < MIN_MTR_PROBES => MIN_MTR_PROBES,
        v if v > MAX_MTR_PROBES => MAX_MTR_PROBES,
        v => v,
    }
}

/// `ApplyProbeDefaults`：在行的副本上补默认值，不落库。
pub fn apply_probe_defaults(row: &mut CollectorRow) {
    let kind = normalize_collector_kind(&row.kind);
    row.kind = if kind.is_empty() {
        COLLECTOR_KIND_OBSERVER.to_string()
    } else {
        kind
    };
    if row.probe_interval_sec == 0 {
        row.probe_interval_sec = DEFAULT_PROBE_INTERVAL_SEC;
    }
    if row.mtr_interval_sec == 0 {
        row.mtr_interval_sec = DEFAULT_MTR_INTERVAL_SEC;
    }
    row.mtr_probes = normalize_mtr_probes(row.mtr_probes);
    if row.tcp_ports.trim().is_empty() {
        row.tcp_ports = DEFAULT_PROBE_TCP_PORTS.to_string();
    }
    if row.fail_threshold == 0 {
        row.fail_threshold = DEFAULT_PROBE_FAIL_THRESHOLD;
    }
    if row
        .notification_tag
        .as_deref()
        .unwrap_or("")
        .trim()
        .is_empty()
    {
        row.notification_tag = Some("default".to_string());
    }
    if !row.enable_ipv4 && !row.enable_ipv6 {
        row.enable_ipv4 = true;
        row.enable_ipv6 = true;
    }
    row.route_interval_sec = normalize_route_interval_sec(row.route_interval_sec);
    row.route_keep = normalize_route_keep(row.route_keep);
}

/// `ProtoCollectorKind`。
pub fn proto_collector_kind(kind: &str) -> i32 {
    if is_probe_kind(kind) {
        proto::CollectorKind::Probe as i32
    } else {
        proto::CollectorKind::Observer as i32
    }
}

/// `IPFamiliesProto`：双开时返回空列表（= 不限制），与 Go 的 nil 语义一致。
pub fn ip_families_proto(enable_ipv4: bool, enable_ipv6: bool) -> Vec<i32> {
    if enable_ipv4 && enable_ipv6 {
        return Vec::new();
    }
    let mut families = Vec::new();
    if enable_ipv4 {
        families.push(proto::ProbeIpFamily::Ipv4 as i32);
    }
    if enable_ipv6 {
        families.push(proto::ProbeIpFamily::Ipv6 as i32);
    }
    families
}

/// `ProbeConfigFromCollector`：collector 行 → proto 探针配置（含默认值）。
pub fn probe_config_from_collector(row: &CollectorRow) -> proto::ProbeConfig {
    let mut collector = row.clone();
    apply_probe_defaults(&mut collector);
    proto::ProbeConfig {
        kind: proto_collector_kind(&collector.kind),
        interval_seconds: to_u32(collector.probe_interval_sec),
        mtr_interval_seconds: to_u32(collector.mtr_interval_sec),
        tcp_ports: parse_ports(&collector.tcp_ports),
        enable_icmp: collector.enable_icmp,
        enable_tcp: collector.enable_tcp,
        enable_mtr: collector.enable_mtr,
        notify: collector.probe_notify,
        notification_tag: collector.notification_tag.unwrap_or_default(),
        latency_notify: collector.latency_notify,
        min_latency_ms: collector.min_latency_ms,
        max_latency_ms: collector.max_latency_ms,
        fail_threshold: to_u32(collector.fail_threshold),
        ip_families: ip_families_proto(collector.enable_ipv4, collector.enable_ipv6),
        route_interval_seconds: to_u32(collector.route_interval_sec),
        route_keep: to_u32(collector.route_keep),
        mtr_probes: to_u32(collector.mtr_probes),
    }
}

/// DB 整数列 → proto uint32：负值与超大值夹到合法区间，避免静默回绕。
fn to_u32(value: i64) -> u32 {
    value.clamp(0, u32::MAX as i64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> CollectorRow {
        CollectorRow {
            collector_uuid: "c1".into(),
            name: "n".into(),
            address: "addr".into(),
            token_hash: vec![0u8; 32],
            generation: 1,
            config_version: 1,
            tls: false,
            insecure_tls: false,
            kind: "observer".into(),
            listen_port: 0,
            location: None,
            probe_interval_sec: 0,
            mtr_interval_sec: 0,
            mtr_probes: 0,
            tcp_ports: String::new(),
            enable_icmp: true,
            enable_tcp: true,
            enable_mtr: false,
            enable_ipv4: true,
            enable_ipv6: true,
            probe_notify: false,
            notification_tag: None,
            latency_notify: false,
            min_latency_ms: 0.0,
            max_latency_ms: 0.0,
            fail_threshold: 0,
            route_interval_sec: 0,
            route_keep: 0,
            revoked: false,
        }
    }

    #[test]
    fn normalize_kind_and_is_probe() {
        assert_eq!(normalize_collector_kind(""), COLLECTOR_KIND_OBSERVER);
        assert_eq!(normalize_collector_kind(" Probe "), COLLECTOR_KIND_PROBE);
        assert_eq!(normalize_collector_kind("weird"), "");
        assert!(is_probe_kind("probe"));
        assert!(!is_probe_kind("observer"));
        assert!(!is_probe_kind("weird"));
    }

    #[test]
    fn parse_ports_dedups_and_filters() {
        assert_eq!(
            parse_ports(" 22, 443 ,22, 0, 65536, abc, 80 "),
            vec![22, 443, 80]
        );
        assert_eq!(parse_ports("65535"), vec![65535]);
        assert!(parse_ports("").is_empty());
        assert!(parse_ports(",,").is_empty());
    }

    #[test]
    fn defaults_applied_on_copy() {
        let mut collector = row();
        collector.kind = "probe".into();
        collector.mtr_probes = 99;
        collector.route_keep = 99;
        collector.route_interval_sec = 42;
        collector.enable_ipv4 = false;
        collector.enable_ipv6 = false;
        apply_probe_defaults(&mut collector);
        assert_eq!(collector.probe_interval_sec, 30);
        assert_eq!(collector.mtr_interval_sec, 300);
        assert_eq!(collector.mtr_probes, MAX_MTR_PROBES);
        assert_eq!(collector.route_keep, MAX_ROUTE_KEEP);
        assert_eq!(collector.route_interval_sec, DEFAULT_ROUTE_INTERVAL_SEC);
        assert_eq!(collector.tcp_ports, DEFAULT_PROBE_TCP_PORTS);
        assert_eq!(collector.fail_threshold, 3);
        assert_eq!(collector.notification_tag.as_deref(), Some("default"));
        assert!(collector.enable_ipv4 && collector.enable_ipv6);
    }

    #[test]
    fn probe_config_maps_fields() {
        let mut collector = row();
        collector.kind = "probe".into();
        collector.tcp_ports = "80,443".into();
        collector.enable_mtr = true;
        collector.probe_notify = true;
        collector.notification_tag = Some("ops".into());
        collector.latency_notify = true;
        collector.min_latency_ms = 10.0;
        collector.max_latency_ms = 500.0;
        collector.fail_threshold = 5;
        collector.enable_ipv4 = true;
        collector.enable_ipv6 = false;
        let config = probe_config_from_collector(&collector);
        assert_eq!(config.kind, proto::CollectorKind::Probe as i32);
        assert_eq!(config.interval_seconds, 30);
        assert_eq!(config.mtr_interval_seconds, 300);
        assert_eq!(config.tcp_ports, vec![80, 443]);
        assert!(config.enable_mtr && config.notify && config.latency_notify);
        assert_eq!(config.notification_tag, "ops");
        assert_eq!(config.min_latency_ms, 10.0);
        assert_eq!(config.max_latency_ms, 500.0);
        assert_eq!(config.fail_threshold, 5);
        assert_eq!(config.ip_families, vec![proto::ProbeIpFamily::Ipv4 as i32]);
        assert_eq!(config.route_interval_seconds, 86_400);
        assert_eq!(config.route_keep, 10);
        assert_eq!(config.mtr_probes, 10);
    }

    #[test]
    fn observer_defaults_to_unrestricted_families() {
        let config = probe_config_from_collector(&row());
        assert_eq!(config.kind, proto::CollectorKind::Observer as i32);
        assert!(config.ip_families.is_empty());
    }
}
