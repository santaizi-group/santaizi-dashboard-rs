//! 主端探测写入：对齐 Go `service/telemetry/probes.go` 与 `probes_route.go`。
//!
//! 配置下发时按 scope 解析目标；Sync 摄入 ICMP/TCP 分钟桶、最新值、MTR 与路由，
//! 并在连续失败或时延越界时产出通知。路由任务在运行态帧之后下发。

use std::net::IpAddr;

use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

use crate::proto;
use crate::repo::collectors::CollectorRow;
use crate::telemetry::probes;
use crate::util::time::unix_nanos_to_sqlite;

const MINUTE_NANOS: i64 = 60 * 1_000_000_000;
const ROUTE_JOB_TTL_NANOS: i64 = 10 * 60 * 1_000_000_000;
const JOB_EXPIRE_ERROR: &str = "从端未领取";

/// 一次探测告警要发出的通知。调用方在事务提交后交给 `Notifier`。
#[derive(Debug, Clone, PartialEq)]
pub struct ProbeNotice {
    pub tag: String,
    pub message: String,
    pub mute_label: String,
    pub server_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedProbeTarget {
    pub source: String,
    pub hostname: String,
    pub ipv4: String,
    pub ipv6: String,
}

impl ResolvedProbeTarget {
    fn none() -> Self {
        Self {
            source: "none".to_string(),
            hostname: String::new(),
            ipv4: String::new(),
            ipv6: String::new(),
        }
    }
}

/// `ResolveProbeTarget`：覆盖地址优先，否则用运行态 `last_ip`（`v4/v6`）。
pub fn resolve_probe_target(probe_target: &str, last_ip: &str) -> ResolvedProbeTarget {
    let override_target = probe_target.trim();
    if !override_target.is_empty() {
        if let Ok(ip) = override_target.parse::<IpAddr>() {
            return match ip {
                IpAddr::V4(ip) => ResolvedProbeTarget {
                    source: "override".to_string(),
                    hostname: String::new(),
                    ipv4: ip.to_string(),
                    ipv6: String::new(),
                },
                IpAddr::V6(ip) => ResolvedProbeTarget {
                    source: "override".to_string(),
                    hostname: String::new(),
                    ipv4: String::new(),
                    ipv6: ip.to_string(),
                },
            };
        }
        return ResolvedProbeTarget {
            source: "override".to_string(),
            hostname: override_target.to_string(),
            ipv4: String::new(),
            ipv6: String::new(),
        };
    }
    let (ipv4, ipv6) = split_ip_addr(last_ip);
    if ipv4.is_empty() && ipv6.is_empty() {
        return ResolvedProbeTarget::none();
    }
    ResolvedProbeTarget {
        source: "host_ip".to_string(),
        hostname: String::new(),
        ipv4,
        ipv6,
    }
}

/// `ApplyProbeIPFamilies`：关掉的地址族清空；什么都不剩则来源改为 none。
pub fn apply_ip_families(
    mut target: ResolvedProbeTarget,
    v4: bool,
    v6: bool,
) -> ResolvedProbeTarget {
    if !v4 {
        target.ipv4.clear();
    }
    if !v6 {
        target.ipv6.clear();
    }
    if target.ipv4.is_empty() && target.ipv6.is_empty() && target.hostname.is_empty() {
        target.source = "none".to_string();
    }
    target
}

/// `BuildProbeTargets`：只给探测型从端，且只含 scope 内、仍有有效探测的服务器。
pub fn build_probe_targets(
    conn: &Connection,
    collector: &CollectorRow,
) -> anyhow::Result<Vec<proto::ProbeTarget>> {
    if !collector.is_probe() {
        return Ok(Vec::new());
    }
    let mut collector = collector.clone();
    probes::apply_probe_defaults(&mut collector);
    let scopes = load_scopes(conn, &collector.collector_uuid)?;
    let mut stmt = conn.prepare(
        "SELECT s.id, COALESCE(s.name, ''), COALESCE(s.tag, ''), COALESCE(s.probe_target, ''), \
         COALESCE(s.probe_tcp_ports, ''), COALESCE(s.probe_enable_icmp, 1), \
         COALESCE(s.probe_enable_tcp, 1), COALESCE(s.probe_enable_mtr, 1), COALESCE(r.last_ip, '') \
         FROM servers s \
         LEFT JOIN server_runtimes r ON r.server_id = s.id \
         WHERE s.deleted_at IS NULL \
         ORDER BY s.id ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(ServerProbeRow {
            id: row.get(0)?,
            name: row.get(1)?,
            tag: row.get(2)?,
            probe_target: row.get(3)?,
            probe_tcp_ports: row.get(4)?,
            enable_icmp: row.get::<_, i64>(5)? != 0,
            enable_tcp: row.get::<_, i64>(6)? != 0,
            enable_mtr: row.get::<_, i64>(7)? != 0,
            last_ip: row.get(8)?,
        })
    })?;
    let mut targets = Vec::new();
    for row in rows {
        let server = row?;
        if !scopes_include(&scopes, server.id, &server.tag) {
            continue;
        }
        let resolved = apply_ip_families(
            resolve_probe_target(&server.probe_target, &server.last_ip),
            collector.enable_ipv4,
            collector.enable_ipv6,
        );
        let icmp = collector.enable_icmp && server.enable_icmp;
        let tcp = collector.enable_tcp && server.enable_tcp;
        let mtr = collector.enable_mtr && server.enable_mtr;
        if !has_active_probe(icmp, tcp, mtr, &resolved) {
            continue;
        }
        let mut ports = probes::parse_ports(&server.probe_tcp_ports);
        if ports.is_empty() {
            ports = probes::parse_ports(&collector.tcp_ports);
        }
        targets.push(proto::ProbeTarget {
            server_id: u64::try_from(server.id).unwrap_or(0),
            server_name: server.name,
            ipv4: resolved.ipv4,
            ipv6: resolved.ipv6,
            hostname: resolved.hostname,
            tcp_ports: ports,
            enable_icmp: icmp,
            enable_tcp: tcp,
            enable_mtr: mtr,
            interval_seconds: u32_from_i64(collector.probe_interval_sec),
            mtr_interval_seconds: u32_from_i64(collector.mtr_interval_sec),
            route_interval_seconds: u32_from_i64(collector.route_interval_sec),
            mtr_probes: u32_from_i64(collector.mtr_probes),
        });
    }
    Ok(targets)
}

/// `IngestProbeSamples`。通知在返回值里，由调用方在提交后发送。
pub fn ingest_probe_samples(
    conn: &Connection,
    collector: &CollectorRow,
    batch: &proto::ProbeSampleBatch,
    now_nanos: i64,
) -> anyhow::Result<Vec<ProbeNotice>> {
    let mut notices = Vec::new();
    for sample in &batch.samples {
        notices.extend(ingest_one(conn, collector, sample, now_nanos)?);
    }
    Ok(notices)
}

/// `ListPendingProbeRouteJobs`：先把超时未领取的任务标失败，再按 id 升序返回。
pub fn pending_route_jobs(
    conn: &Connection,
    collector_uuid: &str,
    now_nanos: i64,
) -> anyhow::Result<Vec<proto::ProbeRouteJob>> {
    let cutoff = now_nanos.saturating_sub(ROUTE_JOB_TTL_NANOS);
    let updated = unix_nanos_to_sqlite(now_nanos);
    conn.execute(
        "UPDATE probe_route_jobs SET status = 'failed', error = ?1, finished_at = ?2, updated_at = ?3 \
         WHERE status = 'pending' AND requested_at < ?4",
        params![JOB_EXPIRE_ERROR, now_nanos, updated, cutoff],
    )?;
    let mut stmt = conn.prepare(
        "SELECT id, server_id, protocol, COALESCE(port, 0) FROM probe_route_jobs \
         WHERE collector_uuid = ?1 AND status = 'pending' ORDER BY id ASC",
    )?;
    let rows = stmt.query_map([collector_uuid], |row| {
        Ok(proto::ProbeRouteJob {
            job_id: u64::try_from(row.get::<_, i64>(0)?).unwrap_or(0),
            server_id: u64::try_from(row.get::<_, i64>(1)?).unwrap_or(0),
            protocol: row.get(2)?,
            port: u32_from_i64(row.get(3)?),
        })
    })?;
    let mut jobs = Vec::new();
    for row in rows {
        jobs.push(row?);
    }
    Ok(jobs)
}

#[derive(Debug, Clone)]
pub struct MtrSummary {
    pub loss: f64,
    pub hop_count: usize,
    pub sampled_at: i64,
    pub protocol: String,
    pub port: u32,
}

/// 从已存的跳 JSON 取末跳丢包。同时认小写 `loss` 与 Go 导出的 `Loss`。
pub fn mtr_from_blob(raw: &[u8], sampled_at: i64, protocol: &str, port: u32) -> Option<MtrSummary> {
    if raw.is_empty() {
        return None;
    }
    let hops: Vec<Value> = serde_json::from_slice(raw).ok()?;
    if hops.is_empty() {
        return None;
    }
    let loss = hops
        .last()
        .and_then(|hop| hop.get("loss").or_else(|| hop.get("Loss")))
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    Some(MtrSummary {
        loss,
        hop_count: hops.len(),
        sampled_at,
        protocol: protocol.to_string(),
        port,
    })
}

/// `selectProbeMTR`：ICMP 末跳全丢时改用 TCP。
pub fn select_mtr(icmp: Option<MtrSummary>, tcp: Option<MtrSummary>) -> Option<MtrSummary> {
    match (icmp, tcp) {
        (Some(icmp), Some(tcp)) if icmp.loss >= 1.0 => Some(tcp),
        (Some(icmp), _) => Some(icmp),
        (None, tcp) => tcp,
    }
}

struct ServerProbeRow {
    id: i64,
    name: String,
    tag: String,
    probe_target: String,
    probe_tcp_ports: String,
    enable_icmp: bool,
    enable_tcp: bool,
    enable_mtr: bool,
    last_ip: String,
}

fn ingest_one(
    conn: &Connection,
    collector: &CollectorRow,
    sample: &proto::ProbeSample,
    now_nanos: i64,
) -> anyhow::Result<Vec<ProbeNotice>> {
    let Some(server_id) = i64::try_from(sample.server_id).ok().filter(|id| *id > 0) else {
        return Ok(Vec::new());
    };
    let sampled_at = if sample.sampled_at_unix_nano == 0 {
        now_nanos
    } else {
        sample.sampled_at_unix_nano
    };
    ingest_routes(conn, collector, server_id, sample, now_nanos)?;
    let has_icmp_trace = sample
        .mtr
        .as_ref()
        .is_some_and(|trace| !trace.hops.is_empty());
    let has_tcp_trace = sample
        .mtr_tcp
        .as_ref()
        .is_some_and(|trace| !trace.hops.is_empty());
    if has_icmp_trace || has_tcp_trace {
        upsert_trace(
            conn,
            &collector.collector_uuid,
            server_id,
            sample,
            now_nanos,
        )?;
    }
    if sample.icmp.is_none() && sample.tcp.is_empty() {
        // MTR / 路由失败样本没有连通性结果，但错误必须留在最新值上。
        if !sample.last_error.trim().is_empty() {
            touch_probe_error(
                conn,
                &collector.collector_uuid,
                server_id,
                &sample.last_error,
                now_nanos,
            )?;
        }
        return Ok(Vec::new());
    }
    let mut any_tcp = false;
    let mut tcp_views = Vec::new();
    for item in &sample.tcp {
        tcp_views.push(json!({
            "port": item.port,
            "ok": item.ok,
            "rtt_ms": item.rtt_ms,
            "error": item.error,
        }));
        if item.ok {
            any_tcp = true;
        }
        upsert_bucket(
            conn,
            &collector.collector_uuid,
            server_id,
            "tcp",
            i64::from(item.port),
            sampled_at,
            item.ok,
            item.rtt_ms,
            0.0,
            now_nanos,
        )?;
    }
    let icmp_ok = sample.icmp.as_ref().is_some_and(|icmp| icmp.ok);
    let icmp_rtt = sample.icmp.as_ref().map(|icmp| icmp.rtt_ms).unwrap_or(0.0);
    let icmp_loss = sample.icmp.as_ref().map(|icmp| icmp.loss).unwrap_or(0.0);
    if let Some(icmp) = &sample.icmp {
        upsert_bucket(
            conn,
            &collector.collector_uuid,
            server_id,
            "icmp",
            0,
            sampled_at,
            icmp.ok,
            icmp.rtt_ms,
            icmp.loss,
            now_nanos,
        )?;
    }
    let has_connectivity = sample.icmp.is_some() || !tcp_views.is_empty();
    let mut reachable = icmp_ok || any_tcp;
    if !has_connectivity {
        reachable = true;
    }
    let mut display = 0.0;
    if icmp_ok {
        display = icmp_rtt;
    } else {
        for item in &sample.tcp {
            if item.ok {
                display = item.rtt_ms;
                break;
            }
        }
    }
    let tcp_json = serde_json::to_vec(&tcp_views)?;
    let updated = unix_nanos_to_sqlite(now_nanos);
    let (sent, recv) = sample
        .icmp
        .as_ref()
        .map(|icmp| {
            (
                i64::from(icmp.packets_sent),
                i64::from(icmp.packets_received),
            )
        })
        .unwrap_or((0, 0));
    conn.execute(
        "INSERT INTO probe_latests (collector_uuid, server_id, sampled_at, reachable, display_rtt_ms, \
         last_error, icmp_ok, icmp_rtt_ms, icmp_loss, icmp_sent, icmp_recv, tcp_json, has_trace, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14) \
         ON CONFLICT(collector_uuid, server_id) DO UPDATE SET \
         sampled_at = excluded.sampled_at, reachable = excluded.reachable, \
         display_rtt_ms = excluded.display_rtt_ms, last_error = excluded.last_error, \
         icmp_ok = excluded.icmp_ok, icmp_rtt_ms = excluded.icmp_rtt_ms, icmp_loss = excluded.icmp_loss, \
         icmp_sent = excluded.icmp_sent, icmp_recv = excluded.icmp_recv, tcp_json = excluded.tcp_json, \
         has_trace = excluded.has_trace, updated_at = excluded.updated_at",
        params![
            collector.collector_uuid,
            server_id,
            sampled_at,
            i64::from(reachable),
            display,
            sample.last_error,
            i64::from(icmp_ok),
            icmp_rtt,
            icmp_loss,
            sent,
            recv,
            tcp_json,
            i64::from(has_icmp_trace || has_tcp_trace),
            updated,
        ],
    )?;
    evaluate_alert(
        conn,
        collector,
        server_id,
        reachable,
        has_connectivity,
        display,
        &sample.last_error,
        now_nanos,
    )
}

fn touch_probe_error(
    conn: &Connection,
    collector_uuid: &str,
    server_id: i64,
    last_error: &str,
    now_nanos: i64,
) -> anyhow::Result<()> {
    conn.execute(
        "UPDATE probe_latests SET last_error = ?1, updated_at = ?2 \
         WHERE collector_uuid = ?3 AND server_id = ?4",
        params![
            last_error.trim(),
            unix_nanos_to_sqlite(now_nanos),
            collector_uuid,
            server_id,
        ],
    )?;
    Ok(())
}

fn upsert_trace(
    conn: &Connection,
    collector_uuid: &str,
    server_id: i64,
    sample: &proto::ProbeSample,
    now_nanos: i64,
) -> anyhow::Result<()> {
    let existing = conn
        .query_row(
            "SELECT sampled_at, destination, hops_json, tcp_sampled_at, tcp_destination, tcp_hops_json, tcp_port \
             FROM probe_traces WHERE collector_uuid = ?1 AND server_id = ?2",
            params![collector_uuid, server_id],
            |row| {
                Ok(StoredTrace {
                    sampled_at: row.get(0)?,
                    destination: row.get(1)?,
                    hops: row.get(2)?,
                    tcp_sampled_at: row.get(3)?,
                    tcp_destination: row.get(4)?,
                    tcp_hops: row.get(5)?,
                    tcp_port: row.get(6)?,
                })
            },
        )
        .optional()?;
    let mut sampled_at = existing.as_ref().and_then(|row| row.sampled_at);
    let mut destination = existing.as_ref().and_then(|row| row.destination.clone());
    let mut hops = existing.as_ref().and_then(|row| row.hops.clone());
    let mut tcp_sampled_at = existing.as_ref().and_then(|row| row.tcp_sampled_at);
    let mut tcp_destination = existing
        .as_ref()
        .and_then(|row| row.tcp_destination.clone());
    let mut tcp_hops = existing.as_ref().and_then(|row| row.tcp_hops.clone());
    let mut tcp_port = existing.as_ref().and_then(|row| row.tcp_port);
    if let Some(blob) = mtr_hops_json(sample.mtr.as_ref()) {
        if let Some(trace) = &sample.mtr {
            sampled_at = Some(trace.sampled_at_unix_nano);
            destination = Some(trace.destination.clone());
        }
        hops = Some(blob);
    }
    if let Some(blob) = mtr_hops_json(sample.mtr_tcp.as_ref()) {
        if let Some(trace) = &sample.mtr_tcp {
            tcp_sampled_at = Some(trace.sampled_at_unix_nano);
            tcp_destination = Some(trace.destination.clone());
            tcp_port = Some(i64::from(trace.port));
        }
        tcp_hops = Some(blob);
    }
    conn.execute(
        "INSERT INTO probe_traces (collector_uuid, server_id, sampled_at, destination, hops_json, \
         tcp_sampled_at, tcp_destination, tcp_hops_json, tcp_port, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) \
         ON CONFLICT(collector_uuid, server_id) DO UPDATE SET \
         sampled_at = excluded.sampled_at, destination = excluded.destination, hops_json = excluded.hops_json, \
         tcp_sampled_at = excluded.tcp_sampled_at, tcp_destination = excluded.tcp_destination, \
         tcp_hops_json = excluded.tcp_hops_json, tcp_port = excluded.tcp_port, updated_at = excluded.updated_at",
        params![
            collector_uuid,
            server_id,
            sampled_at,
            destination,
            hops,
            tcp_sampled_at,
            tcp_destination,
            tcp_hops,
            tcp_port,
            unix_nanos_to_sqlite(now_nanos),
        ],
    )?;
    Ok(())
}

struct StoredTrace {
    sampled_at: Option<i64>,
    destination: Option<String>,
    hops: Option<Vec<u8>>,
    tcp_sampled_at: Option<i64>,
    tcp_destination: Option<String>,
    tcp_hops: Option<Vec<u8>>,
    tcp_port: Option<i64>,
}

fn mtr_hops_json(trace: Option<&proto::ProbeMtrTrace>) -> Option<Vec<u8>> {
    let trace = trace.filter(|trace| !trace.hops.is_empty())?;
    let hops: Vec<Value> = trace
        .hops
        .iter()
        .map(|hop| {
            json!({
                "ttl": hop.ttl,
                "address": hop.address,
                "loss": hop.loss,
                "avg_ms": hop.avg_ms,
                "sent": hop.sent,
            })
        })
        .collect();
    serde_json::to_vec(&hops).ok()
}

#[allow(clippy::too_many_arguments)]
fn upsert_bucket(
    conn: &Connection,
    collector_uuid: &str,
    server_id: i64,
    kind: &str,
    port: i64,
    sampled_at: i64,
    ok: bool,
    rtt_ms: f64,
    loss: f64,
    now_nanos: i64,
) -> anyhow::Result<()> {
    let bucket = sampled_at - sampled_at.rem_euclid(MINUTE_NANOS);
    let existing: Option<(f64, f64, f64, i64, f64, i64, i64)> = conn
        .query_row(
            "SELECT COALESCE(min_ms, 0), COALESCE(max_ms, 0), COALESCE(sum_ms, 0), COALESCE(count, 0), \
             COALESCE(loss_sum, 0), COALESCE(success_count, 0), COALESCE(fail_count, 0) \
             FROM probe_sample_buckets \
             WHERE collector_uuid = ?1 AND server_id = ?2 AND kind = ?3 AND port = ?4 AND bucket_start = ?5",
            params![collector_uuid, server_id, kind, port, bucket],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)),
        )
        .optional()?;
    let (mut min_ms, mut max_ms, mut sum_ms, mut count, mut loss_sum, mut success, mut fail) =
        existing.unwrap_or((0.0, 0.0, 0.0, 0, 0.0, 0, 0));
    count += 1;
    loss_sum += loss;
    if ok {
        success += 1;
        if count == 1 || rtt_ms < min_ms || min_ms == 0.0 {
            min_ms = rtt_ms;
        }
        if rtt_ms > max_ms {
            max_ms = rtt_ms;
        }
        sum_ms += rtt_ms;
    } else {
        fail += 1;
    }
    conn.execute(
        "INSERT INTO probe_sample_buckets (collector_uuid, server_id, kind, port, bucket_start, \
         min_ms, max_ms, sum_ms, count, loss_sum, success_count, fail_count, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13) \
         ON CONFLICT(collector_uuid, server_id, kind, port, bucket_start) DO UPDATE SET \
         min_ms = excluded.min_ms, max_ms = excluded.max_ms, sum_ms = excluded.sum_ms, \
         count = excluded.count, loss_sum = excluded.loss_sum, success_count = excluded.success_count, \
         fail_count = excluded.fail_count, updated_at = excluded.updated_at",
        params![
            collector_uuid,
            server_id,
            kind,
            port,
            bucket,
            min_ms,
            max_ms,
            sum_ms,
            count,
            loss_sum,
            success,
            fail,
            unix_nanos_to_sqlite(now_nanos),
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn evaluate_alert(
    conn: &Connection,
    collector: &CollectorRow,
    server_id: i64,
    reachable: bool,
    has_connectivity: bool,
    rtt_ms: f64,
    last_error: &str,
    now_nanos: i64,
) -> anyhow::Result<Vec<ProbeNotice>> {
    if !collector.is_probe() || !collector.probe_notify {
        return Ok(Vec::new());
    }
    let mut collector = collector.clone();
    probes::apply_probe_defaults(&mut collector);
    let server: Option<(String, String, bool, bool, bool)> = conn
        .query_row(
            "SELECT COALESCE(name, ''), COALESCE(probe_target, ''), COALESCE(probe_enable_icmp, 1), \
             COALESCE(probe_enable_tcp, 1), COALESCE(probe_enable_mtr, 1) \
             FROM servers WHERE id = ?1 AND deleted_at IS NULL",
            [server_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get::<_, i64>(2)? != 0,
                    row.get::<_, i64>(3)? != 0,
                    row.get::<_, i64>(4)? != 0,
                ))
            },
        )
        .optional()?;
    let (server_name, probe_target, server_icmp, server_tcp, server_mtr) =
        server.unwrap_or_else(|| (String::new(), String::new(), true, true, true));
    let last_ip: String = conn
        .query_row(
            "SELECT COALESCE(last_ip, '') FROM server_runtimes WHERE server_id = ?1",
            [server_id],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or_default();
    let resolved = apply_ip_families(
        resolve_probe_target(&probe_target, &last_ip),
        collector.enable_ipv4,
        collector.enable_ipv6,
    );
    let icmp = collector.enable_icmp && server_icmp;
    let tcp = collector.enable_tcp && server_tcp;
    let mtr = collector.enable_mtr && server_mtr;
    if !has_active_probe(icmp, tcp, mtr, &resolved) || !has_connectivity {
        return Ok(Vec::new());
    }
    let state: Option<(i64, i64, i64)> = conn
        .query_row(
            "SELECT COALESCE(consecutive_fails, 0), COALESCE(down_notified, 0), COALESCE(latency_alert, 0) \
             FROM probe_alert_states WHERE collector_uuid = ?1 AND server_id = ?2",
            params![collector.collector_uuid, server_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let (mut fails, mut down_notified, mut latency_alert) = state.unwrap_or((0, 0, 0));
    let mut notices = Vec::new();
    let tag = collector
        .notification_tag
        .clone()
        .unwrap_or_else(|| "default".to_string());
    if reachable {
        if down_notified != 0 {
            notices.push(ProbeNotice {
                tag: tag.clone(),
                message: format!("[ProbeUp] {} → {server_name}", collector.name),
                mute_label: format!("bf::pu-{}-{server_id}", collector.collector_uuid),
                server_id,
            });
        }
        fails = 0;
        down_notified = 0;
    } else {
        fails += 1;
        if down_notified == 0 && fails >= collector.fail_threshold {
            let err_text = if last_error.trim().is_empty() {
                "unreachable"
            } else {
                last_error.trim()
            };
            notices.push(ProbeNotice {
                tag: tag.clone(),
                message: format!("[ProbeDown] {} → {server_name} {err_text}", collector.name),
                mute_label: format!("bf::pd-{}-{server_id}", collector.collector_uuid),
                server_id,
            });
            down_notified = 1;
        }
    }
    if collector.latency_notify && reachable && rtt_ms > 0.0 {
        let over = (collector.max_latency_ms > 0.0 && rtt_ms > collector.max_latency_ms)
            || (collector.min_latency_ms > 0.0 && rtt_ms < collector.min_latency_ms);
        if over && latency_alert == 0 {
            notices.push(ProbeNotice {
                tag,
                message: format!(
                    "[ProbeLatency] {} → {server_name} {rtt:.0}ms",
                    collector.name,
                    rtt = rtt_ms
                ),
                mute_label: format!("bf::pl-{}-{server_id}", collector.collector_uuid),
                server_id,
            });
            latency_alert = 1;
        }
        if !over {
            latency_alert = 0;
        }
    }
    conn.execute(
        "INSERT INTO probe_alert_states (collector_uuid, server_id, consecutive_fails, down_notified, latency_alert, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
         ON CONFLICT(collector_uuid, server_id) DO UPDATE SET \
         consecutive_fails = excluded.consecutive_fails, down_notified = excluded.down_notified, \
         latency_alert = excluded.latency_alert, updated_at = excluded.updated_at",
        params![
            collector.collector_uuid,
            server_id,
            fails,
            down_notified,
            latency_alert,
            unix_nanos_to_sqlite(now_nanos),
        ],
    )?;
    Ok(notices)
}

fn ingest_routes(
    conn: &Connection,
    collector: &CollectorRow,
    server_id: i64,
    sample: &proto::ProbeSample,
    now_nanos: i64,
) -> anyhow::Result<()> {
    let mut collector = collector.clone();
    probes::apply_probe_defaults(&mut collector);
    for trace in [sample.route_icmp.as_ref(), sample.route_tcp.as_ref()]
        .into_iter()
        .flatten()
    {
        insert_route(
            conn,
            &collector.collector_uuid,
            server_id,
            collector.route_keep,
            trace,
            now_nanos,
        )?;
        if trace.job_id > 0 {
            finish_route_job(conn, trace.job_id, &trace.error, now_nanos)?;
        }
    }
    Ok(())
}

fn insert_route(
    conn: &Connection,
    collector_uuid: &str,
    server_id: i64,
    keep: i64,
    trace: &proto::ProbeRouteTrace,
    now_nanos: i64,
) -> anyhow::Result<()> {
    let protocol = trace.protocol.trim().to_ascii_lowercase();
    if protocol != "icmp" && protocol != "tcp" {
        return Ok(());
    }
    let sampled_at = if trace.sampled_at_unix_nano == 0 {
        now_nanos
    } else {
        trace.sampled_at_unix_nano
    };
    let hops: Vec<Value> = trace
        .hops
        .iter()
        .map(|hop| {
            json!({
                "ttl": hop.ttl,
                "address": hop.address,
                "hostname": hop.hostname,
                "rtt_ms": hop.rtt_ms,
                "loss": hop.loss,
                "sent": hop.sent,
                "asn": hop.asn,
                "country": hop.country,
                "province": hop.province,
                "city": hop.city,
                "owner": hop.owner,
            })
        })
        .collect();
    let hops_json = serde_json::to_vec(&hops)?;
    conn.execute(
        "INSERT INTO probe_routes (collector_uuid, server_id, protocol, sampled_at, destination, port, hops_json, error, job_id, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            collector_uuid,
            server_id,
            protocol,
            sampled_at,
            trace.destination,
            i64::from(trace.port),
            hops_json,
            trace.error.trim(),
            i64::try_from(trace.job_id).unwrap_or(0),
            unix_nanos_to_sqlite(now_nanos),
        ],
    )?;
    prune_routes(conn, collector_uuid, server_id, &protocol, keep)
}

fn prune_routes(
    conn: &Connection,
    collector_uuid: &str,
    server_id: i64,
    protocol: &str,
    keep: i64,
) -> anyhow::Result<()> {
    let keep = probes::normalize_route_keep(keep);
    let mut stmt = conn.prepare(
        "SELECT id FROM probe_routes WHERE collector_uuid = ?1 AND server_id = ?2 AND protocol = ?3 \
         ORDER BY sampled_at DESC, id DESC LIMIT -1 OFFSET ?4",
    )?;
    let ids: Vec<i64> = stmt
        .query_map(params![collector_uuid, server_id, protocol, keep], |row| {
            row.get(0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    for id in ids {
        conn.execute("DELETE FROM probe_routes WHERE id = ?1", [id])?;
    }
    Ok(())
}

fn finish_route_job(
    conn: &Connection,
    job_id: u64,
    last_error: &str,
    now_nanos: i64,
) -> anyhow::Result<()> {
    let Some(id) = i64::try_from(job_id).ok().filter(|id| *id > 0) else {
        return Ok(());
    };
    let error = last_error.trim();
    let status = if error.is_empty() { "done" } else { "failed" };
    conn.execute(
        "UPDATE probe_route_jobs SET status = ?1, error = ?2, finished_at = ?3, updated_at = ?4 \
         WHERE id = ?5 AND status = 'pending'",
        params![
            status,
            error,
            now_nanos,
            unix_nanos_to_sqlite(now_nanos),
            id
        ],
    )?;
    Ok(())
}

fn load_scopes(conn: &Connection, collector_uuid: &str) -> anyhow::Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT scope_type, scope_value FROM collector_scopes WHERE collector_uuid = ?1",
    )?;
    let rows = stmt.query_map([collector_uuid], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let mut scopes = Vec::new();
    for row in rows {
        scopes.push(row?);
    }
    Ok(scopes)
}

fn scopes_include(scopes: &[(String, String)], server_id: i64, tag: &str) -> bool {
    let server_id = server_id.to_string();
    scopes
        .iter()
        .any(|(scope_type, scope_value)| match scope_type.as_str() {
            "all" => true,
            "server" => scope_value == &server_id,
            "group" | "tag" => scope_value == tag,
            _ => false,
        })
}

fn has_active_probe(icmp: bool, tcp: bool, mtr: bool, target: &ResolvedProbeTarget) -> bool {
    (icmp || tcp || mtr) && target.source != "none"
}

/// Go `SplitIPAddr`：`v4/v6` 双栈，单地址按是否含 `:` 区分。
fn split_ip_addr(raw: &str) -> (String, String) {
    let raw = raw.trim();
    if raw.is_empty() {
        return (String::new(), String::new());
    }
    let parts: Vec<&str> = raw.split('/').collect();
    if parts.len() > 1 {
        return (parts[0].trim().to_string(), parts[1].trim().to_string());
    }
    if raw.contains(':') {
        (String::new(), raw.to_string())
    } else {
        (raw.to_string(), String::new())
    }
}

fn u32_from_i64(value: i64) -> u32 {
    value.clamp(0, i64::from(u32::MAX)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;
    use crate::repo::collectors;

    fn temp_db(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "santaizi-probe-ingest-{tag}-{}-{}",
            std::process::id(),
            crate::util::time::now_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("panel.db").to_string_lossy().into_owned()
    }

    fn seed(db: &crate::db::Db) {
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, probe_target, probe_tcp_ports) \
                 VALUES (7, 'edge', 'web', X'00', 'edge.example', '443')",
                [],
            )?;
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext) VALUES (8, 'bare', 'web', X'01')",
                [],
            )?;
            tx.execute(
                "INSERT INTO server_runtimes (server_id, last_ip) VALUES (8, '203.0.113.8/2001:db8::8')",
                [],
            )?;
            tx.execute(
                "INSERT INTO collectors (collector_uuid, name, address, token_hash, token_ciphertext, kind, \
                 tcp_ports, probe_notify, fail_threshold, latency_notify, max_latency_ms, route_keep, revoked, deleted) \
                 VALUES ('probe-1', 'probe', '10.0.0.1:5556', X'00', X'00', 'probe', '22', 1, 2, 1, 100, 2, 0, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO collector_scopes (collector_uuid, scope_type, scope_value) VALUES ('probe-1', 'tag', 'web')",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn resolves_override_hostname_and_split_host_ip() {
        let host = resolve_probe_target("edge.example", "");
        assert_eq!(host.source, "override");
        assert_eq!(host.hostname, "edge.example");
        let split = resolve_probe_target("", "203.0.113.8/2001:db8::8");
        assert_eq!(split.source, "host_ip");
        assert_eq!(split.ipv4, "203.0.113.8");
        assert_eq!(split.ipv6, "2001:db8::8");
        let v4_only = apply_ip_families(split, true, false);
        assert!(v4_only.ipv6.is_empty());
        assert_eq!(v4_only.source, "host_ip");
    }

    #[test]
    fn builds_targets_and_ingests_samples_alerts_and_jobs() {
        let path = temp_db("ingest");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db);
        let targets = db
            .read(|conn| {
                let collector = collectors::find_by_uuid(conn, "probe-1")?.unwrap();
                build_probe_targets(conn, &collector)
            })
            .unwrap();
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].server_id, 7);
        assert_eq!(targets[0].hostname, "edge.example");
        assert_eq!(targets[0].tcp_ports, vec![443]);
        assert_eq!(targets[1].ipv4, "203.0.113.8");
        assert!(targets[1].enable_icmp);

        let now = 1_700_000_000_000_000_000;
        let notices = db
            .write(|tx| {
                let collector = collectors::find_by_uuid_tx(tx, "probe-1")?.unwrap();
                let mut batch = proto::ProbeSampleBatch {
                    samples: vec![proto::ProbeSample {
                        server_id: 7,
                        sampled_at_unix_nano: now,
                        icmp: Some(proto::ProbeIcmpSample {
                            ok: false,
                            error: "timeout".into(),
                            ..Default::default()
                        }),
                        last_error: "timeout".into(),
                        mtr: Some(proto::ProbeMtrTrace {
                            sampled_at_unix_nano: now,
                            destination: "edge.example".into(),
                            hops: vec![proto::ProbeMtrHop {
                                ttl: 1,
                                address: "10.0.0.1".into(),
                                loss: 0.0,
                                avg_ms: 1.0,
                                sent: 3,
                            }],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                };
                let first = ingest_probe_samples(tx, &collector, &batch, now)?;
                batch.samples[0].sampled_at_unix_nano = now + MINUTE_NANOS;
                let second = ingest_probe_samples(tx, &collector, &batch, now)?;
                Ok((first, second))
            })
            .unwrap();
        assert!(notices.0.is_empty());
        assert_eq!(notices.1.len(), 1);
        assert!(notices.1[0].message.contains("[ProbeDown]"));
        assert!(notices.1[0].message.contains("timeout"));

        db.write(|tx| {
            tx.execute(
                "INSERT INTO probe_route_jobs (collector_uuid, server_id, protocol, port, status, requested_at, updated_at) \
                 VALUES ('probe-1', 7, 'icmp', 0, 'pending', ?1, ?2)",
                params![now, unix_nanos_to_sqlite(now)],
            )?;
            tx.execute(
                "INSERT INTO probe_route_jobs (collector_uuid, server_id, protocol, port, status, requested_at, updated_at) \
                 VALUES ('probe-1', 8, 'tcp', 443, 'pending', ?1, ?2)",
                params![now - ROUTE_JOB_TTL_NANOS - 1, unix_nanos_to_sqlite(now)],
            )?;
            let jobs = pending_route_jobs(tx, "probe-1", now)?;
            assert_eq!(jobs.len(), 1);
            assert_eq!(jobs[0].server_id, 7);
            let collector = collectors::find_by_uuid_tx(tx, "probe-1")?.unwrap();
            let batch = proto::ProbeSampleBatch {
                samples: vec![proto::ProbeSample {
                    server_id: 7,
                    sampled_at_unix_nano: now + 2 * MINUTE_NANOS,
                    icmp: Some(proto::ProbeIcmpSample {
                        ok: true,
                        rtt_ms: 250.0,
                        packets_sent: 3,
                        packets_received: 3,
                        ..Default::default()
                    }),
                    route_icmp: Some(proto::ProbeRouteTrace {
                        sampled_at_unix_nano: now,
                        destination: "edge.example".into(),
                        protocol: "icmp".into(),
                        job_id: jobs[0].job_id,
                        hops: vec![proto::ProbeRouteHop {
                            ttl: 1,
                            address: "10.0.0.1".into(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
            };
            let up = ingest_probe_samples(tx, &collector, &batch, now)?;
            assert!(up.iter().any(|notice| notice.message.contains("[ProbeUp]")));
            assert!(up.iter().any(|notice| notice.message.contains("[ProbeLatency]")));
            let status: String = tx.query_row(
                "SELECT status FROM probe_route_jobs WHERE id = ?1",
                [i64::try_from(jobs[0].job_id).unwrap()],
                |row| row.get(0),
            )?;
            assert_eq!(status, "done");
            let hops: i64 = tx.query_row(
                "SELECT json_array_length(hops_json) FROM probe_traces WHERE collector_uuid = 'probe-1' AND server_id = 7",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(hops, 1);
            Ok(())
        })
        .unwrap();
    }
}
