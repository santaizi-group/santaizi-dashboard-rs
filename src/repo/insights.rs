//! Admin 读侧查询（Phase 5）：对齐 Go `service/telemetry/connections.go` /
//! `latency.go` 读侧与 `datasets.go` 六个 dataset。
//!
//! 时间统一输出 RFC3339（纳秒截断到秒亦可，Go 用 RFC3339Nano——这里统一
//! RFC3339，前端按 ISO 解析无差）；`observer_kind`/`observer_name` 由
//! hostIndex 等价查询派生（primary 常量 + collectors 表）。

// 行映射元组逐列镜像 SQL SELECT——具名化 8 个结构体反而更噪，整模块豁免。
#![allow(clippy::type_complexity)]

use std::collections::HashMap;

use prost::Message as _;
use rusqlite::{Connection, OptionalExtension};

use crate::proto;
use crate::telemetry::probe_ingest;
use crate::util::time::{self as util_time, unix_nanos_to_sqlite};

pub const PRIMARY_OBSERVER_ID: &str = "primary";
/// 从端心跳在线阈值（Go `COLLECTOR_TIMEOUT`：90s）。
pub const COLLECTOR_TIMEOUT_NANOS: i64 = 90 * 1_000_000_000;

pub fn hex_uuid(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn optional_rfc3339(nanos: i64) -> Value {
    if nanos <= 0 {
        return Value::Null;
    }
    Value::String(util_time::unix_nanos_to_rfc3339(nanos))
}

fn wal_pressure_label(value: i32) -> &'static str {
    match value {
        1 => "healthy",
        2 => "downsampled",
        3 => "rollup",
        4 => "critical",
        5 => "telemetry_data_loss",
        _ => "unknown",
    }
}

fn gap_reason_label(value: i64) -> &'static str {
    match value {
        1 => "compacted",
        2 => "hard_limit_data_loss",
        3 => "corruption",
        _ => "unknown",
    }
}

/// `loadHostIndex` 的等价物：node → server、observer → (kind, name)。
struct HostIndex {
    server_by_node: HashMap<Vec<u8>, i64>,
    /// server_id → (name, tag, display_index)。
    servers: HashMap<i64, (String, String, i64)>,
    observers: HashMap<String, (String, String)>,
}

impl HostIndex {
    fn load(conn: &Connection, nodes: &[Vec<u8>], observers: &[String]) -> anyhow::Result<Self> {
        let mut server_by_node = HashMap::new();
        let mut servers = HashMap::new();
        if !nodes.is_empty() {
            let placeholders = nodes.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let sql = format!(
                "SELECT b.node_uuid, b.server_id, s.name, s.tag, COALESCE(s.display_index, 0) \
                 FROM server_node_bindings b JOIN servers s ON s.id = b.server_id \
                 WHERE b.current = 1 AND b.node_uuid IN ({placeholders})"
            );
            let mut stmt = conn.prepare(&sql)?;
            let refs: Vec<&dyn rusqlite::ToSql> = nodes
                .iter()
                .map(|node| node as &dyn rusqlite::ToSql)
                .collect();
            let rows = stmt.query_map(refs.as_slice(), |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })?;
            for row in rows {
                let (node, id, name, tag, display_index) = row?;
                server_by_node.insert(node, id);
                servers.insert(id, (name, tag, display_index));
            }
        }
        let mut index = Self {
            server_by_node,
            servers,
            observers: HashMap::new(),
        };
        for observer in observers {
            if observer == PRIMARY_OBSERVER_ID {
                index.observers.insert(
                    observer.clone(),
                    ("primary".to_string(), "主面板".to_string()),
                );
            }
        }
        let filtered: Vec<&String> = observers
            .iter()
            .filter(|id| !id.is_empty() && id.as_str() != PRIMARY_OBSERVER_ID)
            .collect();
        if !filtered.is_empty() {
            let placeholders = filtered.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let sql = format!(
                "SELECT collector_uuid, name, kind FROM collectors WHERE collector_uuid IN ({placeholders})"
            );
            let mut stmt = conn.prepare(&sql)?;
            let refs: Vec<&dyn rusqlite::ToSql> = filtered
                .iter()
                .map(|id| id as &dyn rusqlite::ToSql)
                .collect();
            let rows = stmt.query_map(refs.as_slice(), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?;
            for row in rows {
                let (uuid, name, kind) = row?;
                let kind_label = if kind == "probe" {
                    "probe"
                } else {
                    "collector"
                };
                index.observers.insert(uuid, (kind_label.to_string(), name));
            }
        }
        Ok(index)
    }

    fn server(&self, node: &[u8]) -> Option<(i64, String, i64, String)> {
        self.server_by_node.get(node).and_then(|id| {
            self.servers
                .get(id)
                .map(|(name, tag, display)| (*id, name.clone(), *display, tag.clone()))
        })
    }

    fn observer(&self, id: &str) -> (&'static str, String) {
        match self.observers.get(id) {
            Some((kind, name)) => match kind.as_str() {
                "primary" => ("primary", name.clone()),
                "probe" => ("probe", name.clone()),
                _ => ("collector", name.clone()),
            },
            None => ("", String::new()),
        }
    }

    fn is_probe_observer(&self, id: &str) -> bool {
        self.observers
            .get(id)
            .is_some_and(|(kind, _)| kind == "probe")
    }
}

/// Go `LoadConnectionSummary`。
#[derive(Debug, Clone, Default)]
pub struct ConnectionSummary {
    pub collectors_total: i64,
    pub collectors_online: i64,
    pub collectors_offline: i64,
    pub collectors_unknown: i64,
    pub paths_assigned: i64,
    pub paths_connected: i64,
    pub paths_seen: i64,
}

pub fn load_connection_summary(
    conn: &Connection,
    now_nanos: i64,
) -> anyhow::Result<ConnectionSummary> {
    let collectors_total = conn.query_row(
        "SELECT COUNT(*) FROM collectors WHERE deleted = 0 AND revoked = 0 AND kind != 'probe'",
        [],
        |row| row.get(0),
    )?;
    let mut summary = ConnectionSummary {
        collectors_total,
        ..Default::default()
    };
    let rows: Vec<(String, Option<i64>)> = {
        let mut stmt = conn.prepare(
            "SELECT c.collector_uuid, r.last_seen FROM collectors c \
             LEFT JOIN collector_runtimes r ON r.collector_uuid = c.collector_uuid \
             WHERE c.deleted = 0 AND c.revoked = 0 AND c.kind != 'probe'",
        )?;
        let mapped = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let mut out = Vec::new();
        for row in mapped {
            out.push(row?);
        }
        out
    };
    for (_, last_seen) in &rows {
        match last_seen {
            None => summary.collectors_unknown += 1,
            Some(seen) => {
                if now_nanos - *seen <= COLLECTOR_TIMEOUT_NANOS {
                    summary.collectors_online += 1;
                } else {
                    summary.collectors_offline += 1;
                }
            }
        }
    }
    let paths = load_connection_paths(conn, None, None, now_nanos)?;
    summary.paths_assigned = paths.len() as i64;
    summary.paths_connected = paths.iter().filter(|path| path.sink.connected).count() as i64;
    summary.paths_seen = paths.iter().filter(|path| path.last_seen > 0).count() as i64;
    Ok(summary)
}

/// Go `PathSink`。
#[derive(Debug, Clone, Default)]
pub struct PathSink {
    pub connected: bool,
    pub pending_events: u64,
    pub last_error: String,
    pub ack_through: u64,
    pub last_rtt_ms: Option<f64>,
    pub rtt_sampled_at: i64,
}

#[derive(Debug, Clone)]
pub struct ConnectionPath {
    pub server_id: i64,
    pub server_name: String,
    pub display_index: i64,
    pub tag: String,
    pub node_uuid: String,
    pub observer_id: String,
    pub observer_kind: String,
    pub observer_name: String,
    pub last_seen: i64,
    pub sink: PathSink,
}

fn collector_last_seen_by_id(
    conn: &Connection,
    ids: &[String],
) -> anyhow::Result<HashMap<String, i64>> {
    let filtered: Vec<&String> = ids
        .iter()
        .filter(|id| !id.is_empty() && id.as_str() != PRIMARY_OBSERVER_ID)
        .collect();
    let mut out = HashMap::new();
    if filtered.is_empty() {
        return Ok(out);
    }
    let placeholders = filtered.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
    let sql = format!(
        "SELECT collector_uuid, COALESCE(last_seen, 0) FROM collector_runtimes \
         WHERE collector_uuid IN ({placeholders})"
    );
    let mut stmt = conn.prepare(&sql)?;
    let refs: Vec<&dyn rusqlite::ToSql> = filtered
        .iter()
        .map(|id| id as &dyn rusqlite::ToSql)
        .collect();
    let rows = stmt.query_map(refs.as_slice(), |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    for row in rows {
        let (uuid, seen) = row?;
        out.insert(uuid, seen);
    }
    Ok(out)
}

/// Go `sinkHandshaked`。
fn sink_handshaked(
    observer_id: &str,
    sink: &PathSink,
    last_seen: &HashMap<String, i64>,
    now_nanos: i64,
) -> bool {
    if !sink.connected {
        return false;
    }
    if observer_id == PRIMARY_OBSERVER_ID {
        return true;
    }
    last_seen
        .get(observer_id)
        .is_some_and(|seen| now_nanos - *seen <= COLLECTOR_TIMEOUT_NANOS)
}

/// Go `loadConnectionPaths`。
pub fn load_connection_paths(
    conn: &Connection,
    filter_server_id: Option<i64>,
    filter_observer_id: Option<&str>,
    now_nanos: i64,
) -> anyhow::Result<Vec<ConnectionPath>> {
    let mut conditions = vec!["valid_to = 0".to_string()];
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(server_id) = filter_server_id.filter(|id| *id > 0) {
        let node: Option<Vec<u8>> = conn
            .query_row(
                "SELECT node_uuid FROM server_node_bindings WHERE server_id = ?1 AND current = 1 LIMIT 1",
                [server_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        let Some(node) = node else {
            return Ok(Vec::new());
        };
        params.push(Box::new(node));
        conditions.push(format!("node_uuid = ?{}", params.len()));
    }
    if let Some(observer_id) = filter_observer_id.filter(|id| !id.is_empty()) {
        params.push(Box::new(observer_id.to_string()));
        conditions.push(format!("observer_id = ?{}", params.len()));
    }
    let sql = format!(
        "SELECT node_uuid, observer_id, valid_from, config_version FROM observer_assignments \
         WHERE {}",
        conditions.join(" AND ")
    );
    let mut stmt = conn.prepare(&sql)?;
    let refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|boxed| boxed.as_ref()).collect();
    let assignments: Vec<(Vec<u8>, String)> = stmt
        .query_map(refs.as_slice(), |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if assignments.is_empty() {
        return Ok(Vec::new());
    }

    let mut nodes: Vec<Vec<u8>> = Vec::new();
    let mut observers: Vec<String> = Vec::new();
    for (node, observer) in &assignments {
        if !nodes.iter().any(|existing| existing == node) {
            nodes.push(node.clone());
        }
        if !observers.contains(observer) {
            observers.push(observer.clone());
        }
    }
    let index = HostIndex::load(conn, &nodes, &observers)?;

    let node_placeholders = nodes.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
    let observer_placeholders = observers.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
    let last_seen_sql = format!(
        "SELECT node_uuid, observer_id, MAX(last_seen_at) FROM observer_path_buckets \
         WHERE seen = 1 AND node_uuid IN ({node_placeholders}) AND observer_id IN ({observer_placeholders}) \
         GROUP BY node_uuid, observer_id"
    );
    let mut stmt = conn.prepare(&last_seen_sql)?;
    let mut bind: Vec<&dyn rusqlite::ToSql> = nodes
        .iter()
        .map(|node| node as &dyn rusqlite::ToSql)
        .collect();
    bind.extend(observers.iter().map(|id| id as &dyn rusqlite::ToSql));
    let last_seen_rows: Vec<(Vec<u8>, String, i64)> = stmt
        .query_map(bind.as_slice(), |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut last_seen: HashMap<(Vec<u8>, String), i64> = HashMap::new();
    for (node, observer, seen) in last_seen_rows {
        last_seen.insert((node, observer), seen);
    }

    let runtime_sql = format!(
        "SELECT node_uuid, sink_cursors FROM agent_telemetry_runtimes WHERE node_uuid IN ({node_placeholders})"
    );
    let mut stmt = conn.prepare(&runtime_sql)?;
    let refs: Vec<&dyn rusqlite::ToSql> = nodes
        .iter()
        .map(|node| node as &dyn rusqlite::ToSql)
        .collect();
    let runtime_rows: Vec<(Vec<u8>, Vec<u8>)> = stmt
        .query_map(refs.as_slice(), |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    let mut sinks: HashMap<(Vec<u8>, String), PathSink> = HashMap::new();
    for (node, blob) in runtime_rows {
        let Ok(agent) = proto::AgentRuntime::decode(blob.as_slice()) else {
            continue;
        };
        for sink in agent.sinks {
            sinks.insert(
                (node.clone(), sink.endpoint_id.clone()),
                PathSink {
                    connected: sink.connected,
                    pending_events: sink.pending_events,
                    last_error: sink.last_error,
                    ack_through: sink.ack_through,
                    last_rtt_ms: Some(sink.last_rtt_ms),
                    rtt_sampled_at: sink.rtt_sampled_at_unix_nano,
                },
            );
        }
    }

    let observer_last_seen = collector_last_seen_by_id(conn, &observers)?;
    let mut paths = Vec::new();
    for (node, observer_id) in assignments {
        if index.is_probe_observer(&observer_id) {
            continue;
        }
        let server = index.server(&node);
        let (kind, name) = index.observer(&observer_id);
        let mut sink = sinks
            .get(&(node.clone(), observer_id.clone()))
            .cloned()
            .unwrap_or_default();
        if !sink_handshaked(&observer_id, &sink, &observer_last_seen, now_nanos) {
            sink.connected = false;
            sink.last_rtt_ms = None;
            sink.rtt_sampled_at = 0;
        }
        paths.push(ConnectionPath {
            server_id: server.as_ref().map_or(0, |(id, ..)| *id),
            server_name: server
                .as_ref()
                .map_or(String::new(), |(_, name, ..)| name.clone()),
            display_index: server.as_ref().map_or(0, |(.., display, _)| *display),
            tag: server
                .as_ref()
                .map_or(String::new(), |(_, _, _, tag)| tag.clone()),
            node_uuid: hex_uuid(&node),
            observer_id: observer_id.clone(),
            observer_kind: kind.to_string(),
            observer_name: name,
            last_seen: last_seen
                .get(&(node.clone(), observer_id.clone()))
                .copied()
                .unwrap_or(0),
            sink,
        });
    }
    Ok(paths)
}

pub fn connection_path_json(path: &ConnectionPath) -> Value {
    json!({
        "server_id": path.server_id,
        "server_name": path.server_name,
        "display_index": path.display_index,
        "tag": path.tag,
        "node_uuid": path.node_uuid,
        "observer_id": path.observer_id,
        "observer_kind": path.observer_kind,
        "observer_name": path.observer_name,
        "assigned": true,
        "last_seen": optional_rfc3339(path.last_seen),
        "sink": {
            "connected": path.sink.connected,
            "pending_events": path.sink.pending_events,
            "last_error": path.sink.last_error,
            "ack_through": path.sink.ack_through,
            "last_rtt_ms": if path.sink.rtt_sampled_at > 0 { json!(path.sink.last_rtt_ms) } else { Value::Null },
            "rtt_sampled_at": optional_rfc3339(path.sink.rtt_sampled_at),
        },
    })
}

use serde_json::{Value, json};

/// Go `ListConnectionLatency`（桶 1 分钟，`bucket_start DESC`）。
pub fn list_connection_latency(
    conn: &Connection,
    kind: Option<&str>,
    collector_id: Option<&str>,
    observer_id: Option<&str>,
    server_id: Option<i64>,
    offset: i64,
    limit: i64,
) -> anyhow::Result<(Vec<Value>, i64)> {
    let mut conditions: Vec<String> = Vec::new();
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(kind) = kind.filter(|value| !value.is_empty()) {
        params.push(Box::new(kind.to_string()));
        conditions.push(format!("b.kind = ?{}", params.len()));
    }
    if let Some(collector_id) = collector_id.filter(|value| !value.is_empty()) {
        params.push(Box::new(collector_id.to_string()));
        conditions.push(format!("b.collector_uuid = ?{}", params.len()));
    }
    if let Some(observer_id) = observer_id.filter(|value| !value.is_empty()) {
        params.push(Box::new(observer_id.to_string()));
        conditions.push(format!("b.observer_id = ?{}", params.len()));
    }
    if let Some(server_id) = server_id.filter(|id| *id > 0) {
        let node: Option<Vec<u8>> = conn
            .query_row(
                "SELECT node_uuid FROM server_node_bindings WHERE server_id = ?1 AND current = 1 LIMIT 1",
                [server_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        match node {
            Some(node) => {
                params.push(Box::new(node));
                conditions.push(format!("b.node_uuid = ?{}", params.len()));
            }
            None => return Ok((Vec::new(), 0)),
        }
    }
    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };
    let total: i64 = {
        let sql = format!("SELECT COUNT(*) FROM connection_latency_buckets b {where_clause}");
        let mut stmt = conn.prepare(&sql)?;
        let refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|boxed| boxed.as_ref()).collect();
        stmt.query_row(refs.as_slice(), |row| row.get(0))?
    };
    // limit / offset 同为动态值，必须并入 params：直插文本会绕过类型检查。
    params.push(Box::new(limit.max(1)));
    let limit_index = params.len();
    params.push(Box::new(offset.max(0)));
    let offset_index = params.len();
    let sql = format!(
        "SELECT b.kind, b.collector_uuid, b.node_uuid, b.observer_id, b.bucket_start, \
         COALESCE(b.min_ms, 0), COALESCE(b.max_ms, 0), COALESCE(b.sum_ms, 0), b.count \
         FROM connection_latency_buckets b {where_clause} \
         ORDER BY b.bucket_start DESC LIMIT ?{limit_index} OFFSET ?{offset_index}"
    );
    let mut stmt = conn.prepare(&sql)?;
    let refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|boxed| boxed.as_ref()).collect();
    let rows = stmt
        .query_map(refs.as_slice(), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, f64>(5)?,
                row.get::<_, f64>(6)?,
                row.get::<_, f64>(7)?,
                row.get::<_, i64>(8)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    // node → server 批量解析：`server_id` 必须是行的真值（Go `idx.host(row.NodeUUID)`），
    // 回显过滤参数会让「按 A 服务器过滤」的响应里出现 B 的 server_id。
    let nodes: Vec<Vec<u8>> = rows.iter().map(|row| row.2.clone()).collect();
    let index = HostIndex::load(conn, &nodes, &[])?;
    let mut items = Vec::new();
    for (kind, collector_id, node, observer_id, bucket_start, min, max, sum, count) in rows {
        let (server_id, server_name) = match index.server(&node) {
            Some((id, name, _, _)) => (id, name),
            None => (0, String::new()),
        };
        let node_hex = hex_uuid(&node);
        let zero_node = node_hex == "00000000000000000000000000000000";
        items.push(json!({
            "kind": kind,
            "collector_id": collector_id,
            "server_id": server_id,
            "server_name": server_name,
            "node_uuid": if zero_node { Value::String(String::new()) } else { Value::String(node_hex) },
            "observer_id": observer_id,
            "bucket_start": optional_rfc3339(bucket_start),
            "min_ms": min,
            "avg_ms": if count > 0 { sum / count as f64 } else { 0.0 },
            "max_ms": max,
            "count": count,
        }));
    }
    Ok((items, total))
}

// ---------- datasets ----------

fn evidence_annotate(conn: &Connection, blob: Option<Vec<u8>>) -> anyhow::Result<Vec<Value>> {
    let Some(blob) = blob.filter(|blob| !blob.is_empty()) else {
        return Ok(Vec::new());
    };
    let Ok(raw) = serde_json::from_slice::<Vec<Value>>(&blob) else {
        return Ok(Vec::new());
    };
    let mut observer_ids: Vec<String> = Vec::new();
    for item in &raw {
        if let Some(id) = item.get("observer_id").and_then(Value::as_str)
            && !id.is_empty()
        {
            observer_ids.push(id.to_string());
        }
    }
    let index = HostIndex::load(conn, &[], &observer_ids)?;
    let mut out = Vec::new();
    for item in raw {
        let observer_id = item
            .get("observer_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let (kind, name) = index.observer(&observer_id);
        out.push(json!({
            "observer_id": observer_id,
            "observer_kind": kind,
            "observer_name": name,
            "healthy": item.get("healthy").and_then(Value::as_bool).unwrap_or(false),
            "seen": item.get("seen").and_then(Value::as_bool).unwrap_or(false),
        }));
    }
    Ok(out)
}

/// `assignments` dataset（Go `ListObserverAssignments`：valid_to=0，按 valid_from DESC）。
pub fn list_assignments(
    conn: &Connection,
    offset: i64,
    limit: i64,
) -> anyhow::Result<(Vec<Value>, i64)> {
    let total: i64 = conn.query_row(
        "SELECT COUNT(*) FROM observer_assignments WHERE valid_to = 0",
        [],
        |row| row.get(0),
    )?;
    let mut stmt = conn.prepare(
        "SELECT node_uuid, observer_id, valid_from, valid_to, generation, config_version \
         FROM observer_assignments WHERE valid_to = 0 ORDER BY valid_from DESC LIMIT ?1 OFFSET ?2",
    )?;
    let rows: Vec<(Vec<u8>, String, i64, i64, i64, i64)> = stmt
        .query_map([limit, offset], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let nodes: Vec<Vec<u8>> = rows.iter().map(|(node, ..)| node.clone()).collect();
    let observers: Vec<String> = rows
        .iter()
        .map(|(_, observer, ..)| observer.clone())
        .collect();
    let index = HostIndex::load(conn, &nodes, &observers)?;
    let mut items = Vec::new();
    for (node, observer_id, valid_from, valid_to, generation, config_version) in rows {
        let server = index.server(&node);
        let (kind, name) = index.observer(&observer_id);
        items.push(json!({
            "server_id": server.as_ref().map_or(0, |(id, ..)| *id),
            "server_name": server
                .as_ref()
                .map_or(String::new(), |(_, name, ..)| name.clone()),
            "node_uuid": hex_uuid(&node),
            "observer_id": observer_id,
            "observer_kind": kind,
            "observer_name": name,
            "valid_from": optional_rfc3339(valid_from),
            "valid_to": optional_rfc3339(valid_to),
            "generation": generation,
            "config_version": config_version,
        }));
    }
    Ok((items, total))
}

/// `agents` dataset（Go `ListAgentReliability`：全表按 updated_at DESC）。
pub fn list_agents(
    conn: &Connection,
    offset: i64,
    limit: i64,
) -> anyhow::Result<(Vec<Value>, i64)> {
    let total: i64 =
        conn.query_row("SELECT COUNT(*) FROM agent_telemetry_runtimes", [], |row| {
            row.get(0)
        })?;
    let mut stmt = conn.prepare(
        "SELECT node_uuid, wal_pressure, COALESCE(wal_bytes, 0), COALESCE(pending_events, 0), \
         COALESCE(oldest_pending, 0), sink_cursors, COALESCE(clock_untrusted, 0), \
         COALESCE(protocol_version, ''), COALESCE(agent_version, ''), updated_at \
         FROM agent_telemetry_runtimes ORDER BY updated_at DESC LIMIT ?1 OFFSET ?2",
    )?;
    let rows: Vec<(
        Vec<u8>,
        i64,
        i64,
        i64,
        i64,
        Vec<u8>,
        bool,
        String,
        String,
        Option<String>,
    )> = stmt
        .query_map([limit, offset], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get::<_, i64>(6)? != 0,
                row.get(7)?,
                row.get(8)?,
                row.get(9)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let nodes: Vec<Vec<u8>> = rows.iter().map(|(node, ..)| node.clone()).collect();
    let mut observer_ids: Vec<String> = Vec::new();
    let decoded_agents: Vec<Option<proto::AgentRuntime>> = rows
        .iter()
        .map(|(.., cursors, _, _, _, _)| {
            let decoded = proto::AgentRuntime::decode(cursors.as_slice()).ok();
            if let Some(ref agent) = decoded {
                for sink in &agent.sinks {
                    if !sink.endpoint_id.is_empty() && !observer_ids.contains(&sink.endpoint_id) {
                        observer_ids.push(sink.endpoint_id.clone());
                    }
                }
            }
            decoded
        })
        .collect();
    let index = HostIndex::load(conn, &nodes, &observer_ids)?;
    let mut items = Vec::new();
    for (
        (
            node,
            wal_pressure,
            wal_bytes,
            pending_events,
            oldest_pending,
            _cursors,
            clock_untrusted,
            protocol_version,
            agent_version,
            updated_at,
        ),
        decoded_agent,
    ) in rows.into_iter().zip(decoded_agents)
    {
        let server = index.server(&node);
        let mut sinks = Vec::new();
        if let Some(agent) = decoded_agent {
            for sink in agent.sinks {
                let (obs_kind, obs_name) = index.observer(&sink.endpoint_id);
                let observer_kind = if obs_kind.is_empty() {
                    Value::Null
                } else {
                    Value::String(obs_kind.to_string())
                };
                let observer_name = if obs_name.is_empty() {
                    Value::Null
                } else {
                    Value::String(obs_name)
                };
                sinks.push(json!({
                    "endpoint_id": sink.endpoint_id,
                    "observer_kind": observer_kind,
                    "observer_name": observer_name,
                    "connected": sink.connected,
                    "pending_events": sink.pending_events,
                    "last_error": sink.last_error,
                    "ack_through": sink.ack_through,
                    "last_rtt_ms": if sink.rtt_sampled_at_unix_nano > 0 {
                        json!(sink.last_rtt_ms)
                    } else {
                        Value::Null
                    },
                    "rtt_sampled_at": optional_rfc3339(sink.rtt_sampled_at_unix_nano),
                }));
            }
        }
        items.push(json!({
            "server_id": server.as_ref().map_or(0, |(id, ..)| *id),
            "server_name": server
                .as_ref()
                .map_or(String::new(), |(_, name, ..)| name.clone()),
            "node_uuid": hex_uuid(&node),
            "wal_pressure": wal_pressure_label(wal_pressure as i32),
            "wal_bytes": wal_bytes,
            "pending_events": pending_events,
            "oldest_pending": optional_rfc3339(oldest_pending),
            "clock_untrusted": clock_untrusted,
            "protocol_version": protocol_version,
            "agent_version": agent_version,
            "updated_at": updated_at.as_deref().and_then(util_time::sqlite_to_unix_nanos).map(|nanos| Value::String(util_time::unix_nanos_to_rfc3339(nanos))).unwrap_or(Value::Null),
            "sinks": sinks,
        }));
    }
    Ok((items, total))
}

/// `incidents` dataset（`open` 过滤可选，按 started_at DESC）。
pub fn list_incidents(
    conn: &Connection,
    open_only: bool,
    offset: i64,
    limit: i64,
) -> anyhow::Result<(Vec<Value>, i64)> {
    let where_clause = if open_only { "WHERE ended_at = 0" } else { "" };
    let total: i64 = {
        let sql = format!("SELECT COUNT(*) FROM availability_incidents {where_clause}");
        conn.query_row(&sql, [], |row| row.get(0))?
    };
    let sql = format!(
        "SELECT id, node_uuid, COALESCE(initial_classification, ''), current_classification, \
         revision, started_at, COALESCE(ended_at, 0), COALESCE(recalculated_at, 0), \
         COALESCE(reason, ''), observer_evidence \
         FROM availability_incidents {where_clause} ORDER BY started_at DESC LIMIT ?1 OFFSET ?2"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows: Vec<(
        i64,
        Vec<u8>,
        String,
        String,
        i64,
        i64,
        i64,
        i64,
        String,
        Option<Vec<u8>>,
    )> = stmt
        .query_map([limit, offset], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
                row.get(9)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let nodes: Vec<Vec<u8>> = rows.iter().map(|(_, node, ..)| node.clone()).collect();
    let index = HostIndex::load(conn, &nodes, &[])?;
    let mut items = Vec::new();
    for (id, node, initial, current, revision, started, ended, recalculated, reason, evidence) in
        rows
    {
        let server = index.server(&node);
        items.push(json!({
            "id": id,
            "server_id": server.as_ref().map_or(0, |(id, ..)| *id),
            "server_name": server
                .as_ref()
                .map_or(String::new(), |(_, name, ..)| name.clone()),
            "node_uuid": hex_uuid(&node),
            "initial_classification": initial,
            "current_classification": current,
            "revision": revision,
            "started_at": optional_rfc3339(started),
            "ended_at": optional_rfc3339(ended),
            "recalculated_at": optional_rfc3339(recalculated),
            "reason": reason,
            "observer_evidence": evidence_annotate(conn, evidence)?,
        }));
    }
    Ok((items, total))
}

/// `incident-revisions` dataset（按 created_at DESC）。
pub fn list_incident_revisions(
    conn: &Connection,
    offset: i64,
    limit: i64,
) -> anyhow::Result<(Vec<Value>, i64)> {
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM incident_revisions", [], |row| {
        row.get(0)
    })?;
    let mut stmt = conn.prepare(
        "SELECT id, incident_id, revision, classification, COALESCE(reason, ''), \
         recalculated_at, created_at, evidence \
         FROM incident_revisions ORDER BY created_at DESC LIMIT ?1 OFFSET ?2",
    )?;
    let rows: Vec<(
        i64,
        i64,
        i64,
        String,
        String,
        i64,
        Option<String>,
        Option<Vec<u8>>,
    )> = stmt
        .query_map([limit, offset], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut items = Vec::new();
    for (id, incident_id, revision, classification, reason, recalculated, created, evidence) in rows
    {
        items.push(json!({
            "id": id,
            "incident_id": incident_id,
            "revision": revision,
            "classification": classification,
            "reason": reason,
            "recalculated_at": optional_rfc3339(recalculated),
            "created_at": created.as_deref().and_then(util_time::sqlite_to_unix_nanos).map(|nanos| Value::String(util_time::unix_nanos_to_rfc3339(nanos))).unwrap_or(Value::Null),
            "observer_evidence": evidence_annotate(conn, evidence)?,
        }));
    }
    Ok((items, total))
}

/// `data-loss` dataset（按 occurred_at DESC）。
pub fn list_data_loss(
    conn: &Connection,
    offset: i64,
    limit: i64,
) -> anyhow::Result<(Vec<Value>, i64)> {
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM telemetry_data_losses", [], |row| {
        row.get(0)
    })?;
    let mut stmt = conn.prepare(
        "SELECT fact_id, COALESCE(component_id, ''), occurred_at, reason, \
         COALESCE(first_spool_id, 0), COALESCE(last_spool_id, 0), COALESCE(lost_records, 0), \
         COALESCE(detail, ''), created_at \
         FROM telemetry_data_losses ORDER BY occurred_at DESC LIMIT ?1 OFFSET ?2",
    )?;
    let rows: Vec<(
        Vec<u8>,
        String,
        i64,
        i64,
        i64,
        i64,
        i64,
        String,
        Option<String>,
    )> = stmt
        .query_map([limit, offset], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut items = Vec::new();
    for (
        fact_id,
        component_id,
        occurred_at,
        reason,
        first_spool,
        last_spool,
        lost_records,
        detail,
        created,
    ) in rows
    {
        items.push(json!({
            "fact_id": hex_uuid(&fact_id),
            "component_id": component_id,
            "occurred_at": optional_rfc3339(occurred_at),
            "reason": gap_reason_label(reason),
            "first_spool_id": first_spool,
            "last_spool_id": last_spool,
            "lost_records": lost_records,
            "detail": detail,
            "created_at": created.as_deref().and_then(util_time::sqlite_to_unix_nanos).map(|nanos| Value::String(util_time::unix_nanos_to_rfc3339(nanos))).unwrap_or(Value::Null),
        }));
    }
    Ok((items, total))
}

/// `alerts` dataset（按 occurred_at DESC）。
pub fn list_alerts(
    conn: &Connection,
    offset: i64,
    limit: i64,
) -> anyhow::Result<(Vec<Value>, i64)> {
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM telemetry_alerts", [], |row| {
        row.get(0)
    })?;
    let mut stmt = conn.prepare(
        "SELECT id, alert_type, COALESCE(severity, ''), node_uuid, COALESCE(component_id, ''), \
         occurred_at, created_at, COALESCE(notified, 0), COALESCE(message, ''), dedup_key \
         FROM telemetry_alerts ORDER BY occurred_at DESC LIMIT ?1 OFFSET ?2",
    )?;
    let rows: Vec<(
        i64,
        String,
        String,
        Option<Vec<u8>>,
        String,
        i64,
        Option<String>,
        bool,
        String,
        String,
    )> = stmt
        .query_map([limit, offset], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get::<_, i64>(7)? != 0,
                row.get(8)?,
                row.get(9)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let nodes: Vec<Vec<u8>> = rows.iter().filter_map(|row| row.3.clone()).collect();
    let index = HostIndex::load(conn, &nodes, &[])?;
    let mut items = Vec::new();
    for (
        id,
        alert_type,
        severity,
        node,
        component_id,
        occurred_at,
        created,
        notified,
        message,
        dedup_key,
    ) in rows
    {
        let server = node.as_ref().and_then(|node| index.server(node));
        items.push(json!({
            "id": id,
            "alert_type": alert_type,
            "severity": severity,
            "server_id": server.as_ref().map_or(0, |(id, ..)| *id),
            "server_name": server
                .as_ref()
                .map_or(String::new(), |(_, name, ..)| name.clone()),
            "node_uuid": node.as_ref().map(|node| Value::String(hex_uuid(node))).unwrap_or(Value::Null),
            "component_id": component_id,
            "occurred_at": optional_rfc3339(occurred_at),
            "created_at": created.as_deref().and_then(util_time::sqlite_to_unix_nanos).map(|nanos| Value::String(util_time::unix_nanos_to_rfc3339(nanos))).unwrap_or(Value::Null),
            "notified": notified,
            "message": message,
            "dedup_key": dedup_key,
        }));
    }
    Ok((items, total))
}

// ---------- offline history / monitors history / availability ----------

/// `server_offline_histories` 全列行（snake_case 原文输出给前端）。
pub fn list_offline_history(
    conn: &Connection,
    server_id: i64,
    offset: i64,
    limit: i64,
) -> anyhow::Result<(Vec<Value>, i64)> {
    let total: i64 = conn.query_row(
        "SELECT COUNT(*) FROM server_offline_histories WHERE server_id = ?1 AND deleted_at IS NULL",
        [server_id],
        |row| row.get(0),
    )?;
    let mut stmt = conn.prepare(
        "SELECT id, created_at, updated_at, deleted_at, server_id, started_at, detected_at, \
         ended_at, duration_seconds, reason, status, threshold_seconds, last_seen_at, \
         last_boot_time, last_uptime, last_ip, recovered_at, recovered_boot_time, \
         recovered_uptime, recovered_ip, note \
         FROM server_offline_histories WHERE server_id = ?1 AND deleted_at IS NULL \
         ORDER BY started_at DESC LIMIT ?2 OFFSET ?3",
    )?;
    let rows = stmt.query_map(rusqlite::params![server_id, limit, offset], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<i64>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<String>>(10)?,
            row.get::<_, Option<i64>>(11)?,
            row.get::<_, Option<String>>(12)?,
            row.get::<_, Option<i64>>(13)?,
            row.get::<_, Option<i64>>(14)?,
            row.get::<_, Option<String>>(15)?,
            row.get::<_, Option<String>>(16)?,
            row.get::<_, Option<i64>>(17)?,
            row.get::<_, Option<i64>>(18)?,
            row.get::<_, Option<String>>(19)?,
            row.get::<_, Option<String>>(20)?,
        ))
    })?;
    fn dt(value: Option<String>) -> Value {
        value
            .as_deref()
            .and_then(util_time::sqlite_to_unix_nanos)
            .map(|nanos| Value::String(util_time::unix_nanos_to_rfc3339(nanos)))
            .unwrap_or(Value::Null)
    }
    let mut items = Vec::new();
    for row in rows {
        let (
            id,
            created,
            updated,
            deleted,
            server_id,
            started,
            detected,
            ended,
            duration,
            reason,
            status,
            threshold,
            last_seen,
            last_boot,
            last_uptime,
            last_ip,
            recovered_at,
            recovered_boot,
            recovered_uptime,
            recovered_ip,
            note,
        ) = row?;
        items.push(json!({
            "id": id, "created_at": dt(created), "updated_at": dt(updated),
            "deleted_at": dt(deleted), "server_id": server_id,
            "started_at": dt(started), "detected_at": dt(detected), "ended_at": dt(ended),
            "duration_seconds": duration, "reason": reason, "status": status,
            "threshold_seconds": threshold, "last_seen_at": dt(last_seen),
            "last_boot_time": last_boot, "last_uptime": last_uptime, "last_ip": last_ip,
            "recovered_at": dt(recovered_at), "recovered_boot_time": recovered_boot,
            "recovered_uptime": recovered_uptime, "recovered_ip": recovered_ip, "note": note,
        }));
    }
    Ok((items, total))
}

/// 清理已结束的离线历史（Go `CleanupOfflineHistory`：只删已结束区间）。
pub fn cleanup_offline_history(
    conn: &Connection,
    server_id: Option<i64>,
    before_nanos: i64,
) -> anyhow::Result<usize> {
    let before_text = util_time::unix_nanos_to_sqlite(before_nanos);
    match server_id.filter(|id| *id > 0) {
        Some(server_id) => conn.execute(
            "DELETE FROM server_offline_histories \
             WHERE ended_at IS NOT NULL AND ended_at < ?2 AND server_id = ?1",
            rusqlite::params![server_id, before_text],
        ),
        None => conn.execute(
            "DELETE FROM server_offline_histories WHERE ended_at IS NOT NULL AND ended_at < ?1",
            [&before_text],
        ),
    }
    .map_err(Into::into)
}

/// `GET /v2/admin/monitors/:id/history`（及 `/histories`；游标分页：`mh.id < cursor`，id DESC）。
pub fn list_monitor_history(
    conn: &Connection,
    monitor_id: i64,
    from_nanos: Option<i64>,
    to_nanos: Option<i64>,
    cursor: Option<i64>,
    limit: i64,
) -> anyhow::Result<(Vec<Value>, String)> {
    let mut conditions = vec!["mh.monitor_id = ?1".to_string()];
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(monitor_id)];
    // limit 必须 ≥1：下方 `rows[limit - 1]` 与 `take(limit)` 都以它为准。
    let limit = limit.max(1);
    if let Some(from) = from_nanos {
        // `created_at` 由 `CURRENT_TIMESTAMP` 写成无后缀串，直接与
        // `unix_nanos_to_sqlite` 的 `…+00:00` 混比会在同秒边界漏行，故归一化。
        params.push(Box::new(util_time::sqlite_to_datetime_text(
            &util_time::unix_nanos_to_sqlite(from),
        )));
        conditions.push(format!("mh.created_at >= ?{}", params.len()));
    }
    if let Some(to) = to_nanos {
        params.push(Box::new(util_time::sqlite_to_datetime_text(
            &util_time::unix_nanos_to_sqlite(to),
        )));
        conditions.push(format!("mh.created_at <= ?{}", params.len()));
    }
    if let Some(cursor) = cursor.filter(|value| *value > 0) {
        params.push(Box::new(cursor));
        conditions.push(format!("mh.id < ?{}", params.len()));
    }
    let where_clause = conditions.join(" AND ");
    let sql = format!(
        "SELECT mh.id, mh.created_at, mh.updated_at, mh.deleted_at, mh.monitor_id, mh.server_id, \
         COALESCE(s.name, ''), COALESCE(mh.avg_delay, 0), COALESCE(mh.up, 0), COALESCE(mh.down, 0), \
         COALESCE(mh.data, '') \
         FROM monitor_histories mh LEFT JOIN servers s ON s.id = mh.server_id \
         WHERE {where_clause} ORDER BY mh.id DESC LIMIT ?"
    );
    let mut stmt = conn.prepare(&sql)?;
    params.push(Box::new(limit + 1));
    let refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|boxed| boxed.as_ref()).collect();
    let rows = stmt
        .query_map(refs.as_slice(), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, f64>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, i64>(9)?,
                row.get::<_, String>(10)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = rows.len() as i64 > limit;
    let next_cursor = if has_more {
        rows.get(limit as usize - 1)
            .map(|row| row.0.to_string())
            .unwrap_or_default()
    } else {
        String::new()
    };
    fn dt(value: Option<String>) -> Value {
        value
            .as_deref()
            .and_then(util_time::sqlite_to_unix_nanos)
            .map(|nanos| Value::String(util_time::unix_nanos_to_rfc3339(nanos)))
            .unwrap_or(Value::Null)
    }
    let items = rows
        .into_iter()
        .take(limit as usize)
        .map(
            |(
                id,
                created,
                updated,
                deleted,
                monitor_id,
                server_id,
                server_name,
                avg_delay,
                up,
                down,
                data,
            )| {
                json!({
                    "id": id, "created_at": dt(created), "updated_at": dt(updated),
                    "deleted_at": dt(deleted), "monitor_id": monitor_id, "server_id": server_id,
                    "server_name": server_name,
                    "avg_delay": avg_delay, "up": up, "down": down, "data": data,
                })
            },
        )
        .collect();
    Ok((items, next_cursor))
}

/// admin 可用性桶列表（Go `v2AdminServerAvailability`：最新 current 绑定的
/// 桶序列 + 证据标注；`next_cursor` = 末桶 bucket_start 纳秒）。
pub fn list_admin_availability(
    conn: &Connection,
    server_id: i64,
    from_nanos: Option<i64>,
    to_nanos: Option<i64>,
    cursor: Option<i64>,
    limit: i64,
) -> anyhow::Result<(Vec<Value>, String)> {
    let node: Option<Vec<u8>> = conn
        .query_row(
            "SELECT node_uuid FROM server_node_bindings WHERE server_id = ?1 AND current = 1 \
             ORDER BY valid_from DESC LIMIT 1",
            [server_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    let Some(node) = node else {
        return Ok((Vec::new(), String::new()));
    };
    let node_for_hex = node.clone();
    let mut conditions = vec!["node_uuid = ?1".to_string()];
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(node)];
    if let Some(from) = from_nanos {
        params.push(Box::new(from));
        conditions.push(format!("bucket_start >= ?{}", params.len()));
    }
    if let Some(to) = to_nanos {
        params.push(Box::new(to));
        conditions.push(format!("bucket_start <= ?{}", params.len()));
    }
    if let Some(cursor) = cursor.filter(|value| *value > 0) {
        params.push(Box::new(cursor));
        conditions.push(format!("bucket_start < ?{}", params.len()));
    }
    let where_clause = conditions.join(" AND ");
    let sql = format!(
        "SELECT bucket_start, window_end, host_state, connectivity_state, expected_observers, \
         healthy_observers, seen_observers, observer_summary, revision, finalized, \
         COALESCE(recalculated_at, 0), COALESCE(resolution, '30s') \
         FROM availability_buckets WHERE {where_clause} ORDER BY bucket_start DESC LIMIT ?"
    );
    let mut stmt = conn.prepare(&sql)?;
    params.push(Box::new(limit + 1));
    let refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|boxed| boxed.as_ref()).collect();
    let rows = stmt
        .query_map(refs.as_slice(), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, Option<Vec<u8>>>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, i64>(9)? != 0,
                row.get::<_, i64>(10)?,
                row.get::<_, String>(11)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = rows.len() as i64 > limit;
    let next_cursor = if has_more {
        rows.get(limit as usize - 1)
            .map(|row| row.0.to_string())
            .unwrap_or_default()
    } else {
        String::new()
    };
    let mut items = Vec::new();
    for (
        bucket_start,
        window_end,
        host,
        connectivity,
        expected,
        healthy,
        seen,
        evidence,
        revision,
        finalized,
        recalculated,
        resolution,
    ) in rows.into_iter().take(limit as usize)
    {
        let available = match connectivity.as_str() {
            "full" | "partial" => json!(true),
            "unavailable" => json!(false),
            _ => Value::Null,
        };
        items.push(json!({
            "node_uuid": hex_uuid(&node_for_hex),
            "bucket_start": util_time::unix_nanos_to_rfc3339(bucket_start),
            "host": host,
            "connectivity": connectivity,
            "available": available,
            "coverage": if expected == 0 { "unknown".to_string() } else { format!("{seen}/{expected}") },
            "expected_observers": expected,
            "healthy_observers": healthy,
            "seen_observers": seen,
            "observer_evidence": evidence_annotate(conn, evidence)?,
            "revision": revision,
            "finalized": finalized,
            "recalculated_at": optional_rfc3339(recalculated),
            "window_end": if window_end > bucket_start { optional_rfc3339(window_end) } else { Value::Null },
            "resolution": resolution,
        }));
    }
    Ok((items, next_cursor))
}

// ---------- probes（读侧；样本写入属监控派发引擎，表当前为空） ----------

/// 探测型从端 + 最近心跳在线状态（Go `CollectorStatus` 90s 规则）。
fn probe_collector_status(
    conn: &Connection,
    now_nanos: i64,
) -> anyhow::Result<Vec<(String, String, &'static str)>> {
    let mut stmt = conn.prepare(
        "SELECT c.collector_uuid, c.name, r.last_seen FROM collectors c \
         LEFT JOIN collector_runtimes r ON r.collector_uuid = c.collector_uuid \
         WHERE c.deleted = 0 AND c.revoked = 0 AND c.kind = 'probe'",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<i64>>(2)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (uuid, name, last_seen) = row?;
        let status = match last_seen {
            Some(seen) if now_nanos - seen <= COLLECTOR_TIMEOUT_NANOS => "online",
            Some(_) => "offline",
            None => "unknown",
        };
        out.push((uuid, name, status));
    }
    Ok(out)
}

/// Go `LoadProbeSummary`。无有效目标的路径计入 `paths_no_target`。
pub fn load_probe_summary(conn: &Connection, now_nanos: i64) -> anyhow::Result<Value> {
    let collectors = probe_collector_status(conn, now_nanos)?;
    let total = collectors.len() as i64;
    let online = collectors.iter().filter(|(.., s)| *s == "online").count() as i64;
    let offline = collectors.iter().filter(|(.., s)| *s == "offline").count() as i64;
    let unknown = collectors.iter().filter(|(.., s)| *s == "unknown").count() as i64;
    let mut paths_assigned = 0i64;
    let mut paths_no_target = 0i64;
    // 服务器清单与 collector 能力/范围都只查一次：旧实现在 collector 循环里
    // 逐端重跑「全表 servers」查询，O(collector × server) 行 + 2×collector 条语句。
    let servers: Vec<(i64, String, String, bool, bool, bool)> = {
        let mut stmt = conn.prepare(
            "SELECT s.id, COALESCE(s.tag, ''), COALESCE(r.last_ip, ''), \
             COALESCE(s.probe_enable_icmp, 1), COALESCE(s.probe_enable_tcp, 1), \
             COALESCE(s.probe_enable_mtr, 1) \
             FROM servers s \
             LEFT JOIN server_runtimes r ON r.server_id = s.id \
             WHERE s.deleted_at IS NULL",
        )?;
        let mapped = stmt.query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get::<_, i64>(3)? != 0,
                row.get::<_, i64>(4)? != 0,
                row.get::<_, i64>(5)? != 0,
            ))
        })?;
        mapped.collect::<Result<Vec<_>, _>>()?
    };
    let probe_targets: Vec<(i64, String)> = {
        let mut stmt = conn.prepare(
            "SELECT id, COALESCE(probe_target, '') FROM servers WHERE deleted_at IS NULL",
        )?;
        let mapped = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        mapped.collect::<Result<Vec<_>, _>>()?
    };
    let target_of: std::collections::HashMap<i64, String> = probe_targets.into_iter().collect();
    let mut capabilities: std::collections::HashMap<String, (bool, bool, bool, bool, bool)> =
        std::collections::HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT collector_uuid, enable_icmp, enable_tcp, enable_mtr, enable_ipv4, enable_ipv6 \
             FROM collectors",
        )?;
        let mapped = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                (
                    row.get::<_, i64>(1)? != 0,
                    row.get::<_, i64>(2)? != 0,
                    row.get::<_, i64>(3)? != 0,
                    row.get::<_, i64>(4)? != 0,
                    row.get::<_, i64>(5)? != 0,
                ),
            ))
        })?;
        for row in mapped {
            let (uuid, caps) = row?;
            capabilities.insert(uuid, caps);
        }
    }
    let mut scopes_by_collector: std::collections::HashMap<String, Vec<(String, String)>> =
        std::collections::HashMap::new();
    {
        let mut stmt =
            conn.prepare("SELECT collector_uuid, scope_type, scope_value FROM collector_scopes")?;
        let mapped = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                (row.get::<_, String>(1)?, row.get::<_, String>(2)?),
            ))
        })?;
        for row in mapped {
            let (uuid, scope) = row?;
            scopes_by_collector.entry(uuid).or_default().push(scope);
        }
    }
    let mut assigned_pairs: std::collections::HashSet<(String, i64)> =
        std::collections::HashSet::new();
    for (uuid, _, _) in &collectors {
        let (enable_icmp, enable_tcp, enable_mtr, enable_ipv4, enable_ipv6) = capabilities
            .get(uuid)
            .copied()
            .unwrap_or((false, false, false, false, false));
        let scopes = scopes_by_collector
            .get(uuid)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        for (server_id, tag, last_ip, server_icmp, server_tcp, server_mtr) in &servers {
            if !scopes_include_server(scopes, &server_id.to_string(), tag) {
                continue;
            }
            paths_assigned += 1;
            assigned_pairs.insert((uuid.clone(), *server_id));
            let probe_target = target_of.get(server_id).map(String::as_str).unwrap_or("");
            let target = probe_ingest::apply_ip_families(
                probe_ingest::resolve_probe_target(probe_target, last_ip),
                enable_ipv4,
                enable_ipv6,
            );
            let active = (enable_icmp && *server_icmp)
                || (enable_tcp && *server_tcp)
                || (enable_mtr && *server_mtr);
            if !active || target.source == "none" {
                paths_no_target += 1;
            }
        }
    }
    let mut reachable = 0i64;
    let mut down = 0i64;
    if !assigned_pairs.is_empty() {
        let mut stmt = conn.prepare(
            "SELECT collector_uuid, server_id, COALESCE(reachable, 0), COALESCE(sampled_at, 0) \
             FROM probe_latests",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        for row in rows {
            let (uuid, sid, reach_flag, sampled_at) = row?;
            if !assigned_pairs.contains(&(uuid, sid)) {
                continue;
            }
            if reach_flag != 0 {
                reachable += 1;
            } else if sampled_at > 0 {
                down += 1;
            }
        }
    }
    Ok(json!({
        "collectors_total": total,
        "collectors_online": online,
        "collectors_offline": offline,
        "collectors_unknown": unknown,
        "paths_assigned": paths_assigned,
        "paths_reachable": reachable,
        "paths_down": down,
        "paths_no_target": paths_no_target,
    }))
}

fn scopes_include_server(scopes: &[(String, String)], server_id: &str, tag: &str) -> bool {
    crate::repo::bindings::scopes_include_server(scopes, server_id, tag)
}

struct ProbePathServer {
    id: i64,
    name: String,
    tag: String,
    display_index: i64,
    probe_target: String,
    last_ip: String,
    enable_icmp: bool,
    enable_tcp: bool,
    enable_mtr: bool,
}

/// collector 能力开关（Go `Collector.EnableICMP` 等）。
type CollectorCaps = (bool, bool, bool, bool, bool);
/// `probe_latests` 一行：按 (collector_uuid, server_id) 批量预载。
type ProbeLatestRow = (
    Option<i64>,
    Option<i64>,
    Option<f64>,
    Option<String>,
    Option<i64>,
    Option<f64>,
    Option<f64>,
    Option<i64>,
    Option<i64>,
    Option<Vec<u8>>,
    Option<i64>,
);
/// `probe_traces` 一行：按 (collector_uuid, server_id) 批量预载。
type ProbeTraceRow = (
    Option<Vec<u8>>,
    Option<i64>,
    Option<Vec<u8>>,
    Option<i64>,
    Option<i64>,
);

/// Go `LoadProbePaths`：目标来自覆盖地址或运行态 IP，MTR 摘要取末跳丢包。
pub fn load_probe_paths(
    conn: &Connection,
    filter_collector: Option<&str>,
    filter_server_id: Option<i64>,
) -> anyhow::Result<Vec<Value>> {
    let now_nanos = util_time::now_nanos();
    let collectors = probe_collector_status(conn, now_nanos)?;
    let servers: Vec<ProbePathServer> = {
        let mut stmt = conn.prepare(
            "SELECT s.id, COALESCE(s.name, ''), COALESCE(s.tag, ''), COALESCE(s.display_index, 0), \
             COALESCE(s.probe_target, ''), COALESCE(r.last_ip, ''), \
             COALESCE(s.probe_enable_icmp, 1), COALESCE(s.probe_enable_tcp, 1), COALESCE(s.probe_enable_mtr, 1) \
             FROM servers s \
             LEFT JOIN server_runtimes r ON r.server_id = s.id \
             WHERE s.deleted_at IS NULL \
             ORDER BY s.display_index DESC, s.id ASC",
        )?;
        let mapped = stmt.query_map([], |row| {
            Ok(ProbePathServer {
                id: row.get(0)?,
                name: row.get(1)?,
                tag: row.get(2)?,
                display_index: row.get(3)?,
                probe_target: row.get(4)?,
                last_ip: row.get(5)?,
                enable_icmp: row.get::<_, i64>(6)? != 0,
                enable_tcp: row.get::<_, i64>(7)? != 0,
                enable_mtr: row.get::<_, i64>(8)? != 0,
            })
        })?;
        mapped.collect::<Result<Vec<_>, _>>()?
    };
    // collector 能力与范围一次性载入；旧实现在 collector 循环里逐端查询，
    // collector 数 × server 数就是语句数。
    let (capabilities, scopes_by_collector) = load_collector_caps_and_scopes(conn)?;
    // probe_latests / probe_traces 同样按 (collector, server) 批量预载：
    // 逐对查询是 O(collector × server) 次往返。
    let latests = load_probe_latests(conn, filter_collector)?;
    let traces = load_probe_traces(conn, filter_collector)?;
    let mut items = Vec::new();
    for (collector_id, collector_name, _status) in &collectors {
        if let Some(filter) = filter_collector.filter(|value| !value.is_empty())
            && filter != collector_id
        {
            continue;
        }
        let (enable_icmp, enable_tcp, enable_mtr, enable_ipv4, enable_ipv6) = capabilities
            .get(collector_id)
            .copied()
            .unwrap_or((false, false, false, false, false));
        let scopes = scopes_by_collector
            .get(collector_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        for server in &servers {
            if let Some(filter) = filter_server_id.filter(|id| *id > 0)
                && filter != server.id
            {
                continue;
            }
            if !scopes_include_server(scopes, &server.id.to_string(), &server.tag) {
                continue;
            }
            let server_id = server.id;
            let name = &server.name;
            let tag = &server.tag;
            let display_index = server.display_index;
            let mut target = probe_ingest::apply_ip_families(
                probe_ingest::resolve_probe_target(&server.probe_target, &server.last_ip),
                enable_ipv4,
                enable_ipv6,
            );
            let path_icmp = enable_icmp && server.enable_icmp;
            let path_tcp = enable_tcp && server.enable_tcp;
            let path_mtr = enable_mtr && server.enable_mtr;
            if !(path_icmp || path_tcp || path_mtr) || target.source == "none" {
                target = probe_ingest::ResolvedProbeTarget {
                    source: "none".to_string(),
                    hostname: String::new(),
                    ipv4: String::new(),
                    ipv6: String::new(),
                };
            }
            let latest = latests.get(&(collector_id.clone(), server_id)).cloned();
            let tcp = latest
                .as_ref()
                .and_then(|(.., tcp_json, _)| tcp_json.clone())
                .filter(|blob| !blob.is_empty())
                .and_then(|blob| serde_json::from_slice::<Value>(&blob).ok())
                .unwrap_or_else(|| Value::Array(Vec::new()));
            let mut item = json!({
                "server_id": server_id,
                "server_name": name,
                "display_index": display_index,
                "tag": tag,
                "collector_id": collector_id,
                "collector_name": collector_name,
                "target": {
                    "source": target.source,
                    "hostname": target.hostname,
                    "ipv4": target.ipv4,
                    "ipv6": target.ipv6,
                },
                "reachable": latest.as_ref().is_some_and(|row| row.1.is_some_and(|value| value != 0)),
                "has_trace": latest.as_ref().is_some_and(|row| row.10.is_some_and(|value| value != 0)),
                "last_error": latest.as_ref().and_then(|row| row.3.clone()).unwrap_or_default(),
                "enable_icmp": path_icmp,
                "enable_tcp": path_tcp,
                "tcp": tcp,
            });
            let latest_ref = latest.as_ref();
            let (icmp_ok, icmp_rtt, icmp_loss, icmp_sent, icmp_recv) = (
                latest_ref.map(|row| row.4),
                latest_ref.map(|row| row.5),
                latest_ref.map(|row| row.6),
                latest_ref.map(|row| row.7),
                latest_ref.map(|row| row.8),
            );
            if icmp_ok.is_some() {
                item["icmp"] = json!({
                    "ok": icmp_ok.flatten().is_some_and(|value| value != 0),
                    "rtt_ms": icmp_rtt.flatten(),
                    "loss": icmp_loss.flatten(),
                    "packets_sent": icmp_sent.flatten(),
                    "packets_received": icmp_recv.flatten(),
                });
            }
            let sampled_at = latest.as_ref().and_then(|row| row.0).unwrap_or(0);
            if sampled_at > 0 {
                item["sampled_at"] = optional_rfc3339(sampled_at);
                item["display_rtt_ms"] = json!(latest.as_ref().and_then(|row| row.2));
            }
            if path_mtr {
                let summary = traces
                    .get(&(collector_id.clone(), server_id))
                    .and_then(|row| mtr_summary_from_row(row, path_icmp, path_tcp));
                if let Some(summary) = summary {
                    let mut mtr = json!({
                        "loss": summary.loss,
                        "hop_count": summary.hop_count,
                        "sampled_at": optional_rfc3339(summary.sampled_at),
                        "protocol": summary.protocol,
                    });
                    if summary.port > 0 {
                        mtr["port"] = json!(summary.port);
                    }
                    item["mtr"] = mtr;
                    item["has_trace"] = json!(true);
                }
            }
            items.push(item);
        }
    }
    Ok(items)
}

fn load_collector_caps_and_scopes(
    conn: &Connection,
) -> anyhow::Result<(
    HashMap<String, CollectorCaps>,
    HashMap<String, Vec<(String, String)>>,
)> {
    let mut capabilities: HashMap<String, CollectorCaps> = HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT collector_uuid, enable_icmp, enable_tcp, enable_mtr, enable_ipv4, enable_ipv6 \
             FROM collectors",
        )?;
        let mapped = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                (
                    row.get::<_, i64>(1)? != 0,
                    row.get::<_, i64>(2)? != 0,
                    row.get::<_, i64>(3)? != 0,
                    row.get::<_, i64>(4)? != 0,
                    row.get::<_, i64>(5)? != 0,
                ),
            ))
        })?;
        for row in mapped {
            let (uuid, caps) = row?;
            capabilities.insert(uuid, caps);
        }
    }
    let mut scopes: HashMap<String, Vec<(String, String)>> = HashMap::new();
    {
        let mut stmt =
            conn.prepare("SELECT collector_uuid, scope_type, scope_value FROM collector_scopes")?;
        let mapped = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                (row.get::<_, String>(1)?, row.get::<_, String>(2)?),
            ))
        })?;
        for row in mapped {
            let (uuid, scope) = row?;
            scopes.entry(uuid).or_default().push(scope);
        }
    }
    Ok((capabilities, scopes))
}

fn load_probe_latests(
    conn: &Connection,
    filter_collector: Option<&str>,
) -> anyhow::Result<HashMap<(String, i64), ProbeLatestRow>> {
    let (sql, params): (String, Vec<Box<dyn rusqlite::ToSql>>) = match filter_collector
        .filter(|value| !value.is_empty())
    {
        Some(collector) => (
            "SELECT collector_uuid, server_id, sampled_at, reachable, display_rtt_ms, last_error, \
             icmp_ok, icmp_rtt_ms, icmp_loss, icmp_sent, icmp_recv, tcp_json, has_trace \
             FROM probe_latests WHERE collector_uuid = ?1"
                .to_string(),
            vec![Box::new(collector.to_string())],
        ),
        None => (
            "SELECT collector_uuid, server_id, sampled_at, reachable, display_rtt_ms, last_error, \
             icmp_ok, icmp_rtt_ms, icmp_loss, icmp_sent, icmp_recv, tcp_json, has_trace \
             FROM probe_latests"
                .to_string(),
            Vec::new(),
        ),
    };
    let mut stmt = conn.prepare(&sql)?;
    let refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|boxed| boxed.as_ref()).collect();
    let rows = stmt.query_map(refs.as_slice(), |row| {
        let collector: String = row.get(0)?;
        Ok((
            collector,
            row.get::<_, i64>(1)?,
            (
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
                row.get(9)?,
                row.get(10)?,
                row.get(11)?,
                row.get(12)?,
            ),
        ))
    })?;
    let mut out: HashMap<(String, i64), ProbeLatestRow> = HashMap::new();
    for row in rows {
        let (collector, server_id, value) = row?;
        out.insert((collector, server_id), value);
    }
    Ok(out)
}

fn load_probe_traces(
    conn: &Connection,
    filter_collector: Option<&str>,
) -> anyhow::Result<HashMap<(String, i64), ProbeTraceRow>> {
    let (sql, params): (String, Vec<Box<dyn rusqlite::ToSql>>) =
        match filter_collector.filter(|value| !value.is_empty()) {
            Some(collector) => (
                "SELECT collector_uuid, server_id, hops_json, sampled_at, tcp_hops_json, \
             tcp_sampled_at, tcp_port FROM probe_traces WHERE collector_uuid = ?1"
                    .to_string(),
                vec![Box::new(collector.to_string())],
            ),
            None => (
                "SELECT collector_uuid, server_id, hops_json, sampled_at, tcp_hops_json, \
             tcp_sampled_at, tcp_port FROM probe_traces"
                    .to_string(),
                Vec::new(),
            ),
        };
    let mut stmt = conn.prepare(&sql)?;
    let refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|boxed| boxed.as_ref()).collect();
    let rows = stmt.query_map(refs.as_slice(), |row| {
        let collector: String = row.get(0)?;
        Ok((
            collector,
            row.get::<_, i64>(1)?,
            (
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ),
        ))
    })?;
    let mut out: HashMap<(String, i64), ProbeTraceRow> = HashMap::new();
    for row in rows {
        let (collector, server_id, value) = row?;
        out.insert((collector, server_id), value);
    }
    Ok(out)
}

fn mtr_summary_from_row(
    row: &ProbeTraceRow,
    enable_icmp: bool,
    enable_tcp: bool,
) -> Option<probe_ingest::MtrSummary> {
    let (hops, sampled_at, tcp_hops, tcp_sampled_at, tcp_port) = row;
    let icmp = if enable_icmp {
        hops.as_deref()
            .and_then(|raw| probe_ingest::mtr_from_blob(raw, sampled_at.unwrap_or(0), "icmp", 0))
    } else {
        None
    };
    let tcp = if enable_tcp {
        let port = u32::try_from(tcp_port.unwrap_or(0)).unwrap_or(0);
        tcp_hops.as_deref().and_then(|raw| {
            probe_ingest::mtr_from_blob(raw, tcp_sampled_at.unwrap_or(0), "tcp", port)
        })
    } else {
        None
    };
    probe_ingest::select_mtr(icmp, tcp)
}

/// Go `ListProbeSamples`（probe_sample_buckets，bucket_start DESC）。
pub fn list_probe_samples(
    conn: &Connection,
    filter_collector: Option<&str>,
    filter_kind: Option<&str>,
    filter_server_id: Option<i64>,
    filter_port: Option<i64>,
    offset: i64,
    limit: i64,
) -> anyhow::Result<(Vec<Value>, i64)> {
    let mut conditions: Vec<String> = Vec::new();
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(collector) = filter_collector.filter(|value| !value.is_empty()) {
        params.push(Box::new(collector.to_string()));
        conditions.push(format!("collector_uuid = ?{}", params.len()));
    }
    if let Some(kind) = filter_kind.filter(|value| !value.is_empty()) {
        params.push(Box::new(kind.to_string()));
        conditions.push(format!("kind = ?{}", params.len()));
    }
    if let Some(server_id) = filter_server_id.filter(|id| *id > 0) {
        params.push(Box::new(server_id));
        conditions.push(format!("server_id = ?{}", params.len()));
    }
    if let Some(port) = filter_port.filter(|value| *value > 0) {
        params.push(Box::new(port));
        conditions.push(format!("port = ?{}", params.len()));
    }
    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };
    let total: i64 = {
        let sql = format!("SELECT COUNT(*) FROM probe_sample_buckets {where_clause}");
        let mut stmt = conn.prepare(&sql)?;
        let refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|boxed| boxed.as_ref()).collect();
        stmt.query_row(refs.as_slice(), |row| row.get(0))?
    };
    // limit / offset 同为动态值，必须并入 params：直插文本会绕过类型检查。
    params.push(Box::new(limit.max(1)));
    let limit_index = params.len();
    params.push(Box::new(offset.max(0)));
    let offset_index = params.len();
    let sql = format!(
        "SELECT collector_uuid, server_id, kind, port, bucket_start, \
         COALESCE(min_ms, 0), COALESCE(max_ms, 0), COALESCE(sum_ms, 0), count, \
         COALESCE(loss_sum, 0), COALESCE(success_count, 0), COALESCE(fail_count, 0) \
         FROM probe_sample_buckets {where_clause} ORDER BY bucket_start DESC \
         LIMIT ?{limit_index} OFFSET ?{offset_index}"
    );
    let mut stmt = conn.prepare(&sql)?;
    let refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|boxed| boxed.as_ref()).collect();
    let rows = stmt.query_map(refs.as_slice(), |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, f64>(5)?,
            row.get::<_, f64>(6)?,
            row.get::<_, f64>(7)?,
            row.get::<_, i64>(8)?,
            row.get::<_, f64>(9)?,
            row.get::<_, i64>(10)?,
            row.get::<_, i64>(11)?,
        ))
    })?;
    let mut items = Vec::new();
    let mut rows = rows.collect::<Result<Vec<_>, _>>()?;
    // server_name 批量解析：逐行反查在分页满载时是 N+1。
    let mut names: HashMap<i64, String> = HashMap::new();
    let ids: Vec<i64> = {
        let mut seen = std::collections::BTreeSet::new();
        rows.iter()
            .filter(|row| seen.insert(row.1))
            .map(|row| row.1)
            .collect()
    };
    if !ids.is_empty() {
        let placeholders = vec!["?"; ids.len()].join(", ");
        let sql =
            format!("SELECT id, COALESCE(name, '') FROM servers WHERE id IN ({placeholders})");
        let params: Vec<Box<dyn rusqlite::ToSql>> = ids
            .iter()
            .map(|id| Box::new(*id) as Box<dyn rusqlite::ToSql>)
            .collect();
        let refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|boxed| boxed.as_ref()).collect();
        let mut stmt = conn.prepare(&sql)?;
        let found = stmt.query_map(refs.as_slice(), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in found {
            let (id, name) = row?;
            names.insert(id, name);
        }
    }
    for row in rows.drain(..) {
        let (
            collector_id,
            server_id,
            kind,
            port,
            bucket_start,
            min,
            max,
            sum,
            count,
            loss_sum,
            success,
            fail,
        ) = row;
        let server_name = names.get(&server_id).cloned().unwrap_or_default();
        items.push(json!({
            "collector_id": collector_id,
            "server_id": server_id,
            "server_name": server_name,
            "kind": kind,
            "port": port,
            "bucket_start": optional_rfc3339(bucket_start),
            "min_ms": min,
            "avg_ms": if count > 0 { sum / count as f64 } else { 0.0 },
            "max_ms": max,
            "loss": if count > 0 { loss_sum / count as f64 } else { 0.0 },
            "success_count": success,
            "fail_count": fail,
        }));
    }
    Ok((items, total))
}

/// Go `GetProbeTrace`：无记录返回 null。
pub fn get_probe_trace(
    conn: &Connection,
    collector_id: &str,
    server_id: i64,
) -> anyhow::Result<Option<Value>> {
    let row: Option<(Option<i64>, Option<String>, Option<Vec<u8>>, Option<i64>, Option<String>, Option<Vec<u8>>, Option<i64>)> =
        conn.query_row(
            "SELECT sampled_at, destination, hops_json, tcp_sampled_at, tcp_destination, \
             tcp_hops_json, tcp_port FROM probe_traces WHERE collector_uuid = ?1 AND server_id = ?2",
            rusqlite::params![collector_id, server_id],
            |row| {
                Ok((
                    row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((sampled_at, destination, hops, tcp_sampled_at, tcp_destination, tcp_hops, tcp_port)) =
        row
    else {
        return Ok(None);
    };
    let parse_hops = |blob: &Option<Vec<u8>>| -> Vec<Value> {
        blob.as_ref()
            .filter(|blob| !blob.is_empty())
            .and_then(|blob| serde_json::from_slice::<Vec<Value>>(blob).ok())
            .unwrap_or_default()
    };
    let mut payload = json!({
        "collector_id": collector_id,
        "server_id": server_id,
        "sampled_at": optional_rfc3339(sampled_at.unwrap_or(0)),
        "destination": destination.unwrap_or_default(),
        "hops": parse_hops(&hops),
    });
    if tcp_sampled_at.unwrap_or(0) > 0 {
        let mut tcp = json!({
            "sampled_at": optional_rfc3339(tcp_sampled_at.unwrap_or(0)),
            "destination": tcp_destination.unwrap_or_default(),
            "hops": parse_hops(&tcp_hops),
        });
        if let Some(port) = tcp_port.filter(|value| *value > 0) {
            tcp["port"] = json!(port);
        }
        payload["tcp"] = tcp;
    }
    Ok(Some(payload))
}

/// 跳点读侧装饰：入库只存原始 geo 字段（Go `nexttrace.Hop` 口径），前端要的
/// `geo` / `private` / 规范 `asn` 在这里补齐（Go `decodeRouteHops` +
/// `formatRouteHopGeo`）。
///
/// ipinfo 数据源会把保留地址的归属写进 `whois`（`RFC1918` 之类），ASN 只在
/// `asnumber` 有值或为 `AS<数字>` 时出现；这里不做本机 GeoIP 补算——读侧
/// 凭空发明 ASN 会让同一跳在不同主机上显示不同。
fn decorate_route_hops(hops: Vec<Value>) -> Vec<Value> {
    hops.into_iter()
        .map(|mut hop| {
            let address = hop
                .get("address")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            let private_ip = is_private_hop(&address);
            let country = hop_field(&hop, "country");
            let province = hop_field(&hop, "province");
            let city = hop_field(&hop, "city");
            let owner = hop_field(&hop, "owner");
            let asn = normalize_hop_asn(&hop_field(&hop, "asn"));
            if let Value::Object(ref mut map) = hop {
                map.insert("asn".to_string(), Value::String(asn.clone()));
                map.insert("private".to_string(), Value::Bool(private_ip));
                map.insert(
                    "geo".to_string(),
                    Value::String(format_hop_geo(
                        private_ip, &country, &province, &city, &owner, &asn,
                    )),
                );
            }
            hop
        })
        .collect()
}

/// Go `LookupHop` 的私网判定：回环 / 私网 / 链路本地 / 未指定地址。
/// 地址不可解析时不算私网（可能是主机名）。
fn is_private_hop(address: &str) -> bool {
    match address.parse::<std::net::IpAddr>() {
        Ok(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || match ip {
                    std::net::IpAddr::V4(v4) => {
                        v4.is_private() || v4.is_link_local() || v4.is_broadcast()
                    }
                    std::net::IpAddr::V6(v6) => v6.is_unique_local() || v6.is_unicast_link_local(),
                }
        }
        Err(_) => false,
    }
}

fn hop_field(hop: &Value, key: &str) -> String {
    hop.get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string()
}

/// Go `FormatASN`：空串 / 非数字 → 空；否则补 `AS` 前缀并大写。
fn normalize_hop_asn(raw: &str) -> String {
    let digits = raw
        .strip_prefix("AS")
        .or_else(|| raw.strip_prefix("as"))
        .unwrap_or(raw);
    if digits.is_empty() || !digits.chars().all(|c| c.is_ascii_digit()) {
        return String::new();
    }
    format!("AS{digits}")
}

/// Go `formatRouteHopGeo`：国家 · 省 · 市 · 运营商 · ASN。
/// 私网跳不展示任何归属。
fn format_hop_geo(
    private_ip: bool,
    country: &str,
    province: &str,
    city: &str,
    owner: &str,
    asn: &str,
) -> String {
    if private_ip {
        return String::new();
    }
    let mut parts: Vec<&str> = Vec::new();
    for part in [country, province, city, owner, asn] {
        if !part.is_empty() {
            parts.push(part);
        }
    }
    parts.join(" · ")
}

/// Go `ListProbeRoutes`：每协议按 `sampled_at DESC, id DESC` 取 route_keep 条。
pub fn list_probe_routes(
    conn: &Connection,
    collector_id: &str,
    server_id: i64,
) -> anyhow::Result<Option<Value>> {
    let collector: Option<(i64, i64, i64)> = conn
        .query_row(
            "SELECT enable_icmp, enable_tcp, route_keep FROM collectors \
             WHERE collector_uuid = ?1 AND deleted = 0 AND revoked = 0",
            [collector_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((enable_icmp, enable_tcp, route_keep)) = collector else {
        return Ok(None);
    };
    let server_exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM servers WHERE id = ?1 AND deleted_at IS NULL",
            [server_id],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count > 0)?;
    if !server_exists {
        return Ok(None);
    }
    let keep = route_keep.clamp(1, 50);
    let fetch = |protocol: &'static str| -> anyhow::Result<Vec<Value>> {
        let mut stmt = conn.prepare(
            "SELECT id, sampled_at, COALESCE(destination, ''), COALESCE(error, ''), \
             COALESCE(hops_json, X''), COALESCE(port, 0), COALESCE(job_id, 0) \
             FROM probe_routes WHERE collector_uuid = ?1 AND server_id = ?2 AND protocol = ?3 \
             ORDER BY sampled_at DESC, id DESC LIMIT ?4",
        )?;
        let rows = stmt.query_map(
            rusqlite::params![collector_id, server_id, protocol, keep],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )?;
        let mut out = Vec::new();
        for row in rows {
            let (id, sampled_at, destination, error, hops, port, job_id) = row?;
            let hops: Vec<Value> = if hops.is_empty() {
                Vec::new()
            } else {
                serde_json::from_slice(&hops).unwrap_or_default()
            };
            let hops = decorate_route_hops(hops);
            let mut record = json!({
                "id": id,
                "sampled_at": optional_rfc3339(sampled_at),
                "destination": destination,
                "error": error,
                "hops": hops,
            });
            if port > 0 {
                record["port"] = json!(port);
            }
            if job_id > 0 {
                record["job_id"] = json!(job_id);
            }
            out.push(record);
        }
        Ok(out)
    };
    let icmp = fetch("icmp")?;
    let tcp = fetch("tcp")?;
    let mut payload = json!({
        "collector_id": collector_id,
        "server_id": server_id,
        "enable_icmp": enable_icmp != 0,
        "enable_tcp": enable_tcp != 0,
        "icmp": icmp,
        "tcp": tcp,
    });
    let job: Option<(i64, String, i64, Option<i64>, Option<String>)> = conn
        .query_row(
            "SELECT id, protocol, requested_at, port, error FROM probe_route_jobs \
             WHERE collector_uuid = ?1 AND server_id = ?2 AND status = 'pending' \
             ORDER BY id DESC LIMIT 1",
            rusqlite::params![collector_id, server_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?;
    if let Some((id, protocol, requested_at, port, error)) = job {
        let mut job_json = json!({
            "id": id,
            "protocol": protocol,
            "status": "pending",
            "requested_at": optional_rfc3339(requested_at),
            "error": error,
        });
        if let Some(port) = port.filter(|value| *value > 0) {
            job_json["port"] = json!(port);
        }
        payload["job"] = job_json;
    }
    Ok(Some(payload))
}

/// Go `EnqueueProbeRouteJob`：幂等（同协议已有 pending 直接返回）；返回
/// `(job_id, protocol, port)`；校验失败返回 Err(文案)。
///
/// 探测型从端与协议开关校验：`(icmp_on, tcp_on, tcp_has_port)`。
/// 单目标与批量入队共用同一口径，避免两条路径的开启条件漂移。
fn probe_route_protocols(
    conn: &Connection,
    collector_id: &str,
) -> anyhow::Result<Result<(bool, bool, bool), String>> {
    let Some((enable_icmp, enable_tcp, tcp_ports)) = conn
        .query_row(
            "SELECT enable_icmp, enable_tcp, COALESCE(tcp_ports, '') FROM collectors \
             WHERE collector_uuid = ?1 AND deleted = 0 AND revoked = 0 AND kind = 'probe'",
            [collector_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?
    else {
        return Ok(Err("not_found".into()));
    };
    let effective_ports = if tcp_ports.trim().is_empty() {
        crate::telemetry::probes::DEFAULT_PROBE_TCP_PORTS
    } else {
        tcp_ports.as_str()
    };
    let tcp_ready = effective_ports
        .split(',')
        .any(|port| !port.trim().is_empty());
    Ok(Ok((enable_icmp != 0, enable_tcp != 0, tcp_ready)))
}

/// 协议是否可入队；icmp/tcp 之外的值按 invalid_protocol 拒绝。
fn probe_route_protocol_enabled(
    protocol: &str,
    icmp_on: bool,
    tcp_on: bool,
    tcp_ready: bool,
) -> Result<(), String> {
    let enabled = match protocol {
        "icmp" => icmp_on,
        "tcp" => tcp_on && tcp_ready,
        _ => return Err("invalid_protocol".into()),
    };
    if enabled {
        Ok(())
    } else {
        Err("probe_route_disabled".into())
    }
}

/// 目标主机是否可入队：存在、未删除，且落在该探测从端的 scope 内。
/// 单目标与批量两条入队路径共用同一判定——越权或不存在的目标从端从不追踪，
/// 建了单只会在队列里留一条永久 pending。
fn probe_route_target_state(
    conn: &Connection,
    collector_id: &str,
    server_id: i64,
) -> anyhow::Result<Result<(), String>> {
    let tag: Option<String> = conn
        .query_row(
            "SELECT COALESCE(tag, '') FROM servers WHERE id = ?1 AND deleted_at IS NULL",
            [server_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(tag) = tag else {
        return Ok(Err("server_not_found".into()));
    };
    let scopes: Vec<(String, String)> = {
        let mut stmt = conn.prepare(
            "SELECT scope_type, scope_value FROM collector_scopes WHERE collector_uuid = ?1",
        )?;
        stmt.query_map([collector_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?
    };
    if !scopes_include_server(&scopes, &server_id.to_string(), &tag) {
        return Ok(Err("server_out_of_scope".into()));
    }
    Ok(Ok(()))
}

pub fn enqueue_probe_route_job(
    conn: &Connection,
    collector_id: &str,
    server_id: i64,
    protocol: &str,
) -> anyhow::Result<Result<(i64, String, i64), String>> {
    let (icmp_on, tcp_on, tcp_ready) = match probe_route_protocols(conn, collector_id)? {
        Ok(value) => value,
        Err(reason) => return Ok(Err(reason)),
    };
    let protocol = protocol.to_lowercase();
    if let Err(reason) = probe_route_protocol_enabled(&protocol, icmp_on, tcp_on, tcp_ready) {
        return Ok(Err(reason));
    };
    // 与批量路径同口径：已删除或不在 scope 内的主机没有对应目标，建了单也只会永久 pending。
    if let Err(reason) = probe_route_target_state(conn, collector_id, server_id)? {
        return Ok(Err(reason));
    }
    let existing: Option<i64> = conn
        .query_row(
            "SELECT id FROM probe_route_jobs \
             WHERE collector_uuid = ?1 AND server_id = ?2 AND protocol = ?3 AND status = 'pending' \
             ORDER BY id DESC LIMIT 1",
            rusqlite::params![collector_id, server_id, protocol],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(id) = existing {
        return Ok(Ok((id, protocol, 0)));
    }
    conn.execute(
        "INSERT INTO probe_route_jobs (collector_uuid, server_id, protocol, status, requested_at) \
         VALUES (?1, ?2, ?3, 'pending', ?4)",
        rusqlite::params![collector_id, server_id, protocol, util_time::now_nanos()],
    )?;
    Ok(Ok((conn.last_insert_rowid(), protocol, 0)))
}

/// Go `EnqueueProbeRouteJob` 的批量形态：同一从端、同一协议、一组服务器。
/// 已在该协议上排着 pending 的服务器复用原任务（不重复建单），其余插入。
/// 返回 `(enqueued, reused)`；已删除、越权、无有效探测目标或单机关闭对应协议的服务器自动跳过。
pub fn enqueue_probe_route_jobs(
    conn: &Connection,
    collector_id: &str,
    server_ids: &[i64],
    protocol: &str,
) -> anyhow::Result<Result<(usize, usize), String>> {
    let (icmp_on, tcp_on, tcp_ready) = match probe_route_protocols(conn, collector_id)? {
        Ok(value) => value,
        Err(reason) => return Ok(Err(reason)),
    };
    let protocol = protocol.to_lowercase();
    if let Err(reason) = probe_route_protocol_enabled(&protocol, icmp_on, tcp_on, tcp_ready) {
        return Ok(Err(reason));
    };
    let (enable_ipv4, enable_ipv6): (bool, bool) = conn
        .query_row(
            "SELECT COALESCE(enable_ipv4, 1), COALESCE(enable_ipv6, 1) \
             FROM collectors WHERE collector_uuid = ?1",
            [collector_id],
            |row| Ok((row.get::<_, i64>(0)? != 0, row.get::<_, i64>(1)? != 0)),
        )
        .optional()?
        .unwrap_or((true, true));
    let now = util_time::now_nanos();
    let mut enqueued = 0usize;
    let mut reused = 0usize;
    for server_id in server_ids {
        // 与单目标路径同口径：不存在或越权的目标静默跳过。
        if probe_route_target_state(conn, collector_id, *server_id)?.is_err() {
            continue;
        }
        let server_meta: Option<(String, String, bool, bool)> = conn
            .query_row(
                "SELECT COALESCE(s.probe_target, ''), COALESCE(r.last_ip, ''), \
                 COALESCE(s.probe_enable_icmp, 1), COALESCE(s.probe_enable_tcp, 1) \
                 FROM servers s LEFT JOIN server_runtimes r ON r.server_id = s.id \
                 WHERE s.id = ?1 AND s.deleted_at IS NULL",
                [server_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get::<_, i64>(2)? != 0,
                        row.get::<_, i64>(3)? != 0,
                    ))
                },
            )
            .optional()?;
        let Some((probe_target, last_ip, server_icmp, server_tcp)) = server_meta else {
            continue;
        };
        let server_proto_on = match protocol.as_str() {
            "icmp" => server_icmp,
            "tcp" => server_tcp,
            _ => false,
        };
        if !server_proto_on {
            continue;
        }
        let resolved = probe_ingest::apply_ip_families(
            probe_ingest::resolve_probe_target(&probe_target, &last_ip),
            enable_ipv4,
            enable_ipv6,
        );
        if resolved.source == "none" {
            continue;
        }
        let existing: Option<i64> = conn
            .query_row(
                "SELECT id FROM probe_route_jobs \
                 WHERE collector_uuid = ?1 AND server_id = ?2 AND protocol = ?3 AND status = 'pending' \
                 ORDER BY id DESC LIMIT 1",
                rusqlite::params![collector_id, server_id, protocol],
                |row| row.get(0),
            )
            .optional()?;
        if existing.is_some() {
            reused += 1;
            continue;
        }
        conn.execute(
            "INSERT INTO probe_route_jobs (collector_uuid, server_id, protocol, status, requested_at, updated_at) \
             VALUES (?1, ?2, ?3, 'pending', ?4, ?5)",
            rusqlite::params![collector_id, server_id, protocol, now, unix_nanos_to_sqlite(now)],
        )?;
        enqueued += 1;
    }
    Ok(Ok((enqueued, reused)))
}

#[cfg(test)]
mod route_tests {
    use super::*;
    use crate::db::migrate;

    fn temp_db(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "santaizi-insights-route-{tag}-{}-{}",
            std::process::id(),
            util_time::now_nanos()
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
                "INSERT INTO collectors (collector_uuid, name, address, token_hash, token_ciphertext, kind, \
                 tcp_ports, enable_icmp, enable_tcp, probe_notify, fail_threshold, route_keep, revoked, deleted) \
                 VALUES ('probe-1', 'probe', '10.0.0.1:5556', X'00', X'00', 'probe', '22', 1, 1, 1, 2, 5, 0, 0)",
                [],
            )?;
            // scope：只有 web 组在内——入队校验与 `build_probe_targets` 同一套判定。
            tx.execute(
                "INSERT INTO collector_scopes (collector_uuid, scope_type, scope_value, created_at) \
                 VALUES ('probe-1', 'tag', 'web', CURRENT_TIMESTAMP)",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    }

    fn insert_route(conn: &Connection, hops_json: &str) {
        conn.execute(
            "INSERT INTO probe_routes (collector_uuid, server_id, protocol, sampled_at, destination, \
             port, hops_json, error, updated_at) VALUES ('probe-1', 7, 'icmp', 1700000000000000000, \
             'edge.example', 0, ?1, '', ?2)",
            rusqlite::params![hops_json.as_bytes(), unix_nanos_to_sqlite(1700000000000000000)],
        )
        .unwrap();
    }

    /// ipinfo 形态：asnumber 是 ASN，geo 由归属拼出。
    #[test]
    fn ipinfo_hops_expose_asn_and_geo() {
        let path = temp_db("ipinfo");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db);
        db.write(|conn| {
            insert_route(
                conn,
                r#"[{"ttl":1,"address":"10.0.0.6","asn":""},
                   {"ttl":2,"address":"192.0.2.1","asn":"13335","country":"ANYCAST",
                    "city":"","owner":"Cloudflare, Inc."},
                   {"ttl":3,"address":"198.51.100.19","asn":"AS41335"},
                   {"ttl":4,"address":"203.0.113.118","asn":"4809","country":"中国",
                    "province":"上海","city":"上海","owner":"电信"}]"#,
            );
            let payload = list_probe_routes(conn, "probe-1", 7).unwrap().unwrap();
            let hops = payload["icmp"][0]["hops"].as_array().unwrap();
            assert_eq!(hops[0]["private"], json!(true), "内网跳标私网");
            assert_eq!(hops[0]["geo"], json!(""));
            assert_eq!(hops[0]["asn"], json!(""));
            assert_eq!(hops[1]["asn"], json!("AS13335"));
            assert_eq!(
                hops[1]["geo"],
                json!("ANYCAST · Cloudflare, Inc. · AS13335")
            );
            assert_eq!(hops[2]["asn"], json!("AS41335"));
            assert_eq!(hops[3]["geo"], json!("中国 · 上海 · 上海 · 电信 · AS4809"));
            assert_eq!(hops[3]["private"], json!(false));
            Ok(())
        })
        .unwrap();
    }

    /// 非数字 ASN（如 ipinfo 的 RFC whois 串）不得变成 ASN。
    #[test]
    fn non_numeric_asn_is_dropped() {
        let path = temp_db("asn");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db);
        db.write(|conn| {
            insert_route(
                conn,
                r#"[{"ttl":1,"address":"10.0.0.6","asn":"RFC1918","country":"私网"},
                   {"ttl":2,"address":"203.0.113.9","asn":"as4134"}]"#,
            );
            let payload = list_probe_routes(conn, "probe-1", 7).unwrap().unwrap();
            let hops = payload["icmp"][0]["hops"].as_array().unwrap();
            assert_eq!(hops[0]["asn"], json!(""));
            assert_eq!(hops[0]["geo"], json!(""), "私网不展示归属");
            assert_eq!(hops[1]["asn"], json!("AS4134"));
            Ok(())
        })
        .unwrap();
    }

    /// 批量入队：新任务插入、已有 pending 复用、已删/无目标/单机关协议的服务器自动跳过。
    #[test]
    fn batch_enqueue_reuses_pending_and_skips_dead_servers() {
        let path = temp_db("batch");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db);
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, deleted_at) \
                 VALUES (9, 'gone', 'web', X'02', 1700000000000000000)",
                [],
            )?;
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, probe_target) \
                 VALUES (10, 'edge-2', 'web', X'03', 'edge2.example')",
                [],
            )?;
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, probe_target, probe_enable_icmp) \
                 VALUES (12, 'icmp-off', 'web', X'04', 'icmpoff.example', 0)",
                [],
            )?;
            // 7/10 有探测目标且开 ICMP → 入队；8 无目标、9 已删、12 关闭 ICMP → 自动跳过。
            let first = enqueue_probe_route_jobs(tx, "probe-1", &[7, 8, 9, 10, 12], "icmp")?;
            assert_eq!(first, Ok((2, 0)), "仅有效探测目标且启用协议的活服务器入队");
            let second = enqueue_probe_route_jobs(tx, "probe-1", &[7, 8, 10], "icmp")?;
            assert_eq!(second, Ok((0, 2)), "再次下发全部复用");
            let pending: i64 = tx.query_row(
                "SELECT COUNT(*) FROM probe_route_jobs WHERE status = 'pending'",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(pending, 2, "不重复建单");

            // tcp_ports 为空串时按默认 "22,443" 判定可用。
            tx.execute(
                "UPDATE collectors SET tcp_ports = '' WHERE collector_uuid = 'probe-1'",
                [],
            )?;
            let tcp_batch = enqueue_probe_route_jobs(tx, "probe-1", &[7, 8], "tcp")?;
            assert_eq!(tcp_batch, Ok((1, 0)), "空 tcp_ports 应按默认端口判定可用");

            let disabled = enqueue_probe_route_jobs(tx, "probe-1", &[7], "udp")?;
            assert_eq!(disabled, Err("invalid_protocol".into()));
            Ok(())
        })
        .unwrap();
    }

    /// 非探测型从端与不存在的从端一律拒绝。
    #[test]
    fn batch_enqueue_rejects_unknown_collector() {
        let path = temp_db("reject");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db);
        db.write(|tx| {
            let missing = enqueue_probe_route_jobs(tx, "nope", &[7], "icmp")?;
            assert_eq!(missing, Err("not_found".into()));
            Ok(())
        })
        .unwrap();
    }

    /// 单目标与批量同口径：不存在的服务器不得建单（否则任务永久 pending）。
    #[test]
    fn single_enqueue_rejects_unknown_server() {
        let path = temp_db("single");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db);
        db.write(|tx| {
            let missing = enqueue_probe_route_job(tx, "probe-1", 404, "icmp")?;
            assert_eq!(missing, Err("server_not_found".into()));
            let jobs: i64 = tx.query_row("SELECT COUNT(*) FROM probe_route_jobs", [], |row| {
                row.get(0)
            })?;
            assert_eq!(jobs, 0, "不得为不存在的服务器留下任务");
            assert!(enqueue_probe_route_job(tx, "probe-1", 7, "icmp")?.is_ok());
            assert!(
                enqueue_probe_route_job(tx, "probe-1", 7, "icmp")?.is_ok(),
                "重复下发复用原任务"
            );
            Ok(())
        })
        .unwrap();
    }

    /// scope 外的目标：单目标报错，批量静默跳过（从端从不追踪它）。
    #[test]
    fn enqueue_rejects_servers_outside_collector_scope() {
        let path = temp_db("scope");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db);
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext) VALUES (11, 'db', 'storage', X'03')",
                [],
            )?;
            let out = enqueue_probe_route_job(tx, "probe-1", 11, "icmp")?;
            assert_eq!(out, Err("server_out_of_scope".into()));
            let batch = enqueue_probe_route_jobs(tx, "probe-1", &[7, 11], "icmp")?;
            assert_eq!(batch, Ok((1, 0)), "scope 外目标跳过，scope 内照常入队");
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn probe_summary_ignores_orphan_latests_and_agents_history_fill_names() {
        let path = temp_db("summary-agents");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db);
        let node = [0x99u8; 16];
        db.write(|tx| {
            // 合法路径 (probe-1, 7) + 孤儿行 (revoked-probe, 7) + (probe-1, 999 已删服) + (probe-1, 11 越界服)。
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, probe_target) VALUES (11, 'other', 'storage', X'09', 'other.example')",
                [],
            )?;
            tx.execute(
                "INSERT INTO probe_latests (collector_uuid, server_id, sampled_at, reachable) VALUES ('probe-1', 7, 100, 1)",
                [],
            )?;
            tx.execute(
                "INSERT INTO probe_latests (collector_uuid, server_id, sampled_at, reachable) VALUES ('ghost-probe', 7, 100, 1)",
                [],
            )?;
            tx.execute(
                "INSERT INTO probe_latests (collector_uuid, server_id, sampled_at, reachable) VALUES ('probe-1', 999, 100, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO probe_latests (collector_uuid, server_id, sampled_at, reachable) VALUES ('probe-1', 11, 100, 0)",
                [],
            )?;

            // list_agents observer_name / observer_kind
            tx.execute(
                "INSERT INTO collectors (collector_uuid, name, address, token_hash, token_ciphertext, kind, tcp_ports, revoked, deleted) \
                 VALUES ('obs-hk', '香港从端', '10.0.0.2:5555', X'00', X'00', 'observer', '22', 0, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason, created_at) \
                 VALUES (7, ?1, 0, NULL, 1, 'r', CURRENT_TIMESTAMP)",
                [&node.to_vec()],
            )?;
            let runtime = proto::AgentRuntime {
                sinks: vec![
                    proto::SinkRuntime {
                        endpoint_id: "primary".into(),
                        connected: true,
                        ..Default::default()
                    },
                    proto::SinkRuntime {
                        endpoint_id: "obs-hk".into(),
                        connected: true,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
            .encode_to_vec();
            tx.execute(
                "INSERT INTO agent_telemetry_runtimes (node_uuid, wal_pressure, sink_cursors, updated_at) \
                 VALUES (?1, 1, ?2, CURRENT_TIMESTAMP)",
                rusqlite::params![&node.to_vec(), runtime],
            )?;

            // list_monitor_history server_name
            tx.execute(
                "INSERT INTO monitor_histories (id, monitor_id, server_id, created_at, avg_delay, up, down) \
                 VALUES (1, 5, 7, '2026-10-16 00:00:00', 12.5, 1, 0)",
                [],
            )?;
            Ok(())
        })
        .unwrap();

        db.read(|conn| {
            let summary = load_probe_summary(conn, util_time::now_nanos())?;
            assert_eq!(summary["paths_assigned"], 2, "web 组含 7 与 8 两台服务器");
            assert_eq!(summary["paths_reachable"], 1);
            assert_eq!(summary["paths_down"], 0, "孤儿与越界行不得计入 paths_down");

            let (agents, _) = list_agents(conn, 0, 10)?;
            let sinks = agents[0]["sinks"].as_array().unwrap();
            assert_eq!(sinks[0]["observer_kind"], "primary");
            assert_eq!(sinks[0]["observer_name"], "主面板");
            assert_eq!(sinks[1]["observer_kind"], "collector");
            assert_eq!(sinks[1]["observer_name"], "香港从端");

            let (hist, _) = list_monitor_history(conn, 5, None, None, None, 10)?;
            assert_eq!(hist[0]["server_name"], "edge");
            Ok(())
        })
        .unwrap();
    }
}
