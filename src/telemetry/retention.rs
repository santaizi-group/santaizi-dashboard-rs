//! 保留期执行：对齐 Go `service/telemetry/rollup.go` 的 `DrainRetention` 及其助手。
//!
//! 所有删除走 `rowid IN (SELECT ... LIMIT batch)` 分批执行，受 `max_runtime`
//! 截止时间约束，单轮不会长占写锁；表名走白名单，条件 SQL 只允许内部拼装。
//! datetime 列（`*_at`）与 SQLite `CURRENT_TIMESTAMP` 同为 UTC 秒精度，
//! 统一用 `datetime(?, 'unixepoch')` 比较，绕开驱动时区格式差异。

use std::collections::HashMap;
use std::result::Result as StdResult;
use std::time::Instant;

use rusqlite::Transaction;

use crate::availability::engine::{AvailabilityBucketRow, RESOLUTION_RAW, RESOLUTION_SPAN};
use crate::db::Db;
use crate::proto;
use crate::telemetry::rollup::RetentionPolicy;

const MINUTE_NANOS: i64 = 60_000_000_000;
const HOUR_NANOS: i64 = 60 * MINUTE_NANOS;
/// `availabilityRowEnd` 兜底桶长：未封窗桶按 30s 推 window_end。
const BUCKET_NANOS: i64 = 30_000_000_000;

const PROBE_ROUTE_JOB_TTL_NANOS: i64 = 10 * 60 * 1_000_000_000;
const LATENCY_KEEP_MULTIPLIER: i64 = 100;
const DEFAULT_ROUTE_KEEP: i64 = 10;
const MIN_ROUTE_KEEP: i64 = 1;
const MAX_ROUTE_KEEP: i64 = 50;

const COLLECTOR_KIND_PROBE: &str = "probe";
const JOB_STATUS_PENDING: &str = "pending";
const JOB_STATUS_FAILED: &str = "failed";
/// 从端超时未领取的路由任务错误文案（与 Go 一致，前端直接展示）。
const JOB_EXPIRE_ERROR: &str = "从端未领取";

/// 不留证据、只做计数的高频事件类型：HEARTBEAT / STATE / STATE_ROLLUP。
fn high_frequency_event_types_sql() -> String {
    [
        proto::TelemetryEventType::Heartbeat as i32,
        proto::TelemetryEventType::State as i32,
        proto::TelemetryEventType::StateRollup as i32,
    ]
    .iter()
    .map(|value| value.to_string())
    .collect::<Vec<_>>()
    .join(",")
}

/// `DrainRetention`：按保留策略清一轮历史。返回删除行数；
/// 截止时间到即停，剩余下一轮继续（Go 同）。
pub fn drain_retention(db: &Db, policy: &RetentionPolicy, now_nanos: i64) -> anyhow::Result<i64> {
    let policy = policy.normalized();
    let deadline = Instant::now() + policy.max_runtime;
    let batch = policy.batch_size as i64;
    let mut deleted = 0i64;

    if sqlite_table_exists(db, "probe_route_jobs") {
        expire_stale_probe_route_jobs(db, now_nanos)?;
    }
    if sqlite_table_exists(db, "probe_routes") {
        prune_all_probe_routes(db)?;
    }
    if sqlite_table_exists(db, "connection_latency_buckets") {
        deleted += prune_connection_latency_by_count(db, deadline, batch)?;
    }

    let completed_minute = now_nanos / MINUTE_NANOS * MINUTE_NANOS;
    if sqlite_table_exists(db, "telemetry_events") {
        let hot = high_frequency_event_types_sql();
        deleted += drain_until(
            db,
            deadline,
            batch,
            "telemetry_observations",
            &format!(
                "event_id IN (SELECT event_id FROM telemetry_events WHERE event_type IN ({hot}) AND collected_at < ?)"
            ),
            &[&completed_minute],
        )?;
        deleted += drain_until(
            db,
            deadline,
            batch,
            "telemetry_events",
            &format!("collected_at < ? AND event_type IN ({hot})"),
            &[&completed_minute],
        )?;
        // HOST payload 与 server_runtimes.host_payload 重复且无人回读；
        // 与 STATE 一并剥掉，只留事件骨架。
        deleted += update_until(
            db,
            deadline,
            batch,
            "UPDATE telemetry_events SET payload = NULL, payload_retained = 0 \
             WHERE rowid IN (SELECT rowid FROM telemetry_events \
             WHERE event_type IN (?, ?) AND payload_retained = 1 AND collected_at < ? LIMIT ?)",
            &[
                &(proto::TelemetryEventType::State as i32),
                &(proto::TelemetryEventType::Host as i32),
                &completed_minute,
            ],
        )?;
    }

    let observation_before = now_nanos - policy.observation;
    let evidence_before = now_nanos - policy.evidence;
    deleted += drain_until(
        db,
        deadline,
        batch,
        "telemetry_observations",
        "received_at < ?",
        &[&observation_before],
    )?;
    deleted += drain_until(
        db,
        deadline,
        batch,
        "telemetry_events",
        &format!(
            "collected_at < ? AND event_type != {}",
            proto::TelemetryEventType::Lifecycle as i32
        ),
        &[&observation_before],
    )?;
    deleted += drain_until(
        db,
        deadline,
        batch,
        "observer_path_buckets",
        "bucket_start < ?",
        &[&evidence_before],
    )?;
    deleted += drain_until(
        db,
        deadline,
        batch,
        "observer_health_buckets",
        "bucket_start < ?",
        &[&evidence_before],
    )?;
    deleted += drain_until(
        db,
        deadline,
        batch,
        "availability_recompute_queues",
        "bucket_start < ?",
        &[&evidence_before],
    )?;
    deleted += drain_until(
        db,
        deadline,
        batch,
        "telemetry_alerts",
        "occurred_at < ?",
        &[&observation_before],
    )?;
    deleted += drain_until(
        db,
        deadline,
        batch,
        "telemetry_data_losses",
        "occurred_at < ?",
        &[&observation_before],
    )?;
    deleted += drain_until(
        db,
        deadline,
        batch,
        "collector_replication_receipts",
        "created_at < datetime(?, 'unixepoch')",
        &[&((now_nanos - policy.receipt) / 1_000_000_000)],
    )?;
    deleted += drain_until(
        db,
        deadline,
        batch,
        "probe_route_jobs",
        "status != ? AND requested_at < ?",
        &[&JOB_STATUS_PENDING, &observation_before],
    )?;
    deleted += drain_until(
        db,
        deadline,
        batch,
        "telemetry_ingest_cursors",
        "updated_at < datetime(?, 'unixepoch')",
        &[&(observation_before / 1_000_000_000)],
    )?;
    deleted += compact_availability_spans(db, deadline, batch, evidence_before)?;

    let lifecycle_before = now_nanos - policy.lifecycle;
    deleted += drain_until(
        db,
        deadline,
        batch,
        "incident_revisions",
        "incident_id IN (SELECT id FROM availability_incidents WHERE started_at < ?)",
        &[&lifecycle_before],
    )?;
    for (table, condition, before) in [
        (
            "telemetry_events",
            format!(
                "collected_at < ? AND event_type = {}",
                proto::TelemetryEventType::Lifecycle as i32
            ),
            lifecycle_before,
        ),
        (
            "telemetry_gaps",
            "created_at_unix_nano < ?".to_string(),
            lifecycle_before,
        ),
        (
            "availability_buckets",
            "bucket_start < ?".to_string(),
            lifecycle_before,
        ),
        (
            "availability_incidents",
            "started_at < ?".to_string(),
            lifecycle_before,
        ),
        (
            "state_rollups",
            "resolution = '1m' AND window_start < ?".to_string(),
            now_nanos - policy.state_one_minute,
        ),
        (
            "state_rollups",
            "resolution = '1h' AND window_start < ?".to_string(),
            now_nanos - policy.state_one_hour,
        ),
        (
            "connection_latency_buckets",
            "bucket_start < ?".to_string(),
            evidence_before,
        ),
        (
            "probe_sample_buckets",
            "bucket_start < ?".to_string(),
            evidence_before,
        ),
        (
            "probe_latests",
            "sampled_at < ?".to_string(),
            evidence_before,
        ),
        (
            "probe_traces",
            "sampled_at < ?".to_string(),
            evidence_before,
        ),
    ] {
        deleted += drain_until(db, deadline, batch, table, &condition, &[&before])?;
    }
    deleted += drain_until(
        db,
        deadline,
        batch,
        "monitor_histories",
        "server_id != 0 AND created_at < datetime(?, 'unixepoch')",
        &[&((now_nanos - 24 * HOUR_NANOS) / 1_000_000_000)],
    )?;
    deleted += drain_until(
        db,
        deadline,
        batch,
        "monitor_histories",
        "created_at < datetime(?, 'unixepoch')",
        &[&((now_nanos - 30 * 24 * HOUR_NANOS) / 1_000_000_000)],
    )?;
    if sqlite_table_exists(db, "monitors") {
        deleted += drain_until(
            db,
            deadline,
            batch,
            "monitor_histories",
            "monitor_id NOT IN (SELECT id FROM monitors)",
            &[],
        )?;
    }
    deleted += drain_until(
        db,
        deadline,
        batch,
        "transfers",
        "created_at < datetime(?, 'unixepoch')",
        &[&((now_nanos - 48 * HOUR_NANOS) / 1_000_000_000)],
    )?;
    if sqlite_table_exists(db, "servers") {
        deleted += drain_until(
            db,
            deadline,
            batch,
            "transfers",
            "server_id NOT IN (SELECT id FROM servers)",
            &[],
        )?;
    }
    Ok(deleted)
}

/// `drainUntil`：分批删除直到无匹配或超过截止时间。
/// 表名必须在白名单内——条件 SQL 由内部拼装，白名单是唯一的注入面收敛。
fn drain_until(
    db: &Db,
    deadline: Instant,
    batch: i64,
    table: &str,
    condition: &str,
    args: &[&dyn rusqlite::ToSql],
) -> anyhow::Result<i64> {
    if !retention_table_allowed(table) {
        anyhow::bail!("retention table is not allowlisted: {table}");
    }
    if !sqlite_table_exists(db, table) {
        return Ok(0);
    }
    let query = format!(
        "DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} WHERE {condition} LIMIT ?)"
    );
    let mut total = 0i64;
    loop {
        if Instant::now() >= deadline {
            return Ok(total);
        }
        let mut params: Vec<&dyn rusqlite::ToSql> = args.to_vec();
        params.push(&batch);
        let affected =
            db.write(|tx| Ok(tx.execute(&query, rusqlite::params_from_iter(params))?))?;
        total += affected as i64;
        if affected == 0 {
            return Ok(total);
        }
    }
}

/// `updateUntil`：`update_until` 的分批版；查询自带末尾 `LIMIT ?`。
fn update_until(
    db: &Db,
    deadline: Instant,
    batch: i64,
    query: &str,
    args: &[&dyn rusqlite::ToSql],
) -> anyhow::Result<i64> {
    let mut total = 0i64;
    loop {
        if Instant::now() >= deadline {
            return Ok(total);
        }
        let mut params: Vec<&dyn rusqlite::ToSql> = args.to_vec();
        params.push(&batch);
        let affected = db.write(|tx| Ok(tx.execute(query, rusqlite::params_from_iter(params))?))?;
        total += affected as i64;
        if affected == 0 {
            return Ok(total);
        }
    }
}

fn sqlite_table_exists(db: &Db, table: &str) -> bool {
    db.read(|conn| {
        let count: i64 = conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
            rusqlite::params![table],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    })
    .unwrap_or(false)
}

/// `retentionTableAllowed`：可清表白名单。
fn retention_table_allowed(table: &str) -> bool {
    matches!(
        table,
        "telemetry_observations"
            | "telemetry_events"
            | "telemetry_gaps"
            | "availability_buckets"
            | "availability_incidents"
            | "incident_revisions"
            | "state_rollups"
            | "connection_latency_buckets"
            | "observer_path_buckets"
            | "observer_health_buckets"
            | "collector_replication_receipts"
            | "telemetry_alerts"
            | "telemetry_data_losses"
            | "probe_sample_buckets"
            | "probe_latests"
            | "probe_traces"
            | "monitor_histories"
            | "transfers"
            | "availability_recompute_queues"
            | "probe_route_jobs"
            | "telemetry_ingest_cursors"
    )
}

/// `expireStaleProbeRouteJobs`：pending 超过 TTL 判为失败（从端没来领）。
fn expire_stale_probe_route_jobs(db: &Db, now_nanos: i64) -> anyhow::Result<()> {
    let cutoff = now_nanos - PROBE_ROUTE_JOB_TTL_NANOS;
    db.write(|tx| {
        tx.execute(
            "UPDATE probe_route_jobs SET status = ?1, error = ?2, finished_at = ?3, updated_at = CURRENT_TIMESTAMP \
             WHERE status = ?4 AND requested_at < ?5",
            rusqlite::params![JOB_STATUS_FAILED, JOB_EXPIRE_ERROR, now_nanos, JOB_STATUS_PENDING, cutoff],
        )?;
        Ok(())
    })
}

/// `NormalizeRouteKeep`：0→默认，截到 [1, 50]。
fn normalize_route_keep(value: i64) -> i64 {
    if value == 0 {
        return DEFAULT_ROUTE_KEEP;
    }
    value.clamp(MIN_ROUTE_KEEP, MAX_ROUTE_KEEP)
}

/// `PruneAllProbeRoutes`：按各探针从端的 route_keep 保留最新 N 条路由。
fn prune_all_probe_routes(db: &Db) -> anyhow::Result<()> {
    let keep_by_uuid: HashMap<String, i64> = db.read(|conn| {
        let mut stmt = conn.prepare(
            "SELECT collector_uuid, route_keep FROM collectors WHERE kind = ? AND deleted = 0",
        )?;
        let rows = stmt
            .query_map(rusqlite::params![COLLECTOR_KIND_PROBE], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<StdResult<Vec<_>, _>>()?;
        Ok(rows.into_iter().collect())
    })?;
    let keys: Vec<(String, i64, String)> = db.read(|conn| {
        let mut stmt =
            conn.prepare("SELECT DISTINCT collector_uuid, server_id, protocol FROM probe_routes")?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<StdResult<Vec<_>, _>>()?;
        Ok(rows)
    })?;
    for (collector_uuid, server_id, protocol) in keys {
        let keep = normalize_route_keep(keep_by_uuid.get(&collector_uuid).copied().unwrap_or(0));
        prune_probe_routes(db, &collector_uuid, server_id, &protocol, keep)?;
    }
    Ok(())
}

/// `pruneProbeRoutes`：保留 (collector, server, protocol) 下最新 keep 条。
fn prune_probe_routes(
    db: &Db,
    collector_uuid: &str,
    server_id: i64,
    protocol: &str,
    keep: i64,
) -> anyhow::Result<()> {
    let stale_ids: Vec<i64> = db.read(|conn| {
        let mut stmt = conn.prepare(
            "SELECT id FROM probe_routes \
             WHERE collector_uuid = ? AND server_id = ? AND protocol = ? \
             ORDER BY sampled_at DESC, id DESC LIMIT -1 OFFSET ?",
        )?;
        let rows = stmt
            .query_map(
                rusqlite::params![collector_uuid, server_id, protocol, keep],
                |row| row.get::<_, i64>(0),
            )?
            .collect::<StdResult<Vec<_>, _>>()?;
        Ok(rows)
    })?;
    if stale_ids.is_empty() {
        return Ok(());
    }
    let marks = stale_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let query = format!("DELETE FROM probe_routes WHERE id IN ({marks})");
    db.write(|tx| {
        tx.execute(&query, rusqlite::params_from_iter(stale_ids.iter()))?;
        Ok(())
    })
}

/// `connectionLatencyKeep`：非探针从端数 × 100，至少 100。
fn connection_latency_keep(db: &Db) -> anyhow::Result<i64> {
    if !sqlite_table_exists(db, "collectors") {
        return Ok(LATENCY_KEEP_MULTIPLIER);
    }
    let count: i64 = db.read(|conn| {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM collectors WHERE deleted = 0 AND revoked = 0 AND kind != ?",
            rusqlite::params![COLLECTOR_KIND_PROBE],
            |row| row.get(0),
        )?)
    })?;
    Ok(count.max(1) * LATENCY_KEEP_MULTIPLIER)
}

/// `PruneConnectionLatencyByCount`：每个 (kind, collector, node, observer)
/// 分区只保留最新 keep 个桶。
fn prune_connection_latency_by_count(
    db: &Db,
    deadline: Instant,
    batch: i64,
) -> anyhow::Result<i64> {
    let keep = connection_latency_keep(db)?;
    let batch = if batch <= 0 { 5000 } else { batch };
    let query = "DELETE FROM connection_latency_buckets WHERE rowid IN (
        SELECT rowid FROM (
            SELECT rowid, ROW_NUMBER() OVER (
                PARTITION BY kind, collector_uuid, node_uuid, observer_id
                ORDER BY bucket_start DESC
            ) AS rn FROM connection_latency_buckets
        ) WHERE rn > ? LIMIT ?)";
    let mut total = 0i64;
    loop {
        if Instant::now() >= deadline {
            return Ok(total);
        }
        let affected = db.write(|tx| Ok(tx.execute(query, rusqlite::params![keep, batch])?))?;
        total += affected as i64;
        if affected == 0 {
            return Ok(total);
        }
    }
}

/// `compactAvailabilitySpans`：把早于 cutoff 的原始分辨率桶按小时合并成
/// span 行（resolution='span'），相邻同态桶并成一段。
fn compact_availability_spans(
    db: &Db,
    deadline: Instant,
    limit: i64,
    cutoff_nanos: i64,
) -> anyhow::Result<i64> {
    if !sqlite_table_exists(db, "availability_buckets") {
        return Ok(0);
    }
    let limit = if limit <= 0 { 5000 } else { limit };
    let hour_cutoff = cutoff_nanos / HOUR_NANOS * HOUR_NANOS;
    let mut total = 0i64;
    loop {
        if Instant::now() >= deadline {
            return Ok(total);
        }
        let hours: Vec<(Vec<u8>, i64)> = db.read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT node_uuid, (bucket_start / ?) * ? AS hour_start \
                 FROM availability_buckets \
                 WHERE (resolution = '' OR resolution = ?) AND bucket_start < ? \
                 GROUP BY node_uuid, hour_start \
                 ORDER BY hour_start ASC LIMIT ?",
            )?;
            let rows = stmt
                .query_map(
                    rusqlite::params![HOUR_NANOS, HOUR_NANOS, RESOLUTION_RAW, hour_cutoff, limit],
                    |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)),
                )?
                .collect::<StdResult<Vec<_>, _>>()?;
            Ok(rows)
        })?;
        if hours.is_empty() {
            return Ok(total);
        }
        let mut progress = false;
        for (node_uuid, hour_start) in hours {
            let compacted = compact_availability_hour(db, &node_uuid, hour_start)?;
            if compacted > 0 {
                progress = true;
                total += compacted;
            }
        }
        if !progress {
            return Ok(total);
        }
    }
}

/// `compactAvailabilityHour`：一个小时窗内的原始桶 → span 行。
/// 删除与插入在同一事务，避免中间态被引擎读到。
fn compact_availability_hour(db: &Db, node_uuid: &[u8], hour_start: i64) -> anyhow::Result<i64> {
    let hour_end = hour_start + HOUR_NANOS;
    let rows: Vec<AvailabilityBucketRow> = db.read(|conn| {
        let mut stmt = conn.prepare(
            "SELECT node_uuid, bucket_start, window_end, resolution, host_state, \
             connectivity_state, expected_observers, healthy_observers, seen_observers, \
             observer_summary, revision, finalized, recalculated_at \
             FROM availability_buckets \
             WHERE node_uuid = ? AND bucket_start >= ? AND bucket_start < ? AND (resolution = '' OR resolution = ?) \
             ORDER BY bucket_start ASC",
        )?;
        let rows = stmt
            .query_map(
                rusqlite::params![node_uuid, hour_start, hour_end, RESOLUTION_RAW],
                |row| {
                    Ok(AvailabilityBucketRow {
                        node_uuid: row.get(0)?,
                        bucket_start: row.get(1)?,
                        window_end: row.get(2)?,
                        resolution: row.get(3)?,
                        host_state: row.get(4)?,
                        connectivity_state: row.get(5)?,
                        expected_observers: row.get::<_, i64>(6)?.max(0) as u32,
                        healthy_observers: row.get::<_, i64>(7)?.max(0) as u32,
                        seen_observers: row.get::<_, i64>(8)?.max(0) as u32,
                        observer_summary: row.get::<_, Option<Vec<u8>>>(9)?.unwrap_or_default(),
                        revision: row.get::<_, i64>(10)?.max(0) as u64,
                        finalized: row.get::<_, i64>(11)? != 0,
                        recalculated_at: row.get::<_, Option<i64>>(12)?.unwrap_or(0),
                    })
                },
            )?
            .collect::<StdResult<Vec<_>, _>>()?;
        Ok(rows)
    })?;
    if rows.is_empty() {
        return Ok(0);
    }
    let spans = merge_availability_runs(&rows);
    db.write(|tx| {
        tx.execute(
            "DELETE FROM availability_buckets \
             WHERE node_uuid = ? AND bucket_start >= ? AND bucket_start < ? AND (resolution = '' OR resolution = ?)",
            rusqlite::params![node_uuid, hour_start, hour_end, RESOLUTION_RAW],
        )?;
        for span in &spans {
            insert_availability_span(tx, span)?;
        }
        Ok(())
    })?;
    Ok(rows.len() as i64)
}

/// span 行 upsert：冲突键与 Go 一致，只取 (node_uuid, bucket_start)。
fn insert_availability_span(
    tx: &Transaction<'_>,
    span: &AvailabilityBucketRow,
) -> anyhow::Result<()> {
    tx.execute(
        "INSERT INTO availability_buckets (node_uuid, bucket_start, window_end, resolution, host_state, \
         connectivity_state, expected_observers, healthy_observers, seen_observers, observer_summary, \
         revision, finalized, recalculated_at, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT (node_uuid, bucket_start) DO UPDATE SET \
         window_end = ?3, resolution = ?4, host_state = ?5, connectivity_state = ?6, \
         expected_observers = ?7, healthy_observers = ?8, seen_observers = ?9, observer_summary = ?10, \
         revision = ?11, finalized = ?12, recalculated_at = ?13, updated_at = CURRENT_TIMESTAMP",
        rusqlite::params![
            span.node_uuid,
            span.bucket_start,
            span.window_end,
            span.resolution,
            span.host_state,
            span.connectivity_state,
            span.expected_observers,
            span.healthy_observers,
            span.seen_observers,
            span.observer_summary,
            span.revision,
            span.finalized as i64,
            span.recalculated_at,
        ],
    )?;
    Ok(())
}

/// `mergeAvailabilityRuns`：相邻且六元组相同的桶并成一段 span。
fn merge_availability_runs(rows: &[AvailabilityBucketRow]) -> Vec<AvailabilityBucketRow> {
    let Some((first, rest)) = rows.split_first() else {
        return Vec::new();
    };
    let mut spans: Vec<AvailabilityBucketRow> = Vec::with_capacity(rows.len());
    let mut current = start_availability_span(first);
    for row in rest {
        let end = availability_row_end(row);
        if same_availability_run(&current, row) && row.bucket_start == current.window_end {
            current.window_end = end;
            if row.revision > current.revision {
                current.revision = row.revision;
            }
            current.observer_summary = row.observer_summary.clone();
            current.finalized = row.finalized;
            current.recalculated_at = row.recalculated_at;
            continue;
        }
        spans.push(std::mem::replace(
            &mut current,
            start_availability_span(row),
        ));
    }
    spans.push(current);
    spans
}

fn start_availability_span(row: &AvailabilityBucketRow) -> AvailabilityBucketRow {
    let mut span = row.clone();
    span.resolution = RESOLUTION_SPAN.to_string();
    span.window_end = availability_row_end(row);
    span
}

fn availability_row_end(row: &AvailabilityBucketRow) -> i64 {
    if row.window_end > row.bucket_start {
        row.window_end
    } else {
        row.bucket_start + BUCKET_NANOS
    }
}

fn same_availability_run(span: &AvailabilityBucketRow, row: &AvailabilityBucketRow) -> bool {
    span.host_state == row.host_state
        && span.connectivity_state == row.connectivity_state
        && span.expected_observers == row.expected_observers
        && span.healthy_observers == row.healthy_observers
        && span.seen_observers == row.seen_observers
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;
    use crate::telemetry::rollup;

    const NODE: [u8; 16] = [8u8; 16];
    const MINUTE: i64 = MINUTE_NANOS;
    const HOUR: i64 = HOUR_NANOS;
    /// 2026-10-16T00:00:00Z，整分整小时。
    const NOW: i64 = 1_792_108_800_000_000_000;

    fn db(name: &str) -> (Db, String) {
        let dir =
            std::env::temp_dir().join(format!("santaizi-retention-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("panel.db").to_string_lossy().into_owned();
        (migrate::open_primary(&path).unwrap(), path)
    }

    fn count(db: &Db, table: &str) -> i64 {
        db.read(|conn| {
            Ok(
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })?,
            )
        })
        .unwrap()
    }

    fn scalar(db: &Db, query: &str) -> i64 {
        db.read(|conn| Ok(conn.query_row(query, [], |row| row.get(0))?))
            .unwrap()
    }

    fn insert_event(db: &Db, event_id: u8, event_type: i32, collected_at: i64, retained: bool) {
        db.write(|tx| {
            tx.execute(
                "INSERT INTO telemetry_events (event_id, node_uuid, session_id, sequence, event_type, priority, collected_at, \
                 agent_uptime_nano, session_elapsed_nano, clock_untrusted, source_protocol, reliability, payload, payload_retained, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 2, ?6, 0, 0, 0, 0, 0, ?7, ?8, CURRENT_TIMESTAMP)",
                rusqlite::params![
                    vec![event_id; 16],
                    NODE.to_vec(),
                    vec![event_id; 16],
                    event_id as i64,
                    event_type,
                    collected_at,
                    if retained { Some(vec![1u8, 2, 3]) } else { None },
                    retained as i64,
                ],
            )?;
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn drain_until_rejects_unknown_table() {
        let (db, _path) = db("allowlist");
        let err = drain_until(
            &db,
            Instant::now() + std::time::Duration::from_secs(5),
            100,
            "servers",
            "1 = 1",
            &[],
        )
        .unwrap_err();
        assert!(err.to_string().contains("allowlist"));
    }

    #[test]
    fn drain_until_deletes_matching_rows_only() {
        let (db, _path) = db("drain");
        db.exec_batch(
            "INSERT INTO telemetry_gaps (gap_id, node_uuid, session_id, start_sequence, end_sequence, reason, created_at_unix_nano, created_at) \
             VALUES (X'01', X'08080808080808080808080808080808', X'01', 1, 2, 1, 1, CURRENT_TIMESTAMP); \
             INSERT INTO telemetry_gaps (gap_id, node_uuid, session_id, start_sequence, end_sequence, reason, created_at_unix_nano, created_at) \
             VALUES (X'02', X'08080808080808080808080808080808', X'02', 1, 2, 1, 999999999999, CURRENT_TIMESTAMP);",
        )
        .unwrap();
        let deleted = drain_until(
            &db,
            Instant::now() + std::time::Duration::from_secs(5),
            100,
            "telemetry_gaps",
            "created_at_unix_nano < ?",
            &[&1_000_000_000i64],
        )
        .unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(count(&db, "telemetry_gaps"), 1);
    }

    #[test]
    fn expire_stale_probe_route_jobs_marks_pending_failed() {
        let (db, _path) = db("jobs");
        db.write(|tx| {
            tx.execute(
                "INSERT INTO probe_route_jobs (collector_uuid, server_id, protocol, status, requested_at) \
                 VALUES ('c1', 1, 'icmp', 'pending', ?)",
                rusqlite::params![NOW - 20 * MINUTE],
            )?;
            tx.execute(
                "INSERT INTO probe_route_jobs (collector_uuid, server_id, protocol, status, requested_at) \
                 VALUES ('c1', 1, 'icmp', 'pending', ?)",
                rusqlite::params![NOW],
            )?;
            tx.execute(
                "INSERT INTO probe_route_jobs (collector_uuid, server_id, protocol, status, requested_at) \
                 VALUES ('c1', 1, 'icmp', 'done', ?)",
                rusqlite::params![NOW - 20 * MINUTE],
            )?;
            Ok(())
        })
        .unwrap();

        expire_stale_probe_route_jobs(&db, NOW).unwrap();

        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM probe_route_jobs WHERE status = 'failed'"
            ),
            1
        );
        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM probe_route_jobs WHERE status = 'pending'"
            ),
            1
        );
        // 已完成的任务不动。
        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM probe_route_jobs WHERE status = 'done'"
            ),
            1
        );
    }

    #[test]
    fn prune_probe_routes_keeps_newest_per_key() {
        let (db, _path) = db("routes");
        for index in 0..12i64 {
            db.write(|tx| {
                tx.execute(
                    "INSERT INTO probe_routes (collector_uuid, server_id, protocol, sampled_at) \
                     VALUES ('c1', 7, 'icmp', ?)",
                    rusqlite::params![NOW - index * MINUTE],
                )?;
                Ok(())
            })
            .unwrap();
        }
        // 另一个 key 不受影响。
        db.write(|tx| {
            tx.execute(
                "INSERT INTO probe_routes (collector_uuid, server_id, protocol, sampled_at) \
                 VALUES ('c1', 7, 'tcp', ?)",
                rusqlite::params![NOW],
            )?;
            Ok(())
        })
        .unwrap();

        prune_probe_routes(&db, "c1", 7, "icmp", normalize_route_keep(0)).unwrap();

        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM probe_routes WHERE protocol = 'icmp'"
            ),
            10
        );
        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM probe_routes WHERE protocol = 'tcp'"
            ),
            1
        );
    }

    #[test]
    fn prune_all_probe_routes_uses_collector_route_keep() {
        let (db, _path) = db("routes-all");
        db.write(|tx| {
            tx.execute(
                "INSERT INTO collectors (collector_uuid, name, address, token_hash, token_ciphertext, kind, tcp_ports, route_keep, revoked, deleted) \
                 VALUES ('probe-1', 'p', 'addr', X'00', X'00', 'probe', '', 3, 0, 0)",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        for index in 0..5i64 {
            db.write(|tx| {
                tx.execute(
                    "INSERT INTO probe_routes (collector_uuid, server_id, protocol, sampled_at) \
                     VALUES ('probe-1', 7, 'icmp', ?)",
                    rusqlite::params![NOW - index * MINUTE],
                )?;
                Ok(())
            })
            .unwrap();
        }

        prune_all_probe_routes(&db).unwrap();

        assert_eq!(count(&db, "probe_routes"), 3);
    }

    #[test]
    fn prune_connection_latency_keeps_newest_per_partition() {
        let (db, _path) = db("latency");
        // 无 collectors 行 → keep = 1 * 100 = 100。
        for index in 0..150i64 {
            db.write(|tx| {
                tx.execute(
                    "INSERT INTO connection_latency_buckets (kind, collector_uuid, node_uuid, observer_id, bucket_start, count) \
                     VALUES ('path', 'c1', X'08080808080808080808080808080808', 'o1', ?, 1)",
                    rusqlite::params![NOW - index * MINUTE],
                )?;
                Ok(())
            })
            .unwrap();
        }
        // 另一分区不受影响。
        db.write(|tx| {
            tx.execute(
                "INSERT INTO connection_latency_buckets (kind, collector_uuid, node_uuid, observer_id, bucket_start, count) \
                 VALUES ('path', 'c1', X'08080808080808080808080808080808', 'o2', ?, 1)",
                rusqlite::params![NOW],
            )?;
            Ok(())
        })
        .unwrap();

        let deleted = prune_connection_latency_by_count(
            &db,
            Instant::now() + std::time::Duration::from_secs(10),
            1000,
        )
        .unwrap();

        assert_eq!(deleted, 50);
        // o1 分区保留最新 100 行，o2 分区 1 行不受影响。
        assert_eq!(count(&db, "connection_latency_buckets"), 101);
    }

    fn raw_bucket(bucket_start: i64, host_state: &str, seen: u32) -> AvailabilityBucketRow {
        AvailabilityBucketRow {
            node_uuid: NODE.to_vec(),
            bucket_start,
            window_end: bucket_start + BUCKET_NANOS,
            resolution: RESOLUTION_RAW.to_string(),
            host_state: host_state.to_string(),
            connectivity_state: "full".to_string(),
            expected_observers: 1,
            healthy_observers: 1,
            seen_observers: seen,
            revision: 1,
            finalized: true,
            ..Default::default()
        }
    }

    #[test]
    fn compact_availability_spans_merges_runs() {
        let (db, _path) = db("compact");
        let base = NOW - 2 * HOUR;
        db.write(|tx| {
            // 三段 online（相邻同态）→ 一段 span。
            for index in 0..3i64 {
                let bucket = raw_bucket(base + index * BUCKET_NANOS, "online", 1);
                tx.execute(
                    "INSERT INTO availability_buckets (node_uuid, bucket_start, window_end, resolution, host_state, \
                     connectivity_state, expected_observers, healthy_observers, seen_observers, revision, finalized, recalculated_at, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, CURRENT_TIMESTAMP)",
                    rusqlite::params![
                        bucket.node_uuid, bucket.bucket_start, bucket.window_end, bucket.resolution,
                        bucket.host_state, bucket.connectivity_state, bucket.expected_observers,
                        bucket.healthy_observers, bucket.seen_observers, bucket.revision,
                        bucket.finalized as i64, bucket.recalculated_at,
                    ],
                )?;
            }
            // 一段 offline（host_state 不同）独立成 span。
            let offline = raw_bucket(base + 3 * BUCKET_NANOS, "offline", 0);
            tx.execute(
                "INSERT INTO availability_buckets (node_uuid, bucket_start, window_end, resolution, host_state, \
                 connectivity_state, expected_observers, healthy_observers, seen_observers, revision, finalized, recalculated_at, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, CURRENT_TIMESTAMP)",
                rusqlite::params![
                    offline.node_uuid, offline.bucket_start, offline.window_end, offline.resolution,
                    offline.host_state, offline.connectivity_state, offline.expected_observers,
                    offline.healthy_observers, offline.seen_observers, offline.revision,
                    offline.finalized as i64, offline.recalculated_at,
                ],
            )?;
            Ok(())
        })
        .unwrap();

        let compacted = compact_availability_spans(
            &db,
            Instant::now() + std::time::Duration::from_secs(10),
            5000,
            NOW,
        )
        .unwrap();

        assert_eq!(compacted, 4);
        let spans = scalar(
            &db,
            "SELECT COUNT(*) FROM availability_buckets WHERE resolution = 'span'",
        );
        assert_eq!(spans, 2);
        // 原始桶已清空。
        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM availability_buckets WHERE resolution = '30s'"
            ),
            0
        );
        // online span 覆盖前三桶，终点为第三桶的 window_end。
        let online_span: (i64, i64) = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT bucket_start, window_end FROM availability_buckets \
                     WHERE resolution = 'span' AND host_state = 'online'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?)
            })
            .unwrap();
        assert_eq!(online_span.0, base);
        assert_eq!(online_span.1, base + 3 * BUCKET_NANOS);
    }

    #[test]
    fn drain_retention_clears_history_and_keeps_recent() {
        let (db, _path) = db("drain-all");
        let old = NOW - 40 * 24 * HOUR; // 超过 observation 默认 30d
        let lifecycle_old = NOW - 11 * 365 * 24 * HOUR; // 超过 lifecycle 默认 10y

        insert_event(&db, 1, proto::TelemetryEventType::Host as i32, old, true);
        insert_event(
            &db,
            2,
            proto::TelemetryEventType::DataLoss as i32,
            old,
            false,
        );
        // LIFECYCLE 活得比 observation 久，只受 lifecycle 约束。
        insert_event(
            &db,
            3,
            proto::TelemetryEventType::Lifecycle as i32,
            lifecycle_old,
            false,
        );
        insert_event(
            &db,
            4,
            proto::TelemetryEventType::Lifecycle as i32,
            old,
            false,
        );
        // 新事件必须留下。
        insert_event(
            &db,
            5,
            proto::TelemetryEventType::DataLoss as i32,
            NOW,
            false,
        );

        db.write(|tx| {
            tx.execute(
                "INSERT INTO state_rollups (node_uuid, resolution, window_start, window_end, sample_count, payload, net_in_total, net_out_total, created_at) \
                 VALUES (?1, '1m', ?2, ?3, 1, X'00', 0, 0, CURRENT_TIMESTAMP)",
                rusqlite::params![NODE.to_vec(), NOW - 40 * 24 * HOUR, NOW - 40 * 24 * HOUR + MINUTE],
            )?;
            tx.execute(
                "INSERT INTO state_rollups (node_uuid, resolution, window_start, window_end, sample_count, payload, net_in_total, net_out_total, created_at) \
                 VALUES (?1, '1m', ?2, ?3, 1, X'00', 0, 0, CURRENT_TIMESTAMP)",
                rusqlite::params![NODE.to_vec(), NOW - MINUTE, NOW],
            )?;
            Ok(())
        })
        .unwrap();

        let deleted = drain_retention(&db, &rollup::RetentionPolicy::default(), NOW).unwrap();
        assert!(deleted > 0);

        // 旧 HOST 事件整行删掉（observation 超期）。
        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM telemetry_events WHERE event_type = 3"
            ),
            0
        );
        // 旧非 lifecycle 事件删掉。
        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM telemetry_events WHERE event_type = 6"
            ),
            1
        );
        // 旧 lifecycle 删掉、新的留下。
        assert_eq!(
            scalar(
                &db,
                "SELECT COUNT(*) FROM telemetry_events WHERE event_type = 4"
            ),
            1
        );
        // 新事件在。
        assert_eq!(count(&db, "telemetry_events"), 2);
        // 1m rollup：超 30d 的删、新的留。
        assert_eq!(count(&db, "state_rollups"), 1);
    }

    #[test]
    fn merge_availability_runs_handles_empty_and_single() {
        assert!(merge_availability_runs(&[]).is_empty());
        let single = vec![raw_bucket(NOW, "online", 1)];
        let spans = merge_availability_runs(&single);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].resolution, RESOLUTION_SPAN);
        assert_eq!(spans[0].window_end, NOW + BUCKET_NANOS);
    }
}
