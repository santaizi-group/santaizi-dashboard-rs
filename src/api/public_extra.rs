//! Public v2 扩展端点：对齐 Go `api_v2_public_extra.go` 与
//! `api_v2_spa.go::v2PublicCycleTransfer` 的 `/servers/:id/availability`、
//! `/metrics/:id`、`/cycle-transfer`，以及监控派发就绪后的
//! `/services`（服务监控列表 + 24h 聚合）与 `/network/:id`（24h 延迟序列）。

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use prost::Message as _;
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::proto;
use crate::repo::{availability, bindings, traffic};
use crate::rpc::support::blocking_read;
use crate::telemetry::rollup::{self, StateRollupRow};
use crate::util::time as util_time;

use super::auth::{ApiShared, OptionalUser, data_json, list_json, problem};
use super::public::{require_public_access, snapshot};

/// `show_availability_to_guest` 关闭时仅登录会员可见
/// （Go `publicAvailabilityAllowed`；查看密码会话不放行可用性）。
fn availability_allowed(shared: &ApiShared, user: Option<&super::auth::CurrentUser>) -> bool {
    shared.conf().show_availability_to_guest || user.is_some()
}

/// `GET /api/v2/public/servers/{id}/availability`（Go `v2PublicServerAvailability`）。
async fn server_availability(
    State(shared): State<ApiShared>,
    Path(server_id): Path<i64>,
    OptionalUser(user): OptionalUser,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_public_access(&headers, &shared, user.as_ref()) {
        return *response;
    }
    if !availability_allowed(&shared, user.as_ref()) {
        return problem(
            StatusCode::FORBIDDEN,
            "availability_hidden",
            "前台可用性展示已关闭",
        );
    }
    if server_id <= 0 {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_server_id",
            "server_id 无效",
        );
    }
    let rows = match snapshot(&headers, &shared, user.as_ref()).await {
        Ok(rows) => rows,
        Err(response) => return *response,
    };
    if !rows.iter().any(|row| row.id == server_id) {
        return problem(StatusCode::NOT_FOUND, "server_not_found", "服务器不存在");
    }
    // Go：缺省 "30"，解析失败或 <1 一律回落 30，超上限截断。
    let days = params
        .get("days")
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|days| *days >= 1)
        .unwrap_or(30)
        .min(availability::MAX_SUMMARY_DAYS);
    let now_nanos = util_time::now_nanos();
    let summary = blocking_read(&shared.db, move |conn| {
        availability::summarize(conn, server_id, days, now_nanos)
    })
    .await;
    match summary {
        Ok(summary) => data_json(json!({
            "server_id": summary.server_id,
            "days": summary.days,
            "offline_count": summary.offline_count,
            "total_offline_seconds": summary.total_offline_seconds,
            "longest_offline_seconds": summary.longest_offline_seconds,
            "availability_percent": summary.availability_percent,
        })),
        Err(err) => {
            tracing::warn!(error = %err, "availability summary query failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "查询可用性失败",
            )
        }
    }
}

fn normalize_metric_resolution(resolution: &str) -> &'static str {
    if resolution == "1h" { "1h" } else { "1m" }
}

/// Go `time.Hour`：一小时的纳秒数（小时窗与跨度都按它换算）。
const HOUR_NANOS: i64 = 3_600_000_000_000;

/// 指标窗口上限（小时）：1h 用 `state_one_hour_days`，1m 用
/// `state_one_minute_days`；0 值回落默认（Go 同款 0 → 365 / 30）。
fn metric_max_hours(shared: &ApiShared, resolution: &str) -> f64 {
    let resolution = normalize_metric_resolution(resolution);
    let days = if resolution == "1h" {
        let days = shared.conf().retention.state_one_hour_days;
        if days == 0 { 365 } else { days }
    } else {
        let days = shared.conf().retention.state_one_minute_days;
        if days == 0 { 30 } else { days }
    };
    (days as i64 * 24) as f64
}

/// Go `parsePublicMetricTime`：纯数字且长度 ≥10 视为 Unix 秒，否则 RFC3339。
/// 返回 Unix 纳秒。
fn parse_metric_time(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(unix_secs) = raw.parse::<i64>()
        && raw.len() >= 10
    {
        return unix_secs.checked_mul(1_000_000_000);
    }
    OffsetDateTime::parse(raw, &Rfc3339)
        .ok()
        .map(OffsetDateTime::unix_timestamp_nanos)
        .and_then(|nanos| i64::try_from(nanos).ok())
}

/// Go `resolvePublicMetricRange` 的 problem 形态。
#[derive(Debug)]
struct MetricRangeProblem {
    status: StatusCode,
    code: &'static str,
    detail: &'static str,
}

fn metric_problem(status: StatusCode, code: &'static str, detail: &'static str) -> Response {
    problem(status, code, detail)
}

/// 解析 start/end 或 hours 查询窗。返回 `(resolution, from_nanos, to_nanos)`。
fn resolve_metric_range(
    shared: &ApiShared,
    resolution: &str,
    hours_raw: Option<&str>,
    start_raw: Option<&str>,
    end_raw: Option<&str>,
    now_nanos: i64,
) -> Result<(&'static str, i64, i64), MetricRangeProblem> {
    let start_raw = start_raw.unwrap_or("").trim();
    let end_raw = end_raw.unwrap_or("").trim();
    if !start_raw.is_empty() || !end_raw.is_empty() {
        if start_raw.is_empty() || end_raw.is_empty() {
            return Err(MetricRangeProblem {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_range",
                detail: "start 与 end 必须成对提供",
            });
        }
        let Some(start) = parse_metric_time(start_raw) else {
            return Err(MetricRangeProblem {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_start",
                detail: "start 必须是 RFC3339 或 Unix 秒",
            });
        };
        let Some(end) = parse_metric_time(end_raw) else {
            return Err(MetricRangeProblem {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_end",
                detail: "end 必须是 RFC3339 或 Unix 秒",
            });
        };
        if end <= start {
            return Err(MetricRangeProblem {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_range",
                detail: "end 必须晚于 start",
            });
        }
        let resolution = normalize_metric_resolution(resolution);
        // Go：float64 小时 → Duration（纳秒）截断后再比较。
        let max_span = (metric_max_hours(shared, resolution) * HOUR_NANOS as f64) as i64;
        if end - start > max_span {
            return Err(MetricRangeProblem {
                status: StatusCode::BAD_REQUEST,
                code: "range_too_large",
                detail: "查询跨度超过保留期",
            });
        }
        return Ok((resolution, start, end));
    }
    let mut hours = 24.0f64;
    if let Some(hours_raw) = hours_raw.map(str::trim).filter(|raw| !raw.is_empty()) {
        match hours_raw.parse::<f64>() {
            Ok(parsed) if parsed >= 0.5 => hours = parsed,
            _ => {
                return Err(MetricRangeProblem {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_hours",
                    detail: "hours 无效",
                });
            }
        }
    }
    let resolution = normalize_metric_resolution(resolution);
    if hours <= 0.0 {
        hours = 24.0;
    }
    let max_hours = metric_max_hours(shared, resolution);
    if hours > max_hours {
        hours = max_hours;
    }
    let span = (hours * HOUR_NANOS as f64) as i64;
    Ok((resolution, now_nanos - span, now_nanos))
}

/// Go `publicMetricWindowSeconds`：window 差优先，退化回分辨率时长。
fn metric_window_seconds(row: &StateRollupRow, resolution: &str) -> i64 {
    if row.window_end > row.window_start {
        let seconds = (row.window_end - row.window_start) / 1_000_000_000;
        if seconds > 0 {
            return seconds;
        }
    }
    if normalize_metric_resolution(resolution) == "1h" {
        3600
    } else {
        60
    }
}

/// Go `publicMetricSpeed`：avg 优先；avg 为 0 且有总量时按总量/窗秒回推。
fn metric_speed(average: u64, total: u64, window_seconds: i64) -> u64 {
    if average > 0 || total == 0 || window_seconds <= 0 {
        return average;
    }
    total / window_seconds as u64
}

/// Go `decodePublicMetricPoints`：rollup 行 → 前台指标点。
/// payload 解码失败的行整行跳过；avg 缺省按空 State 处理。
fn decode_metric_points(rows: &[StateRollupRow], resolution: &str) -> Vec<Value> {
    rows.iter()
        .filter_map(|row| {
            let payload = proto::StateRollupPayload::decode(row.payload.as_slice()).ok()?;
            let average = payload.average.clone().unwrap_or_default();
            let window_seconds = metric_window_seconds(row, resolution);
            Some(json!({
                "window_start": util_time::unix_nanos_to_rfc3339(row.window_start),
                "cpu": average.cpu,
                "mem_used": average.mem_used,
                "disk_used": average.disk_used,
                "net_in_speed": metric_speed(average.net_in_speed, payload.net_in_total, window_seconds),
                "net_out_speed": metric_speed(average.net_out_speed, payload.net_out_total, window_seconds),
                "net_in_total": payload.net_in_total,
                "net_out_total": payload.net_out_total,
                "process_count": average.process_count,
                "tcp_conn_count": average.tcp_conn_count,
                "udp_conn_count": average.udp_conn_count,
            }))
        })
        .collect()
}

/// `GET /api/v2/public/metrics/{id}`（Go `v2PublicMetrics`）。
async fn metrics(
    State(shared): State<ApiShared>,
    Path(server_id): Path<i64>,
    OptionalUser(user): OptionalUser,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_public_access(&headers, &shared, user.as_ref()) {
        return *response;
    }
    if server_id <= 0 {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_server_id",
            "server_id 无效",
        );
    }
    let rows = match snapshot(&headers, &shared, user.as_ref()).await {
        Ok(rows) => rows,
        Err(response) => return *response,
    };
    if !rows.iter().any(|row| row.id == server_id) {
        return problem(StatusCode::NOT_FOUND, "server_not_found", "服务器不存在");
    }
    let resolution = params.get("resolution").map(String::as_str).unwrap_or("1m");
    let range = resolve_metric_range(
        &shared,
        resolution,
        params.get("hours").map(String::as_str),
        params.get("start").map(String::as_str),
        params.get("end").map(String::as_str),
        util_time::now_nanos(),
    );
    let (resolution, from_nanos, to_nanos) = match range {
        Ok(range) => range,
        Err(problem) => return metric_problem(problem.status, problem.code, problem.detail),
    };
    let result = blocking_read(&shared.db, move |conn| {
        let Some(binding) = bindings::current_binding_for_server(conn, server_id)? else {
            return Ok(Vec::new());
        };
        rollup::list_window(conn, &binding.node_uuid, resolution, from_nanos, to_nanos)
    })
    .await;
    match result {
        Ok(rows) => {
            let items = decode_metric_points(&rows, resolution);
            let total = items.len() as i64;
            list_json(Value::Array(items), total)
        }
        Err(err) => {
            tracing::warn!(error = %err, "public metrics query failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "查询指标失败",
            )
        }
    }
}

/// Go `publicWarningBytes`：`(quota * percent / 100)` 截断为 u64。
fn warning_bytes(quota: u64, warning_percent: f64) -> u64 {
    if quota == 0 || warning_percent <= 0.0 {
        return 0;
    }
    (quota as f64 * warning_percent / 100.0) as u64
}

/// Go `publicRemainingBytes`。
fn remaining_bytes(used: u64, quota: u64) -> u64 {
    quota.saturating_sub(used)
}

/// `GET /api/v2/public/cycle-transfer`（Go `v2PublicCycleTransfer`）。
async fn cycle_transfer(
    State(shared): State<ApiShared>,
    OptionalUser(user): OptionalUser,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_public_access(&headers, &shared, user.as_ref()) {
        return *response;
    }
    let server_id = match params.get("server_id").map(String::as_str) {
        Some(raw) if !raw.trim().is_empty() => match raw.trim().parse::<i64>() {
            Ok(id) if id > 0 => Some(id),
            _ => {
                return problem(
                    StatusCode::BAD_REQUEST,
                    "invalid_server_id",
                    "server_id 无效",
                );
            }
        },
        _ => None,
    };
    let include_hidden = user.is_some() || super::public::view_password_verified(&headers, &shared);
    let location = shared.conf().location.clone();
    let now_nanos = util_time::now_nanos();
    let rows = blocking_read(&shared.db, move |conn| {
        let rows = traffic::list_public_enabled(conn, server_id, include_hidden)?;
        // Go 对 Calculate 报错的行静默省略 usage 字段——这里同样按 None 处理。
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let usage =
                traffic::usage_in_location(conn, &row, now_nanos, &location).unwrap_or(None);
            out.push((row, usage));
        }
        Ok(out)
    })
    .await;
    let rows = match rows {
        Ok(rows) => rows,
        Err(err) => {
            tracing::warn!(error = %err, "cycle transfer query failed");
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "查询流量策略失败",
            );
        }
    };
    let items: Vec<Value> = rows
        .into_iter()
        .map(|(row, usage)| {
            let mut item = json!({
                "policy_id": row.id,
                "server_id": row.server_id,
                "name": row.name,
                "direction": row.direction,
                "mode": row.mode,
                "cycle_unit": row.cycle_unit,
            });
            let Some(usage) = usage else {
                return item;
            };
            let window_start = util_time::unix_nanos_to_rfc3339(usage.window_start);
            let next_reset_at = usage
                .window_end
                .map(util_time::unix_nanos_to_rfc3339)
                .map(|window_end| json!({ "window_end": window_end, "next_reset_at": window_end }));
            if let Some(extra) = next_reset_at {
                item["window_end"] = extra["window_end"].clone();
                item["next_reset_at"] = extra["next_reset_at"].clone();
            } else {
                item["next_reset_at"] = Value::Null;
            }
            item["window_start"] = Value::String(window_start);
            item["used_bytes"] = json!(usage.used_bytes);
            item["quota_bytes"] = json!(usage.quota_bytes);
            item["usage_percent"] = json!(usage.usage_percent);
            item["status"] = json!(usage.status);
            item["warning_percent"] = json!(usage.warning_percent);
            item["warning_bytes"] = json!(warning_bytes(usage.quota_bytes, usage.warning_percent));
            item["remaining_bytes"] = json!(remaining_bytes(usage.used_bytes, usage.quota_bytes));
            item
        })
        .collect();
    let total = items.len() as i64;
    list_json(Value::Array(items), total)
}

struct ServiceMonthSeries {
    up: Vec<i64>,
    down: Vec<i64>,
    delay: Vec<f64>,
    current_up: i64,
    current_down: i64,
}

/// 最近 30 个面板配置时区日键，顺序 `[29 天前, …, 今天]`（不依赖容器系统 `'localtime'`）。
fn recent_local_days_in_location(now_nanos: i64, location: &str) -> Vec<String> {
    let (ty, tm, td) = util_time::local_ymd_in_location(now_nanos, location);
    let today_days = util_time::days_from_civil(ty, tm, td)
        .unwrap_or_else(|| now_nanos.div_euclid(86_400_000_000_000));
    let mut days = Vec::with_capacity(30);
    for offset in (0..30).rev() {
        let day_nanos = (today_days - offset) * 86_400_000_000_000;
        let (y, m, d) = util_time::ymd_from_nanos(day_nanos);
        days.push(format!("{y:04}-{m:02}-{d:02}"));
    }
    days
}

/// 全部对外展示 monitor 的 30 天日聚合 + 自适应当前窗口，各一条查询。
/// 日序列按面板 `location` 偏移切分；`current_up/current_down` 取至少最近
/// `max(300, duration * 3)` 秒（并保底 900s 内最新样本窗口），避免长间隔监控周期性归零。
fn service_month_series(
    conn: &rusqlite::Connection,
    days: &[String],
    monitor_ids: &[i64],
    now_nanos: i64,
    location: &str,
) -> anyhow::Result<std::collections::HashMap<i64, ServiceMonthSeries>> {
    let window_start_day = days
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("empty day window"))?;
    let now_secs = now_nanos.div_euclid(1_000_000_000);
    let offset_secs = util_time::offset_seconds_for_location(location, now_secs);
    let offset_modifier = format!("{offset_secs:+} seconds");
    // 先用宽松 UTC 下界命中 created_at 索引，再按面板时区日键精确过滤。
    let utc_floor = util_time::sqlite_to_datetime_text(&util_time::unix_nanos_to_sqlite(
        now_nanos.saturating_sub(32 * 86_400_000_000_000),
    ));
    let mut daily: std::collections::HashMap<(i64, String), (i64, i64, f64)> =
        std::collections::HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT mh.monitor_id, date(mh.created_at, ?1), \
             COALESCE(SUM(mh.up), 0), COALESCE(SUM(mh.down), 0), COALESCE(AVG(mh.avg_delay), 0) \
             FROM monitor_histories mh \
             INNER JOIN monitors m ON m.id = mh.monitor_id \
             WHERE mh.deleted_at IS NULL AND m.deleted_at IS NULL \
               AND COALESCE(m.enable_show_in_service, 0) != 0 \
               AND mh.created_at >= ?2 \
               AND date(mh.created_at, ?1) >= ?3 \
             GROUP BY mh.monitor_id, date(mh.created_at, ?1)",
        )?;
        let rows = stmt.query_map(
            rusqlite::params![&offset_modifier, &utc_floor, &window_start_day],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    (
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, f64>(4)?,
                    ),
                ))
            },
        )?;
        for row in rows {
            let (id, day, value) = row?;
            daily.insert((id, day), value);
        }
    }
    let now_dt = util_time::sqlite_to_datetime_text(&util_time::unix_nanos_to_sqlite(now_nanos));
    let mut current: std::collections::HashMap<i64, (i64, i64)> = std::collections::HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT mh.monitor_id, COALESCE(SUM(mh.up), 0), COALESCE(SUM(mh.down), 0) \
             FROM monitor_histories mh \
             INNER JOIN monitors m ON m.id = mh.monitor_id \
             WHERE mh.deleted_at IS NULL AND m.deleted_at IS NULL \
               AND COALESCE(m.enable_show_in_service, 0) != 0 \
               AND datetime(mh.created_at) >= datetime( \
                   ?1, \
                   '-' || CAST(MAX(900, COALESCE(m.duration, 30) * 3) AS TEXT) || ' seconds' \
               ) \
             GROUP BY mh.monitor_id",
        )?;
        let rows = stmt.query_map([&now_dt], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                (row.get::<_, i64>(1)?, row.get::<_, i64>(2)?),
            ))
        })?;
        for row in rows {
            let (id, value) = row?;
            current.insert(id, value);
        }
    }
    let mut out = std::collections::HashMap::new();
    for id in monitor_ids {
        let mut up = Vec::with_capacity(days.len());
        let mut down = Vec::with_capacity(days.len());
        let mut delay = Vec::with_capacity(days.len());
        for day in days {
            let (day_up, day_down, day_delay) = daily
                .get(&(*id, day.clone()))
                .copied()
                .unwrap_or((0, 0, 0.0));
            up.push(day_up);
            down.push(day_down);
            delay.push(day_delay);
        }
        let (current_up, current_down) = current.get(id).copied().unwrap_or((0, 0));
        out.insert(
            *id,
            ServiceMonthSeries {
                up,
                down,
                delay,
                current_up,
                current_down,
            },
        );
    }
    Ok(out)
}

// ---------- services / network（监控派发 Phase 6 接线后启用） ----------

/// monitor 类型 → 展示名（Go `monitorTypeName`）。
fn monitor_type_label(value: i64) -> &'static str {
    match value {
        crate::repo::typed_resources::MONITOR_TYPE_HTTP => "http",
        crate::repo::typed_resources::MONITOR_TYPE_ICMP => "icmp",
        crate::repo::typed_resources::MONITOR_TYPE_TCP => "tcp",
        _ => "",
    }
}

/// `GET /api/v2/public/services`：对外展示的服务监控。
/// `up` / `down` / `delay` 各 30 个点，按面板配置时区日聚合最近 30 天（含未结束的今天）；
/// `current_*` 按监控间隔自适应取最近窗口。
async fn public_services(
    State(shared): State<ApiShared>,
    OptionalUser(user): OptionalUser,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_public_access(&headers, &shared, user.as_ref()) {
        return *response;
    }
    let location = shared.conf().location.clone();
    let now_nanos = util_time::now_nanos();
    let result = blocking_read(&shared.db, move |conn| {
        let mut stmt = conn.prepare(
            "SELECT m.id, COALESCE(m.name, ''), COALESCE(m.type, 0), COALESCE(m.target, ''), \
             COALESCE(m.duration, 30), COALESCE(m.latency_notify, 0), \
             COALESCE(m.min_latency, 0), COALESCE(m.max_latency, 0) \
             FROM monitors m WHERE m.deleted_at IS NULL AND COALESCE(m.enable_show_in_service, 0) != 0 \
             ORDER BY m.id ASC",
        )?;
        let mapped = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)? != 0,
                row.get::<_, f64>(6)?,
                row.get::<_, f64>(7)?,
            ))
        })?;
        let mut items = Vec::new();
        let mut monitor_rows = Vec::new();
        for row in mapped {
            monitor_rows.push(row?);
        }
        let days = recent_local_days_in_location(now_nanos, &location);
        let monitor_ids: Vec<i64> = monitor_rows.iter().map(|row| row.0).collect();
        let series = service_month_series(conn, &days, &monitor_ids, now_nanos, &location)?;
        for (id, name, monitor_type, target, interval, latency_notify, min_ms, max_ms) in
            monitor_rows
        {
            let series = series.get(&id);
            items.push(json!({
                "id": id,
                "name": name,
                "type": monitor_type_label(monitor_type),
                "target": target,
                "interval_seconds": interval,
                "latency_notify": latency_notify,
                "min_latency_ms": min_ms,
                "max_latency_ms": max_ms,
                "up": series.map(|s| s.up.clone()).unwrap_or_default(),
                "down": series.map(|s| s.down.clone()).unwrap_or_default(),
                "delay": series.map(|s| s.delay.clone()).unwrap_or_default(),
                "current_up": series.map(|s| s.current_up).unwrap_or(0),
                "current_down": series.map(|s| s.current_down).unwrap_or(0),
            }));
        }
        Ok(items)
    })
    .await;
    match result {
        Ok(items) => {
            let total = items.len() as i64;
            list_json(Value::Array(items), total)
        }
        Err(err) => {
            tracing::warn!(error = %err, "public services query failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "查询服务监控失败",
            )
        }
    }
}

/// `GET /api/v2/public/network/:id`（Go `v2PublicNetwork` → MonitorAPI）：
/// 近 24h 该服务器的对外监控延迟序列，按 monitor 分组（created_at 毫秒）。
async fn public_network(
    State(shared): State<ApiShared>,
    OptionalUser(user): OptionalUser,
    Path(server_id): Path<i64>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = require_public_access(&headers, &shared, user.as_ref()) {
        return *response;
    }
    if server_id <= 0 {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_server_id",
            "server_id 无效",
        );
    }
    let rows = match snapshot(&headers, &shared, user.as_ref()).await {
        Ok(rows) => rows,
        Err(response) => return *response,
    };
    let Some(target_server) = rows.iter().find(|row| row.id == server_id) else {
        return problem(StatusCode::NOT_FOUND, "server_not_found", "服务器不存在");
    };
    let server_name = target_server.name.clone();
    let result = blocking_read(&shared.db, move |conn| {
        let mut stmt = conn.prepare(
            "SELECT h.monitor_id, COALESCE(m.name, ''), h.created_at, COALESCE(h.avg_delay, 0) \
             FROM monitor_histories h INNER JOIN monitors m ON m.id = h.monitor_id \
             WHERE h.server_id = ?1 AND h.deleted_at IS NULL AND h.created_at >= ?2 \
               AND m.deleted_at IS NULL AND COALESCE(m.enable_show_in_service, 0) != 0 \
             ORDER BY h.monitor_id ASC, h.created_at ASC",
        )?;
        // `unix_nanos_to_sqlite` 产 `…+00:00`，而 `monitor_histories.created_at` 由
        // `CURRENT_TIMESTAMP` 写成无后缀形态；同秒内无后缀串恒「小于」带后缀串，
        // 会把窗口边界那一秒的行漏掉。两侧都走 `datetime()` 归一化再比。
        let window_start =
            crate::util::time::sqlite_to_datetime_text(&crate::util::time::unix_nanos_to_sqlite(
                crate::util::time::now_nanos() - 24 * 3_600_000_000_000,
            ));
        let mapped = stmt.query_map(rusqlite::params![server_id, window_start], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, f64>(3)?,
            ))
        })?;
        let mut series: Vec<(i64, String, Vec<i64>, Vec<f64>)> = Vec::new();
        for row in mapped {
            let (monitor_id, monitor_name, created_at, avg_delay) = row?;
            let created_ms = crate::util::time::sqlite_to_unix_nanos(&created_at)
                .map(|nanos| nanos / 1_000_000)
                .unwrap_or(0);
            match series.last_mut() {
                Some((last_id, _, created, delays)) if *last_id == monitor_id => {
                    created.push(created_ms);
                    delays.push(avg_delay);
                }
                _ => series.push((monitor_id, monitor_name, vec![created_ms], vec![avg_delay])),
            }
        }
        Ok(series)
    })
    .await;
    match result {
        Ok(series) => {
            let items: Vec<Value> = series
                .into_iter()
                .map(|(monitor_id, monitor_name, created, delays)| {
                    json!({
                        "monitor_id": monitor_id,
                        "server_id": server_id,
                        "monitor_name": monitor_name,
                        "server_name": server_name,
                        "created_at": created,
                        "avg_delay": delays,
                    })
                })
                .collect();
            let total = items.len() as i64;
            list_json(Value::Array(items), total)
        }
        Err(err) => {
            tracing::warn!(error = %err, "public network query failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "查询网络监控失败",
            )
        }
    }
}

/// public 扩展路由组（Go `public.GET` 注册）。
pub fn routes() -> axum::Router<ApiShared> {
    axum::Router::new()
        .route(
            "/api/v2/public/servers/{serverId}/availability",
            axum::routing::get(server_availability),
        )
        .route(
            "/api/v2/public/metrics/{serverId}",
            axum::routing::get(metrics),
        )
        .route(
            "/api/v2/public/cycle-transfer",
            axum::routing::get(cycle_transfer),
        )
        .route(
            "/api/v2/public/services",
            axum::routing::get(public_services),
        )
        .route(
            "/api/v2/public/network/{serverId}",
            axum::routing::get(public_network),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;

    #[test]
    fn service_series_uses_panel_location_and_adaptive_current_window() {
        let path = std::env::temp_dir().join(format!(
            "santaizi-public-extra-series-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let db = migrate::open_primary(&path.to_string_lossy()).unwrap();
        // 2026-10-16 01:00:00 +08:00 == 2026-10-15 17:00:00Z
        // UTC 仍是 10-15，东八区已是 10-16。
        let now = util_time::sqlite_to_unix_nanos("2026-10-16 01:00:00+08:00").unwrap();
        // 样本在 20 分钟前（2026-10-16 00:40:00+08:00 == 2026-10-15 16:40:00Z）：
        // 对 600s（10m）间隔的 monitor 而言，固定 -5m 会归零，自适应窗口（max(900, 1800)=1800s）仍命中。
        let sample_ts = util_time::sqlite_to_datetime_text(&util_time::unix_nanos_to_sqlite(
            now - 20 * 60 * 1_000_000_000,
        ));
        db.write(|tx| {
            tx.execute(
                "INSERT INTO monitors (id, name, type, target, duration, enable_show_in_service) \
                 VALUES (1, 'long-check', 1, 'https://example.com', 600, 1)",
                [],
            )?;
            tx.execute(
                "INSERT INTO monitor_histories (monitor_id, server_id, created_at, avg_delay, up, down) \
                 VALUES (1, 1, ?1, 42.0, 5, 1)",
                [&sample_ts],
            )?;
            Ok(())
        })
        .unwrap();

        let days_cst = recent_local_days_in_location(now, "Asia/Shanghai");
        assert_eq!(days_cst.len(), 30);
        assert_eq!(days_cst.last().map(String::as_str), Some("2026-10-16"));

        let series = db
            .read(|conn| service_month_series(conn, &days_cst, &[1], now, "Asia/Shanghai"))
            .unwrap();
        let s = series.get(&1).unwrap();
        assert_eq!(
            s.up.last().copied(),
            Some(5),
            "东八区凌晨样本应落入 10-16 当日桶"
        );
        assert_eq!(s.current_up, 5, "长间隔监控在自适应窗口内不得归零");
        assert_eq!(s.current_down, 1);
        let _ = std::fs::remove_file(&path);
    }
}
