//! Admin 读侧端点（Phase 5）：summary / telemetry（overview + 六 dataset）/
//! connections（summary/paths/latency）/ probes（summary/paths/samples/trace/
//! route）/ offline-history / database / monitors history / admin 可用性桶，
//! 对齐 Go `api_v2_spa.go` 对应 handler。

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::repo::insights;
use crate::repo::servers;
use crate::rpc::support::{blocking_read, blocking_write};
use crate::util::time as util_time;

use super::auth::{AdminUser, ApiShared, data_json, problem};
use super::pagination::{offset_of, parse_page};
use super::presentation;

/// 游标分页信封（Go monitor history / admin availability：meta 只带 next_cursor）。
fn list_cursor(data: Vec<Value>, next_cursor: String) -> Response {
    let mut meta = json!({});
    if !next_cursor.is_empty() {
        meta["next_cursor"] = json!(next_cursor);
    }
    let body = json!({"data": data, "meta": meta});
    let mut response = Response::new(serde_json::to_string(&body).unwrap_or_default().into());
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );
    response
}

/// `{"data": [...], "meta": {page, page_size, total, next_cursor(可选)}}`。
fn list_meta(
    data: Vec<Value>,
    page: i64,
    page_size: i64,
    total: i64,
    next_cursor: String,
) -> Response {
    let mut meta = json!({"page": page, "page_size": page_size, "total": total});
    if !next_cursor.is_empty() {
        meta["next_cursor"] = json!(next_cursor);
    }
    let body = json!({"data": data, "meta": meta});
    let mut response = Response::new(serde_json::to_string(&body).unwrap_or_default().into());
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );
    response
}

fn database_error(err: anyhow::Error, detail: &'static str) -> Response {
    tracing::warn!(error = %err, "{detail}");
    problem(StatusCode::INTERNAL_SERVER_ERROR, "database_error", detail)
}

/// `GET /api/v2/admin/summary`（Go `v2AdminSummary`）。
async fn admin_summary(State(shared): State<ApiShared>, AdminUser(_admin): AdminUser) -> Response {
    let now_nanos = util_time::now_nanos();
    let threshold = shared.conf().telemetry.offline_threshold_seconds;
    let result = blocking_read(&shared.db, move |conn| {
        let total_servers: i64 = conn.query_row(
            "SELECT COUNT(*) FROM servers WHERE deleted_at IS NULL",
            [],
            |row| row.get(0),
        )?;
        let active_incidents: i64 = conn.query_row(
            "SELECT COUNT(*) FROM availability_incidents WHERE ended_at = 0",
            [],
            |row| row.get(0),
        )?;
        let data_loss: i64 =
            conn.query_row("SELECT COUNT(*) FROM telemetry_data_losses", [], |row| {
                row.get(0)
            })?;
        let telemetry_alerts: i64 =
            conn.query_row("SELECT COUNT(*) FROM telemetry_alerts", [], |row| {
                row.get(0)
            })?;
        let rows = servers::list_public(conn, true, now_nanos)?;
        let online_servers = rows
            .iter()
            .map(presentation::runtime_presentation)
            .filter(|snapshot| presentation::online_flag(snapshot, threshold))
            .count() as i64;
        let connection = insights::load_connection_summary(conn, now_nanos)?;
        Ok((
            total_servers,
            online_servers,
            active_incidents,
            data_loss,
            telemetry_alerts,
            connection,
        ))
    })
    .await;
    match result {
        Ok((
            total_servers,
            online_servers,
            active_incidents,
            data_loss,
            telemetry_alerts,
            connection,
        )) => data_json(json!({
            "total_servers": total_servers,
            "online_servers": online_servers,
            "active_collectors": connection.collectors_online,
            "active_incidents": active_incidents,
            "data_loss": data_loss,
            "telemetry_alerts": telemetry_alerts,
            "telemetry_pending": 0,
            "collectors_offline": connection.collectors_offline,
            "paths_assigned": connection.paths_assigned,
            "paths_connected": connection.paths_connected,
        })),
        Err(err) => database_error(err, "汇总查询失败"),
    }
}

/// `GET /api/v2/admin/telemetry/overview`。
async fn telemetry_overview(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
) -> Response {
    let result = blocking_read(&shared.db, |conn| {
        Ok((
            conn.query_row(
                "SELECT COUNT(*) FROM collectors WHERE deleted = 0",
                [],
                |row| row.get::<_, i64>(0),
            )?,
            conn.query_row("SELECT COUNT(*) FROM agent_telemetry_runtimes", [], |row| {
                row.get::<_, i64>(0)
            })?,
            conn.query_row(
                "SELECT COUNT(*) FROM observer_assignments WHERE valid_to = 0",
                [],
                |row| row.get::<_, i64>(0),
            )?,
            conn.query_row("SELECT COUNT(*) FROM telemetry_data_losses", [], |row| {
                row.get::<_, i64>(0)
            })?,
            conn.query_row("SELECT COUNT(*) FROM telemetry_alerts", [], |row| {
                row.get::<_, i64>(0)
            })?,
        ))
    })
    .await;
    match result {
        Ok((collectors, agents, assignments, data_loss, alerts)) => data_json(json!({
            "collectors": collectors,
            "agents": agents,
            "active_assignments": assignments,
            "data_loss": data_loss,
            "alerts": alerts,
        })),
        Err(err) => database_error(err, "遥测概览查询失败"),
    }
}

/// `GET /api/v2/admin/telemetry/{dataset}`（六 dataset + 别名兜底）。
async fn telemetry_dataset(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(dataset): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let (page, size) = parse_page(&params);
    let offset = offset_of(page, size);
    let open_only = params
        .get("open")
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    let result = blocking_read(&shared.db, move |conn| {
        Ok(match dataset.as_str() {
            "assignments" | "observer-assignments" => {
                let (items, total) = insights::list_assignments(conn, offset, size)?;
                (items, total, String::new())
            }
            "agents" => {
                let (items, total) = insights::list_agents(conn, offset, size)?;
                (items, total, String::new())
            }
            "incidents" => {
                let (items, total) = insights::list_incidents(conn, open_only, offset, size)?;
                (items, total, String::new())
            }
            "incident-revisions" => {
                let (items, total) = insights::list_incident_revisions(conn, offset, size)?;
                (items, total, String::new())
            }
            "data-loss" => {
                let (items, total) = insights::list_data_loss(conn, offset, size)?;
                (items, total, String::new())
            }
            "alerts" => {
                let (items, total) = insights::list_alerts(conn, offset, size)?;
                (items, total, String::new())
            }
            _ => return Err(anyhow::anyhow!("dataset_not_found")),
        })
    })
    .await;
    match result {
        Ok((items, total, next_cursor)) => list_meta(items, page, size, total, next_cursor),
        Err(err) if err.to_string() == "dataset_not_found" => {
            problem(StatusCode::NOT_FOUND, "dataset_not_found", "未知的数据集")
        }
        Err(err) => database_error(err, "数据集查询失败"),
    }
}

fn optional_server_id(
    params: &HashMap<String, String>,
    key: &str,
) -> Result<Option<i64>, Box<Response>> {
    match params.get(key).map(String::as_str).unwrap_or("") {
        "" => Ok(None),
        raw => raw.parse::<i64>().map(Some).map_err(|_| {
            Box::new(problem(
                StatusCode::BAD_REQUEST,
                "invalid_server_id",
                "server_id 无效",
            ))
        }),
    }
}

/// `GET /api/v2/admin/connections/summary`。
async fn connection_summary(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
) -> Response {
    let result = blocking_read(&shared.db, move |conn| {
        insights::load_connection_summary(conn, util_time::now_nanos())
    })
    .await;
    match result {
        Ok(summary) => data_json(json!({
            "collectors_total": summary.collectors_total,
            "collectors_online": summary.collectors_online,
            "collectors_offline": summary.collectors_offline,
            "collectors_unknown": summary.collectors_unknown,
            "paths_assigned": summary.paths_assigned,
            "paths_connected": summary.paths_connected,
            "paths_seen": summary.paths_seen,
        })),
        Err(err) => database_error(err, "连接汇总查询失败"),
    }
}

/// `GET /api/v2/admin/connections/paths`。
async fn connection_paths(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let server_id = match optional_server_id(&params, "server_id") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let observer_id = params
        .get("observer_id")
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let result = blocking_read(&shared.db, move |conn| {
        insights::load_connection_paths(
            conn,
            server_id,
            observer_id.as_deref(),
            util_time::now_nanos(),
        )
    })
    .await;
    match result {
        Ok(paths) => {
            let items: Vec<Value> = paths.iter().map(insights::connection_path_json).collect();
            let total = items.len() as i64;
            super::auth::list_json(Value::Array(items), total)
        }
        Err(err) => database_error(err, "连接路径查询失败"),
    }
}

/// `GET /api/v2/admin/connections/latency`。
async fn connection_latency(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let kind = params
        .get("kind")
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if let Some(kind) = &kind
        && !matches!(
            kind.as_str(),
            "collector_heartbeat" | "collector_replication" | "path"
        )
    {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_latency_kind",
            "kind 必须是 collector_heartbeat、collector_replication 或 path",
        );
    }
    let server_id = match optional_server_id(&params, "server_id") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let (page, size) = parse_page(&params);
    let collector_id = params.get("collector_id").cloned();
    let observer_id = params.get("observer_id").cloned();
    let result = blocking_read(&shared.db, move |conn| {
        insights::list_connection_latency(
            conn,
            kind.as_deref(),
            collector_id.as_deref(),
            observer_id.as_deref(),
            server_id,
            offset_of(page, size),
            size,
        )
    })
    .await;
    match result {
        Ok((items, total)) => list_meta(items, page, size, total, String::new()),
        Err(err) => database_error(err, "连接延迟查询失败"),
    }
}

// ---------- probes ----------

/// `GET /api/v2/admin/probes/summary`。
async fn probe_summary(State(shared): State<ApiShared>, AdminUser(_admin): AdminUser) -> Response {
    let result = blocking_read(&shared.db, |conn| {
        insights::load_probe_summary(conn, util_time::now_nanos())
    })
    .await;
    match result {
        Ok(payload) => data_json(payload),
        Err(err) => database_error(err, "探测汇总查询失败"),
    }
}

/// `GET /api/v2/admin/probes/paths`。
async fn probe_paths(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let server_id = match optional_server_id(&params, "server_id") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let collector_id = params.get("collector_id").cloned();
    let result = blocking_read(&shared.db, move |conn| {
        insights::load_probe_paths(conn, collector_id.as_deref(), server_id)
    })
    .await;
    match result {
        Ok(items) => {
            let total = items.len() as i64;
            super::auth::list_json(Value::Array(items), total)
        }
        Err(err) => database_error(err, "探测路径查询失败"),
    }
}

/// `GET /api/v2/admin/probes/samples`。
async fn probe_samples(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let server_id = match optional_server_id(&params, "server_id") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let port = match params.get("port").map(String::as_str).unwrap_or("") {
        "" => None,
        raw => match raw.parse::<i64>() {
            Ok(port) => Some(port),
            Err(_) => return problem(StatusCode::BAD_REQUEST, "invalid_port", "port 无效"),
        },
    };
    let (page, size) = parse_page(&params);
    let collector_id = params.get("collector_id").cloned();
    let kind = params.get("kind").cloned();
    let result = blocking_read(&shared.db, move |conn| {
        insights::list_probe_samples(
            conn,
            collector_id.as_deref(),
            kind.as_deref(),
            server_id,
            port,
            offset_of(page, size),
            size,
        )
    })
    .await;
    match result {
        Ok((items, total)) => list_meta(items, page, size, total, String::new()),
        Err(err) => database_error(err, "探测样本查询失败"),
    }
}

/// `GET /api/v2/admin/probes/trace`（collector + server 必填）。
async fn probe_trace(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let collector_id = params.get("collector_id").cloned().unwrap_or_default();
    let server_id = match optional_server_id(&params, "server_id") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    if collector_id.is_empty() || server_id.is_none() {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_probe_trace",
            "collector_id 与 server_id 必填",
        );
    }
    let result = blocking_read(&shared.db, move |conn| {
        insights::get_probe_trace(conn, &collector_id, server_id.unwrap_or(0))
    })
    .await;
    match result {
        Ok(Some(payload)) => data_json(payload),
        Ok(None) => data_json(Value::Null),
        Err(err) => database_error(err, "探测跟踪查询失败"),
    }
}

/// `GET /api/v2/admin/probes/route`。
async fn probe_route(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let collector_id = params.get("collector_id").cloned().unwrap_or_default();
    let server_id = match optional_server_id(&params, "server_id") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    if collector_id.is_empty() || server_id.is_none() {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_probe_route",
            "collector_id 与 server_id 必填",
        );
    }
    let result = blocking_read(&shared.db, move |conn| {
        insights::list_probe_routes(conn, &collector_id, server_id.unwrap_or(0))
    })
    .await;
    match result {
        Ok(Some(payload)) => data_json(payload),
        Ok(None) => data_json(Value::Null),
        Err(err) => database_error(err, "探测路由查询失败"),
    }
}

/// `POST /api/v2/admin/probes/route`（入队探测任务，202）。
async fn create_probe_route(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    body: axum::body::Bytes,
) -> Response {
    #[derive(Deserialize)]
    struct Request {
        #[serde(default)]
        collector_id: String,
        #[serde(default)]
        server_id: i64,
        #[serde(default)]
        protocol: String,
    }
    let request: Request = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_probe_route",
                "请求体不是合法 JSON",
            );
        }
    };
    if request.collector_id.trim().is_empty()
        || request.server_id <= 0
        || !matches!(request.protocol.as_str(), "icmp" | "tcp" | "ICMP" | "TCP")
    {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_probe_route",
            "collector_id、server_id 与 protocol（icmp/tcp）必填",
        );
    }
    let result = blocking_write(&shared.db, move |tx| {
        insights::enqueue_probe_route_job(
            tx,
            &request.collector_id,
            request.server_id,
            &request.protocol.to_lowercase(),
        )
    })
    .await;
    match result {
        Ok(Ok((id, protocol, port))) => {
            let mut payload = json!({
                "id": id,
                "protocol": protocol,
                "status": "pending",
                "requested_at": util_time::unix_nanos_to_rfc3339(util_time::now_nanos()),
            });
            if port > 0 {
                payload["port"] = json!(port);
            }
            let mut response = data_json(payload);
            *response.status_mut() = StatusCode::ACCEPTED;
            response
        }
        Ok(Err(reason)) => {
            let (code, detail) = match reason.as_str() {
                "not_found" => ("not_found", "探测从端或服务器不存在"),
                "invalid_protocol" => ("invalid_probe_route", "protocol 必须是 icmp 或 tcp"),
                _ => (
                    "probe_route_disabled",
                    "该从端未开启此探测协议（TCP 还需配置端口）",
                ),
            };
            let status = if code == "not_found" {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::BAD_REQUEST
            };
            problem(status, code, detail)
        }
        Err(err) => database_error(err, "探测任务入队失败"),
    }
}

// ---------- offline history ----------

/// `GET /api/v2/admin/offline-history`。
async fn offline_history(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let server_id = match optional_server_id(&params, "server_id") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let Some(server_id) = server_id else {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_server_id",
            "server_id 无效",
        );
    };
    let (page, size) = parse_page(&params);
    let result = blocking_read(&shared.db, move |conn| {
        insights::list_offline_history(conn, server_id, offset_of(page, size), size)
    })
    .await;
    match result {
        Ok((items, total)) => list_meta(items, page, size, total, String::new()),
        Err(err) => database_error(err, "离线历史查询失败"),
    }
}

/// `DELETE /api/v2/admin/offline-history/{id}`。
async fn delete_offline_history(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(resource_id): Path<i64>,
) -> Response {
    if resource_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    let deleted = blocking_write(&shared.db, move |tx| {
        Ok(tx.execute(
            "DELETE FROM server_offline_histories WHERE id = ?1",
            [resource_id],
        )?)
    })
    .await;
    match deleted {
        Ok(1) => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => problem(
            StatusCode::NOT_FOUND,
            "offline_history_not_found",
            "离线历史不存在",
        ),
        Err(err) => database_error(err, "离线历史删除失败"),
    }
}

/// `POST /api/v2/admin/offline-history/cleanup`。
async fn cleanup_offline_history(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    body: axum::body::Bytes,
) -> Response {
    #[derive(Deserialize, Default)]
    struct Request {
        #[serde(default)]
        before: Option<String>,
        #[serde(default)]
        server_id: i64,
    }
    let request: Request = serde_json::from_slice(&body).unwrap_or_default();
    let before = match request
        .before
        .as_deref()
        .map(str::trim)
        .filter(|raw| !raw.is_empty())
    {
        Some(raw) => {
            match time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
                .ok()
                .map(time::OffsetDateTime::unix_timestamp_nanos)
                .and_then(|nanos| i64::try_from(nanos).ok())
            {
                Some(nanos) => nanos,
                None => {
                    return problem(
                        StatusCode::BAD_REQUEST,
                        "invalid_cleanup_before",
                        "before 必须是 RFC3339",
                    );
                }
            }
        }
        None => {
            let retention_days = shared.conf().offline_history_retention_days as i64;
            // `retention_days` 由 admin 接口直传，可远超 `i64` 纳秒跨度；
            // 直接 `* 24 * 3_600 * 1_000_000_000` 会溢出 panic（debug）或回绕
            // 成「未来时刻」（release），导致清理区间为空甚至反向。
            util_time::now_nanos() - retention_days.saturating_mul(24 * 3_600 * 1_000_000_000)
        }
    };
    let server_id = request.server_id;
    let deleted = blocking_read(&shared.db, move |conn| {
        insights::cleanup_offline_history(conn, Some(server_id), before)
    })
    .await;
    match deleted {
        Ok(count) => data_json(json!({ "deleted": count })),
        Err(err) => database_error(err, "离线历史清理失败"),
    }
}

// ---------- database ----------

fn database_status_json(status: &crate::telemetry::maintenance::DatabaseStatus) -> Value {
    let mut payload = json!({
        "file_bytes": status.file_bytes,
        "wal_bytes": status.wal_bytes,
        "reclaimable_bytes": status.reclaimable_bytes,
        "running": status.running,
    });
    if let Some(last) = &status.last_run {
        let mut last_run = json!({
            "started_at": last.started_at,
            "deleted": last.deleted,
            "compacted": last.compacted,
        });
        if !last.ended_at.is_empty() {
            last_run["ended_at"] = json!(last.ended_at);
        }
        if !last.skipped.is_empty() {
            last_run["skipped"] = json!(last.skipped);
        }
        if !last.error.is_empty() {
            last_run["error"] = json!(last.error);
        }
        payload["last_run"] = last_run;
    }
    payload
}

/// `GET /api/v2/admin/database`。
async fn get_database(State(shared): State<ApiShared>, AdminUser(_admin): AdminUser) -> Response {
    match shared.maintainer.clone() {
        Some(maintainer) => data_json(database_status_json(&maintainer.status())),
        None => problem(
            StatusCode::INTERNAL_SERVER_ERROR,
            "database_maintenance_unavailable",
            "数据库维护器未启用",
        ),
    }
}

/// `POST /api/v2/admin/database/optimize`。
async fn optimize_database(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
) -> Response {
    let Some(maintainer) = shared.maintainer.clone() else {
        return problem(
            StatusCode::INTERNAL_SERVER_ERROR,
            "database_maintenance_unavailable",
            "数据库维护器未启用",
        );
    };
    if maintainer.status().running {
        return problem(
            StatusCode::CONFLICT,
            "optimize_in_progress",
            "数据库优化正在进行中",
        );
    }
    // Rust 版同步执行一轮后回状态（Go 是异步 Start；效果一致，时机为阻塞完成）。
    let _ = tokio::task::spawn_blocking(move || maintainer.run_once(true)).await;
    let maintainer = shared.maintainer.clone().unwrap();
    data_json(database_status_json(&maintainer.status()))
}

// ---------- monitors history / admin availability ----------

fn parse_rfc3339_param(
    params: &HashMap<String, String>,
    key: &str,
) -> Result<Option<i64>, Box<Response>> {
    match params.get(key).map(String::as_str).unwrap_or("") {
        "" => Ok(None),
        raw => time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
            .ok()
            .map(time::OffsetDateTime::unix_timestamp_nanos)
            .and_then(|nanos| i64::try_from(nanos).ok())
            .map(Some)
            .ok_or_else(|| {
                let code: &str = Box::leak(format!("invalid_{key}").into_boxed_str());
                Box::new(problem(
                    StatusCode::BAD_REQUEST,
                    code,
                    &format!("{key} 必须是 RFC3339 时间"),
                ))
            }),
    }
}

/// `GET /api/v2/admin/monitors/{id}/history`。
async fn monitor_history(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(resource_id): Path<i64>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if resource_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    let limit = params
        .get("limit")
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|value| *value >= 1)
        .unwrap_or(200)
        .min(1000);
    let from = match parse_rfc3339_param(&params, "from") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let to = match parse_rfc3339_param(&params, "to") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let cursor = params
        .get("cursor")
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|value| *value > 0);
    let result = blocking_read(&shared.db, move |conn| {
        insights::list_monitor_history(conn, resource_id, from, to, cursor, limit)
    })
    .await;
    match result {
        Ok((items, next_cursor)) => list_cursor(items, next_cursor),
        Err(err) => database_error(err, "监控历史查询失败"),
    }
}

/// `GET /api/v2/admin/servers/{serverId}/availability`。
async fn admin_server_availability(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(server_id): Path<i64>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if server_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    let limit = params
        .get("limit")
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|value| *value >= 1)
        .unwrap_or(200)
        .min(1000);
    let from = match parse_rfc3339_param(&params, "from") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let to = match parse_rfc3339_param(&params, "to") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let cursor = params
        .get("cursor")
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|value| *value > 0);
    let result = blocking_read(&shared.db, move |conn| {
        insights::list_admin_availability(conn, server_id, from, to, cursor, limit)
    })
    .await;
    match result {
        Ok((items, next_cursor)) => list_cursor(items, next_cursor),
        Err(err) => database_error(err, "可用性查询失败"),
    }
}

/// admin 读侧路由组。
pub fn routes() -> axum::Router<ApiShared> {
    use axum::routing::{get, post};
    axum::Router::new()
        .route("/api/v2/admin/summary", get(admin_summary))
        .route("/api/v2/admin/telemetry/overview", get(telemetry_overview))
        .route("/api/v2/admin/telemetry/{dataset}", get(telemetry_dataset))
        .route("/api/v2/admin/connections/summary", get(connection_summary))
        .route("/api/v2/admin/connections/paths", get(connection_paths))
        .route("/api/v2/admin/connections/latency", get(connection_latency))
        .route("/api/v2/admin/probes/summary", get(probe_summary))
        .route("/api/v2/admin/probes/paths", get(probe_paths))
        .route("/api/v2/admin/probes/samples", get(probe_samples))
        .route("/api/v2/admin/probes/trace", get(probe_trace))
        .route(
            "/api/v2/admin/probes/route",
            get(probe_route).post(create_probe_route),
        )
        .route("/api/v2/admin/offline-history", get(offline_history))
        .route(
            "/api/v2/admin/offline-history/{resourceId}",
            axum::routing::delete(delete_offline_history),
        )
        .route(
            "/api/v2/admin/offline-history/cleanup",
            post(cleanup_offline_history),
        )
        .route("/api/v2/admin/database", get(get_database))
        .route("/api/v2/admin/database/optimize", post(optimize_database))
        .route(
            "/api/v2/admin/monitors/{resourceId}/history",
            get(monitor_history),
        )
        .route(
            "/api/v2/admin/servers/{serverId}/availability",
            get(admin_server_availability),
        )
}
