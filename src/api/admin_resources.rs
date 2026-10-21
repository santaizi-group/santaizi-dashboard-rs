//! Admin typed 资源端点（Phase 5）：monitors / notifications / alert-rules
//! 的 CRUD 与静态端点（probe-capabilities / script-commands / install /
//! upgrade preview），对齐 Go `api_v2_typed.go`。
//!
//! `OnMonitorUpdate` / `OnRefreshOrAddNotification` / `OnRefreshOrAddAlert`
//! 等内存引擎钩子属 Phase 6/7，本层只落库；`POST /notifications/:id/test`
//! 待通知发送引擎（刀 7）接线，当前显式 501。

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::repo::typed_resources as repo;
use crate::rpc::support::{blocking_read, blocking_write};
use crate::util::time as util_time;

use super::auth::{AdminUser, ApiShared, data_json, list_json_paged, problem};

fn parse_page(params: &HashMap<String, String>) -> (i64, i64) {
    let page = params
        .get("page")
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|page| *page >= 1)
        .unwrap_or(1);
    let size = params
        .get("page_size")
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|size| *size >= 1)
        .unwrap_or(20)
        .min(200);
    (page, size)
}

/// 列表查询公共参数。
struct ListQuery {
    q: String,
    sort: String,
    ascending: bool,
    page: i64,
    size: i64,
}

fn list_query(params: &HashMap<String, String>) -> ListQuery {
    let (page, size) = parse_page(params);
    ListQuery {
        q: params
            .get("q")
            .map(String::as_str)
            .unwrap_or("")
            .trim()
            .to_string(),
        sort: params.get("sort").cloned().unwrap_or_default(),
        ascending: params
            .get("order")
            .is_some_and(|value| value.eq_ignore_ascii_case("asc")),
        page,
        size,
    }
}

fn invalid_id() -> Response {
    problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效")
}

// ---------- monitors ----------

/// scope 形态（Go `monitorScopeDTO`）。
#[derive(Clone, Deserialize, Default)]
struct ScopeDto {
    #[serde(default)]
    mode: String,
    #[serde(default)]
    server_ids: Vec<i64>,
}

#[derive(Deserialize)]
struct MonitorWrite {
    #[serde(default)]
    name: String,
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    target: String,
    #[serde(default)]
    interval_seconds: u64,
    #[serde(default)]
    scope: ScopeDto,
    #[serde(default)]
    notify: bool,
    #[serde(default)]
    notification_tag: String,
    #[serde(default)]
    show_in_service: bool,
    #[serde(default)]
    latency_notify: bool,
    #[serde(default)]
    min_latency_ms: f64,
    #[serde(default)]
    max_latency_ms: f64,
}

fn monitor_type_id(name: &str) -> Option<i64> {
    match name.trim().to_lowercase().as_str() {
        "http" => Some(repo::MONITOR_TYPE_HTTP),
        "icmp" => Some(repo::MONITOR_TYPE_ICMP),
        "tcp" => Some(repo::MONITOR_TYPE_TCP),
        _ => None,
    }
}

fn monitor_type_name(value: i64) -> &'static str {
    match value {
        repo::MONITOR_TYPE_HTTP => "http",
        repo::MONITOR_TYPE_ICMP => "icmp",
        repo::MONITOR_TYPE_TCP => "tcp",
        _ => "",
    }
}

/// Go `InitSkipServers` 的读侧：skip_servers_raw → 排序去重的 id 列表。
fn skip_server_ids(raw: &str) -> Vec<i64> {
    let mut ids: Vec<i64> = serde_json::from_str::<Vec<i64>>(raw).unwrap_or_default();
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn scope_of(cover: i64, ids: Vec<i64>) -> Value {
    let mode = if cover == repo::MONITOR_COVER_IGNORE_ALL {
        "include"
    } else if ids.is_empty() {
        "all"
    } else {
        "exclude"
    };
    json!({ "mode": mode, "server_ids": ids })
}

fn monitor_dto(row: &repo::MonitorRow) -> Value {
    json!({
        "id": row.id,
        "name": row.name,
        "type": monitor_type_name(row.monitor_type),
        "target": row.target,
        "interval_seconds": row.duration,
        "scope": scope_of(row.cover, skip_server_ids(&row.skip_servers_raw)),
        "notify": row.notify,
        "notification_tag": row.notification_tag,
        "show_in_service": row.enable_show_in_service,
        "latency_notify": row.latency_notify,
        "min_latency_ms": row.min_latency,
        "max_latency_ms": row.max_latency,
        "created_at": row.created_at_nanos.map(util_time::unix_nanos_to_rfc3339),
        "updated_at": row.updated_at_nanos.map(util_time::unix_nanos_to_rfc3339),
    })
}

/// Go `applyMonitorWrite`。
fn apply_monitor_write(row: &mut repo::MonitorRow, request: &MonitorWrite) -> Result<(), String> {
    row.name = request.name.trim().to_string();
    row.target = request.target.trim().to_string();
    if row.name.is_empty() || row.target.is_empty() {
        return Err("name and target are required".into());
    }
    let Some(type_id) = monitor_type_id(&request.r#type) else {
        return Err("monitor type must be http, icmp, or tcp".into());
    };
    row.monitor_type = type_id;
    let interval = if request.interval_seconds == 0 {
        30
    } else {
        request.interval_seconds
    };
    if !(5..=86400).contains(&interval) {
        return Err("interval_seconds must be between 5 and 86400".into());
    }
    row.duration = interval as i64;
    let mut server_ids = request.scope.server_ids.clone();
    match request.scope.mode.as_str() {
        "all" => {
            row.cover = repo::MONITOR_COVER_ALL;
            server_ids.clear();
        }
        "exclude" => {
            row.cover = repo::MONITOR_COVER_ALL;
        }
        "include" => {
            if server_ids.is_empty() {
                return Err("include scope requires at least one server".into());
            }
            row.cover = repo::MONITOR_COVER_IGNORE_ALL;
        }
        _ => return Err("scope mode must be all, include, or exclude".into()),
    }
    row.skip_servers_raw = serde_json::to_string(&server_ids).unwrap_or_else(|_| "[]".into());
    row.notify = request.notify;
    row.notification_tag = request.notification_tag.trim().to_string();
    row.enable_show_in_service = request.show_in_service;
    row.latency_notify = request.latency_notify;
    row.min_latency = request.min_latency_ms;
    row.max_latency = request.max_latency_ms;
    if row.latency_notify && row.max_latency > 0.0 && row.min_latency > row.max_latency {
        return Err("min_latency_ms must not exceed max_latency_ms".into());
    }
    Ok(())
}

async fn list_monitors(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let mut query = list_query(&params);
    if !query.q.is_empty() {
        query.q = format!("%{}%", query.q);
    }
    let result = blocking_read(&shared.db, move |conn| {
        repo::list_monitors(
            conn,
            &query.q,
            &query.sort,
            query.ascending,
            (query.page - 1) * query.size,
            query.size,
        )
    })
    .await;
    match result {
        Ok((rows, total)) => {
            let items: Vec<Value> = rows.iter().map(monitor_dto).collect();
            list_json_paged(Value::Array(items), query.page, query.size, total)
        }
        Err(err) => database_error(err, "查询服务监控失败"),
    }
}

async fn get_monitor(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(resource_id): Path<i64>,
) -> Response {
    if resource_id <= 0 {
        return invalid_id();
    }
    match blocking_read(&shared.db, move |conn| {
        repo::monitor_by_id(conn, resource_id)
    })
    .await
    {
        Ok(Some(row)) => data_json(monitor_dto(&row)),
        Ok(None) => monitor_not_found(),
        Err(err) => database_error(err, "查询服务监控失败"),
    }
}

async fn save_monitor(shared: ApiShared, resource_id: i64, body: axum::body::Bytes) -> Response {
    let request: MonitorWrite = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return invalid_json("invalid_monitor"),
    };
    let mut row = repo::MonitorRow {
        id: resource_id,
        name: String::new(),
        monitor_type: 0,
        target: String::new(),
        skip_servers_raw: "[]".into(),
        duration: 30,
        notify: false,
        notification_tag: String::new(),
        cover: repo::MONITOR_COVER_ALL,
        enable_show_in_service: false,
        min_latency: 0.0,
        max_latency: 0.0,
        latency_notify: false,
        created_at_nanos: None,
        updated_at_nanos: None,
    };
    if resource_id > 0 {
        match blocking_read(&shared.db, move |conn| {
            repo::monitor_by_id(conn, resource_id)
        })
        .await
        {
            Ok(Some(existing)) => row = existing,
            Ok(None) => return monitor_not_found(),
            Err(err) => return database_error(err, "查询服务监控失败"),
        }
    }
    if let Err(detail) = apply_monitor_write(&mut row, &request) {
        return problem(StatusCode::BAD_REQUEST, "invalid_monitor", &detail);
    }
    let saved = blocking_write(&shared.db, move |tx| repo::save_monitor(tx, &row)).await;
    match saved {
        Ok(()) => {}
        Err(err) => return database_error(err, "保存服务监控失败"),
    }
    let reloaded = if resource_id > 0 {
        blocking_read(&shared.db, move |conn| {
            repo::monitor_by_id(conn, resource_id)
        })
        .await
    } else {
        blocking_read(&shared.db, |conn| {
            let id: i64 = conn.query_row("SELECT MAX(id) FROM monitors", [], |row| row.get(0))?;
            repo::monitor_by_id(conn, id)
        })
        .await
    };
    match reloaded {
        Ok(Some(row)) => {
            let mut response = data_json(monitor_dto(&row));
            if resource_id == 0 {
                *response.status_mut() = StatusCode::CREATED;
            }
            response
        }
        _ => monitor_not_found(),
    }
}

fn monitor_not_found() -> Response {
    problem(StatusCode::NOT_FOUND, "monitor_not_found", "服务监控不存在")
}

async fn create_monitor(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    body: axum::body::Bytes,
) -> Response {
    save_monitor(shared, 0, body).await
}

async fn update_monitor(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(resource_id): Path<i64>,
    body: axum::body::Bytes,
) -> Response {
    if resource_id <= 0 {
        return invalid_id();
    }
    save_monitor(shared, resource_id, body).await
}

async fn delete_monitor(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(resource_id): Path<i64>,
) -> Response {
    if resource_id <= 0 {
        return invalid_id();
    }
    match blocking_write(&shared.db, move |tx| repo::delete_monitor(tx, resource_id)).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => monitor_not_found(),
        Err(err) => database_error(err, "删除服务监控失败"),
    }
}

fn invalid_json(code: &'static str) -> Response {
    problem(StatusCode::BAD_REQUEST, code, "请求体不是合法 JSON")
}

fn database_error(err: anyhow::Error, detail: &'static str) -> Response {
    tracing::warn!(error = %err, "{detail}");
    problem(StatusCode::INTERNAL_SERVER_ERROR, "database_error", detail)
}

// ---------- notifications ----------

#[derive(Clone, Deserialize, Default)]
struct KeyValueDto {
    #[serde(default)]
    key: String,
    #[serde(default)]
    value: String,
}

#[derive(Deserialize)]
struct NotificationWrite {
    #[serde(default)]
    name: String,
    #[serde(default)]
    tag: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    method: String,
    #[serde(default)]
    request_type: String,
    #[serde(default)]
    headers: Vec<KeyValueDto>,
    #[serde(default)]
    body: String,
    #[serde(default)]
    verify_tls: bool,
}

fn header_pairs(raw: &str) -> Vec<Value> {
    let values: HashMap<String, String> = serde_json::from_str(raw).unwrap_or_default();
    let mut keys: Vec<&String> = values.keys().collect();
    keys.sort();
    keys.iter()
        .map(|key| json!({ "key": key, "value": values[*key] }))
        .collect()
}

fn notification_dto(row: &repo::NotificationRow) -> Value {
    json!({
        "id": row.id,
        "name": row.name,
        "tag": row.tag,
        "url": row.url,
        "method": if row.request_method == repo::NOTIFY_METHOD_POST { "post" } else { "get" },
        "request_type": if row.request_type == repo::NOTIFY_TYPE_FORM { "form" } else { "json" },
        "headers": header_pairs(&row.request_header),
        "body": row.request_body,
        "verify_tls": row.verify_ssl.map(|v| v != 0).unwrap_or(false),
        "created_at": row.created_at_nanos.map(util_time::unix_nanos_to_rfc3339),
        "updated_at": row.updated_at_nanos.map(util_time::unix_nanos_to_rfc3339),
    })
}

/// Go `applyNotificationWrite`（URL 绝对性校验走手写解析，免拉 url crate）。
fn apply_notification_write(
    row: &mut repo::NotificationRow,
    request: &NotificationWrite,
) -> Result<(), String> {
    row.name = request.name.trim().to_string();
    row.tag = request.tag.trim().to_string();
    row.url = request.url.trim().to_string();
    if row.name.is_empty() || row.tag.is_empty() {
        return Err("name and tag are required".into());
    }
    if !is_absolute_http_url(&row.url) {
        return Err("url must be an absolute HTTP or HTTPS URL".into());
    }
    row.request_method = match request.method.trim().to_lowercase().as_str() {
        "get" => repo::NOTIFY_METHOD_GET,
        "post" => repo::NOTIFY_METHOD_POST,
        _ => return Err("method must be get or post".into()),
    };
    row.request_type = match request.request_type.trim().to_lowercase().as_str() {
        "json" => repo::NOTIFY_TYPE_JSON,
        "form" => repo::NOTIFY_TYPE_FORM,
        _ => return Err("request_type must be json or form".into()),
    };
    let mut headers = serde_json::Map::new();
    for item in &request.headers {
        let key = item.key.trim().to_string();
        if key.is_empty() {
            return Err("header key is required".into());
        }
        headers.insert(key, json!(item.value));
    }
    row.request_header = Value::Object(headers).to_string();
    row.request_body = request.body.clone();
    row.verify_ssl = Some(request.verify_tls as i64);
    Ok(())
}

fn is_absolute_http_url(value: &str) -> bool {
    let rest = value
        .strip_prefix("https://")
        .or_else(|| value.strip_prefix("http://"));
    rest.is_some_and(|rest| !rest.is_empty() && !rest.starts_with('/'))
}

async fn list_notifications_handler(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let mut query = list_query(&params);
    if !query.q.is_empty() {
        query.q = format!("%{}%", query.q);
    }
    let result = blocking_read(&shared.db, move |conn| {
        repo::list_notifications(
            conn,
            &query.q,
            &query.sort,
            query.ascending,
            (query.page - 1) * query.size,
            query.size,
        )
    })
    .await;
    match result {
        Ok((rows, total)) => {
            let items: Vec<Value> = rows.iter().map(notification_dto).collect();
            list_json_paged(Value::Array(items), query.page, query.size, total)
        }
        Err(err) => database_error(err, "查询通知渠道失败"),
    }
}

async fn get_notification(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(resource_id): Path<i64>,
) -> Response {
    if resource_id <= 0 {
        return invalid_id();
    }
    match blocking_read(&shared.db, move |conn| {
        repo::notification_by_id(conn, resource_id)
    })
    .await
    {
        Ok(Some(row)) => data_json(notification_dto(&row)),
        Ok(None) => notification_not_found(),
        Err(err) => database_error(err, "查询通知渠道失败"),
    }
}

fn notification_not_found() -> Response {
    problem(
        StatusCode::NOT_FOUND,
        "notification_not_found",
        "通知渠道不存在",
    )
}

async fn save_notification_handler(
    shared: ApiShared,
    resource_id: i64,
    body: axum::body::Bytes,
) -> Response {
    let request: NotificationWrite = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return invalid_json("invalid_notification"),
    };
    let mut row = repo::NotificationRow {
        id: resource_id,
        name: String::new(),
        tag: String::new(),
        url: String::new(),
        request_method: repo::NOTIFY_METHOD_GET,
        request_type: repo::NOTIFY_TYPE_JSON,
        request_header: "{}".into(),
        request_body: String::new(),
        verify_ssl: Some(0),
        created_at_nanos: None,
        updated_at_nanos: None,
    };
    if resource_id > 0 {
        match blocking_read(&shared.db, move |conn| {
            repo::notification_by_id(conn, resource_id)
        })
        .await
        {
            Ok(Some(existing)) => row = existing,
            Ok(None) => return notification_not_found(),
            Err(err) => return database_error(err, "查询通知渠道失败"),
        }
    }
    if let Err(detail) = apply_notification_write(&mut row, &request) {
        return problem(StatusCode::BAD_REQUEST, "invalid_notification", &detail);
    }
    match blocking_write(&shared.db, move |tx| repo::save_notification(tx, &row)).await {
        Ok(()) => {}
        Err(err) => return database_error(err, "保存通知渠道失败"),
    }
    // 新建行回读：按 updated_at 排序的最新行兜底不可靠，直接查最大 id。
    let reloaded = if resource_id > 0 {
        blocking_read(&shared.db, move |conn| {
            repo::notification_by_id(conn, resource_id)
        })
        .await
    } else {
        blocking_read(&shared.db, |conn| {
            let id: i64 =
                conn.query_row("SELECT MAX(id) FROM notifications", [], |row| row.get(0))?;
            repo::notification_by_id(conn, id)
        })
        .await
    };
    match reloaded {
        Ok(Some(row)) => {
            let mut response = data_json(notification_dto(&row));
            if resource_id == 0 {
                *response.status_mut() = StatusCode::CREATED;
            }
            response
        }
        _ => notification_not_found(),
    }
}

async fn create_notification(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    body: axum::body::Bytes,
) -> Response {
    save_notification_handler(shared, 0, body).await
}

async fn update_notification(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(resource_id): Path<i64>,
    body: axum::body::Bytes,
) -> Response {
    if resource_id <= 0 {
        return invalid_id();
    }
    save_notification_handler(shared, resource_id, body).await
}

async fn delete_notification(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(resource_id): Path<i64>,
) -> Response {
    if resource_id <= 0 {
        return invalid_id();
    }
    match blocking_write(&shared.db, move |tx| {
        repo::delete_notification(tx, resource_id)
    })
    .await
    {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => notification_not_found(),
        Err(err) => database_error(err, "删除通知渠道失败"),
    }
}

/// `POST /api/v2/admin/notifications/{id}/test`（Go `v2TestNotification`：
/// 直发「这是测试消息」，2xx 视为成功，失败 502 带原因）。
async fn test_notification(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(resource_id): Path<i64>,
) -> Response {
    if resource_id <= 0 {
        return invalid_id();
    }
    let channel = blocking_read(&shared.db, move |conn| {
        repo::notification_by_id(conn, resource_id)
    })
    .await;
    let channel = match channel {
        Ok(Some(channel)) => channel,
        Ok(None) => return notification_not_found(),
        Err(err) => return database_error(err, "查询通知渠道失败"),
    };
    let outcome = tokio::task::spawn_blocking(move || {
        crate::notify::deliver_channel_blocking(
            &channel,
            "这是测试消息",
            None,
            crate::util::time::now_nanos(),
        )
    })
    .await;
    match outcome {
        Ok(Ok(())) => {
            let mut response = data_json(json!({ "accepted": true }));
            *response.status_mut() = StatusCode::ACCEPTED;
            response
        }
        Ok(Err(err)) => problem(
            StatusCode::BAD_GATEWAY,
            "notification_test_failed",
            &format!("发送失败：{err}"),
        ),
        Err(err) => database_error(err.into(), "通知测试任务崩溃"),
    }
}

// ---------- alert rules ----------

#[derive(Clone, Deserialize, Default)]
struct AlertConditionDto {
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    min: Option<f64>,
    #[serde(default)]
    max: Option<f64>,
    #[serde(default)]
    duration_seconds: u64,
    #[serde(default)]
    scope: ScopeDto,
}

#[derive(Deserialize)]
struct AlertRuleWrite {
    #[serde(default)]
    name: String,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    trigger_mode: String,
    #[serde(default)]
    notification_tag: String,
    #[serde(default)]
    conditions: Vec<AlertConditionDto>,
}

const ALERT_TYPES: [&str; 16] = [
    "cpu",
    "gpu",
    "temperature_max",
    "memory",
    "swap",
    "disk",
    "net_in_speed",
    "net_out_speed",
    "net_all_speed",
    "offline",
    "load1",
    "load5",
    "load15",
    "tcp_conn_count",
    "udp_conn_count",
    "process_count",
];

fn scope_from_ignore(cover: i64, ignore: &serde_json::Map<String, Value>) -> Value {
    let mut ids: Vec<i64> = ignore
        .keys()
        .filter_map(|key| key.parse::<i64>().ok())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    scope_of(cover, ids)
}

fn alert_rule_dto(row: &repo::AlertRuleRow) -> Value {
    let rules: Vec<Value> = serde_json::from_str(&row.rules_raw).unwrap_or_default();
    let conditions: Vec<Value> = rules
        .iter()
        .map(|rule| {
            let cover = rule.get("cover").and_then(Value::as_i64).unwrap_or(0);
            let ignore = rule
                .get("ignore")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            let min = rule
                .get("min")
                .and_then(Value::as_f64)
                .filter(|value| *value != 0.0);
            let max = rule
                .get("max")
                .and_then(Value::as_f64)
                .filter(|value| *value != 0.0);
            json!({
                "type": rule.get("type").cloned().unwrap_or(Value::Null),
                "min": min,
                "max": max,
                "duration_seconds": rule.get("duration").cloned().unwrap_or(json!(0)),
                "scope": scope_from_ignore(cover, &ignore),
            })
        })
        .collect();
    json!({
        "id": row.id,
        "name": row.name,
        "enabled": row.enable.map(|v| v != 0).unwrap_or(false),
        "trigger_mode": if row.trigger_mode == repo::ALERT_TRIGGER_ONCE { "once" } else { "always" },
        "notification_tag": row.notification_tag,
        "conditions": conditions,
        "created_at": row.created_at_nanos.map(util_time::unix_nanos_to_rfc3339),
        "updated_at": row.updated_at_nanos.map(util_time::unix_nanos_to_rfc3339),
    })
}

/// Go `applyAlertRuleWrite`：条件序列化为 Go `model.Rule` 的 JSON 形态。
fn apply_alert_rule_write(
    row: &mut repo::AlertRuleRow,
    request: &AlertRuleWrite,
) -> Result<(), String> {
    row.name = request.name.trim().to_string();
    row.notification_tag = request.notification_tag.trim().to_string();
    if row.name.is_empty() || row.notification_tag.is_empty() {
        return Err("name and notification_tag are required".into());
    }
    row.trigger_mode = match request.trigger_mode.as_str() {
        "always" => repo::ALERT_TRIGGER_ALWAYS,
        "once" => repo::ALERT_TRIGGER_ONCE,
        _ => return Err("trigger_mode must be always or once".into()),
    };
    row.enable = Some(request.enabled as i64);
    if request.conditions.is_empty() {
        return Err("at least one condition is required".into());
    }
    let mut rules: Vec<Value> = Vec::with_capacity(request.conditions.len());
    for condition in &request.conditions {
        let condition_type = condition.r#type.trim().to_lowercase();
        if !ALERT_TYPES.contains(&condition_type.as_str()) {
            return Err(format!("unsupported alert condition \"{condition_type}\""));
        }
        if condition_type != "offline" && condition.min.is_none() && condition.max.is_none() {
            return Err(format!("condition {condition_type} requires min or max"));
        }
        if let (Some(min), Some(max)) = (condition.min, condition.max)
            && min > max
        {
            return Err(format!(
                "condition {condition_type} min must not exceed max"
            ));
        }
        let mut ignore = serde_json::Map::new();
        let cover = match condition.scope.mode.as_str() {
            "all" => 0i64,
            "exclude" => {
                for id in &condition.scope.server_ids {
                    ignore.insert(id.to_string(), json!(true));
                }
                0
            }
            "include" => {
                if condition.scope.server_ids.is_empty() {
                    return Err("include scope requires at least one server".into());
                }
                for id in &condition.scope.server_ids {
                    ignore.insert(id.to_string(), json!(true));
                }
                1
            }
            _ => return Err("scope mode must be all, include, or exclude".into()),
        };
        let mut rule = json!({
            "type": condition_type,
            "duration": condition.duration_seconds,
            "cover": cover,
            "ignore": ignore,
        });
        if let Some(min) = condition.min {
            rule["min"] = json!(min);
        }
        if let Some(max) = condition.max {
            rule["max"] = json!(max);
        }
        rules.push(rule);
    }
    row.rules_raw = serde_json::to_string(&rules).unwrap_or_else(|_| "[]".into());
    Ok(())
}

async fn list_alert_rules_handler(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let mut query = list_query(&params);
    if !query.q.is_empty() {
        query.q = format!("%{}%", query.q);
    }
    let result = blocking_read(&shared.db, move |conn| {
        repo::list_alert_rules(
            conn,
            &query.q,
            &query.sort,
            query.ascending,
            (query.page - 1) * query.size,
            query.size,
        )
    })
    .await;
    match result {
        Ok((rows, total)) => {
            let items: Vec<Value> = rows.iter().map(alert_rule_dto).collect();
            list_json_paged(Value::Array(items), query.page, query.size, total)
        }
        Err(err) => database_error(err, "查询告警规则失败"),
    }
}

async fn get_alert_rule(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(resource_id): Path<i64>,
) -> Response {
    if resource_id <= 0 {
        return invalid_id();
    }
    match blocking_read(&shared.db, move |conn| {
        repo::alert_rule_by_id(conn, resource_id)
    })
    .await
    {
        Ok(Some(row)) => data_json(alert_rule_dto(&row)),
        Ok(None) => alert_rule_not_found(),
        Err(err) => database_error(err, "查询告警规则失败"),
    }
}

fn alert_rule_not_found() -> Response {
    problem(
        StatusCode::NOT_FOUND,
        "alert_rule_not_found",
        "告警规则不存在",
    )
}

async fn save_alert_rule_handler(
    shared: ApiShared,
    resource_id: i64,
    body: axum::body::Bytes,
) -> Response {
    let request: AlertRuleWrite = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return invalid_json("invalid_alert_rule"),
    };
    let mut row = repo::AlertRuleRow {
        id: resource_id,
        name: String::new(),
        rules_raw: "[]".into(),
        enable: Some(0),
        trigger_mode: repo::ALERT_TRIGGER_ALWAYS,
        notification_tag: String::new(),
        created_at_nanos: None,
        updated_at_nanos: None,
    };
    if resource_id > 0 {
        match blocking_read(&shared.db, move |conn| {
            repo::alert_rule_by_id(conn, resource_id)
        })
        .await
        {
            Ok(Some(existing)) => row = existing,
            Ok(None) => return alert_rule_not_found(),
            Err(err) => return database_error(err, "查询告警规则失败"),
        }
    }
    if let Err(detail) = apply_alert_rule_write(&mut row, &request) {
        return problem(StatusCode::BAD_REQUEST, "invalid_alert_rule", &detail);
    }
    match blocking_write(&shared.db, move |tx| repo::save_alert_rule(tx, &row)).await {
        Ok(()) => {}
        Err(err) => return database_error(err, "保存告警规则失败"),
    }
    let reloaded = if resource_id > 0 {
        blocking_read(&shared.db, move |conn| {
            repo::alert_rule_by_id(conn, resource_id)
        })
        .await
    } else {
        blocking_read(&shared.db, |conn| {
            let id: i64 =
                conn.query_row("SELECT MAX(id) FROM alert_rules", [], |row| row.get(0))?;
            repo::alert_rule_by_id(conn, id)
        })
        .await
    };
    match reloaded {
        Ok(Some(row)) => {
            let mut response = data_json(alert_rule_dto(&row));
            if resource_id == 0 {
                *response.status_mut() = StatusCode::CREATED;
            }
            response
        }
        _ => alert_rule_not_found(),
    }
}

async fn create_alert_rule(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    body: axum::body::Bytes,
) -> Response {
    save_alert_rule_handler(shared, 0, body).await
}

async fn update_alert_rule(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(resource_id): Path<i64>,
    body: axum::body::Bytes,
) -> Response {
    if resource_id <= 0 {
        return invalid_id();
    }
    save_alert_rule_handler(shared, resource_id, body).await
}

async fn delete_alert_rule(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(resource_id): Path<i64>,
) -> Response {
    if resource_id <= 0 {
        return invalid_id();
    }
    match blocking_write(&shared.db, move |tx| {
        repo::delete_alert_rule(tx, resource_id)
    })
    .await
    {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => alert_rule_not_found(),
        Err(err) => database_error(err, "删除告警规则失败"),
    }
}

// ---------- 静态端点 ----------

/// `GET /api/v2/admin/probe-capabilities`（纯静态，Go 同款字段）。
async fn probe_capabilities(
    State(_shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
) -> Response {
    let optional = [
        ("cpu", "disable_flag", "--disable-cpu"),
        ("memory", "disable_flag", "--disable-memory"),
        ("disk", "disable_flag", "--disable-disk"),
        ("network", "disable_flag", "--disable-network"),
        ("connections", "disable_flag", "--disable-connections"),
        ("processes", "disable_flag", "--disable-processes"),
        ("temperature", "enable_flag", "--temperature"),
        ("gpu", "enable_flag", "--gpu"),
        ("host_info", "disable_flag", "--disable-host-info"),
        ("ip_report", "disable_flag", "--disable-ip-report"),
        ("http_probe", "disable_flag", "--disable-http-probe"),
        ("icmp_probe", "disable_flag", "--disable-icmp-probe"),
        ("tcp_probe", "disable_flag", "--disable-tcp-probe"),
        ("nat", "disable_flag", "--disable-nat"),
    ];
    let options = |temperature: bool, gpu: bool| {
        json!({
            "cpu": true, "memory": true, "disk": true, "network": true,
            "connections": true, "processes": true,
            "temperature": temperature, "gpu": gpu,
            "host_info": true, "ip_report": true,
            "http_probe": true, "icmp_probe": true, "tcp_probe": true,
            "nat": false,
        })
    };
    data_json(json!({
        "required": ["heartbeat", "identity"],
        "optional": optional
            .iter()
            .map(|(id, flag_kind, flag)| {
                let mut entry = json!({ "id": id });
                entry[flag_kind] = json!(flag);
                entry
            })
            .collect::<Vec<_>>(),
        "presets": {
            "standard_cloud": options(false, false),
            "standard_physical": options(true, true),
            "light": {
                "cpu": true, "memory": true, "disk": true, "network": true,
                "host_info": true, "ip_report": true,
                "http_probe": true, "icmp_probe": true, "tcp_probe": true,
            },
            "alive": {},
        },
    }))
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn powershell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Go `appendServerIPFlags`：提示 IP 追加（Rust 版无 DNS 预解析，
/// 仅在 host 本身是 IP 字面量时提示自身）。
fn append_server_ip_flags(parts: &mut Vec<String>, host: &str, windows: bool) {
    let host = host.trim().trim_matches(['[', ']']);
    if host.parse::<std::net::IpAddr>().is_ok() {
        if windows {
            parts.push("-ServerIP".into());
            parts.push(powershell_quote(host));
        } else {
            parts.push("--server-ip".into());
            parts.push(shell_quote(host));
        }
    }
}

fn monitoring_options_from(value: Option<&Value>) -> (HashMap<String, bool>, Value) {
    let empty = json!({});
    let raw = value.cloned().unwrap_or(empty);
    let mut map = HashMap::new();
    if let Some(object) = raw.as_object() {
        for (key, value) in object {
            map.insert(key.clone(), value.as_bool().unwrap_or(false));
        }
    }
    (map, raw)
}

/// Go `installFlags`。
fn install_flags(
    options: &HashMap<String, bool>,
    windows: bool,
    ip_cfg: Option<&Value>,
) -> Vec<String> {
    let pairs: [(&str, bool, &str, &str); 14] = [
        ("cpu", false, "--disable-cpu", "-DisableCPU"),
        ("memory", false, "--disable-memory", "-DisableMemory"),
        ("disk", false, "--disable-disk", "-DisableDisk"),
        ("network", false, "--disable-network", "-DisableNetwork"),
        (
            "connections",
            false,
            "--disable-connections",
            "-DisableConnections",
        ),
        (
            "processes",
            false,
            "--disable-processes",
            "-DisableProcesses",
        ),
        ("temperature", true, "--temperature", "-Temperature"),
        ("gpu", true, "--gpu", "-GPU"),
        (
            "host_info",
            false,
            "--disable-host-info",
            "-DisableHostInfo",
        ),
        (
            "ip_report",
            false,
            "--disable-ip-report",
            "-DisableIPReport",
        ),
        (
            "http_probe",
            false,
            "--disable-http-probe",
            "-DisableHTTPProbe",
        ),
        (
            "icmp_probe",
            false,
            "--disable-icmp-probe",
            "-DisableICMPProbe",
        ),
        (
            "tcp_probe",
            false,
            "--disable-tcp-probe",
            "-DisableTCPProbe",
        ),
        ("nat", false, "--disable-nat", "-DisableNAT"),
    ];
    let mut out: Vec<String> = Vec::new();
    for (key, positive, unix_flag, win_flag) in pairs {
        let enabled = options.get(key).copied().unwrap_or(false);
        let include = if positive { enabled } else { !enabled };
        if include {
            out.push(if windows {
                win_flag.into()
            } else {
                unix_flag.into()
            });
        }
    }
    if options.get("ip_report").copied().unwrap_or(false)
        && let Some(ip_cfg) = ip_cfg
    {
        let iface = ip_cfg
            .get("interface")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        let code = ip_cfg
            .get("country_code")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        let prefer_ipv6 = ip_cfg
            .get("prefer_ipv6")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if windows {
            if !iface.is_empty() {
                out.push("-IpReportInterface".into());
                out.push(powershell_quote(&iface));
            }
            if !code.is_empty() {
                out.push("-CountryCode".into());
                out.push(powershell_quote(&code));
            }
            if prefer_ipv6 {
                out.push("-UseIPv6CountryCode".into());
            }
        } else {
            if !iface.is_empty() {
                out.push("--ip-report-interface".into());
                out.push(shell_quote(&iface));
            }
            if !code.is_empty() {
                out.push("--country-code".into());
                out.push(shell_quote(&code));
            }
            if prefer_ipv6 {
                out.push("--use-ipv6-countrycode".into());
            }
        }
    }
    out
}

fn public_grpc_port(conf: &crate::config::Config) -> u16 {
    if conf.proxy_grpc_port > 0 {
        conf.proxy_grpc_port
    } else if conf.grpc_port > 0 {
        conf.grpc_port
    } else {
        5555
    }
}

/// `POST /api/v2/admin/servers/{serverId}/install-preview`（Go `v2InstallPreview`）。
async fn install_preview(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(server_id): Path<i64>,
    body: axum::body::Bytes,
) -> Response {
    #[derive(Deserialize, Default)]
    struct Request {
        #[serde(default)]
        platform: String,
        #[serde(default)]
        clean_install: bool,
        #[serde(default)]
        options: Option<Value>,
        #[serde(default)]
        ip_report_config: Option<Value>,
        #[serde(default)]
        implementation: String,
    }
    if server_id <= 0 {
        return invalid_id();
    }
    let request: Request = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_install_preview",
                "请求体不是合法 JSON",
            );
        }
    };
    let exists = blocking_read(&shared.db, move |conn| {
        crate::repo::traffic::server_exists(conn, server_id)
    })
    .await;
    if !matches!(exists, Ok(true)) {
        return problem(StatusCode::NOT_FOUND, "server_not_found", "服务器不存在");
    }
    let implementation = request.implementation.trim().to_lowercase();
    let implementation = if implementation.is_empty() {
        "go".to_string()
    } else {
        implementation
    };
    if implementation != "go" && implementation != "rust" {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_install_preview",
            "implementation must be go or rust",
        );
    }
    let platform = request.platform.trim().to_lowercase();
    let conf = shared.conf();
    let host = if conf.grpc_host.is_empty() {
        "127.0.0.1".to_string()
    } else {
        conf.grpc_host.clone()
    };
    let script = if implementation == "rust" {
        if platform != "linux" {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_platform",
                "rust agent install is linux only",
            );
        }
        conf.install_script.linux_rust.clone()
    } else if platform == "macos" {
        conf.install_script.macos.clone()
    } else if platform == "windows" {
        conf.install_script.windows.clone()
    } else if platform == "linux" {
        conf.install_script.linux.clone()
    } else {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_platform",
            "platform must be linux, macos, or windows",
        );
    };
    let (options, options_raw) = monitoring_options_from(request.options.as_ref());
    let port = public_grpc_port(&conf) as u64;
    let use_tls = conf.tls;
    let windows = platform == "windows";
    let secret_plain = server_secret(&shared, server_id).await;
    let Some(secret_plain) = secret_plain else {
        return problem(StatusCode::NOT_FOUND, "server_not_found", "服务器不存在");
    };
    let mut parts: Vec<String> = if windows {
        vec![
            "& ([scriptblock]::Create((irm".into(),
            powershell_quote(&script),
            "))) -Server".into(),
            powershell_quote(&host),
            "-Port".into(),
            port.to_string(),
            "-Key".into(),
            powershell_quote(&secret_plain),
        ]
    } else {
        vec![
            "curl -fsSL".into(),
            shell_quote(&script),
            "| bash -s --".into(),
            shell_quote(&host),
            port.to_string(),
            shell_quote(&secret_plain),
        ]
    };
    if request.clean_install {
        if windows {
            parts.push("-CleanInstall".into());
            parts.push("-ConfirmCleanInstall".into());
        } else {
            parts.push("--clean-install".into());
            parts.push("--confirm-clean-install".into());
        }
    }
    if use_tls {
        parts.push(if windows {
            "-Tls".into()
        } else {
            "--tls".into()
        });
    }
    append_server_ip_flags(&mut parts, &host, windows);
    parts.extend(install_flags(
        &options,
        windows,
        request.ip_report_config.as_ref(),
    ));
    data_json(json!({
        "platform": platform,
        "command": parts.join(" "),
        "clean_install": request.clean_install,
        "options": options_raw,
        "ip_report_config": request.ip_report_config,
        "implementation": implementation,
    }))
}

async fn server_secret(shared: &ApiShared, server_id: i64) -> Option<String> {
    let ciphertext = blocking_read(&shared.db, move |conn| {
        conn.query_row(
            "SELECT secret_ciphertext FROM servers WHERE id = ?1 AND deleted_at IS NULL",
            [server_id],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .map_err(Into::into)
    })
    .await
    .ok()?;
    crate::secret::decrypt_secret(&ciphertext).ok()
}

/// `POST /api/v2/admin/servers/{serverId}/upgrade-preview`。
async fn upgrade_preview(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(server_id): Path<i64>,
    body: axum::body::Bytes,
) -> Response {
    #[derive(Deserialize, Default)]
    struct Request {
        #[serde(default)]
        platform: String,
    }
    if server_id <= 0 {
        return invalid_id();
    }
    let request: Request = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_upgrade_preview",
                "请求体不是合法 JSON",
            );
        }
    };
    let exists = blocking_read(&shared.db, move |conn| {
        crate::repo::traffic::server_exists(conn, server_id)
    })
    .await;
    if !matches!(exists, Ok(true)) {
        return problem(StatusCode::NOT_FOUND, "server_not_found", "服务器不存在");
    }
    let platform = request.platform.trim().to_lowercase();
    let conf = shared.conf();
    let script = match platform.as_str() {
        "macos" => conf.install_script.upgrade_macos.clone(),
        "windows" => conf.install_script.upgrade_windows.clone(),
        "linux" => conf.install_script.upgrade_linux.clone(),
        _ => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_platform",
                "platform must be linux, macos, or windows",
            );
        }
    };
    if script.trim().is_empty() {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_platform",
            "upgrade script url is empty",
        );
    }
    let command = match platform.as_str() {
        "macos" => format!("curl -fsSL {} | sudo bash", shell_quote(&script)),
        "windows" => format!(
            "& ([scriptblock]::Create((irm {}))))",
            powershell_quote(&script)
        ),
        _ => format!("curl -fsSL {} | bash", shell_quote(&script)),
    };
    data_json(json!({ "platform": platform, "command": command }))
}

/// `GET /api/v2/admin/script-commands`（Go `buildScriptCommands`）。
async fn script_commands(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
) -> Response {
    let conf = shared.conf();
    let scripts = &conf.install_script;
    let mut out: Vec<Value> = Vec::new();
    if !scripts.dashboard.trim().is_empty() {
        out.push(json!({
            "id": "dashboard_install", "group": "dashboard", "platform": "linux",
            "command": format!("sh -c \"$(curl -fsSL {})\"", shell_quote(&scripts.dashboard)),
            "destructive": false,
        }));
        // 二进制 + systemd 与 Docker 两种安装形态通吃，脚本按既有安装自动识别。
        out.push(json!({
            "id": "dashboard_upgrade", "group": "dashboard", "platform": "linux",
            "command": format!(
                "sh -c \"$(curl -fsSL {})\" -- --upgrade",
                shell_quote(&scripts.dashboard)
            ),
            "destructive": false,
        }));
    }
    for (id, platform, script) in [
        ("collector_upgrade", "linux", &scripts.upgradecollector),
        ("agent_upgrade_linux", "linux", &scripts.upgrade_linux),
        ("agent_upgrade_macos", "macos", &scripts.upgrade_macos),
        ("agent_upgrade_windows", "windows", &scripts.upgrade_windows),
    ] {
        if script.trim().is_empty() {
            continue;
        }
        let command = match platform {
            "macos" => format!("curl -fsSL {} | sudo bash", shell_quote(script)),
            "windows" => format!(
                "& ([scriptblock]::Create((irm {}))))",
                powershell_quote(script)
            ),
            _ => format!("curl -fsSL {} | bash", shell_quote(script)),
        };
        out.push(json!({
            "id": id, "group": if id.starts_with("collector") { "collector" } else { "agent" },
            "platform": platform, "command": command, "destructive": false,
        }));
    }
    out.push(json!({
        "id": "collector_remove", "group": "collector", "platform": "linux",
        // Rust 版从端安装目录（与 Go 版 /opt/santaizi/collector 共存）。
        "command": "cd /opt/santaizi/collector-rs && docker compose down", "destructive": true,
    }));
    out.push(json!({
        "id": "agent_uninstall_posix", "group": "agent", "platform": "posix",
        "command": "santaizi-agent-uninstall", "destructive": true,
    }));
    out.push(json!({
        "id": "agent_uninstall_windows", "group": "agent", "platform": "windows",
        "command": "C:\\santaizi\\santaizi-agent-uninstall.cmd", "destructive": true,
    }));
    data_json(json!({ "commands": out }))
}

/// typed 资源 + 静态端点路由组。
pub fn routes() -> axum::Router<ApiShared> {
    axum::Router::new()
        .route(
            "/api/v2/admin/monitors",
            axum::routing::get(list_monitors).post(create_monitor),
        )
        .route(
            "/api/v2/admin/monitors/{resourceId}",
            axum::routing::get(get_monitor)
                .patch(update_monitor)
                .delete(delete_monitor),
        )
        .route(
            "/api/v2/admin/notifications",
            axum::routing::get(list_notifications_handler).post(create_notification),
        )
        .route(
            "/api/v2/admin/notifications/{resourceId}",
            axum::routing::get(get_notification)
                .patch(update_notification)
                .delete(delete_notification),
        )
        .route(
            "/api/v2/admin/notifications/{resourceId}/test",
            axum::routing::post(test_notification),
        )
        .route(
            "/api/v2/admin/alert-rules",
            axum::routing::get(list_alert_rules_handler).post(create_alert_rule),
        )
        .route(
            "/api/v2/admin/alert-rules/{resourceId}",
            axum::routing::get(get_alert_rule)
                .patch(update_alert_rule)
                .delete(delete_alert_rule),
        )
        .route(
            "/api/v2/admin/probe-capabilities",
            axum::routing::get(probe_capabilities),
        )
        .route(
            "/api/v2/admin/script-commands",
            axum::routing::get(script_commands),
        )
        .route(
            "/api/v2/admin/servers/{serverId}/install-preview",
            axum::routing::post(install_preview),
        )
        .route(
            "/api/v2/admin/servers/{serverId}/upgrade-preview",
            axum::routing::post(upgrade_preview),
        )
}
