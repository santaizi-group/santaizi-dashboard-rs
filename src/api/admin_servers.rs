//! Admin 服务器端点（Phase 5）：对齐 Go `api_v2_spa.go` 的 `v2AdminServers` /
//! `v2SaveServer` / 批量与分组操作，和 `api_v2_typed.go` 的 credential 与
//! traffic-policies CRUD。
//!
//! schema v20 已裁 DDNS：`enable_ddns` / `ddns_profiles` 字段不接收不输出
//! （Go DTO 携带，这里省略）。导出/导入备份属后续刀。

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::api::pagination::{offset_of, parse_page};
use crate::db::RepoError;
use crate::repo::servers::{self, AdminServerRow, ServerWriteFields};
use crate::repo::traffic::PolicyWrite;
use crate::repo::{bindings, traffic as traffic_repo};
use crate::rpc::support::{blocking_read, blocking_write, blocking_write_repo};
use crate::secret;
use crate::util::time as util_time;

use super::auth::{AdminUser, ApiShared, data_json, list_json, list_json_paged, problem};
use super::presentation;

/// 纯 RFC3339 → Unix 纳秒（Go `*time.Time` 的 JSON 反序列化语义）。
fn parse_rfc3339(raw: &str) -> Option<i64> {
    OffsetDateTime::parse(raw.trim(), &Rfc3339)
        .ok()
        .map(OffsetDateTime::unix_timestamp_nanos)
        .and_then(|nanos| i64::try_from(nanos).ok())
}

fn invalid_server(detail: &str) -> Response {
    problem(StatusCode::BAD_REQUEST, "invalid_server", detail)
}

fn invalid_body() -> Response {
    invalid_server("请求体不是合法 JSON")
}

/// Go `serverAdminDTO`：管理侧完整行（密钥仅在 reveal 时附带）。
fn server_admin_dto(
    row: &AdminServerRow,
    secret_plain: Option<&str>,
    offline_threshold_seconds: u64,
    summaries: &[traffic_repo::TrafficSummary],
) -> Value {
    let presentation = presentation::runtime_presentation(&row.public);
    let mut item = json!({
        "id": row.public.id,
        "name": row.public.name,
        "tag": row.public.tag,
        "note": row.public.note,
        "public_note": presentation::decode_public_note(&row.public.public_note),
        "monitoring_options": serde_json::from_str::<Value>(&row.public.monitoring_options_raw)
            .unwrap_or_else(|_| json!({})),
        "display_index": row.public.display_index,
        "hide_for_guest": row.public.hide_for_guest,
        "probe_target": row.probe_target,
        "probe_tcp_ports": row.probe_tcp_ports,
        "probe_enable_icmp": row.probe_enable_icmp.map(|v| v != 0).unwrap_or(true),
        "probe_enable_tcp": row.probe_enable_tcp.map(|v| v != 0).unwrap_or(true),
        "probe_enable_mtr": row.probe_enable_mtr.map(|v| v != 0).unwrap_or(true),
        "last_active": row
            .public
            .last_received_at
            .filter(|nanos| *nanos > 0)
            .map(util_time::unix_nanos_to_rfc3339)
            .unwrap_or_default(),
        "host": presentation::host_json(row.public.host_payload.as_deref(), true),
        "state": presentation::state_json(row.public.state_payload.as_deref()),
        "online": presentation::online_flag(&presentation, offline_threshold_seconds),
        "telemetry": presentation::telemetry_json(&presentation),
        "traffic_summaries": summaries
            .iter()
            .map(|summary| {
                json!({
                    "policy_id": summary.policy_id,
                    "name": summary.name,
                    "used_bytes": summary.used_bytes,
                    "quota_bytes": summary.quota_bytes,
                    "usage_percent": summary.usage_percent,
                    "status": summary.status,
                })
            })
            .collect::<Vec<_>>(),
    });
    if let Some(secret) = secret_plain {
        item["secret"] = json!(secret);
    }
    item
}

/// 服务器可编辑字段的请求体（Go `serverV2Write`）。
#[derive(Deserialize, Default)]
struct ServerWrite {
    #[serde(default)]
    name: String,
    #[serde(default)]
    tag: String,
    #[serde(default)]
    note: String,
    #[serde(default)]
    public_note: Option<Value>,
    #[serde(default)]
    monitoring_options: Option<Value>,
    #[serde(default)]
    display_index: i64,
    #[serde(default)]
    hide_for_guest: bool,
    #[serde(default)]
    probe_target: String,
    #[serde(default)]
    probe_tcp_ports: String,
    probe_enable_icmp: Option<bool>,
    probe_enable_tcp: Option<bool>,
    probe_enable_mtr: Option<bool>,
    #[serde(default)]
    traffic_policies: Option<Vec<TrafficPolicyUpsert>>,
}

/// `map[string]any` → JSON 串；缺失/null 一律 `{}`（Go `applyServerWriteFields`）。
fn json_or_empty_object(value: &Option<Value>) -> String {
    let value = value.clone().unwrap_or_else(|| json!({}));
    if value.is_null() {
        return "{}".to_string();
    }
    serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string())
}

/// Go `boolOrKeep`：显式提供用新值；建号回落 true；更新保留现值。
fn bool_or_keep(provided: Option<bool>, created: bool, current: Option<i64>) -> bool {
    match provided {
        Some(value) => value,
        None if created => true,
        None => current.map(|v| v != 0).unwrap_or(true),
    }
}

/// 请求体 → 落库字段（不含探测开关的解析，见 `resolve_probe_flags`）。
fn write_fields(
    write: &ServerWrite,
    row: Option<&AdminServerRow>,
    created: bool,
) -> Result<ServerWriteFields, Box<Response>> {
    let name = write.name.trim().to_string();
    if name.is_empty() {
        return Err(Box::new(invalid_server("name is required")));
    }
    Ok(ServerWriteFields {
        name,
        tag: write.tag.trim().to_string(),
        note: write.note.clone(),
        public_note_raw: json_or_empty_object(&write.public_note),
        monitoring_options_raw: json_or_empty_object(&write.monitoring_options),
        display_index: write.display_index,
        hide_for_guest: write.hide_for_guest,
        probe_target: write.probe_target.trim().to_string(),
        probe_tcp_ports: write.probe_tcp_ports.trim().to_string(),
        probe_enable_icmp: bool_or_keep(
            write.probe_enable_icmp,
            created,
            row.and_then(|row| row.probe_enable_icmp),
        ),
        probe_enable_tcp: bool_or_keep(
            write.probe_enable_tcp,
            created,
            row.and_then(|row| row.probe_enable_tcp),
        ),
        probe_enable_mtr: bool_or_keep(
            write.probe_enable_mtr,
            created,
            row.and_then(|row| row.probe_enable_mtr),
        ),
    })
}

/// 策略 upsert 请求体（Go `trafficPolicyUpsertDTO`）。
#[derive(Clone, Deserialize)]
struct TrafficPolicyUpsert {
    #[serde(default)]
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    direction: String,
    #[serde(default)]
    mode: String,
    #[serde(default)]
    cycle_start: Option<String>,
    #[serde(default)]
    cycle_interval: u64,
    #[serde(default)]
    cycle_unit: String,
    #[serde(default)]
    quota_bytes: u64,
    #[serde(default)]
    warning_percent: f64,
    #[serde(default)]
    notification_tag: String,
    #[serde(default)]
    enabled: bool,
}

impl TrafficPolicyUpsert {
    fn to_write(&self) -> PolicyWrite {
        PolicyWrite {
            name: self.name.clone(),
            direction: self.direction.clone(),
            mode: self.mode.clone(),
            cycle_start_nanos: self
                .cycle_start
                .as_deref()
                .map(str::trim)
                .filter(|raw| !raw.is_empty())
                .and_then(parse_rfc3339),
            cycle_interval: self.cycle_interval,
            cycle_unit: self.cycle_unit.clone(),
            quota_bytes: self.quota_bytes,
            warning_percent: self.warning_percent,
            notification_tag: self.notification_tag.clone(),
            enabled: self.enabled,
        }
    }
}

fn policy_write_error(detail: String) -> Response {
    problem(StatusCode::BAD_REQUEST, "invalid_traffic_policy", &detail)
}

/// 管理端策略行 → JSON（Go `trafficPolicyDTO`；时间为 RFC3339）。
fn policy_dto(row: &traffic_repo::TrafficPolicyFullRow) -> Value {
    json!({
        "id": row.id,
        "server_id": row.server_id,
        "name": row.name,
        "direction": row.direction,
        "mode": row.mode,
        "cycle_start": row.cycle_start_nanos.map(util_time::unix_nanos_to_rfc3339),
        "cycle_interval": row.cycle_interval,
        "cycle_unit": row.cycle_unit,
        "quota_bytes": row.quota_bytes,
        "warning_percent": row.warning_percent,
        "notification_tag": row.notification_tag,
        "enabled": row.enabled,
        "created_at": row.created_at_nanos.map(util_time::unix_nanos_to_rfc3339),
        "updated_at": row.updated_at_nanos.map(util_time::unix_nanos_to_rfc3339),
    })
}

/// Go `trafficUsageDTO`（usage 结果 + 策略身份字段）。
fn usage_dto(
    row: &traffic_repo::TrafficPolicyFullRow,
    usage: &traffic_repo::TrafficUsage,
    now_nanos: i64,
) -> Value {
    json!({
        "policy_id": row.id,
        "server_id": row.server_id,
        "direction": row.direction,
        "mode": row.mode,
        "window_start": util_time::unix_nanos_to_rfc3339(usage.window_start),
        "window_end": usage.window_end.map(util_time::unix_nanos_to_rfc3339),
        "used_bytes": usage.used_bytes,
        "quota_bytes": usage.quota_bytes,
        "warning_percent": usage.warning_percent,
        "usage_percent": usage.usage_percent,
        "status": usage.status,
        "updated_at": util_time::unix_nanos_to_rfc3339(now_nanos),
    })
}

/// `GET /api/v2/admin/servers`（Go `v2AdminServers`）。
async fn list_servers(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let (page, size) = parse_page(&params);
    // 通配符转义与 `%…%` 包装都在 repo 层（`list_admin`）做，调用方只给原始词。
    let q = params
        .get("q")
        .map(String::as_str)
        .unwrap_or("")
        .trim()
        .to_owned();
    let sort = params.get("sort").cloned().unwrap_or_default();
    let ascending = params
        .get("order")
        .is_some_and(|value| value.eq_ignore_ascii_case("asc"));
    let now_nanos = util_time::now_nanos();
    let result = blocking_read(&shared.db, move |conn| {
        let (rows, total) = servers::list_admin(
            conn,
            &q,
            &sort,
            ascending,
            offset_of(page, size),
            size,
            now_nanos,
        )?;
        let ids: Vec<i64> = rows.iter().map(|row| row.public.id).collect();
        let summaries = traffic_repo::summaries(conn, &ids, now_nanos)?;
        Ok((rows, total, summaries))
    })
    .await;
    let (rows, total, summaries) = match result {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(error = %err, "admin server list query failed");
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "查询服务器失败",
            );
        }
    };
    let threshold = shared.conf().telemetry.offline_threshold_seconds;
    let items: Vec<Value> = rows
        .iter()
        .map(|row| {
            let empty = Vec::new();
            let row_summaries = summaries.get(&row.public.id).unwrap_or(&empty);
            server_admin_dto(row, None, threshold, row_summaries)
        })
        .collect();
    list_json_paged(Value::Array(items), page, size, total)
}

/// `GET /api/v2/admin/servers/{serverId}`（Go `v2AdminServer`）。
async fn get_server(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(server_id): Path<i64>,
) -> Response {
    if server_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    let now_nanos = util_time::now_nanos();
    let row = blocking_read(&shared.db, move |conn| {
        let row = servers::admin_server_by_id(conn, server_id, now_nanos)?;
        let mut summaries = traffic_repo::summaries(conn, &[server_id], now_nanos)?;
        Ok(row.map(|row| (row, summaries.remove(&server_id).unwrap_or_default())))
    })
    .await;
    match row {
        Ok(Some((row, summaries))) => {
            let threshold = shared.conf().telemetry.offline_threshold_seconds;
            data_json(server_admin_dto(&row, None, threshold, &summaries))
        }
        Ok(None) => problem(StatusCode::NOT_FOUND, "server_not_found", "服务器不存在"),
        Err(err) => {
            tracing::warn!(error = %err, "admin server query failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "查询服务器失败",
            )
        }
    }
}

/// `POST /api/v2/admin/servers`（Go `v2CreateServer` → `v2SaveServer(0)`）。
async fn create_server(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    body: axum::body::Bytes,
) -> Response {
    let write: ServerWrite = match serde_json::from_slice(&body) {
        Ok(write) => write,
        Err(_) => return invalid_body(),
    };
    let fields = match write_fields(&write, None, true) {
        Ok(fields) => fields,
        Err(response) => return *response,
    };
    let policies = write
        .traffic_policies
        .clone()
        .map(|list| {
            list.iter()
                .map(|item| (item.id, item.to_write()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let plain = servers::new_client_secret();
    let ciphertext = match secret::encrypt_secret(&plain) {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(error = %err, "encrypt server secret failed");
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "secret_generation_failed",
                "生成服务器密钥失败",
            );
        }
    };
    let now_nanos = util_time::now_nanos();
    let created = blocking_write(&shared.db, move |tx| {
        let id = servers::insert_admin_server(tx, &ciphertext, &fields)?;
        if !policies.is_empty() {
            traffic_repo::replace_policies(tx, id, policies)?;
        }
        Ok(id)
    })
    .await;
    let id = match created {
        Ok(id) => id,
        Err(err) => {
            if traffic_write_error(&err) {
                return policy_write_error(format!("{err:#}"));
            }
            tracing::warn!(error = %err, "server create failed");
            return problem(
                StatusCode::BAD_REQUEST,
                "server_create_failed",
                "创建服务器失败",
            );
        }
    };
    shared.secrets.rebind(id, &plain);
    server_response_after_save(&shared, id, now_nanos, Some(&plain), StatusCode::CREATED).await
}

/// `PATCH /api/v2/admin/servers/{serverId}`（Go `v2UpdateServer`）。
async fn update_server(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(server_id): Path<i64>,
    body: axum::body::Bytes,
) -> Response {
    if server_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    let write: ServerWrite = match serde_json::from_slice(&body) {
        Ok(write) => write,
        Err(_) => return invalid_body(),
    };
    let policies = write
        .traffic_policies
        .clone()
        .map(|list| {
            list.iter()
                .map(|item| (item.id, item.to_write()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let now_nanos = util_time::now_nanos();
    let row = blocking_read(&shared.db, move |conn| {
        servers::admin_server_by_id(conn, server_id, now_nanos)
    })
    .await;
    let row = match row {
        Ok(Some(row)) => row,
        Ok(None) => return problem(StatusCode::NOT_FOUND, "server_not_found", "服务器不存在"),
        Err(err) => {
            tracing::warn!(error = %err, "admin server query failed");
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "查询服务器失败",
            );
        }
    };
    let fields = match write_fields(&write, Some(&row), false) {
        Ok(fields) => fields,
        Err(response) => return *response,
    };
    let saved = blocking_write(&shared.db, move |tx| {
        anyhow::ensure!(
            servers::update_admin_server(tx, server_id, &fields)? > 0,
            "server missing"
        );
        if !policies.is_empty() {
            traffic_repo::replace_policies(tx, server_id, policies)?;
        }
        Ok(())
    })
    .await;
    match saved {
        Ok(()) => {}
        Err(err) if traffic_write_error(&err) => return policy_write_error(format!("{err:#}")),
        Err(err) => {
            tracing::warn!(error = %err, "server update failed");
            return problem(
                StatusCode::BAD_REQUEST,
                "server_update_failed",
                "更新服务器失败",
            );
        }
    }
    // Go：指派刷新是保存后的第二个事务，失败返回 500。
    let refreshed = blocking_write(&shared.db, move |tx| {
        bindings::refresh_observer_assignments_for_server(tx, server_id, util_time::now_nanos())
    })
    .await;
    if let Err(err) = refreshed {
        tracing::warn!(error = %err, "assignment refresh failed");
        return problem(
            StatusCode::INTERNAL_SERVER_ERROR,
            "assignment_refresh_failed",
            "刷新观测指派失败",
        );
    }
    server_response_after_save(&shared, server_id, now_nanos, None, StatusCode::OK).await
}

fn traffic_write_error(err: &anyhow::Error) -> bool {
    let text = format!("{err:#}");
    text.starts_with("traffic policy ")
        || text.starts_with("duplicate traffic policy")
        || text.starts_with("traffic direction")
        || text.starts_with("traffic mode")
        || text.starts_with("traffic quota")
        || text.starts_with("warning percent")
        || text.starts_with("recurring traffic policy")
        || text.starts_with("cycle unit")
}

/// 保存成功后的响应：单查回 DTO + 附 traffic_policies（Go `v2SaveServer` 收尾）。
async fn server_response_after_save(
    shared: &ApiShared,
    server_id: i64,
    now_nanos: i64,
    secret_plain: Option<&str>,
    status: StatusCode,
) -> Response {
    let result = blocking_read(&shared.db, move |conn| {
        let row = servers::admin_server_by_id(conn, server_id, now_nanos)?
            .ok_or_else(|| anyhow::anyhow!("server missing"))?;
        let policies = traffic_repo::list_for_server(conn, server_id)?;
        Ok((row, policies))
    })
    .await;
    match result {
        Ok((row, policies)) => {
            let threshold = shared.conf().telemetry.offline_threshold_seconds;
            let mut item = server_admin_dto(&row, secret_plain, threshold, &[]);
            item["traffic_policies"] = Value::Array(policies.iter().map(policy_dto).collect());
            let mut response = Response::new(
                serde_json::to_string(&json!({ "data": item }))
                    .unwrap_or_default()
                    .into(),
            );
            *response.status_mut() = status;
            response.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                "application/json".parse().unwrap(),
            );
            response
        }
        Err(err) => {
            tracing::warn!(error = %err, "server reload after save failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "查询服务器失败",
            )
        }
    }
}

/// `PATCH /api/v2/admin/servers/{serverId}/display-index`。
async fn update_display_index(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(server_id): Path<i64>,
    body: axum::body::Bytes,
) -> Response {
    #[derive(Deserialize, Default)]
    struct Request {
        #[serde(default)]
        display_index: i64,
    }
    if server_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    let request: Request = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_display_index",
                "请求体不是合法 JSON",
            );
        }
    };
    let now_nanos = util_time::now_nanos();
    let updated = blocking_write(&shared.db, move |tx| {
        tx.execute(
            "UPDATE servers SET display_index = ?2, updated_at = CURRENT_TIMESTAMP \
             WHERE id = ?1 AND deleted_at IS NULL",
            rusqlite::params![server_id, request.display_index],
        )
        .map_err(Into::into)
    })
    .await;
    match updated {
        Ok(changed) if changed > 0 => {}
        Ok(_) => return problem(StatusCode::NOT_FOUND, "server_not_found", "服务器不存在"),
        Err(err) => {
            tracing::warn!(error = %err, "display index update failed");
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "display_index_update_failed",
                "更新排序失败",
            );
        }
    }
    server_response_after_save(&shared, server_id, now_nanos, None, StatusCode::OK).await
}

/// `DELETE /api/v2/admin/servers/{serverId}`（Go `v2DeleteServer`）。
async fn delete_server(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(server_id): Path<i64>,
) -> Response {
    if server_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    let deleted = blocking_write(&shared.db, move |tx| servers::delete_server(tx, server_id)).await;
    match deleted {
        Ok(_) => {
            shared.secrets.forget_server(server_id);
            axum::http::StatusCode::NO_CONTENT.into_response()
        }
        Err(err) => {
            tracing::warn!(error = %err, "server delete failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_delete_failed",
                "删除服务器失败",
            )
        }
    }
}

/// `POST /api/v2/admin/servers/{serverId}/reset-secret`。
async fn reset_secret(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(server_id): Path<i64>,
) -> Response {
    if server_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    let plain = servers::new_client_secret();
    let ciphertext = match secret::encrypt_secret(&plain) {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(error = %err, "encrypt server secret failed");
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "secret_generation_failed",
                "生成服务器密钥失败",
            );
        }
    };
    // 错误带类型而不是字符串：旧实现用 `err.to_string().contains("server not found")`
    // 反推 404，文案一改就静默退化成 500。
    let updated = blocking_write_repo(&shared.db, move |tx| {
        let changed = tx
            .execute(
                "UPDATE servers SET secret_ciphertext = ?2, updated_at = CURRENT_TIMESTAMP \
                 WHERE id = ?1 AND deleted_at IS NULL",
                rusqlite::params![server_id, &ciphertext],
            )
            .map_err(RepoError::from_rusqlite)?;
        if changed == 0 {
            return Err(RepoError::not_found("服务器不存在"));
        }
        Ok(())
    })
    .await;
    match updated {
        Ok(()) => {
            shared.secrets.rebind(server_id, &plain);
            data_json(json!({ "secret": plain }))
        }
        Err(RepoError::NotFound(_)) => {
            problem(StatusCode::NOT_FOUND, "server_not_found", "服务器不存在")
        }
        Err(err) => {
            tracing::warn!(error = %err, "secret reset failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "secret_save_failed",
                "保存服务器密钥失败",
            )
        }
    }
}

/// `POST /api/v2/admin/servers/{serverId}/reset-availability`。
async fn reset_availability(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(server_id): Path<i64>,
) -> Response {
    if server_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    let now_nanos = util_time::now_nanos();
    let deleted = blocking_write(&shared.db, move |tx| {
        servers::reset_availability(tx, server_id, now_nanos)
    })
    .await;
    match deleted {
        Ok(count) => data_json(json!({ "deleted": count })),
        Err(err) => {
            tracing::warn!(error = %err, "availability reset failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "availability_reset_failed",
                "重置可用性失败",
            )
        }
    }
}

#[derive(Deserialize, Default)]
struct BatchServerRequest {
    #[serde(default)]
    ids: Vec<i64>,
    #[serde(default)]
    group: String,
}

fn invalid_batch() -> Response {
    problem(
        StatusCode::BAD_REQUEST,
        "invalid_batch",
        "至少选择一台服务器",
    )
}

/// `POST /api/v2/admin/servers/batch/group`（Go `v2BatchServerGroup`）。
async fn batch_group(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    body: axum::body::Bytes,
) -> Response {
    let request: BatchServerRequest = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return invalid_batch(),
    };
    if request.ids.is_empty() {
        return invalid_batch();
    }
    let ids = request.ids.clone();
    let group = request.group;
    let now_nanos = util_time::now_nanos();
    let updated = blocking_write(&shared.db, move |tx| {
        // 全部编号占位：ids 占 ?1..?n，组名占 ?n+1（SQLite `?` 按文本序取号）。
        let placeholders = (1..=ids.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "UPDATE servers SET tag = ?{}, updated_at = CURRENT_TIMESTAMP \
             WHERE id IN ({placeholders})",
            ids.len() + 1
        );
        let mut bind: Vec<&dyn rusqlite::ToSql> =
            ids.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
        bind.push(&group);
        tx.execute(&sql, bind.as_slice())
            .map_err(anyhow::Error::from)?;
        for id in &ids {
            bindings::refresh_observer_assignments_for_server(tx, *id, now_nanos)?;
        }
        Ok(ids.len())
    })
    .await;
    match updated {
        Ok(count) => data_json(json!({ "updated": count })),
        Err(err) => {
            tracing::warn!(error = %err, "batch group update failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "batch_update_failed",
                "批量更新失败",
            )
        }
    }
}

/// `POST /api/v2/admin/servers/batch/delete`（Go `v2BatchServerDelete`）。
async fn batch_delete(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    body: axum::body::Bytes,
) -> Response {
    let request: BatchServerRequest = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return invalid_batch(),
    };
    if request.ids.is_empty() {
        return invalid_batch();
    }
    let ids = request.ids;
    let deleted = blocking_write(&shared.db, move |tx| {
        for id in &ids {
            servers::delete_server(tx, *id)?;
        }
        Ok(ids)
    })
    .await;
    match deleted {
        Ok(ids) => {
            for id in ids {
                shared.secrets.forget_server(id);
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Err(err) => {
            tracing::warn!(error = %err, "batch delete failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "batch_delete_failed",
                "批量删除失败",
            )
        }
    }
}

/// `GET /api/v2/admin/server-groups`（Go `v2ListServerGroups`）。
async fn list_groups(State(shared): State<ApiShared>, AdminUser(_admin): AdminUser) -> Response {
    let groups = blocking_read(&shared.db, servers::list_groups).await;
    match groups {
        Ok(groups) => {
            let items: Vec<Value> = groups
                .into_iter()
                .map(|(name, count)| json!({ "name": name, "count": count }))
                .collect();
            let total = items.len() as i64;
            list_json(Value::Array(items), total)
        }
        Err(err) => {
            tracing::warn!(error = %err, "server groups query failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "查询分组失败",
            )
        }
    }
}

/// `POST /api/v2/admin/server-groups/rename`（Go `v2RenameServerGroup`）。
async fn rename_group(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    body: axum::body::Bytes,
) -> Response {
    #[derive(Deserialize, Default)]
    struct Request {
        #[serde(default)]
        from: String,
        #[serde(default)]
        to: String,
    }
    let request: Request = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_group_rename",
                "请求体不是合法 JSON",
            );
        }
    };
    let from = request.from.trim().to_string();
    let to = request.to.trim().to_string();
    if from == to {
        return data_json(json!({ "updated": 0 }));
    }
    let now_nanos = util_time::now_nanos();
    let renamed = blocking_write(&shared.db, move |tx| {
        let ids: Vec<i64> = {
            let mut stmt =
                tx.prepare("SELECT id FROM servers WHERE tag = ?1 AND deleted_at IS NULL")?;
            let rows = stmt.query_map([&from], |row| row.get(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        if ids.is_empty() {
            return Ok(0);
        }
        tx.execute(
            "UPDATE servers SET tag = ?2, updated_at = CURRENT_TIMESTAMP WHERE tag = ?1",
            [&from, &to],
        )?;
        for id in &ids {
            bindings::refresh_observer_assignments_for_server(tx, *id, now_nanos)?;
        }
        Ok(ids.len())
    })
    .await;
    match renamed {
        Ok(0) => problem(
            StatusCode::NOT_FOUND,
            "group_not_found",
            "分组不存在或没有服务器",
        ),
        Ok(count) => data_json(json!({ "updated": count })),
        Err(err) => {
            tracing::warn!(error = %err, "group rename failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "group_rename_failed",
                "重命名分组失败",
            )
        }
    }
}

/// `GET /api/v2/admin/servers/{serverId}/credential`（Go `v2ServerCredential`）。
async fn server_credential(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(server_id): Path<i64>,
) -> Response {
    if server_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    let row = blocking_read(&shared.db, move |conn| {
        conn.query_row(
            "SELECT secret_ciphertext FROM servers WHERE id = ?1 AND deleted_at IS NULL",
            [server_id],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .map_err(Into::into)
    })
    .await;
    match row {
        Ok(ciphertext) => {
            let secret_plain = secret::decrypt_secret(&ciphertext).unwrap_or_default();
            let conf = shared.conf();
            let grpc_port = if conf.proxy_grpc_port > 0 {
                conf.proxy_grpc_port
            } else {
                conf.grpc_port
            };
            data_json(json!({
                "server_id": server_id,
                "secret": secret_plain,
                "grpc_host": conf.grpc_host,
                "grpc_port": grpc_port,
            }))
        }
        Err(err) => {
            tracing::warn!(error = %err, "server credential query failed");
            problem(StatusCode::NOT_FOUND, "server_not_found", "服务器不存在")
        }
    }
}

/// `GET /api/v2/admin/servers/{serverId}/traffic-policies`（Go `v2ListTrafficPolicies`）。
async fn list_traffic_policies(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(server_id): Path<i64>,
) -> Response {
    if server_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    let now_nanos = util_time::now_nanos();
    let rows = blocking_read(&shared.db, move |conn| {
        let rows = traffic_repo::list_for_server(conn, server_id)?;
        let mut items = Vec::with_capacity(rows.len());
        for row in &rows {
            let usage = traffic_repo::usage(conn, &row.window_row(), now_nanos).unwrap_or(None);
            items.push((row.clone(), usage));
        }
        Ok(items)
    })
    .await;
    match rows {
        Ok(rows) => {
            let items: Vec<Value> = rows
                .iter()
                .map(|(row, usage)| {
                    let mut item = policy_dto(row);
                    if let Some(usage) = usage {
                        item["usage"] = usage_dto(row, usage, now_nanos);
                    }
                    item
                })
                .collect();
            let total = items.len() as i64;
            list_json(Value::Array(items), total)
        }
        Err(err) => {
            tracing::warn!(error = %err, "traffic policies query failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "查询流量策略失败",
            )
        }
    }
}

/// `GET /api/v2/admin/servers/{serverId}/traffic-policies/{policyId}`。
async fn get_traffic_policy(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path((server_id, policy_id)): Path<(i64, i64)>,
) -> Response {
    if server_id <= 0 || policy_id <= 0 {
        return policy_id_problem(server_id, policy_id);
    }
    let row = blocking_read(&shared.db, move |conn| {
        traffic_repo::get_policy(conn, server_id, policy_id)
    })
    .await;
    match row {
        Ok(Some(row)) => data_json(policy_dto(&row)),
        Ok(None) => policy_not_found(),
        Err(err) => {
            tracing::warn!(error = %err, "traffic policy query failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "查询流量策略失败",
            )
        }
    }
}

fn policy_id_problem(server_id: i64, _policy_id: i64) -> Response {
    if server_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    problem(
        StatusCode::BAD_REQUEST,
        "invalid_policy_id",
        "流量策略 ID 无效",
    )
}

fn policy_not_found() -> Response {
    problem(
        StatusCode::NOT_FOUND,
        "traffic_policy_not_found",
        "流量策略不存在",
    )
}

/// `POST` 与 `PATCH /traffic-policies`（Go `v2SaveTrafficPolicy`）。
async fn save_traffic_policy(
    shared: ApiShared,
    server_id: i64,
    policy_id: Option<i64>,
    body: axum::body::Bytes,
) -> Response {
    let upsert: TrafficPolicyUpsert = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return policy_write_error("请求体不是合法 JSON".into());
        }
    };
    if server_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    if policy_id.is_some_and(|id| id <= 0) {
        return policy_id_problem(server_id, policy_id.unwrap_or(0));
    }
    let mut write = upsert.to_write();
    if let Err(detail) = traffic_repo::validate(&mut write) {
        return policy_write_error(detail);
    }
    let saved = blocking_write(&shared.db, move |tx| {
        if !traffic_repo::server_exists(tx, server_id)? {
            return Ok(None);
        }
        match policy_id {
            None => traffic_repo::insert_policy(tx, server_id, &write).map(Some),
            Some(policy_id) => {
                if traffic_repo::update_policy(tx, server_id, policy_id, &write)? {
                    Ok(Some(policy_id))
                } else {
                    Ok(None)
                }
            }
        }
    })
    .await;
    let id = match saved {
        Ok(Some(id)) => id,
        Ok(None) => {
            // 服务器或策略不存在同形 404，由存在性查询区分（Go 两次查询）。
            let exists = blocking_read(&shared.db, move |conn| {
                traffic_repo::server_exists(conn, server_id)
            })
            .await;
            return match exists {
                Ok(true) => policy_not_found(),
                _ => problem(StatusCode::NOT_FOUND, "server_not_found", "服务器不存在"),
            };
        }
        Err(err) => {
            tracing::warn!(error = %err, "traffic policy save failed");
            return problem(
                StatusCode::BAD_REQUEST,
                "traffic_policy_save_failed",
                "保存流量策略失败",
            );
        }
    };
    let row = blocking_read(&shared.db, move |conn| {
        traffic_repo::get_policy(conn, server_id, id)
    })
    .await;
    match row {
        Ok(Some(row)) => {
            let mut response = data_json(policy_dto(&row));
            if policy_id.is_none() {
                *response.status_mut() = StatusCode::CREATED;
            }
            response
        }
        _ => policy_not_found(),
    }
}

async fn create_traffic_policy(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(server_id): Path<i64>,
    body: axum::body::Bytes,
) -> Response {
    save_traffic_policy(shared, server_id, None, body).await
}

async fn update_traffic_policy(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path((server_id, policy_id)): Path<(i64, i64)>,
    body: axum::body::Bytes,
) -> Response {
    save_traffic_policy(shared, server_id, Some(policy_id), body).await
}

/// `DELETE /traffic-policies/{policyId}`（Go `v2DeleteTrafficPolicy`）。
async fn delete_traffic_policy(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path((server_id, policy_id)): Path<(i64, i64)>,
) -> Response {
    if server_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    if policy_id <= 0 {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_policy_id",
            "流量策略 ID 无效",
        );
    }
    let deleted = blocking_write(&shared.db, move |tx| {
        traffic_repo::delete_policy(tx, server_id, policy_id)
    })
    .await;
    match deleted {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => policy_not_found(),
        Err(err) => {
            tracing::warn!(error = %err, "traffic policy delete failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "traffic_policy_delete_failed",
                "删除流量策略失败",
            )
        }
    }
}

/// `GET /traffic-policies/{policyId}/usage`（Go `v2TrafficPolicyUsage`）。
async fn traffic_policy_usage(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path((server_id, policy_id)): Path<(i64, i64)>,
) -> Response {
    if server_id <= 0 || policy_id <= 0 {
        return policy_id_problem(server_id, policy_id);
    }
    let now_nanos = util_time::now_nanos();
    let result = blocking_read(&shared.db, move |conn| {
        let row = traffic_repo::get_policy(conn, server_id, policy_id)?;
        let usage = match &row {
            Some(row) => traffic_repo::usage(conn, &row.window_row(), now_nanos)?,
            None => None,
        };
        Ok(row.zip(usage))
    })
    .await;
    match result {
        Ok(Some((row, usage))) => data_json(usage_dto(&row, &usage, now_nanos)),
        Ok(None) => policy_not_found(),
        Err(err) => {
            tracing::warn!(error = %err, "traffic usage query failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "traffic_usage_failed",
                "查询流量用量失败",
            )
        }
    }
}

/// `GET /api/v2/admin/servers/{serverId}/traffic-history`（Go `v2ServerTrafficHistory`）。
async fn server_traffic_history(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(server_id): Path<i64>,
) -> Response {
    if server_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    let now_nanos = util_time::now_nanos();
    let result = blocking_read(&shared.db, move |conn| {
        if !traffic_repo::server_exists(conn, server_id)? {
            return Ok(None);
        }
        traffic_repo::histories(conn, server_id, now_nanos).map(Some)
    })
    .await;
    let items = match result {
        Ok(Some(items)) => items,
        Ok(None) => return problem(StatusCode::NOT_FOUND, "server_not_found", "服务器不存在"),
        Err(err) => {
            tracing::warn!(error = %err, "traffic history query failed");
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "traffic_history_failed",
                "查询流量历史失败",
            );
        }
    };
    let payload: Vec<Value> = items
        .iter()
        .map(|item| {
            json!({
                "policy_id": item.policy.id,
                "server_id": item.policy.server_id,
                "name": item.policy.name,
                "enabled": item.policy.enabled,
                "direction": item.policy.direction,
                "usage": item.usage.as_ref().map(|usage| usage_dto(&item.policy, usage, now_nanos)),
                "hourly": item.hourly.iter().map(point_dto).collect::<Vec<_>>(),
                "daily": item.daily.iter().map(point_dto).collect::<Vec<_>>(),
            })
        })
        .collect();
    let total = payload.len() as i64;
    list_json(Value::Array(payload), total)
}

fn point_dto(point: &traffic_repo::TrafficPoint) -> Value {
    json!({
        "window_start": util_time::unix_nanos_to_rfc3339(point.start_nanos),
        "window_end": util_time::unix_nanos_to_rfc3339(point.end_nanos),
        "bytes": point.bytes,
    })
}

/// admin 服务器路由组（Go `admin.GET/POST/PATCH/DELETE` 注册）。
pub fn routes() -> axum::Router<ApiShared> {
    use axum::routing::{get, post};
    axum::Router::new()
        .route(
            "/api/v2/admin/servers",
            get(list_servers).post(create_server),
        )
        .route("/api/v2/admin/servers/export", get(export_servers))
        .route(
            "/api/v2/admin/servers/import/preview",
            axum::routing::post(preview_server_import),
        )
        .route(
            "/api/v2/admin/servers/import",
            axum::routing::post(import_servers),
        )
        .route("/api/v2/admin/servers/batch/group", post(batch_group))
        .route("/api/v2/admin/servers/batch/delete", post(batch_delete))
        .route("/api/v2/admin/server-groups", get(list_groups))
        .route("/api/v2/admin/server-groups/rename", post(rename_group))
        .route(
            "/api/v2/admin/servers/{serverId}",
            get(get_server).patch(update_server).delete(delete_server),
        )
        .route(
            "/api/v2/admin/servers/{serverId}/display-index",
            axum::routing::patch(update_display_index),
        )
        .route(
            "/api/v2/admin/servers/{serverId}/reset-secret",
            post(reset_secret),
        )
        .route(
            "/api/v2/admin/servers/{serverId}/reset-availability",
            post(reset_availability),
        )
        .route(
            "/api/v2/admin/servers/{serverId}/credential",
            get(server_credential),
        )
        .route(
            "/api/v2/admin/servers/{serverId}/traffic-policies",
            get(list_traffic_policies).post(create_traffic_policy),
        )
        .route(
            "/api/v2/admin/servers/{serverId}/traffic-policies/{policyId}",
            get(get_traffic_policy)
                .patch(update_traffic_policy)
                .delete(delete_traffic_policy),
        )
        .route(
            "/api/v2/admin/servers/{serverId}/traffic-policies/{policyId}/usage",
            get(traffic_policy_usage),
        )
        .route(
            "/api/v2/admin/servers/{serverId}/traffic-history",
            get(server_traffic_history),
        )
}

// ---------- export / import 备份（Go api_v2_server_backup.go） ----------

/// 备份格式标识（Go `serverBackupFormatV1`）。
const BACKUP_FORMAT_V1: &str = "santaizi.servers.v1";

#[derive(Clone, Deserialize)]
struct BackupItem {
    #[serde(default)]
    name: String,
    #[serde(default)]
    tag: String,
    #[serde(default)]
    note: String,
    #[serde(default)]
    public_note: Option<Value>,
    #[serde(default)]
    monitoring_options: Option<Value>,
    #[serde(default)]
    display_index: i64,
    #[serde(default)]
    hide_for_guest: bool,
    #[serde(default)]
    traffic_policies: Vec<TrafficPolicyUpsert>,
    #[serde(default)]
    probe_target: String,
    #[serde(default)]
    probe_tcp_ports: String,
    probe_enable_icmp: Option<bool>,
    probe_enable_tcp: Option<bool>,
    probe_enable_mtr: Option<bool>,
    #[serde(default)]
    secret: Option<String>,
}

#[derive(Clone, Deserialize)]
struct BackupDocument {
    format: String,
    #[serde(default)]
    servers: Vec<BackupItem>,
}

/// 规范化备份文档（Go `normalizeServerBackup`；DDNS 字段 schema v20 已裁，
/// 出现在文档里的 enable_ddns/ddns_profiles 会被 serde 忽略）。
fn normalize_backup(document: &mut BackupDocument) -> Result<(), String> {
    if document.format != BACKUP_FORMAT_V1 {
        return Err("unsupported backup format".into());
    }
    if document.servers.len() > 5000 {
        return Err("backup contains too many servers".into());
    }
    let mut seen = std::collections::HashSet::new();
    for item in &mut document.servers {
        if let Some(secret) = item
            .secret
            .as_deref()
            .map(str::trim)
            .filter(|raw| !raw.is_empty())
        {
            if secret.len() > 128 {
                return Err("secret is too long".into());
            }
            item.secret = Some(secret.to_string());
        } else {
            item.secret = None;
        }
        item.name = item.name.trim().to_string();
        if item.name.is_empty() || item.name.len() > 100 {
            return Err("server name is required".into());
        }
        if !seen.insert(item.name.clone()) {
            return Err("backup contains duplicate server names".into());
        }
        item.tag = item.tag.trim().to_string();
        item.probe_target = item.probe_target.trim().to_string();
        item.probe_tcp_ports = item.probe_tcp_ports.trim().to_string();
    }
    Ok(())
}

/// serde_json Map 默认 BTreeMap（键序输出），字符串化即 canonical JSON。
fn canonical(value: &Value) -> String {
    value.to_string()
}

/// 当前库内策略 → 备份 DTO 形态（与导入侧 PolicyWrite 归一化后同形可比）。
fn policy_backup_canonical(rows: &[traffic_repo::TrafficPolicyFullRow]) -> Value {
    Value::Array(
        rows.iter()
            .map(|row| {
                json!({
                    "name": row.name,
                    "direction": row.direction,
                    "mode": row.mode,
                    "cycle_start": row.cycle_start_nanos.map(util_time::unix_nanos_to_rfc3339),
                    "cycle_interval": row.cycle_interval,
                    "cycle_unit": row.cycle_unit,
                    "quota_bytes": row.quota_bytes,
                    "warning_percent": row.warning_percent,
                    "notification_tag": row.notification_tag,
                    "enabled": row.enabled,
                })
            })
            .collect(),
    )
}

/// 导入侧策略 → 归一化后的可比形态。
fn policy_incoming_canonical(items: &[TrafficPolicyUpsert]) -> Result<Value, String> {
    let mut out = Vec::new();
    for item in items {
        let mut write = item.to_write();
        traffic_repo::validate(&mut write)?;
        out.push(json!({
            "name": write.name,
            "direction": write.direction,
            "mode": write.mode,
            "cycle_start": write.cycle_start_nanos.map(util_time::unix_nanos_to_rfc3339),
            "cycle_interval": write.cycle_interval,
            "cycle_unit": write.cycle_unit,
            "quota_bytes": write.quota_bytes,
            "warning_percent": write.warning_percent,
            "notification_tag": write.notification_tag,
            "enabled": write.enabled,
        }));
    }
    Ok(Value::Array(out))
}

/// `GET /api/v2/admin/servers/export`。
async fn export_servers(State(shared): State<ApiShared>, AdminUser(_admin): AdminUser) -> Response {
    let now_nanos = util_time::now_nanos();
    let result = blocking_read(&shared.db, move |conn| {
        let (rows, _) = servers::list_admin(conn, "", "", false, 0, i64::MAX, now_nanos)?;
        let mut items = Vec::with_capacity(rows.len());
        for row in &rows {
            let policies =
                policy_backup_canonical(&traffic_repo::list_for_server(conn, row.public.id)?);
            let secret_plain = secret::decrypt_secret(&row.secret_ciphertext).unwrap_or_default();
            items.push((
                row.public.name.clone(),
                row.public.tag.clone(),
                row.public.note.clone(),
                presentation::decode_public_note(&row.public.public_note),
                serde_json::from_str::<Value>(&row.public.monitoring_options_raw)
                    .unwrap_or_else(|_| json!({})),
                row.public.display_index,
                row.public.hide_for_guest,
                row.probe_target.clone(),
                row.probe_tcp_ports.clone(),
                row.probe_enable_icmp.map(|value| value != 0),
                row.probe_enable_tcp.map(|value| value != 0),
                row.probe_enable_mtr.map(|value| value != 0),
                policies,
                secret_plain,
            ));
        }
        Ok(items)
    })
    .await;
    let items = match result {
        Ok(items) => items,
        Err(err) => {
            tracing::warn!(error = %err, "server export failed");
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "导出服务器失败",
            );
        }
    };
    let servers: Vec<Value> = items
        .into_iter()
        .map(
            |(
                name,
                tag,
                note,
                public_note,
                monitoring_options,
                display_index,
                hide_for_guest,
                probe_target,
                probe_tcp_ports,
                icmp,
                tcp,
                mtr,
                policies,
                secret_plain,
            )| {
                json!({
                    "name": name,
                    "tag": tag,
                    "note": note,
                    "public_note": public_note,
                    "monitoring_options": monitoring_options,
                    "display_index": display_index,
                    "hide_for_guest": hide_for_guest,
                    "traffic_policies": policies,
                    "probe_target": probe_target,
                    "probe_tcp_ports": probe_tcp_ports,
                    "probe_enable_icmp": icmp,
                    "probe_enable_tcp": tcp,
                    "probe_enable_mtr": mtr,
                    "secret": secret_plain,
                })
            },
        )
        .collect();
    data_json(json!({
        "format": BACKUP_FORMAT_V1,
        "exported_at": util_time::unix_nanos_to_rfc3339(util_time::now_nanos()),
        "servers": servers,
    }))
}

/// 备份快照：现有服务器（含明文密钥）+ 按名索引 + 按服务器策略。
struct BackupSnapshot {
    rows: Vec<servers::AdminServerRow>,
    by_name: HashMap<String, Vec<usize>>,
    policies: HashMap<i64, Vec<traffic_repo::TrafficPolicyFullRow>>,
    used_secrets: std::collections::HashSet<String>,
}

fn load_backup_snapshot(
    conn: &rusqlite::Connection,
    now_nanos: i64,
) -> anyhow::Result<BackupSnapshot> {
    let (rows, _) = servers::list_admin(conn, "", "", false, 0, i64::MAX, now_nanos)?;
    let mut by_name: HashMap<String, Vec<usize>> = HashMap::new();
    let mut used_secrets = std::collections::HashSet::new();
    for (index, row) in rows.iter().enumerate() {
        by_name
            .entry(row.public.name.clone())
            .or_default()
            .push(index);
        let plain = secret::decrypt_secret(&row.secret_ciphertext).unwrap_or_default();
        if !plain.is_empty() {
            used_secrets.insert(plain);
        }
    }
    let mut policies: HashMap<i64, Vec<traffic_repo::TrafficPolicyFullRow>> = HashMap::new();
    for row in &rows {
        let list = traffic_repo::list_for_server(conn, row.public.id)?;
        policies.insert(row.public.id, list);
    }
    Ok(BackupSnapshot {
        rows,
        by_name,
        policies,
        used_secrets,
    })
}

/// Go `diffServerBackup`（DDNS 字段 schema v20 已裁，不参与 diff）。
fn diff_backup(
    item: &BackupItem,
    current: &servers::AdminServerRow,
    current_policies: &[traffic_repo::TrafficPolicyFullRow],
) -> Vec<String> {
    let mut changes = Vec::new();
    if item.tag != current.public.tag {
        changes.push("tag".into());
    }
    if item.note != current.public.note {
        changes.push("note".into());
    }
    // Go：两侧都走 decodePublicNote 后再比 canonicalJSON（原始库串格式不参与）。
    let public_note = json_or_empty_object(&item.public_note);
    let current_public_note = serde_json::to_string(&presentation::decode_public_note(
        &current.public.public_note,
    ))
    .unwrap_or_else(|_| "{}".into());
    if public_note != current_public_note {
        changes.push("public_note".into());
    }
    let monitoring = json_or_empty_object(&item.monitoring_options);
    if monitoring != current.public.monitoring_options_raw {
        changes.push("monitoring_options".into());
    }
    if item.display_index != current.public.display_index {
        changes.push("display_index".into());
    }
    if item.hide_for_guest != current.public.hide_for_guest {
        changes.push("hide_for_guest".into());
    }
    if item.probe_target != current.probe_target {
        changes.push("probe_target".into());
    }
    if item.probe_tcp_ports != current.probe_tcp_ports {
        changes.push("probe_tcp_ports".into());
    }
    if let Ok(incoming) = policy_incoming_canonical(&item.traffic_policies)
        && canonical(&incoming) != canonical(&policy_backup_canonical(current_policies))
    {
        changes.push("traffic_policies".into());
    }
    for (provided, current_raw, key) in [
        (
            item.probe_enable_icmp,
            current.probe_enable_icmp,
            "probe_enable_icmp",
        ),
        (
            item.probe_enable_tcp,
            current.probe_enable_tcp,
            "probe_enable_tcp",
        ),
        (
            item.probe_enable_mtr,
            current.probe_enable_mtr,
            "probe_enable_mtr",
        ),
    ] {
        let incoming = provided.unwrap_or(true);
        let existing = current_raw.map(|value| value != 0).unwrap_or(true);
        if incoming != existing {
            changes.push(key.into());
        }
    }
    changes
}

/// `POST /api/v2/admin/servers/import/preview`。
async fn preview_server_import(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    body: axum::body::Bytes,
) -> Response {
    // Go v2PreviewServerImport：请求体就是备份文档本身（不像 import 有包裹层）。
    let mut document: BackupDocument = match serde_json::from_slice::<BackupDocument>(&body) {
        Ok(value) => value,
        Err(err) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_server_backup",
                &format!("request body: {err}"),
            );
        }
    };
    if let Err(detail) = normalize_backup(&mut document) {
        return problem(StatusCode::BAD_REQUEST, "invalid_server_backup", &detail);
    }
    let names: Vec<String> = document
        .servers
        .iter()
        .map(|item| item.name.clone())
        .collect();
    let now_nanos = util_time::now_nanos();
    let result = blocking_read(&shared.db, move |conn| {
        load_backup_snapshot(conn, now_nanos)
    })
    .await;
    let snapshot = match result {
        Ok(snapshot) => snapshot,
        Err(err) => {
            tracing::warn!(error = %err, "backup snapshot failed");
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "读取服务器快照失败",
            );
        }
    };
    let mut used_secrets = snapshot.used_secrets.clone();
    let items: Vec<Value> = document
        .servers
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let mut warnings: Vec<String> = Vec::new();
            let name_index = snapshot.by_name.get(&item.name);
            let mut preview = json!({
                "index": index,
                "name": item.name,
                "changes": [],
                "warnings": warnings,
            });
            match name_index.map(Vec::as_slice) {
                None => {
                    preview["match"] = json!("create");
                    preview["suggested_action"] = json!("create");
                    preview["allowed_actions"] = json!(["create", "skip"]);
                    if let Some(secret_plain) = item.secret.clone() {
                        if used_secrets.contains(&secret_plain) {
                            warnings.push("secret_conflict".into());
                        } else {
                            used_secrets.insert(secret_plain);
                        }
                    }
                }
                Some([only]) => {
                    let current = &snapshot.rows[*only];
                    preview["current_id"] = json!(current.public.id);
                    let changes = diff_backup(
                        item,
                        current,
                        snapshot
                            .policies
                            .get(&current.public.id)
                            .map_or(&[], Vec::as_slice),
                    );
                    if changes.is_empty() {
                        preview["match"] = json!("unchanged");
                        preview["suggested_action"] = json!("skip");
                        preview["allowed_actions"] = json!(["skip"]);
                    } else {
                        preview["match"] = json!("update");
                        preview["suggested_action"] = json!("overwrite");
                        preview["allowed_actions"] = json!(["overwrite", "skip"]);
                    }
                    preview["changes"] = json!(changes);
                }
                Some(_) => {
                    preview["match"] = json!("ambiguous");
                    preview["suggested_action"] = json!("skip");
                    preview["allowed_actions"] = json!(["skip"]);
                }
            }
            preview["warnings"] = json!(warnings);
            preview
        })
        .collect();
    let _ = names;
    data_json(json!({ "items": items }))
}

#[derive(Deserialize)]
struct ImportRequest {
    document: BackupDocument,
    #[serde(default)]
    actions: Vec<ImportAction>,
}

#[derive(Clone, Deserialize)]
struct ImportAction {
    index: i64,
    action: String,
}

/// `POST /api/v2/admin/servers/import`。
async fn import_servers(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    body: axum::body::Bytes,
) -> Response {
    let mut request: ImportRequest = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_server_backup",
                "请求体不是合法 JSON",
            );
        }
    };
    if let Err(detail) = normalize_backup(&mut request.document) {
        return problem(StatusCode::BAD_REQUEST, "invalid_server_backup", &detail);
    }
    let now_nanos = util_time::now_nanos();

    let snapshot = match blocking_read(&shared.db, {
        move |conn| load_backup_snapshot(conn, now_nanos)
    })
    .await
    {
        Ok(snapshot) => snapshot,
        Err(err) => {
            tracing::warn!(error = %err, "backup snapshot failed");
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "读取服务器快照失败",
            );
        }
    };
    // 预演 + 动作覆盖校验（Go planServerImport）。
    enum Planned {
        Create(usize),
        Overwrite(usize),
        Skip,
    }
    let mut planned: Vec<Planned> = Vec::new();
    let mut used_actions = vec![false; request.document.servers.len()];
    let (mut created_n, mut overwritten_n, mut skipped_n) = (0i64, 0i64, 0i64);
    for action in &request.actions {
        if action.index < 0
            || action.index as usize >= used_actions.len()
            || used_actions[action.index as usize]
        {
            return problem(
                StatusCode::BAD_REQUEST,
                "server_import_conflict",
                "import action index is invalid",
            );
        }
        used_actions[action.index as usize] = true;
        let item = &request.document.servers[action.index as usize];
        let allowed: &[&str] = match snapshot.by_name.get(&item.name).map(Vec::as_slice) {
            None => &["create", "skip"],
            Some([only]) => {
                let changes = diff_backup(
                    item,
                    &snapshot.rows[*only],
                    snapshot
                        .policies
                        .get(&snapshot.rows[*only].public.id)
                        .map_or(&[], Vec::as_slice),
                );
                if changes.is_empty() {
                    &["skip"]
                } else {
                    &["overwrite", "skip"]
                }
            }
            Some(_) => &["skip"],
        };
        if !allowed.contains(&action.action.as_str()) {
            return problem(
                StatusCode::BAD_REQUEST,
                "server_import_conflict",
                &format!("import action is not allowed for {}", item.name),
            );
        }
        match action.action.as_str() {
            "skip" => {
                skipped_n += 1;
                planned.push(Planned::Skip);
            }
            "create" => {
                created_n += 1;
                planned.push(Planned::Create(action.index as usize));
            }
            "overwrite" => {
                overwritten_n += 1;
                planned.push(Planned::Overwrite(action.index as usize));
            }
            _ => {
                return problem(
                    StatusCode::BAD_REQUEST,
                    "server_import_conflict",
                    "unknown import action",
                );
            }
        }
    }
    if used_actions.iter().any(|seen| !seen) {
        return problem(
            StatusCode::BAD_REQUEST,
            "server_import_conflict",
            "import actions must cover every backup row",
        );
    }

    // 逐条落库：策略归一化失败 / 唯一约束冲突 → 400（Go server_import_failed）。
    let mut used_secrets = snapshot.used_secrets.clone();
    let document = request.document.clone();
    let applied = blocking_write(&shared.db, move |tx| {
        let mut created: Vec<(i64, String, bool)> = Vec::new();
        let mut overwritten: Vec<i64> = Vec::new();
        for entry in planned {
            let (index, overwrite) = match entry {
                Planned::Skip => continue,
                Planned::Create(index) => (index, false),
                Planned::Overwrite(index) => (index, true),
            };
            let item = &document.servers[index];
            // 策略全部按新建处理（Go：导入策略 ID 归零）。
            let mut policies: Vec<(i64, PolicyWrite)> = Vec::new();
            for upsert in &item.traffic_policies {
                let mut write = upsert.to_write();
                traffic_repo::validate(&mut write).map_err(anyhow::Error::msg)?;
                policies.push((0, write));
            }
            if overwrite {
                let current_index = snapshot.by_name[&item.name][0];
                let current = &snapshot.rows[current_index];
                let write = backup_as_write(item);
                let fields = write_fields(&write, Some(current), false)
                    .map_err(|_| anyhow::anyhow!("invalid server backup fields"))?;
                anyhow::ensure!(
                    servers::update_admin_server(tx, current.public.id, &fields)? > 0,
                    "server missing"
                );
                traffic_repo::replace_policies(tx, current.public.id, policies)?;
                overwritten.push(current.public.id);
                continue;
            }
            let write = backup_as_write(item);
            let fields = write_fields(&write, None, true)
                .map_err(|_| anyhow::anyhow!("invalid server backup fields"))?;
            let backup_secret = item.secret.clone().unwrap_or_default();
            let had_secret = !backup_secret.is_empty();
            let reused = had_secret && !used_secrets.contains(&backup_secret);
            let secret_plain = if had_secret && reused {
                backup_secret.clone()
            } else {
                servers::new_client_secret()
            };
            let ciphertext = secret::encrypt_secret(&secret_plain)
                .map_err(|err| anyhow::anyhow!("encrypt secret: {err}"))?;
            let id = servers::insert_admin_server(tx, &ciphertext, &fields)?;
            traffic_repo::replace_policies(tx, id, policies)?;
            used_secrets.insert(secret_plain.clone());
            created.push((id, secret_plain, had_secret && reused));
        }
        Ok((created, overwritten))
    })
    .await;
    let (created_rows, overwritten_ids) = match applied {
        Ok(value) => value,
        Err(err) => {
            if traffic_write_error(&err) {
                return policy_write_error(format!("{err:#}"));
            }
            tracing::warn!(error = %err, "server import failed");
            return problem(
                StatusCode::BAD_REQUEST,
                "server_import_failed",
                "导入服务器失败",
            );
        }
    };
    // 建号热更秘密索引；覆盖刷新观测指派（Go rememberImportedServers）。
    for (id, plain, _) in &created_rows {
        shared.secrets.rebind(*id, plain);
    }
    for id in &overwritten_ids {
        let id = *id;
        let refreshed = blocking_write(&shared.db, move |tx| {
            bindings::refresh_observer_assignments_for_server(tx, id, util_time::now_nanos())
        })
        .await;
        if let Err(err) = refreshed {
            tracing::warn!(error = %err, "assignment refresh failed");
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "assignment_refresh_failed",
                "刷新观测指派失败",
            );
        }
    }
    let secrets_reused = created_rows.iter().filter(|(.., reused)| *reused).count() as i64;
    let secrets_regenerated = created_rows.iter().filter(|(.., reused)| !*reused).count() as i64;
    data_json(json!({
        "created": created_n,
        "overwritten": overwritten_n,
        "skipped": skipped_n,
        "secrets_reused": secrets_reused,
        "secrets_regenerated": secrets_regenerated,
    }))
}

/// 备份行 → 写载荷（traffic_policies 由导入循环单独替换）。
fn backup_as_write(item: &BackupItem) -> ServerWrite {
    ServerWrite {
        name: item.name.clone(),
        tag: item.tag.clone(),
        note: item.note.clone(),
        public_note: item.public_note.clone(),
        monitoring_options: item.monitoring_options.clone(),
        display_index: item.display_index,
        hide_for_guest: item.hide_for_guest,
        probe_target: item.probe_target.clone(),
        probe_tcp_ports: item.probe_tcp_ports.clone(),
        probe_enable_icmp: item.probe_enable_icmp,
        probe_enable_tcp: item.probe_enable_tcp,
        probe_enable_mtr: item.probe_enable_mtr,
        traffic_policies: None,
    }
}
