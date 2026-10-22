//! 从端管理（对齐 Go `v2Collectors` … `v2UpdateCollectorScope`）。
//!
//! 路由必须比 `/telemetry/{dataset}` 更具体，否则 `collectors` 会被当成数据集。

use std::collections::HashSet;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse as _, Response};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::config::Config;
use crate::repo::collectors::{self, CollectorRow, ScopeWrite};
use crate::rpc::support::{blocking_read, blocking_write};
use crate::secret;
use crate::telemetry::credential;
use crate::telemetry::probes;
use crate::util::time as util_time;

use super::auth::{AdminUser, ApiShared, data_json, list_json, problem};

const COLLECTOR_TIMEOUT_NANOS: i64 = 90_000_000_000;

#[derive(Debug, Deserialize)]
struct ScopeBody {
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    value: String,
}

#[derive(Debug, Deserialize)]
struct CollectorBody {
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    address: String,
    #[serde(default)]
    listen_port: i64,
    #[serde(default)]
    tls: bool,
    #[serde(default)]
    insecure_tls: bool,
    #[serde(default)]
    location: String,
    #[serde(default)]
    probe_interval_seconds: i64,
    #[serde(default)]
    mtr_interval_seconds: i64,
    #[serde(default)]
    mtr_probes: i64,
    #[serde(default)]
    tcp_ports: String,
    enable_icmp: Option<bool>,
    enable_tcp: Option<bool>,
    enable_mtr: Option<bool>,
    enable_ipv4: Option<bool>,
    enable_ipv6: Option<bool>,
    #[serde(default)]
    notify: bool,
    #[serde(default)]
    notification_tag: String,
    #[serde(default)]
    latency_notify: bool,
    #[serde(default)]
    min_latency_ms: f64,
    #[serde(default)]
    max_latency_ms: f64,
    #[serde(default)]
    fail_threshold: i64,
    #[serde(default)]
    route_interval_seconds: i64,
    #[serde(default)]
    route_keep: i64,
    #[serde(default)]
    scopes: Vec<ScopeBody>,
}

#[derive(Debug, Deserialize)]
struct ScopeReplaceBody {
    #[serde(default)]
    scopes: Vec<ScopeBody>,
}

#[derive(Debug, Deserialize, Default)]
struct InstallBody {
    #[serde(default)]
    primary_endpoint: String,
    #[serde(default)]
    primary_tls: bool,
    #[serde(default)]
    primary_insecure_tls: bool,
    #[serde(default)]
    grpc_port: i64,
}

pub fn routes() -> axum::Router<ApiShared> {
    use axum::routing::{get, post, put};
    axum::Router::new()
        .route(
            "/api/v2/admin/telemetry/collectors",
            get(list_collectors).post(create_collector),
        )
        .route(
            "/api/v2/admin/telemetry/collectors/{collectorId}",
            get(get_collector)
                .patch(update_collector)
                .delete(delete_collector),
        )
        .route(
            "/api/v2/admin/telemetry/collectors/{collectorId}/rotate-token",
            post(rotate_token),
        )
        .route(
            "/api/v2/admin/telemetry/collectors/{collectorId}/token",
            get(get_token),
        )
        .route(
            "/api/v2/admin/telemetry/collectors/{collectorId}/revoke",
            post(revoke_collector),
        )
        .route(
            "/api/v2/admin/telemetry/collectors/{collectorId}/scope",
            put(replace_scope),
        )
        .route(
            "/api/v2/admin/telemetry/collectors/{collectorId}/install-preview",
            post(install_preview),
        )
}

async fn list_collectors(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
) -> Response {
    let now = util_time::now_nanos();
    match blocking_read(&shared.db, move |conn| {
        let rows = collectors::list_admin(conn)?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(collector_dto(conn, &row, now)?);
        }
        Ok(items)
    })
    .await
    {
        Ok(items) => {
            let total = items.len() as i64;
            list_json(Value::Array(items), total)
        }
        Err(err) => database_error(err),
    }
}

async fn get_collector(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(id): Path<String>,
) -> Response {
    let now = util_time::now_nanos();
    match blocking_read(&shared.db, move |conn| {
        let Some(row) = collectors::find_admin(conn, &id)? else {
            return Ok(None);
        };
        Ok(Some(collector_dto(conn, &row, now)?))
    })
    .await
    {
        Ok(Some(dto)) => data_json(dto),
        Ok(None) => not_found(),
        Err(err) => database_error(err),
    }
}

async fn create_collector(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    body: Bytes,
) -> Response {
    let request: CollectorBody = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return invalid("invalid_collector", "请求体不是合法的从端配置"),
    };
    let kind = probes::normalize_collector_kind(&request.kind);
    if kind.is_empty() {
        return invalid("invalid_collector", "kind must be observer or probe");
    }
    if kind == probes::COLLECTOR_KIND_OBSERVER && request.address.trim().is_empty() {
        return invalid(
            "invalid_collector",
            "address is required for observer collectors",
        );
    }
    let listen_port = match normalize_listen_port(request.listen_port, &request.address) {
        Ok(port) => port,
        Err(detail) => return invalid("invalid_collector", &detail),
    };
    let scopes = match normalize_scopes(&request.scopes) {
        Ok(scopes) => scopes,
        Err(detail) => return invalid("invalid_collector", &detail),
    };
    let (plain, hash) = match credential::new_registration_token() {
        Ok(token) => token,
        Err(err) => return database_error(err),
    };
    let ciphertext = match secret::encrypt_secret(&plain) {
        Ok(bytes) => bytes,
        Err(err) => return database_error(anyhow::anyhow!(err)),
    };
    let now = util_time::now_nanos();
    let saved = blocking_write(&shared.db, move |tx| {
        let version = collectors::next_config_version(tx)?;
        let mut row = blank_row();
        row.collector_uuid = random_collector_id();
        row.name = request.name.trim().to_string();
        row.address = request.address.trim().to_string();
        row.listen_port = listen_port;
        row.token_hash = hash.to_vec();
        row.generation = 1;
        row.config_version = version;
        row.tls = request.tls;
        row.insecure_tls = request.insecure_tls;
        row.location = Some(request.location.trim().to_string()).filter(|value| !value.is_empty());
        row.kind = kind;
        apply_probe_fields(&mut row, &request, true)?;
        probes::apply_probe_defaults(&mut row);
        if row.name.is_empty() {
            anyhow::bail!(CollectorReject::NameRequired);
        }
        let draft = collectors::NewCollector {
            row: row.clone(),
            token_plain: plain.clone(),
            token_ciphertext: ciphertext,
            scopes,
        };
        collectors::insert_collector(tx, &draft, now)?;
        Ok((row.collector_uuid, plain))
    })
    .await;
    match saved {
        Ok((id, token)) => {
            let now = util_time::now_nanos();
            match blocking_read(&shared.db, move |conn| {
                let row = collectors::find_admin(conn, &id)?.expect("inserted collector");
                collector_dto(conn, &row, now)
            })
            .await
            {
                Ok(dto) => {
                    let mut response = data_json(json!({
                        "collector": dto,
                        "registration_token": token,
                    }));
                    *response.status_mut() = StatusCode::CREATED;
                    response
                }
                Err(err) => database_error(err),
            }
        }
        Err(err) => map_write_error(err),
    }
}

async fn update_collector(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let request: CollectorBody = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return invalid("invalid_collector", "请求体不是合法的从端配置"),
    };
    let listen_port = match normalize_listen_port(request.listen_port, &request.address) {
        Ok(port) => port,
        Err(detail) => return invalid("invalid_collector", &detail),
    };
    let scopes = match normalize_scopes(&request.scopes) {
        Ok(scopes) => scopes,
        Err(detail) => return invalid("invalid_collector", &detail),
    };
    let now = util_time::now_nanos();
    let id_for_read = id.clone();
    let saved = blocking_write(&shared.db, move |tx| {
        let Some(mut row) = collectors::find_admin(tx, &id)? else {
            anyhow::bail!(CollectorReject::NotFound);
        };
        if !row.is_probe() && request.address.trim().is_empty() {
            anyhow::bail!(CollectorReject::AddressRequired);
        }
        row.name = request.name.trim().to_string();
        row.address = request.address.trim().to_string();
        row.listen_port = listen_port;
        row.tls = request.tls;
        row.insecure_tls = request.insecure_tls;
        row.location = Some(request.location.trim().to_string()).filter(|value| !value.is_empty());
        apply_probe_fields(&mut row, &request, false)?;
        probes::apply_probe_defaults(&mut row);
        row.config_version += 1;
        if row.name.is_empty() {
            anyhow::bail!(CollectorReject::NameRequired);
        }
        collectors::save_collector(tx, &row, &scopes, now)?;
        Ok(())
    })
    .await;
    match saved {
        Ok(()) => load_dto(&shared, &id_for_read).await,
        Err(err) => map_write_error(err),
    }
}

async fn load_dto(shared: &ApiShared, id: &str) -> Response {
    let id = id.to_string();
    let now = util_time::now_nanos();
    match blocking_read(&shared.db, move |conn| {
        let Some(row) = collectors::find_admin(conn, &id)? else {
            return Ok(None);
        };
        Ok(Some(collector_dto(conn, &row, now)?))
    })
    .await
    {
        Ok(Some(dto)) => data_json(dto),
        Ok(None) => not_found(),
        Err(err) => database_error(err),
    }
}

async fn delete_collector(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(id): Path<String>,
) -> Response {
    disable(&shared, &id, true).await
}

async fn revoke_collector(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(id): Path<String>,
) -> Response {
    let response = disable(&shared, &id, false).await;
    if response.status() == StatusCode::NO_CONTENT {
        load_dto(&shared, &id).await
    } else {
        response
    }
}

async fn disable(shared: &ApiShared, id: &str, deleted: bool) -> Response {
    let id = id.to_string();
    let now = util_time::now_nanos();
    match blocking_write(&shared.db, move |tx| {
        collectors::disable_collector(tx, &id, deleted, now)
    })
    .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => map_write_error(err),
    }
}

async fn rotate_token(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(id): Path<String>,
) -> Response {
    let (plain, hash) = match credential::new_registration_token() {
        Ok(token) => token,
        Err(err) => return database_error(err),
    };
    let ciphertext = match secret::encrypt_secret(&plain) {
        Ok(bytes) => bytes,
        Err(err) => return database_error(anyhow::anyhow!(err)),
    };
    let collector_id = id.clone();
    match blocking_write(&shared.db, move |tx| {
        collectors::rotate_registration_token(tx, &id, &plain, &hash, &ciphertext)?;
        Ok(plain)
    })
    .await
    {
        Ok(token) => data_json(json!({
            "collector_id": collector_id,
            "registration_token": token,
            "revoked": false,
        })),
        Err(err) => map_write_error(err),
    }
}

async fn get_token(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(id): Path<String>,
) -> Response {
    match blocking_read(&shared.db, move |conn| {
        let Some(row) = collectors::find_admin(conn, &id)? else {
            return Ok(None);
        };
        let token = collectors::registration_token_plain(conn, &id)?.unwrap_or_default();
        Ok(Some(json!({
            "collector_id": row.collector_uuid,
            "registration_token": token,
            "revoked": row.revoked,
        })))
    })
    .await
    {
        Ok(Some(dto)) => data_json(dto),
        Ok(None) => not_found(),
        Err(err) => database_error(err),
    }
}

async fn replace_scope(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let request: ScopeReplaceBody = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return invalid("invalid_scope", "请求体不是合法的观测范围"),
    };
    let scopes = match normalize_scopes(&request.scopes) {
        Ok(scopes) => scopes,
        Err(detail) => return invalid("invalid_scope", &detail),
    };
    let now = util_time::now_nanos();
    let id_for_read = id.clone();
    let saved = blocking_write(&shared.db, move |tx| {
        let Some(mut row) = collectors::find_admin(tx, &id)? else {
            anyhow::bail!(CollectorReject::NotFound);
        };
        row.config_version += 1;
        collectors::save_collector(tx, &row, &scopes, now)
    })
    .await;
    match saved {
        Ok(()) => load_dto(&shared, &id_for_read).await,
        Err(err) => map_write_error(err),
    }
}

async fn install_preview(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request: InstallBody = if body.is_empty() {
        InstallBody::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(request) => request,
            Err(_) => return invalid("invalid_install_preview", "请求体不是合法的安装参数"),
        }
    };
    let conf = shared.conf();
    let collector_id = id.clone();
    match blocking_read(&shared.db, move |conn| {
        let Some(row) = collectors::find_admin(conn, &id)? else {
            return Ok(None);
        };
        Ok(Some((row.listen_port, row.address)))
    })
    .await
    {
        Ok(None) => not_found(),
        Ok(Some((listen_port, address))) => {
            let token = match blocking_read(&shared.db, move |conn| {
                collectors::registration_token_plain(conn, &collector_id)
            })
            .await
            {
                Ok(Some(token)) if !token.trim().is_empty() => token,
                Ok(_) => {
                    return invalid("invalid_collector", "从端注册 Token 不可用，请先轮换 Token");
                }
                Err(err) => return database_error(err),
            };
            let endpoint = if request.primary_endpoint.trim().is_empty() {
                let host = if conf.grpc_host.trim().is_empty() {
                    "127.0.0.1".to_string()
                } else {
                    conf.grpc_host.clone()
                };
                format!("{host}:{}", public_grpc_port(&conf))
            } else {
                request.primary_endpoint.trim().to_string()
            };
            let grpc_port = match resolve_install_port(listen_port, &address, request.grpc_port) {
                Ok(port) => port,
                Err(detail) => return invalid("invalid_install_preview", &detail),
            };
            let script = conf.install_script.collector.clone();
            let command = build_install_command(
                &script,
                &endpoint,
                &token,
                grpc_port,
                request.primary_tls,
                request.primary_insecure_tls,
            );
            let _ = headers;
            data_json(json!({
                "command": command,
                "primary_endpoint": endpoint,
                "grpc_port": grpc_port,
                "primary_tls": request.primary_tls,
                "primary_insecure_tls": request.primary_insecure_tls,
                "default_primary_tls": conf.grpc_tls.enabled,
            }))
        }
        Err(err) => database_error(err),
    }
}

fn public_grpc_port(conf: &Config) -> u16 {
    if conf.proxy_grpc_port != 0 {
        conf.proxy_grpc_port
    } else if conf.grpc_port != 0 {
        conf.grpc_port
    } else {
        5555
    }
}

fn build_install_command(
    script: &str,
    endpoint: &str,
    token: &str,
    grpc_port: i64,
    primary_tls: bool,
    primary_insecure_tls: bool,
) -> String {
    format!(
        "curl -fsSL {} | bash -s -- --primary-endpoint {} --token {} --grpc-port {grpc_port} --primary-tls {primary_tls} --primary-insecure-tls {primary_insecure_tls}",
        shell_quote(script),
        shell_quote(endpoint),
        shell_quote(token),
    )
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn collector_dto(
    conn: &rusqlite::Connection,
    row: &CollectorRow,
    now_nanos: i64,
) -> anyhow::Result<Value> {
    let scopes = collectors::list_scopes(conn, &row.collector_uuid)?;
    let runtime = collectors::runtime_view(conn, &row.collector_uuid)?;
    let scope_items: Vec<Value> = scopes
        .into_iter()
        .map(|(scope_type, value)| json!({ "type": scope_type, "value": value }))
        .collect();
    let mut dto = json!({
        "id": row.collector_uuid,
        "name": row.name,
        "address": row.address,
        "listen_port": row.listen_port,
        "tls": row.tls,
        "insecure_tls": row.insecure_tls,
        "location": row.location.clone().unwrap_or_default(),
        "kind": probes::normalize_collector_kind(&row.kind),
        "scopes": scope_items,
        "revoked": row.revoked,
        "status": collector_status(runtime.last_seen, now_nanos),
        "generation": row.generation,
        "config_version": row.config_version,
    });
    dto["probe_interval_seconds"] = json!(row.probe_interval_sec);
    dto["mtr_interval_seconds"] = json!(row.mtr_interval_sec);
    dto["mtr_probes"] = json!(row.mtr_probes);
    dto["tcp_ports"] = json!(row.tcp_ports);
    dto["enable_icmp"] = json!(row.enable_icmp);
    dto["enable_tcp"] = json!(row.enable_tcp);
    dto["enable_mtr"] = json!(row.enable_mtr);
    dto["enable_ipv4"] = json!(row.enable_ipv4);
    dto["enable_ipv6"] = json!(row.enable_ipv6);
    dto["notify"] = json!(row.probe_notify);
    dto["notification_tag"] = json!(row.notification_tag.clone().unwrap_or_default());
    dto["latency_notify"] = json!(row.latency_notify);
    dto["min_latency_ms"] = json!(row.min_latency_ms);
    dto["max_latency_ms"] = json!(row.max_latency_ms);
    dto["fail_threshold"] = json!(row.fail_threshold);
    dto["route_interval_seconds"] = json!(row.route_interval_sec);
    dto["route_keep"] = json!(row.route_keep);
    dto["last_seen"] = nanos_or_null(runtime.last_seen);
    dto["last_sync"] = nanos_or_null(runtime.last_sync);
    dto["last_primary_seen"] = nanos_or_null(runtime.last_primary_seen);
    dto["spool_size"] = json!(runtime.spool_size);
    dto["pending_records"] = json!(runtime.pending_records);
    dto["oldest_pending"] = nanos_or_null(runtime.oldest_pending);
    dto["replication_cursor"] = json!(runtime.replication_cursor);
    dto["connected_agents"] = json!(runtime.connected_agents);
    dto["protocol_version"] = json!(runtime.protocol_version);
    dto["software_version"] = json!(runtime.software_version);
    dto["heartbeat_rtt_ms"] =
        optional_float(runtime.heartbeat_rtt_sampled_at, runtime.heartbeat_rtt_ms);
    dto["heartbeat_rtt_sampled_at"] = nanos_or_null(runtime.heartbeat_rtt_sampled_at);
    dto["replication_rtt_ms"] = optional_float(
        runtime.replication_rtt_sampled_at,
        runtime.replication_rtt_ms,
    );
    dto["replication_rtt_sampled_at"] = nanos_or_null(runtime.replication_rtt_sampled_at);
    if row.is_probe() {
        for key in [
            "connected_agents",
            "pending_records",
            "spool_size",
            "oldest_pending",
            "replication_cursor",
            "replication_rtt_ms",
            "replication_rtt_sampled_at",
        ] {
            dto[key] = Value::Null;
        }
    }
    Ok(dto)
}

fn collector_status(last_seen: i64, now_nanos: i64) -> &'static str {
    if last_seen <= 0 {
        "unknown"
    } else if now_nanos.saturating_sub(last_seen) <= COLLECTOR_TIMEOUT_NANOS {
        "online"
    } else {
        "offline"
    }
}

fn nanos_or_null(nanos: i64) -> Value {
    if nanos <= 0 {
        Value::Null
    } else {
        Value::String(util_time::unix_nanos_to_rfc3339(nanos))
    }
}

fn optional_float(sampled_at: i64, value: f64) -> Value {
    if sampled_at <= 0 {
        Value::Null
    } else {
        json!(value)
    }
}

fn blank_row() -> CollectorRow {
    CollectorRow {
        collector_uuid: String::new(),
        name: String::new(),
        address: String::new(),
        token_hash: Vec::new(),
        generation: 1,
        config_version: 1,
        tls: false,
        insecure_tls: false,
        kind: probes::COLLECTOR_KIND_OBSERVER.into(),
        listen_port: 0,
        location: None,
        probe_interval_sec: 0,
        mtr_interval_sec: 0,
        mtr_probes: 0,
        tcp_ports: String::new(),
        enable_icmp: false,
        enable_tcp: false,
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

fn apply_probe_fields(
    row: &mut CollectorRow,
    request: &CollectorBody,
    created: bool,
) -> anyhow::Result<()> {
    if request.probe_interval_seconds > 0 {
        row.probe_interval_sec = request.probe_interval_seconds;
    }
    if request.mtr_interval_seconds > 0 {
        row.mtr_interval_sec = request.mtr_interval_seconds;
    }
    if request.mtr_probes > 0 {
        row.mtr_probes = request.mtr_probes;
    } else if created {
        row.mtr_probes = 10;
    }
    if !request.tcp_ports.trim().is_empty() {
        row.tcp_ports = request.tcp_ports.trim().to_string();
    }
    row.enable_icmp = request
        .enable_icmp
        .unwrap_or(if created { true } else { row.enable_icmp });
    row.enable_tcp = request
        .enable_tcp
        .unwrap_or(if created { true } else { row.enable_tcp });
    row.enable_mtr = request
        .enable_mtr
        .unwrap_or(if created { true } else { row.enable_mtr });
    row.enable_ipv4 = request
        .enable_ipv4
        .unwrap_or(if created { true } else { row.enable_ipv4 });
    row.enable_ipv6 = request
        .enable_ipv6
        .unwrap_or(if created { true } else { row.enable_ipv6 });
    if !row.enable_ipv4 && !row.enable_ipv6 {
        anyhow::bail!(CollectorReject::IpFamilyRequired);
    }
    row.probe_notify = request.notify;
    row.notification_tag = Some(request.notification_tag.trim().to_string());
    row.latency_notify = request.latency_notify;
    row.min_latency_ms = request.min_latency_ms;
    row.max_latency_ms = request.max_latency_ms;
    if request.fail_threshold > 0 {
        row.fail_threshold = request.fail_threshold;
    }
    if request.route_interval_seconds > 0 {
        row.route_interval_sec = request.route_interval_seconds;
    } else if created {
        row.route_interval_sec = 86_400;
    }
    if request.route_keep > 0 {
        row.route_keep = request.route_keep;
    } else if created {
        row.route_keep = 10;
    }
    Ok(())
}

fn normalize_scopes(scopes: &[ScopeBody]) -> Result<Vec<ScopeWrite>, String> {
    if scopes.is_empty() {
        return Err("at least one collector scope is required".into());
    }
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(scopes.len());
    for scope in scopes {
        let scope_type = scope.r#type.trim().to_ascii_lowercase();
        let mut value = scope.value.trim().to_string();
        match scope_type.as_str() {
            "all" => {
                if scopes.len() != 1 {
                    return Err("all scope cannot be combined with another scope".into());
                }
                value.clear();
            }
            "server" => {
                let Ok(id) = value.parse::<u64>() else {
                    return Err("server scope requires a valid server ID".into());
                };
                if id == 0 {
                    return Err("server scope requires a valid server ID".into());
                }
            }
            "group" | "tag" => {
                if value.is_empty() {
                    return Err(format!("{scope_type} scope requires a value"));
                }
            }
            _ => return Err("scope type must be all, server, group, or tag".into()),
        }
        let key = format!("{scope_type}\0{value}");
        if !seen.insert(key) {
            return Err("collector scopes must not contain duplicates".into());
        }
        out.push(ScopeWrite {
            scope_type,
            scope_value: value,
        });
    }
    Ok(out)
}

fn normalize_listen_port(listen_port: i64, address: &str) -> Result<i64, String> {
    if !(0..=65535).contains(&listen_port) {
        return Err("listen_port must be between 0 and 65535".into());
    }
    if listen_port != 0 {
        return Ok(listen_port);
    }
    if address.trim().is_empty() {
        return Ok(0);
    }
    parse_address_port(address).map(|port| port as i64)
}

fn parse_address_port(address: &str) -> Result<u16, String> {
    let address = address.trim();
    if address.is_empty() || !address.contains(':') {
        return Ok(5556);
    }
    let port = if let Some(rest) = address.strip_prefix('[') {
        rest.rsplit_once("]:")
            .map(|(_, port)| port)
            .ok_or_else(|| "invalid collector address".to_string())?
    } else {
        address
            .rsplit_once(':')
            .map(|(_, port)| port)
            .ok_or_else(|| "invalid collector address".to_string())?
    };
    port.parse::<u16>()
        .ok()
        .filter(|value| *value >= 1)
        .ok_or_else(|| "collector address port must be between 1 and 65535".into())
}

fn resolve_install_port(listen_port: i64, address: &str, requested: i64) -> Result<i64, String> {
    if requested != 0 {
        if !(1..=65535).contains(&requested) {
            return Err("grpc_port must be between 1 and 65535".into());
        }
        return Ok(requested);
    }
    if listen_port != 0 {
        if !(1..=65535).contains(&listen_port) {
            return Err("listen_port must be between 1 and 65535".into());
        }
        return Ok(listen_port);
    }
    let port = parse_address_port(address)?;
    Ok(i64::from(port))
}

fn random_collector_id() -> String {
    let mut bytes = [0u8; 16];
    rand::Rng::fill_bytes(&mut rand::rng(), &mut bytes);
    format!("collector-{}", hex::encode(bytes))
}

fn not_found() -> Response {
    problem(StatusCode::NOT_FOUND, "collector_not_found", "从端不存在")
}

fn invalid(code: &str, detail: &str) -> Response {
    problem(StatusCode::BAD_REQUEST, code, detail)
}

fn database_error(err: impl std::fmt::Display) -> Response {
    tracing::warn!(error = %err, "collector admin query failed");
    problem(
        StatusCode::INTERNAL_SERVER_ERROR,
        "database_error",
        "从端操作失败",
    )
}

/// 业务拒绝码：随 anyhow 传递，`map_write_error` 按类型分类而不是按文案匹配
/// （文案一改，旧的 `message.contains(...)` 就会静默退化成 500）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CollectorReject {
    NotFound,
    NameRequired,
    AddressRequired,
    IpFamilyRequired,
}

impl std::fmt::Display for CollectorReject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "collector not found"),
            Self::NameRequired => write!(f, "name is required"),
            Self::AddressRequired => write!(f, "address is required for observer collectors"),
            Self::IpFamilyRequired => write!(f, "enable_ipv4 or enable_ipv6 is required"),
        }
    }
}

impl std::error::Error for CollectorReject {}

fn map_write_error(err: anyhow::Error) -> Response {
    match err.downcast_ref::<CollectorReject>() {
        Some(CollectorReject::NotFound) => not_found(),
        Some(CollectorReject::IpFamilyRequired) => invalid(
            "invalid_probe_ip_family",
            "enable_ipv4 or enable_ipv6 is required",
        ),
        Some(CollectorReject::AddressRequired) => {
            invalid("invalid_collector", "观察型从端必须填写地址")
        }
        Some(CollectorReject::NameRequired) => invalid("invalid_collector", "名称不能为空"),
        None => database_error(err),
    }
}
