//! Admin settings 与 API Token 端点（Phase 5）：对齐 Go `v2GetSettings` /
//! `v2UpdateSettings` / `v2ListAPITokens` 等五个 token handler。
//!
//! settings 写路径 = 校验（Go 语义）→ 序列化写回 yaml（0600，`Conf.Save`
//! 等价）→ 原子替换运行时句柄；任一步失败内存不变。

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::config::{self, Config};
use crate::repo::api_tokens;
use crate::rpc::support::{blocking_read, blocking_write};
use crate::secret;
use crate::util::time as util_time;

use super::auth::{AdminUser, ApiShared, data_json, list_json, problem};
use super::public::safe_asset_url;
use super::tokens::ApiTokenEntry;

/// 公开的 CSS 安全判定（公开站 bootstrap 兜底用：写路径已校验，这里防手工改配置）。
pub(crate) fn css_is_safe(css: &str) -> bool {
    !forbidden_css(css)
}

/// 自定义 CSS 白名单校验。
///
/// 原实现是黑名单（`contains("@import")` / `contains("url(")`…），CSS 的转义与
/// 注释语法让它可以被一行绕过：`@im/**/port`、`\75 rl(http://evil)`、
/// `java\73script:`、`url(\2f\2fevil.com)` 全部原样落地，而这份 CSS 是
/// `style.textContent` 直接注入公开站的——等于给任意人开远程加载 + 追踪信道。
///
/// 改为「先归一化再走白名单」：
/// 1. 去掉注释（注释可把关键字劈开：`@im/**/port`）；
/// 2. 解掉 CSS 转义（`\75` = `u`、`\2f` = `/`），混淆在检查面前现形；
/// 3. 只放行白名单内的 at-rule，且一律禁掉会发起网络请求的函数。
///
/// 这是主题字段，不是通用 CSS 沙箱：宁可拒掉正常但罕见的写法。
fn forbidden_css(css: &str) -> bool {
    let normalized = normalize_css(css);

    // HTML 结构字符：CSS 里没有合法用途（`<style>` 由 textContent 注入，
    // 但 `</style` 之类的残留仍应拒）。
    if normalized.contains(['<', '>', '&']) {
        return true;
    }

    // 会发起网络请求 / 执行脚本的构造，一律禁止。
    for needle in [
        "url(",
        "image-set(",
        "-webkit-image-set(",
        "src(",
        "-moz-binding",
        "behavior",
        "expression(",
        "javascript:",
        "vbscript:",
        "@import",
        "@charset",
        "@namespace",
        "@font-face",
        "@document",
    ] {
        if normalized.contains(needle) {
            return true;
        }
    }

    // at-rule白名单：其余一律拒（`@import` 上面已挡，这里兜未知新规则）。
    // 条目统一带 `@`：与下面拼出的 `@{name}` 比对，漏一个前缀就等于漏放一类规则。
    const ALLOWED_AT_RULES: [&str; 8] = [
        "@media",
        "@supports",
        "@keyframes",
        "@-webkit-keyframes",
        "@-moz-keyframes",
        "@-o-keyframes",
        "@page",
        "@layer",
    ];
    let mut rest = normalized.as_str();
    while let Some(index) = rest.find('@') {
        let tail = &rest[index..];
        // 从 `@` 之后开始取标识符；从 `@` 本身开始会立刻停下、拿到空串。
        let name: String = tail
            .chars()
            .skip(1)
            .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '-' || *ch == '_')
            .collect();
        if !name.is_empty() {
            // 白名单条目带 `@`，这里补回去再比。
            let rule = format!("@{name}");
            if !ALLOWED_AT_RULES
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(&rule))
            {
                return true;
            }
        }
        rest = &tail[name.len().max(1)..];
    }
    false
}

/// 去注释 + 解 CSS 转义 + 转小写。仅用于校验，不改变入库原文。
fn normalize_css(css: &str) -> String {
    let without_comments = strip_css_comments(css);
    decode_css_escapes(&without_comments).to_lowercase()
}

/// 去掉 `/* … */` 注释（CSS 注释不可嵌套，遇到 `*/` 即结束）。
fn strip_css_comments(css: &str) -> String {
    let mut out = String::with_capacity(css.len());
    let mut rest = css;
    while let Some(start) = rest.find("/*") {
        out.push_str(&rest[..start]);
        match rest[start + 2..].find("*/") {
            Some(end) => rest = &rest[start + 2 + end + 2..],
            // 未闭合注释：其后内容全部丢弃（浏览器同样如此）。
            None => return out,
        }
    }
    out.push_str(rest);
    out
}

/// 解 CSS 转义：`\XXXXXX`（1-6 位十六进制，后可跟一个空白）与 `\<任意字符>`。
fn decode_css_escapes(css: &str) -> String {
    let mut out = String::with_capacity(css.len());
    let mut chars = css.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let mut hex = String::new();
        while hex.len() < 6 && chars.peek().is_some_and(|next| next.is_ascii_hexdigit()) {
            hex.push(chars.next().unwrap());
        }
        if hex.is_empty() {
            // `\<字符>`：字面量（`\;` → `;`）。
            if let Some(next) = chars.next() {
                out.push(next);
            }
            continue;
        }
        // 十六进制转义后可跟一个空白作为终止符，该空白不属于转义值。
        if chars.peek().is_some_and(|next| next.is_whitespace()) {
            chars.next();
        }
        if let Ok(code) = u32::from_str_radix(&hex, 16)
            && let Some(decoded) = char::from_u32(code)
        {
            out.push(decoded);
        }
    }
    out
}

/// Go `safeColor`：六位十六进制。
fn safe_color(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 7 && bytes[0] == b'#' && bytes[1..].iter().all(|b| b.is_ascii_hexdigit())
}

/// Go `splitNonEmpty`：按逗号/分号/空白切分并去空。
fn split_non_empty(value: &str) -> Vec<String> {
    value
        .split([',', ';', ' ', '\t', '\n', '\r'])
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect()
}

/// Go `asBool`：bool / "on"|"true"|"1" / 非 0 数字。
fn as_bool(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(value)) => *value,
        Some(Value::String(value)) => matches!(value.as_str(), "on" | "true" | "1"),
        Some(Value::Number(value)) => value.as_f64().unwrap_or(0.0) != 0.0,
        _ => false,
    }
}

/// Go `stringValue`：非字符串原样转文本后 trim。
fn string_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(value)) => value.trim().to_string(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string().trim().trim_matches('"').to_string(),
    }
}

/// Go `uintValue`：解析失败或缺席保留现值；容忍 `123.0` 形态。
fn uint_value(value: Option<&Value>, current: u64) -> u64 {
    let Some(value) = value else {
        return current;
    };
    let text = match value {
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        _ => return current,
    };
    text.trim_end_matches(".0")
        .parse::<u64>()
        .unwrap_or(current)
}

/// Go `v2SettingsDTO`（view_password 恒空串 + configured 布尔）。
fn settings_dto(conf: &Config) -> Value {
    json!({
        "site_title": conf.site.brand,
        "language": conf.language,
        "view_password": "",
        "view_password_configured": !conf.site.view_password.is_empty(),
        "grpc_host": conf.grpc_host,
        "proxy_grpc_port": conf.proxy_grpc_port,
        "tls": conf.tls,
        "nameservers": split_non_empty(&conf.dns_servers),
        "enable_offline_history": conf.enable_offline_history,
        "offline_threshold": conf.offline_threshold_seconds,
        "check_interval": conf.offline_check_interval_seconds,
        "merge_gap": conf.offline_merge_gap_seconds,
        "retention_days": conf.offline_history_retention_days,
        "notify_offline": conf.enable_offline_notification,
        "notify_recovery": conf.enable_recovery_notification,
        "plain_ip_in_notification": conf.enable_plain_ip_in_notification,
        "show_availability_guest": conf.show_availability_to_guest,
        "connectivity_notification": conf.telemetry.enable_connectivity_notification,
        "correction_notification": conf.telemetry.enable_correction_notification,
        "collector_offline_notification": conf.telemetry.enable_collector_offline_notification,
        "collector_online_notification": conf.telemetry.enable_collector_online_notification,
        "data_loss_notification": conf.telemetry.enable_data_loss_notification,
        "primary_color": conf.site.primary_color,
        "footer_text": conf.site.footer_text,
        "logo_url": conf.site.logo_url,
        "custom_css": conf.site.safe_custom_css,
        "primary_location": conf.site.primary_location,
        "theme": config::normalize_public_theme(&conf.site.theme),
        "allow_frontend_theme_switch": !conf.disable_switch_template_in_frontend,
        "retention": serde_json::to_value(&conf.retention).unwrap_or_else(|_| json!({})),
        "web_delivery": conf.web.delivery,
    })
}

/// `GET /api/v2/admin/settings`。
async fn get_settings(State(shared): State<ApiShared>, AdminUser(_admin): AdminUser) -> Response {
    data_json(settings_dto(&shared.conf()))
}

/// `PATCH /api/v2/admin/settings`（Go `v2UpdateSettings` 字段子集）。
async fn update_settings(
    State(shared): State<ApiShared>,
    AdminUser(_admin): AdminUser,
    body: axum::body::Bytes,
) -> Response {
    let body: serde_json::Map<String, Value> = match serde_json::from_slice(&body) {
        Ok(map) => map,
        Err(_) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_settings",
                "请求体不是合法 JSON",
            );
        }
    };
    let get = |key: &str| body.get(key);
    let exists = |key: &str| body.contains_key(key);

    // 在克隆上修改；任一校验失败直接丢弃，内存与文件都不动（Go 回滚语义）。
    let mut updated = (*shared.conf()).clone();

    let site_title = string_value(get("site_title"));
    if !site_title.is_empty() {
        updated.site.brand = site_title;
    }
    let language = string_value(get("language"));
    if !language.is_empty() {
        if !config::LANGUAGES.iter().any(|(code, _)| *code == language) {
            return problem(StatusCode::BAD_REQUEST, "invalid_language", "不支持的语言");
        }
        updated.language = language;
    }
    if as_bool(get("clear_view_password")) {
        updated.site.view_password = String::new();
    } else if let Some(password) = get("view_password") {
        let password = string_value(Some(password));
        if !password.is_empty() {
            updated.site.view_password = password;
        }
    }
    if exists("grpc_host") {
        updated.grpc_host = string_value(get("grpc_host"));
    }
    if exists("proxy_grpc_port") {
        let port = uint_value(get("proxy_grpc_port"), updated.proxy_grpc_port as u64);
        if port > 65535 {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_proxy_grpc_port",
                "公网 gRPC 端口必须在 0–65535 之间，0 表示使用监听端口",
            );
        }
        updated.proxy_grpc_port = port as u16;
    }
    if exists("tls") {
        updated.tls = as_bool(get("tls"));
    }
    if let Some(raw) = get("nameservers") {
        match raw {
            Value::Array(values) => {
                let parts: Vec<String> = values
                    .iter()
                    .map(|value| string_value(Some(value)))
                    .filter(|part| !part.is_empty())
                    .collect();
                updated.dns_servers = parts.join(",");
            }
            Value::String(text) => updated.dns_servers = text.clone(),
            _ => {}
        }
    }
    if exists("enable_offline_history") {
        updated.enable_offline_history = as_bool(get("enable_offline_history"));
    }
    updated.offline_threshold_seconds =
        uint_value(get("offline_threshold"), updated.offline_threshold_seconds);
    updated.offline_check_interval_seconds = uint_value(
        get("check_interval"),
        updated.offline_check_interval_seconds,
    );
    updated.offline_merge_gap_seconds =
        uint_value(get("merge_gap"), updated.offline_merge_gap_seconds);
    updated.offline_history_retention_days = uint_value(
        get("retention_days"),
        updated.offline_history_retention_days,
    );
    if updated.offline_threshold_seconds < 10
        || updated.offline_check_interval_seconds < 5
        || updated.offline_check_interval_seconds > updated.offline_threshold_seconds
        || updated.offline_history_retention_days < 1
        || updated.offline_merge_gap_seconds > 3600
    {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_offline_settings",
            "离线阈值、检测间隔或保留策略超出允许范围",
        );
    }
    if exists("notify_offline") {
        updated.enable_offline_notification = as_bool(get("notify_offline"));
    }
    if exists("notify_recovery") {
        updated.enable_recovery_notification = as_bool(get("notify_recovery"));
    }
    if exists("plain_ip_in_notification") {
        updated.enable_plain_ip_in_notification = as_bool(get("plain_ip_in_notification"));
    }
    if exists("show_availability_guest") {
        updated.show_availability_to_guest = as_bool(get("show_availability_guest"));
    }
    if exists("connectivity_notification") {
        updated.telemetry.enable_connectivity_notification =
            as_bool(get("connectivity_notification"));
    }
    if exists("correction_notification") {
        updated.telemetry.enable_correction_notification = as_bool(get("correction_notification"));
    }
    if exists("collector_offline_notification") {
        updated.telemetry.enable_collector_offline_notification =
            as_bool(get("collector_offline_notification"));
    }
    if exists("collector_online_notification") {
        updated.telemetry.enable_collector_online_notification =
            as_bool(get("collector_online_notification"));
    }
    if exists("data_loss_notification") {
        updated.telemetry.enable_data_loss_notification = as_bool(get("data_loss_notification"));
    }
    if exists("theme") {
        updated.site.theme = config::normalize_public_theme(&string_value(get("theme")));
    }
    if exists("allow_frontend_theme_switch") {
        updated.disable_switch_template_in_frontend = !as_bool(get("allow_frontend_theme_switch"));
    }
    let primary_color = string_value(get("primary_color"));
    if !primary_color.is_empty() {
        if !safe_color(&primary_color) {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_primary_color",
                "品牌色必须使用六位十六进制颜色",
            );
        }
        updated.site.primary_color = primary_color;
    }
    if exists("footer_text") {
        updated.site.footer_text = string_value(get("footer_text"));
    }
    if exists("primary_location") {
        let location = string_value(get("primary_location"));
        if location.chars().count() > 64 {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_primary_location",
                "主控位置不得超过 64 个字符",
            );
        }
        updated.site.primary_location = location;
    }
    if exists("logo_url") {
        let candidate = string_value(get("logo_url"));
        if !candidate.is_empty() && safe_asset_url(&candidate, "").is_empty() {
            return problem(
                StatusCode::BAD_REQUEST,
                "unsafe_asset_url",
                "Logo 只允许 /static/ 或 data:image/ 地址",
            );
        }
        updated.site.logo_url = candidate;
    }
    if let Some(raw) = get("custom_css") {
        let css = match raw {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        if forbidden_css(&css) {
            return problem(
                StatusCode::BAD_REQUEST,
                "unsafe_custom_css",
                "自定义 CSS 包含被禁止的远程或可执行规则",
            );
        }
        if css.len() > 32768 {
            return problem(
                StatusCode::BAD_REQUEST,
                "custom_css_too_large",
                "自定义 CSS 不得超过 32 KiB",
            );
        }
        updated.site.safe_custom_css = css;
    }

    // 默认值补全 + 校验 + 写回文件；失败则内存不动。
    updated.apply_defaults();
    if let Err(err) = updated.validate() {
        tracing::warn!(error = %err, "settings validation failed");
        return problem(StatusCode::BAD_REQUEST, "invalid_settings", "设置校验失败");
    }
    if let Err(err) = updated.save() {
        tracing::warn!(error = %err, "settings save failed");
        return problem(
            StatusCode::INTERNAL_SERVER_ERROR,
            "settings_save_failed",
            "保存设置失败",
        );
    }
    shared_update_conf(&shared, updated);
    // 语言切换热更新通知文案（Go InitLocalizer）。
    if let Some(notifier) = &shared.notifier {
        notifier.reload_language();
    }
    data_json(settings_dto(&shared.conf()))
}

/// 通过共享句柄原子替换运行时配置（测试与生产共用路径）。
fn shared_update_conf(shared: &ApiShared, conf: Config) {
    *shared.conf.write().expect("config lock") = std::sync::Arc::new(conf);
}

/// Go `apiTokenDTO`（token 字段仅 include_plain 时输出）。
fn api_token_dto(row: &api_tokens::ApiTokenRow, include_plain: bool, now_nanos: i64) -> Value {
    let plain = secret::decrypt_secret(&row.token_ciphertext).unwrap_or_default();
    let prefix: String = plain.chars().take(8).collect();
    let permission = if row.permission.is_empty() {
        "write".to_string()
    } else {
        row.permission.clone()
    };
    let expired = row
        .expires_at_nanos
        .is_some_and(|expires| expires > 0 && expires <= now_nanos);
    let mut dto = json!({
        "id": row.id,
        "note": row.note,
        "permission": permission,
        "expires_at": row.expires_at_nanos.map(util_time::unix_nanos_to_rfc3339),
        "enabled": row.enabled,
        "expired": expired,
        "token_prefix": format!("{prefix}…"),
        "created_at": row.created_at_nanos.map(util_time::unix_nanos_to_rfc3339),
    });
    if include_plain {
        dto["token"] = json!(plain);
    }
    dto
}

/// `GET /api/v2/admin/api-tokens`。
async fn list_api_tokens(State(shared): State<ApiShared>, AdminUser(admin): AdminUser) -> Response {
    let rows = blocking_read(&shared.db, move |conn| {
        api_tokens::list_by_user(conn, admin.user.id)
    })
    .await;
    match rows {
        Ok(rows) => {
            let now_nanos = util_time::now_nanos();
            let items: Vec<Value> = rows
                .iter()
                .map(|row| api_token_dto(row, false, now_nanos))
                .collect();
            let total = items.len() as i64;
            list_json(Value::Array(items), total)
        }
        Err(err) => {
            tracing::warn!(error = %err, "api tokens query failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "查询 Token 失败",
            )
        }
    }
}

fn parse_expires_at(raw: Option<&str>) -> Result<Option<i64>, Box<Response>> {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let parsed = time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(time::OffsetDateTime::unix_timestamp_nanos)
        .and_then(|nanos| i64::try_from(nanos).ok());
    parsed.map(Some).ok_or_else(|| {
        Box::new(problem(
            StatusCode::BAD_REQUEST,
            "invalid_token_expiry",
            "到期时间必须是 RFC3339",
        ))
    })
}

/// `POST /api/v2/admin/api-tokens`（创建并一次性返回明文）。
async fn create_api_token(
    State(shared): State<ApiShared>,
    AdminUser(admin): AdminUser,
    body: axum::body::Bytes,
) -> Response {
    #[derive(Deserialize)]
    struct Request {
        #[serde(default)]
        note: String,
        #[serde(default)]
        permission: String,
        #[serde(default)]
        expires_at: Option<String>,
    }
    let request: Request = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_token",
                "请求体不是合法 JSON",
            );
        }
    };
    if request.note.trim().is_empty() || request.permission.is_empty() {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_token",
            "note 与 permission 必填",
        );
    }
    let permission = request.permission.to_lowercase();
    if permission != "read" && permission != "write" {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_token_permission",
            "permission 必须是 read 或 write",
        );
    }
    let now_nanos = util_time::now_nanos();
    let expires_at = match parse_expires_at(request.expires_at.as_deref()) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    if expires_at.is_some_and(|expires| expires <= now_nanos) {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_token_expiry",
            "到期时间必须晚于当前时间",
        );
    }
    let plain = {
        use rand::distr::{Alphanumeric, SampleString as _};
        Alphanumeric.sample_string(&mut rand::rng(), 32)
    };
    let ciphertext = match secret::encrypt_secret(&plain) {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(error = %err, "encrypt api token failed");
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "token_generation_failed",
                "生成 Token 失败",
            );
        }
    };
    let hash: [u8; 32] = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(plain.as_bytes());
        hasher.finalize().into()
    };
    let note = request.note.trim().to_string();
    let user_id = admin.user.id;
    let created = {
        let (ciphertext, note, permission) = (ciphertext.clone(), note.clone(), permission.clone());
        blocking_write(&shared.db, move |tx| {
            api_tokens::insert(
                tx,
                user_id,
                &ciphertext,
                &hash,
                &note,
                &permission,
                expires_at,
            )
        })
        .await
    };
    let id = match created {
        Ok(id) => id,
        Err(err) => {
            tracing::warn!(error = %err, "api token create failed");
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "token_create_failed",
                "创建 Token 失败",
            );
        }
    };
    shared.tokens.insert(
        plain,
        ApiTokenEntry {
            user_id,
            permission: permission.clone(),
            expires_at_nanos: expires_at.unwrap_or(0),
            enabled: true,
        },
    );
    let row = api_tokens::ApiTokenRow {
        id,
        user_id,
        token_ciphertext: ciphertext,
        note,
        permission,
        expires_at_nanos: expires_at,
        enabled: true,
        created_at_nanos: Some(now_nanos),
    };
    let mut response = data_json(api_token_dto(&row, true, now_nanos));
    *response.status_mut() = StatusCode::CREATED;
    response
}

/// `GET /api/v2/admin/api-tokens/{tokenId}`。
///
/// 明文只回给交互式管理员会话（Cookie）。API Token 是设计上可外发给看板 /
/// CI / 监控的 Bearer 凭据：若它也能读明文，一个**只读**令牌就能读出同账号下
/// 写令牌的明文值，从而把自己提升成写权限——这正是只读令牌要防的事。
/// （Go 同源缺陷，这里修掉：`api_v2_spa.go` 的 token 详情不区分调用方。）
async fn get_api_token(
    State(shared): State<ApiShared>,
    AdminUser(admin): AdminUser,
    Path(token_id): Path<i64>,
) -> Response {
    if token_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    let include_plain = !admin.is_api;
    let row = blocking_read(&shared.db, move |conn| {
        api_tokens::get_by_id(conn, admin.user.id, token_id)
    })
    .await;
    match row {
        Ok(Some(row)) => data_json(api_token_dto(&row, include_plain, util_time::now_nanos())),
        Ok(None) => token_not_found(),
        Err(err) => {
            tracing::warn!(error = %err, "api token query failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "查询 Token 失败",
            )
        }
    }
}

/// `PATCH /api/v2/admin/api-tokens/{tokenId}`（启停开关）。
async fn patch_api_token(
    State(shared): State<ApiShared>,
    AdminUser(admin): AdminUser,
    Path(token_id): Path<i64>,
    body: axum::body::Bytes,
) -> Response {
    #[derive(Deserialize)]
    struct Request {
        enabled: Option<bool>,
    }
    if token_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    let request: Request = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid_token_patch",
                "enabled 必填",
            );
        }
    };
    let Some(enabled) = request.enabled else {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid_token_patch",
            "enabled 必填",
        );
    };
    let user_id = admin.user.id;
    let plain = blocking_write(&shared.db, move |tx| {
        if !api_tokens::set_enabled(tx, user_id, token_id, enabled)? {
            return Ok(None);
        }
        let row = api_tokens::get_by_id(tx, user_id, token_id)?;
        Ok(row.map(|row| secret::decrypt_secret(&row.token_ciphertext).unwrap_or_default()))
    })
    .await;
    match plain {
        Ok(Some(plain)) => {
            shared.tokens.set_enabled(&plain, enabled);
            let row = blocking_read(&shared.db, move |conn| {
                api_tokens::get_by_id(conn, user_id, token_id)
            })
            .await;
            match row {
                Ok(Some(row)) => data_json(api_token_dto(&row, false, util_time::now_nanos())),
                _ => token_not_found(),
            }
        }
        Ok(None) => token_not_found(),
        Err(err) => {
            tracing::warn!(error = %err, "api token update failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "token_update_failed",
                "更新 Token 失败",
            )
        }
    }
}

/// `DELETE /api/v2/admin/api-tokens/{tokenId}`。
async fn delete_api_token(
    State(shared): State<ApiShared>,
    AdminUser(admin): AdminUser,
    Path(token_id): Path<i64>,
) -> Response {
    if token_id <= 0 {
        return problem(StatusCode::BAD_REQUEST, "invalid_id", "资源 ID 无效");
    }
    let user_id = admin.user.id;
    let removed = blocking_write(&shared.db, move |tx| {
        let Some(row) = api_tokens::get_by_id(tx, user_id, token_id)? else {
            return Ok(None);
        };
        api_tokens::delete(tx, user_id, token_id)?;
        Ok(Some(
            secret::decrypt_secret(&row.token_ciphertext).unwrap_or_default(),
        ))
    })
    .await;
    match removed {
        Ok(Some(plain)) => {
            shared.tokens.remove(&plain);
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(None) => token_not_found(),
        Err(err) => {
            tracing::warn!(error = %err, "api token delete failed");
            problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "token_delete_failed",
                "删除 Token 失败",
            )
        }
    }
}

fn token_not_found() -> Response {
    problem(StatusCode::NOT_FOUND, "token_not_found", "Token 不存在")
}

/// settings + api-tokens 路由组。
pub fn routes() -> axum::Router<ApiShared> {
    axum::Router::new()
        .route(
            "/api/v2/admin/settings",
            axum::routing::get(get_settings).patch(update_settings),
        )
        .route(
            "/api/v2/admin/api-tokens",
            axum::routing::get(list_api_tokens).post(create_api_token),
        )
        .route(
            "/api/v2/admin/api-tokens/{tokenId}",
            axum::routing::get(get_api_token)
                .patch(patch_api_token)
                .delete(delete_api_token),
        )
}

#[cfg(test)]
mod css_tests {
    use super::{css_is_safe, decode_css_escapes, normalize_css, strip_css_comments};

    fn safe(css: &str) -> bool {
        css_is_safe(css)
    }

    #[test]
    fn normal_theme_css_is_allowed() {
        for css in [
            ".card { border-radius: 8px; }",
            "@media (max-width: 720px) { .row { display: none } }",
            "@supports (display: grid) { .grid { display: grid } }",
            "@keyframes pulse { from { opacity: .4 } to { opacity: 1 } }",
            "@-webkit-keyframes spin { to { transform: rotate(360deg) } }",
            ".a::after { content: \"\\2014\"; }",
            "body { --brand: #2563eb; color: var(--brand) }",
            "/* 注释 */ .x { margin: 0 }",
            "@layer base { .y { padding: 0 } }",
            ".z { background: linear-gradient(180deg, #fff, #eee) }",
        ] {
            assert!(safe(css), "正常主题 CSS 不应被拒：{css}");
        }
    }

    #[test]
    fn remote_and_executable_constructs_are_rejected() {
        for css in [
            "@import url('http://evil/x.css');",
            "@import \"http://evil/x.css\";",
            ".a { background: url(http://evil/p.png) }",
            ".a { background: url('//evil/p.png') }",
            ".a { behavior: url(x.htc) }",
            ".a { -moz-binding: url(x.xml#a) }",
            ".a { width: expression(alert(1)) }",
            ".a { background: javascript:alert(1) }",
            ".a { background-image: image-set('http://evil/p.png' 1x) }",
            "@font-face { font-family: x; src: url(http://evil/f.woff2) }",
            "@namespace url(http://evil);",
        ] {
            assert!(!safe(css), "应被拒：{css}");
        }
    }

    #[test]
    fn obfuscation_does_not_slip_through() {
        for css in [
            // 注释劈开关键字。
            "@im/**/port url('http://evil/x.css');",
            "@import\\\n url('http://evil/x.css');",
            // 十六进制转义。
            ".a { background: \\75 rl(http://evil/p.png) }",
            ".a { background: url(\\2f\\2fevil/p.png) }",
            // 字符转义。
            ".a { background: \\6a\\61vascript:alert(1) }",
            // 大小写混淆。
            "@IMPORT url('http://evil/x.css');",
            ".a { BEHAVIOR: url(x.htc) }",
            // 未知 at-rule。
            "@unknown-rule foo { .a { color: red } }",
            // 结构字符。
            ".a { content: '</style><script>alert(1)</script>' }",
        ] {
            assert!(!safe(css), "混淆写法应被拒：{css}");
        }
    }

    #[test]
    fn helpers_normalize_as_expected() {
        assert_eq!(strip_css_comments("a/*x*/b/*y"), "ab");
        assert_eq!(strip_css_comments("a/*未闭合"), "a");
        assert_eq!(decode_css_escapes("\\75 rl"), "url");
        assert_eq!(decode_css_escapes("\\2f\\2f"), "//");
        assert_eq!(decode_css_escapes("\\;"), ";");
        assert_eq!(decode_css_escapes("\\41 "), "A");
        assert_eq!(normalize_css("@IM/**/PORT \\75 RL(X)"), "@import url(x)");
    }
}
