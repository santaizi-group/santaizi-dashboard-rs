//! OAuth2 登录流（对齐 Go `cmd/dashboard/controller/oauth2.go`）：
//! `GET /oauth2/login` 与 `GET /oauth2/callback` 两个页面级路由。
//!
//! 本刀范围：mock（debug 专用一键登录）+ GitHub / Gitee / GitLab / JiHuLab /
//! Gitea / Cloudflare 六个「授权码交换 + userinfo GET」provider。
//! **OIDC 不实现**：Go 用 go-oidc 校验 ID token 签名，只拉 userinfo 属安全
//! 相关偏离，故显式拒绝而不是假造一个不验签的登录。
//! 错误一律 problem+json（Go 是 HTML 错误页；本仓 HTTP 面统一 problem 契约，
//! 有意偏离）。

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use axum::routing::get;
use serde::Deserialize;
use url::form_urlencoded;

use super::auth::{ApiShared, cookie_value, problem, set_cookie};
use crate::repo::users::OauthProfile;
use crate::util::time as util_time;

/// state 缓存 TTL（Go `cache.New(5*time.Minute, ...)` 的默认过期）。
const STATE_TTL: Duration = Duration::from_secs(300);
/// 登录态 Cookie 寿命（Go `60*60*24`）。
const SESSION_COOKIE_MAX_AGE: i64 = 60 * 60 * 24;
/// 会话令牌有效期：2 个月（Go `time.Now().AddDate(0, 2, 0)`）。
const SESSION_TTL_NANOS: i64 = 60 * 24 * 3600 * 1_000_000_000;
/// 随机串长度（Go `utils.GenerateRandomString(32)`）。
const RANDOM_LEN: usize = 32;
/// state 在随机串中的切分长度（Go `randomString[:16]` / `[16:]`）。
const STATE_SPLIT: usize = 16;
/// provider 上游请求超时（Go 用无超时 context，这里设上限防止挂死登录）。
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(10);

/// `stateKey → state` 短时缓存（Go `singleton.Cache` 的 oauth2 state 用途）：
/// 登录写入、回调取出即删；惰性清理过期项，条目有上界（登录频率）。
#[derive(Debug, Default)]
pub struct Oauth2StateStore {
    entries: Mutex<HashMap<String, (String, Instant)>>,
}

impl Oauth2StateStore {
    fn put(&self, state_key: String, state: String) {
        let mut entries = self.entries.lock().expect("oauth2 state lock");
        let now = Instant::now();
        entries.retain(|_, (_, expires)| *expires > now);
        entries.insert(state_key, (state, now + STATE_TTL));
    }

    /// 取出即删（Go `Cache.Get` + `Cache.Delete` 的组合语义）；过期视为未命中。
    fn take(&self, state_key: &str) -> Option<String> {
        let mut entries = self.entries.lock().expect("oauth2 state lock");
        match entries.get(state_key) {
            Some((state, expires)) if *expires > Instant::now() => {
                let state = state.clone();
                entries.remove(state_key);
                Some(state)
            }
            _ => {
                entries.remove(state_key);
                None
            }
        }
    }
}

/// provider 类型（Go `model.ConfigType*` 常量值）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderKind {
    Mock,
    Github,
    Gitee,
    Gitlab,
    Jihulab,
    Gitea,
    Cloudflare,
    Oidc,
    Unknown,
}

fn provider_kind(conf: &crate::config::Oauth2Config) -> ProviderKind {
    match conf.type_.to_ascii_lowercase().as_str() {
        "mock" => ProviderKind::Mock,
        "github" => ProviderKind::Github,
        "gitee" => ProviderKind::Gitee,
        "gitlab" => ProviderKind::Gitlab,
        "jihulab" => ProviderKind::Jihulab,
        "gitea" => ProviderKind::Gitea,
        "cloudflare" => ProviderKind::Cloudflare,
        "oidc" => ProviderKind::Oidc,
        _ => ProviderKind::Unknown,
    }
}

/// mock 一键登录的准入：仅 debug 生效（Go 同，防生产被恶意利用）。
fn mock_login_allowed(conf: &crate::config::Config) -> bool {
    provider_kind(&conf.oauth2) == ProviderKind::Mock && conf.debug
}

/// 单个 provider 的端点与 scope（Go `getCommonOauth2Config` 的逐分支）。
struct ProviderEndpoints {
    auth_url: String,
    token_url: String,
    userinfo_url: String,
    scopes: Vec<String>,
}

fn provider_endpoints(
    conf: &crate::config::Oauth2Config,
    kind: ProviderKind,
) -> Option<ProviderEndpoints> {
    let endpoints = match kind {
        // GitHub：默认分支，scope 为空串列表（Go `else` 分支）。
        ProviderKind::Github => ProviderEndpoints {
            auth_url: "https://github.com/login/oauth/authorize".into(),
            token_url: "https://github.com/login/oauth/access_token".into(),
            userinfo_url: "https://api.github.com/user".into(),
            scopes: vec![],
        },
        ProviderKind::Gitee => ProviderEndpoints {
            auth_url: "https://gitee.com/oauth/authorize".into(),
            token_url: "https://gitee.com/oauth/token".into(),
            userinfo_url: "https://gitee.com/api/v5/user".into(),
            scopes: vec![],
        },
        ProviderKind::Gitlab => ProviderEndpoints {
            auth_url: "https://gitlab.com/oauth/authorize".into(),
            token_url: "https://gitlab.com/oauth/token".into(),
            userinfo_url: "https://gitlab.com/api/v4/user".into(),
            scopes: vec!["read_user".into(), "read_api".into()],
        },
        ProviderKind::Jihulab => ProviderEndpoints {
            auth_url: "https://jihulab.com/oauth/authorize".into(),
            token_url: "https://jihulab.com/oauth/token".into(),
            userinfo_url: "https://jihulab.com/api/v4/user".into(),
            scopes: vec!["read_user".into(), "read_api".into()],
        },
        // Gitea：SDK base URL 规则为 `{endpoint}/api/v1/`，故 userinfo 在其下。
        ProviderKind::Gitea => ProviderEndpoints {
            auth_url: format!("{}/login/oauth/authorize", conf.endpoint),
            token_url: format!("{}/login/oauth/access_token", conf.endpoint),
            userinfo_url: format!("{}/api/v1/user", conf.endpoint),
            scopes: vec![],
        },
        ProviderKind::Cloudflare => ProviderEndpoints {
            auth_url: format!(
                "{}/cdn-cgi/access/sso/oidc/{}/authorization",
                conf.endpoint, conf.clientid
            ),
            token_url: format!(
                "{}/cdn-cgi/access/sso/oidc/{}/token",
                conf.endpoint, conf.clientid
            ),
            userinfo_url: format!(
                "{}/cdn-cgi/access/sso/oidc/{}/userinfo",
                conf.endpoint, conf.clientid
            ),
            scopes: vec![
                "openid".into(),
                "email".into(),
                "profile".into(),
                "groups".into(),
            ],
        },
        ProviderKind::Mock | ProviderKind::Oidc | ProviderKind::Unknown => return None,
    };
    Some(endpoints)
}

/// userinfo 响应字段族：GitHub/Gitee/GitLab/Gitea/Cloudflare 的并集，
/// 按 provider 取用（Go 侧是各 SDK 的强类型结构体）。
#[derive(Debug, Default, Deserialize)]
struct RawUserInfo {
    login: Option<String>,
    username: Option<String>,
    sub: Option<String>,
    avatar_url: Option<String>,
    name: Option<String>,
    full_name: Option<String>,
    blog: Option<String>,
    website: Option<String>,
    website_url: Option<String>,
    email: Option<String>,
    bio: Option<String>,
    description: Option<String>,
    hireable: Option<bool>,
}

/// 把各 provider 的 userinfo 映射成登录档案（Go `NewUserFrom*`）。
/// 昵称为空时回落登录名（Go 同）；login 为空返回 None（调用方按失败处理）。
fn map_user(kind: ProviderKind, raw: &RawUserInfo) -> Option<OauthProfile> {
    let profile = match kind {
        ProviderKind::Github | ProviderKind::Gitee => OauthProfile {
            login: raw.login.clone()?,
            name: raw.name.clone().unwrap_or_default(),
            avatar_url: raw.avatar_url.clone().unwrap_or_default(),
            blog: raw.blog.clone().unwrap_or_default(),
            email: raw.email.clone().unwrap_or_default(),
            bio: raw.bio.clone().unwrap_or_default(),
            hireable: raw.hireable.unwrap_or(false),
            super_admin: false,
        },
        ProviderKind::Gitlab | ProviderKind::Jihulab => OauthProfile {
            login: raw.username.clone()?,
            name: raw.name.clone().unwrap_or_default(),
            avatar_url: raw.avatar_url.clone().unwrap_or_default(),
            blog: raw.website_url.clone().unwrap_or_default(),
            email: raw.email.clone().unwrap_or_default(),
            bio: raw.bio.clone().unwrap_or_default(),
            hireable: false,
            super_admin: false,
        },
        ProviderKind::Gitea => OauthProfile {
            login: raw.login.clone()?,
            name: raw.full_name.clone().unwrap_or_default(),
            avatar_url: raw.avatar_url.clone().unwrap_or_default(),
            blog: raw.website.clone().unwrap_or_default(),
            email: raw.email.clone().unwrap_or_default(),
            bio: raw.description.clone().unwrap_or_default(),
            hireable: false,
            super_admin: false,
        },
        ProviderKind::Cloudflare => OauthProfile {
            login: raw.sub.clone()?,
            name: raw.name.clone().unwrap_or_default(),
            avatar_url: String::new(),
            blog: String::new(),
            email: raw.email.clone().unwrap_or_default(),
            bio: String::new(),
            hireable: false,
            super_admin: false,
        },
        ProviderKind::Mock | ProviderKind::Oidc | ProviderKind::Unknown => return None,
    };
    if profile.login.is_empty() {
        return None;
    }
    let name = if profile.name.is_empty() {
        profile.login.clone()
    } else {
        profile.name
    };
    Some(OauthProfile { name, ..profile })
}

/// 管理员判定：`oauth2.admin` 逗号列表，空项忽略，大小写不敏感
/// （Go `strings.EqualFold` 循环）。
fn is_admin_login(login: &str, admin_list: &str) -> bool {
    admin_list
        .split(',')
        .any(|admin| !admin.is_empty() && login.eq_ignore_ascii_case(admin))
}

/// mock 登录的默认身份：`oauth2.admin` 第一个非空项（Go 默认 `"dev"`）。
fn mock_admin_login(admin_list: &str) -> &str {
    admin_list
        .split(',')
        .find(|admin| !admin.is_empty())
        .unwrap_or("dev")
}

fn random_string(len: usize) -> String {
    use rand::Rng;
    // 32B 随机数 hex 编码后截取：与仓内其它令牌（注册/会话）同构。
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)[..len].to_string()
}

/// 回调地址：scheme + Host + `/oauth2/callback`（Go `getRedirectURL`）。
/// scheme 看 `X-Forwarded-Proto` 或 Referer 前缀；本进程不直挂 TLS
/// （Go 的 `Request.TLS != nil` 分支无对应物，故只判代理头）。
fn redirect_url(headers: &HeaderMap) -> String {
    let https = headers
        .get("X-Forwarded-Proto")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("https"))
        || headers
            .get(header::REFERER)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("https://"));
    let scheme = if https { "https://" } else { "http://" };
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    format!("{scheme}{host}/oauth2/callback")
}

/// 会话 Cookie 是否带 Secure（Go `oauthCookieSecure`；进程不直挂 TLS，
/// 只识别反向代理头）。
fn cookie_secure(headers: &HeaderMap) -> bool {
    headers
        .get("X-Forwarded-Proto")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("https"))
        || headers
            .get("X-Forwarded-Ssl")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("on"))
}

fn state_cookie_name(cookie_name: &str) -> String {
    format!("{cookie_name}-sk")
}

/// 授权跳转（Go `AuthCodeURL(state, AccessTypeOnline)` 的查询串等价物）。
fn authorize_url(
    endpoints: &ProviderEndpoints,
    conf: &crate::config::Oauth2Config,
    state: &str,
    headers: &HeaderMap,
) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("response_type", "code");
    serializer.append_pair("client_id", &conf.clientid);
    serializer.append_pair("redirect_uri", &redirect_url(headers));
    if !endpoints.scopes.is_empty() {
        serializer.append_pair("scope", &endpoints.scopes.join(" "));
    }
    serializer.append_pair("state", state);
    format!("{}?{}", endpoints.auth_url, serializer.finish())
}

fn redirect_to(location: &str) -> Response {
    match HeaderValue::try_from(location) {
        Ok(value) => {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::FOUND;
            response.headers_mut().insert(header::LOCATION, value);
            response
        }
        Err(_) => problem(
            StatusCode::BAD_REQUEST,
            "oauth2_redirect_invalid",
            "登录跳转地址不合法",
        ),
    }
}

fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(UPSTREAM_TIMEOUT)
            .build()
            .expect("reqwest client")
    })
}

/// token 端点响应：成功取 `access_token`，失败带 `error` 描述。
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: Option<String>,
    error: Option<String>,
}

/// 授权码换令牌（Go `oauth2Config.Exchange`）。
/// 显式 `Accept: application/json`：GitHub 等默认回 urlencoded 表单体，
/// Go 的 oauth2 库按 content-type 分流，这里统一要 JSON。
async fn exchange_code(
    endpoints: &ProviderEndpoints,
    conf: &crate::config::Oauth2Config,
    code: &str,
    headers: &HeaderMap,
) -> anyhow::Result<String> {
    let form = [
        ("grant_type", "authorization_code"),
        ("code", code),
        ("redirect_uri", &redirect_url(headers)),
        ("client_id", &conf.clientid),
        ("client_secret", &conf.clientsecret),
    ];
    let response = http_client()
        .post(&endpoints.token_url)
        .header(header::ACCEPT, "application/json")
        .form(&form)
        .send()
        .await?;
    if !response.status().is_success() {
        anyhow::bail!("token endpoint status {}", response.status().as_u16());
    }
    let parsed: TokenResponse = response.json().await?;
    match parsed.access_token.filter(|token| !token.is_empty()) {
        Some(token) => Ok(token),
        None => anyhow::bail!(
            "token endpoint error: {}",
            parsed
                .error
                .unwrap_or_else(|| "missing access_token".into())
        ),
    }
}

/// 拉取 userinfo 并映射（Go 各 SDK 的 `CurrentUser` / `GetMyUserInfo`）。
async fn fetch_user(
    endpoints: &ProviderEndpoints,
    kind: ProviderKind,
    access_token: &str,
) -> anyhow::Result<OauthProfile> {
    let mut request = http_client()
        .get(&endpoints.userinfo_url)
        .bearer_auth(access_token);
    if kind == ProviderKind::Github {
        request = request.header(header::ACCEPT, "application/vnd.github+json");
    }
    let response = request.send().await?;
    if !response.status().is_success() {
        anyhow::bail!("userinfo status {}", response.status().as_u16());
    }
    let raw: RawUserInfo = response.json().await?;
    map_user(kind, &raw).ok_or_else(|| anyhow::anyhow!("empty login in userinfo"))
}

/// 登录收尾：签令牌 → upsert → 种会话 Cookie → 302 `/admin/`
/// （Go `callback` 尾部 + `oauth2MockLogin` 的公共部分）。
async fn finish_login(shared: &ApiShared, profile: OauthProfile, secure: bool) -> Response {
    let token = random_string(RANDOM_LEN);
    let cookie_token = token.clone();
    let expired_nanos = util_time::now_nanos() + SESSION_TTL_NANOS;
    let persisted = crate::rpc::support::blocking_write(&shared.db, move |tx| {
        crate::repo::users::upsert_oauth_user(tx, &profile, &token, expired_nanos)
    })
    .await;
    match persisted {
        Ok(_) => {
            let mut response = redirect_to("/admin/");
            set_cookie(
                &mut response,
                &shared.cookie_name,
                &cookie_token,
                SESSION_COOKIE_MAX_AGE,
                true,
                secure,
            );
            response
        }
        _ => problem(
            StatusCode::INTERNAL_SERVER_ERROR,
            "oauth2_session_persist_failed",
            "登录会话写入失败，请重试",
        ),
    }
}

/// `GET /oauth2/login`：mock 一键登录，否则生成 state 并跳转 provider 授权页。
async fn login(State(shared): State<ApiShared>, headers: HeaderMap) -> Response {
    let conf = &shared.conf().oauth2;
    if mock_login_allowed(&shared.conf()) {
        let admin_login = mock_admin_login(&conf.admin);
        let profile = OauthProfile {
            login: admin_login.to_string(),
            name: admin_login.to_string(),
            super_admin: true,
            ..Default::default()
        };
        return finish_login(&shared, profile, cookie_secure(&headers)).await;
    }

    let kind = provider_kind(conf);
    if kind == ProviderKind::Oidc {
        return problem(
            StatusCode::NOT_IMPLEMENTED,
            "oauth2_oidc_unsupported",
            "OIDC 登录方式暂未支持",
        );
    }
    let Some(endpoints) = provider_endpoints(conf, kind) else {
        return problem(
            StatusCode::BAD_REQUEST,
            "oauth2_provider_unsupported",
            "未配置的 OAuth2 登录方式",
        );
    };

    let random = random_string(RANDOM_LEN);
    let (state, state_key) = random.split_at(STATE_SPLIT);
    shared
        .oauth2_states
        .put(state_key.to_string(), state.to_string());

    let mut response = redirect_to(&authorize_url(&endpoints, conf, state, &headers));
    set_cookie(
        &mut response,
        &state_cookie_name(&shared.cookie_name),
        state_key,
        STATE_TTL.as_secs() as i64,
        true,
        cookie_secure(&headers),
    );
    response
}

#[derive(Debug, Deserialize)]
struct CallbackQuery {
    state: Option<String>,
    code: Option<String>,
}

/// `GET /oauth2/callback`：state 校验 → 换令牌 → 拉用户 → 管理员判定 →
/// upsert + 会话 Cookie → 302 `/admin/`（Go `callback`）。
async fn callback(
    State(shared): State<ApiShared>,
    headers: HeaderMap,
    Query(query): Query<CallbackQuery>,
) -> Response {
    let secure = cookie_secure(&headers);
    let state_cookie = state_cookie_name(&shared.cookie_name);

    // state 校验：cookie ↔ 查询参数，一次性消费；无论成败都清掉 state cookie
    // （Go 同）。
    let state_ok = match cookie_value(&headers, &state_cookie) {
        Some(state_key) => shared
            .oauth2_states
            .take(&state_key)
            .is_some_and(|expected| expected == query.state.as_deref().unwrap_or_default()),
        None => false,
    };
    if !state_ok {
        let mut response = problem(
            StatusCode::BAD_REQUEST,
            "oauth2_state_invalid",
            "登录状态校验失败，请重新登录",
        );
        set_cookie(&mut response, &state_cookie, "", -1, true, secure);
        return response;
    }

    let kind = provider_kind(&shared.conf().oauth2);
    if kind == ProviderKind::Oidc {
        return problem(
            StatusCode::NOT_IMPLEMENTED,
            "oauth2_oidc_unsupported",
            "OIDC 登录方式暂未支持",
        );
    }
    let conf = &shared.conf().oauth2;
    let Some(endpoints) = provider_endpoints(conf, kind) else {
        return problem(
            StatusCode::BAD_REQUEST,
            "oauth2_provider_unsupported",
            "未配置的 OAuth2 登录方式",
        );
    };
    let Some(code) = query.code.filter(|code| !code.is_empty()) else {
        return problem(
            StatusCode::BAD_REQUEST,
            "oauth2_code_missing",
            "登录授权码缺失，请重新登录",
        );
    };

    let access_token = match exchange_code(&endpoints, conf, &code, &headers).await {
        Ok(token) => token,
        Err(err) => {
            tracing::warn!(error = %err, "oauth2 token exchange failed");
            return problem(
                StatusCode::BAD_GATEWAY,
                "oauth2_token_exchange_failed",
                "登录授权失败，请稍后重试",
            );
        }
    };
    let profile = match fetch_user(&endpoints, kind, &access_token).await {
        Ok(profile) => profile,
        Err(err) => {
            tracing::warn!(error = %err, "oauth2 userinfo failed");
            return problem(
                StatusCode::BAD_GATEWAY,
                "oauth2_userinfo_failed",
                "获取用户信息失败，请稍后重试",
            );
        }
    };

    // 能登录后台的用户均为管理员（Go：通过 admin 判定即置 SuperAdmin）。
    if !is_admin_login(&profile.login, &conf.admin) {
        return problem(
            StatusCode::FORBIDDEN,
            "oauth2_not_admin",
            "该用户不是本站点管理员，无法登录",
        );
    }
    let profile = OauthProfile {
        super_admin: true,
        ..profile
    };
    finish_login(&shared, profile, secure).await
}

/// 页面级路由：须注册在 SPA fallback **之前**（`/oauth2/*` 不是 `/api/v2`）。
pub fn routes() -> axum::Router<ApiShared> {
    axum::Router::new()
        .route("/oauth2/login", get(login))
        .route("/oauth2/callback", get(callback))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Oauth2Config;

    fn conf_with(type_: &str, admin: &str) -> Oauth2Config {
        Oauth2Config {
            type_: type_.into(),
            admin: admin.into(),
            ..Default::default()
        }
    }

    #[test]
    fn state_store_take_is_single_use_and_expires() {
        let store = Oauth2StateStore::default();
        store.put("key".into(), "state".into());
        assert_eq!(store.take("key").as_deref(), Some("state"));
        // 一次性消费：第二次取不到。
        assert_eq!(store.take("key"), None);

        store.put("old".into(), "stale".into());
        {
            let mut entries = store.entries.lock().unwrap();
            entries.insert(
                "old".into(),
                ("stale".into(), Instant::now() - Duration::from_secs(1)),
            );
        }
        assert_eq!(store.take("old"), None);
    }

    #[test]
    fn mock_login_only_in_debug() {
        let debug = crate::config::Config {
            debug: true,
            oauth2: conf_with("mock", "dev"),
            ..Default::default()
        };
        assert!(mock_login_allowed(&debug));

        let prod = crate::config::Config {
            oauth2: conf_with("mock", "dev"),
            ..Default::default()
        };
        assert!(!mock_login_allowed(&prod));

        let github = crate::config::Config {
            debug: true,
            oauth2: conf_with("github", "dev"),
            ..Default::default()
        };
        assert!(!mock_login_allowed(&github));
    }

    #[test]
    fn admin_matching_is_case_insensitive_and_skips_empty() {
        assert!(is_admin_login("Dev", "dev,alice"));
        assert!(is_admin_login("dev", "DEV"));
        assert!(is_admin_login("alice", ",dev,,alice,"));
        assert!(!is_admin_login("bob", "dev,alice"));
        assert!(!is_admin_login("", "dev"));
        assert!(!is_admin_login("dev", ""));
    }

    #[test]
    fn mock_admin_login_picks_first_non_empty() {
        assert_eq!(mock_admin_login("dev,alice"), "dev");
        assert_eq!(mock_admin_login(",alice"), "alice");
        assert_eq!(mock_admin_login(""), "dev");
    }

    #[test]
    fn provider_endpoints_match_go_config() {
        let gitea = conf_with("gitea", "dev");
        gitea_endpoint_check(&gitea);
        let endpoints = provider_endpoints(&gitea, ProviderKind::Gitea).expect("gitea");
        assert_eq!(endpoints.auth_url, "/login/oauth/authorize");
        assert_eq!(endpoints.token_url, "/login/oauth/access_token");
        assert_eq!(endpoints.userinfo_url, "/api/v1/user");

        let cloudflare = conf_with("cloudflare", "dev");
        let endpoints =
            provider_endpoints(&cloudflare, ProviderKind::Cloudflare).expect("cloudflare");
        assert_eq!(
            endpoints.scopes,
            vec!["openid", "email", "profile", "groups"]
        );
        assert!(
            endpoints
                .auth_url
                .ends_with("/cdn-cgi/access/sso/oidc//authorization")
        );

        let gitlab =
            provider_endpoints(&conf_with("gitlab", "dev"), ProviderKind::Gitlab).expect("gitlab");
        assert_eq!(gitlab.scopes, vec!["read_user", "read_api"]);
        assert_eq!(gitlab.userinfo_url, "https://gitlab.com/api/v4/user");

        let github =
            provider_endpoints(&conf_with("github", "dev"), ProviderKind::Github).expect("github");
        assert!(github.scopes.is_empty());
        assert_eq!(github.userinfo_url, "https://api.github.com/user");

        assert!(provider_endpoints(&conf_with("oidc", "dev"), ProviderKind::Oidc).is_none());
        assert!(provider_endpoints(&conf_with("weird", "dev"), ProviderKind::Unknown).is_none());
    }

    /// gitea 端点带配置 endpoint 前缀（与 cloudflare 同一形状）。
    fn gitea_endpoint_check(conf: &Oauth2Config) {
        let mut conf = conf.clone();
        conf.endpoint = "https://git.example.test".into();
        let endpoints = provider_endpoints(&conf, ProviderKind::Gitea).expect("gitea");
        assert_eq!(
            endpoints.auth_url,
            "https://git.example.test/login/oauth/authorize"
        );
        assert_eq!(
            endpoints.userinfo_url,
            "https://git.example.test/api/v1/user"
        );
        let endpoints = provider_endpoints(&conf, ProviderKind::Cloudflare).expect("cloudflare");
        assert_eq!(
            endpoints.userinfo_url,
            "https://git.example.test/cdn-cgi/access/sso/oidc//userinfo"
        );
    }

    #[test]
    fn map_user_github_shape() {
        let raw: RawUserInfo = serde_json::from_str(
            r#"{"login":"octocat","name":"Octo Cat","avatar_url":"https://a.test/a.png",
                "blog":"https://blog.test","email":"octo@example.test","bio":"hi","hireable":true}"#,
        )
        .unwrap();
        let profile = map_user(ProviderKind::Github, &raw).expect("mapped");
        assert_eq!(profile.login, "octocat");
        assert_eq!(profile.name, "Octo Cat");
        assert_eq!(profile.blog, "https://blog.test");
        assert_eq!(profile.email, "octo@example.test");
        assert_eq!(profile.bio, "hi");
        assert!(profile.hireable);
    }

    #[test]
    fn map_user_gitlab_gitea_cloudflare_shapes() {
        let gitlab: RawUserInfo = serde_json::from_str(
            r#"{"username":"gl","name":"","avatar_url":"https://a.test/g.png",
                "website_url":"https://site.test","email":"gl@example.test","bio":"b"}"#,
        )
        .unwrap();
        let profile = map_user(ProviderKind::Gitlab, &gitlab).expect("mapped");
        assert_eq!(profile.login, "gl");
        // 昵称为空回落登录名（Go 同）。
        assert_eq!(profile.name, "gl");
        assert_eq!(profile.blog, "https://site.test");
        assert!(!profile.hireable);

        let gitea: RawUserInfo = serde_json::from_str(
            r#"{"login":"gt","full_name":"Gitea User","avatar_url":"https://a.test/t.png",
                "website":"https://w.test","email":"gt@example.test","description":"d"}"#,
        )
        .unwrap();
        let profile = map_user(ProviderKind::Gitea, &gitea).expect("mapped");
        assert_eq!(profile.name, "Gitea User");
        assert_eq!(profile.blog, "https://w.test");
        assert_eq!(profile.bio, "d");

        let cloudflare: RawUserInfo =
            serde_json::from_str(r#"{"sub":"cf-user","email":"cf@example.test","name":"CF"}"#)
                .unwrap();
        let profile = map_user(ProviderKind::Cloudflare, &cloudflare).expect("mapped");
        assert_eq!(profile.login, "cf-user");
        assert_eq!(profile.name, "CF");
        assert_eq!(profile.avatar_url, "");
        assert_eq!(profile.bio, "");
    }

    #[test]
    fn map_user_rejects_empty_login() {
        let raw: RawUserInfo = serde_json::from_str(r#"{"login":"","name":"x"}"#).unwrap();
        assert!(map_user(ProviderKind::Github, &raw).is_none());
        let raw: RawUserInfo = serde_json::from_str(r#"{"name":"x"}"#).unwrap();
        assert!(map_user(ProviderKind::Github, &raw).is_none());
        // 不支持的 provider 形态直接拒绝。
        let raw: RawUserInfo = serde_json::from_str(r#"{"sub":"x"}"#).unwrap();
        assert!(map_user(ProviderKind::Oidc, &raw).is_none());
    }

    #[test]
    fn authorize_url_encodes_query() {
        let conf = conf_with("github", "dev");
        let endpoints = provider_endpoints(&conf, ProviderKind::Github).expect("github");
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("panel.test"));
        let url = authorize_url(&endpoints, &conf, "st", &headers);
        assert!(url.starts_with("https://github.com/login/oauth/authorize?"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("client_id="));
        assert!(url.contains("redirect_uri=http%3A%2F%2Fpanel.test%2Foauth2%2Fcallback"));
        assert!(url.contains("state=st"));
        // scope 为空时不出现（Go `AuthCodeURL` 同）。
        assert!(!url.contains("scope="));
    }
}
