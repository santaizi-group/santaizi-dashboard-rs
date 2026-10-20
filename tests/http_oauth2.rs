//! OAuth2 登录流集成测试：真实 axum 服务 + 本地 stub provider。
//!
//! 覆盖 mock 一键登录（debug 专用）、真实 provider 的 authorize 重定向与
//! state cookie、state 校验（不匹配 / 缺失 / 一次性消费）、gitea 形态的
//! 完整回调（token 交换 → userinfo → 管理员判定 → upsert → 会话 Cookie），
//! 以及非管理员拒绝。外部网络零依赖：真实 provider 走本地 stub。

mod common;

use common::*;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const SESSION_COOKIE: &str = "santaizi-dashboard";
const STATE_COOKIE: &str = "santaizi-dashboard-sk";

/// 读全一个 HTTP 请求（头 + Content-Length 声明的主体；stub 场景体很小）。
async fn read_request(socket: &mut TcpStream) -> String {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 2048];
    loop {
        let read = socket.read(&mut chunk).await.unwrap_or(0);
        if read == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..read]);
        let text = String::from_utf8_lossy(&buf);
        if let Some(split) = text.find("\r\n\r\n") {
            let content_length = text[..split]
                .lines()
                .filter_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .next()
                .unwrap_or(0);
            if buf.len() >= split + 4 + content_length {
                break;
            }
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

/// 起一个 gitea 形态 stub：POST 换令牌、GET `/api/v1/user` 回给定用户 JSON。
/// 返回 `(endpoint, 收到的请求列表)`。
async fn spawn_gitea_stub(user_json: &'static str) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
    let endpoint = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorder = seen.clone();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let recorder = recorder.clone();
            tokio::spawn(async move {
                let request = read_request(&mut socket).await;
                let is_post = request.starts_with("POST ");
                recorder.lock().unwrap().push(request);
                let body: &[u8] = if is_post {
                    br#"{"access_token":"stub-token","token_type":"bearer","expires_in":3600}"#
                } else {
                    user_json.as_bytes()
                };
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(body).await;
            });
        }
    });
    (endpoint, seen)
}

fn state_from_location(location: &str) -> String {
    location
        .split("state=")
        .nth(1)
        .and_then(|rest| rest.split('&').next())
        .expect("state in authorize url")
        .to_string()
}

fn set_cookie_header(response: &reqwest::Response, name: &str) -> Option<String> {
    response
        .headers()
        .get_all(reqwest::header::SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .find(|raw| raw.split('=').next() == Some(name))
        .map(|raw| raw.to_string())
}

/// 上游请求必须带产品 UA：GitHub / Gitee 等 API 对无 User-Agent 的请求直接 403
/// （Go 侧 go-github / go-gitlab SDK 每请求必带）。
fn assert_user_agent(request: &str) {
    assert!(
        request
            .to_ascii_lowercase()
            .contains("user-agent: santaizi-dashboard-rs/"),
        "{request}"
    );
}

/// mock 一键登录：debug+mock 下 302 `/admin/`、种会话 Cookie、库内用户
/// 建成且为超级管理员；随后该 Cookie 能通过会话端点认出人。
#[tokio::test]
async fn mock_login_signs_session_and_redirects() {
    let fixture = setup_primary_with("oauth2-mock", false, |conf| {
        conf.debug = true;
        conf.oauth2.type_ = "mock".into();
        conf.oauth2.admin = "dev,alice".into();
    })
    .await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);

    let response = client()
        .get(format!("{base}/oauth2/login"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 302);
    assert_eq!(
        response.headers().get(reqwest::header::LOCATION).unwrap(),
        "/admin/"
    );
    // mock 分支不走 state 机：不种 `-sk`。
    assert_eq!(cookie_value(&response, STATE_COOKIE), None);

    let session_cookie = set_cookie_header(&response, SESSION_COOKIE).expect("session cookie");
    assert!(session_cookie.contains("HttpOnly"), "{session_cookie}");
    assert!(session_cookie.contains("Max-Age=86400"), "{session_cookie}");
    let token = cookie_value(&response, SESSION_COOKIE).expect("session token");

    // 带会话 Cookie 访问会话端点：认出 mock 身份。
    let session = client()
        .get(format!("{base}/api/v2/auth/session"))
        .header(reqwest::header::COOKIE, format!("{SESSION_COOKIE}={token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(session.status(), 200);
    let body: serde_json::Value = session.json().await.unwrap();
    assert_eq!(body["data"]["authenticated"], true);
    assert_eq!(body["data"]["user"]["login"], "dev");
    assert_eq!(body["data"]["user"]["super_admin"], true);

    // 库内用户行：令牌即 Cookie 值，超管置位。
    let (db_login, db_token, db_super) = fixture
        .db
        .read(|conn| {
            Ok(
                conn.query_row("SELECT login, token, super_admin FROM users", [], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })?,
            )
        })
        .expect("read user row");
    assert_eq!(db_login, "dev");
    assert_eq!(db_token, token);
    assert_eq!(db_super, 1);

    cancel.cancel();
    let _ = server.await;
}

/// 真实 provider 的 login：跳转 authorize 页并种 5 分钟 state cookie。
#[tokio::test]
async fn github_login_redirects_with_state_cookie() {
    let fixture = setup_primary_with("oauth2-gh-login", false, |conf| {
        conf.oauth2.type_ = "github".into();
        conf.oauth2.clientid = "gh-client".into();
        conf.oauth2.clientsecret = "gh-secret".into();
        conf.oauth2.admin = "dev".into();
    })
    .await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);

    let response = client()
        .get(format!("{base}/oauth2/login"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 302);
    let location = response
        .headers()
        .get(reqwest::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        location.starts_with("https://github.com/login/oauth/authorize?"),
        "{location}"
    );
    assert!(location.contains("client_id=gh-client"), "{location}");
    assert!(location.contains("response_type=code"), "{location}");
    assert!(
        location.contains("redirect_uri=http%3A%2F%2F127.0.0.1%3A"),
        "{location}"
    );
    let state = state_from_location(&location);
    assert_eq!(state.len(), 16);

    let state_key = cookie_value(&response, STATE_COOKIE).expect("state cookie");
    assert_eq!(state_key.len(), 16);
    let header = set_cookie_header(&response, STATE_COOKIE).expect("state cookie header");
    assert!(header.contains("Max-Age=300"), "{header}");
    assert!(header.contains("HttpOnly"), "{header}");

    // state 不匹配：拒绝且消费掉（一次性）。
    let mismatch = client()
        .get(format!("{base}/oauth2/callback"))
        .query(&[("state", "tampered"), ("code", "ignored")])
        .header(
            reqwest::header::COOKIE,
            format!("{STATE_COOKIE}={state_key}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(mismatch.status(), 400);
    // 失败也清 state cookie。
    assert_eq!(
        set_cookie_header(&mismatch, STATE_COOKIE).map(|raw| raw.contains("Max-Age=-1")),
        Some(true)
    );
    let body: serde_json::Value = mismatch.json().await.unwrap();
    assert_eq!(body["code"], "oauth2_state_invalid");

    // 同一 stateKey 再用正确 state 重放：已消费，仍拒绝。
    let replay = client()
        .get(format!("{base}/oauth2/callback"))
        .query(&[("state", state.as_str()), ("code", "ignored")])
        .header(
            reqwest::header::COOKIE,
            format!("{STATE_COOKIE}={state_key}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), 400);
    assert_eq!(
        replay.json::<serde_json::Value>().await.unwrap()["code"],
        "oauth2_state_invalid"
    );

    // 完全没有 state cookie：拒绝。
    let no_cookie = client()
        .get(format!("{base}/oauth2/callback"))
        .query(&[("state", state.as_str()), ("code", "ignored")])
        .send()
        .await
        .unwrap();
    assert_eq!(no_cookie.status(), 400);
    assert_eq!(
        no_cookie.json::<serde_json::Value>().await.unwrap()["code"],
        "oauth2_state_invalid"
    );

    cancel.cancel();
    let _ = server.await;
}

/// gitea 形态完整回调：token 交换（表单断言）→ userinfo（Bearer 断言）→
/// 大小写不敏感的管理员判定 → upsert → 会话 Cookie → `/admin/`。
#[tokio::test]
async fn gitea_stub_completes_full_login_flow() {
    let (stub_endpoint, stub_seen) = spawn_gitea_stub(
        r#"{"id":42,"login":"dev","full_name":"Dev User","avatar_url":"https://avatar.test/d.png",
            "website":"https://blog.test","email":"dev@example.test","description":"hello"}"#,
    )
    .await;
    let fixture = setup_primary_with("oauth2-gitea", false, |conf| {
        conf.oauth2.type_ = "gitea".into();
        conf.oauth2.endpoint = stub_endpoint.clone();
        conf.oauth2.clientid = "gitea-client".into();
        conf.oauth2.clientsecret = "gitea-secret".into();
        // 大小写不一致：stub 回 `dev`，配置写 `Dev`（Go EqualFold 语义）。
        conf.oauth2.admin = "Dev".into();
    })
    .await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);

    let login = client()
        .get(format!("{base}/oauth2/login"))
        .send()
        .await
        .unwrap();
    assert_eq!(login.status(), 302);
    let location = login
        .headers()
        .get(reqwest::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        location.starts_with(&format!("{stub_endpoint}/login/oauth/authorize?")),
        "{location}"
    );
    let state = state_from_location(&location);
    let state_key = cookie_value(&login, STATE_COOKIE).expect("state cookie");

    let callback = client()
        .get(format!("{base}/oauth2/callback"))
        .query(&[("state", state.as_str()), ("code", "stub-code")])
        .header(
            reqwest::header::COOKIE,
            format!("{STATE_COOKIE}={state_key}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(callback.status(), 302);
    assert_eq!(
        callback.headers().get(reqwest::header::LOCATION).unwrap(),
        "/admin/"
    );
    let token = cookie_value(&callback, SESSION_COOKIE).expect("session cookie");

    // stub 侧：先是 token 交换（表单字段齐全），再是 userinfo（Gitea `token` 方案）。
    let requests = stub_seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2, "stub saw: {requests:?}");
    assert_user_agent(&requests[0]);
    assert_user_agent(&requests[1]);
    assert!(
        requests[0].starts_with("POST /login/oauth/access_token"),
        "{:?}",
        requests[0]
    );
    assert!(
        requests[0].contains("grant_type=authorization_code"),
        "{:?}",
        requests[0]
    );
    assert!(requests[0].contains("code=stub-code"), "{:?}", requests[0]);
    assert!(
        requests[0].contains("client_id=gitea-client"),
        "{:?}",
        requests[0]
    );
    assert!(
        requests[0].contains("client_secret=gitea-secret"),
        "{:?}",
        requests[0]
    );
    assert!(
        requests[0].contains("redirect_uri=http%3A%2F%2F127.0.0.1"),
        "{:?}",
        requests[0]
    );
    assert!(
        requests[1].starts_with("GET /api/v1/user"),
        "{:?}",
        requests[1]
    );
    // hyper 归一化头名为小写。Gitea SDK 用 `token` 方案（Go `SetToken` 形态）。
    assert!(
        requests[1]
            .to_ascii_lowercase()
            .contains("authorization: token stub-token"),
        "{:?}",
        requests[1]
    );

    // 会话可用，档案按 gitea 映射落库。
    let session = client()
        .get(format!("{base}/api/v2/auth/session"))
        .header(reqwest::header::COOKIE, format!("{SESSION_COOKIE}={token}"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = session.json().await.unwrap();
    assert_eq!(body["data"]["user"]["login"], "dev");
    assert_eq!(body["data"]["user"]["name"], "Dev User");

    let (name, avatar, blog, email, bio, super_admin) = fixture
        .db
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT name, avatar_url, blog, email, bio, super_admin FROM users WHERE login = 'dev'",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )?)
        })
        .expect("read user row");
    assert_eq!(name, "Dev User");
    assert_eq!(avatar, "https://avatar.test/d.png");
    assert_eq!(blog, "https://blog.test");
    assert_eq!(email, "dev@example.test");
    assert_eq!(bio, "hello");
    assert_eq!(super_admin, 1);

    cancel.cancel();
    let _ = server.await;
}

/// 非管理员：回调成功拿到用户但不放行，不种会话 Cookie，不写库。
#[tokio::test]
async fn gitea_stub_rejects_non_admin() {
    let (stub_endpoint, _stub_seen) =
        spawn_gitea_stub(r#"{"login":"dev","full_name":"Dev"}"#).await;
    let fixture = setup_primary_with("oauth2-gitea-denied", false, |conf| {
        conf.oauth2.type_ = "gitea".into();
        conf.oauth2.endpoint = stub_endpoint.clone();
        conf.oauth2.clientid = "gitea-client".into();
        conf.oauth2.clientsecret = "gitea-secret".into();
        conf.oauth2.admin = "alice".into();
    })
    .await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);

    let login = client()
        .get(format!("{base}/oauth2/login"))
        .send()
        .await
        .unwrap();
    let location = login
        .headers()
        .get(reqwest::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let state = state_from_location(&location);
    let state_key = cookie_value(&login, STATE_COOKIE).expect("state cookie");

    let callback = client()
        .get(format!("{base}/oauth2/callback"))
        .query(&[("state", state.as_str()), ("code", "stub-code")])
        .header(
            reqwest::header::COOKIE,
            format!("{STATE_COOKIE}={state_key}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(callback.status(), 403);
    assert_eq!(cookie_value(&callback, SESSION_COOKIE), None);
    assert_eq!(
        callback.json::<serde_json::Value>().await.unwrap()["code"],
        "oauth2_not_admin"
    );

    let count: i64 = fixture
        .db
        .read(|conn| Ok(conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get::<_, i64>(0))?))
        .expect("count users");
    assert_eq!(count, 0);

    cancel.cancel();
    let _ = server.await;
}

/// OIDC：显式拒绝而非伪造登录。
#[tokio::test]
async fn oidc_provider_is_rejected_explicitly() {
    let fixture = setup_primary_with("oauth2-oidc", false, |conf| {
        conf.oauth2.type_ = "oidc".into();
        conf.oauth2.clientid = "oidc-client".into();
        conf.oauth2.clientsecret = "oidc-secret".into();
        conf.oauth2.oidcissuer = "https://issuer.test".into();
        conf.oauth2.admin = "dev".into();
    })
    .await;
    let (port, cancel, server) = spawn_http(&fixture).await;

    let login = client()
        .get(format!("{}/oauth2/login", base_url(port)))
        .send()
        .await
        .unwrap();
    assert_eq!(login.status(), 501);
    assert_eq!(
        login.json::<serde_json::Value>().await.unwrap()["code"],
        "oauth2_oidc_unsupported"
    );

    cancel.cancel();
    let _ = server.await;
}
