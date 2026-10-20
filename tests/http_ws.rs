//! 公开 WebSocket 集成测试：帧形状（now + servers 行同 HTTP 列表）、
//! 查看密码门（匿名 403 拒升级、查看会话放行）。

mod common;

use common::*;
use futures_util::StreamExt;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

/// 种子：服务器 7 的 V2 运行态（host 带 IP，验证匿名帧不暴露）。
fn seed_runtime(fixture: &PrimaryFixture) {
    use prost::Message as _;
    use santaizi_dashboard::proto;
    let host_payload = proto::Host {
        platform: "debian".into(),
        ip: "10.0.0.7".into(),
        ..Default::default()
    }
    .encode_to_vec();
    let now = santaizi_dashboard::util::time::now_nanos();
    let bucket_start = now - 60_000_000_000;
    let window_end = now - 30_000_000_000;
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO server_runtimes (server_id, status, protocol, current_node_uuid, \
                 host_state, connectivity_state, last_collected_at, last_received_at, host_payload) \
                 VALUES (?1, 'online', 'v2', ?2, 'online', 'full', ?3, ?3, ?4)",
                rusqlite::params![SERVER_ID, node(), now, host_payload],
            )?;
            tx.execute(
                "INSERT INTO availability_buckets (node_uuid, bucket_start, window_end, resolution, \
                 host_state, connectivity_state, expected_observers, healthy_observers, \
                 seen_observers, finalized) \
                 VALUES (?1, ?2, ?3, '30s', 'online', 'full', 3, 3, 2, 1)",
                rusqlite::params![node(), bucket_start, window_end],
            )?;
            Ok(())
        })
        .unwrap();
}

/// 拨 ws；带可选 Cookie 头。失败返回 HTTP 状态（握手被拒时）。
async fn ws_connect(
    port: u16,
    path: &str,
    cookie: Option<String>,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    u16,
> {
    let mut request = format!("ws://127.0.0.1:{port}{path}")
        .into_client_request()
        .unwrap();
    if let Some(cookie) = cookie
        && let Ok(value) = cookie.parse()
    {
        request.headers_mut().append("Cookie", value);
    }
    match tokio_tungstenite::connect_async(request).await {
        Ok((stream, _response)) => Ok(stream),
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            Err(response.status().as_u16())
        }
        Err(err) => panic!("ws dial failed unexpectedly: {err}"),
    }
}

async fn read_text_frame(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> serde_json::Value {
    loop {
        let message = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
            .await
            .expect("frame within 5s")
            .expect("stream open")
            .expect("frame ok");
        if let Message::Text(text) = message {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

/// 匿名帧：now 毫秒 + servers 行同 HTTP 列表（无 IP）。
#[tokio::test]
async fn ws_streams_public_runtime() {
    let fixture = setup_primary("ws-anon", false).await;
    seed_runtime(&fixture);
    let (port, cancel, server) = spawn_http(&fixture).await;

    let mut ws = ws_connect(port, "/ws", None).await.unwrap();
    let body = read_text_frame(&mut ws).await;
    assert!(
        body["now"].as_i64().unwrap() > 1_000_000_000_000,
        "毫秒时间戳"
    );
    let servers = body["servers"].as_array().unwrap();
    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0]["id"], 7);
    assert_eq!(servers[0]["name"], "node-7");
    assert_eq!(servers[0]["online"], true);
    assert!(servers[0]["host"].get("ip").is_none(), "匿名帧不暴露 IP");
    assert!(servers[0].get("note").is_none());

    // 第二帧继续到达（2s 周期）。
    let second = read_text_frame(&mut ws).await;
    assert!(second["servers"].as_array().unwrap().len() == 1);

    cancel.cancel();
    let _ = server.await;
}

/// 查看密码门：匿名拒升级（HTTP 403）、`-vp` 会话放行且看全量。
#[tokio::test]
async fn ws_view_password_gate() {
    let fixture = setup_primary_with("ws-vp", false, |conf| {
        conf.site.view_password = "s3cret".into();
    })
    .await;
    seed_runtime(&fixture);
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);

    // 匿名握手 → 403（不走 upgrade）。
    let denied = ws_connect(port, "/ws", None).await.unwrap_err();
    assert_eq!(denied, 403);

    // 现取 CSRF 换 -vp Cookie。
    let warmup = client()
        .get(format!("{base}/api/v2/public/bootstrap"))
        .send()
        .await
        .unwrap();
    let csrf = cookie_value(&warmup, "santaizi-dashboard-csrf").expect("csrf cookie");
    let ok = client()
        .post(format!("{base}/api/v2/public/view-password/session"))
        .header(
            reqwest::header::COOKIE,
            format!("santaizi-dashboard-csrf={csrf}"),
        )
        .header("X-CSRF-Token", &csrf)
        .json(&serde_json::json!({"password": "s3cret"}))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 204);
    let vp = cookie_value(&ok, "santaizi-dashboard-vp").expect("vp cookie");

    let mut ws = ws_connect(
        port,
        "/ws/v2/public/runtime",
        Some(format!("santaizi-dashboard-vp={vp}")),
    )
    .await
    .unwrap();
    let body = read_text_frame(&mut ws).await;
    assert_eq!(body["servers"].as_array().unwrap().len(), 1);
    assert_eq!(body["servers"][0]["id"], 7);

    cancel.cancel();
    let _ = server.await;
}
