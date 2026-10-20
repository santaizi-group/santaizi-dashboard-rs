//! 通知引擎集成测试：SSRF 拒内网（test 端点 502）、本地 stub 全投递链路
//! （占位符展开 / JSON 转义 / 自定义 header）、防骚扰静音与聚合窗口。

mod common;

use common::*;
use santaizi_dashboard::notify;
use santaizi_dashboard::repo::typed_resources as repo;
use std::io::{Read as _, Write as _};
use std::sync::{Arc, Mutex};

/// 单请求 HTTP stub：收一个请求、回 200、把请求行与 body 交给回调。
fn spawn_one_shot_stub() -> (u16, Arc<Mutex<String>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let captured = Arc::new(Mutex::new(String::new()));
    let captured_for_task = Arc::clone(&captured);
    std::thread::spawn(move || {
        if let Ok((mut socket, _)) = listener.accept() {
            let mut buffer = vec![0u8; 65536];
            let mut raw = String::new();
            loop {
                match socket.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(read) => {
                        raw.push_str(&String::from_utf8_lossy(&buffer[..read]));
                        if raw.contains("\r\n\r\n") {
                            let header_end = raw.find("\r\n\r\n").unwrap() + 4;
                            let content_length = raw
                                .to_lowercase()
                                .split("content-length:")
                                .nth(1)
                                .and_then(|rest| {
                                    rest.split_whitespace().next()?.parse::<usize>().ok()
                                })
                                .unwrap_or(0);
                            if raw[header_end..].len() >= content_length {
                                break;
                            }
                        }
                    }
                }
            }
            let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
            let _ = socket.flush();
            *captured_for_task.lock().unwrap() = raw;
        }
    });
    (port, captured)
}

fn channel_row(tag: &str, url: &str, body: &str, headers: &str) -> repo::NotificationRow {
    repo::NotificationRow {
        id: 1,
        tag: tag.into(),
        name: "stub".into(),
        url: url.into(),
        request_method: repo::NOTIFY_METHOD_POST,
        request_type: repo::NOTIFY_TYPE_JSON,
        request_header: headers.into(),
        request_body: body.into(),
        verify_ssl: Some(0),
        created_at_nanos: None,
        updated_at_nanos: None,
    }
}

/// SSRF：test 端点对内网 URL 502 + 中文原因。
#[tokio::test]
async fn test_endpoint_rejects_internal_url() {
    let fixture = setup_primary("notify-ssrf", false).await;
    let (port, cancel, server) = spawn_http(&fixture).await;
    let base = base_url(port);
    let member = insert_user(&fixture.db, "uma", true);
    let user_id = user_id_by_login(&fixture.db, "uma");
    let write_token = insert_api_token(&fixture.db, user_id, "write");

    let created = client()
        .post(format!("{base}/api/v2/admin/notifications"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {write_token}"),
        )
        .json(&serde_json::json!({
            "name": "local", "tag": "default",
            "url": "http://127.0.0.1:9/hook",
            "method": "post", "request_type": "json",
            "body": "{\"text\":\"#SANTAIZI#\"}"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let notification_id = created.json::<serde_json::Value>().await.unwrap()["data"]["id"]
        .as_i64()
        .unwrap();
    let _ = member;

    let test = client()
        .post(format!(
            "{base}/api/v2/admin/notifications/{notification_id}/test"
        ))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {write_token}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(test.status(), 502);
    let body = test.json::<serde_json::Value>().await.unwrap();
    assert_eq!(body["code"], "notification_test_failed");
    assert!(
        body["detail"].as_str().unwrap().contains("内部地址被禁止"),
        "detail 应带 SSRF 原因"
    );

    cancel.cancel();
    let _ = server.await;
}

/// 本地 stub 全链路：POST JSON 占位符展开 + json 转义 + 自定义 header + GET 无 body。
#[tokio::test]
async fn webhook_delivery_round_trip() {
    let (stub_port, captured) = spawn_one_shot_stub();
    let now = santaizi_dashboard::util::time::now_nanos();
    let channel = channel_row(
        "default",
        &format!("http://127.0.0.1:{stub_port}/hook?from=notify"),
        r##"{"text":"#SANTAIZI#","server":"#SERVER.NAME#"}"##,
        r#"{"X-Token":"abc"}"#,
    );
    let server_ctx = notify::ServerContext {
        id: 7,
        name: "node-7".into(),
        state: Default::default(),
        host: Default::default(),
    };
    let outcome = tokio::task::spawn_blocking(move || {
        notify::deliver_channel_allow_internal(&channel, "这是测试\n消息", Some(&server_ctx), now)
    })
    .await
    .unwrap();
    assert!(outcome.is_ok(), "投递应成功：{outcome:?}");

    let raw = captured.lock().unwrap().clone();
    assert!(
        raw.starts_with("POST /hook?from=notify HTTP/1.1"),
        "请求行 {}",
        &raw[..40.min(raw.len())]
    );
    assert!(
        raw.to_lowercase()
            .contains("content-type: application/json")
    );
    assert!(raw.to_lowercase().contains("x-token: abc"));
    // #SANTAIZI# = JSON 转义后的消息（换行 → \n）；#SERVER.NAME# 原值。
    let escaped_newline = format!("{}n", '\\');
    assert!(
        raw.contains(&format!(r##""text":"这是测试{escaped_newline}消息""##)),
        "消息应 JSON 转义"
    );
    assert!(raw.contains(r##""server":"node-7""##));

    // GET 渠道：无 body。
    let (get_port, get_captured) = spawn_one_shot_stub();
    let mut get_channel = channel_row(
        "default",
        &format!("http://127.0.0.1:{get_port}/get-hook"),
        "",
        "",
    );
    get_channel.request_method = repo::NOTIFY_METHOD_GET;
    let outcome = tokio::task::spawn_blocking(move || {
        notify::deliver_channel_allow_internal(&get_channel, "ping", None, now)
    })
    .await
    .unwrap();
    assert!(outcome.is_ok());
    let raw = get_captured.lock().unwrap().clone();
    assert!(raw.starts_with("GET /get-hook HTTP/1.1"));
}

/// 防骚扰静音：同 muteLabel 首放次静；聚合窗口合并同类。
#[tokio::test]
async fn mute_and_aggregate() {
    let fixture = setup_primary("notify-mute", false).await;
    fixture
        .db
        .write(|tx| {
            tx.execute(
                "INSERT INTO notifications (name, tag, url, request_method, request_type, request_body) VALUES ('n', 'default', 'http://127.0.0.1:9/x', 2, 1, '{}')",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    let notifier = fixture.state.notifier();

    // 聚合：同 tag+kind 两条进一个桶。
    notifier
        .send(
            "default",
            "[离线] node-a\n节点失联",
            Some("server-offline-1".into()),
            None,
        )
        .await;
    notifier
        .send(
            "default",
            "[离线] node-b\n节点失联",
            Some("server-offline-2".into()),
            None,
        )
        .await;
    assert_eq!(notifier.pending_buckets(), 1, "同 tag+kind 合一桶");

    // 静音：同 label 第二发在窗口内被吞（直接投递路径）。
    notifier
        .send("default", "一次性告警", Some("alert-once-1".into()), None)
        .await;
    let state_snapshot = notifier.pending_buckets();
    // 纯幂等性检查：再次发送不 panic 且桶数不变（静音路径未进聚合）。
    notifier
        .send("default", "一次性告警", Some("alert-once-1".into()), None)
        .await;
    assert_eq!(
        notifier.pending_buckets(),
        state_snapshot,
        "静音路径不进聚合桶"
    );
}
