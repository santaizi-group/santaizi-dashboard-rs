//! 公开实时推送（Phase 5）：`/ws` 与 `/ws/v2/public/runtime` 同一处理器，
//! 对齐 Go `controller/streams.go`——2 秒一帧 `{"now": 毫秒, "servers": [...]}`
//! （与 HTTP public servers 行同形），查看密码门（会员/查看会话全量、
//! 匿名 guest 列表），每 4 帧一个 WebSocket Ping，上行仅用于保活。

use std::time::Duration;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::HeaderMap;
use axum::response::Response;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};

use crate::rpc::support::blocking_read;
use crate::util::time as util_time;

use super::auth::{ApiShared, OptionalUser};
use super::{presentation, public};

/// 帧间隔（Go `frameInterval`）。
const FRAME_INTERVAL: Duration = Duration::from_secs(2);
/// 每 N 帧一个 Ping（Go `framesPerPing`）。
const FRAMES_PER_PING: usize = 4;

/// ws 路由组（注册须在 SPA fallback 之前）。
pub fn routes() -> axum::Router<ApiShared> {
    axum::Router::new()
        .route("/ws", axum::routing::get(ws_handler))
        .route("/ws/v2/public/runtime", axum::routing::get(ws_handler))
}

async fn ws_handler(
    State(shared): State<ApiShared>,
    OptionalUser(user): OptionalUser,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    // 查看密码门（Go `ValidateViewPassword{AbortWhenFail: true}`）。
    if let Err(response) = public::require_public_access(&headers, &shared, user.as_ref()) {
        return *response;
    }
    let include_hidden = user.is_some() || public::view_password_verified(&headers, &shared);
    let token_full = user.as_ref().is_some_and(|current| current.is_api);
    upgrade.on_upgrade(move |socket| run_stream(shared, include_hidden, token_full, socket))
}

/// 快照行 → 帧内 JSON（与 HTTP 列表行同形；Go 注释明确禁止直接 Marshal model.Server）。
async fn frame_rows(shared: &ApiShared, include_hidden: bool, token_full: bool) -> Vec<Value> {
    let now_nanos = util_time::now_nanos();
    let rows = blocking_read(&shared.db, move |conn| {
        crate::repo::servers::list_public(conn, include_hidden, now_nanos)
    })
    .await
    .unwrap_or_default();
    let threshold = shared.conf().telemetry.offline_threshold_seconds;
    rows.iter()
        .map(|row| {
            let snapshot = presentation::runtime_presentation(row);
            public::server_json(row, &snapshot, token_full, threshold)
        })
        .collect()
}

async fn build_frame(shared: &ApiShared, include_hidden: bool, token_full: bool) -> String {
    let servers = frame_rows(shared, include_hidden, token_full).await;
    json!({
        "now": util_time::now_nanos() / 1_000_000,
        "servers": servers,
    })
    .to_string()
}

async fn run_stream(shared: ApiShared, include_hidden: bool, token_full: bool, socket: WebSocket) {
    let (mut sender, mut receiver) = socket.split();
    let mut ticker = tokio::time::interval(FRAME_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // interval 首拍立即触发：对齐 Go `time.NewTicker` 之后的先发一帧体验。
    ticker.reset();
    let mut frames: usize = 0;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                frames += 1;
                if frames.is_multiple_of(FRAMES_PER_PING)
                    && sender.send(Message::Ping(Default::default())).await.is_err()
                {
                    break;
                }
                let frame = build_frame(&shared, include_hidden, token_full).await;
                if sender.send(Message::Text(frame.into())).await.is_err() {
                    break;
                }
            }
            message = receiver.next() => {
                match message {
                    // 上行只用于保活：Pong 自动回，Close/错误断开。
                    Some(Ok(Message::Ping(payload))) => {
                        if sender.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
        }
    }
}
