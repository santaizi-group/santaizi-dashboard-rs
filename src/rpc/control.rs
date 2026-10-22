//! 主端 Control 流：对齐 Go `service/rpc/v2.go` 的 `V2Handler.Control`。
//!
//! 无 NAT / DDNS / Bot：hello 认证（agent 证书，或回落 `client_secret`
//! 元数据）→ 绑节点 → 先签凭证再派端点指派，随后 15s 周期重推指派。
//! 探测回执暂只记录（派发引擎属 Phase 6），运行态帧落
//! `agent_telemetry_runtimes`。

use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::pki::uri::{DeviceIdentity, DeviceKind, UriError};
use crate::proto;
use crate::repo::bindings;
use crate::rpc::auth;
use crate::rpc::support::{blocking_read, blocking_write};
use crate::state::AppState;
use crate::telemetry::assignment;
use crate::telemetry::latency;
use crate::telemetry::runtime;

/// 指派重推周期：对齐 Go `time.NewTicker(15 * time.Second)`。
const ASSIGNMENT_REFRESH: Duration = Duration::from_secs(15);
/// 响应通道容量。
const CONTROL_RESPONSE_CAPACITY: usize = 32;
const CLIENT_SECRET_KEY: &str = "client_secret";

#[derive(Clone)]
pub struct ControlHandler {
    state: Arc<AppState>,
}

impl ControlHandler {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    /// `authHandler.Check`：`client_secret` 元数据 → server_id。
    fn check_client_secret(&self, secret: Option<&str>) -> StdResult<i64, Status> {
        match secret.and_then(|secret| self.state.secrets().server_id(secret)) {
            Some(server_id) => Ok(server_id),
            None => Err(Status::unauthenticated("客户端认证失败")),
        }
    }

    /// `authenticateControl`：有证书走 agent mTLS 身份，无证书回落秘密元数据。
    async fn authenticate_control(
        &self,
        peer: Option<&StdResult<DeviceIdentity, UriError>>,
        hello: &proto::AgentControlHello,
        client_secret: Option<&str>,
    ) -> StdResult<i64, Status> {
        match peer {
            Some(Err(err)) => Err(Status::permission_denied(err.to_string())),
            Some(Ok(identity)) => {
                if identity.kind != DeviceKind::Agent {
                    return Err(Status::permission_denied(
                        "control requires an agent certificate",
                    ));
                }
                if identity.node_uuid != hello.node_uuid {
                    return Err(Status::permission_denied(
                        "certificate UUID does not match hello",
                    ));
                }
                blocking_read(self.state.db(), {
                    let node_uuid = identity.node_uuid.clone();
                    move |conn| bindings::server_id_from_node_uuid(conn, &node_uuid)
                })
                .await
                .map_err(|err| Status::internal(err.to_string()))?
                .ok_or_else(|| Status::unauthenticated("node is not bound to a server"))
            }
            None => self.check_client_secret(client_secret),
        }
    }

    /// 首帧三连发：绑节点、签凭证、派初始指派，同一事务内完成。
    async fn establish_session(
        &self,
        server_id: i64,
        hello: &proto::AgentControlHello,
    ) -> StdResult<(proto::SignedAgentCredential, proto::EndpointAssignment), Status> {
        let node_uuid = hello.node_uuid.clone();
        let session_id = hello.session_id.clone();
        let now_nanos = crate::util::time::now_nanos();
        let now_unix = now_nanos / 1_000_000_000;
        let validity_secs = self.state.conf().telemetry.credential_validity_days as i64 * 24 * 3600;
        let state = Arc::clone(&self.state);
        blocking_write(self.state.db(), move |tx| {
            bindings::bind_server_node_for_protocol(tx, server_id, &node_uuid, now_nanos, true)?;
            let version = assignment::current_config_version(tx);
            let credential = state
                .signer()
                .sign(&node_uuid, version, now_unix, validity_secs)
                .map_err(|err| anyhow::anyhow!("sign telemetry credential: {err}"))?;
            let assignment = assignment::endpoint_assignment_for_node(
                tx,
                &state.conf(),
                &node_uuid,
                &session_id,
                1,
            )?;
            Ok((credential, assignment))
        })
        .await
        .map_err(|err| Status::internal(err.to_string()))
    }

    /// 周期重推：activation = 当前序列 + 1，让新挂从端从最新序列开始。
    async fn refreshed_assignment(
        &self,
        server_id: i64,
        hello: &proto::AgentControlHello,
    ) -> StdResult<proto::EndpointAssignment, Status> {
        let node_uuid = hello.node_uuid.clone();
        let session_id = hello.session_id.clone();
        let state = Arc::clone(&self.state);
        blocking_read(self.state.db(), move |conn| {
            let tx = conn.unchecked_transaction()?;
            let activation = runtime::current_sequence_for_server(&tx, server_id)? + 1;
            let assignment = assignment::endpoint_assignment_for_node(
                &tx,
                &state.conf(),
                &node_uuid,
                &session_id,
                activation,
            )?;
            tx.commit()?;
            Ok(assignment)
        })
        .await
        .map_err(|err| Status::internal(err.to_string()))
    }

    /// Control 运行态帧 → `agent_telemetry_runtimes` upsert。
    async fn upsert_agent_runtime(
        &self,
        hello: &proto::AgentControlHello,
        agent_runtime: &proto::AgentRuntime,
    ) -> StdResult<(), Status> {
        let row = runtime::AgentRuntimeRow {
            node_uuid: hello.node_uuid.clone(),
            wal_pressure: agent_runtime.wal_pressure,
            wal_bytes: agent_runtime.wal_bytes,
            pending_events: agent_runtime.pending_events,
            oldest_pending_nanos: agent_runtime.oldest_pending_unix_nano as u64,
            sink_cursors: prost::Message::encode_to_vec(agent_runtime),
            clock_untrusted: agent_runtime.clock_untrusted,
            protocol_version: agent_runtime.protocol_version.clone(),
            agent_version: hello.agent_version.clone(),
        };
        blocking_write(self.state.db(), move |tx| {
            runtime::upsert_agent_runtime(tx, &row)
        })
        .await
        .map_err(|err| Status::internal(err.to_string()))
    }

    /// Control 会话：hello 认证 → 绑节点 → 凭证 + 指派 → 主循环。
    async fn run_control(
        &self,
        peer: Option<StdResult<DeviceIdentity, UriError>>,
        client_secret: Option<String>,
        mut stream: Streaming<proto::AgentControlRequest>,
        tx: mpsc::Sender<StdResult<proto::PrimaryControlResponse, Status>>,
    ) -> StdResult<(), Status> {
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("valid control hello is required"))?;
        let hello = match first.body {
            Some(proto::agent_control_request::Body::Hello(hello)) => hello,
            _ => return Err(Status::invalid_argument("valid control hello is required")),
        };
        if hello.node_uuid.len() != 16 || hello.session_id.len() != 16 {
            return Err(Status::invalid_argument("valid control hello is required"));
        }
        let server_id = self
            .authenticate_control(peer.as_ref(), &hello, client_secret.as_deref())
            .await?;
        // 在线节点的 Control 流还在，别的节点不许重绑同一个 server：
        // 对齐 Go `enroll.go:66` 的 FailedPrecondition 分支。
        if self
            .state
            .monitor()
            .session_node_conflict(server_id, &hello.node_uuid)
        {
            return Err(Status::failed_precondition(format!(
                "server {server_id} has a live control session from another node"
            )));
        }
        let (credential, assignment) = self.establish_session(server_id, &hello).await?;
        // 先凭证后指派：探针拿到凭证才能向主端 Ingest 上报。
        send_control_response(
            &tx,
            proto::PrimaryControlResponse {
                body: Some(proto::primary_control_response::Body::Credential(
                    credential,
                )),
            },
        )
        .await?;
        send_control_response(
            &tx,
            proto::PrimaryControlResponse {
                body: Some(proto::primary_control_response::Body::Assignment(
                    assignment,
                )),
            },
        )
        .await?;
        // 派发引擎会话注册（capabilities 来自 hello；断流注销）。
        let capabilities = hello
            .capabilities
            .clone()
            .map(|caps| caps.enabled)
            .unwrap_or_default();
        self.state.monitor().register_session(
            server_id,
            hello.node_uuid.clone(),
            capabilities,
            tx.clone(),
        );
        let node_uuid = hello.node_uuid.clone();
        let outcome = self
            .control_loop(server_id, hello, stream, tx.clone())
            .await;
        self.state
            .monitor()
            .unregister_session(server_id, &node_uuid);
        outcome
    }

    /// Control 主循环：凭证/指派已发，此后只收请求并按周期重推指派。
    async fn control_loop(
        &self,
        server_id: i64,
        hello: proto::AgentControlHello,
        mut stream: Streaming<proto::AgentControlRequest>,
        tx: mpsc::Sender<StdResult<proto::PrimaryControlResponse, Status>>,
    ) -> StdResult<(), Status> {
        let mut ticker = tokio::time::interval(ASSIGNMENT_REFRESH);
        ticker.tick().await; // 首个 tick 立即返回，跳过与初始指派的重复推送。
        loop {
            tokio::select! {
                message = stream.message() => match message? {
                    Some(request) => match request.body {
                        Some(proto::agent_control_request::Body::ProbeResult(result)) => {
                            // 反查 pending；命中且窗口满 30 样本则落历史 + 延迟告警。
                            if let Some((monitor_id, sample_server, sample_delay, avg_delay, up, down)) =
                                self.state.monitor().handle_result(server_id, &result)
                            {
                                self.state
                                    .monitor()
                                    .finalize_window(
                                        monitor_id,
                                        sample_server,
                                        sample_delay,
                                        avg_delay,
                                        up,
                                        down,
                                    )
                                    .await;
                            }
                        }
                        Some(proto::agent_control_request::Body::NatOpenResult(_)) => {
                            tracing::trace!(server_id, "nat open result ignored: NAT is not supported");
                        }
                        Some(proto::agent_control_request::Body::Runtime(agent_runtime)) => {
                            self.upsert_agent_runtime(&hello, &agent_runtime).await?;
                            // sink 路径时延采样：与 Go v2.go 一致，落库失败即断流重连。
                            let db = self.state.db().clone();
                            let node_uuid = hello.node_uuid.clone();
                            let runtime = agent_runtime.clone();
                            tokio::task::spawn_blocking(move || {
                                latency::record_agent_sink_latency(&db, &node_uuid, &runtime)
                            })
                            .await
                            .map_err(|err| Status::internal(format!("record sink latency: {err}")))?
                            .map_err(|err| Status::internal(err.to_string()))?;
                        }
                        Some(proto::agent_control_request::Body::Hello(_)) | None => {}
                    },
                    None => return Ok(()),
                },
                _ = ticker.tick() => {
                    let assignment = self.refreshed_assignment(server_id, &hello).await?;
                    send_control_response(
                        &tx,
                        proto::PrimaryControlResponse {
                            body: Some(proto::primary_control_response::Body::Assignment(assignment)),
                        },
                    )
                    .await?;
                }
            }
        }
    }
}

async fn send_control_response(
    tx: &mpsc::Sender<StdResult<proto::PrimaryControlResponse, Status>>,
    response: proto::PrimaryControlResponse,
) -> StdResult<(), Status> {
    tx.send(Ok(response))
        .await
        .map_err(|err| Status::internal(format!("send control response: {err}")))
}

#[tonic::async_trait]
impl proto::santaizi_control_service_server::SantaiziControlService for ControlHandler {
    type ControlStream = ReceiverStream<StdResult<proto::PrimaryControlResponse, Status>>;

    async fn control(
        &self,
        request: Request<Streaming<proto::AgentControlRequest>>,
    ) -> StdResult<Response<Self::ControlStream>, Status> {
        // 对端身份与秘密元数据须在 `into_inner` 前取出：TLS 扩展与 metadata 都挂在 Request 上。
        let peer = auth::peer_device_identity_result(request.extensions());
        let client_secret = request
            .metadata()
            .get(CLIENT_SECRET_KEY)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let stream = request.into_inner();
        let (tx, rx) = mpsc::channel::<StdResult<proto::PrimaryControlResponse, Status>>(
            CONTROL_RESPONSE_CAPACITY,
        );
        // hello 校验放进任务：响应头随流立即返回，客户端无需等服务端首帧。
        let handler = self.clone();
        tokio::spawn(async move {
            if let Err(status) = handler
                .run_control(peer, client_secret, stream, tx.clone())
                .await
            {
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
