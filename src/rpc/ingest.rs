//! 主端 Ingest 流：对齐 Go `service/rpc/v2.go` 的 `V2Handler.Ingest` 与
//! `runPrimaryIngest`。
//!
//! 首帧 hello 必须声明 `endpoint_id == "primary"`，随后验签凭证、核对
//! hello/凭证/证书三方 node UUID；批次经有界队列交给单 worker 串行落库，
//! 队列满按 Go 同款语义回 ResourceExhausted。快照与 ping 不落证据表。

use std::result::Result as StdResult;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::pki::uri::UriError;
use crate::proto;
use crate::repo::bindings::PRIMARY_OBSERVER_ID;
use crate::rpc::auth;
use crate::rpc::support::blocking_write;
use crate::state::AppState;
use crate::telemetry::offline::OfflineHistoryPolicy;
use crate::telemetry::runtime;
use crate::telemetry::store::{self, IngestResult};

/// 队列容量默认值：对齐 Go `IngestQueueSize` 默认 4096。
const DEFAULT_INGEST_QUEUE_SIZE: usize = 4096;
/// 响应通道容量：acks / pong 顺序送达，吸收突发批次足够。
const INGEST_RESPONSE_CAPACITY: usize = 32;

/// 摄入作业：批次 + 主端接收钟；结果经 oneshot 回传调用方。
struct IngestJob {
    batch: proto::TelemetryBatch,
    received_at_nanos: i64,
    reply: oneshot::Sender<anyhow::Result<IngestResult>>,
}

#[derive(Clone)]
pub struct IngestHandler {
    state: Arc<AppState>,
    queue: mpsc::Sender<IngestJob>,
}

impl IngestHandler {
    pub fn new(state: Arc<AppState>) -> Self {
        let configured = state.conf().telemetry.ingest_queue_size;
        let capacity = if configured == 0 {
            DEFAULT_INGEST_QUEUE_SIZE
        } else {
            configured
        };
        let (queue, jobs) = mpsc::channel(capacity);
        let worker_state = Arc::clone(&state);
        // 单 worker 串行落库：与 Go `go runPrimaryIngest` 同序，天然串行化写事务。
        tokio::spawn(async move { run_ingest_worker(worker_state, jobs).await });
        Self { state, queue }
    }

    /// `enqueuePrimaryIngest`：空批 / 超限 / 队列满都在入队前拒绝。
    async fn enqueue_ingest(
        &self,
        batch: proto::TelemetryBatch,
        received_at_nanos: i64,
    ) -> StdResult<IngestResult, Status> {
        if batch.records.is_empty() {
            return Err(Status::invalid_argument("telemetry batch is empty"));
        }
        let limit = self.state.conf().telemetry.ingest_batch_size;
        if limit > 0 && batch.records.len() > limit {
            return Err(Status::resource_exhausted(format!(
                "telemetry batch exceeds limit {limit}"
            )));
        }
        let (reply, result) = oneshot::channel();
        let job = IngestJob {
            batch,
            received_at_nanos,
            reply,
        };
        self.queue
            .try_send(job)
            .map_err(|_| Status::resource_exhausted("primary telemetry ingest queue is full"))?;
        result
            .await
            .map_err(|_| Status::internal("ingest worker stopped"))?
            // store 的错误都是数据/事务层问题，Go 同样归 InvalidArgument。
            .map_err(|err| Status::invalid_argument(err.to_string()))
    }

    /// `ApplyV2Event`：fresh events 逐条刷运行态。
    async fn apply_event_runtime(
        &self,
        event: &proto::TelemetryEvent,
        received_at_nanos: i64,
    ) -> StdResult<(), Status> {
        let event = event.clone();
        let offline = OfflineHistoryPolicy::from_conf(&self.state.conf());
        blocking_write(self.state.db(), move |tx| {
            runtime::apply_event_runtime(tx, &event, received_at_nanos, offline).map(|_| ())
        })
        .await
        .map_err(|err| Status::internal(err.to_string()))
    }

    /// `ApplyRealtimeSnapshot`：节点必须匹配 hello，超窗快照丢弃。
    async fn apply_realtime_snapshot(
        &self,
        snapshot: &proto::RealtimeSnapshot,
        received_at_nanos: i64,
    ) -> StdResult<(), Status> {
        let snapshot = snapshot.clone();
        let threshold = self.state.conf().telemetry.offline_threshold_seconds;
        let offline = OfflineHistoryPolicy::from_conf(&self.state.conf());
        blocking_write(self.state.db(), move |tx| {
            runtime::apply_realtime_snapshot(tx, &snapshot, received_at_nanos, threshold, offline)
        })
        .await
        .map_err(|err| Status::invalid_argument(err.to_string()))
    }

    /// Ingest 会话：hello 校验 → 逐帧处理 batch / snapshot / ping。
    async fn run_ingest(
        &self,
        peer: Option<StdResult<crate::pki::uri::DeviceIdentity, UriError>>,
        mut stream: Streaming<proto::TelemetryRequest>,
        tx: mpsc::Sender<StdResult<proto::TelemetryResponse, Status>>,
    ) -> StdResult<(), Status> {
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("primary telemetry hello is required"))?;
        let hello = match &first.body {
            Some(proto::telemetry_request::Body::Hello(hello)) => hello.clone(),
            _ => {
                return Err(Status::invalid_argument(
                    "primary telemetry hello is required",
                ));
            }
        };
        if hello.endpoint_id != "primary" {
            return Err(Status::invalid_argument(
                "primary telemetry hello is required",
            ));
        }
        let now_unix = crate::util::time::now_nanos() / 1_000_000_000;
        let verification = match &hello.credential {
            Some(credential) => runtime::verify_ingest_credential(
                self.state.signer(),
                credential,
                now_unix,
                0,
                false,
            )
            .map_err(|err| Status::unauthenticated(err.to_string()))?,
            None => return Err(Status::unauthenticated("telemetry credential is required")),
        };
        if verification.claims.node_uuid != hello.node_uuid {
            return Err(Status::permission_denied(
                "credential node does not match telemetry hello",
            ));
        }
        match_agent_certificate(
            peer.as_ref(),
            &hello.node_uuid,
            &verification.claims.node_uuid,
            self.state.conf().grpc_tls.require_agent_mtls,
        )?;
        let node_uuid = hello.node_uuid;

        while let Some(request) = stream.message().await? {
            let received_at_nanos = crate::util::time::now_nanos();
            match request.body {
                Some(proto::telemetry_request::Body::Batch(batch)) => {
                    // 批次内每条事件都必须挂在已认证身份上：`hello` 的凭证只证明
                    // 「这个连接属于 node X」，不证明「这批数据属于 node X」。
                    // 少了这道校验，任一探针凭据都能伪造任意节点的事件
                    // （RealtimeSnapshot 分支一直有同款校验，Batch 漏了）。
                    for record in &batch.records {
                        if let Some(proto::telemetry_record::Record::Event(event)) = &record.record
                            && event.node_uuid != node_uuid
                        {
                            return Err(Status::permission_denied("telemetry batch node mismatch"));
                        }
                    }
                    let result = self.enqueue_ingest(batch, received_at_nanos).await?;
                    for event in &result.fresh_events {
                        self.apply_event_runtime(event, received_at_nanos).await?;
                    }
                    send_response(
                        &tx,
                        proto::TelemetryResponse {
                            acks: result.acks,
                            ..Default::default()
                        },
                    )
                    .await?;
                }
                Some(proto::telemetry_request::Body::RealtimeSnapshot(snapshot)) => {
                    if snapshot.node_uuid != node_uuid {
                        return Err(Status::permission_denied("realtime snapshot node mismatch"));
                    }
                    self.apply_realtime_snapshot(&snapshot, received_at_nanos)
                        .await?;
                }
                Some(proto::telemetry_request::Body::Ping(_)) => {
                    send_response(
                        &tx,
                        proto::TelemetryResponse {
                            pong: Some(proto::TelemetryPong {}),
                            ..Default::default()
                        },
                    )
                    .await?;
                }
                _ => return Err(Status::invalid_argument("unexpected telemetry request")),
            }
        }
        Ok(())
    }
}

/// `runPrimaryIngest`：单 worker 串行消费摄入队列。
async fn run_ingest_worker(state: Arc<AppState>, mut jobs: mpsc::Receiver<IngestJob>) {
    let grace_nanos = hole_grace_nanos(&state.conf());
    let store = Arc::clone(state.telemetry_store());
    while let Some(job) = jobs.recv().await {
        let store = Arc::clone(&store);
        let outcome = blocking_write(state.db(), move |tx| {
            store.ingest(
                tx,
                &job.batch,
                PRIMARY_OBSERVER_ID,
                job.received_at_nanos,
                grace_nanos,
            )
        })
        .await;
        let _ = job.reply.send(outcome);
    }
}

/// `sequenceHoleGrace`：0 = 默认 300s；<0 = 关闭（立即停在空洞前）。
fn hole_grace_nanos(conf: &crate::config::Config) -> i64 {
    let seconds = conf.telemetry.sequence_hole_grace_seconds;
    if seconds == 0 {
        return store::DEFAULT_SEQUENCE_HOLE_GRACE_SECONDS * 1_000_000_000;
    }
    if seconds < 0 {
        return 0;
    }
    seconds * 1_000_000_000
}

/// `matchAgentCertificate`：无证书且不强制 mTLS 时放行；有证书则
/// kind 必须是 agent 且 UUID 同时匹配 hello 与凭证。
fn match_agent_certificate(
    peer: Option<&StdResult<crate::pki::uri::DeviceIdentity, UriError>>,
    hello_uuid: &[u8],
    credential_uuid: &[u8],
    require_agent_mtls: bool,
) -> StdResult<(), Status> {
    let Some(peer) = peer else {
        if require_agent_mtls {
            return Err(Status::unauthenticated("agent certificate is required"));
        }
        return Ok(());
    };
    let identity = peer
        .as_ref()
        .map_err(|err| Status::permission_denied(err.to_string()))?;
    if identity.kind != crate::pki::uri::DeviceKind::Agent {
        return Err(Status::permission_denied(
            "ingest requires an agent certificate",
        ));
    }
    if identity.node_uuid != hello_uuid || identity.node_uuid != credential_uuid {
        return Err(Status::permission_denied(
            "certificate UUID does not match hello or credential",
        ));
    }
    Ok(())
}

async fn send_response(
    tx: &mpsc::Sender<StdResult<proto::TelemetryResponse, Status>>,
    response: proto::TelemetryResponse,
) -> StdResult<(), Status> {
    tx.send(Ok(response))
        .await
        .map_err(|err| Status::internal(format!("send telemetry response: {err}")))
}

#[tonic::async_trait]
impl proto::santaizi_telemetry_service_server::SantaiziTelemetryService for IngestHandler {
    type IngestStream = ReceiverStream<StdResult<proto::TelemetryResponse, Status>>;

    async fn ingest(
        &self,
        request: Request<Streaming<proto::TelemetryRequest>>,
    ) -> StdResult<Response<Self::IngestStream>, Status> {
        // 对端身份须在 `into_inner` 前取出：TLS 扩展挂在 Request 上。
        let peer = auth::peer_device_identity_result(request.extensions());
        let stream = request.into_inner();
        let (tx, rx) =
            mpsc::channel::<StdResult<proto::TelemetryResponse, Status>>(INGEST_RESPONSE_CAPACITY);
        // hello 校验放进任务：响应头随流立即返回，与 Go `SendHeader(nil)` 等价，
        // 避免 tonic 客户端因等服务端首帧而阻塞在开流调用上。
        let handler = self.clone();
        tokio::spawn(async move {
            if let Err(status) = handler.run_ingest(peer, stream, tx.clone()).await {
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
