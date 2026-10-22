//! Collector 服务：对齐 Go `service/rpc/collector_primary.go`。
//!
//! Register/RenewCollector 经注册令牌或 mTLS 证书签发 collector 证书；
//! Sync 首帧 hello 后推送授权配置，随后循环接收 runtime / probe 样本；
//! Replicate 摄入从端复制批次（回执去重 + 运行态刷新 + 时延采样）。
//! 探测样本写入分钟桶、最新值、MTR 与路由，并在运行态帧之后下发待执行路由任务。

use std::result::Result as StdResult;
use std::sync::Arc;

use time::OffsetDateTime;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::pki::uri::{DeviceIdentity, DeviceKind, UriError};
use crate::proto;
use crate::repo::bindings;
use crate::repo::collectors::{self, CollectorRow};
use crate::rpc::auth;
use crate::rpc::support::{blocking_read, blocking_write};
use crate::state::AppState;
use crate::telemetry::latency;
use crate::telemetry::offline::OfflineHistoryPolicy;
use crate::telemetry::probe_ingest;
use crate::telemetry::probes;
use crate::telemetry::runtime;
use crate::telemetry::store;

/// Sync 响应队列容量：与 Go `IngestQueueSize` 量级一致，足以吸收突发 runtime 帧。
const SYNC_CHANNEL_CAPACITY: usize = 32;

#[derive(Clone)]
pub struct CollectorHandler {
    state: Arc<AppState>,
}

impl CollectorHandler {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    fn agent_ca_pem(&self) -> String {
        self.state
            .pki()
            .map(|bundle| String::from_utf8_lossy(&bundle.agent.cert_pem).into_owned())
            .unwrap_or_default()
    }

    /// `findCollectorByToken`：空令牌与无匹配都归 Unauthenticated。
    async fn find_collector_by_token(&self, token: &str) -> StdResult<CollectorRow, Status> {
        if token.is_empty() {
            return Err(Status::unauthenticated("collector token is required"));
        }
        let token = token.to_string();
        blocking_read(self.state.db(), move |conn| {
            collectors::find_by_token(conn, &token)
        })
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| Status::unauthenticated("collector token is invalid or revoked"))
    }

    /// `findCollectorByUUID`：吊销/删除的 collector 视同不存在。
    async fn find_collector_by_uuid(
        &self,
        collector_uuid: &str,
    ) -> StdResult<CollectorRow, Status> {
        if collector_uuid.is_empty() {
            return Err(Status::unauthenticated("collector UUID is required"));
        }
        let uuid = collector_uuid.to_string();
        blocking_read(self.state.db(), move |conn| {
            collectors::find_by_uuid(conn, &uuid)
        })
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| Status::unauthenticated("collector certificate is invalid or revoked"))
    }

    /// `identifyCollector`：有证书走 collector mTLS 身份，否则走注册令牌。
    async fn identify_collector(
        &self,
        peer: Option<&StdResult<DeviceIdentity, UriError>>,
        collector_uuid: &str,
        token: &str,
    ) -> StdResult<CollectorRow, Status> {
        match peer {
            Some(Err(err)) => Err(Status::permission_denied(err.to_string())),
            Some(Ok(ident)) => {
                if ident.kind != DeviceKind::Collector {
                    return Err(Status::permission_denied(
                        "collector certificate is required",
                    ));
                }
                if !collector_uuid.is_empty() && ident.collector_uuid != collector_uuid {
                    return Err(Status::permission_denied(
                        "certificate UUID does not match hello",
                    ));
                }
                self.find_collector_by_uuid(&ident.collector_uuid).await
            }
            None => {
                let collector = self.find_collector_by_token(token).await?;
                if !collector_uuid.is_empty() && collector.collector_uuid != collector_uuid {
                    return Err(Status::permission_denied(
                        "collector token identity mismatch",
                    ));
                }
                Ok(collector)
            }
        }
    }

    /// `sendCollectorConfig`：observer 推指派清单，probe 推探针配置与已解析目标。
    async fn send_collector_config(
        &self,
        tx: &tokio::sync::mpsc::Sender<StdResult<proto::CollectorSyncResponse, Status>>,
        collector: &CollectorRow,
    ) -> StdResult<(), Status> {
        let (assignments, revoked) = if collector.is_probe() {
            (Vec::new(), Vec::new())
        } else {
            let observer_id = collector.collector_uuid.clone();
            let observer_for_revoke = observer_id.clone();
            let (assignments, revoked) = tokio::join!(
                blocking_read(self.state.db(), move |conn| {
                    bindings::assignments_for_observer(conn, &observer_id)
                }),
                blocking_read(self.state.db(), move |conn| {
                    bindings::revoked_node_uuids_for_observer(conn, &observer_for_revoke)
                }),
            );
            (
                assignments.map_err(|err| Status::internal(err.to_string()))?,
                revoked.map_err(|err| Status::internal(err.to_string()))?,
            )
        };
        let targets = if collector.is_probe() {
            let row = collector.clone();
            blocking_read(self.state.db(), move |conn| {
                probe_ingest::build_probe_targets(conn, &row)
            })
            .await
            .map_err(|err| Status::internal(err.to_string()))?
        } else {
            Vec::new()
        };
        let config = proto::CollectorAuthorizationConfig {
            config_version: collector.config_version,
            primary_public_key: self.state.signer().public_key(),
            key_id: self.state.signer().key_id(),
            // 已终结的绑定节点：从端据此把吊销缓存填实，`IsNodeAuthorized`
            // 的吊销分支才不是死代码（此前恒为空表）。
            revoked_node_uuids: revoked,
            assignments: assignments
                .into_iter()
                .map(|row| proto::NodeAssignment {
                    node_uuid: row.node_uuid,
                    observer_id: row.observer_id,
                    valid_from_unix_nano: row.valid_from,
                    valid_to_unix_nano: row.valid_to,
                    generation: to_u64(row.generation),
                    config_version: to_u64(row.config_version),
                })
                .collect(),
            agent_ca_certificate_pem: self.agent_ca_pem(),
            kind: probes::proto_collector_kind(&collector.kind),
            probe: Some(probes::probe_config_from_collector(collector)),
            targets,
        };
        tx.send(Ok(proto::CollectorSyncResponse {
            body: Some(proto::collector_sync_response::Body::Config(config)),
        }))
        .await
        .map_err(|err| Status::internal(format!("send collector config: {err}")))
    }

    async fn send_accepted(
        tx: &tokio::sync::mpsc::Sender<StdResult<proto::CollectorSyncResponse, Status>>,
    ) -> StdResult<(), Status> {
        tx.send(Ok(proto::CollectorSyncResponse {
            body: Some(proto::collector_sync_response::Body::Accepted(true)),
        }))
        .await
        .map_err(|err| Status::internal(format!("send ack: {err}")))
    }

    /// `saveCollectorRuntime` + 版本变更重推配置。
    async fn handle_runtime(
        &self,
        collector: &mut CollectorRow,
        runtime: &proto::CollectorRuntime,
        tx: &tokio::sync::mpsc::Sender<StdResult<proto::CollectorSyncResponse, Status>>,
    ) -> StdResult<(), Status> {
        let now_nanos = crate::util::time::now_nanos();
        let row = collectors::CollectorRuntimeRow::from_proto(&collector.collector_uuid, runtime);
        // 心跳 / 复制 RTT 时延样本：与 Go `UpsertCollectorRuntime` 尾部的
        // `RecordCollectorLatency` 一致；先取快照，供事务提交后另起采样写。
        let latency_input = latency::CollectorLatencyInput {
            collector_uuid: row.collector_uuid.clone(),
            heartbeat_rtt_ms: row.heartbeat_rtt_ms,
            heartbeat_rtt_sampled_at: row.heartbeat_rtt_sampled_at,
            replication_rtt_ms: row.replication_rtt_ms,
            replication_rtt_sampled_at: row.replication_rtt_sampled_at,
        };
        blocking_write(self.state.db(), move |tx| {
            collectors::upsert_runtime(tx, &row, now_nanos, false)
        })
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
        let db = self.state.db().clone();
        tokio::task::spawn_blocking(move || latency::record_collector_latency(&db, &latency_input))
            .await
            .map_err(|err| Status::internal(format!("record collector latency: {err}")))?
            .map_err(|err| Status::internal(err.to_string()))?;
        let current = self
            .find_collector_by_uuid(&collector.collector_uuid)
            .await?;
        if current.config_version != collector.config_version {
            *collector = current;
            self.send_collector_config(tx, collector).await?;
        } else {
            Self::send_accepted(tx).await?;
        }
        Ok(())
    }

    /// `IngestProbeSamples`：落库后发告警，再回执 accepted。
    async fn handle_probe_samples(
        &self,
        collector: &CollectorRow,
        batch: proto::ProbeSampleBatch,
        tx: &tokio::sync::mpsc::Sender<StdResult<proto::CollectorSyncResponse, Status>>,
    ) -> StdResult<(), Status> {
        // 探测样本批次大小同样由对端决定，不设上限时一次超大批次就能把
        // 主端探测写入拖成一个超长事务。与 Ingest / Replicate 同一处置。
        let limit = self.state.conf().telemetry.probe_batch_size;
        if limit > 0 && batch.samples.len() > limit {
            return Err(Status::resource_exhausted(format!(
                "probe batch exceeds limit {limit}"
            )));
        }
        let collector = collector.clone();
        let now_nanos = crate::util::time::now_nanos();
        let notices = blocking_write(self.state.db(), move |conn| {
            probe_ingest::ingest_probe_samples(conn, &collector, &batch, now_nanos)
        })
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
        let notifier = self.state.notifier();
        for notice in notices {
            notifier
                .send(
                    &notice.tag,
                    &notice.message,
                    Some(notice.mute_label),
                    Some(notice.server_id),
                )
                .await;
        }
        Self::send_accepted(tx).await
    }

    /// 运行态帧之后把该从端尚未领取的路由任务推下去。
    async fn send_pending_route_jobs(
        &self,
        collector: &CollectorRow,
        tx: &tokio::sync::mpsc::Sender<StdResult<proto::CollectorSyncResponse, Status>>,
    ) -> StdResult<(), Status> {
        if !collector.is_probe() {
            return Ok(());
        }
        let collector_uuid = collector.collector_uuid.clone();
        let now_nanos = crate::util::time::now_nanos();
        let jobs = blocking_write(self.state.db(), move |conn| {
            probe_ingest::pending_route_jobs(conn, &collector_uuid, now_nanos)
        })
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
        if jobs.is_empty() {
            return Ok(());
        }
        tx.send(Ok(proto::CollectorSyncResponse {
            body: Some(proto::collector_sync_response::Body::RouteJobs(
                proto::ProbeRouteJobBatch { jobs },
            )),
        }))
        .await
        .map_err(|err| Status::internal(format!("send route jobs: {err}")))
    }

    /// `Replicate`：识别 collector 后逐帧摄入复制批次并回执。
    ///
    /// 对齐 Go `PrimaryCollectorHandler.Replicate`：store 事务（回执去重 + 全量落库）
    /// → 时钟可信事件逐条刷运行态（`ApplyTrustedTelemetryEvents`）→ 回执帧。
    /// 重复投递也刷新：touch 按序号去重，不会把运行态倒退。
    async fn run_replicate(
        &self,
        peer: Option<StdResult<DeviceIdentity, UriError>>,
        token: String,
        mut stream: Streaming<proto::ReplicationBatch>,
        tx: tokio::sync::mpsc::Sender<StdResult<proto::ReplicationAck, Status>>,
    ) -> StdResult<(), Status> {
        let collector = self.identify_collector(peer.as_ref(), "", &token).await?;
        let store = Arc::clone(self.state.telemetry_store());
        let offline = OfflineHistoryPolicy::from_conf(&self.state.conf());
        while let Some(batch) = stream.message().await? {
            if batch.collector_uuid != collector.collector_uuid {
                return Err(Status::permission_denied(
                    "replication collector identity mismatch",
                ));
            }
            // 批次头绑定身份只证明「这条流是谁开的」；事件里的 node_uuid 仍可任填。
            // 逐个比对在效指派，否则一条 collector 凭据就能伪报任意节点。
            let observer_id = collector.collector_uuid.clone();
            let assigned = blocking_read(self.state.db(), move |conn| {
                bindings::assigned_node_uuids(conn, &observer_id)
            })
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
            for event in &batch.events {
                if !assigned.contains(&event.node_uuid) {
                    return Err(Status::permission_denied(
                        "replication node is not assigned to this collector",
                    ));
                }
            }
            // 批次上限：复制流的批次大小完全由对端决定，不设上限时一个超大批次
            // 既能撑爆主端内存，又把写事务拖到超过 busy_timeout。与 Ingest 的
            // `ingest_batch_size` 同一处置（超限即 ResourceExhausted）。
            let limit = self.state.conf().telemetry.replication_batch_size;
            if limit > 0 && batch.events.len() > limit {
                return Err(Status::resource_exhausted(format!(
                    "replication batch exceeds limit {limit}"
                )));
            }
            let received_at_nanos = crate::util::time::now_nanos();
            let committed = {
                let store = Arc::clone(&store);
                let batch = batch.clone();
                blocking_write(self.state.db(), move |tx| {
                    store.replicate(tx, &batch, received_at_nanos)
                })
                .await
                // store 的错误都是数据/事务层问题，Go 同样归 InvalidArgument。
                .map_err(|err| Status::invalid_argument(err.to_string()))?
            };
            // 运行态刷新合进一个事务：逐事件一开事务会把批次耗时放大到事件数倍，
            // 在 busy_timeout 内排不上写锁。
            let events: Vec<proto::TelemetryEvent> = batch
                .events
                .iter()
                .filter(|event| {
                    event.node_uuid.len() == 16
                        && (received_at_nanos - event.collected_at_unix_nano).abs()
                            <= store::CLOCK_SKEW_LIMIT_NANOS
                })
                .cloned()
                .collect();
            blocking_write(self.state.db(), move |tx| {
                for event in &events {
                    runtime::apply_event_runtime(tx, event, received_at_nanos, offline)?;
                }
                Ok(())
            })
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
            // 复制 RTT 时延样本：与 Go `UpsertCollectorRuntime` 尾部一致，
            // 仅 runtime 帧携带 RTT 时落桶。
            if let Some(runtime) = &batch.runtime {
                let latency_input = latency::CollectorLatencyInput {
                    collector_uuid: batch.collector_uuid.clone(),
                    heartbeat_rtt_ms: runtime.heartbeat_rtt_ms,
                    heartbeat_rtt_sampled_at: runtime.heartbeat_rtt_sampled_at_unix_nano,
                    replication_rtt_ms: runtime.replication_rtt_ms,
                    replication_rtt_sampled_at: runtime.replication_rtt_sampled_at_unix_nano,
                };
                let db = self.state.db().clone();
                tokio::task::spawn_blocking(move || {
                    latency::record_collector_latency(&db, &latency_input)
                })
                .await
                .map_err(|err| Status::internal(format!("record collector latency: {err}")))?
                .map_err(|err| Status::internal(err.to_string()))?;
            }
            tx.send(Ok(proto::ReplicationAck {
                collector_uuid: batch.collector_uuid,
                replication_session: batch.replication_session,
                batch_sequence: batch.batch_sequence,
                committed_spool_through_id: committed,
                error: String::new(),
            }))
            .await
            .map_err(|err| Status::internal(format!("send replication ack: {err}")))?;
        }
        Ok(())
    }

    /// Sync 主循环：hello 后先推配置，再逐帧处理 runtime / probe 样本。
    async fn run_sync(
        &self,
        mut collector: CollectorRow,
        mut stream: Streaming<proto::CollectorSyncRequest>,
        tx: tokio::sync::mpsc::Sender<StdResult<proto::CollectorSyncResponse, Status>>,
    ) -> StdResult<(), Status> {
        self.send_collector_config(&tx, &collector).await?;
        while let Some(request) = stream.message().await? {
            match request.body {
                Some(proto::collector_sync_request::Body::Runtime(runtime)) => {
                    self.handle_runtime(&mut collector, &runtime, &tx).await?;
                    self.send_pending_route_jobs(&collector, &tx).await?;
                }
                Some(proto::collector_sync_request::Body::ProbeSamples(batch)) => {
                    self.handle_probe_samples(&collector, batch, &tx).await?;
                }
                Some(proto::collector_sync_request::Body::Hello(_)) | None => {}
            }
        }
        Ok(())
    }

    async fn sign_collector_certificate(
        &self,
        collector_uuid: &str,
        csr_der: &[u8],
    ) -> StdResult<proto::CollectorEnrollResponse, Status> {
        let bundle = self
            .state
            .pki()
            .ok_or_else(|| Status::failed_precondition("collector CA is not available"))?;
        let (cert_pem, not_before, not_after) = crate::pki::sign_collector_csr(
            &bundle.collector,
            csr_der,
            collector_uuid,
            OffsetDateTime::now_utc(),
        )
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
        Ok(proto::CollectorEnrollResponse {
            certificate_pem: String::from_utf8_lossy(&cert_pem).into_owned(),
            ca_certificate_pem: String::from_utf8_lossy(&bundle.collector.cert_pem).into_owned(),
            agent_ca_certificate_pem: self.agent_ca_pem(),
            not_before_unix: not_before.unix_timestamp(),
            expires_at_unix: not_after.unix_timestamp(),
        })
    }
}

/// DB 整数列 → proto uint64：负值夹到 0，避免静默回绕。
fn to_u64(value: i64) -> u64 {
    value.clamp(0, i64::MAX) as u64
}

/// 注册失败节流键（进程级单桶）。
const REGISTRATION_THROTTLE_KEY: &str = "collector-registration";

/// 注册节流器：进程内唯一，随 handler 复用。
fn registration_throttle() -> &'static crate::api::rate_limit::FailureThrottle {
    static THROTTLE: std::sync::OnceLock<crate::api::rate_limit::FailureThrottle> =
        std::sync::OnceLock::new();
    THROTTLE.get_or_init(crate::api::rate_limit::FailureThrottle::default)
}

#[tonic::async_trait]
impl proto::santaizi_collector_service_server::SantaiziCollectorService for CollectorHandler {
    type SyncStream = ReceiverStream<StdResult<proto::CollectorSyncResponse, Status>>;

    async fn register(
        &self,
        request: Request<proto::RegisterCollectorRequest>,
    ) -> StdResult<Response<proto::RegisterCollectorResponse>, Status> {
        let req = request.get_ref();
        // 注册令牌是长期凭据，明文走非 TLS 连接等于把它直接暴露给同网段。
        // 与 `Enroll` 同一准入门槛（见 `rpc/enroll.rs`）。
        if !auth::connection_is_tls(request.extensions()) {
            return Err(Status::unauthenticated(
                "collector registration requires TLS",
            ));
        }
        // 注册令牌是长期共享秘密：与查看密码同型，必须限流才不会被离线爆破。
        // 进程级单桶——秘密只有一个，按 IP 计数对换 IP 爆破形同虚设。
        let throttle = registration_throttle();
        if let Err(retry) = throttle.check(REGISTRATION_THROTTLE_KEY) {
            return Err(Status::resource_exhausted(format!(
                "too many failed registration attempts; retry in {}s",
                retry.seconds()
            )));
        }
        let collector = match self.find_collector_by_token(&req.registration_token).await {
            Ok(collector) => {
                throttle.record_success(REGISTRATION_THROTTLE_KEY);
                collector
            }
            Err(status) => {
                throttle.record_failure(REGISTRATION_THROTTLE_KEY);
                return Err(status);
            }
        };
        let mut response = proto::RegisterCollectorResponse {
            collector_uuid: collector.collector_uuid.clone(),
            primary_public_key: self.state.signer().public_key(),
            key_id: self.state.signer().key_id(),
            config_version: collector.config_version,
            agent_ca_certificate_pem: self.agent_ca_pem(),
            ..Default::default()
        };
        if !req.csr_der.is_empty() {
            let bundle = self
                .state
                .pki()
                .ok_or_else(|| Status::failed_precondition("collector CA is not available"))?;
            let (cert_pem, not_before, not_after) = crate::pki::sign_collector_csr(
                &bundle.collector,
                &req.csr_der,
                &collector.collector_uuid,
                OffsetDateTime::now_utc(),
            )
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
            response.collector_certificate_pem = String::from_utf8_lossy(&cert_pem).into_owned();
            response.collector_ca_certificate_pem =
                String::from_utf8_lossy(&bundle.collector.cert_pem).into_owned();
            response.not_before_unix = not_before.unix_timestamp();
            response.expires_at_unix = not_after.unix_timestamp();
        }
        Ok(Response::new(response))
    }

    async fn sync(
        &self,
        request: Request<Streaming<proto::CollectorSyncRequest>>,
    ) -> StdResult<Response<Self::SyncStream>, Status> {
        // 对端身份须在 `into_inner` 前取出：TLS 扩展挂在 Request 上。
        let peer = auth::peer_device_identity_result(request.extensions());
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("collector sync hello is required"))?;
        let hello = match first.body {
            Some(proto::collector_sync_request::Body::Hello(hello)) => hello,
            _ => return Err(Status::invalid_argument("collector sync hello is required")),
        };
        let collector = self
            .identify_collector(
                peer.as_ref(),
                &hello.collector_uuid,
                &hello.registration_token,
            )
            .await?;

        let (tx, rx) = tokio::sync::mpsc::channel::<StdResult<proto::CollectorSyncResponse, Status>>(
            SYNC_CHANNEL_CAPACITY,
        );
        let handler = self.clone();
        tokio::spawn(async move {
            if let Err(status) = handler.run_sync(collector, stream, tx.clone()).await {
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_status(
        &self,
        _request: Request<proto::CollectorStatusRequest>,
    ) -> StdResult<Response<proto::CollectorStatus>, Status> {
        // Go 主端同样不实现 GetStatus（状态由 collector 从端 Runtime 提供）。
        Err(Status::unimplemented(
            "collector status is served by the collector runtime",
        ))
    }

    async fn renew_collector(
        &self,
        request: Request<proto::CollectorRenewRequest>,
    ) -> StdResult<Response<proto::CollectorEnrollResponse>, Status> {
        let identity = match auth::peer_device_identity_result(request.extensions()) {
            Some(Err(err)) => return Err(Status::permission_denied(err.to_string())),
            Some(Ok(ident)) => ident,
            None => return Err(Status::unauthenticated("collector certificate is required")),
        };
        if identity.kind != DeviceKind::Collector {
            return Err(Status::unauthenticated("collector certificate is required"));
        }
        if identity.collector_uuid != request.get_ref().collector_uuid {
            return Err(Status::permission_denied(
                "certificate UUID does not match request",
            ));
        }
        self.find_collector_by_uuid(&identity.collector_uuid)
            .await?;
        let response = self
            .sign_collector_certificate(&identity.collector_uuid, &request.get_ref().csr_der)
            .await?;
        Ok(Response::new(response))
    }
}

#[tonic::async_trait]
impl proto::santaizi_replication_service_server::SantaiziReplicationService for CollectorHandler {
    type ReplicateStream = ReceiverStream<StdResult<proto::ReplicationAck, Status>>;

    async fn replicate(
        &self,
        request: Request<Streaming<proto::ReplicationBatch>>,
    ) -> StdResult<Response<Self::ReplicateStream>, Status> {
        // 对端身份与令牌须在 `into_inner` 前取出：TLS 扩展与 metadata 都挂在 Request 上。
        let peer = auth::peer_device_identity_result(request.extensions());
        let token = request
            .metadata()
            .get("collector_token")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel::<StdResult<proto::ReplicationAck, Status>>(
            SYNC_CHANNEL_CAPACITY,
        );
        let handler = self.clone();
        tokio::spawn(async move {
            if let Err(status) = handler.run_replicate(peer, token, stream, tx.clone()).await {
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
