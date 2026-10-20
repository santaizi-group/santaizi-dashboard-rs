//! Enrollment 服务：对齐 Go `service/rpc/enroll.go`。
//!
//! Enroll 走 `client_secret` 元数据认证并在需要时换绑；Renew 走 agent 证书
//! mTLS 认证，按证书内节点 UUID 续签，不触碰绑定关系。

use std::sync::Arc;

use time::OffsetDateTime;
use tonic::{Request, Response, Status};

use crate::pki::uri::DeviceKind;
use crate::proto;
use crate::repo::bindings::{self, BindingError};
use crate::rpc::auth;
use crate::rpc::support::{blocking_read, blocking_write};
use crate::state::AppState;
const CLIENT_SECRET_KEY: &str = "client_secret";

pub struct EnrollmentHandler {
    state: Arc<AppState>,
}

impl EnrollmentHandler {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    /// `authHandler.Check`：`client_secret` 元数据 → server_id。
    fn check_client_secret<T>(&self, req: &Request<T>) -> std::result::Result<i64, Status> {
        let secret = req
            .metadata()
            .get(CLIENT_SECRET_KEY)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        match self.state.secrets().server_id(secret) {
            Some(id) => Ok(id),
            None => Err(Status::unauthenticated("客户端认证失败")),
        }
    }

    async fn issue_agent_certificate(
        &self,
        server_id: i64,
        node_uuid: &[u8],
        csr_der: &[u8],
        bind: bool,
    ) -> std::result::Result<Response<proto::AgentEnrollResponse>, Status> {
        if node_uuid.len() != 16 {
            return Err(Status::invalid_argument("node UUID must be 16 bytes"));
        }
        if csr_der.is_empty() {
            return Err(Status::invalid_argument("csr_der is required"));
        }
        let bundle = self
            .state
            .pki()
            .ok_or_else(|| Status::failed_precondition("agent CA is not available"))?;
        if bind {
            let available = blocking_read(self.state.db(), {
                let node_uuid = node_uuid.to_vec();
                move |conn| {
                    bindings::ensure_server_node_available_for_enroll(conn, server_id, &node_uuid)
                }
            })
            .await;
            match available {
                Ok(()) => {}
                // 已绑到别的节点：Go 在此检查控制会话是否在线（Phase 3 运行态），
                // 当前无在线会话即允许换绑。
                Err(err) if is_bound_to_other_node(&err) => {}
                Err(err) => return Err(Status::internal(err.to_string())),
            }
            let changed = blocking_write(self.state.db(), {
                let node_uuid = node_uuid.to_vec();
                let now_nanos = crate::util::time::now_nanos();
                move |tx| {
                    bindings::bind_server_node_for_protocol(
                        tx, server_id, &node_uuid, now_nanos, true,
                    )
                }
            })
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
            if changed {
                tracing::info!(server_id, "agent 换绑完成");
            }
        }
        let (cert_pem, not_before, not_after) = crate::pki::sign_agent_csr(
            &bundle.agent,
            csr_der,
            node_uuid,
            OffsetDateTime::now_utc(),
        )
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
        Ok(Response::new(proto::AgentEnrollResponse {
            certificate_pem: String::from_utf8_lossy(&cert_pem).into_owned(),
            ca_certificate_pem: String::from_utf8_lossy(&bundle.agent.cert_pem).into_owned(),
            not_before_unix: not_before.unix_timestamp(),
            expires_at_unix: not_after.unix_timestamp(),
        }))
    }
}

fn is_bound_to_other_node(err: &anyhow::Error) -> bool {
    matches!(
        err.downcast_ref::<BindingError>(),
        Some(BindingError::BoundToOtherNode)
    )
}

#[tonic::async_trait]
impl proto::santaizi_enrollment_service_server::SantaiziEnrollmentService for EnrollmentHandler {
    async fn enroll(
        &self,
        request: Request<proto::AgentEnrollRequest>,
    ) -> std::result::Result<Response<proto::AgentEnrollResponse>, Status> {
        if !auth::connection_is_tls(request.extensions()) {
            return Err(Status::unauthenticated("enrollment requires TLS"));
        }
        let server_id = self.check_client_secret(&request)?;
        self.issue_agent_certificate(
            server_id,
            &request.get_ref().node_uuid,
            &request.get_ref().csr_der,
            true,
        )
        .await
    }

    async fn renew(
        &self,
        request: Request<proto::AgentRenewRequest>,
    ) -> std::result::Result<Response<proto::AgentEnrollResponse>, Status> {
        let identity = auth::peer_identity(request.extensions())
            .ok_or_else(|| Status::unauthenticated("agent certificate is required"))?;
        if identity.kind != DeviceKind::Agent {
            return Err(Status::unauthenticated("agent certificate is required"));
        }
        if identity.node_uuid != request.get_ref().node_uuid {
            return Err(Status::permission_denied(
                "certificate UUID does not match request",
            ));
        }
        let server_id = blocking_read(self.state.db(), {
            let node_uuid = identity.node_uuid.clone();
            move |conn| bindings::server_id_from_node_uuid(conn, &node_uuid)
        })
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| Status::unauthenticated("node is not bound to a server"))?;
        self.issue_agent_certificate(
            server_id,
            &identity.node_uuid,
            &request.get_ref().csr_der,
            false,
        )
        .await
    }
}
