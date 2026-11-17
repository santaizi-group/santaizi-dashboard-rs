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
                // 已绑到别的节点：若原绑定节点的 Control 流当前在线则拒绝抢绑，
                // 仅在原节点离线时允许换绑（对齐 Go `enroll.go:66`）。
                Err(err) if is_bound_to_other_node(&err) => {
                    if self
                        .state
                        .monitor()
                        .session_node_conflict(server_id, node_uuid)
                    {
                        return Err(Status::failed_precondition(
                            "server is already bound to an active agent",
                        ));
                    }
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tonic::Code;

    #[tokio::test]
    async fn enroll_blocks_rebind_when_existing_node_control_session_is_online() {
        crate::util::tls::install_crypto_provider();
        let dir = std::env::temp_dir().join(format!(
            "santaizi-enroll-conflict-{}-{}",
            std::process::id(),
            crate::util::time::now_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db = crate::db::migrate::open_primary(dir.join("panel.db").to_str().unwrap()).unwrap();
        let mut conf = Config::default();
        conf.telemetry.data_dir = dir.to_string_lossy().into_owned();
        conf.telemetry.signing_key_path = dir.join("sign.key").to_string_lossy().into_owned();
        conf.telemetry.secret_key_path = dir.join("secret.key").to_string_lossy().into_owned();
        let state = AppState::primary(conf, db.clone()).await.unwrap();
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, secret_ciphertext) VALUES (7, 'node-7', X'01')",
                [],
            )?;
            Ok(())
        })
        .unwrap();

        let handler = EnrollmentHandler::new(Arc::clone(&state));
        let node_a = vec![0xAAu8; 16];
        let node_b = vec![0xBBu8; 16];
        let key_a = crate::pki::client::generate_key().unwrap();
        let csr_a =
            crate::pki::client::create_csr(&key_a, &crate::pki::uri::encode_agent_uri(&node_a))
                .unwrap();
        let key_b = crate::pki::client::generate_key().unwrap();
        let csr_b =
            crate::pki::client::create_csr(&key_b, &crate::pki::uri::encode_agent_uri(&node_b))
                .unwrap();

        // 首次绑定 node_a 成功。
        handler
            .issue_agent_certificate(7, &node_a, &csr_a, true)
            .await
            .unwrap();

        // node_a 建立在线 Control 会话期间，node_b 尝试换绑必须被拒为 FailedPrecondition。
        let (tx, _rx) = tokio::sync::mpsc::channel(2);
        state
            .monitor()
            .register_session(7, node_a.clone(), vec![], tx);
        let err = handler
            .issue_agent_certificate(7, &node_b, &csr_b, true)
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert_eq!(err.message(), "server is already bound to an active agent");
        let bound = db
            .read(|conn| bindings::server_id_from_node_uuid(conn, &node_a))
            .unwrap();
        assert_eq!(bound, Some(7), "原在线绑定不得被覆盖");

        // node_a 离线断流后，node_b 允许正常换绑。
        state.monitor().unregister_session(7, &node_a);
        handler
            .issue_agent_certificate(7, &node_b, &csr_b, true)
            .await
            .unwrap();
        let bound_b = db
            .read(|conn| bindings::server_id_from_node_uuid(conn, &node_b))
            .unwrap();
        assert_eq!(bound_b, Some(7));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
