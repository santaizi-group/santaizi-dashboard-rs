//! 旧版 `SantaiziService`（v1 unary）服务：对齐 Go `service/rpc/santaizi.go`。
//!
//! v1 探针（Go / Rust 探针的 `client_secret` 回退通路）只会 ReportSystemInfo /
//! ReportSystemState，V2 探针不调用这三个方法。Rust 版面板此前没挂这个服务，
//! `client_secret` 回退路径拿到的是 `Unimplemented`——探针会误判成「旧面板」
//! 而永远卡在密钥认证上；这里补齐 Go 同款语义，让互通回到「密钥认证可用、
//! 运行态/离线历史正常」的旧面板形态。
//!
//! 与 Go 的差异只有一处：Go 把 ServerList 里的 `Host`/`State` 内存缓存一并
//! 更新，Rust 版读侧走 `host_state`/`state_payload`（`ApplyV2Event` 同一套
//! 运行态列），不维护第二份内存副本。

use std::result::Result as StdResult;

use prost::Message as _;
use rusqlite::OptionalExtension as _;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::proto;
use crate::repo::bindings;
use crate::rpc::support::blocking_write;
use crate::state::AppState;
use crate::telemetry::offline::OfflineHistoryPolicy;
use crate::telemetry::runtime;
use crate::util::time as util_time;

/// `authHandler.Check`：`client_secret` → server_id；失败统一 Unauthenticated。
fn server_id_from_metadata(state: &AppState, metadata: &MetadataMap) -> StdResult<i64, Status> {
    let secret = metadata
        .get("client_secret")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    state
        .secrets()
        .server_id(secret)
        .ok_or_else(|| Status::unauthenticated("客户端认证失败"))
}

/// `updateServerRuntimeOnReport`：单事务关离线记录 + 写回上报字段。
/// v1 没有 node 绑定，绑定由首次上报按 `PLAIN_BINDING_REASON` 建立；
/// 运行态 `protocol` 落非 `v2`，读侧因此回落运行态列（`runtime_presentation`）。
async fn apply_report(
    state: &AppState,
    server_id: i64,
    now_nanos: i64,
    state_payload: Option<proto::State>,
    host_payload: Option<proto::Host>,
) -> StdResult<(), Status> {
    let offline = OfflineHistoryPolicy::from_conf(&state.conf());
    let db = state.db().clone();
    blocking_write(&db, move |tx| {
        let node_uuid = bindings::current_binding_for_server(tx, server_id)?
            .map(|binding| binding.node_uuid)
            .filter(|node_uuid| node_uuid.len() == 16)
            .unwrap_or_else(|| legacy_node_uuid(server_id));
        bindings::bind_server_node_for_protocol(tx, server_id, &node_uuid, now_nanos, false)?;
        // 乱序旧帧（v1 无序列号，只有接收钟参与判旧）返回 false，不算失败。
        runtime::touch_legacy_runtime(
            tx,
            &node_uuid,
            0,
            now_nanos,
            state_payload.as_ref(),
            host_payload.as_ref(),
            offline,
        )
        .map(|_| ())
    })
    .await
    .map_err(|err| Status::internal(err.to_string()))
}

/// `LookupGeoIP` 的写回：只更新国家码，不重置 last_ip / host_payload 其它字段
/// （Go 只动 `Host.CountryCode`）。无运行态行时回 `host not found`。
async fn apply_geoip_country_code(
    state: &AppState,
    server_id: i64,
    ip: &str,
    country_code: &str,
) -> StdResult<(), Status> {
    let db = state.db().clone();
    let ip = ip.to_string();
    let country_code = country_code.to_string();
    // `Db::write` 的闭包错误是 anyhow，Status 会被降级成 Internal；
    // 用显式 Result<_, Option<Status>> 把状态码带出事务边界。
    let outcome: StdResult<StdResult<(), Status>, anyhow::Error> = blocking_write(
        &db,
        move |tx| -> anyhow::Result<StdResult<(), Status>> {
            let host_payload: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT host_payload FROM server_runtimes WHERE server_id = ?1",
                    [server_id],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(host_payload) = host_payload else {
                return Ok(Err(Status::not_found("host not found")));
            };
            let mut host = if host_payload.is_empty() {
                proto::Host::default()
            } else {
                proto::Host::decode(host_payload.as_slice()).map_err(anyhow::Error::from)?
            };
            if !ip.trim().is_empty() {
                host.ip = ip;
            }
            host.country_code = country_code;
            tx.execute(
                "UPDATE server_runtimes SET host_payload = ?1, last_ip = ?2, updated_at = CURRENT_TIMESTAMP \
                 WHERE server_id = ?3",
                rusqlite::params![host.encode_to_vec(), host.ip.clone(), server_id],
            )?;
            Ok(Ok(()))
        },
    )
    .await;
    match outcome {
        Ok(Ok(())) => Ok(()),
        Ok(Err(status)) => Err(status),
        Err(err) => Err(Status::internal(err.to_string())),
    }
}

/// v1 无 node UUID 时按 server_id 派生稳定值（协议只要求 16 字节、同机稳定）。
fn legacy_node_uuid(server_id: i64) -> Vec<u8> {
    let mut node = vec![0u8; 16];
    node[0] = 0x76; // 'v'：标记 v1 派生节点
    node[8..16].copy_from_slice(&(server_id as u64).to_be_bytes());
    node
}

#[derive(Clone)]
pub struct LegacyHandler {
    state: std::sync::Arc<AppState>,
}

impl LegacyHandler {
    pub fn new(state: std::sync::Arc<AppState>) -> Self {
        Self { state }
    }

    fn lookup_country_code(&self, ip: &str) -> String {
        let parsed = std::net::ToSocketAddrs::to_socket_addrs(&format!("{ip}:0"))
            .ok()
            .and_then(|mut addrs| addrs.next())
            .map(|addr| addr.ip());
        match parsed {
            Some(addr) => crate::geoip::lookup_code(addr),
            None => crate::geoip::lookup_code_from_addr(Some(ip)),
        }
    }
}

#[tonic::async_trait]
impl proto::santaizi_service_server::SantaiziService for LegacyHandler {
    /// `ReportSystemState`：只刷运行态（uptime 等），不写证据表。
    async fn report_system_state(
        &self,
        request: Request<proto::State>,
    ) -> StdResult<Response<proto::Receipt>, Status> {
        let server_id = server_id_from_metadata(&self.state, request.metadata())?;
        let state_payload = request.into_inner();
        let now_nanos = util_time::now_nanos();
        apply_report(&self.state, server_id, now_nanos, Some(state_payload), None).await?;
        Ok(Response::new(proto::Receipt { proced: true }))
    }

    /// `ReportSystemInfo`：空 IP / 国家码继承上一帧，GeoIP 按地址兜底。
    async fn report_system_info(
        &self,
        request: Request<proto::Host>,
    ) -> StdResult<Response<proto::Receipt>, Status> {
        let server_id = server_id_from_metadata(&self.state, request.metadata())?;
        let mut host_payload = request.into_inner();
        if host_payload.country_code.trim().is_empty() && !host_payload.ip.trim().is_empty() {
            host_payload.country_code = self.lookup_country_code(&host_payload.ip);
        }
        let now_nanos = util_time::now_nanos();
        apply_report(&self.state, server_id, now_nanos, None, Some(host_payload)).await?;
        Ok(Response::new(proto::Receipt { proced: true }))
    }

    /// `LookupGeoIP`：按 IP 查国家码并写回 Host（Go 同款副作用）。
    async fn lookup_geo_ip(
        &self,
        request: Request<proto::GeoIp>,
    ) -> StdResult<Response<proto::GeoIp>, Status> {
        let server_id = server_id_from_metadata(&self.state, request.metadata())?;
        let mut geo = request.into_inner();
        let country_code = self.lookup_country_code(&geo.ip);
        apply_geoip_country_code(&self.state, server_id, &geo.ip, &country_code).await?;
        geo.country_code = country_code;
        Ok(Response::new(geo))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_node_uuid_is_stable_and_16_bytes() {
        let first = legacy_node_uuid(7);
        assert_eq!(first.len(), 16);
        assert_eq!(first, legacy_node_uuid(7));
        assert_ne!(first, legacy_node_uuid(8));
    }
}
