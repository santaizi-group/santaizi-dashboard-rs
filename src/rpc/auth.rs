//! 设备认证层：对齐 Go `service/rpc/mtls.go` 的 `UnaryDeviceAuth` /
//! `StreamDeviceAuth` 与 `service/pki/tls.go` 的对端身份解析。
//!
//! 与 Go 的差异：tonic 拦截器拿不到完整方法路径，因此用 tower 层在
//! `http::Request` 上做同序检查；`TlsConnectInfo` 由 tonic 在 TLS 连接上
//! 插入扩展，等价 Go 的 `peer.AuthInfo.(credentials.TLSInfo)`。

use std::future::Future;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::task::{Context, Poll};

use http::{Request, Response};
use tonic::Status;
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tower_layer::Layer;
use tower_service::Service;

use crate::pki::uri::{self, DeviceIdentity, DeviceKind, UriError};

const METHOD_ENROLL: &str = "/proto.SantaiziEnrollmentService/Enroll";
const METHOD_RENEW: &str = "/proto.SantaiziEnrollmentService/Renew";
const METHOD_REGISTER: &str = "/proto.SantaiziCollectorService/Register";
const METHOD_SYNC: &str = "/proto.SantaiziCollectorService/Sync";
const METHOD_GET_STATUS: &str = "/proto.SantaiziCollectorService/GetStatus";
const METHOD_RENEW_COLLECTOR: &str = "/proto.SantaiziCollectorService/RenewCollector";
const METHOD_CONTROL: &str = "/proto.SantaiziControlService/Control";
const METHOD_INGEST: &str = "/proto.SantaiziTelemetryService/Ingest";
const METHOD_NAT: &str = "/proto.SantaiziNATService/NATStream";
const METHOD_REPLICATE: &str = "/proto.SantaiziReplicationService/Replicate";

const HEALTH_PREFIX: &str = "/grpc.health.v1.Health/";

/// 认证策略：与 Go `DeviceAuthPolicy` 同字段。
#[derive(Debug, Clone, Default)]
pub struct DeviceAuthPolicy {
    pub require_agent_mtls: bool,
    pub require_collector_mtls: bool,
    /// 从端监听 + 主端 TLS 开启时，Ingest 必须带 agent 证书。
    pub force_agent_ingest: bool,
}

/// `AuthorizeDevice`：按完整方法路径裁决。返回 `Ok(())` 放行。
pub fn authorize_device<T>(
    req: &Request<T>,
    full_method: &str,
    policy: &DeviceAuthPolicy,
) -> StdResult<(), Status> {
    if full_method.starts_with(HEALTH_PREFIX) {
        return Ok(());
    }
    match full_method {
        METHOD_ENROLL | METHOD_REGISTER | METHOD_GET_STATUS => return Ok(()),
        _ => {}
    }

    let identity = peer_device_identity(req.extensions());
    let has_cert = identity.is_some();
    if let Some(Err(err)) = &identity {
        return Err(Status::permission_denied(err.to_string()));
    }
    let identity = identity.and_then(|result| result.ok());

    let need_agent = full_method == METHOD_RENEW
        || (policy.require_agent_mtls && is_agent_method(full_method))
        || (policy.force_agent_ingest && full_method == METHOD_INGEST);
    let need_collector = full_method == METHOD_RENEW_COLLECTOR
        || (policy.require_collector_mtls && is_collector_method(full_method));

    if need_agent {
        if !has_cert {
            return Err(Status::unauthenticated("agent certificate is required"));
        }
        return match identity {
            Some(ident) if ident.kind == DeviceKind::Agent => Ok(()),
            _ => Err(Status::permission_denied("agent certificate is required")),
        };
    }
    if need_collector {
        if !has_cert {
            return Err(Status::unauthenticated("collector certificate is required"));
        }
        return match identity {
            Some(ident) if ident.kind == DeviceKind::Collector => Ok(()),
            _ => Err(Status::permission_denied(
                "collector certificate is required",
            )),
        };
    }

    if let Some(ident) = identity {
        if is_agent_method(full_method) && ident.kind != DeviceKind::Agent {
            return Err(Status::permission_denied("agent certificate is required"));
        }
        if is_collector_method(full_method) && ident.kind != DeviceKind::Collector {
            return Err(Status::permission_denied(
                "collector certificate is required",
            ));
        }
    }
    Ok(())
}

fn is_agent_method(full_method: &str) -> bool {
    matches!(
        full_method,
        METHOD_CONTROL | METHOD_INGEST | METHOD_NAT | METHOD_RENEW
    )
}

fn is_collector_method(full_method: &str) -> bool {
    matches!(
        full_method,
        METHOD_SYNC | METHOD_REPLICATE | METHOD_RENEW_COLLECTOR
    )
}

/// `PeerDeviceIdentityFromContext`：从 TLS 对端证书解析设备身份。
/// `Some(Err(..))` 表示带了证书但解析失败，与 Go 的 `(nil, true, err)` 一致。
fn peer_device_identity(
    extensions: &http::Extensions,
) -> Option<StdResult<DeviceIdentity, UriError>> {
    let certs = extensions
        .get::<TlsConnectInfo<TcpConnectInfo>>()
        .and_then(|info| info.peer_certs())?;
    let leaf = certs.first()?;
    Some(uri::device_identity_from_der(leaf))
}

/// 连接是否为 TLS——Enrollment 的准入前提。
pub fn connection_is_tls(extensions: &http::Extensions) -> bool {
    extensions.get::<TlsConnectInfo<TcpConnectInfo>>().is_some()
}

/// 已通过 mTLS 校验的对端设备身份；无证书或解析失败返回 `None`。
/// 调用方需要区分「无证书」与「证书非法」时用 [`peer_device_identity_result`]。
pub fn peer_identity(extensions: &http::Extensions) -> Option<DeviceIdentity> {
    peer_device_identity(extensions).and_then(|result| result.ok())
}

/// 与 [`peer_identity`] 相同，但保留解析错误。
pub fn peer_device_identity_result(
    extensions: &http::Extensions,
) -> Option<StdResult<DeviceIdentity, UriError>> {
    peer_device_identity(extensions)
}

/// tower 层：对每个 gRPC 请求执行设备认证，等价 Go 的拦截器链。
#[derive(Debug, Clone)]
pub struct DeviceAuthLayer {
    policy: DeviceAuthPolicy,
}

impl DeviceAuthLayer {
    pub fn new(policy: DeviceAuthPolicy) -> Self {
        Self { policy }
    }
}

impl<S> Layer<S> for DeviceAuthLayer {
    type Service = DeviceAuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        DeviceAuthService {
            inner,
            policy: self.policy.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeviceAuthService<S> {
    inner: S,
    policy: DeviceAuthPolicy,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for DeviceAuthService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    ResBody: Default,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = DeviceAuthFuture<S::Future, ResBody>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<StdResult<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let full_method = req.uri().path().to_string();
        match authorize_device(&req, &full_method, &self.policy) {
            Ok(()) => DeviceAuthFuture::call(self.inner.call(req)),
            Err(status) => DeviceAuthFuture::rejected(status),
        }
    }
}

/// 认证通过则转发内层 future，失败则直接以 gRPC 状态响应。
pub struct DeviceAuthFuture<F, B> {
    kind: DeviceAuthFutureKind<F>,
    // `B` 只出现在 Future 实现的输出类型上，需要占位满足类型参数使用规则。
    body: std::marker::PhantomData<B>,
}

enum DeviceAuthFutureKind<F> {
    Call(F),
    Rejected(Option<Status>),
}

impl<F, B> DeviceAuthFuture<F, B> {
    fn call(future: F) -> Self {
        Self {
            kind: DeviceAuthFutureKind::Call(future),
            body: std::marker::PhantomData,
        }
    }

    fn rejected(status: Status) -> Self {
        Self {
            kind: DeviceAuthFutureKind::Rejected(Some(status)),
            body: std::marker::PhantomData,
        }
    }
}

impl<F, B, E> Future for DeviceAuthFuture<F, B>
where
    F: Future<Output = StdResult<Response<B>, E>>,
    B: Default,
{
    type Output = StdResult<Response<B>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `kind` 的变体在构造后不再变更，内层 future 只在本就固定的
        // 位置上被 poll，不会被移动。
        let this = unsafe { self.get_unchecked_mut() };
        match &mut this.kind {
            DeviceAuthFutureKind::Call(future) => unsafe { Pin::new_unchecked(future) }.poll(cx),
            DeviceAuthFutureKind::Rejected(slot) => {
                let status = slot
                    .take()
                    .expect("device auth future polled after completion");
                Poll::Ready(Ok(status.into_http()))
            }
        }
    }
}
