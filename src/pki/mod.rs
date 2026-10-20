//! PKI：双 CA 签发体系、设备身份 URI、原子写入与服务端证书。

pub mod atomic;
pub mod ca;
pub mod client;
pub mod tls;
pub mod uri;

pub use ca::{Authority, Bundle, default_dir, load_or_create, sign_agent_csr, sign_collector_csr};
pub use client::{ClientBundle, ClientStore};
pub use uri::{DeviceIdentity, DeviceKind, device_identity_from_der};
