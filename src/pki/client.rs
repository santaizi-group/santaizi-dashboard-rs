//! 设备侧（collector 从端）PKI 客户端存储，对齐 Go `service/pki/client.go`。
//!
//! 目录布局与 Go 一致：`client.key`（0600）/ `client.crt` / `ca.crt` /
//! `agent-ca.crt`（后三者为 0644）。私钥为 Ed25519 PKCS#8 PEM；
//! `ClientBundle` 持有 tonic 拨号所需的全部 PEM。

use std::path::{Path, PathBuf};
use std::time::Duration;

use rcgen::string::Ia5String;
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, PKCS_ED25519, SanType};
use time::OffsetDateTime;
use tonic::transport::Identity;
use x509_parser::certificate::X509Certificate;
use x509_parser::prelude::FromDer;

use super::atomic::write_file_atomic;
use super::ca::{DEFAULT_RENEW_WINDOW, cert_der_from_pem};
use crate::Result;

const CLIENT_KEY_NAME: &str = "client.key";
const CLIENT_CERT_NAME: &str = "client.crt";
const CLIENT_CA_NAME: &str = "ca.crt";
const AGENT_CA_NAME: &str = "agent-ca.crt";
const KEY_FILE_MODE: u32 = 0o600;
const CERT_FILE_MODE: u32 = 0o644;

/// 生成 Ed25519 设备密钥对（Go `pki.GenerateKey`；返回 rcgen 句柄，
/// 调用方需要 PEM 时 `serialize_pem`）。
pub fn generate_key() -> Result<KeyPair> {
    KeyPair::generate_for(&PKCS_ED25519).map_err(|e| anyhow::anyhow!("generate device key: {e}"))
}

/// 创建带设备 URI SAN 的 CSR（Go `pki.CreateCSR`）。
/// 主端签证书时会用自身 URI 覆盖 SAN，CSR 里的 URI 仅作标识。
pub fn create_csr(key: &KeyPair, uri: &str) -> Result<Vec<u8>> {
    let mut params = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, uri);
    params.distinguished_name = dn;
    if !uri.is_empty() {
        params.subject_alt_names = vec![SanType::URI(
            Ia5String::try_from(uri.to_string())
                .map_err(|e| anyhow::anyhow!("csr uri san: {e}"))?,
        )];
    }
    let csr = params
        .serialize_request(key)
        .map_err(|e| anyhow::anyhow!("serialize csr: {e}"))?;
    Ok(csr.der().to_vec())
}

/// 客户端证书包：密钥 + 证书 + CA（均为 PEM），与 Go `pki.ClientBundle` 对应。
#[derive(Debug, Clone)]
pub struct ClientBundle {
    pub key_pem: Vec<u8>,
    pub cert_pem: Vec<u8>,
    pub ca_pem: Vec<u8>,
    cert_der: Vec<u8>,
}

impl ClientBundle {
    /// 组装并校验：证书 PEM 必须可解析（对齐 Go `ParseCertificatePEM` 的失败即错）。
    pub fn new(key_pem: Vec<u8>, cert_pem: Vec<u8>, ca_pem: Vec<u8>) -> Result<Self> {
        let cert_der = cert_der_from_pem(&cert_pem)
            .ok_or_else(|| anyhow::anyhow!("client certificate PEM is not a certificate"))?;
        Ok(Self {
            key_pem,
            cert_pem,
            ca_pem,
            cert_der,
        })
    }

    pub fn cert_der(&self) -> &[u8] {
        &self.cert_der
    }

    /// 证书已过期（Go `ClientBundle.Expired`）。
    pub fn expired(&self, now: OffsetDateTime) -> bool {
        self.not_after()
            .map(|not_after| now.unix_timestamp() >= not_after)
            .unwrap_or(true)
    }

    /// 进入续期窗口（Go `ClientBundle.NeedsRenew`）。
    pub fn needs_renew(&self, now: OffsetDateTime, window: Duration) -> bool {
        let window = if window.is_zero() {
            DEFAULT_RENEW_WINDOW
        } else {
            window
        };
        self.not_after()
            .map(|not_after| {
                now.unix_timestamp() >= not_after - i64::try_from(window.as_secs()).unwrap_or(0)
            })
            .unwrap_or(true)
    }

    /// tonic 客户端身份（mTLS 拨号用）。
    pub fn identity(&self) -> Result<Identity> {
        Ok(Identity::from_pem(&self.cert_pem, &self.key_pem))
    }

    fn not_after(&self) -> Option<i64> {
        let (_, cert) = X509Certificate::from_der(&self.cert_der).ok()?;
        Some(cert.validity().not_after.timestamp())
    }
}

/// 客户端 PKI 目录：证书包的落盘存储（Go `pki.ClientStore`）。
pub struct ClientStore {
    dir: PathBuf,
}

impl ClientStore {
    pub fn new(dir: &str) -> Result<Self> {
        if dir.is_empty() {
            anyhow::bail!("client pki directory is empty");
        }
        std::fs::create_dir_all(dir)
            .map_err(|e| anyhow::anyhow!("create client pki dir {dir}: {e}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
        }
        Ok(Self {
            dir: PathBuf::from(dir),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// 加载证书包；不存在返回 `Ok(None)`（Go 的 `ErrClientBundleNotFound`）。
    pub fn load(&self) -> Result<Option<ClientBundle>> {
        let key_pem = match std::fs::read(self.dir.join(CLIENT_KEY_NAME)) {
            Ok(pem) => pem,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(anyhow::anyhow!("read client key: {err}")),
        };
        let cert_pem = match std::fs::read(self.dir.join(CLIENT_CERT_NAME)) {
            Ok(pem) => pem,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(anyhow::anyhow!("read client certificate: {err}")),
        };
        let ca_pem = std::fs::read(self.dir.join(CLIENT_CA_NAME)).unwrap_or_default();
        ClientBundle::new(key_pem, cert_pem, ca_pem).map(Some)
    }

    /// 保存证书包（Go `ClientStore.Save`）。
    pub fn save(&self, bundle: &ClientBundle) -> Result<()> {
        if bundle.key_pem.is_empty() || bundle.cert_pem.is_empty() {
            anyhow::bail!("client certificate bundle is incomplete");
        }
        write_file_atomic(
            &self.dir.join(CLIENT_KEY_NAME),
            &bundle.key_pem,
            KEY_FILE_MODE,
        )?;
        write_file_atomic(
            &self.dir.join(CLIENT_CERT_NAME),
            &bundle.cert_pem,
            CERT_FILE_MODE,
        )?;
        if !bundle.ca_pem.is_empty() {
            write_file_atomic(
                &self.dir.join(CLIENT_CA_NAME),
                &bundle.ca_pem,
                CERT_FILE_MODE,
            )?;
        }
        Ok(())
    }

    /// 保存主端下发的 Agent CA（Go `ClientStore.SaveAgentCA`）。
    pub fn save_agent_ca(&self, pem: &[u8]) -> Result<()> {
        if pem.is_empty() {
            return Ok(());
        }
        write_file_atomic(&self.dir.join(AGENT_CA_NAME), pem, CERT_FILE_MODE)
    }

    /// 丢弃盘上客户端密钥与证书（主端不承认当前证书时重新注册；
    /// 等价于运维手工删除 `client.key` / `client.crt` 的 remedy）。
    /// `ca.crt`（主端 CA，仍可用于校验服务端证书）与 `agent-ca.crt`
    /// （从端 agent 服务信任池）保留，注册成功后会一并刷新。
    pub fn discard_identity(&self) -> Result<()> {
        for name in [CLIENT_KEY_NAME, CLIENT_CERT_NAME] {
            let path = self.dir.join(name);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(anyhow::anyhow!("remove {}: {err}", path.display())),
            }
        }
        Ok(())
    }

    /// 读取 Agent CA；不存在返回空（Go `ClientStore.LoadAgentCA`）。
    pub fn load_agent_ca(&self) -> Result<Vec<u8>> {
        match std::fs::read(self.dir.join(AGENT_CA_NAME)) {
            Ok(pem) => Ok(pem),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(err) => Err(anyhow::anyhow!("read agent ca: {err}")),
        }
    }

    /// 盘上证书包对应的 tonic 身份；无证书返回 `None`。
    pub fn identity(&self) -> Result<Option<Identity>> {
        match self.load()? {
            Some(bundle) => bundle.identity().map(Some),
            None => Ok(None),
        }
    }

    /// 盘上是否存在未过期的证书包（Go `hasCollectorCert`）。
    pub fn has_bundle(&self) -> Result<bool> {
        match self.load()? {
            Some(bundle) => Ok(!bundle.expired(OffsetDateTime::now_utc())),
            None => Ok(false),
        }
    }

    /// 主端 CA PEM（`ca.crt`）；不存在返回 `None`。
    pub fn ca_pem(&self) -> Result<Option<Vec<u8>>> {
        match std::fs::read(self.dir.join(CLIENT_CA_NAME)) {
            Ok(pem) if !pem.is_empty() => Ok(Some(pem)),
            Ok(_) => Ok(None),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(anyhow::anyhow!("read client ca: {err}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("santaizi-pki-client-{tag}-{}", std::process::id()))
    }

    /// 自签一张not_after 明确的测试证书（设备证书由主端签发，这里只验过期逻辑）。
    fn bundle_with_validity(dir: &PathBuf, not_after: OffsetDateTime) -> ClientBundle {
        let key = generate_key().unwrap();
        let mut params = CertificateParams::default();
        params.not_before = OffsetDateTime::now_utc() - Duration::from_secs(60);
        params.not_after = not_after;
        let cert = params.self_signed(&key).unwrap();
        let _ = dir;
        ClientBundle::new(
            key.serialize_pem().into_bytes(),
            cert.pem().into_bytes(),
            Vec::new(),
        )
        .unwrap()
    }

    #[test]
    fn save_load_roundtrip_and_agent_ca() {
        let dir = test_dir("roundtrip");
        let _ = std::fs::remove_dir_all(&dir);
        let store = ClientStore::new(dir.to_str().unwrap()).unwrap();
        assert!(store.load().unwrap().is_none());
        assert!(store.load_agent_ca().unwrap().is_empty());
        assert!(store.ca_pem().unwrap().is_none());

        let bundle =
            bundle_with_validity(&dir, OffsetDateTime::now_utc() + Duration::from_secs(3600));
        store.save(&bundle).unwrap();
        let loaded = store.load().unwrap().expect("bundle");
        assert_eq!(loaded.cert_pem, bundle.cert_pem);
        assert_eq!(loaded.key_pem, bundle.key_pem);
        assert_eq!(loaded.cert_der(), bundle.cert_der());
        assert!(store.identity().unwrap().is_some());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.join("client.key"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, KEY_FILE_MODE);
        }

        store.save_agent_ca(b"agent-ca-pem").unwrap();
        assert_eq!(store.load_agent_ca().unwrap(), b"agent-ca-pem");
        store.save_agent_ca(b"").unwrap();
        assert_eq!(store.load_agent_ca().unwrap(), b"agent-ca-pem");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn expiry_and_renew_window() {
        let dir = test_dir("expiry");
        let _ = std::fs::remove_dir_all(&dir);
        let now = OffsetDateTime::now_utc();
        let fresh = bundle_with_validity(&dir, now + Duration::from_secs(30 * 24 * 3600));
        assert!(!fresh.expired(now));
        assert!(!fresh.needs_renew(now, DEFAULT_RENEW_WINDOW));

        let expiring = bundle_with_validity(&dir, now + Duration::from_secs(3 * 24 * 3600));
        assert!(!expiring.expired(now));
        assert!(expiring.needs_renew(now, DEFAULT_RENEW_WINDOW));

        let stale = bundle_with_validity(&dir, now - Duration::from_secs(60));
        assert!(stale.expired(now));
        assert!(stale.needs_renew(now, DEFAULT_RENEW_WINDOW));

        // 无证书 DER 时按过期处理（保守）。
        let broken = ClientBundle {
            key_pem: Vec::new(),
            cert_pem: b"not a certificate".to_vec(),
            ca_pem: Vec::new(),
            cert_der: Vec::new(),
        };
        assert!(broken.expired(now));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn discard_identity_keeps_ca_files() {
        let dir = test_dir("discard");
        let _ = std::fs::remove_dir_all(&dir);
        let store = ClientStore::new(dir.to_str().unwrap()).unwrap();
        let bundle =
            bundle_with_validity(&dir, OffsetDateTime::now_utc() + Duration::from_secs(3600));
        store.save(&bundle).unwrap();
        store.save_agent_ca(b"agent-ca-pem").unwrap();
        std::fs::write(dir.join("ca.crt"), b"primary-ca-pem").unwrap();

        store.discard_identity().unwrap();
        assert!(store.load().unwrap().is_none());
        assert!(store.identity().unwrap().is_none());
        // CA 与 Agent CA 保留：注册刷新前仍可用于校验主端 / agent 服务。
        assert_eq!(store.load_agent_ca().unwrap(), b"agent-ca-pem");
        assert_eq!(store.ca_pem().unwrap(), Some(b"primary-ca-pem".to_vec()));
        // 重复作废（无证书）是幂等空操作。
        store.discard_identity().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn create_csr_carries_uri_san() {
        let key = generate_key().unwrap();
        let csr = create_csr(&key, "santaizi://collector/pending").unwrap();
        assert!(!csr.is_empty());
        // CSR 可被 x509 解析且带 URI SAN（主端签证书时会覆盖 SAN）。
        let (_, parsed) = x509_parser::prelude::X509CertificationRequest::from_der(&csr).unwrap();
        let exts: Vec<_> = parsed.requested_extensions().unwrap().collect();
        assert!(exts.iter().any(|ext| matches!(
            ext,
            x509_parser::extensions::ParsedExtension::SubjectAlternativeName(_)
        )));
    }

    #[test]
    fn empty_dir_is_rejected() {
        assert!(ClientStore::new("").is_err());
    }
}
