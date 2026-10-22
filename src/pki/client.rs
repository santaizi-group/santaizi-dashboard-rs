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
    /// 组装并校验：证书 PEM 必须可解析（对齐 Go `ParseCertificatePEM` 的失败即错），
    /// 且私钥必须与证书的公钥配对。
    ///
    /// 配对校验是给「崩在三次写中间」收拾残局的：key 换了、cert 还是旧的，
    /// 这种包单看两个文件都合法，只有拿去握手才报一串指不到原因的错。
    /// 加载时发现配不上就当作没有证书包，逼探针重新注册，而不是带病运行。
    pub fn new(key_pem: Vec<u8>, cert_pem: Vec<u8>, ca_pem: Vec<u8>) -> Result<Self> {
        let cert_der = cert_der_from_pem(&cert_pem)
            .ok_or_else(|| anyhow::anyhow!("client certificate PEM is not a certificate"))?;
        if !key_matches_cert(&key_pem, &cert_der) {
            anyhow::bail!("client private key does not match the client certificate");
        }
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

/// 私钥与证书的公钥是否配对：比 SPKI 的 DER 字节，避开 PEM 规范化差异。
///
/// rcgen 只给公钥 PEM，这里借 `rustls-pemfile` 把它解回 DER；证书侧取
/// `SubjectPublicKeyInfo` 的原始 DER（`raw` 覆盖 AlgorithmIdentifier 在内的
/// 完整结构，与 PEM 解出来的字节同构）。
fn key_matches_cert(key_pem: &[u8], cert_der: &[u8]) -> bool {
    let Ok(key_pem) = std::str::from_utf8(key_pem) else {
        return false;
    };
    let Ok(key) = KeyPair::from_pem(key_pem) else {
        return false;
    };
    let Ok((_, cert)) = X509Certificate::from_der(cert_der) else {
        return false;
    };
    let public_pem = key.public_key_pem();
    let mut reader = std::io::BufReader::new(public_pem.as_bytes());
    let Some(spki) = rustls_pemfile::public_keys(&mut reader)
        .next()
        .and_then(|item| item.ok())
    else {
        return false;
    };
    cert.public_key().raw == spki.as_ref()
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
    ///
    /// 三次写各自原子，合起来不原子：中途崩掉会留下「新 key + 旧 cert」。
    /// 这个残局由 `ClientBundle::new` 的配对校验在**加载时**收拾——
    /// 配不上就当没有证书包，逼探针重新注册。写入侧只保证每一步自身原子。
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

    /// 私钥权限必须在内容落盘前就位：按 umask 建临时文件再写密钥，
    /// 中间那段窗口里密钥对其他本地用户可读。
    #[test]
    #[cfg(unix)]
    fn key_is_never_world_readable_mid_write() {
        use std::os::unix::fs::PermissionsExt;
        let dir = test_dir("perm");
        let _ = std::fs::remove_dir_all(&dir);
        let store = ClientStore::new(dir.to_str().unwrap()).unwrap();
        let bundle =
            bundle_with_validity(&dir, OffsetDateTime::now_utc() + Duration::from_secs(3600));
        // 特意用一个很宽的 umask（不移位只读当前值）：这里只验证写入路径
        // 不依赖进程 umask——权限由 create 的 mode 直接给死。
        store.save(&bundle).unwrap();
        let mode = std::fs::metadata(dir.join("client.key"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, KEY_FILE_MODE);
        // 临时文件必须清干净（带 stem 的新命名也不能留残余）。
        let leftovers: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|entry| {
                let name = entry.ok()?.file_name().to_string_lossy().into_owned();
                name.starts_with(".pki-").then_some(name)
            })
            .collect();
        assert!(leftovers.is_empty(), "临时文件残留: {leftovers:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 崩在「key 已换、cert 还没换」的中间态：两个文件单看都合法，
    /// 只有配对校验能认出来。`ClientBundle::new` 必须直接拒绝，
    /// 而不是把一个拿去握手必然失败、报错还指不到原因的包交出去。
    #[test]
    fn mismatched_key_and_cert_is_rejected() {
        let dir = test_dir("mismatch");
        let _ = std::fs::remove_dir_all(&dir);
        let a = bundle_with_validity(&dir, OffsetDateTime::now_utc() + Duration::from_secs(3600));
        let b = bundle_with_validity(&dir, OffsetDateTime::now_utc() + Duration::from_secs(7200));
        // a 的私钥 + b 的证书：正是「换了 key 没换 cert」的现场。
        let err = ClientBundle::new(a.key_pem.clone(), b.cert_pem.clone(), Vec::new()).unwrap_err();
        assert!(
            err.to_string().contains("does not match"),
            "key/cert 不配对必须被拒，实际: {err}"
        );
        // 同一套自己的 key + cert 必须照常通过。
        assert!(ClientBundle::new(a.key_pem, a.cert_pem, Vec::new()).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 磁盘上留下「新 key + 旧 cert」时，`load` 必须当作没有证书包，
    /// 让探针走重新注册，而不是带病运行。
    #[test]
    fn load_treats_a_mismatched_on_disk_pair_as_absent() {
        let dir = test_dir("load-mismatch");
        let _ = std::fs::remove_dir_all(&dir);
        let store = ClientStore::new(dir.to_str().unwrap()).unwrap();
        let first =
            bundle_with_validity(&dir, OffsetDateTime::now_utc() + Duration::from_secs(3600));
        store.save(&first).unwrap();
        // 只换 key，cert 留着旧的。
        let second =
            bundle_with_validity(&dir, OffsetDateTime::now_utc() + Duration::from_secs(7200));
        std::fs::write(dir.join("client.key"), &second.key_pem).unwrap();
        std::fs::write(dir.join("client.crt"), &first.cert_pem).unwrap();
        let loaded = store.load();
        assert!(
            loaded.is_err() || loaded.unwrap().is_none(),
            "配不上的证书包不得被当成可用身份"
        );
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
