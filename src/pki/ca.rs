//! 双 CA（agent-ca / collector-ca）签发体系，对齐 Go `service/pki/ca.go`。
//!
//! CA 与设备证书均为 Ed25519；CA 10 年、设备证书 30 天、NotBefore 回拨 5 分钟。

use std::fs;
use std::path::Path;
use std::result::Result as StdResult;
use std::time::Duration;

use rand::Rng;
use rcgen::string::Ia5String;
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DistinguishedName,
    DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, PKCS_ED25519, SanType,
    SerialNumber,
};
use time::OffsetDateTime;
use x509_parser::certificate::X509Certificate;
use x509_parser::prelude::FromDer;

use super::atomic::write_file_atomic;
use super::uri;
use crate::Result;

pub const DEFAULT_CERTIFICATE_VALIDITY: Duration = Duration::from_secs(30 * 24 * 60 * 60);
pub const DEFAULT_CA_VALIDITY: Duration = Duration::from_secs(10 * 365 * 24 * 60 * 60);
pub const NOT_BEFORE_SKEW: Duration = Duration::from_secs(5 * 60);
pub const DEFAULT_RENEW_WINDOW: Duration = Duration::from_secs(7 * 24 * 60 * 60);

const AGENT_CA_NAME: &str = "agent-ca";
const COLLECTOR_CA_NAME: &str = "collector-ca";
const AGENT_CA_CN: &str = "Santaizi Agent CA";
const COLLECTOR_CA_CN: &str = "Santaizi Collector CA";
const ORGANIZATION: &str = "Santaizi";
const CA_KEY_FILE_MODE: u32 = 0o600;
const CA_CERT_FILE_MODE: u32 = 0o644;

#[derive(Debug, thiserror::Error)]
pub enum CaError {
    #[error("pki directory is empty")]
    EmptyDir,
    #[error("read {0} key: {1}")]
    ReadKey(String, String),
    #[error("read {0} certificate: {1}")]
    ReadCert(String, String),
    #[error("{0} key is not PEM")]
    KeyNotPem(String),
    #[error("parse {0} key: {1}")]
    ParseKey(String, String),
    #[error("{0} key must be Ed25519")]
    KeyNotEd25519(String),
    #[error("{0} certificate is not a PEM certificate")]
    CertNotPem(String),
    #[error("node UUID must be 16 bytes")]
    NodeUuidLength,
    #[error("collector UUID is required")]
    CollectorUuidMissing,
    #[error("invalid SAN URI: {0}")]
    SanUri(String),
    #[error("parse CSR: {0}")]
    ParseCsr(String),
    #[error("sign certificate: {0}")]
    SignCertificate(String),
}

pub struct Authority {
    pub name: String,
    pub cert_pem: Vec<u8>,
    pub cert_der: Vec<u8>,
    issuer: Issuer<'static, KeyPair>,
}

impl std::fmt::Debug for Authority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authority")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

pub struct Bundle {
    pub dir: String,
    pub agent: Authority,
    pub collector: Authority,
}

pub fn default_dir(data_dir: &str) -> String {
    let base = if data_dir.is_empty() {
        "/var/lib/santaizi-dashboard"
    } else {
        data_dir
    };
    Path::new(base).join("pki").to_string_lossy().into_owned()
}

pub fn load_or_create(dir: &str) -> Result<Bundle> {
    if dir.is_empty() {
        return Err(CaError::EmptyDir.into());
    }
    fs::create_dir_all(dir).map_err(|e| anyhow::anyhow!("create pki dir {dir}: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(dir, fs::Permissions::from_mode(0o700));
    }
    let agent = load_or_create_ca(dir, AGENT_CA_NAME, AGENT_CA_CN)?;
    let collector = load_or_create_ca(dir, COLLECTOR_CA_NAME, COLLECTOR_CA_CN)?;
    Ok(Bundle {
        dir: dir.to_string(),
        agent,
        collector,
    })
}

fn load_or_create_ca(dir: &str, name: &str, common_name: &str) -> Result<Authority> {
    let key_path = Path::new(dir).join(format!("{name}.key"));
    let cert_path = Path::new(dir).join(format!("{name}.crt"));
    let key_pem = fs::read(&key_path);
    let cert_pem = fs::read(&cert_path);
    match (key_pem, cert_pem) {
        (Err(e), Err(_)) if e.kind() == std::io::ErrorKind::NotFound => {
            create_ca(&key_path, &cert_path, common_name)
        }
        (Err(e), _) => Err(CaError::ReadKey(name.to_string(), e.to_string()).into()),
        (_, Err(e)) => Err(CaError::ReadCert(name.to_string(), e.to_string()).into()),
        (Ok(key_pem), Ok(cert_pem)) => parse_ca(name, &key_pem, &cert_pem),
    }
}

fn create_ca(key_path: &Path, cert_path: &Path, common_name: &str) -> Result<Authority> {
    let key_pair = KeyPair::generate_for(&PKCS_ED25519)
        .map_err(|e| anyhow::anyhow!("generate CA key: {e}"))?;
    let now = OffsetDateTime::now_utc();
    let mut params = CertificateParams::default();
    params.serial_number = Some(random_serial());
    params.distinguished_name = ca_distinguished_name(common_name);
    params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    params.not_before = now - NOT_BEFORE_SKEW;
    params.not_after = now + DEFAULT_CA_VALIDITY;
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| anyhow::anyhow!("create CA certificate: {e}"))?;
    let cert_der = cert.der().to_vec();
    let cert_pem = cert.pem().into_bytes();
    let key_pem = key_pair.serialize_pem();
    write_file_atomic(key_path, key_pem.as_bytes(), CA_KEY_FILE_MODE)?;
    write_file_atomic(cert_path, &cert_pem, CA_CERT_FILE_MODE)?;
    Ok(Authority {
        name: common_name.to_string(),
        cert_pem,
        cert_der,
        issuer: Issuer::new(params, key_pair),
    })
}

fn parse_ca(name: &str, key_pem: &[u8], cert_pem: &[u8]) -> Result<Authority> {
    let key_str = std::str::from_utf8(key_pem).map_err(|_| CaError::KeyNotPem(name.to_string()))?;
    let key_pair = KeyPair::from_pem(key_str)
        .map_err(|e| CaError::ParseKey(name.to_string(), e.to_string()))?;
    if key_pair.algorithm() != &PKCS_ED25519 {
        return Err(CaError::KeyNotEd25519(name.to_string()).into());
    }
    let cert_der =
        cert_der_from_pem(cert_pem).ok_or_else(|| CaError::CertNotPem(name.to_string()))?;
    let common_name = cert_common_name(&cert_der, name)?;
    // 签发者 DN 由 CA 证书主题重建；与 Go 版共用同一组常量，跨实现加载的 CA 也能正确签发。
    let mut params = CertificateParams::default();
    params.distinguished_name = ca_distinguished_name(&common_name);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    Ok(Authority {
        name: common_name,
        cert_pem: cert_pem.to_vec(),
        cert_der,
        issuer: Issuer::new(params, key_pair),
    })
}

fn ca_distinguished_name(common_name: &str) -> DistinguishedName {
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, common_name);
    dn.push(DnType::OrganizationName, ORGANIZATION);
    dn
}

fn cert_common_name(der: &[u8], name: &str) -> StdResult<String, CaError> {
    let (_, cert) = X509Certificate::from_der(der)
        .map_err(|e| CaError::ParseKey(name.to_string(), e.to_string()))?;
    cert.tbs_certificate
        .subject
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(|cn| cn.to_string())
        .ok_or_else(|| CaError::CertNotPem(name.to_string()))
}

pub fn cert_der_from_pem(pem_bytes: &[u8]) -> Option<Vec<u8>> {
    rustls_pemfile::certs(&mut std::io::BufReader::new(pem_bytes))
        .next()?
        .ok()
        .map(|cert| cert.to_vec())
}

fn random_serial() -> SerialNumber {
    let mut serial = [0u8; 16];
    rand::rng().fill_bytes(&mut serial);
    serial[0] &= 0x7f;
    if serial[0] == 0 {
        serial[0] = 1;
    }
    SerialNumber::from_slice(&serial)
}

pub fn sign_agent_csr(
    ca: &Authority,
    csr_der: &[u8],
    node_uuid: &[u8],
    now: OffsetDateTime,
) -> StdResult<(Vec<u8>, OffsetDateTime, OffsetDateTime), CaError> {
    if node_uuid.len() != 16 {
        return Err(CaError::NodeUuidLength);
    }
    sign_csr(
        ca,
        csr_der,
        &uri::encode_agent_uri(node_uuid),
        DEFAULT_CERTIFICATE_VALIDITY,
        now,
    )
}

pub fn sign_collector_csr(
    ca: &Authority,
    csr_der: &[u8],
    collector_uuid: &str,
    now: OffsetDateTime,
) -> StdResult<(Vec<u8>, OffsetDateTime, OffsetDateTime), CaError> {
    if collector_uuid.is_empty() {
        return Err(CaError::CollectorUuidMissing);
    }
    sign_csr(
        ca,
        csr_der,
        &uri::encode_collector_uri(collector_uuid),
        DEFAULT_CERTIFICATE_VALIDITY,
        now,
    )
}

fn sign_csr(
    ca: &Authority,
    csr_der: &[u8],
    uri: &str,
    validity: Duration,
    now: OffsetDateTime,
) -> StdResult<(Vec<u8>, OffsetDateTime, OffsetDateTime), CaError> {
    if csr_der.is_empty() {
        return Err(CaError::ParseCsr("CSR is required".to_string()));
    }
    let csr = CertificateSigningRequestParams::from_der(
        &rustls_pki_types::CertificateSigningRequestDer::from(csr_der.to_vec()),
    )
    .map_err(|e| CaError::ParseCsr(e.to_string()))?;
    let validity = if validity.is_zero() {
        DEFAULT_CERTIFICATE_VALIDITY
    } else {
        validity
    };
    let not_before = now - NOT_BEFORE_SKEW;
    let not_after = now + validity;
    let mut params = CertificateParams::default();
    params.serial_number = Some(random_serial());
    params.distinguished_name = leaf_distinguished_name(uri);
    params.is_ca = IsCa::ExplicitNoCa;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    params.subject_alt_names = vec![SanType::URI(
        Ia5String::try_from(uri).map_err(|e| CaError::SanUri(e.to_string()))?,
    )];
    params.not_before = not_before;
    params.not_after = not_after;
    let cert = params
        .signed_by(&csr.public_key, &ca.issuer)
        .map_err(|e| CaError::SignCertificate(e.to_string()))?;
    Ok((cert.pem().into_bytes(), not_before, not_after))
}

fn leaf_distinguished_name(uri: &str) -> DistinguishedName {
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, uri);
    dn.push(DnType::OrganizationName, ORGANIZATION);
    dn
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn test_dir(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("santaizi-pki-{tag}-{}", std::process::id()))
    }

    fn csr_der() -> Vec<u8> {
        let key_pair = KeyPair::generate_for(&PKCS_ED25519).unwrap();
        let params = CertificateParams::default();
        params.serialize_request(&key_pair).unwrap().der().to_vec()
    }

    #[test]
    fn create_and_reload_bundle() {
        let dir = test_dir("bundle");
        let _ = fs::remove_dir_all(&dir);
        let bundle = load_or_create(dir.to_str().unwrap()).unwrap();
        assert_eq!(bundle.agent.name, AGENT_CA_CN);
        assert_eq!(bundle.collector.name, COLLECTOR_CA_CN);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let key_mode = fs::metadata(dir.join("agent-ca.key"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            let cert_mode = fs::metadata(dir.join("agent-ca.crt"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(key_mode, 0o600);
            assert_eq!(cert_mode, 0o644);
        }
        let reloaded = load_or_create(dir.to_str().unwrap()).unwrap();
        assert_eq!(reloaded.agent.cert_pem, bundle.agent.cert_pem);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sign_csr_roundtrip_agent_and_collector() {
        let dir = test_dir("sign");
        let _ = fs::remove_dir_all(&dir);
        let bundle = load_or_create(dir.to_str().unwrap()).unwrap();

        let (pem, not_before, not_after) = sign_agent_csr(
            &bundle.agent,
            &csr_der(),
            &[3u8; 16],
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        let identity = uri::device_identity_from_der(&cert_der_from_pem(&pem).unwrap()).unwrap();
        assert_eq!(identity.kind, uri::DeviceKind::Agent);
        assert_eq!(identity.node_uuid, vec![3u8; 16]);
        assert!(not_after - not_before >= DEFAULT_CERTIFICATE_VALIDITY - Duration::from_secs(1));

        let (pem, _, _) = sign_collector_csr(
            &bundle.collector,
            &csr_der(),
            "collector-1",
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        let identity = uri::device_identity_from_der(&cert_der_from_pem(&pem).unwrap()).unwrap();
        assert_eq!(identity.kind, uri::DeviceKind::Collector);
        assert_eq!(identity.collector_uuid, "collector-1");

        // 重载后的 CA 仍能签发，且证书可解析。
        let reloaded = load_or_create(dir.to_str().unwrap()).unwrap();
        let (pem, _, _) = sign_agent_csr(
            &reloaded.agent,
            &csr_der(),
            &[4u8; 16],
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        let identity = uri::device_identity_from_der(&cert_der_from_pem(&pem).unwrap()).unwrap();
        assert_eq!(identity.node_uuid, vec![4u8; 16]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sign_csr_rejects_garbage() {
        let dir = test_dir("garbage");
        let _ = fs::remove_dir_all(&dir);
        let bundle = load_or_create(dir.to_str().unwrap()).unwrap();
        assert!(matches!(
            sign_agent_csr(
                &bundle.agent,
                b"not-a-csr",
                &[1u8; 16],
                OffsetDateTime::now_utc()
            ),
            Err(CaError::ParseCsr(_))
        ));
        assert!(matches!(
            sign_agent_csr(
                &bundle.agent,
                &[1, 2, 3],
                &[1u8; 15],
                OffsetDateTime::now_utc()
            ),
            Err(CaError::NodeUuidLength)
        ));
        let _ = fs::remove_dir_all(&dir);
    }
}
