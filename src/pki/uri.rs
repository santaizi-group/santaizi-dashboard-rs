//! 设备身份 URI（`urn:santaizi:agent:<hex>` / `urn:santaizi:collector:<uuid>`）与证书 SAN 解析。

use thiserror::Error;
use x509_parser::certificate::X509Certificate;
use x509_parser::extensions::GeneralName;

pub const AGENT_URI_PREFIX: &str = "urn:santaizi:agent:";
pub const COLLECTOR_URI_PREFIX: &str = "urn:santaizi:collector:";

#[derive(Debug, Error)]
pub enum UriError {
    #[error("not an agent URI")]
    NotAgentUri,
    #[error("not a collector URI")]
    NotCollectorUri,
    #[error("agent URI must contain a 16-byte UUID")]
    AgentUuidLength,
    #[error("collector URI is missing collector UUID")]
    CollectorUuidMissing,
    #[error("invalid hex in agent URI")]
    AgentUuidHex,
    #[error("certificate is required")]
    CertificateRequired,
    #[error("certificate has conflicting Santaizi URIs")]
    ConflictingUris,
    #[error("certificate is missing a Santaizi device URI SAN")]
    MissingDeviceUri,
    #[error("certificate is not an agent device certificate")]
    NotAgentCertificate,
    #[error("certificate is not a collector device certificate")]
    NotCollectorCertificate,
    #[error("parse certificate: {0}")]
    ParseCertificate(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceKind {
    Unknown,
    Agent,
    Collector,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceIdentity {
    pub kind: DeviceKind,
    pub node_uuid: Vec<u8>,
    pub collector_uuid: String,
}

pub fn encode_agent_uri(node_uuid: &[u8]) -> String {
    format!("{AGENT_URI_PREFIX}{}", hex::encode(node_uuid))
}

pub fn encode_collector_uri(collector_uuid: &str) -> String {
    format!("{COLLECTOR_URI_PREFIX}{collector_uuid}")
}

pub fn parse_agent_uri(raw: &str) -> Result<Vec<u8>, UriError> {
    let Some(rest) = raw.strip_prefix(AGENT_URI_PREFIX) else {
        return Err(UriError::NotAgentUri);
    };
    let cleaned = rest.replace('-', "");
    let decoded = hex::decode(&cleaned).map_err(|_| UriError::AgentUuidHex)?;
    if decoded.len() != 16 {
        return Err(UriError::AgentUuidLength);
    }
    Ok(decoded)
}

pub fn parse_collector_uri(raw: &str) -> Result<String, UriError> {
    let Some(rest) = raw.strip_prefix(COLLECTOR_URI_PREFIX) else {
        return Err(UriError::NotCollectorUri);
    };
    let id = rest.trim();
    if id.is_empty() {
        return Err(UriError::CollectorUuidMissing);
    }
    Ok(id.to_string())
}

pub fn parse_device_identity_from_certificate(
    cert: &X509Certificate,
) -> Result<DeviceIdentity, UriError> {
    let mut identity: Option<DeviceIdentity> = None;
    let Ok(Some(san)) = cert.subject_alternative_name() else {
        return Err(UriError::MissingDeviceUri);
    };
    for name in &san.value.general_names {
        let GeneralName::URI(raw) = name else {
            continue;
        };
        if raw.starts_with(AGENT_URI_PREFIX) {
            let node_uuid = parse_agent_uri(raw)?;
            if identity.is_some() {
                return Err(UriError::ConflictingUris);
            }
            identity = Some(DeviceIdentity {
                kind: DeviceKind::Agent,
                node_uuid,
                collector_uuid: String::new(),
            });
        } else if raw.starts_with(COLLECTOR_URI_PREFIX) {
            let collector_uuid = parse_collector_uri(raw)?;
            if identity.is_some() {
                return Err(UriError::ConflictingUris);
            }
            identity = Some(DeviceIdentity {
                kind: DeviceKind::Collector,
                node_uuid: Vec::new(),
                collector_uuid,
            });
        }
    }
    identity.ok_or(UriError::MissingDeviceUri)
}

pub fn parse_agent_identity_from_certificate(cert: &X509Certificate) -> Result<Vec<u8>, UriError> {
    let identity = parse_device_identity_from_certificate(cert)?;
    if identity.kind != DeviceKind::Agent {
        return Err(UriError::NotAgentCertificate);
    }
    Ok(identity.node_uuid)
}

pub fn parse_collector_identity_from_certificate(
    cert: &X509Certificate,
) -> Result<String, UriError> {
    let identity = parse_device_identity_from_certificate(cert)?;
    if identity.kind != DeviceKind::Collector {
        return Err(UriError::NotCollectorCertificate);
    }
    Ok(identity.collector_uuid)
}

/// 从 DER 证书中解析设备身份（供 mTLS 对端校验使用）。
pub fn device_identity_from_der(der: &[u8]) -> Result<DeviceIdentity, UriError> {
    use x509_parser::prelude::FromDer;
    let (_, cert) =
        X509Certificate::from_der(der).map_err(|e| UriError::ParseCertificate(e.to_string()))?;
    parse_device_identity_from_certificate(&cert)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_uri_roundtrip() {
        let uuid = [7u8; 16];
        let uri = encode_agent_uri(&uuid);
        assert_eq!(uri, format!("urn:santaizi:agent:{}", "07".repeat(16)));
        assert_eq!(parse_agent_uri(&uri).unwrap(), uuid.to_vec());
        assert_eq!(
            parse_agent_uri(&uri.replace('-', "")).unwrap(),
            uuid.to_vec()
        );
    }

    #[test]
    fn collector_uri_roundtrip() {
        let id = "b3f1e4c2-9a1d-4e2f-8c3b-1a2b3c4d5e6f";
        let uri = encode_collector_uri(id);
        assert_eq!(uri, format!("urn:santaizi:collector:{id}"));
        assert_eq!(parse_collector_uri(&uri).unwrap(), id);
    }

    #[test]
    fn rejects_wrong_prefix_and_length() {
        assert!(matches!(
            parse_agent_uri("urn:other:agent:00"),
            Err(UriError::NotAgentUri)
        ));
        assert!(matches!(
            parse_agent_uri("urn:santaizi:agent:00ff"),
            Err(UriError::AgentUuidLength)
        ));
        assert!(matches!(
            parse_agent_uri("urn:santaizi:agent:zz"),
            Err(UriError::AgentUuidHex)
        ));
        assert!(matches!(
            parse_collector_uri("urn:santaizi:agent:x"),
            Err(UriError::NotCollectorUri)
        ));
        assert!(matches!(
            parse_collector_uri("urn:santaizi:collector:"),
            Err(UriError::CollectorUuidMissing)
        ));
    }
}
