//! 服务端自签证书（测试与本地固定场景）与设备证书加载辅助，对齐 Go `service/pki/tls.go`。

use std::result::Result as StdResult;
use std::time::Duration;

use rcgen::string::Ia5String;
use rcgen::{
    CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    PKCS_ED25519, SanType,
};
use time::OffsetDateTime;

use super::ca::NOT_BEFORE_SKEW;
use crate::Result;

/// 生成独立于设备 CA 的自签服务端证书（Ed25519，ServerAuth，DNS SAN）。
/// 测试与本地场景不得使用 Agent CA / Collector CA 签服务端证书。
pub fn new_self_signed_server_certificate(
    dns_names: &[String],
    now: OffsetDateTime,
    validity: Duration,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let validity = if validity.is_zero() {
        super::ca::DEFAULT_CERTIFICATE_VALIDITY
    } else {
        validity
    };
    let mut names = dns_names.to_vec();
    if names.is_empty() {
        names.push("localhost".to_string());
    }
    let key_pair = KeyPair::generate_for(&PKCS_ED25519)
        .map_err(|e| anyhow::anyhow!("generate server key: {e}"))?;
    let mut dn = rcgen::DistinguishedName::new();
    dn.push(DnType::CommonName, names[0].clone());
    dn.push(DnType::OrganizationName, "Santaizi");
    let mut params = CertificateParams::default();
    params.distinguished_name = dn;
    params.is_ca = IsCa::ExplicitNoCa;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    // SAN 名来自配置：`Ia5String` 只接受 ASCII，非 ASCII 配置项会让
    // `expect` 直接 panic 把整个进程带走。这里逐个校验，把问题报成可操作的错误。
    // SAN 名来自配置：`Ia5String` 只接受 ASCII，非 ASCII 配置项会让
    // `expect` 直接 panic 把整个进程带走。这里逐个校验，把问题报成可操作的错误。
    let mut subject_alt_names = Vec::with_capacity(names.len());
    for name in &names {
        let san = Ia5String::try_from(name.clone())
            .map_err(|_| anyhow::anyhow!("server certificate DNS name is not ASCII: {name:?}"))?;
        subject_alt_names.push(SanType::DnsName(san));
    }
    params.subject_alt_names = subject_alt_names;
    params.not_before = now - NOT_BEFORE_SKEW;
    params.not_after = now + validity;
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| anyhow::anyhow!("create server certificate: {e}"))?;
    Ok((
        cert.pem().into_bytes(),
        key_pair.serialize_pem().into_bytes(),
    ))
}

/// 从 PEM 加载证书链（供 `grpc_tls.cert_file` 使用；私钥由调用方按需加载）。
pub fn load_certificate_chain(
    cert_pem: &[u8],
) -> Result<Vec<rustls_pki_types::CertificateDer<'static>>> {
    let certs: Vec<_> = rustls_pemfile::certs(&mut std::io::BufReader::new(cert_pem))
        .collect::<StdResult<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("read server certificate PEM: {e}"))?;
    if certs.is_empty() {
        anyhow::bail!("server certificate PEM contains no certificates");
    }
    Ok(certs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use x509_parser::certificate::X509Certificate;
    use x509_parser::prelude::FromDer;

    #[test]
    fn self_signed_server_certificate_parses() {
        let (cert_pem, _key_pem) = new_self_signed_server_certificate(
            &["localhost".to_string()],
            OffsetDateTime::now_utc(),
            Duration::ZERO,
        )
        .unwrap();
        let certs = load_certificate_chain(&cert_pem).unwrap();
        assert_eq!(certs.len(), 1);
        let (_, cert) = X509Certificate::from_der(&certs[0]).unwrap();
        let san = cert.subject_alternative_name().unwrap().unwrap();
        let names: Vec<_> = san
            .value
            .general_names
            .iter()
            .filter_map(|n| match n {
                x509_parser::extensions::GeneralName::DNSName(name) => Some(*name),
                _ => None,
            })
            .collect();
        assert_eq!(names, vec!["localhost"]);
    }

    /// 非 ASCII 的 SAN 名必须报错，不能让 `expect` 把进程带走。
    /// 配置里出现中文域名/误粘贴的字符时，启动应当失败并说清原因。
    #[test]
    fn non_ascii_dns_name_is_reported_not_panicked() {
        let err = new_self_signed_server_certificate(
            &["三太子.example".to_string()],
            OffsetDateTime::now_utc(),
            Duration::from_secs(60),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("not ASCII"),
            "非 ASCII SAN 名必须报可操作的错误，实际: {err}"
        );
    }

    /// 空名单有默认值（localhost），不能因此报错。
    #[test]
    fn empty_dns_name_list_falls_back_to_localhost() {
        let (cert_pem, _) = new_self_signed_server_certificate(
            &[],
            OffsetDateTime::now_utc(),
            Duration::from_secs(60),
        )
        .unwrap();
        let certs = load_certificate_chain(&cert_pem).unwrap();
        assert_eq!(certs.len(), 1);
    }
}
