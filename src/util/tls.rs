//! 进程级 rustls 加密提供者装配。
//!
//! 依赖图里 `ring`（tonic `tls-ring`）与 `aws-lc-rs`（reqwest 经
//! hyper-rustls）同时启用，rustls 无法按 crate feature 自动二选一，
//! 必须在首 TLS 握手前显式安装默认提供者，否则服务端与客户端都会 panic。

use std::sync::OnceLock;

static INSTALLED: OnceLock<()> = OnceLock::new();

/// 安装 ring 为进程级默认加密提供者；重复调用无副作用。
pub fn install_crypto_provider() {
    INSTALLED.get_or_init(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
    });
}

/// 对齐 Go `InsecureSkipVerify`：不校验证书链、主机名与握手签名。
/// 仅供 `collector.primary_insecure_tls` 测试场景使用；调用方须先记 WARNING。
#[derive(Debug)]
pub struct SkipServerVerify;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}
