//! 遥测凭证：Ed25519 签发/验签与注册令牌，对齐 Go `service/telemetry/credential.go`。

use std::path::Path;
use std::result::Result as StdResult;

use ed25519_dalek::{Signature, Signer as _, SigningKey, Verifier, VerifyingKey};
use prost::Message;
use rand::Rng;
use sha2::{Digest, Sha256};

use crate::Result;
use crate::proto;

pub const TELEMETRY_CAPABILITY: &str = "telemetry";
const CLAIM_NOT_ACTIVE_SKEW_SECS: i64 = 300;

#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    #[error("signing key path is empty")]
    EmptySigningKeyPath,
    #[error("signing key has invalid length {0}")]
    SigningKeyLength(usize),
    #[error("node UUID must be 16 bytes")]
    NodeUuidLength,
    #[error("invalid telemetry credential")]
    InvalidCredential,
    #[error("telemetry credential key ID mismatch")]
    KeyIdMismatch,
    #[error("telemetry credential signature is invalid")]
    SignatureInvalid,
    #[error("telemetry credential claims are invalid")]
    ClaimsInvalid,
    #[error("telemetry credential is not active")]
    NotActive,
    #[error("telemetry credential expired")]
    Expired,
}

pub struct Signer {
    signing: SigningKey,
    public: [u8; 32],
    key_id: [u8; 16],
}

impl std::fmt::Debug for Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Signer")
            .field("key_id", &hex::encode(self.key_id))
            .finish_non_exhaustive()
    }
}

impl Signer {
    pub fn load_or_create(path: &str) -> Result<Self> {
        if path.is_empty() {
            return Err(CredentialError::EmptySigningKeyPath.into());
        }
        let path = Path::new(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| anyhow::anyhow!("create signing key directory: {e}"))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
            }
        }
        let seed: [u8; 32] = match std::fs::read(path) {
            Ok(bytes) => {
                if bytes.len() != 32 {
                    return Err(CredentialError::SigningKeyLength(bytes.len()).into());
                }
                let mut seed = [0u8; 32];
                seed.copy_from_slice(&bytes);
                seed
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut seed = [0u8; 32];
                rand::rng().fill_bytes(&mut seed);
                crate::pki::atomic::write_file_atomic(path, &seed, 0o600)?;
                seed
            }
            Err(e) => return Err(anyhow::anyhow!("read signing key {}: {e}", path.display())),
        };
        Ok(Self::from_seed(seed))
    }

    pub fn from_seed(seed: [u8; 32]) -> Self {
        let signing = SigningKey::from_bytes(&seed);
        let public = signing.verifying_key().to_bytes();
        let key_id = sha256_first16(&public);
        Self {
            signing,
            public,
            key_id,
        }
    }

    pub fn public_key(&self) -> Vec<u8> {
        self.public.to_vec()
    }

    pub fn key_id(&self) -> Vec<u8> {
        self.key_id.to_vec()
    }

    pub fn sign(
        &self,
        node_uuid: &[u8],
        config_version: u64,
        now_unix: i64,
        validity_secs: i64,
    ) -> StdResult<proto::SignedAgentCredential, CredentialError> {
        if node_uuid.len() != 16 {
            return Err(CredentialError::NodeUuidLength);
        }
        let claims = proto::AgentCredentialClaims {
            node_uuid: node_uuid.to_vec(),
            issued_at_unix: now_unix,
            expires_at_unix: now_unix + validity_secs,
            capability: TELEMETRY_CAPABILITY.to_string(),
            config_version,
        };
        let encoded = claims.encode_to_vec();
        let signature = self.signing.sign(&encoded);
        Ok(proto::SignedAgentCredential {
            claims: encoded,
            signature: signature.to_bytes().to_vec(),
            key_id: self.key_id.to_vec(),
        })
    }
}

pub struct Verification {
    pub claims: proto::AgentCredentialClaims,
    pub in_grace: bool,
}

pub fn verify_credential(
    public_key: &[u8],
    expected_key_id: &[u8],
    credential: &proto::SignedAgentCredential,
    now_unix: i64,
    grace_secs: i64,
    previously_authorized: bool,
) -> StdResult<Verification, CredentialError> {
    let public: [u8; 32] = public_key
        .try_into()
        .map_err(|_| CredentialError::InvalidCredential)?;
    if !expected_key_id.is_empty() && !constant_time_eq(expected_key_id, &credential.key_id) {
        return Err(CredentialError::KeyIdMismatch);
    }
    let verifying =
        VerifyingKey::from_bytes(&public).map_err(|_| CredentialError::InvalidCredential)?;
    let signature_bytes: [u8; 64] = credential
        .signature
        .as_slice()
        .try_into()
        .map_err(|_| CredentialError::SignatureInvalid)?;
    let signature = Signature::from_bytes(&signature_bytes);
    verifying
        .verify(&credential.claims, &signature)
        .map_err(|_| CredentialError::SignatureInvalid)?;
    let claims = proto::AgentCredentialClaims::decode(credential.claims.as_slice())
        .map_err(|_| CredentialError::ClaimsInvalid)?;
    if claims.node_uuid.len() != 16 || claims.capability != TELEMETRY_CAPABILITY {
        return Err(CredentialError::ClaimsInvalid);
    }
    if now_unix < claims.issued_at_unix - CLAIM_NOT_ACTIVE_SKEW_SECS {
        return Err(CredentialError::NotActive);
    }
    if now_unix <= claims.expires_at_unix {
        return Ok(Verification {
            claims,
            in_grace: false,
        });
    }
    if previously_authorized && now_unix <= claims.expires_at_unix + grace_secs {
        return Ok(Verification {
            claims,
            in_grace: true,
        });
    }
    Err(CredentialError::Expired)
}

/// 生成注册令牌：`stc_<32B hex>`；仅返回一次，库中只存 SHA-256。
pub fn new_registration_token() -> Result<(String, [u8; 32])> {
    let mut token = [0u8; 32];
    rand::rng().fill_bytes(&mut token);
    let plain = format!("stc_{}", hex::encode(token));
    let digest = sha256_of(plain.as_bytes());
    Ok((plain, digest))
}

pub fn registration_token_matches(plain: &str, expected_hash: &[u8]) -> bool {
    let digest = sha256_of(plain.as_bytes());
    constant_time_eq(&digest, expected_hash)
}

fn sha256_first16(bytes: &[u8]) -> [u8; 16] {
    let digest = Sha256::digest(bytes);
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    out
}

fn sha256_of(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    left.len() == right.len() && bool::from(left.ct_eq(right))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signer_load_or_create_and_sign_verify_roundtrip() {
        let dir = std::env::temp_dir().join(format!("santaizi-signer-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("signing.key");
        let signer = Signer::load_or_create(path.to_str().unwrap()).unwrap();
        assert_eq!(std::fs::read(&path).unwrap().len(), 32);
        let reloaded = Signer::load_or_create(path.to_str().unwrap()).unwrap();
        assert_eq!(reloaded.key_id(), signer.key_id());

        let node_uuid = [9u8; 16];
        let credential = signer.sign(&node_uuid, 7, 1_000, 600).unwrap();
        let ok = verify_credential(
            &signer.public_key(),
            &signer.key_id(),
            &credential,
            1_100,
            300,
            false,
        )
        .unwrap();
        assert!(!ok.in_grace);
        assert_eq!(ok.claims.config_version, 7);

        // 过期且无宽限授权
        assert!(matches!(
            verify_credential(
                &signer.public_key(),
                &signer.key_id(),
                &credential,
                1_700,
                300,
                false
            ),
            Err(CredentialError::Expired)
        ));
        // 过期但曾授权 → 宽限期
        let grace = verify_credential(
            &signer.public_key(),
            &signer.key_id(),
            &credential,
            1_700,
            300,
            true,
        )
        .unwrap();
        assert!(grace.in_grace);
        // 时钟过于超前 → 未生效
        assert!(matches!(
            verify_credential(
                &signer.public_key(),
                &signer.key_id(),
                &credential,
                0,
                300,
                false
            ),
            Err(CredentialError::NotActive)
        ));
        // key_id 不匹配
        assert!(matches!(
            verify_credential(
                &signer.public_key(),
                &[0u8; 16],
                &credential,
                1_100,
                300,
                false
            ),
            Err(CredentialError::KeyIdMismatch)
        ));
        // 篡改签名
        let mut tampered = credential.clone();
        tampered.signature[0] ^= 0xff;
        assert!(matches!(
            verify_credential(
                &signer.public_key(),
                &signer.key_id(),
                &tampered,
                1_100,
                300,
                false
            ),
            Err(CredentialError::SignatureInvalid)
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn registration_token_hash_matches_once() {
        let (plain, hash) = new_registration_token().unwrap();
        assert!(plain.starts_with("stc_"));
        assert_eq!(plain.len(), 4 + 64);
        assert!(registration_token_matches(&plain, &hash));
        assert!(!registration_token_matches("stc_wrong", &hash));
    }
}
