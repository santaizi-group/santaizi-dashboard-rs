//! 业务秘密加密：AES-256-GCM，密钥由主端数据目录持有，永不落库。
//! 布局 `[0x01][12B nonce][ct+tag]`，与 Go `model/secret.go` 一致。

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use rand::Rng;
use std::path::Path;
use std::result::Result as StdResult;
use std::sync::RwLock;

use crate::Result;

const ENCRYPTED_SECRET_VERSION: u8 = 1;
const BUSINESS_SECRET_KEY_SIZE: usize = 32;

static SECRET_CIPHER: RwLock<Option<Aes256Gcm>> = RwLock::new(None);

#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("business secret key must contain exactly 32 bytes")]
    KeyLength,
    #[error("business secret encryption is not configured")]
    NotConfigured,
    #[error("unsupported or malformed encrypted secret")]
    Malformed,
    #[error("decrypt business secret: authentication failed")]
    Authentication,
}

/// 安装进程级密钥（主端数据目录持有，重启后从 `telemetry.secret_key_path` 重新加载）。
pub fn configure_secret_encryption(key: &[u8]) -> StdResult<(), SecretError> {
    if key.len() != BUSINESS_SECRET_KEY_SIZE {
        return Err(SecretError::KeyLength);
    }
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|_| SecretError::KeyLength)?;
    *SECRET_CIPHER.write().unwrap() = Some(cipher);
    Ok(())
}

/// 从文件加载或生成 32 字节密钥并安装为进程级密钥（文件权限 0600）。
pub fn init_business_secret_encryption(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(SecretError::NotConfigured.into());
    }
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("create secret key directory: {e}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }
    }
    let key = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let mut key = vec![0u8; BUSINESS_SECRET_KEY_SIZE];
            rand::rng().fill_bytes(&mut key);
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(|e| {
                    anyhow::anyhow!("create business secret key {}: {e}", path.display())
                })?;
            use std::io::Write;
            let mut file = file;
            file.write_all(&key)
                .map_err(|e| anyhow::anyhow!("write business secret key: {e}"))?;
            file.sync_all()
                .map_err(|e| anyhow::anyhow!("sync business secret key: {e}"))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
            }
            key
        }
        Err(e) => {
            return Err(anyhow::anyhow!(
                "read business secret key {}: {e}",
                path.display()
            ));
        }
    };
    if key.len() != BUSINESS_SECRET_KEY_SIZE {
        return Err(anyhow::anyhow!(
            "business secret key must contain exactly {BUSINESS_SECRET_KEY_SIZE} bytes"
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    configure_secret_encryption(&key)?;
    Ok(())
}

pub fn encrypt_secret(plain: &str) -> StdResult<Vec<u8>, SecretError> {
    if plain.is_empty() {
        return Ok(Vec::new());
    }
    let guard = SECRET_CIPHER.read().unwrap();
    let Some(cipher) = guard.as_ref() else {
        return Err(SecretError::NotConfigured);
    };
    let mut nonce = [0u8; 12];
    rand::rng().fill_bytes(&mut nonce);
    let mut out = Vec::with_capacity(1 + nonce.len() + plain.len() + 16);
    out.push(ENCRYPTED_SECRET_VERSION);
    out.extend_from_slice(&nonce);
    cipher
        .encrypt(Nonce::from_slice(&nonce), plain.as_bytes())
        .map(|ct| {
            out.extend_from_slice(&ct);
            out
        })
        .map_err(|_| SecretError::Malformed)
}

pub fn decrypt_secret(value: &[u8]) -> StdResult<String, SecretError> {
    if value.is_empty() {
        return Ok(String::new());
    }
    let guard = SECRET_CIPHER.read().unwrap();
    let Some(cipher) = guard.as_ref() else {
        return Err(SecretError::NotConfigured);
    };
    if value[0] != ENCRYPTED_SECRET_VERSION || value.len() < 1 + 12 + 16 {
        return Err(SecretError::Malformed);
    }
    cipher
        .decrypt(Nonce::from_slice(&value[1..13]), &value[13..])
        .map(|plain| String::from_utf8(plain).map_err(|_| SecretError::Malformed))
        .map_err(|_| SecretError::Authentication)?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        configure_secret_encryption(&[7u8; 32]).unwrap();
        let ciphertext = encrypt_secret("stc_secret_value").unwrap();
        assert_eq!(ciphertext[0], 1);
        assert_eq!(decrypt_secret(&ciphertext).unwrap(), "stc_secret_value");
        assert_eq!(decrypt_secret(&[]).unwrap(), "");
        assert_eq!(encrypt_secret("").unwrap(), Vec::<u8>::new());

        let mut tampered = ciphertext.clone();
        *tampered.last_mut().unwrap() ^= 0xff;
        assert!(matches!(
            decrypt_secret(&tampered),
            Err(SecretError::Authentication)
        ));
        assert!(matches!(
            decrypt_secret(&[1u8, 2, 3]),
            Err(SecretError::Malformed)
        ));

        // 不同 nonce → 不同密文
        let again = encrypt_secret("stc_secret_value").unwrap();
        assert_ne!(ciphertext, again);
    }

    #[test]
    fn init_business_secret_encryption_persists_key() {
        let dir = std::env::temp_dir().join(format!("santaizi-secret-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("business.key");
        init_business_secret_encryption(path.to_str().unwrap()).unwrap();
        let first = encrypt_secret("hello").unwrap();
        init_business_secret_encryption(path.to_str().unwrap()).unwrap();
        assert_eq!(decrypt_secret(&first).unwrap(), "hello");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
