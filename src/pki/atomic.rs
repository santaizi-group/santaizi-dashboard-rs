//! 原子文件写入：同目录临时文件 + rename + fsync 目录，对齐 Go `service/pki/atomic.go`。

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;

use crate::Result;

pub fn write_file_atomic(path: &Path, data: &[u8], perm: u32) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("path has no parent directory: {}", path.display()))?;
    fs::create_dir_all(parent).map_err(|e| anyhow::anyhow!("create {}: {e}", parent.display()))?;
    let tmp_path = parent.join(format!(".pki-{}.tmp", std::process::id()));
    {
        let tmp = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|e| anyhow::anyhow!("create temp file in {}: {e}", parent.display()))?;
        write_all_sync(&tmp, data).inspect_err(|_| {
            let _ = fs::remove_file(&tmp_path);
        })?;
        set_permissions(&tmp_path, perm).inspect_err(|_| {
            let _ = fs::remove_file(&tmp_path);
        })?;
    }
    fs::rename(&tmp_path, path)
        .map_err(|e| anyhow::anyhow!("rename to {}: {e}", path.display()))?;
    sync_dir(parent)
}

fn write_all_sync(mut file: &File, data: &[u8]) -> Result<()> {
    file.write_all(data)
        .map_err(|e| anyhow::anyhow!("write temp file: {e}"))?;
    file.sync_all()
        .map_err(|e| anyhow::anyhow!("sync temp file: {e}"))
}

#[cfg(unix)]
fn set_permissions(path: &Path, perm: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(perm))
        .map_err(|e| anyhow::anyhow!("chmod {}: {e}", path.display()))
}

#[cfg(not(unix))]
fn set_permissions(_path: &Path, _perm: u32) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn sync_dir(dir: &Path) -> Result<()> {
    let handle =
        fs::File::open(dir).map_err(|e| anyhow::anyhow!("open dir {}: {e}", dir.display()))?;
    handle
        .sync_all()
        .map_err(|e| anyhow::anyhow!("fsync dir {}: {e}", dir.display()))
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_creates_and_replaces() {
        let dir = std::env::temp_dir().join(format!("santaizi-atomic-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("nested").join("secret.key");
        write_file_atomic(&path, b"first", 0o600).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"first");
        write_file_atomic(&path, b"second", 0o600).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"second");
        assert!(!path.parent().unwrap().read_dir().unwrap().any(|e| {
            e.unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".pki-")
        }));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
