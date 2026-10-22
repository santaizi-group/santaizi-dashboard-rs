//! 原子文件写入：同目录临时文件 + rename + fsync 目录，对齐 Go `service/pki/atomic.go`。

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::Result;

/// 进程内自增序号：让同目录并发写的临时文件名互不相同。
/// 只按 pid 命名时，同进程两次写会抢同一个临时文件——后一次 truncate
/// 能把前一次还没 rename 的内容清掉，留下空/半截的目标文件。
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub fn write_file_atomic(path: &Path, data: &[u8], perm: u32) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("path has no parent directory: {}", path.display()))?;
    fs::create_dir_all(parent).map_err(|e| anyhow::anyhow!("create {}: {e}", parent.display()))?;
    let tmp_path = parent.join(temp_name(path));
    // 权限必须先落：临时文件按 umask 创建（常见 0644），私钥内容写进去之后
    // 才 chmod，中间那一段窗口里密钥对其他本地用户可读。
    let tmp = create_private(&tmp_path, perm)
        .inspect_err(|_| {
            let _ = fs::remove_file(&tmp_path);
        })
        .map_err(|e| anyhow::anyhow!("create temp file in {}: {e}", parent.display()))?;
    write_all_sync(&tmp, data).inspect_err(|_| {
        let _ = fs::remove_file(&tmp_path);
    })?;
    drop(tmp);
    fs::rename(&tmp_path, path)
        .inspect_err(|_| {
            let _ = fs::remove_file(&tmp_path);
        })
        .map_err(|e| anyhow::anyhow!("rename to {}: {e}", path.display()))?;
    sync_dir(parent)
}

/// 临时名带目标文件名 + pid + 序号：同目录不同目标不互踩，同目标并发也不互踩。
fn temp_name(path: &Path) -> String {
    let stem = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "file".into());
    format!(
        ".pki-{stem}.{}.{}.tmp",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

/// 以目标权限创建临时文件；权限在写入任何内容之前就已生效。
fn create_private(path: &Path, perm: u32) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(perm);
    }
    options.open(path)
}

fn write_all_sync(mut file: &File, data: &[u8]) -> Result<()> {
    file.write_all(data)
        .map_err(|e| anyhow::anyhow!("write temp file: {e}"))?;
    file.sync_all()
        .map_err(|e| anyhow::anyhow!("sync temp file: {e}"))
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

    /// 同目录不同目标、以及同一目标的连续两次写，临时名都必须不同。
    ///
    /// 只按 pid 命名时这些名字全等：后一次 `truncate` 会把前一次还没
    /// rename 的内容清掉，目标文件于是拿到别人的数据（或是空的）。
    #[test]
    fn temp_names_never_collide() {
        let dir = std::env::temp_dir().join(format!("santaizi-atomic-name-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let a = temp_name(&dir.join("client.key"));
        let b = temp_name(&dir.join("client.crt"));
        let c = temp_name(&dir.join("client.key"));
        assert_ne!(a, b, "同目录不同目标的临时名不能相同");
        assert_ne!(a, c, "同一目标的连续两次写临时名也不能相同");
        // 名字必须仍以 .pki- 开头：调用方与测试都靠这个前缀识别临时文件。
        for name in [&a, &b, &c] {
            assert!(name.starts_with(".pki-"), "临时名前缀被改坏了: {name}");
            assert!(name.ends_with(".tmp"), "临时名后缀被改坏了: {name}");
        }
    }
}
