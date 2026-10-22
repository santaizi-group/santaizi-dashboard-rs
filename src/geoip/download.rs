//! GitHub 多源自助下载（release CI 与启动自愈共用源表）。
//! 逐源顺序拉取，带 `.sha256sum` 伴生校验（若有），临时文件原子落盘。

use std::path::{Path, PathBuf};

use sha2::Digest as _;

/// 带生命周期的源描述：`SOURCES` 是 `'static`，测试里的本地 stub 用短生命周期即可，
/// 不必再靠 `Box::leak` 伪造 `'static`（那会在进程里永久泄漏内存）。
pub struct Source<'a> {
    pub url: &'a str,
    /// 可选 sha256 伴生文件（`<hex>  <filename>` 或纯 hex）。
    pub checksum_url: Option<&'a str>,
}

/// 源表顺序即回退顺序；全部来自 GitHub release 资产，免令牌。
pub const SOURCES: &[Source<'_>] = &[
    Source {
        url: "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat",
        checksum_url: Some(
            "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat.sha256sum",
        ),
    },
    Source {
        url: "https://github.com/MetaCubeX/meta-rules-dat/releases/latest/download/geoip.db",
        checksum_url: None,
    },
    Source {
        url: "https://github.com/v2fly/geoip/releases/latest/download/geoip.dat",
        checksum_url: Some(
            "https://github.com/v2fly/geoip/releases/latest/download/geoip.dat.sha256sum",
        ),
    },
    Source {
        url: "https://github.com/P3TERX/GeoLite.mmdb/releases/latest/download/GeoLite2-Country.mmdb",
        checksum_url: None,
    },
];

/// 拉取成功返回落盘路径；全部源失败返回 None。
pub fn fetch_first(sources: &[Source<'_>], dest: &Path) -> Option<PathBuf> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .user_agent(concat!("santaizi-dashboard/", env!("CARGO_PKG_VERSION")))
        .build()
        .ok()?;
    for source in sources {
        match fetch_one(&client, source, dest) {
            Ok(path) => {
                tracing::info!(url = source.url, path = %path.display(), "GeoIP 库下载成功");
                return Some(path);
            }
            Err(err) => {
                tracing::warn!(url = source.url, "GeoIP 库下载失败：{err}");
            }
        }
    }
    None
}

fn fetch_one(
    client: &reqwest::blocking::Client,
    source: &Source<'_>,
    dest: &Path,
) -> Result<PathBuf, String> {
    let response = client
        .get(source.url)
        .send()
        .map_err(|err| err.to_string())?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }
    let bytes = response.bytes().map_err(|err| err.to_string())?;
    if bytes.len() < 64 {
        return Err(format!("payload too small: {} bytes", bytes.len()));
    }
    if let Some(checksum_url) = source.checksum_url {
        let expected = fetch_checksum(client, checksum_url)?;
        let actual = hex::encode(sha2::Sha256::digest(&bytes));
        if !actual.eq_ignore_ascii_case(&expected) {
            return Err("sha256 mismatch".to_string());
        }
    }
    let tmp = dest.with_file_name("geoip.download.tmp");
    if let Some(parent) = tmp.parent() {
        std::fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    std::fs::write(&tmp, &bytes).map_err(|err| err.to_string())?;
    std::fs::rename(&tmp, dest).map_err(|err| err.to_string())?;
    Ok(dest.to_path_buf())
}

fn fetch_checksum(client: &reqwest::blocking::Client, url: &str) -> Result<String, String> {
    let response = client.get(url).send().map_err(|err| err.to_string())?;
    if !response.status().is_success() {
        return Err(format!("checksum HTTP {}", response.status()));
    }
    let text = response.text().map_err(|err| err.to_string())?;
    text.split_whitespace()
        .next()
        .map(|token| token.trim().to_string())
        .filter(|token| token.len() == 64 && token.chars().all(|c| c.is_ascii_hexdigit()))
        .ok_or_else(|| "checksum malformed".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;

    /// 极简 HTTP/1.1 stub：按路径回预设响应。
    fn spawn_stub(routes: Vec<(String, (u16, Vec<u8>))>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming().take(routes.len() + 4) {
                let Ok(mut stream) = stream else { continue };
                let mut buf = [0u8; 4096];
                let Ok(n) = stream.read(&mut buf) else {
                    continue;
                };
                let request = String::from_utf8_lossy(&buf[..n]).to_string();
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/")
                    .to_string();
                let (status, body) = routes
                    .iter()
                    .find(|(route, _)| *route == path)
                    .map(|(_, res)| res.clone())
                    .unwrap_or((404, b"missing".to_vec()));
                let reason = if status == 200 { "OK" } else { "ERR" };
                let header = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.write_all(&body);
            }
        });
        format!("http://{addr}")
    }

    fn payload() -> Vec<u8> {
        vec![7u8; 128]
    }

    #[test]
    fn falls_back_to_next_source_and_renames_atomically() {
        let good = payload();
        let checksum = hex::encode(sha2::Sha256::digest(&good));
        let base = spawn_stub(vec![
            ("/bad.dat".into(), (500, b"boom".to_vec())),
            ("/good.dat".into(), (200, good.clone())),
            (
                "/good.dat.sha256sum".into(),
                (200, format!("{checksum}  good.dat").into_bytes()),
            ),
        ]);
        let dest = std::env::temp_dir().join(format!("santaizi-geoip-dl-{}", std::process::id()));
        let sources = [
            Source {
                url: &format!("{base}/bad.dat"),
                checksum_url: None,
            },
            Source {
                url: &format!("{base}/good.dat"),
                checksum_url: Some(&format!("{base}/good.dat.sha256sum")),
            },
        ];
        let path = fetch_first(&sources, &dest).expect("第二源应成功");
        assert_eq!(std::fs::read(&path).unwrap(), good);
        assert!(!dest.with_file_name("geoip.download.tmp").exists());
        let _ = std::fs::remove_file(&dest);
    }

    #[test]
    fn checksum_mismatch_rejects_source() {
        let good = payload();
        let base = spawn_stub(vec![
            ("/good.dat".into(), (200, good.clone())),
            (
                "/good.dat.sha256sum".into(),
                (200, format!("{}  good.dat", "0".repeat(64)).into_bytes()),
            ),
        ]);
        let dest =
            std::env::temp_dir().join(format!("santaizi-geoip-dl-bad-{}", std::process::id()));
        let sources = [Source {
            url: &format!("{base}/good.dat"),
            checksum_url: Some(&format!("{base}/good.dat.sha256sum")),
        }];
        assert!(fetch_first(&sources, &dest).is_none());
        assert!(!dest.exists());
        let _ = std::fs::remove_file(&dest);
    }
}
