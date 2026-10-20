//! GeoIP（对齐 Go `pkg/geoip/geoip.go` 并扩展）：双格式库加载 + 国家码/ASN 查询。
//!
//! 加载序：`SANTAIZI_GEOIP_DB` 外部指定 → 内嵌库（release 构建期由 CI 拉真库替换
//! `resource/geoip.db` 占位）→ 数据目录缓存 → GitHub 多源自愈下载（`spawn_blocking`，
//! `SANTAIZI_GEOIP_DOWNLOAD=0|off|false` 可关）。全失败静默降级（查询回空，不 panic）。

mod download;
mod mmdb;
mod v2ray;

use std::net::IpAddr;
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::proto;

/// 占位 4 字节 stub 解析必失败；release 构建前 `script/fetch-geoip.sh` 会替换成真库。
const EMBEDDED: &[u8] = include_bytes!("../../resource/geoip.db");

static DB: RwLock<Option<Arc<GeoDb>>> = RwLock::new(None);

pub enum GeoDb {
    Mmdb(mmdb::Mmdb),
    // Index 含 162 个前缀桶，装箱避免拖大枚举变体（clippy large_enum_variant）。
    V2ray(Box<v2ray::Index>),
}

impl GeoDb {
    fn from_bytes(bytes: Vec<u8>) -> Option<Self> {
        // mmdb 有 metadata 校验，先试；v2ray 解码需非空国家码/前缀才认，避免误判任意字节。
        if let Some(db) = mmdb::Mmdb::from_bytes(bytes.clone()) {
            return Some(Self::Mmdb(db));
        }
        v2ray::Index::decode(&bytes).map(|index| Self::V2ray(Box::new(index)))
    }

    fn country_code(&self, ip: IpAddr) -> String {
        match self {
            Self::Mmdb(db) => db.country_code(ip),
            Self::V2ray(db) => db.country_code(ip),
        }
    }

    fn asn(&self, ip: IpAddr) -> Option<String> {
        match self {
            Self::Mmdb(db) => db.asn(ip),
            // v2ray 系列库无 ASN 树。
            Self::V2ray(_) => None,
        }
    }
}

fn load_path(path: &Path) -> Option<GeoDb> {
    let bytes = std::fs::read(path).ok()?;
    GeoDb::from_bytes(bytes)
}

fn install(db: GeoDb) {
    if let Ok(mut slot) = DB.write() {
        *slot = Some(Arc::new(db));
    }
}

fn current() -> Option<Arc<GeoDb>> {
    DB.read().ok().and_then(|slot| slot.as_ref().cloned())
}

/// Go `twoLetter`：恰好两位字母 → 小写；其余（如 `private`）回空。
pub(crate) fn normalize_code(raw: Option<&str>) -> String {
    let Some(raw) = raw.map(str::trim) else {
        return String::new();
    };
    if raw.len() == 2 && raw.chars().all(|c| c.is_ascii_alphabetic()) {
        return raw.to_ascii_lowercase();
    }
    String::new()
}

/// Go `FormatASN`：`4134` / `as4134` / `AS4134` → `AS4134`。
pub(crate) fn format_asn(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    let digits = trimmed
        .strip_prefix("AS")
        .or_else(|| trimmed.strip_prefix("as"))
        .unwrap_or(trimmed);
    if digits.is_empty() || !digits.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(format!("AS{digits}"))
}

fn download_disabled() -> bool {
    std::env::var("SANTAIZI_GEOIP_DOWNLOAD")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no"
            )
        })
        .unwrap_or(false)
}

/// 装载 GeoIP 库。`external` 来自 env `SANTAIZI_GEOIP_DB`（`Config::geoip_db_path`）；
/// `data_dir` 是自愈下载缓存目录（`--db` 所在目录）。
pub fn bootstrap(external: Option<&str>, data_dir: &Path) {
    if let Some(path) = external.map(str::trim).filter(|value| !value.is_empty()) {
        match load_path(Path::new(path)) {
            Some(db) => {
                tracing::info!(path, "GeoIP 已从外部库加载");
                install(db);
                return;
            }
            None => tracing::warn!(path, "外部 GeoIP 库不可用，回退内嵌库"),
        }
    }
    if let Some(db) = GeoDb::from_bytes(EMBEDDED.to_vec()) {
        tracing::info!("GeoIP 已加载内嵌库");
        install(db);
        return;
    }
    let cache = data_dir.join("geoip.db");
    if let Some(db) = load_path(&cache) {
        tracing::info!(path = %cache.display(), "GeoIP 已加载本地缓存库");
        install(db);
        return;
    }
    if download_disabled() {
        tracing::info!("GeoIP 库缺失且自愈下载已关闭");
        return;
    }
    tokio::task::spawn_blocking(move || {
        if download::fetch_first(download::SOURCES, &cache).is_none() {
            tracing::warn!("GeoIP 自愈下载失败（全部源不可用）");
            return;
        }
        match load_path(&cache) {
            Some(db) => {
                tracing::info!(path = %cache.display(), "GeoIP 自愈下载成功");
                install(db);
            }
            None => tracing::warn!(path = %cache.display(), "GeoIP 自愈下载产物无法解析"),
        }
    });
}

/// IP 字符串 → IpAddr；解析失败返回 None。
fn parse_ip(host: &str) -> Option<IpAddr> {
    host.trim().parse().ok()
}

/// Go `LookupCodeFromAddr`：IP（或 `v4/v6` 捆绑串，优先 v4）→ 两字母国家码。
pub fn lookup_code_from_addr(addr: Option<&str>) -> String {
    let Some(raw) = addr.map(str::trim).filter(|value| !value.is_empty()) else {
        return String::new();
    };
    let mut v4: Option<IpAddr> = None;
    let mut v6: Option<IpAddr> = None;
    for part in raw.split('/') {
        let Some(ip) = parse_ip(part) else { continue };
        match ip {
            IpAddr::V4(_) => {
                if v4.is_none() {
                    v4 = Some(ip);
                }
            }
            IpAddr::V6(_) => {
                if v6.is_none() {
                    v6 = Some(ip);
                }
            }
        }
    }
    let Some(ip) = v4.or(v6) else {
        return String::new();
    };
    lookup_code(ip)
}

pub fn lookup_code(ip: IpAddr) -> String {
    current().map(|db| db.country_code(ip)).unwrap_or_default()
}

/// 探测 hop 的 ASN（`AS4134` 形态；mmdb 含 ASN 字段时可用）。
pub fn lookup_asn(ip: IpAddr) -> Option<String> {
    current().and_then(|db| db.asn(ip))
}

/// V2 HOST 帧的国家码兜底（Go `enrichPBHost`）：country_code 为空时按 IP 补。
pub fn enrich_country_code(host: &mut proto::Host, peer_ip: Option<&str>) {
    if !host.country_code.is_empty() {
        return;
    }
    let candidate = if !host.ip.is_empty() {
        Some(host.ip.as_str())
    } else {
        peer_ip
    };
    host.country_code = lookup_code_from_addr(candidate);
}

/// 测试辅助：直接装入库（不走 env/网络）。
#[cfg(test)]
pub(crate) fn install_for_test(db: GeoDb) {
    install(db);
}

/// 测试辅助：清库。
#[cfg(test)]
pub(crate) fn reset_for_test() {
    if let Ok(mut slot) = DB.write() {
        *slot = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_and_garbage_are_invalid() {
        assert!(GeoDb::from_bytes(b"stub".to_vec()).is_none());
        assert!(GeoDb::from_bytes(vec![0u8; 256]).is_none());
    }

    #[test]
    fn normalize_and_format_rules() {
        assert_eq!(normalize_code(Some("CN")), "cn");
        assert_eq!(normalize_code(Some(" uk ")), "uk");
        assert_eq!(normalize_code(Some("private")), "");
        assert_eq!(normalize_code(Some("C")), "");
        assert_eq!(format_asn("4134").as_deref(), Some("AS4134"));
        assert_eq!(format_asn("as4134").as_deref(), Some("AS4134"));
        assert_eq!(format_asn("AS4134").as_deref(), Some("AS4134"));
        assert_eq!(format_asn("Google LLC"), None);
    }

    #[test]
    fn bundle_prefers_ipv4() {
        reset_for_test();
        let list = v2ray::GeoIpList {
            entry: vec![
                v2ray::GeoIp {
                    country_code: "CN".into(),
                    cidr: vec![v2ray::Cidr {
                        ip: "2001:db8::"
                            .parse::<std::net::Ipv6Addr>()
                            .unwrap()
                            .octets()
                            .to_vec(),
                        prefix: 32,
                    }],
                },
                v2ray::GeoIp {
                    country_code: "US".into(),
                    cidr: vec![v2ray::Cidr {
                        ip: vec![8, 8, 8, 0],
                        prefix: 24,
                    }],
                },
            ],
        };
        install_for_test(GeoDb::V2ray(Box::new(
            v2ray::Index::decode(&prost_encode(list)).unwrap(),
        )));
        // Go `queryIPFromBundle`：v4/v6 捆绑串优先 v4。
        assert_eq!(lookup_code_from_addr(Some("2001:db8::5/8.8.8.8")), "us");
        assert_eq!(lookup_code_from_addr(Some("2001:db8::5")), "cn");
        assert_eq!(lookup_code_from_addr(Some("not-an-ip")), "");
        reset_for_test();
    }

    #[test]
    fn external_path_loads_v2ray_dat() {
        let list = v2ray::GeoIpList {
            entry: vec![v2ray::GeoIp {
                country_code: "US".into(),
                cidr: vec![v2ray::Cidr {
                    ip: vec![8, 8, 8, 0],
                    prefix: 24,
                }],
            }],
        };
        let dir = std::env::temp_dir().join(format!("santaizi-geoip-ext-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ext.dat");
        std::fs::write(&path, prost_encode(list)).unwrap();
        reset_for_test();
        bootstrap(Some(path.to_str().unwrap()), &dir);
        assert_eq!(lookup_code_from_addr(Some("8.8.8.8")), "us");
        reset_for_test();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn enrich_keeps_existing_country_code() {
        let mut host = proto::Host {
            country_code: "CN".into(),
            ip: "1.2.3.4".into(),
            ..Default::default()
        };
        enrich_country_code(&mut host, Some("8.8.8.8"));
        assert_eq!(host.country_code, "CN", "已有码不覆盖");
    }

    fn prost_encode(list: v2ray::GeoIpList) -> Vec<u8> {
        use prost::Message as _;
        list.encode_to_vec()
    }
}
