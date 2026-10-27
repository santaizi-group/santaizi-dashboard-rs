//! MaxMind mmdb 读取。记录字段容错（Go `pkg/geoip.IPInfo` 子集）：
//! 旧 ipinfo `country.mmdb`（`country_code`/`asn`）、`ipinfo_lite.mmdb`（`country`）、
//! GeoLite2（`continent_code`、`autonomous_system_number` 独立 ASN 库）。

use std::net::IpAddr;

use serde::Deserialize;

/// 数字/字符串双形态（GeoLite2-ASN 的 `autonomous_system_number` 是数字）。
#[derive(Deserialize)]
#[serde(untagged)]
enum StrOrNum {
    Str(String),
    Num(u64),
}

impl StrOrNum {
    fn into_string(self) -> String {
        match self {
            Self::Str(value) => value,
            Self::Num(value) => value.to_string(),
        }
    }
}

#[derive(Deserialize, Default)]
struct Record {
    #[serde(default)]
    country: Option<String>,
    #[serde(default)]
    country_code: Option<String>,
    #[serde(default)]
    continent_code: Option<String>,
    #[serde(default)]
    asn: Option<StrOrNum>,
    #[serde(default)]
    autonomous_system_number: Option<StrOrNum>,
}

pub struct Mmdb {
    reader: maxminddb::Reader<Vec<u8>>,
}

impl Mmdb {
    /// 只校验 metadata、不取所有权。调用方据此决定要不要把 `Vec` 交出来：
    /// 真库有几 MB，先整体 clone 一份会让启动瞬时出现两份堆副本。
    pub fn accepts(bytes: &[u8]) -> bool {
        maxminddb::Reader::from_source(bytes).is_ok()
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Option<Self> {
        maxminddb::Reader::from_source(bytes)
            .ok()
            .map(|reader| Self { reader })
    }

    fn record(&self, ip: IpAddr) -> Option<Record> {
        self.reader.lookup::<Record>(ip).ok()
    }

    /// 国家码（Go `Lookup`：国家码缺 → 洲码兜底）。
    pub fn country_code(&self, ip: IpAddr) -> String {
        let Some(record) = self.record(ip) else {
            return String::new();
        };
        let code = crate::geoip::normalize_code(
            record.country_code.as_deref().or(record.country.as_deref()),
        );
        if !code.is_empty() {
            return code;
        }
        crate::geoip::normalize_code(record.continent_code.as_deref())
    }

    /// ASN（`asn`/`autonomous_system_number`，格式化为 `AS4134`）。
    pub fn asn(&self, ip: IpAddr) -> Option<String> {
        let record = self.record(ip)?;
        let raw = record
            .asn
            .map(StrOrNum::into_string)
            .or_else(|| record.autonomous_system_number.map(StrOrNum::into_string))?;
        crate::geoip::format_asn(&raw)
    }
}
