//! v2ray 系列 GeoIP（`geoip.dat` 等 `GeoIPList` protobuf）：查询按前缀长分桶做最长前缀匹配；
//! country_code 规范为两位小写（`private` 等非 ISO 码丢弃）。mmdb 形态的 `geoip.db` 走 mmdb 模块。

use std::collections::HashMap;
use std::net::IpAddr;

use prost::Message as _;

#[derive(Clone, PartialEq, prost::Message)]
pub struct Cidr {
    #[prost(bytes = "vec", tag = "1")]
    pub ip: Vec<u8>,
    #[prost(uint32, tag = "2")]
    pub prefix: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct GeoIp {
    #[prost(string, tag = "1")]
    pub country_code: String,
    #[prost(message, repeated, tag = "2")]
    pub cidr: Vec<Cidr>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct GeoIpList {
    #[prost(message, repeated, tag = "1")]
    pub entry: Vec<GeoIp>,
}

pub struct Index {
    v4: [HashMap<u32, String>; 33],
    v6: [HashMap<u128, String>; 129],
}

impl Index {
    fn new() -> Self {
        Self {
            v4: std::array::from_fn(|_| HashMap::new()),
            v6: std::array::from_fn(|_| HashMap::new()),
        }
    }

    /// 解码并建索引；空表 / 无合法国家码视为无效（避免把任意字节误判为 v2ray 库）。
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let list = GeoIpList::decode(bytes).ok()?;
        let mut index = Self::new();
        let mut codes = 0usize;
        let mut cidrs = 0usize;
        for entry in &list.entry {
            let code = crate::geoip::normalize_code(Some(entry.country_code.as_str()));
            if code.is_empty() {
                continue;
            }
            codes += 1;
            for cidr in &entry.cidr {
                if index.insert(&cidr.ip, cidr.prefix, &code) {
                    cidrs += 1;
                }
            }
        }
        if codes == 0 || cidrs == 0 {
            return None;
        }
        Some(index)
    }

    fn insert(&mut self, ip: &[u8], prefix: u32, code: &str) -> bool {
        match ip.len() {
            4 if prefix <= 32 => {
                let network = u32::from_be_bytes([ip[0], ip[1], ip[2], ip[3]]);
                self.v4[prefix as usize].insert(mask_v4(network, prefix), code.to_string());
                true
            }
            16 if prefix <= 128 => {
                let mut octets = [0u8; 16];
                octets.copy_from_slice(ip);
                self.v6[prefix as usize].insert(
                    mask_v6(u128::from_be_bytes(octets), prefix),
                    code.to_string(),
                );
                true
            }
            _ => false,
        }
    }

    pub fn country_code(&self, ip: IpAddr) -> String {
        match ip {
            IpAddr::V4(v4) => {
                let network = u32::from(v4);
                for prefix in (0..=32usize).rev() {
                    let masked = mask_v4(network, prefix as u32);
                    if let Some(code) = self.v4[prefix].get(&masked) {
                        return code.clone();
                    }
                }
            }
            IpAddr::V6(v6) => {
                let network = u128::from(v6);
                for prefix in (0..=128usize).rev() {
                    let masked = mask_v6(network, prefix as u32);
                    if let Some(code) = self.v6[prefix].get(&masked) {
                        return code.clone();
                    }
                }
            }
        }
        String::new()
    }
}

fn mask_v4(network: u32, prefix: u32) -> u32 {
    if prefix == 0 {
        0
    } else {
        network & (u32::MAX << (32 - prefix))
    }
}

fn mask_v6(network: u128, prefix: u32) -> u128 {
    if prefix == 0 {
        0
    } else {
        network & (u128::MAX << (128 - prefix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(entries: Vec<GeoIp>) -> Vec<u8> {
        GeoIpList { entry: entries }.encode_to_vec()
    }

    fn cidr(ip: [u8; 4], prefix: u32) -> Cidr {
        Cidr {
            ip: ip.to_vec(),
            prefix,
        }
    }

    #[test]
    fn longest_prefix_wins() {
        let bytes = encoded(vec![
            GeoIp {
                country_code: "US".into(),
                cidr: vec![cidr([10, 0, 0, 0], 8)],
            },
            GeoIp {
                country_code: "CN".into(),
                cidr: vec![cidr([10, 1, 2, 0], 24)],
            },
        ]);
        let index = Index::decode(&bytes).unwrap();
        assert_eq!(index.country_code("10.1.2.3".parse().unwrap()), "cn");
        assert_eq!(index.country_code("10.9.9.9".parse().unwrap()), "us");
        assert_eq!(index.country_code("11.0.0.1".parse().unwrap()), "");
    }

    #[test]
    fn ipv6_and_non_iso_codes() {
        let bytes = encoded(vec![
            GeoIp {
                country_code: "private".into(),
                cidr: vec![Cidr {
                    ip: vec![0xfd; 16],
                    prefix: 8,
                }],
            },
            GeoIp {
                country_code: "DE".into(),
                cidr: vec![Cidr {
                    ip: "2001:db8::"
                        .parse::<std::net::Ipv6Addr>()
                        .unwrap()
                        .octets()
                        .to_vec(),
                    prefix: 32,
                }],
            },
        ]);
        let index = Index::decode(&bytes).unwrap();
        assert_eq!(index.country_code("2001:db8::1".parse().unwrap()), "de");
        // 非 ISO 码整组丢弃，不参与匹配。
        assert_eq!(index.country_code("fd00::1".parse().unwrap()), "");
    }

    #[test]
    fn empty_or_garbage_is_rejected() {
        assert!(Index::decode(b"stub").is_none());
        assert!(
            Index::decode(&encoded(vec![GeoIp {
                country_code: "private".into(),
                cidr: vec![cidr([127, 0, 0, 0], 8)],
            }]))
            .is_none()
        );
    }
}
