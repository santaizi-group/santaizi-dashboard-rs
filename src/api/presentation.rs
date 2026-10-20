//! 服务器运行态展示：对齐 Go `controller.runtimeForServer` /
//! `hostAdminDTO` / `model.PB2Host` / `model.PB2State`。
//!
//! 公开端点与 admin 端点共用同一套映射；payload 是 `server_runtimes`
//! 里的 prost 编码（Go 是内存里的 `*model.Host` / `*model.HostState`）。

use std::net::IpAddr;

use prost::Message as _;
use serde_json::{Value, json};

use crate::repo::servers::PublicServerRow;
use crate::util::time as util_time;

pub const HOST_STATE_UNKNOWN: &str = "unknown";
pub const HOST_STATE_ONLINE: &str = "online";
pub const CONNECTIVITY_UNKNOWN: &str = "unknown";
pub const CONNECTIVITY_FULL: &str = "full";
pub const CONNECTIVITY_PARTIAL: &str = "partial";

/// Go `runtimeServerResponse` 的 Rust 等价物（V2 共识 + 运行态兜底）。
#[derive(Debug, Clone, Default)]
pub struct RuntimePresentation {
    pub node_uuid: Option<Vec<u8>>,
    pub protocol: String,
    pub host_state: String,
    pub connectivity: String,
    pub available: Option<bool>,
    pub coverage: String,
    pub last_collected_at: i64,
    pub last_received_at: i64,
}

/// Go `runtimeForServer`：V2 只认已封窗的可用性桶（进行中的桶不是共识），
/// 非 V2 协议回落运行态列。
pub fn runtime_presentation(row: &PublicServerRow) -> RuntimePresentation {
    let mut presentation = RuntimePresentation {
        node_uuid: None,
        protocol: String::new(),
        host_state: HOST_STATE_UNKNOWN.to_string(),
        connectivity: CONNECTIVITY_UNKNOWN.to_string(),
        available: None,
        coverage: "unknown".to_string(),
        last_collected_at: 0,
        last_received_at: 0,
    };
    if let (Some(protocol), Some(node_uuid)) = (&row.protocol, &row.node_uuid) {
        presentation.protocol = protocol.clone();
        presentation.node_uuid = Some(node_uuid.clone());
        presentation.last_collected_at = row.last_collected_at.unwrap_or(0);
        presentation.last_received_at = row.last_received_at.unwrap_or(0);
        // V1 没有可用性桶，沿用运行态。
        if protocol != "v2" {
            presentation.host_state = row
                .runtime_host_state
                .clone()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| HOST_STATE_UNKNOWN.to_string());
            presentation.connectivity = row
                .runtime_connectivity_state
                .clone()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| CONNECTIVITY_UNKNOWN.to_string());
        }
    }
    let has_v2_node = presentation.protocol == "v2"
        && presentation
            .node_uuid
            .as_ref()
            .is_some_and(|uuid| uuid.len() == 16);
    if !has_v2_node {
        return presentation;
    }
    let (Some(host_state), Some(connectivity)) = (
        row.bucket_host_state.clone(),
        row.bucket_connectivity_state.clone(),
    ) else {
        return presentation;
    };
    presentation.host_state = host_state;
    presentation.connectivity = connectivity.clone();
    let expected = row.bucket_expected_observers.unwrap_or(0);
    let seen = row.bucket_seen_observers.unwrap_or(0);
    presentation.coverage = if expected == 0 {
        "unknown".to_string()
    } else {
        format!("{seen}/{expected}")
    };
    if presentation.connectivity != CONNECTIVITY_UNKNOWN {
        presentation.available = Some(matches!(
            presentation.connectivity.as_str(),
            CONNECTIVITY_FULL | CONNECTIVITY_PARTIAL
        ));
    }
    presentation
}

/// Go `serverOnlineFlag`：V2 绑定节点只认共识 host_state；其余回落
/// `LastActive` 离线阈值（Rust 产品无 V1 探针，仅兜底）。
pub fn online_flag(presentation: &RuntimePresentation, offline_threshold_seconds: u64) -> bool {
    let has_v2_node = presentation.protocol == "v2"
        && presentation
            .node_uuid
            .as_ref()
            .is_some_and(|uuid| !uuid.is_empty());
    if has_v2_node {
        return presentation.host_state == HOST_STATE_ONLINE;
    }
    if presentation.last_received_at <= 0 {
        return false;
    }
    let threshold = offline_threshold_seconds as i128 * 1_000_000_000;
    (util_time::now_nanos() as i128 - presentation.last_received_at as i128) < threshold
}

/// Go `publicHostView`：匿名/查看会话的 Host 不含 IP；Bearer Token
/// （`reveal_ip`）追加 `ip` / `ipv4` / `ipv6`（对齐 `hostAdminDTO`）。
pub fn host_json(host_payload: Option<&[u8]>, reveal_ip: bool) -> Value {
    let Some(payload) = host_payload.filter(|payload| !payload.is_empty()) else {
        return Value::Null;
    };
    let Ok(host) = crate::proto::Host::decode(payload) else {
        return Value::Null;
    };
    let mut view = json!({
        "Platform": host.platform,
        "PlatformVersion": host.platform_version,
        "CPU": host.cpu,
        "MemTotal": host.mem_total,
        "DiskTotal": host.disk_total,
        "SwapTotal": host.swap_total,
        "Arch": host.arch,
        "Virtualization": host.virtualization,
        "BootTime": host.boot_time,
        "CountryCode": host.country_code,
        "Version": host.version,
        "GPU": host.gpu,
    });
    if reveal_ip && !host.ip.trim().is_empty() {
        let ip = host.ip.trim();
        if let Some(object) = view.as_object_mut() {
            object.insert("ip".to_string(), json!(ip));
            let (ipv4, ipv6) = split_ip_addr(ip);
            if !ipv4.is_empty() {
                object.insert("ipv4".to_string(), json!(ipv4));
            }
            if !ipv6.is_empty() {
                object.insert("ipv6".to_string(), json!(ipv6));
            }
        }
    }
    view
}

/// Go `PB2State` 的 JSON 形态（前端 Status 页消费的字段集）。
pub fn state_json(state_payload: Option<&[u8]>) -> Value {
    let Some(payload) = state_payload.filter(|payload| !payload.is_empty()) else {
        return Value::Null;
    };
    let Ok(state) = crate::proto::State::decode(payload) else {
        return Value::Null;
    };
    json!({
        "CPU": state.cpu,
        "MemUsed": state.mem_used,
        "SwapUsed": state.swap_used,
        "DiskUsed": state.disk_used,
        "NetInTransfer": state.net_in_transfer,
        "NetOutTransfer": state.net_out_transfer,
        "NetInSpeed": state.net_in_speed,
        "NetOutSpeed": state.net_out_speed,
        "Uptime": state.uptime,
        "Load1": state.load1,
        "Load5": state.load5,
        "Load15": state.load15,
        "TcpConnCount": state.tcp_conn_count,
        "UdpConnCount": state.udp_conn_count,
        "ProcessCount": state.process_count,
        "Temperatures": state
            .temperatures
            .iter()
            .map(|t| json!({"Name": t.name, "Temperature": t.temperature}))
            .collect::<Vec<_>>(),
        "GPU": state.gpu,
    })
}

/// 遥测展示子对象（Go `TelemetryPresentation`）。
pub fn telemetry_json(presentation: &RuntimePresentation) -> Value {
    json!({
        "host": presentation.host_state,
        "connectivity": presentation.connectivity,
        "available": presentation.available,
        "coverage": presentation.coverage,
    })
}

/// Go `decodePublicNote`：公开备注为 JSON 原文；非法 JSON 包一层 `text`。
pub fn decode_public_note(raw: &str) -> Value {
    if raw.trim().is_empty() {
        return json!({});
    }
    serde_json::from_str(raw).unwrap_or_else(|_| json!({ "text": raw }))
}

/// Go `utils.SplitIPAddr` 的单栈部分：双栈 `v4/v6` 由存储侧拆分。
fn split_ip_addr(value: &str) -> (String, String) {
    match value.parse::<IpAddr>() {
        Ok(IpAddr::V4(_)) => (value.to_string(), String::new()),
        Ok(IpAddr::V6(_)) => (String::new(), value.to_string()),
        Err(_) => (String::new(), String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> PublicServerRow {
        PublicServerRow {
            id: 7,
            name: "node-7".into(),
            tag: "tag1".into(),
            note: String::new(),
            public_note: String::new(),
            display_index: 1,
            hide_for_guest: false,
            monitoring_options_raw: "{}".into(),
            protocol: Some("v2".into()),
            node_uuid: Some(vec![0x21; 16]),
            runtime_host_state: Some("online".into()),
            runtime_connectivity_state: Some("full".into()),
            last_collected_at: Some(10),
            last_received_at: Some(20),
            state_payload: None,
            host_payload: None,
            bucket_host_state: None,
            bucket_connectivity_state: None,
            bucket_seen_observers: None,
            bucket_expected_observers: None,
        }
    }

    #[test]
    fn v2_prefers_closed_bucket_over_runtime_columns() {
        let mut row = row();
        // 无桶：unknown（不是 runtime 的 online/full）。
        let presentation = runtime_presentation(&row);
        assert_eq!(presentation.host_state, HOST_STATE_UNKNOWN);
        assert_eq!(presentation.connectivity, CONNECTIVITY_UNKNOWN);
        assert_eq!(presentation.coverage, "unknown");
        assert!(presentation.available.is_none());

        row.bucket_host_state = Some("offline".into());
        row.bucket_connectivity_state = Some("partial".into());
        row.bucket_expected_observers = Some(3);
        row.bucket_seen_observers = Some(2);
        let presentation = runtime_presentation(&row);
        assert_eq!(presentation.host_state, "offline");
        assert_eq!(presentation.connectivity, CONNECTIVITY_PARTIAL);
        assert_eq!(presentation.available, Some(true));
        assert_eq!(presentation.coverage, "2/3");
    }

    #[test]
    fn non_v2_protocol_falls_back_to_runtime_columns() {
        let mut row = row();
        row.protocol = Some("v1".into());
        let presentation = runtime_presentation(&row);
        assert_eq!(presentation.host_state, "online");
        assert_eq!(presentation.connectivity, CONNECTIVITY_FULL);
    }

    #[test]
    fn no_runtime_row_stays_unknown() {
        let mut row = row();
        row.protocol = None;
        row.node_uuid = None;
        let presentation = runtime_presentation(&row);
        assert_eq!(presentation.protocol, "");
        assert_eq!(presentation.host_state, HOST_STATE_UNKNOWN);
        assert!(!online_flag(&presentation, 30));
    }

    #[test]
    fn host_and_state_json_decode_payloads() {
        use prost::Message as _;
        let host = crate::proto::Host {
            platform: "debian".into(),
            cpu: vec!["x".into()],
            mem_total: 8,
            ip: "1.2.3.4".into(),
            ..Default::default()
        };
        let host_payload = host.encode_to_vec();
        let anon = host_json(Some(&host_payload), false);
        assert_eq!(anon["Platform"], "debian");
        assert!(anon.get("ip").is_none(), "匿名不暴露 IP");
        let revealed = host_json(Some(&host_payload), true);
        assert_eq!(revealed["ip"], "1.2.3.4");
        assert_eq!(revealed["ipv4"], "1.2.3.4");
        assert!(revealed.get("ipv6").is_none());
        assert!(host_json(None, true).is_null());
        assert!(host_json(Some(b"garbage"), true).is_null());

        let state = crate::proto::State {
            cpu: 12.5,
            uptime: 99,
            ..Default::default()
        };
        let state_view = state_json(Some(&state.encode_to_vec()));
        assert_eq!(state_view["CPU"], 12.5);
        assert_eq!(state_view["Uptime"], 99);
        assert!(state_json(None).is_null());
    }

    #[test]
    fn public_note_decode_shapes() {
        assert_eq!(decode_public_note(""), json!({}));
        assert_eq!(decode_public_note("  "), json!({}));
        assert_eq!(
            decode_public_note(r#"{"text":"hi"}"#),
            json!({"text": "hi"})
        );
        assert_eq!(decode_public_note("not json"), json!({"text": "not json"}));
    }

    #[test]
    fn v2_online_flag_follows_consensus() {
        let mut row = row();
        row.bucket_host_state = Some("offline".into());
        row.bucket_connectivity_state = Some("unknown".into());
        let presentation = runtime_presentation(&row);
        assert!(!online_flag(&presentation, 30));
        row.bucket_host_state = Some("online".into());
        row.bucket_connectivity_state = Some("full".into());
        let presentation = runtime_presentation(&row);
        assert!(online_flag(&presentation, 30));
    }
}
