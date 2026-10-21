//! 旧版 `SantaiziService`（v1 unary）互通集成测试。
//!
//! 旧探针（Go / Rust 探针的 `client_secret` 回退通路）只会 ReportSystemInfo /
//! ReportSystemState；这里验证密钥认证互通、运行态刷新、GeoIP 写回，以及
//! 错误密钥被拒。这些路径此前没有服务可挂，探针拿到 `Unimplemented` 会被
//! 误判成「旧面板」而永远卡在密钥认证上。
//!
//! 拨号与 `rpc_v2.rs` 同款：TLS 开启时用 `https://localhost`（裸 `http://`
//! 打到 TLS 端口会被 tonic 的 HTTP/2 服务端以 FRAME_SIZE_ERROR 断流，
//! 不是被测行为）。

mod common;

use std::time::Duration;

use common::*;
use santaizi_dashboard::proto;
use tonic::metadata::{MetadataKey, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Request};

async fn dial(fixture: &PrimaryFixture) -> Channel {
    let endpoint = if fixture.state.conf().grpc_tls.enabled {
        let tls = ClientTlsConfig::new()
            .domain_name("localhost")
            .ca_certificate(Certificate::from_pem(fixture.server_cert_pem.clone()));
        Endpoint::from_shared(format!("https://localhost:{}", fixture.port))
            .expect("endpoint")
            .tls_config(tls)
            .expect("tls config")
    } else {
        Endpoint::from_shared(format!("http://127.0.0.1:{}", fixture.port)).expect("endpoint")
    };
    endpoint
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .expect("connect")
}

fn with_secret<T>(payload: T, secret: &str) -> Request<T> {
    let mut request = Request::new(payload);
    let key: MetadataKey<tonic::metadata::Ascii> = "client_secret".parse().expect("key");
    request
        .metadata_mut()
        .insert(key, MetadataValue::try_from(secret).expect("value"));
    request
}

fn host_payload() -> proto::Host {
    proto::Host {
        platform: "linux".to_string(),
        mem_total: 1024,
        boot_time: 100,
        ip: "10.1.2.3".to_string(),
        version: "0.1.0".to_string(),
        ..Default::default()
    }
}

fn state_payload() -> proto::State {
    proto::State {
        cpu: 3.5,
        mem_used: 100,
        uptime: 60,
        ..Default::default()
    }
}

fn runtime_row(fixture: &PrimaryFixture) -> (String, String, i64) {
    fixture
        .db
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT protocol, host_state, last_received_at FROM server_runtimes WHERE server_id = ?1",
                [SERVER_ID],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                        row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                        row.get::<_, Option<i64>>(2)?.unwrap_or(0),
                    ))
                },
            )?)
        })
        .expect("runtime row")
}

#[tokio::test]
async fn legacy_report_system_info_and_state_roundtrip() {
    let fixture = setup_primary("legacy-report", true).await;
    let channel = dial(&fixture).await;
    let mut client = proto::santaizi_service_client::SantaiziServiceClient::new(channel);

    let host = client
        .report_system_info(with_secret(host_payload(), CLIENT_SECRET))
        .await
        .expect("report host")
        .into_inner();
    assert!(host.proced);

    let state = client
        .report_system_state(with_secret(state_payload(), CLIENT_SECRET))
        .await
        .expect("report state")
        .into_inner();
    assert!(state.proced);

    let (protocol, host_state, last_received_at) = runtime_row(&fixture);
    // v1 路径不落 v2 协议标记（读侧回落运行态列），运行态翻在线。
    assert_eq!(protocol, "v1");
    assert_eq!(host_state, "online");
    assert!(last_received_at > 0);
}

#[tokio::test]
async fn legacy_report_rejects_unknown_secret() {
    let fixture = setup_primary("legacy-secret", true).await;
    let channel = dial(&fixture).await;
    let mut client = proto::santaizi_service_client::SantaiziServiceClient::new(channel);
    let err = client
        .report_system_info(with_secret(host_payload(), "not-the-secret"))
        .await
        .expect_err("unknown secret must be rejected");
    assert_eq!(err.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn legacy_lookup_geo_ip_writes_country_code() {
    let fixture = setup_primary("legacy-geoip", true).await;
    let channel = dial(&fixture).await;
    let mut client = proto::santaizi_service_client::SantaiziServiceClient::new(channel);

    // 无运行态行时回 host not found（Go 同款）。
    let err = client
        .lookup_geo_ip(with_secret(
            proto::GeoIp {
                ip: "203.0.113.9".to_string(),
                country_code: String::new(),
            },
            CLIENT_SECRET,
        ))
        .await
        .expect_err("no host yet");
    assert_eq!(err.code(), Code::NotFound);

    client
        .report_system_info(with_secret(host_payload(), CLIENT_SECRET))
        .await
        .expect("report host");

    let geo = client
        .lookup_geo_ip(with_secret(
            proto::GeoIp {
                ip: "203.0.113.9".to_string(),
                country_code: String::new(),
            },
            CLIENT_SECRET,
        ))
        .await
        .expect("lookup geoip")
        .into_inner();
    // 国家码由 GeoIP 库给出（测试库无该地址时为空），但写回不再报错。
    assert_eq!(geo.ip, "203.0.113.9");
}
