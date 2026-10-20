//! Collector 从端 Sync 循环集成测试：真实主端 + 真实从端 Runtime。
//!
//! 覆盖两条路径：mTLS 下首轮注册成功但同步流因缺客户端证书被拒（Go 重连
//! 语义：下一轮带证书即通），以及明文 + 注册令牌下首轮即完成注册与同步；
//! 并断言从端授权缓存 / 客户端 PKI 落盘与主端运行态上线。

mod common;

use std::time::Duration;

use common::*;
use rusqlite::OptionalExtension;
use santaizi_dashboard::collector::store;
use santaizi_dashboard::db;
use santaizi_dashboard::proto;
use tokio_util::sync::CancellationToken;

/// 轮询等待闭包产出 `Some`；DB 写在后台任务中，需给提交留时间。
async fn wait_until<T>(timeout: Duration, mut produce: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(value) = produce() {
            return Some(value);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn collector_cache_row(db: &db::Db) -> Option<(String, i64, String)> {
    db.read(|conn| {
        conn.query_row(
            "SELECT collector_uuid, config_version, kind FROM collector_authorization_caches \
             WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(anyhow::Error::from)
    })
    .ok()
    .flatten()
}

/// 主端运行态行：上线且记录过主端可见时间时返回状态。
fn primary_online(db: &db::Db) -> Option<String> {
    db.read(|conn| {
        conn.query_row(
            "SELECT status, last_primary_seen FROM collector_runtimes WHERE collector_uuid = ?",
            [COLLECTOR_UUID],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(anyhow::Error::from)
    })
    .ok()
    .flatten()
    .filter(|(_, seen)| *seen > 0)
    .map(|(status, _)| status)
}

/// 建从端库 + Runtime；同步测试再缩短运行态帧重发间隔。
async fn setup_collector(
    fixture: &PrimaryFixture,
    tag: &str,
    primary_tls: bool,
    insecure: bool,
) -> (
    std::sync::Arc<santaizi_dashboard::collector::runtime::Runtime>,
    db::Db,
    std::path::PathBuf,
) {
    setup_collector_runtime(
        fixture,
        tag,
        primary_tls,
        insecure,
        Some(Duration::from_millis(50)),
        None,
        "",
    )
    .await
}

#[tokio::test]
async fn collector_registers_and_syncs_over_mtls() {
    let fixture = setup_primary("collector-sync-mtls", true).await;
    let (runtime, db, dir) =
        setup_collector(&fixture, "collector-sync-mtls-edge", true, true).await;

    // 首轮：注册走令牌成功，但同步流连接未带客户端证书，被主端拒绝
    // （Go 同：重连后带证书才进入正常同步）。
    let first = tokio::time::timeout(Duration::from_secs(5), runtime.sync_once()).await;
    assert!(
        matches!(first, Ok(Err(_))),
        "expected the first mTLS round to be rejected, got {first:?}"
    );

    let worker = runtime.start();
    let pki_dir = dir.join("pki");

    // 从端授权缓存：注册与 Config 下发都会落库。
    let cached = wait_until(Duration::from_secs(5), || collector_cache_row(&db)).await;
    assert_eq!(
        cached,
        Some((COLLECTOR_UUID.to_string(), 1, "observer".to_string()))
    );
    // 客户端 PKI：证书包 + 主端下发的 Agent CA 落盘。
    for name in ["client.key", "client.crt", "ca.crt", "agent-ca.crt"] {
        assert!(pki_dir.join(name).exists(), "missing pki file {name}");
    }

    // 主端运行态：收到 runtime 帧后上线。
    let status = wait_until(Duration::from_secs(5), || primary_online(&fixture.db)).await;
    assert_eq!(status, Some("online".to_string()));

    runtime.cancel_token().cancel();
    assert!(worker.await.unwrap().is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn collector_registers_over_plaintext_with_token() {
    let fixture = setup_primary("collector-sync-plain", false).await;
    let (runtime, db, dir) =
        setup_collector(&fixture, "collector-sync-plain-edge", false, false).await;

    // 明文 + 注册令牌：首轮即完成注册并保持同步（超时未返回即为正常运行）。
    let first = tokio::time::timeout(Duration::from_secs(2), runtime.sync_once()).await;
    match first {
        Err(_) => {}
        Ok(Ok(())) => panic!("sync stream closed unexpectedly"),
        Ok(Err(err)) => panic!("first sync round failed: {err:#}"),
    }

    // 首轮数据已落库；再起循环验证取消路径。
    let cached = wait_until(Duration::from_secs(5), || collector_cache_row(&db)).await;
    assert_eq!(
        cached,
        Some((COLLECTOR_UUID.to_string(), 1, "observer".to_string()))
    );
    for name in ["client.key", "client.crt", "agent-ca.crt"] {
        assert!(
            dir.join("pki").join(name).exists(),
            "missing pki file {name}"
        );
    }
    let status = wait_until(Duration::from_secs(5), || primary_online(&fixture.db)).await;
    assert_eq!(status, Some("online".to_string()));

    let worker = runtime.start();
    runtime.cancel_token().cancel();
    assert!(worker.await.unwrap().is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}

/// 重启恢复：已有授权缓存时 `Runtime::new` 恢复 UUID 与 Agent CA。
#[tokio::test]
async fn new_restores_uuid_from_cache() {
    let dir = temp_dir("collector-restore");
    let db_path = dir.join("collector.db");
    let db = db::migrate::open_collector(db_path.to_str().expect("collector db path"))
        .expect("open collector db");
    db.write(|tx| {
        store::save_authorization(
            tx,
            COLLECTOR_UUID,
            &proto::CollectorAuthorizationConfig {
                config_version: 3,
                primary_public_key: vec![0xab; 32],
                agent_ca_certificate_pem: "agent-ca".to_string(),
                ..Default::default()
            },
            123,
        )
    })
    .unwrap();
    let config = santaizi_dashboard::config::CollectorModeConfig {
        primary_endpoint: "127.0.0.1:5555".to_string(),
        registration_token: "token".to_string(),
        ..Default::default()
    };
    let runtime = santaizi_dashboard::collector::runtime::Runtime::new(
        CancellationToken::new(),
        db,
        db_path.to_str().expect("collector db path"),
        config,
    )
    .await
    .unwrap();
    assert_eq!(runtime.collector_uuid(), COLLECTOR_UUID);
    let agent_ca = std::fs::read(dir.join("pki").join("agent-ca.crt")).unwrap();
    assert_eq!(agent_ca, b"agent-ca");
    let _ = std::fs::remove_dir_all(&dir);
}
