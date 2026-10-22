//! Collector 从端 Replication 循环集成测试：真实主端 + 真实从端 Runtime。
//!
//! 覆盖两条路径：mTLS 下带客户端证书复制（证书身份），以及明文 + 注册令牌
//! 复制（`collector_token` metadata 身份）。断言出库记录被主端落库、ack 后
//! 从端游标推进、出库清空，取消后循环干净退出。

mod common;

use std::time::Duration;

use common::*;
use prost::Message;
use rusqlite::OptionalExtension;
use santaizi_dashboard::collector::store;
use santaizi_dashboard::db;
use santaizi_dashboard::proto;
use santaizi_dashboard::telemetry::store::Store;
use santaizi_dashboard::util::time as util_time;
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

fn replication_session() -> Vec<u8> {
    vec![0x23; 16]
}

/// 未指派给夹具从端的节点：归属校验必须把它挡在门外。
fn foreign_node() -> Vec<u8> {
    vec![0x44; 16]
}

/// 出库里的合法 HOST 事件（event_id 由 node/session/sequence 派生）。
fn outbox_event(sequence: u64) -> proto::TelemetryEvent {
    let event_id = Store::event_id(&node(), &replication_session(), sequence).expect("event id");
    proto::TelemetryEvent {
        event_id: event_id.to_vec(),
        node_uuid: node(),
        session_id: replication_session(),
        sequence,
        event_type: proto::TelemetryEventType::Host as i32,
        priority: proto::TelemetryPriority::P1Important as i32,
        collected_at_unix_nano: util_time::now_nanos(),
        protocol_version: 2,
        source_protocol: proto::SourceProtocol::SantaiziV2 as i32,
        reliability: proto::Reliability::ReliableReplay as i32,
        payload: Some(proto::telemetry_event::Payload::Host(proto::Host {
            ip: "10.9.9.9".to_string(),
            version: "0.1.0".to_string(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

/// 预置出库：一条事件 + 引用它的观测 + 一条健康样本（spool_id 1..=3）。
fn seed_outbox(db: &db::Db) {
    let event = outbox_event(1);
    let now = util_time::now_nanos();
    let observation = proto::TelemetryObservation {
        event_id: event.event_id.clone(),
        observer_id: COLLECTOR_UUID.to_string(),
        received_at_unix_nano: now,
        ..Default::default()
    };
    let health = proto::ObserverHealthSample {
        observer_id: COLLECTOR_UUID.to_string(),
        sampled_at_unix_nano: now,
        healthy: true,
        process_session: "repl-session".to_string(),
    };
    db.write(|tx| {
        tx.execute(
            "INSERT INTO collector_outboxes (record_type, payload, created_at) \
             VALUES ('event', ?1, CURRENT_TIMESTAMP)",
            [event.encode_to_vec()],
        )?;
        tx.execute(
            "INSERT INTO collector_outboxes (record_type, payload, created_at) \
             VALUES ('observation', ?1, CURRENT_TIMESTAMP)",
            [observation.encode_to_vec()],
        )?;
        tx.execute(
            "INSERT INTO collector_outboxes (record_type, payload, created_at) \
             VALUES ('health', ?1, CURRENT_TIMESTAMP)",
            [health.encode_to_vec()],
        )?;
        Ok(())
    })
    .unwrap();
}

fn outbox_pending(db: &db::Db) -> i64 {
    db.read(|conn| Ok(conn.query_row("SELECT COUNT(*) FROM collector_outboxes", [], |r| r.get(0))?))
        .unwrap()
}

fn replication_cursor(db: &db::Db) -> Option<i64> {
    db.read(|conn| {
        conn.query_row(
            "SELECT committed_spool_through_id FROM collector_replication_cursors WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .optional()
        .map_err(anyhow::Error::from)
    })
    .ok()
    .flatten()
}

fn primary_event_count(db: &db::Db, event_id: &[u8]) -> i64 {
    db.read(|conn| {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM telemetry_events WHERE event_id = ?1",
            [event_id],
            |r| r.get(0),
        )?)
    })
    .unwrap()
}

fn primary_observation_count(db: &db::Db, event_id: &[u8]) -> i64 {
    db.read(|conn| {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM telemetry_observations WHERE event_id = ?1",
            [event_id],
            |r| r.get(0),
        )?)
    })
    .unwrap()
}

/// 直接把节点绑到夹具从端（明文 fixture 不过 enroll：enroll 要求 TLS）。
/// 同时补在效指派：主端复制路径按 `observer_assignments` 校验节点归属。
fn bind_node(fixture: &PrimaryFixture) {
    common::bind_node_with_assignment(fixture, &node(), COLLECTOR_UUID);
}

/// 复制批次上限：对端可以决定批次多大，主端必须能拦住。
///
/// 从端 `REPLICATION_BATCH_SIZE` = 512，与主端默认上限一致；这里把主端上限
/// 压到 1，于是一个合法小批次也会被拒——用来验证上限真的生效，而不是装饰。
#[tokio::test]
async fn replication_batch_over_the_limit_is_rejected() {
    let fixture = setup_primary_with("collector-repl-limit", false, |conf| {
        conf.telemetry.replication_batch_size = 1;
    })
    .await;
    bind_node(&fixture);
    let (runtime, db, dir) = setup_collector_runtime(
        &fixture,
        "collector-repl-limit-edge",
        false,
        false,
        None,
        Some(observer_authorization(&fixture, &node())),
        "",
    )
    .await;
    let worker = runtime.start();
    seed_outbox(&db);
    // 再补一条：单条事件的批次刚好等于上限 1，两条才越界。
    db.write(|tx| {
        tx.execute(
            "INSERT INTO collector_outboxes (record_type, payload, created_at) \
             VALUES ('event', ?1, CURRENT_TIMESTAMP)",
            [outbox_event(2).encode_to_vec()],
        )?;
        Ok(())
    })
    .unwrap();

    let event_id = Store::event_id(&node(), &replication_session(), 1).expect("event id");
    let outcome = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match runtime.replication_once().await {
                Err(err) if err.to_string().contains("exceeds limit") => return true,
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                Ok(()) => return false,
            }
        }
    })
    .await;
    assert_eq!(outcome, Ok(true), "超限批次必须被主端拒绝");
    assert_eq!(
        primary_event_count(&fixture.db, &event_id),
        0,
        "被拒的批次不得落库"
    );
    assert!(
        outbox_pending(&db) > 0,
        "被拒的批次不得确认，出库记录必须留着"
    );
    assert_eq!(replication_cursor(&db), Some(0), "被拒的批次不得推进游标");

    runtime.cancel_token().cancel();
    let _ = worker.await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// mTLS：带客户端证书的复制流；出库记录落主端，ack 后从端游标推进、出库清空。
#[tokio::test]
async fn replication_flushes_outbox_over_mtls() {
    let fixture = setup_primary("collector-repl-mtls", true).await;
    // enroll 换绑生成节点绑定与指派：复制事件要能刷运行态。
    enroll(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    let (runtime, db, dir) = setup_collector_runtime(
        &fixture,
        "collector-repl-mtls-edge",
        true,
        true,
        None,
        None,
        "",
    )
    .await;
    seed_outbox(&db);
    assert_eq!(outbox_pending(&db), 3);

    // 循环启动后由 sync 侧完成注册，replication 侧退避重试至 UUID 就绪。
    let worker = runtime.start();

    let event_id = Store::event_id(&node(), &replication_session(), 1).expect("event id");
    let count = wait_until(Duration::from_secs(10), || {
        let count = primary_event_count(&fixture.db, &event_id);
        (count > 0).then_some(count)
    })
    .await;
    assert_eq!(count, Some(1), "replicated event missing on primary");
    assert_eq!(primary_observation_count(&fixture.db, &event_id), 1);

    // 游标行在建库时即为 0，须过滤到目标值再断言（否则先轮询到 0 即返回）。
    let cursor = wait_until(Duration::from_secs(10), || {
        replication_cursor(&db).filter(|through| *through >= 3)
    })
    .await;
    assert_eq!(
        cursor,
        Some(3),
        "replication cursor should reach spool through"
    );
    assert_eq!(
        outbox_pending(&db),
        0,
        "acknowledged outbox rows must be cleared"
    );

    runtime.cancel_token().cancel();
    assert!(worker.await.unwrap().is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}

/// 拿不出证书身份时复制流带 `collector_token`，主端按令牌识别。
///
/// 传输是明文：复制流本身不要求 TLS（要求的是 `Register`/`Enroll` 这类
/// 交换长期秘密的入口）。授权缓存预置，因此这一轮不触发注册——被测的
/// 正是「无证书 ⇒ 令牌认证」这条分支。
#[tokio::test]
async fn replication_uses_token_over_plaintext() {
    let fixture = setup_primary("collector-repl-plain", false).await;
    bind_node(&fixture);
    let (runtime, db, dir) = setup_collector_runtime(
        &fixture,
        "collector-repl-plain-edge",
        false,
        false,
        None,
        Some(observer_authorization(&fixture, &node())),
        "",
    )
    .await;
    seed_outbox(&db);

    let worker = runtime.start();

    let event_id = Store::event_id(&node(), &replication_session(), 1).expect("event id");
    let count = wait_until(Duration::from_secs(10), || {
        let count = primary_event_count(&fixture.db, &event_id);
        (count > 0).then_some(count)
    })
    .await;
    assert_eq!(count, Some(1), "token-authenticated replication failed");
    let cursor = wait_until(Duration::from_secs(10), || {
        replication_cursor(&db).filter(|through| *through >= 3)
    })
    .await;
    assert_eq!(cursor, Some(3));
    assert_eq!(outbox_pending(&db), 0);

    runtime.cancel_token().cancel();
    assert!(worker.await.unwrap().is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}

/// 未注册时 replication 循环报错但不 panic，注册成功后自行恢复。
#[tokio::test]
async fn replication_requires_registration_then_recovers() {
    let fixture = setup_primary("collector-repl-late", true).await;
    bind_node(&fixture);
    let (runtime, db, dir) = setup_collector_runtime(
        &fixture,
        "collector-repl-late-edge",
        true,
        true,
        None,
        None,
        "",
    )
    .await;
    seed_outbox(&db);

    // 先只起复制循环：未注册，直接一次调用应失败而不是panic。
    let first = tokio::time::timeout(Duration::from_secs(5), runtime.replication_once()).await;
    assert!(
        matches!(first, Ok(Err(_))),
        "expected unregistered replication to fail, got {first:?}"
    );
    assert_eq!(outbox_pending(&db), 3, "failed round must not drop records");

    // 注册后重试即通。
    let worker = runtime.start();
    let event_id = Store::event_id(&node(), &replication_session(), 1).expect("event id");
    let count = wait_until(Duration::from_secs(10), || {
        let count = primary_event_count(&fixture.db, &event_id);
        (count > 0).then_some(count)
    })
    .await;
    assert_eq!(count, Some(1));

    runtime.cancel_token().cancel();
    assert!(worker.await.unwrap().is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}

/// 摄入与复用的组合：经 `store::ingest` 真实入队（事件 + 缺口 + 自动观测），
/// 由 replication 循环复制到主端；ack 后从端出库与本地事实表一并清空。
#[tokio::test]
async fn replication_flushes_ingested_batch() {
    let fixture = setup_primary("collector-repl-ingest", true).await;
    enroll(&fixture, &node(), Some(CLIENT_SECRET))
        .await
        .unwrap();
    let (runtime, db, dir) = setup_collector_runtime(
        &fixture,
        "collector-repl-ingest-edge",
        true,
        true,
        None,
        None,
        "",
    )
    .await;

    let batch = proto::TelemetryBatch {
        records: vec![
            proto::TelemetryRecord {
                record: Some(proto::telemetry_record::Record::Event(outbox_event(1))),
            },
            proto::TelemetryRecord {
                record: Some(proto::telemetry_record::Record::Event(outbox_event(3))),
            },
            proto::TelemetryRecord {
                record: Some(proto::telemetry_record::Record::Gap(proto::SequenceGap {
                    gap_id: vec![0x33; 16],
                    node_uuid: node(),
                    session_id: replication_session(),
                    start_sequence: 2,
                    end_sequence: 2,
                    reason: proto::GapReason::Compacted as i32,
                    created_at_unix_nano: util_time::now_nanos(),
                    ..Default::default()
                })),
            },
        ],
    };
    let ingested = db
        .write(|tx| store::ingest(tx, &batch, COLLECTOR_UUID, util_time::now_nanos()))
        .unwrap();
    assert_eq!(ingested.enqueued, 5, "两事件各带观测 + 一条缺口");
    assert_eq!(ingested.acks[0].ack_through, 3);

    let worker = runtime.start();

    let first = Store::event_id(&node(), &replication_session(), 1).expect("event id");
    let third = Store::event_id(&node(), &replication_session(), 3).expect("event id");
    let count = wait_until(Duration::from_secs(10), || {
        let count =
            primary_event_count(&fixture.db, &first) + primary_event_count(&fixture.db, &third);
        (count >= 2).then_some(count)
    })
    .await;
    assert_eq!(count, Some(2), "ingested events missing on primary");
    let gaps: i64 = fixture
        .db
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM telemetry_gaps WHERE gap_id = ?1",
                [[0x33u8; 16].as_slice()],
                |r| r.get(0),
            )?)
        })
        .unwrap();
    assert_eq!(gaps, 1, "缺口记录应复制到主端");

    let cursor = wait_until(Duration::from_secs(10), || {
        replication_cursor(&db).filter(|through| *through >= 5)
    })
    .await;
    assert_eq!(cursor, Some(5), "5 条出库记录的 through");
    assert_eq!(outbox_pending(&db), 0);
    let facts: (i64, i64, i64) = db
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT (SELECT COUNT(*) FROM collector_stored_events), \
                 (SELECT COUNT(*) FROM collector_stored_observations), \
                 (SELECT COUNT(*) FROM collector_stored_gaps)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?)
        })
        .unwrap();
    assert_eq!(facts, (0, 0, 0), "ack 后本地事实表应被清理");

    runtime.cancel_token().cancel();
    assert!(worker.await.unwrap().is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}

/// probe 从端不复制：出库留存，循环空转取消即退。
#[tokio::test]
async fn probe_collector_keeps_outbox() {
    let fixture = setup_primary("collector-repl-probe", false).await;
    let (runtime, db, dir) = setup_collector_runtime(
        &fixture,
        "collector-repl-probe-edge",
        false,
        false,
        None,
        None,
        "",
    )
    .await;
    db.write(|tx| {
        store::save_authorization(
            tx,
            COLLECTOR_UUID,
            &proto::CollectorAuthorizationConfig {
                config_version: 1,
                primary_public_key: vec![0xab; 32],
                kind: proto::CollectorKind::Probe as i32,
                ..Default::default()
            },
            1,
        )
    })
    .unwrap();
    seed_outbox(&db);

    let worker = runtime.start();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(outbox_pending(&db), 3, "probe 从端不得复制出库记录");

    runtime.cancel_token().cancel();
    assert!(worker.await.unwrap().is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}

/// B8：批次头绑定身份只证明「这条流是谁开的」，事件里的 `node_uuid` 仍可任填。
/// 主端必须按在效指派逐节点校验，否则一条 collector 凭据就能伪报任意节点。
#[tokio::test]
async fn replication_rejects_events_for_unassigned_nodes() {
    let fixture = setup_primary("collector-repl-own", false).await;
    // 只给 `node()` 建指派；`foreign_node()` 不在该从端名下。
    bind_node(&fixture);
    let (runtime, db, dir) = setup_collector_runtime(
        &fixture,
        "collector-repl-own-edge",
        false,
        false,
        None,
        Some(observer_authorization(&fixture, &node())),
        "",
    )
    .await;
    let worker = runtime.start();

    // 出库换成「foreign 节点」的事件（event_id 仍按 node 派生，只改 node_uuid）。
    db.write(|tx| {
        tx.execute("DELETE FROM collector_outboxes", [])?;
        let mut event = outbox_event(1);
        event.node_uuid = foreign_node();
        tx.execute(
            "INSERT INTO collector_outboxes (record_type, payload, created_at) \
             VALUES ('event', ?1, CURRENT_TIMESTAMP)",
            [event.encode_to_vec()],
        )?;
        Ok(())
    })
    .unwrap();

    // 直接驱动一次复制：注册后的首次调用应因归属不符而失败，而不是静默入库。
    let foreign_event_id =
        Store::event_id(&foreign_node(), &replication_session(), 1).expect("event id");
    let outcome = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match runtime.replication_once().await {
                Err(err) if err.to_string().contains("not assigned") => return true,
                // 注册尚未完成时是另一类错误，重试到超时。
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                Ok(()) => return false,
            }
        }
    })
    .await;
    assert_eq!(outcome, Ok(true), "未指派给该从端的节点事件必须被主端拒绝");
    assert_eq!(
        primary_event_count(&fixture.db, &foreign_event_id),
        0,
        "被拒的事件不得落库"
    );
    assert_eq!(
        outbox_pending(&db),
        1,
        "被拒的批次不得确认，出库记录必须留着"
    );

    runtime.cancel_token().cancel();
    let _ = worker.await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// 反向对照：指派名下的节点正常落地（证明上面拒的是归属，不是整条流）。
#[tokio::test]
async fn replication_accepts_events_for_assigned_nodes() {
    let fixture = setup_primary("collector-repl-own-ok", false).await;
    bind_node(&fixture);
    let (runtime, db, dir) = setup_collector_runtime(
        &fixture,
        "collector-repl-own-ok-edge",
        false,
        false,
        None,
        Some(observer_authorization(&fixture, &node())),
        "",
    )
    .await;
    seed_outbox(&db);
    let worker = runtime.start();

    let event_id = Store::event_id(&node(), &replication_session(), 1).expect("event id");
    let count = wait_until(Duration::from_secs(10), || {
        let count = primary_event_count(&fixture.db, &event_id);
        (count > 0).then_some(count)
    })
    .await;
    assert_eq!(count, Some(1), "指派名下的节点事件应正常复制");

    runtime.cancel_token().cancel();
    let _ = worker.await;
    let _ = std::fs::remove_dir_all(&dir);
}
