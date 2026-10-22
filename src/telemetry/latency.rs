//! 连接时延采样：对齐 Go `service/telemetry/latency.go` 的**记录侧**。
//!
//! 三类样本：collector 心跳 / collector 复制（`RecordCollectorLatency`）与
//! agent→sink 路径时延（`recordAgentSinkLatency`）。统一进分钟桶
//! （min/max/sum/count），游标表按 `last_sampled_at` 去重；读侧
//! （`ListConnectionLatency`）属 Phase 6 HTTP，此处不接。
//! 桶的保留期裁剪已在 retention drain（evidence 48h）内。

use std::collections::HashMap;
use std::result::Result as StdResult;

use rusqlite::{OptionalExtension as _, Transaction};

use crate::db::Db;
use crate::proto;
use crate::repo::bindings::PRIMARY_OBSERVER_ID;
use crate::util::time::now_nanos;

pub const LATENCY_KIND_COLLECTOR_HEARTBEAT: &str = "collector_heartbeat";
pub const LATENCY_KIND_COLLECTOR_REPLICATION: &str = "collector_replication";
pub const LATENCY_KIND_PATH: &str = "path";

const MINUTE_NANOS: i64 = 60_000_000_000;
/// `CollectorTimeout`：从端心跳超时，超过即 offline（`sinkHandshaked` 判定用）。
const COLLECTOR_TIMEOUT_NANOS: i64 = 90 * 1_000_000_000;

const ZERO_NODE_UUID: [u8; 16] = [0u8; 16];

/// `LatencySample`：一条时延样本。node_uuid 非探针场景传零键。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LatencySample {
    pub kind: String,
    pub collector_uuid: String,
    pub node_uuid: Vec<u8>,
    pub observer_id: String,
    pub rtt_ms: f64,
    pub sampled_at: i64,
}

/// `CollectorLatencyInput`：从端运行态里的两类 RTT 样本。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CollectorLatencyInput {
    pub collector_uuid: String,
    pub heartbeat_rtt_ms: f64,
    pub heartbeat_rtt_sampled_at: i64,
    pub replication_rtt_ms: f64,
    pub replication_rtt_sampled_at: i64,
}

/// `RecordCollectorLatency`：心跳与复制两条样本。
pub fn record_collector_latency(db: &Db, input: &CollectorLatencyInput) -> anyhow::Result<()> {
    record_latency_sample(
        db,
        &LatencySample {
            kind: LATENCY_KIND_COLLECTOR_HEARTBEAT.into(),
            collector_uuid: input.collector_uuid.clone(),
            rtt_ms: input.heartbeat_rtt_ms,
            sampled_at: input.heartbeat_rtt_sampled_at,
            ..Default::default()
        },
    )?;
    record_latency_sample(
        db,
        &LatencySample {
            kind: LATENCY_KIND_COLLECTOR_REPLICATION.into(),
            collector_uuid: input.collector_uuid.clone(),
            rtt_ms: input.replication_rtt_ms,
            sampled_at: input.replication_rtt_sampled_at,
            ..Default::default()
        },
    )
}

/// `RecordAgentSinkLatency`：agent 运行态里每个已握手 sink 的路径时延。
pub fn record_agent_sink_latency(
    db: &Db,
    node_uuid: &[u8],
    runtime: &proto::AgentRuntime,
) -> anyhow::Result<()> {
    let ids: Vec<String> = runtime
        .sinks
        .iter()
        .map(|sink| sink.endpoint_id.clone())
        .collect();
    let last_seen = collector_last_seen_by_id(db, &ids)?;
    let now = now_nanos();
    for sink in &runtime.sinks {
        if !sink_handshaked(&sink.endpoint_id, sink.connected, &last_seen, now) {
            continue;
        }
        record_latency_sample(
            db,
            &LatencySample {
                kind: LATENCY_KIND_PATH.into(),
                node_uuid: node_uuid.to_vec(),
                observer_id: sink.endpoint_id.clone(),
                rtt_ms: sink.last_rtt_ms,
                sampled_at: sink.rtt_sampled_at_unix_nano,
                ..Default::default()
            },
        )?;
    }
    Ok(())
}

/// `RecordLatencySample`：游标去重（`last_sampled_at >= sampled_at` 跳过）+
/// 分钟桶聚合 upsert。两写在同一个事务里，与 Go 一致。
///
/// 桶起点一律按**主端钟**取整：`sampled_at` 来自从端/探针自己的钟，既可漂移
/// 也可伪造。直接拿它算桶起点，等于让对端把数据写进任意一分钟——轻则曲线
/// 出现未来/远古点，重则写进已过保留期的桶里躲过清理。游标去重仍用对端
/// 时间戳：它在那里是重放判重的序号，不是事实时间。
pub fn record_latency_sample(db: &Db, sample: &LatencySample) -> anyhow::Result<()> {
    // NaN 也过不了 `< 0.0`（比较恒为假），而非有限值会让 sum_ms 变 NULL，
    // 一整分钟的聚合随之作废（SQLite 把 NaN 存成 NULL）。
    // NaN 也过不了 `< 0.0`（比较恒为假），而非有限值会让 sum_ms 变 NULL，
    // 一整分钟的聚合随之作废（SQLite 把 NaN 存成 NULL）。
    if sample.sampled_at <= 0
        || !sample.rtt_ms.is_finite()
        || sample.rtt_ms < 0.0
        || sample.kind.is_empty()
    {
        return Ok(());
    }
    if !valid_latency_kind(&sample.kind) {
        anyhow::bail!("unknown connection latency kind");
    }
    let node_uuid = latency_node_key(&sample.node_uuid);
    let bucket_start = now_nanos() / MINUTE_NANOS * MINUTE_NANOS;
    db.write(|tx| {
        let last: Option<i64> = tx
            .query_row(
                "SELECT last_sampled_at FROM connection_latency_cursors \
                 WHERE kind = ? AND collector_uuid = ? AND node_uuid = ? AND observer_id = ?",
                rusqlite::params![
                    sample.kind,
                    sample.collector_uuid,
                    node_uuid,
                    sample.observer_id
                ],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(last) = last
            && last >= sample.sampled_at
        {
            return Ok(());
        }
        upsert_bucket(tx, sample, &node_uuid, bucket_start)?;
        upsert_cursor(tx, sample, &node_uuid)?;
        Ok(())
    })
}

fn upsert_bucket(
    tx: &Transaction<'_>,
    sample: &LatencySample,
    node_uuid: &[u8],
    bucket_start: i64,
) -> anyhow::Result<()> {
    tx.execute(
        "INSERT INTO connection_latency_buckets \
         (kind, collector_uuid, node_uuid, observer_id, bucket_start, min_ms, max_ms, sum_ms, count, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6, ?6, 1, CURRENT_TIMESTAMP) \
         ON CONFLICT (kind, collector_uuid, node_uuid, observer_id, bucket_start) DO UPDATE SET \
         min_ms = MIN(min_ms, excluded.min_ms), \
         max_ms = MAX(max_ms, excluded.max_ms), \
         sum_ms = sum_ms + excluded.sum_ms, \
         count = count + excluded.count, \
         updated_at = CURRENT_TIMESTAMP",
        rusqlite::params![
            sample.kind,
            sample.collector_uuid,
            node_uuid,
            sample.observer_id,
            bucket_start,
            sample.rtt_ms,
        ],
    )?;
    Ok(())
}

fn upsert_cursor(
    tx: &Transaction<'_>,
    sample: &LatencySample,
    node_uuid: &[u8],
) -> anyhow::Result<()> {
    tx.execute(
        "INSERT INTO connection_latency_cursors \
         (kind, collector_uuid, node_uuid, observer_id, last_sampled_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, CURRENT_TIMESTAMP) \
         ON CONFLICT (kind, collector_uuid, node_uuid, observer_id) DO UPDATE SET \
         last_sampled_at = excluded.last_sampled_at, updated_at = CURRENT_TIMESTAMP",
        rusqlite::params![
            sample.kind,
            sample.collector_uuid,
            node_uuid,
            sample.observer_id,
            sample.sampled_at,
        ],
    )?;
    Ok(())
}

/// `latencyNodeKey`：非 16 字节一律归零键（Go 同）。
fn latency_node_key(node_uuid: &[u8]) -> Vec<u8> {
    if node_uuid.len() == 16 {
        node_uuid.to_vec()
    } else {
        ZERO_NODE_UUID.to_vec()
    }
}

/// `collectorLastSeenByID`：从端运行态最近心跳时间；空 id 与 primary 不过滤器。
fn collector_last_seen_by_id(db: &Db, ids: &[String]) -> anyhow::Result<HashMap<String, i64>> {
    let mut out = HashMap::new();
    let filtered: Vec<&String> = ids
        .iter()
        .filter(|id| !id.is_empty() && id.as_str() != PRIMARY_OBSERVER_ID)
        .collect();
    if filtered.is_empty() {
        return Ok(out);
    }
    let placeholders = filtered.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let query = format!(
        "SELECT collector_uuid, last_seen FROM collector_runtimes WHERE collector_uuid IN ({placeholders})"
    );
    let rows = db.read(|conn| {
        let mut stmt = conn.prepare(&query)?;
        let params = rusqlite::params_from_iter(filtered.iter().copied());
        let rows = stmt
            .query_map(params, |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<StdResult<Vec<_>, _>>()?;
        Ok(rows)
    })?;
    for (uuid, last_seen) in rows {
        out.insert(uuid, last_seen);
    }
    Ok(out)
}

/// `CollectorStatus`：从端在线判定（`last_seen` 90s 内为 online）。
pub fn collector_status(last_seen: i64, now_nanos: i64) -> &'static str {
    if last_seen <= 0 {
        return "unknown";
    }
    if now_nanos - last_seen <= COLLECTOR_TIMEOUT_NANOS {
        "online"
    } else {
        "offline"
    }
}

/// `sinkHandshaked`：connected 且（primary 自身 / 观测从端在线）才算握手，
/// 否则其时延样本不可信。
fn sink_handshaked(
    observer_id: &str,
    connected: bool,
    last_seen: &HashMap<String, i64>,
    now_nanos: i64,
) -> bool {
    if !connected {
        return false;
    }
    if observer_id == PRIMARY_OBSERVER_ID {
        return true;
    }
    collector_status(last_seen.get(observer_id).copied().unwrap_or(0), now_nanos) == "online"
}

fn valid_latency_kind(kind: &str) -> bool {
    matches!(
        kind,
        LATENCY_KIND_COLLECTOR_HEARTBEAT | LATENCY_KIND_COLLECTOR_REPLICATION | LATENCY_KIND_PATH
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;

    const NODE: [u8; 16] = [11u8; 16];

    fn db(name: &str) -> (Db, String) {
        let dir =
            std::env::temp_dir().join(format!("santaizi-latency-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("panel.db").to_string_lossy().into_owned();
        (migrate::open_primary(&path).unwrap(), path)
    }

    fn bucket(db: &Db, kind: &str, observer_id: &str) -> Option<(f64, f64, f64, i64)> {
        db.read(|conn| {
            Ok(conn
                .query_row(
                    "SELECT min_ms, max_ms, sum_ms, count FROM connection_latency_buckets \
                     WHERE kind = ? AND node_uuid = ? AND observer_id = ?",
                    rusqlite::params![kind, NODE.to_vec(), observer_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?)
        })
        .unwrap()
    }

    fn sample(kind: &str, rtt_ms: f64, sampled_at: i64) -> LatencySample {
        LatencySample {
            kind: kind.into(),
            node_uuid: NODE.to_vec(),
            observer_id: "primary".into(),
            rtt_ms,
            sampled_at,
            ..Default::default()
        }
    }

    #[test]
    fn record_aggregates_into_minute_bucket() {
        let (db, _path) = db("bucket");
        let base = 1_792_100_000_000_000_000i64; // 整分对齐
        record_latency_sample(&db, &sample(LATENCY_KIND_PATH, 10.0, base)).unwrap();
        record_latency_sample(&db, &sample(LATENCY_KIND_PATH, 30.0, base + 1)).unwrap();
        record_latency_sample(&db, &sample(LATENCY_KIND_PATH, 20.0, base + 2)).unwrap();
        let (min, max, sum, count) = bucket(&db, LATENCY_KIND_PATH, "primary").unwrap();
        assert_eq!(min, 10.0);
        assert_eq!(max, 30.0);
        assert_eq!(sum, 60.0);
        assert_eq!(count, 3);
    }

    #[test]
    fn record_skips_stale_samples_via_cursor() {
        let (db, _path) = db("cursor");
        let base = 1_792_100_000_000_000_000i64;
        record_latency_sample(&db, &sample(LATENCY_KIND_PATH, 10.0, base + 5)).unwrap();
        // 同一分区旧样本直接跳过。
        record_latency_sample(&db, &sample(LATENCY_KIND_PATH, 99.0, base)).unwrap();
        let (min, max, sum, count) = bucket(&db, LATENCY_KIND_PATH, "primary").unwrap();
        assert_eq!((min, max, sum, count), (10.0, 10.0, 10.0, 1));
        // 新样本照常聚合。
        record_latency_sample(&db, &sample(LATENCY_KIND_PATH, 4.0, base + 9)).unwrap();
        let (min, max, sum, count) = bucket(&db, LATENCY_KIND_PATH, "primary").unwrap();
        assert_eq!((min, max, sum, count), (4.0, 10.0, 14.0, 2));
    }

    #[test]
    fn record_rejects_invalid_samples() {
        let (db, _path) = db("invalid");
        // 非法 kind 报错。
        let err = record_latency_sample(
            &db,
            &LatencySample {
                kind: "bogus".into(),
                ..sample(LATENCY_KIND_PATH, 1.0, 1_792_100_000_000_000_000)
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown connection latency kind"));
        // 非正时间 / 负 RTT / 空 kind 静默跳过。
        record_latency_sample(&db, &sample(LATENCY_KIND_PATH, 1.0, 0)).unwrap();
        record_latency_sample(
            &db,
            &sample(LATENCY_KIND_PATH, -1.0, 1_792_100_000_000_000_000),
        )
        .unwrap();
        let empty = LatencySample {
            kind: String::new(),
            ..Default::default()
        };
        record_latency_sample(&db, &empty).unwrap();
        assert!(bucket(&db, LATENCY_KIND_PATH, "primary").is_none());
    }

    /// 核心回归：桶起点必须按主端钟取整，不能跟着对端时间戳走。
    ///
    /// 对端时间戳可漂移也可伪造：拿它算桶起点，等于允许把数据写进任意一分钟
    /// ——未来的点会让曲线翘到右边，远古的点能塞进已过保留期的桶里躲过清理。
    #[test]
    fn bucket_start_follows_the_primary_clock_not_the_peer_timestamp() {
        let (primary_db, _path) = db("primary-clock");
        // 一个伪造的「1970 年」时间戳：按对端钟会落到 1970-01-01 那一分钟。
        let forged = 1_000_000_000i64;
        record_latency_sample(&primary_db, &sample(LATENCY_KIND_PATH, 7.0, forged)).unwrap();
        let (_, _, sum, count) = bucket(&primary_db, LATENCY_KIND_PATH, "primary").unwrap();
        assert_eq!((sum, count), (7.0, 1), "样本本身要落库");

        let bucket_start: i64 = primary_db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT bucket_start FROM connection_latency_buckets \
                     WHERE kind = ? AND observer_id = 'primary'",
                    [LATENCY_KIND_PATH],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        let expected = now_nanos() / 60_000_000_000 * 60_000_000_000;
        assert_eq!(bucket_start, expected, "桶起点必须是主端当前分钟");
        assert!(bucket_start > forged, "不得按对端时间戳落到 1970 年");

        // 伪造的「遥远的未来」同样被按到当前分钟。
        let (future_db, _path2) = db("primary-clock-future");
        record_latency_sample(&future_db, &sample(LATENCY_KIND_PATH, 3.0, i64::MAX / 2)).unwrap();
        let future: i64 = future_db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT bucket_start FROM connection_latency_buckets \
                     WHERE kind = ? AND observer_id = 'primary'",
                    [LATENCY_KIND_PATH],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(future, expected, "未来时间戳也不得单独成桶");
    }

    /// NaN 时延会让 sum_ms 变 NULL（SQLite 把 NaN 存成 NULL），
    /// 一整分钟的聚合随之作废——必须静默跳过。
    #[test]
    fn nan_rtt_is_skipped() {
        let (db, _path) = db("nan");
        let base = 1_792_100_000_000_000_000i64;
        record_latency_sample(&db, &sample(LATENCY_KIND_PATH, 5.0, base)).unwrap();
        record_latency_sample(&db, &sample(LATENCY_KIND_PATH, f64::NAN, base + 1)).unwrap();
        record_latency_sample(&db, &sample(LATENCY_KIND_PATH, f64::INFINITY, base + 2)).unwrap();
        let (min, max, sum, count) = bucket(&db, LATENCY_KIND_PATH, "primary").unwrap();
        assert_eq!((min, max, sum, count), (5.0, 5.0, 5.0, 1));
    }

    #[test]
    fn record_collector_latency_writes_both_kinds() {
        let (db, _path) = db("collector");
        let base = 1_792_100_000_000_000_000i64;
        record_collector_latency(
            &db,
            &CollectorLatencyInput {
                collector_uuid: "c1".into(),
                heartbeat_rtt_ms: 12.5,
                heartbeat_rtt_sampled_at: base,
                replication_rtt_ms: 40.0,
                replication_rtt_sampled_at: base + 10,
            },
        )
        .unwrap();
        let count: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM connection_latency_buckets WHERE collector_uuid = 'c1'",
                    [],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(count, 2);
        // 心跳 RTT 为 0 / 时间为 0 时不落桶。
        record_collector_latency(
            &db,
            &CollectorLatencyInput {
                collector_uuid: "c2".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let count: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM connection_latency_buckets WHERE collector_uuid = 'c2'",
                    [],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn record_agent_sink_latency_filters_by_handshake() {
        let (db, _path) = db("sink");
        let now = crate::util::time::now_nanos();
        // 从端在线；primary 无需查表。
        db.write(|tx| {
            tx.execute(
                "INSERT INTO collector_runtimes (collector_uuid, status, last_seen) VALUES ('obs-a', 'online', ?)",
                rusqlite::params![now],
            )?;
            Ok(())
        })
        .unwrap();
        let runtime = proto::AgentRuntime {
            sinks: vec![
                proto::SinkRuntime {
                    endpoint_id: "primary".into(),
                    connected: true,
                    last_rtt_ms: 5.0,
                    rtt_sampled_at_unix_nano: now,
                    ..Default::default()
                },
                proto::SinkRuntime {
                    endpoint_id: "obs-a".into(),
                    connected: true,
                    last_rtt_ms: 7.0,
                    rtt_sampled_at_unix_nano: now,
                    ..Default::default()
                },
                proto::SinkRuntime {
                    endpoint_id: "obs-b".into(),
                    connected: true,
                    last_rtt_ms: 9.0,
                    rtt_sampled_at_unix_nano: now,
                    ..Default::default()
                },
                proto::SinkRuntime {
                    endpoint_id: "obs-c".into(),
                    connected: false,
                    last_rtt_ms: 3.0,
                    rtt_sampled_at_unix_nano: now,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        record_agent_sink_latency(&db, &NODE, &runtime).unwrap();
        assert!(bucket(&db, LATENCY_KIND_PATH, "primary").is_some());
        assert!(bucket(&db, LATENCY_KIND_PATH, "obs-a").is_some());
        // obs-b 无从端运行态（unknown）→ 不握手，不采样。
        assert!(bucket(&db, LATENCY_KIND_PATH, "obs-b").is_none());
        // obs-c 未连接 → 不采样。
        assert!(bucket(&db, LATENCY_KIND_PATH, "obs-c").is_none());
    }

    #[test]
    fn latency_node_key_normalizes() {
        assert_eq!(latency_node_key(&NODE), NODE.to_vec());
        assert_eq!(latency_node_key(&[1, 2, 3]), ZERO_NODE_UUID.to_vec());
        assert_eq!(latency_node_key(&[]), ZERO_NODE_UUID.to_vec());
    }

    #[test]
    fn collector_status_thresholds() {
        let now = 1_792_100_000_000_000_000i64;
        assert_eq!(collector_status(0, now), "unknown");
        assert_eq!(collector_status(-1, now), "unknown");
        assert_eq!(collector_status(now - 89 * 1_000_000_000, now), "online");
        assert_eq!(collector_status(now - 91 * 1_000_000_000, now), "offline");
    }
}
