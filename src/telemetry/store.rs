//! 主端遥测摄入存储：对齐 Go `service/telemetry/store.go`。
//!
//! 每个批次在**一个 SQLite 事务**内完成：事件落库（DO NOTHING 幂等）、
//! 观测证据、缺口、STATE_ROLLUP 与摄入游标推进。HEARTBEAT / STATE /
//! STATE_ROLLUP 不落证据表；STATE 进内存缓冲供每分钟 rollup。
//! 游标推进带空洞宽限（默认 300s）：空洞超窗才推断 COMPACTED 缺口并
//! 落 `telemetry_data_losses`，避免探针重传竞态把正常事件判成丢失。

use std::collections::{HashMap, HashSet};
use std::result::Result as StdResult;
use std::sync::Arc;

use rusqlite::{OptionalExtension, Transaction};
use sha2::{Digest, Sha256};

use crate::proto;
use crate::repo::collectors;
use crate::telemetry::rollup;
use crate::telemetry::state_buffer::StateSampleBuffer;

/// 时钟可信窗：探针采集钟与主端接收钟差超过 5 分钟的事件只留证据，不改运行态。
pub const CLOCK_SKEW_LIMIT_NANOS: i64 = 5 * 60 * 1_000_000_000;
/// 空洞宽限默认值（秒），对齐 Go `sequenceHoleGrace`。
pub const DEFAULT_SEQUENCE_HOLE_GRACE_SECONDS: i64 = 300;

#[derive(Debug)]
pub struct IngestResult {
    pub acks: Vec<proto::SessionAck>,
    /// 时钟可信的新事件，供调用方应用到运行态。
    pub fresh_events: Vec<proto::TelemetryEvent>,
}

pub struct Store {
    bucket_nanos: i64,
    states: Arc<StateSampleBuffer>,
}

impl Store {
    pub fn new(bucket_seconds: u64) -> Self {
        let bucket = if bucket_seconds == 0 {
            30
        } else {
            bucket_seconds as i64
        };
        Self {
            bucket_nanos: bucket * 1_000_000_000,
            states: Arc::new(StateSampleBuffer::new()),
        }
    }

    pub fn states(&self) -> &StateSampleBuffer {
        &self.states
    }

    /// 与 rollup worker 共享同一缓冲（Go `liveStateBuffer` 进程级单例）。
    pub fn shared_states(&self) -> Arc<StateSampleBuffer> {
        Arc::clone(&self.states)
    }

    /// `EventID`：SHA-256(node‖session‖sequence_be)[:16]。
    pub fn event_id(
        node_uuid: &[u8],
        session_id: &[u8],
        sequence: u64,
    ) -> anyhow::Result<[u8; 16]> {
        if node_uuid.len() != 16 || session_id.len() != 16 {
            anyhow::bail!("node UUID and session ID must be 16 bytes");
        }
        let mut input = [0u8; 40];
        input[..16].copy_from_slice(node_uuid);
        input[16..32].copy_from_slice(session_id);
        input[32..].copy_from_slice(&sequence.to_be_bytes());
        let digest = Sha256::digest(input);
        let mut out = [0u8; 16];
        out.copy_from_slice(&digest[..16]);
        Ok(out)
    }
}

/// `persistTelemetryEvent`：HEARTBEAT / STATE / STATE_ROLLUP 不落证据表。
fn persist_event_type(event_type: i32) -> bool {
    !matches!(
        event_type,
        x if x == proto::TelemetryEventType::Heartbeat as i32
            || x == proto::TelemetryEventType::State as i32
            || x == proto::TelemetryEventType::StateRollup as i32
    )
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("invalid telemetry event identity")]
    InvalidEvent,
    #[error("telemetry event ID does not match identity tuple")]
    EventIdMismatch,
    #[error("telemetry event type, priority and payload are required")]
    IncompleteEvent,
    #[error("invalid telemetry sequence gap")]
    InvalidGap,
    #[error("empty telemetry record")]
    EmptyRecord,
}

pub fn validate_event(event: &proto::TelemetryEvent) -> StdResult<(), IngestError> {
    if event.event_id.len() != 16
        || event.node_uuid.len() != 16
        || event.session_id.len() != 16
        || event.sequence == 0
        || event.payload.is_none()
    {
        return Err(IngestError::InvalidEvent);
    }
    let expected = Store::event_id(&event.node_uuid, &event.session_id, event.sequence)
        .map_err(|_| IngestError::EventIdMismatch)?;
    if expected != event.event_id.as_slice() {
        return Err(IngestError::EventIdMismatch);
    }
    if event.event_type == proto::TelemetryEventType::Unspecified as i32
        || event.priority == proto::TelemetryPriority::Unspecified as i32
    {
        return Err(IngestError::IncompleteEvent);
    }
    Ok(())
}

pub fn validate_gap(gap: &proto::SequenceGap) -> StdResult<(), IngestError> {
    if gap.gap_id.len() != 16
        || gap.node_uuid.len() != 16
        || gap.session_id.len() != 16
        || gap.start_sequence == 0
        || gap.end_sequence < gap.start_sequence
        || gap.reason == proto::GapReason::Unspecified as i32
    {
        return Err(IngestError::InvalidGap);
    }
    Ok(())
}

struct EventMeta {
    clock_untrusted: bool,
    persist: bool,
}

impl Store {
    /// 在调用方事务内摄入一个批次。`received_at_nanos` 为主端接收钟（纳秒）。
    pub fn ingest(
        &self,
        tx: &Transaction<'_>,
        batch: &proto::TelemetryBatch,
        observer_id: &str,
        received_at_nanos: i64,
        hole_grace_nanos: i64,
    ) -> anyhow::Result<IngestResult> {
        if observer_id.is_empty() {
            anyhow::bail!("telemetry batch and observer ID are required");
        }
        let mut result = IngestResult {
            acks: Vec::new(),
            fresh_events: Vec::new(),
        };
        // session key = hex(node)/hex(session)；排序后推进游标，顺序确定。
        let mut max_by_session: HashMap<String, u64> = HashMap::new();
        let mut session_ids: HashMap<String, Vec<u8>> = HashMap::new();
        let mut node_by_session: HashMap<String, Vec<u8>> = HashMap::new();
        let mut batch_sequences: HashMap<String, Vec<u64>> = HashMap::new();

        for record in &batch.records {
            match &record.record {
                Some(proto::telemetry_record::Record::Event(event)) => {
                    validate_event(event).map_err(|err| anyhow::anyhow!(err.to_string()))?;
                    let meta = self.accept_event(tx, event, received_at_nanos)?;
                    if meta.persist {
                        tx.execute(
                            "INSERT INTO telemetry_observations (event_id, observer_id, node_uuid, received_at, created_at) \
                             VALUES (?1, ?2, ?3, ?4, CURRENT_TIMESTAMP) \
                             ON CONFLICT (event_id, observer_id) DO NOTHING",
                            rusqlite::params![event.event_id, observer_id, event.node_uuid, received_at_nanos],
                        )?;
                    }
                    // 证据桶键必须用观测方接收钟：探针钟漂移不可控。
                    self.record_path_evidence(
                        tx,
                        &event.node_uuid,
                        observer_id,
                        received_at_nanos,
                    )?;
                    // 不落 telemetry_events 的类型（HEARTBEAT/STATE/STATE_ROLLUP）
                    // 单独留序号回执：否则 advance_cursor 只查 telemetry_events，
                    // 跨批次把这些序列当空洞，宽限一到就误报「探测丢失」。
                    if !meta.persist {
                        tx.execute(
                            "INSERT OR IGNORE INTO telemetry_sequence_receipts                              (receiver_id, node_uuid, session_id, sequence)                              VALUES (?1, ?2, ?3, ?4)",
                            rusqlite::params![
                                observer_id,
                                event.node_uuid,
                                event.session_id,
                                event.sequence
                            ],
                        )?;
                    }
                    let key = session_key(&event.node_uuid, &event.session_id);
                    if event.sequence > *max_by_session.get(&key).unwrap_or(&0) {
                        max_by_session.insert(key.clone(), event.sequence);
                    }
                    session_ids.insert(key.clone(), event.session_id.clone());
                    node_by_session.insert(key.clone(), event.node_uuid.clone());
                    batch_sequences.entry(key).or_default().push(event.sequence);
                    if !meta.clock_untrusted {
                        result.fresh_events.push(event.clone());
                    }
                }
                Some(proto::telemetry_record::Record::Gap(gap)) => {
                    validate_gap(gap).map_err(|err| anyhow::anyhow!(err.to_string()))?;
                    tx.execute(
                        "INSERT INTO telemetry_gaps (gap_id, node_uuid, session_id, start_sequence, end_sequence, reason, replacement_event_id, created_at_unix_nano, created_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, CURRENT_TIMESTAMP) \
                         ON CONFLICT (gap_id) DO NOTHING",
                        rusqlite::params![
                            gap.gap_id,
                            gap.node_uuid,
                            gap.session_id,
                            gap.start_sequence,
                            gap.end_sequence,
                            gap.reason,
                            gap.replacement_event_id,
                            gap.created_at_unix_nano,
                        ],
                    )?;
                    let key = session_key(&gap.node_uuid, &gap.session_id);
                    if gap.end_sequence > *max_by_session.get(&key).unwrap_or(&0) {
                        max_by_session.insert(key.clone(), gap.end_sequence);
                    }
                    session_ids.insert(key.clone(), gap.session_id.clone());
                    node_by_session.insert(key.clone(), gap.node_uuid.clone());
                    batch_sequences
                        .entry(key)
                        .or_default()
                        .push(gap.end_sequence);
                }
                None => anyhow::bail!(IngestError::EmptyRecord.to_string()),
            }
        }

        let mut keys: Vec<&String> = max_by_session.keys().collect();
        keys.sort();
        for key in keys {
            let ack = self.advance_cursor(
                tx,
                observer_id,
                node_by_session
                    .get(key)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
                session_ids.get(key).map(Vec::as_slice).unwrap_or_default(),
                max_by_session[key],
                batch_sequences
                    .get(key)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
                received_at_nanos,
                hole_grace_nanos,
            )?;
            result.acks.push(proto::SessionAck {
                node_uuid: node_by_session[key].clone(),
                session_id: session_ids[key].clone(),
                ack_through: ack,
            });
        }
        Ok(result)
    }

    /// `Store.Replicate`：在调用方事务内摄入从端复制批次。
    ///
    /// 对齐 Go `service/telemetry/store.go` 的 `Replicate`：回执表按
    /// (collector, session, sequence) 去重（重复投递返回已提交游标，不重放）；
    /// 事件/观测/缺口/健康样本/数据丢失事实同事务落库，runtime 快照 upsert
    /// （含 `last_sync`）。**不推进摄入游标**——复制批次与直连摄入共用
    /// event_id 幂等，游标只由直连路径推进。
    pub fn replicate(
        &self,
        tx: &Transaction<'_>,
        batch: &proto::ReplicationBatch,
        received_at_nanos: i64,
    ) -> anyhow::Result<u64> {
        if batch.collector_uuid.is_empty()
            || batch.replication_session.len() != 16
            || batch.batch_sequence == 0
        {
            anyhow::bail!("invalid replication batch identity");
        }
        let committed: Option<i64> = tx
            .query_row(
                "SELECT committed_spool_through FROM collector_replication_receipts \
                 WHERE collector_uuid = ? AND replication_session = ? AND batch_sequence = ?",
                rusqlite::params![
                    batch.collector_uuid,
                    batch.replication_session,
                    batch.batch_sequence
                ],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(committed) = committed {
            // 重复投递也回执：从端按游标推进，不会倒退。
            return Ok(committed.max(0) as u64);
        }
        // event_id → (node_uuid, persist)；观测可引用本批之外的历史事件。
        let mut by_event_id: HashMap<Vec<u8>, (Vec<u8>, bool)> =
            HashMap::with_capacity(batch.events.len());
        for event in &batch.events {
            validate_event(event).map_err(|err| anyhow::anyhow!(err.to_string()))?;
            let meta = self.accept_event(tx, event, received_at_nanos)?;
            by_event_id.insert(
                event.event_id.clone(),
                (event.node_uuid.clone(), meta.persist),
            );
        }
        for observation in &batch.observations {
            if observation.event_id.len() != 16 || observation.observer_id.is_empty() {
                anyhow::bail!("invalid replicated observation");
            }
            let (node_uuid, persist) = match by_event_id.get(&observation.event_id) {
                Some((node_uuid, persist)) => (node_uuid.clone(), *persist),
                None => {
                    let row: Option<(Vec<u8>, i32)> = tx
                        .query_row(
                            "SELECT node_uuid, event_type FROM telemetry_events WHERE event_id = ?",
                            [observation.event_id.as_slice()],
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )
                        .optional()?;
                    match row {
                        Some((node_uuid, event_type)) => {
                            (node_uuid, persist_event_type(event_type))
                        }
                        // 引用了未知事件：与 Go 一致跳过，不阻断整批。
                        None => continue,
                    }
                }
            };
            if persist {
                tx.execute(
                    "INSERT INTO telemetry_observations (event_id, observer_id, node_uuid, received_at, replicated_at, metadata, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, CURRENT_TIMESTAMP) \
                     ON CONFLICT (event_id, observer_id) DO NOTHING",
                    rusqlite::params![
                        observation.event_id,
                        observation.observer_id,
                        node_uuid,
                        observation.received_at_unix_nano,
                        received_at_nanos,
                        observation.metadata,
                    ],
                )?;
            }
            // 证据桶键必须用观测方接收钟：探针钟漂移不可控；脏数据回退主端接收钟。
            let evidence_at = if observation.received_at_unix_nano > 0 {
                observation.received_at_unix_nano
            } else {
                received_at_nanos
            };
            self.record_path_evidence(tx, &node_uuid, &observation.observer_id, evidence_at)?;
        }
        for gap in &batch.gaps {
            validate_gap(gap).map_err(|err| anyhow::anyhow!(err.to_string()))?;
            tx.execute(
                "INSERT INTO telemetry_gaps (gap_id, node_uuid, session_id, start_sequence, end_sequence, reason, replacement_event_id, created_at_unix_nano, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, CURRENT_TIMESTAMP) \
                 ON CONFLICT (gap_id) DO NOTHING",
                rusqlite::params![
                    gap.gap_id,
                    gap.node_uuid,
                    gap.session_id,
                    gap.start_sequence,
                    gap.end_sequence,
                    gap.reason,
                    gap.replacement_event_id,
                    gap.created_at_unix_nano,
                ],
            )?;
        }
        for health in &batch.health {
            let bucket_start = health.sampled_at_unix_nano / self.bucket_nanos * self.bucket_nanos;
            tx.execute(
                "INSERT INTO observer_health_buckets (observer_id, bucket_start, healthy, process_session, revision, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, 1, CURRENT_TIMESTAMP) \
                 ON CONFLICT (observer_id, bucket_start) DO UPDATE SET \
                 healthy = ?3, process_session = ?4, revision = 1, updated_at = CURRENT_TIMESTAMP",
                rusqlite::params![
                    health.observer_id,
                    bucket_start,
                    health.healthy as i32,
                    health.process_session,
                ],
            )?;
            let mut stmt = tx.prepare(
                "SELECT node_uuid FROM observer_assignments \
                 WHERE observer_id = ? AND valid_from < ? AND (valid_to = 0 OR valid_to > ?)",
            )?;
            let nodes = stmt
                .query_map(
                    rusqlite::params![
                        health.observer_id,
                        bucket_start + self.bucket_nanos,
                        bucket_start
                    ],
                    |row| row.get::<_, Vec<u8>>(0),
                )?
                .collect::<Result<Vec<_>, _>>()?;
            drop(stmt);
            for node_uuid in nodes {
                enqueue_availability(tx, &node_uuid, bucket_start, "observer_health")?;
                // 上一个桶此刻已封窗，随健康样本触发终结重算，否则迟到证据的桶永远停在 unknown。
                enqueue_availability(
                    tx,
                    &node_uuid,
                    bucket_start - self.bucket_nanos,
                    "observer_health_closed",
                )?;
            }
        }
        for fact in &batch.data_loss {
            if fact.fact_id.len() != 16
                || fact.collector_uuid != batch.collector_uuid
                || fact.reason == proto::GapReason::Unspecified as i32
            {
                anyhow::bail!("invalid collector data loss fact");
            }
            tx.execute(
                "INSERT INTO telemetry_data_losses (fact_id, component_id, occurred_at, reason, first_spool_id, last_spool_id, lost_records, detail, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, CURRENT_TIMESTAMP) \
                 ON CONFLICT (fact_id) DO NOTHING",
                rusqlite::params![
                    fact.fact_id,
                    fact.collector_uuid,
                    fact.occurred_at_unix_nano,
                    fact.reason,
                    fact.first_spool_id,
                    fact.last_spool_id,
                    fact.lost_records,
                    fact.detail,
                ],
            )?;
        }
        if let Some(runtime) = &batch.runtime {
            let row = collectors::CollectorRuntimeRow::from_proto(&batch.collector_uuid, runtime);
            collectors::upsert_runtime(tx, &row, received_at_nanos, true)?;
        }
        tx.execute(
            "INSERT INTO collector_replication_receipts (collector_uuid, replication_session, batch_sequence, committed_spool_through, created_at) \
             VALUES (?1, ?2, ?3, ?4, CURRENT_TIMESTAMP)",
            rusqlite::params![
                batch.collector_uuid,
                batch.replication_session,
                batch.batch_sequence,
                batch.spool_through_id,
            ],
        )?;
        Ok(batch.spool_through_id)
    }

    fn accept_event(
        &self,
        tx: &Transaction<'_>,
        event: &proto::TelemetryEvent,
        received_at_nanos: i64,
    ) -> anyhow::Result<EventMeta> {
        let skew = (received_at_nanos - event.collected_at_unix_nano).abs();
        let clock_untrusted = skew > CLOCK_SKEW_LIMIT_NANOS;
        let persist = persist_event_type(event.event_type);
        let meta = EventMeta {
            clock_untrusted,
            persist,
        };
        if event.event_type == proto::TelemetryEventType::State as i32
            && let Some(proto::telemetry_event::Payload::State(state)) = &event.payload
        {
            self.states.add(
                &event.event_id,
                &event.node_uuid,
                event.collected_at_unix_nano,
                state,
            );
        }
        if event.event_type == proto::TelemetryEventType::StateRollup as i32
            && let Some(proto::telemetry_event::Payload::StateRollup(rollup)) = &event.payload
        {
            rollup::upsert_state_rollup(tx, &event.node_uuid, rollup::RESOLUTION_1M, rollup)?;
        }
        if !persist {
            return Ok(meta);
        }
        let encoded = prost::Message::encode_to_vec(event);
        tx.execute(
            "INSERT INTO telemetry_events (event_id, node_uuid, session_id, sequence, event_type, priority, collected_at, \
             agent_uptime_nano, session_elapsed_nano, clock_untrusted, source_protocol, reliability, payload, payload_retained, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, 1, CURRENT_TIMESTAMP) \
             ON CONFLICT (event_id) DO NOTHING",
            rusqlite::params![
                event.event_id,
                event.node_uuid,
                event.session_id,
                event.sequence,
                event.event_type,
                event.priority,
                event.collected_at_unix_nano,
                event.agent_uptime_nano,
                event.session_elapsed_nano,
                clock_untrusted as i32,
                event.source_protocol,
                event.reliability,
                encoded,
            ],
        )?;
        Ok(meta)
    }

    /// 落观测点对某节点的「看到」证据与健康行。evidence_at 必须是观测方接收钟。
    fn record_path_evidence(
        &self,
        tx: &Transaction<'_>,
        node_uuid: &[u8],
        observer_id: &str,
        evidence_at_nanos: i64,
    ) -> anyhow::Result<()> {
        let bucket_start = evidence_at_nanos / self.bucket_nanos * self.bucket_nanos;
        tx.execute(
            "INSERT INTO observer_health_buckets (observer_id, bucket_start, healthy, revision, updated_at) \
             VALUES (?1, ?2, 1, 1, CURRENT_TIMESTAMP) \
             ON CONFLICT (observer_id, bucket_start) DO UPDATE SET healthy = 1, updated_at = CURRENT_TIMESTAMP",
            rusqlite::params![observer_id, bucket_start],
        )?;
        tx.execute(
            "INSERT INTO observer_path_buckets (node_uuid, observer_id, bucket_start, seen, last_seen_at, revision, updated_at) \
             VALUES (?1, ?2, ?3, 1, ?4, 1, CURRENT_TIMESTAMP) \
             ON CONFLICT (node_uuid, observer_id, bucket_start) DO UPDATE SET seen = 1, last_seen_at = ?4, updated_at = CURRENT_TIMESTAMP",
            rusqlite::params![node_uuid, observer_id, bucket_start, evidence_at_nanos],
        )?;
        enqueue_availability(tx, node_uuid, bucket_start, "observation")
    }

    /// `advanceCursor`：按 (receiver, node, session) 推进 ack 游标。
    /// 空洞超过宽限才推断 COMPACTED 缺口（落 gap + data loss）后跳过。
    #[allow(clippy::too_many_arguments)]
    fn advance_cursor(
        &self,
        tx: &Transaction<'_>,
        receiver_id: &str,
        node_uuid: &[u8],
        session_id: &[u8],
        max_sequence: u64,
        batch_sequences: &[u64],
        received_at_nanos: i64,
        hole_grace_nanos: i64,
    ) -> anyhow::Result<u64> {
        struct Cursor {
            ack_through: u64,
            hole_sequence: u64,
            hole_since: i64,
        }
        let mut cursor = tx
            .query_row(
                "SELECT ack_through, hole_sequence, hole_since FROM telemetry_ingest_cursors \
                 WHERE receiver_id = ? AND node_uuid = ? AND session_id = ?",
                rusqlite::params![receiver_id, node_uuid, session_id],
                |row| {
                    Ok(Cursor {
                        ack_through: row.get::<_, i64>(0)?.max(0) as u64,
                        hole_sequence: row.get::<_, Option<i64>>(1)?.unwrap_or(0).max(0) as u64,
                        hole_since: row.get::<_, Option<i64>>(2)?.unwrap_or(0),
                    })
                },
            )
            .optional()?
            .unwrap_or(Cursor {
                ack_through: 0,
                hole_sequence: 0,
                hole_since: 0,
            });
        if max_sequence <= cursor.ack_through {
            return Ok(cursor.ack_through);
        }

        let mut present: HashSet<u64> = HashSet::new();
        {
            // 落表事件 + 不落表事件的序号回执，合起来才是「已收到的序列」全集。
            let mut stmt = tx.prepare(
                "SELECT sequence FROM telemetry_events \
                 WHERE node_uuid = ? AND session_id = ? AND sequence > ? AND sequence <= ? \
                 UNION \
                 SELECT sequence FROM telemetry_sequence_receipts \
                 WHERE receiver_id = ? AND node_uuid = ? AND session_id = ? \
                   AND sequence > ? AND sequence <= ?",
            )?;
            let rows = stmt.query_map(
                rusqlite::params![
                    node_uuid,
                    session_id,
                    cursor.ack_through,
                    max_sequence,
                    receiver_id,
                    node_uuid,
                    session_id,
                    cursor.ack_through,
                    max_sequence
                ],
                |row| Ok(row.get::<_, i64>(0)?.max(0) as u64),
            )?;
            for row in rows {
                present.insert(row?);
            }
        }
        for sequence in batch_sequences {
            if *sequence > cursor.ack_through && *sequence <= max_sequence {
                present.insert(*sequence);
            }
        }
        let mut gaps: Vec<(u64, u64)> = Vec::new();
        {
            let mut stmt = tx.prepare(
                "SELECT start_sequence, end_sequence FROM telemetry_gaps \
                 WHERE node_uuid = ? AND session_id = ? AND end_sequence > ? AND start_sequence <= ?",
            )?;
            let rows = stmt.query_map(
                rusqlite::params![node_uuid, session_id, cursor.ack_through, max_sequence],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?.max(0) as u64,
                        row.get::<_, i64>(1)?.max(0) as u64,
                    ))
                },
            )?;
            for row in rows {
                gaps.push(row?);
            }
        }
        let gap_covers = |gaps: &[(u64, u64)], sequence: u64| {
            gaps.iter()
                .any(|(start, end)| *start <= sequence && *end >= sequence)
        };

        while cursor.ack_through < max_sequence {
            let next = cursor.ack_through + 1;
            if present.contains(&next) || gap_covers(&gaps, next) {
                if present.contains(&next) {
                    cursor.ack_through = next;
                } else if let Some((_, end)) = gaps
                    .iter()
                    .find(|(start, end)| *start <= next && *end >= next)
                {
                    cursor.ack_through = *end;
                }
                if cursor.hole_sequence != 0 && cursor.ack_through >= cursor.hole_sequence {
                    cursor.hole_sequence = 0;
                    cursor.hole_since = 0;
                }
                continue;
            }
            if hole_grace_nanos <= 0 {
                break;
            }
            if cursor.hole_sequence != next {
                cursor.hole_sequence = next;
                cursor.hole_since = received_at_nanos;
                break;
            }
            if received_at_nanos - cursor.hole_since < hole_grace_nanos {
                break;
            }
            let mut end = next;
            while end < max_sequence && !present.contains(&(end + 1)) && !gap_covers(&gaps, end + 1)
            {
                end += 1;
            }
            infer_compacted_hole(
                tx,
                receiver_id,
                node_uuid,
                session_id,
                next,
                end,
                received_at_nanos,
            )?;
            gaps.push((next, end));
            cursor.ack_through = end;
            cursor.hole_sequence = 0;
            cursor.hole_since = 0;
        }

        tx.execute(
            "INSERT INTO telemetry_ingest_cursors (receiver_id, node_uuid, session_id, ack_through, hole_sequence, hole_since, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, CURRENT_TIMESTAMP) \
             ON CONFLICT (receiver_id, node_uuid, session_id) DO UPDATE SET \
             ack_through = ?4, hole_sequence = ?5, hole_since = ?6, updated_at = CURRENT_TIMESTAMP",
            rusqlite::params![
                receiver_id,
                node_uuid,
                session_id,
                cursor.ack_through,
                if cursor.hole_sequence == 0 { None } else { Some(cursor.hole_sequence) },
                if cursor.hole_since == 0 { None } else { Some(cursor.hole_since) },
            ],
        )?;
        // 已确认收到的序号不再需要回执：只保留 ack_through 之上的桥接行。
        tx.execute(
            "DELETE FROM telemetry_sequence_receipts \
             WHERE receiver_id = ?1 AND node_uuid = ?2 AND session_id = ?3 AND sequence <= ?4",
            rusqlite::params![receiver_id, node_uuid, session_id, cursor.ack_through],
        )?;
        Ok(cursor.ack_through)
    }
}

fn session_key(node_uuid: &[u8], session_id: &[u8]) -> String {
    format!("{}/{}", hex::encode(node_uuid), hex::encode(session_id))
}

/// `inferredGapID`：SHA-256(node‖session‖start_be‖end_be)[:16]。
fn inferred_gap_id(node_uuid: &[u8], session_id: &[u8], start: u64, end: u64) -> [u8; 16] {
    let mut input = [0u8; 48];
    input[..16].copy_from_slice(node_uuid);
    input[16..32].copy_from_slice(session_id);
    input[32..40].copy_from_slice(&start.to_be_bytes());
    input[40..48].copy_from_slice(&end.to_be_bytes());
    let digest = Sha256::digest(input);
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    out
}

/// 超窗空洞：落 COMPACTED 缺口与数据丢失事实，ack 跳到 end。
fn infer_compacted_hole(
    tx: &Transaction<'_>,
    receiver_id: &str,
    node_uuid: &[u8],
    session_id: &[u8],
    start: u64,
    end: u64,
    received_at_nanos: i64,
) -> anyhow::Result<()> {
    let gap_id = inferred_gap_id(node_uuid, session_id, start, end);
    tx.execute(
        "INSERT INTO telemetry_gaps (gap_id, node_uuid, session_id, start_sequence, end_sequence, reason, created_at_unix_nano, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, CURRENT_TIMESTAMP) ON CONFLICT (gap_id) DO NOTHING",
        rusqlite::params![
            gap_id.as_slice(),
            node_uuid,
            session_id,
            start,
            end,
            proto::GapReason::Compacted as i32,
            received_at_nanos,
        ],
    )?;
    tx.execute(
        "INSERT INTO telemetry_data_losses (fact_id, component_id, occurred_at, reason, first_spool_id, last_spool_id, lost_records, detail, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, CURRENT_TIMESTAMP) ON CONFLICT (fact_id) DO NOTHING",
        rusqlite::params![
            gap_id.as_slice(),
            receiver_id,
            received_at_nanos,
            proto::GapReason::Compacted as i32,
            start,
            end,
            // `start`/`end` 是序列号区间，u64 直接绑定；`ToSql for u64` 是 fallible 的，
            // 越界会响亮报错，而不是 `as i64` 那样静默回绕成负数。
            end.saturating_sub(start).saturating_add(1),
            format!("receiver inferred compacted hole {start}-{end}"),
        ],
    )?;
    Ok(())
}

fn enqueue_availability(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    bucket_start: i64,
    reason: &str,
) -> anyhow::Result<()> {
    if bucket_start <= 0 {
        return Ok(());
    }
    tx.execute(
        "INSERT INTO availability_recompute_queues (node_uuid, bucket_start, reason, created_at, updated_at) \
         VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT (node_uuid, bucket_start) DO UPDATE SET reason = ?3, updated_at = CURRENT_TIMESTAMP",
        rusqlite::params![node_uuid, bucket_start, reason],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;
    use crate::repo::bindings::PRIMARY_OBSERVER_ID;

    fn temp_db(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!("santaizi-store-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("panel.db").to_string_lossy().into_owned()
    }

    fn event(sequence: u64, event_type: proto::TelemetryEventType) -> proto::TelemetryEvent {
        let node = [3u8; 16];
        let session = [4u8; 16];
        let event_id = Store::event_id(&node, &session, sequence).unwrap();
        proto::TelemetryEvent {
            event_id: event_id.to_vec(),
            node_uuid: node.to_vec(),
            session_id: session.to_vec(),
            sequence,
            event_type: event_type as i32,
            priority: proto::TelemetryPriority::P1Important as i32,
            collected_at_unix_nano: 1_700_000_000_000_000_000,
            payload: Some(proto::telemetry_event::Payload::State(proto::State {
                uptime: 1,
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[test]
    fn ingest_persists_events_and_acks_through_max() {
        let path = temp_db("ingest");
        let db = migrate::open_primary(&path).unwrap();
        let store = Store::new(30);
        let received = 1_700_000_000_000_000_000i64;
        let batch = proto::TelemetryBatch {
            records: vec![
                proto::TelemetryRecord {
                    record: Some(proto::telemetry_record::Record::Event(event(
                        1,
                        proto::TelemetryEventType::Host,
                    ))),
                },
                proto::TelemetryRecord {
                    record: Some(proto::telemetry_record::Record::Event(event(
                        2,
                        proto::TelemetryEventType::Host,
                    ))),
                },
            ],
        };
        let result = db
            .write(|tx| {
                store.ingest(
                    tx,
                    &batch,
                    PRIMARY_OBSERVER_ID,
                    received,
                    300 * 1_000_000_000,
                )
            })
            .unwrap();
        assert_eq!(result.acks.len(), 1);
        assert_eq!(result.acks[0].ack_through, 2);
        assert_eq!(result.fresh_events.len(), 2);
        let count: i64 = db
            .read(|conn| {
                Ok(conn.query_row("SELECT COUNT(*) FROM telemetry_events", [], |r| r.get(0))?)
            })
            .unwrap();
        assert_eq!(count, 2);
        // STATE 不落表但进缓冲
        let state_batch = proto::TelemetryBatch {
            records: vec![proto::TelemetryRecord {
                record: Some(proto::telemetry_record::Record::Event(event(
                    3,
                    proto::TelemetryEventType::State,
                ))),
            }],
        };
        db.write(|tx| store.ingest(tx, &state_batch, PRIMARY_OBSERVER_ID, received, 0))
            .unwrap();
        assert_eq!(store.states().len(), 1);
        let count: i64 = db
            .read(|conn| {
                Ok(conn.query_row("SELECT COUNT(*) FROM telemetry_events", [], |r| r.get(0))?)
            })
            .unwrap();
        assert_eq!(count, 2);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    /// HEARTBEAT/STATE 不落 `telemetry_events`，但它们的 sequence 同样占位。
    /// 游标推进只查 `telemetry_events` 时，跨批次会把 3、5 当成空洞，
    /// 宽限一到就 infer_compacted_hole + data loss —— 误报「探测丢失」。
    #[test]
    fn unpersisted_event_sequences_do_not_look_like_holes() {
        let path = temp_db("receipts");
        let db = migrate::open_primary(&path).unwrap();
        let store = Store::new(30);
        let received = 1_700_000_000_000_000_000i64;
        // 宽限给 0：任何被当成空洞的序列都会立刻推断成缺口。
        let grace = 0i64;

        // 批次 1：seq 1 (Host，落表) + seq 2 (STATE，不落表)。
        let first = proto::TelemetryBatch {
            records: vec![
                proto::TelemetryRecord {
                    record: Some(proto::telemetry_record::Record::Event(event(
                        1,
                        proto::TelemetryEventType::Host,
                    ))),
                },
                proto::TelemetryRecord {
                    record: Some(proto::telemetry_record::Record::Event(event(
                        2,
                        proto::TelemetryEventType::State,
                    ))),
                },
            ],
        };
        let result = db
            .write(|tx| store.ingest(tx, &first, PRIMARY_OBSERVER_ID, received, grace))
            .unwrap();
        assert_eq!(result.acks[0].ack_through, 2);

        // 批次 2：只有 seq 4 (Host)。seq 3 是个 STATE（不落表）——它必须被
        // 序号回承认出来，不能算空洞。
        let second = proto::TelemetryBatch {
            records: vec![
                proto::TelemetryRecord {
                    record: Some(proto::telemetry_record::Record::Event(event(
                        3,
                        proto::TelemetryEventType::Heartbeat,
                    ))),
                },
                proto::TelemetryRecord {
                    record: Some(proto::telemetry_record::Record::Event(event(
                        4,
                        proto::TelemetryEventType::Host,
                    ))),
                },
            ],
        };
        let result = db
            .write(|tx| store.ingest(tx, &second, PRIMARY_OBSERVER_ID, received, grace))
            .unwrap();
        assert_eq!(result.acks[0].ack_through, 4, "回执应桥接 seq 3");

        let gaps: i64 = db
            .read(|conn| {
                Ok(conn.query_row("SELECT COUNT(*) FROM telemetry_gaps", [], |r| r.get(0))?)
            })
            .unwrap();
        assert_eq!(gaps, 0, "不得推断出任何缺口");
        let losses: i64 = db
            .read(|conn| {
                Ok(
                    conn.query_row("SELECT COUNT(*) FROM telemetry_data_losses", [], |r| {
                        r.get(0)
                    })?,
                )
            })
            .unwrap();
        assert_eq!(losses, 0, "不得误报数据丢失");

        // 回执表只保留 ack 之上的行：推进完成后应被清空。
        let receipts: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM telemetry_sequence_receipts",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(receipts, 0, "已确认的序号回执应被清理");

        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn genuinely_missing_sequence_still_reports_loss() {
        // 反向对照：真的丢了 seq 3（连回执都没有）时，缺口推断必须照旧工作。
        let path = temp_db("real-hole");
        let db = migrate::open_primary(&path).unwrap();
        let store = Store::new(30);
        let received = 1_700_000_000_000_000_000i64;
        // 宽限必须 > 0：`hole_grace_nanos <= 0` 是「关闭空洞推断」的保险阀。
        let grace = 1i64;
        let first = proto::TelemetryBatch {
            records: vec![proto::TelemetryRecord {
                record: Some(proto::telemetry_record::Record::Event(event(
                    1,
                    proto::TelemetryEventType::Host,
                ))),
            }],
        };
        db.write(|tx| store.ingest(tx, &first, PRIMARY_OBSERVER_ID, received, grace))
            .unwrap();
        let second = proto::TelemetryBatch {
            records: vec![proto::TelemetryRecord {
                record: Some(proto::telemetry_record::Record::Event(event(
                    4,
                    proto::TelemetryEventType::Host,
                ))),
            }],
        };
        // 第一次看到空洞只登记 hole_sequence 并 break（需要两次相隔宽限的观测）。
        let result = db
            .write(|tx| store.ingest(tx, &second, PRIMARY_OBSERVER_ID, received, grace))
            .unwrap();
        assert_eq!(result.acks[0].ack_through, 1, "首次目击空洞不推进游标");

        // 第三个批次（时间越过宽限）：空洞 2-3 被推断成 COMPACTED 缺口。
        let third = proto::TelemetryBatch {
            records: vec![proto::TelemetryRecord {
                record: Some(proto::telemetry_record::Record::Event(event(
                    5,
                    proto::TelemetryEventType::Host,
                ))),
            }],
        };
        let result = db
            .write(|tx| store.ingest(tx, &third, PRIMARY_OBSERVER_ID, received + grace + 1, grace))
            .unwrap();
        assert_eq!(result.acks[0].ack_through, 5, "空洞被跳过并推进游标");
        let gaps: i64 = db
            .read(|conn| {
                Ok(conn.query_row("SELECT COUNT(*) FROM telemetry_gaps", [], |r| r.get(0))?)
            })
            .unwrap();
        assert_eq!(gaps, 1, "真实空洞必须推断出缺口");
        let losses: i64 = db
            .read(|conn| {
                Ok(
                    conn.query_row("SELECT COUNT(*) FROM telemetry_data_losses", [], |r| {
                        r.get(0)
                    })?,
                )
            })
            .unwrap();
        assert_eq!(losses, 1, "真实空洞必须记数据丢失");
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn ingest_is_idempotent_for_replayed_batch() {
        let path = temp_db("idempotent");
        let db = migrate::open_primary(&path).unwrap();
        let store = Store::new(30);
        let received = 1_700_000_000_000_000_000i64;
        let batch = proto::TelemetryBatch {
            records: vec![proto::TelemetryRecord {
                record: Some(proto::telemetry_record::Record::Event(event(
                    1,
                    proto::TelemetryEventType::Host,
                ))),
            }],
        };
        for _ in 0..2 {
            let result = db
                .write(|tx| store.ingest(tx, &batch, PRIMARY_OBSERVER_ID, received, 0))
                .unwrap();
            assert_eq!(result.acks[0].ack_through, 1);
        }
        let count: i64 = db
            .read(|conn| {
                Ok(conn.query_row("SELECT COUNT(*) FROM telemetry_events", [], |r| r.get(0))?)
            })
            .unwrap();
        assert_eq!(count, 1);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn hole_defers_ack_until_grace_then_infers_compacted_gap() {
        let path = temp_db("hole");
        let db = migrate::open_primary(&path).unwrap();
        let store = Store::new(30);
        let received = 1_700_000_000_000_000_000i64;
        let grace = 300 * 1_000_000_000i64;
        // 先到 seq 3，1 缺失 → 宽限内 ack 停在 0，记录 hole。
        let batch = proto::TelemetryBatch {
            records: vec![proto::TelemetryRecord {
                record: Some(proto::telemetry_record::Record::Event(event(
                    3,
                    proto::TelemetryEventType::Host,
                ))),
            }],
        };
        let result = db
            .write(|tx| store.ingest(tx, &batch, PRIMARY_OBSERVER_ID, received, grace))
            .unwrap();
        assert_eq!(result.acks[0].ack_through, 0);
        // 宽限未到：仍不动。
        let later = received + grace - 1;
        db.write(|tx| store.ingest(tx, &batch, PRIMARY_OBSERVER_ID, later, grace))
            .unwrap();
        // 宽限已过：推断 COMPACTED 1-2，ack 跳到 3。
        let after = received + grace + 1;
        let result = db
            .write(|tx| store.ingest(tx, &batch, PRIMARY_OBSERVER_ID, after, grace))
            .unwrap();
        assert_eq!(result.acks[0].ack_through, 3);
        let gaps: i64 = db
            .read(|conn| {
                Ok(conn.query_row("SELECT COUNT(*) FROM telemetry_gaps", [], |r| r.get(0))?)
            })
            .unwrap();
        assert_eq!(gaps, 1);
        let losses: i64 = db
            .read(|conn| {
                Ok(
                    conn.query_row("SELECT COUNT(*) FROM telemetry_data_losses", [], |r| {
                        r.get(0)
                    })?,
                )
            })
            .unwrap();
        assert_eq!(losses, 1);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn gap_record_covers_hole() {
        let path = temp_db("gap");
        let db = migrate::open_primary(&path).unwrap();
        let store = Store::new(30);
        let received = 1_700_000_000_000_000_000i64;
        let gap = proto::SequenceGap {
            gap_id: Store::event_id(&[3u8; 16], &[4u8; 16], 1).unwrap().to_vec(),
            node_uuid: vec![3u8; 16],
            session_id: vec![4u8; 16],
            start_sequence: 1,
            end_sequence: 2,
            reason: proto::GapReason::Compacted as i32,
            created_at_unix_nano: received,
            ..Default::default()
        };
        let batch = proto::TelemetryBatch {
            records: vec![
                proto::TelemetryRecord {
                    record: Some(proto::telemetry_record::Record::Gap(gap)),
                },
                proto::TelemetryRecord {
                    record: Some(proto::telemetry_record::Record::Event(event(
                        3,
                        proto::TelemetryEventType::Host,
                    ))),
                },
            ],
        };
        let result = db
            .write(|tx| store.ingest(tx, &batch, PRIMARY_OBSERVER_ID, received, 0))
            .unwrap();
        assert_eq!(result.acks[0].ack_through, 3);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn tampered_event_id_is_rejected() {
        let path = temp_db("tamper");
        let db = migrate::open_primary(&path).unwrap();
        let store = Store::new(30);
        let mut bad = event(1, proto::TelemetryEventType::Host);
        bad.event_id[0] ^= 0xff;
        let batch = proto::TelemetryBatch {
            records: vec![proto::TelemetryRecord {
                record: Some(proto::telemetry_record::Record::Event(bad)),
            }],
        };
        let err = db
            .write(|tx| {
                store.ingest(
                    tx,
                    &batch,
                    PRIMARY_OBSERVER_ID,
                    1_700_000_000_000_000_000,
                    0,
                )
            })
            .unwrap_err();
        assert!(err.to_string().contains("identity tuple"));
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn clock_untrusted_events_are_not_fresh() {
        let path = temp_db("skew");
        let db = migrate::open_primary(&path).unwrap();
        let store = Store::new(30);
        let mut skewed = event(1, proto::TelemetryEventType::Host);
        skewed.collected_at_unix_nano = 1_000_000_000_000_000_000; // 远早于接收钟
        let batch = proto::TelemetryBatch {
            records: vec![proto::TelemetryRecord {
                record: Some(proto::telemetry_record::Record::Event(skewed)),
            }],
        };
        let result = db
            .write(|tx| {
                store.ingest(
                    tx,
                    &batch,
                    PRIMARY_OBSERVER_ID,
                    1_700_000_000_000_000_000,
                    0,
                )
            })
            .unwrap();
        assert!(result.fresh_events.is_empty());
        assert_eq!(result.acks[0].ack_through, 1);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    const REPLICATE_RECEIVED: i64 = 1_700_000_000_000_000_000;

    fn replicate_batch(sequence: u64) -> proto::ReplicationBatch {
        proto::ReplicationBatch {
            collector_uuid: "collector-1".to_string(),
            replication_session: vec![9u8; 16],
            batch_sequence: sequence,
            spool_through_id: 100 + sequence,
            events: vec![event(1, proto::TelemetryEventType::Host)],
            observations: vec![proto::TelemetryObservation {
                event_id: event(1, proto::TelemetryEventType::Host).event_id,
                observer_id: "observer-a".to_string(),
                received_at_unix_nano: REPLICATE_RECEIVED,
                metadata: Vec::new(),
                ..Default::default()
            }],
            gaps: vec![proto::SequenceGap {
                gap_id: Store::event_id(&[3u8; 16], &[4u8; 16], 7).unwrap().to_vec(),
                node_uuid: vec![3u8; 16],
                session_id: vec![4u8; 16],
                start_sequence: 5,
                end_sequence: 6,
                reason: proto::GapReason::Compacted as i32,
                created_at_unix_nano: REPLICATE_RECEIVED,
                ..Default::default()
            }],
            health: vec![proto::ObserverHealthSample {
                observer_id: "observer-a".to_string(),
                sampled_at_unix_nano: REPLICATE_RECEIVED,
                healthy: true,
                process_session: "sess-a".to_string(),
            }],
            runtime: Some(proto::CollectorRuntime {
                collector_uuid: "collector-1".to_string(),
                spool_size: 42,
                protocol_version: "2".to_string(),
                software_version: "0.1.0".to_string(),
                replication_rtt_ms: 12.5,
                replication_rtt_sampled_at_unix_nano: REPLICATE_RECEIVED,
                ..Default::default()
            }),
            data_loss: vec![proto::CollectorDataLossFact {
                fact_id: Store::event_id(&[7u8; 16], &[8u8; 16], 1).unwrap().to_vec(),
                collector_uuid: "collector-1".to_string(),
                occurred_at_unix_nano: REPLICATE_RECEIVED,
                reason: proto::GapReason::Compacted as i32,
                first_spool_id: 5,
                last_spool_id: 6,
                lost_records: 2,
                detail: "edge spool gap".to_string(),
            }],
        }
    }

    #[test]
    fn replicate_persists_batch_and_writes_receipt() {
        let path = temp_db("replicate");
        let db = migrate::open_primary(&path).unwrap();
        let store = Store::new(30);
        db.write(|tx| {
            tx.execute(
                "INSERT INTO observer_assignments (node_uuid, observer_id, valid_from, valid_to, generation, config_version) \
                 VALUES (?1, 'observer-a', 1, 0, 1, 1)",
                rusqlite::params![[3u8; 16].as_slice()],
            )?;
            Ok(())
        })
        .unwrap();
        let batch = replicate_batch(1);
        let committed = db
            .write(|tx| store.replicate(tx, &batch, REPLICATE_RECEIVED))
            .unwrap();
        assert_eq!(committed, 101);

        let events: i64 = db
            .read(|conn| {
                Ok(conn.query_row("SELECT COUNT(*) FROM telemetry_events", [], |r| r.get(0))?)
            })
            .unwrap();
        assert_eq!(events, 1);
        let (received_at, replicated_at): (i64, i64) = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT received_at, replicated_at FROM telemetry_observations \
                     WHERE observer_id = 'observer-a'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .unwrap();
        assert_eq!(received_at, REPLICATE_RECEIVED);
        assert_eq!(replicated_at, REPLICATE_RECEIVED);
        // 证据桶 + 健康桶 + 队列（observer_health 与上一封窗桶 observer_health_closed）。
        let health: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT healthy FROM observer_health_buckets WHERE observer_id = 'observer-a'",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(health, 1);
        let queued: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM availability_recompute_queues",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(queued, 2);
        let reasons: Vec<String> = db
            .read(|conn| {
                let mut stmt = conn.prepare("SELECT reason FROM availability_recompute_queues")?;
                let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
                Ok(rows.collect::<Result<_, _>>()?)
            })
            .unwrap();
        assert!(reasons.contains(&"observer_health".to_string()));
        assert!(reasons.contains(&"observer_health_closed".to_string()));
        let gaps: i64 = db
            .read(|conn| {
                Ok(conn.query_row("SELECT COUNT(*) FROM telemetry_gaps", [], |r| r.get(0))?)
            })
            .unwrap();
        assert_eq!(gaps, 1);
        let losses: i64 = db
            .read(|conn| {
                Ok(
                    conn.query_row("SELECT COUNT(*) FROM telemetry_data_losses", [], |r| {
                        r.get(0)
                    })?,
                )
            })
            .unwrap();
        assert_eq!(losses, 1);
        // runtime upsert 带 last_sync（复制路径），值取本次接收钟。
        let (last_seen, last_sync): (i64, i64) = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT last_seen, last_sync FROM collector_runtimes WHERE collector_uuid = 'collector-1'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .unwrap();
        assert_eq!(last_seen, REPLICATE_RECEIVED);
        assert_eq!(last_sync, REPLICATE_RECEIVED);
        let receipts: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM collector_replication_receipts WHERE batch_sequence = 1",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(receipts, 1);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn replicate_is_idempotent_via_receipt() {
        let path = temp_db("replicate-dup");
        let db = migrate::open_primary(&path).unwrap();
        let store = Store::new(30);
        let batch = replicate_batch(1);
        let first = db
            .write(|tx| store.replicate(tx, &batch, REPLICATE_RECEIVED))
            .unwrap();
        let second = db
            .write(|tx| store.replicate(tx, &batch, REPLICATE_RECEIVED + 5))
            .unwrap();
        // 重复投递返回已提交游标，不重放也不报错。
        assert_eq!(first, 101);
        assert_eq!(second, 101);
        let events: i64 = db
            .read(|conn| {
                Ok(conn.query_row("SELECT COUNT(*) FROM telemetry_events", [], |r| r.get(0))?)
            })
            .unwrap();
        assert_eq!(events, 1);
        let observations: i64 = db
            .read(|conn| {
                Ok(
                    conn.query_row("SELECT COUNT(*) FROM telemetry_observations", [], |r| {
                        r.get(0)
                    })?,
                )
            })
            .unwrap();
        assert_eq!(observations, 1);
        let receipts: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM collector_replication_receipts",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(receipts, 1);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn replicate_rejects_invalid_batch_identity() {
        let path = temp_db("replicate-identity");
        let db = migrate::open_primary(&path).unwrap();
        let store = Store::new(30);
        for mutate in [
            |batch: &mut proto::ReplicationBatch| batch.collector_uuid.clear(),
            |batch: &mut proto::ReplicationBatch| batch.replication_session = vec![1u8; 8],
            |batch: &mut proto::ReplicationBatch| batch.batch_sequence = 0,
        ] {
            let mut batch = replicate_batch(1);
            mutate(&mut batch);
            let err = db
                .write(|tx| store.replicate(tx, &batch, REPLICATE_RECEIVED))
                .unwrap_err();
            assert!(
                err.to_string()
                    .contains("invalid replication batch identity")
            );
        }
        let receipts: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM collector_replication_receipts",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(receipts, 0);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn replicate_rejects_invalid_data_loss_fact() {
        let path = temp_db("replicate-loss");
        let db = migrate::open_primary(&path).unwrap();
        let store = Store::new(30);
        // collector 不匹配：整批拒绝，且不写回执。
        let mut batch = replicate_batch(1);
        batch.data_loss[0].collector_uuid = "other".to_string();
        let err = db
            .write(|tx| store.replicate(tx, &batch, REPLICATE_RECEIVED))
            .unwrap_err();
        assert!(err.to_string().contains("invalid collector data loss fact"));
        // reason 未指定同样拒绝。
        let mut batch = replicate_batch(2);
        batch.data_loss[0].reason = proto::GapReason::Unspecified as i32;
        let err = db
            .write(|tx| store.replicate(tx, &batch, REPLICATE_RECEIVED))
            .unwrap_err();
        assert!(err.to_string().contains("invalid collector data loss fact"));
        let receipts: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM collector_replication_receipts",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(receipts, 0);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn replicate_skips_observations_of_unknown_events() {
        let path = temp_db("replicate-unknown");
        let db = migrate::open_primary(&path).unwrap();
        let store = Store::new(30);
        let mut batch = replicate_batch(1);
        batch.observations.push(proto::TelemetryObservation {
            event_id: vec![0xAAu8; 16],
            observer_id: "observer-a".to_string(),
            received_at_unix_nano: REPLICATE_RECEIVED,
            ..Default::default()
        });
        db.write(|tx| store.replicate(tx, &batch, REPLICATE_RECEIVED))
            .unwrap();
        let observations: i64 = db
            .read(|conn| {
                Ok(
                    conn.query_row("SELECT COUNT(*) FROM telemetry_observations", [], |r| {
                        r.get(0)
                    })?,
                )
            })
            .unwrap();
        assert_eq!(observations, 1);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }

    #[test]
    fn replicate_health_without_assignment_marks_bucket_only() {
        let path = temp_db("replicate-health");
        let db = migrate::open_primary(&path).unwrap();
        let store = Store::new(30);
        let batch = replicate_batch(1);
        db.write(|tx| store.replicate(tx, &batch, REPLICATE_RECEIVED))
            .unwrap();
        let health: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM observer_health_buckets WHERE observer_id = 'observer-a'",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(health, 1);
        // 无指派：健康样本不产生 observer_health 队列项；仅观测证据各一条。
        let queued: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM availability_recompute_queues",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(queued, 1);
        let reason: String = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT reason FROM availability_recompute_queues",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(reason, "observation");
        let _ = std::fs::remove_dir_all(std::path::Path::new(&path).parent().unwrap());
    }
}
