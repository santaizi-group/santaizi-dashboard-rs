//! 探测面告警：对齐 Go `service/telemetry/alerts.go`。
//!
//! 约 15 秒扫描可用性事件、从端离线/恢复和数据丢失，写入 `telemetry_alerts`。
//! 离线历史和离线通知同时打开时，不再为 `HOST_OFFLINE` 重复发通知。

use prost::Message as _;
use rusqlite::{Connection, OptionalExtension, params};

use crate::config::Config;
use crate::repo::insights::COLLECTOR_TIMEOUT_NANOS;

pub const HOST_OFFLINE: &str = "HOST_OFFLINE";
pub const CONNECTIVITY_DEGRADED: &str = "CONNECTIVITY_DEGRADED";
pub const COLLECTOR_OFFLINE: &str = "COLLECTOR_OFFLINE";
pub const COLLECTOR_ONLINE: &str = "COLLECTOR_ONLINE";
pub const DATA_LOSS: &str = "TELEMETRY_DATA_LOSS";

#[derive(Debug, Clone)]
pub struct AlertPolicy {
    pub notify_host_offline: bool,
    pub notify_connectivity: bool,
    pub notify_correction: bool,
    pub notify_collector_offline: bool,
    pub notify_collector_online: bool,
    pub notify_data_loss: bool,
    pub suppress_host_offline_notify: bool,
    pub collector_timeout_nanos: i64,
    pub availability_bucket_nanos: i64,
}

impl AlertPolicy {
    pub fn from_config(conf: &Config) -> Self {
        let bucket_seconds = if conf.telemetry.availability_bucket_seconds == 0 {
            30
        } else {
            conf.telemetry.availability_bucket_seconds
        };
        Self {
            notify_host_offline: conf.enable_offline_notification,
            notify_connectivity: conf.telemetry.enable_connectivity_notification,
            notify_correction: conf.telemetry.enable_correction_notification,
            notify_collector_offline: conf.telemetry.enable_collector_offline_notification,
            notify_collector_online: conf.telemetry.enable_collector_online_notification,
            notify_data_loss: conf.telemetry.enable_data_loss_notification,
            suppress_host_offline_notify: conf.enable_offline_history
                && conf.enable_offline_notification,
            collector_timeout_nanos: COLLECTOR_TIMEOUT_NANOS,
            availability_bucket_nanos: (bucket_seconds as i64).saturating_mul(1_000_000_000),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingNotice {
    pub alert_type: String,
    pub title: String,
    pub name: String,
    pub mute_key: String,
}

#[derive(Clone)]
struct Incident {
    id: i64,
    node_uuid: Vec<u8>,
    initial: String,
    current: String,
    revision: i64,
    started_at: i64,
    ended_at: i64,
}

pub fn scan(
    conn: &Connection,
    policy: &AlertPolicy,
    now_nanos: i64,
) -> anyhow::Result<Vec<PendingNotice>> {
    let mut pending = Vec::new();
    scan_incidents(conn, policy, now_nanos, &mut pending)?;
    scan_collectors(conn, policy, now_nanos, &mut pending)?;
    scan_data_loss(conn, policy, &mut pending)?;
    scan_event_data_loss(conn, policy, &mut pending)?;
    Ok(pending)
}

pub fn format_burst(notices: &[PendingNotice]) -> Vec<(String, String)> {
    let mut groups: Vec<(String, Vec<&PendingNotice>)> = Vec::new();
    for notice in notices {
        if let Some((_, items)) = groups
            .iter_mut()
            .find(|(kind, _)| kind == &notice.alert_type)
        {
            items.push(notice);
        } else {
            groups.push((notice.alert_type.clone(), vec![notice]));
        }
    }
    groups
        .into_iter()
        .map(|(_, items)| {
            if items.len() == 1 {
                (
                    format!("[{}] {}", items[0].title, items[0].name),
                    items[0].mute_key.clone(),
                )
            } else {
                let mut message = format!("[{}] {} 台", items[0].title, items.len());
                for item in &items {
                    message.push('\n');
                    message.push_str(&item.name);
                }
                (message, format!("bf::tburst-{}", items[0].alert_type))
            }
        })
        .collect()
}

fn scan_incidents(
    conn: &Connection,
    policy: &AlertPolicy,
    now_nanos: i64,
    pending: &mut Vec<PendingNotice>,
) -> anyhow::Result<()> {
    let cutoff = now_nanos.saturating_sub(3_600_000_000_000);
    let mut stmt = conn.prepare(
        "SELECT id, node_uuid, initial_classification, current_classification, revision, started_at, \
         COALESCE(ended_at, 0) FROM availability_incidents \
         WHERE current_classification IN ('HOST_OFFLINE', 'CONNECTIVITY_DEGRADED') \
           AND (COALESCE(ended_at, 0) = 0 OR started_at >= ?1)",
    )?;
    let rows = stmt.query_map([cutoff], |row| {
        Ok(Incident {
            id: row.get(0)?,
            node_uuid: row.get(1)?,
            initial: row.get(2)?,
            current: row.get(3)?,
            revision: row.get(4)?,
            started_at: row.get(5)?,
            ended_at: row.get(6)?,
        })
    })?;
    let mut incidents = Vec::new();
    for row in rows {
        incidents.push(row?);
    }
    drop(stmt);
    let bucket = policy.availability_bucket_nanos;
    for incident in incidents.iter() {
        let mut notify = match incident.current.as_str() {
            HOST_OFFLINE => policy.notify_host_offline && !policy.suppress_host_offline_notify,
            CONNECTIVITY_DEGRADED => policy.notify_connectivity,
            _ => false,
        };
        if incident.revision > 1
            && (incident.initial == incident.current || !policy.notify_correction)
        {
            notify = false;
        }
        let name = host_name(conn, &incident.node_uuid)?;
        let title = title(&incident.current);
        let message = format!("[{title}] {name}");
        let dedup = format!(
            "episode/{}/{}/{}",
            incident.current,
            hex(&incident.node_uuid),
            episode_anchor(conn, incident, bucket)?
        );
        if create_alert(
            conn,
            &dedup,
            &incident.current,
            "critical",
            Some(&incident.node_uuid),
            "",
            incident.started_at,
            &message,
            notify,
        )? && notify
        {
            pending.push(PendingNotice {
                alert_type: incident.current.clone(),
                title: title.to_string(),
                name,
                mute_key: format!("bf::tho-{}-{}", incident.current, hex(&incident.node_uuid)),
            });
        }
    }
    Ok(())
}

fn scan_collectors(
    conn: &Connection,
    policy: &AlertPolicy,
    now_nanos: i64,
    pending: &mut Vec<PendingNotice>,
) -> anyhow::Result<()> {
    let mut stmt = conn.prepare(
        "SELECT c.collector_uuid, COALESCE(c.name, ''), COALESCE(r.last_seen, 0), \
         COALESCE(CAST(strftime('%s', c.created_at) AS INTEGER), 0) \
         FROM collectors c \
         LEFT JOIN collector_runtimes r ON r.collector_uuid = c.collector_uuid \
         WHERE c.revoked = 0 AND c.deleted = 0",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
        ))
    })?;
    let mut collectors = Vec::new();
    for row in rows {
        collectors.push(row?);
    }
    drop(stmt);
    let cutoff = now_nanos.saturating_sub(policy.collector_timeout_nanos);
    for (uuid, name, last_seen, created_unix) in collectors {
        let created_nanos = created_unix.saturating_mul(1_000_000_000);
        let grace_new = last_seen == 0 && created_nanos > 0 && created_nanos > cutoff;
        if last_seen < cutoff && !grace_new {
            let display = display_name(&name, &uuid);
            let message = format!("[{}] {display}", title(COLLECTOR_OFFLINE));
            let dedup = format!("collector/{uuid}/{last_seen}");
            if create_alert(
                conn,
                &dedup,
                COLLECTOR_OFFLINE,
                "critical",
                None,
                &uuid,
                now_nanos,
                &message,
                policy.notify_collector_offline,
            )? && policy.notify_collector_offline
            {
                pending.push(PendingNotice {
                    alert_type: COLLECTOR_OFFLINE.into(),
                    title: title(COLLECTOR_OFFLINE).into(),
                    name: display,
                    mute_key: format!("bf::tco-{uuid}"),
                });
            }
            continue;
        }
        if last_seen == 0 || last_seen < cutoff {
            continue;
        }
        let Some(offline) = latest_alert(conn, COLLECTOR_OFFLINE, &uuid)? else {
            continue;
        };
        if let Some(online) = latest_alert(conn, COLLECTOR_ONLINE, &uuid)?
            && online.0 >= offline.0
        {
            continue;
        }
        let mut episode = episode_from_dedup(&offline.1);
        if episode == 0 {
            episode = offline.0;
        }
        if last_seen <= episode {
            continue;
        }
        let display = display_name(&name, &uuid);
        let message = format!("[{}] {display}", title(COLLECTOR_ONLINE));
        let dedup = format!("collector-online/{uuid}/{episode}");
        if create_alert(
            conn,
            &dedup,
            COLLECTOR_ONLINE,
            "info",
            None,
            &uuid,
            now_nanos,
            &message,
            policy.notify_collector_online,
        )? && policy.notify_collector_online
        {
            pending.push(PendingNotice {
                alert_type: COLLECTOR_ONLINE.into(),
                title: title(COLLECTOR_ONLINE).into(),
                name: display,
                mute_key: format!("bf::tcu-{uuid}"),
            });
        }
    }
    Ok(())
}

fn scan_data_loss(
    conn: &Connection,
    policy: &AlertPolicy,
    pending: &mut Vec<PendingNotice>,
) -> anyhow::Result<()> {
    let mut stmt = conn.prepare(
        "SELECT fact_id, component_id, occurred_at FROM telemetry_data_losses \
         ORDER BY occurred_at DESC LIMIT 1000",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;
    let mut losses = Vec::new();
    for row in rows {
        losses.push(row?);
    }
    drop(stmt);
    // 接收端推断的空洞（detail 以 `receiver inferred` 开头）按节点归并：
    // 一次重启会给每台机器、每个旧会话各写一条，component_id 却都是接收端
    // （`primary`）。直接拿 component_id 当名字，通知就变成 30 行 primary。
    // 同一轮（15 分钟）只发一条，名字用绑定的主机名。
    let mut inferred: std::collections::BTreeMap<Vec<u8>, InferredLoss> =
        std::collections::BTreeMap::new();
    for (fact_id, component_id, occurred_at) in losses {
        if let Some(node) = inferred_loss_node(conn, &fact_id)? {
            let entry = inferred.entry(node).or_insert(InferredLoss {
                component_id,
                occurred_at,
            });
            entry.occurred_at = entry.occurred_at.max(occurred_at);
            continue;
        }
        let message = format!("[{}] {component_id}", title(DATA_LOSS));
        let dedup = format!("data-loss/{}", hex(&fact_id));
        if create_alert(
            conn,
            &dedup,
            DATA_LOSS,
            "critical",
            None,
            &component_id,
            occurred_at,
            &message,
            policy.notify_data_loss,
        )? && policy.notify_data_loss
        {
            pending.push(PendingNotice {
                alert_type: DATA_LOSS.into(),
                title: title(DATA_LOSS).into(),
                name: component_id.clone(),
                mute_key: format!("bf::tdl-{component_id}"),
            });
        }
    }
    for (node, loss) in inferred {
        // 15 分钟一档：同一轮重启补出的多条空洞共用一个去重键。
        let bucket = loss.occurred_at.div_euclid(15 * 60 * 1_000_000_000);
        let dedup = format!("data-loss-node/{}/{}", hex(&node), bucket);
        let name = host_name(conn, &node)?;
        let message = format!("[{}] {name}", title(DATA_LOSS));
        if create_alert(
            conn,
            &dedup,
            DATA_LOSS,
            "critical",
            Some(&node),
            &loss.component_id,
            loss.occurred_at,
            &message,
            policy.notify_data_loss,
        )? && policy.notify_data_loss
        {
            pending.push(PendingNotice {
                alert_type: DATA_LOSS.into(),
                title: title(DATA_LOSS).into(),
                name,
                mute_key: format!("bf::tdl-node-{}", hex(&node)),
            });
        }
    }
    Ok(())
}

struct InferredLoss {
    component_id: String,
    occurred_at: i64,
}

/// 接收端自己补的压缩空洞：`fact_id` 就是 `telemetry_gaps.gap_id`，
/// detail 以 `receiver inferred` 开头。从端上报的丢失事实不进这张缺口表。
fn inferred_loss_node(conn: &Connection, fact_id: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
    conn.query_row(
        "SELECT g.node_uuid FROM telemetry_gaps g \
         JOIN telemetry_data_losses d ON d.fact_id = g.gap_id \
         WHERE g.gap_id = ?1 AND d.detail LIKE 'receiver inferred %'",
        [fact_id],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

fn scan_event_data_loss(
    conn: &Connection,
    policy: &AlertPolicy,
    pending: &mut Vec<PendingNotice>,
) -> anyhow::Result<()> {
    let mut stmt = conn.prepare(
        "SELECT event_id, node_uuid, collected_at, payload FROM telemetry_events \
         WHERE event_type = ?1 ORDER BY collected_at DESC LIMIT 1000",
    )?;
    let rows = stmt.query_map([proto_data_loss_type()], |row| {
        Ok((
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<Vec<u8>>>(3)?,
        ))
    })?;
    let mut events = Vec::new();
    for row in rows {
        events.push(row?);
    }
    drop(stmt);
    for (event_id, node_uuid, collected_at, payload) in events {
        let Some(payload) = payload else {
            continue;
        };
        let Ok(event) = crate::proto::TelemetryEvent::decode(payload.as_slice()) else {
            continue;
        };
        if !matches!(
            event.payload,
            Some(crate::proto::telemetry_event::Payload::DataLoss(_))
        ) {
            continue;
        }
        let name = host_name(conn, &node_uuid)?;
        let message = format!("[{}] {name}", title(DATA_LOSS));
        let dedup = format!("event-data-loss/{}", hex(&event_id));
        if create_alert(
            conn,
            &dedup,
            DATA_LOSS,
            "critical",
            Some(&node_uuid),
            "agent_wal",
            collected_at,
            &message,
            policy.notify_data_loss,
        )? && policy.notify_data_loss
        {
            pending.push(PendingNotice {
                alert_type: DATA_LOSS.into(),
                title: title(DATA_LOSS).into(),
                name,
                mute_key: format!("bf::tdl-agent_wal-{}", hex(&node_uuid)),
            });
        }
    }
    Ok(())
}

fn proto_data_loss_type() -> i32 {
    crate::proto::TelemetryEventType::DataLoss as i32
}

#[allow(clippy::too_many_arguments)]
fn create_alert(
    conn: &Connection,
    dedup_key: &str,
    alert_type: &str,
    severity: &str,
    node_uuid: Option<&[u8]>,
    component_id: &str,
    occurred_at: i64,
    message: &str,
    notified: bool,
) -> anyhow::Result<bool> {
    let inserted = conn.execute(
        "INSERT INTO telemetry_alerts \
         (dedup_key, alert_type, severity, node_uuid, component_id, occurred_at, message, notified, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, CURRENT_TIMESTAMP) \
         ON CONFLICT(dedup_key) DO NOTHING",
        params![
            dedup_key,
            alert_type,
            severity,
            node_uuid,
            component_id,
            occurred_at,
            message,
            i64::from(notified),
        ],
    )?;
    Ok(inserted > 0)
}

fn latest_alert(
    conn: &Connection,
    alert_type: &str,
    component_id: &str,
) -> anyhow::Result<Option<(i64, String)>> {
    conn.query_row(
        "SELECT occurred_at, dedup_key FROM telemetry_alerts \
         WHERE alert_type = ?1 AND component_id = ?2 ORDER BY occurred_at DESC LIMIT 1",
        params![alert_type, component_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .map_err(Into::into)
}

fn host_name(conn: &Connection, node_uuid: &[u8]) -> anyhow::Result<String> {
    let name: Option<String> = conn
        .query_row(
            "SELECT s.name FROM server_node_bindings b \
             JOIN servers s ON s.id = b.server_id \
             WHERE b.node_uuid = ?1 AND b.current != 0 AND s.deleted_at IS NULL \
             ORDER BY b.id DESC LIMIT 1",
            [node_uuid],
            |row| row.get(0),
        )
        .optional()?;
    Ok(display_name(name.as_deref().unwrap_or(""), &hex(node_uuid)))
}

fn display_name(name: &str, fallback: &str) -> String {
    let name = name.trim();
    if !name.is_empty() {
        return name.to_string();
    }
    if fallback.is_empty() {
        return "未知主机".to_string();
    }
    fallback.chars().take(8).collect()
}

fn title(kind: &str) -> &'static str {
    match kind {
        HOST_OFFLINE => "离线",
        CONNECTIVITY_DEGRADED => "连通降级",
        COLLECTOR_OFFLINE => "从端离线",
        COLLECTOR_ONLINE => "从端上线",
        DATA_LOSS => "探测丢失",
        _ => "告警",
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn episode_from_dedup(dedup: &str) -> i64 {
    dedup
        .rsplit_once('/')
        .and_then(|(_, tail)| tail.parse().ok())
        .unwrap_or(0)
}

/// episode 锚点：沿 `bucket` 链一路回溯到链头事件。
///
/// **必须与 1h 扫描窗无关**。扫描窗只决定「哪些事件值得发告警」，不决定
/// 「它们属于哪个 episode」。若锚点按窗口内的集合分组算，窗口每滑过一次，
/// 链头事件（`started_at` 早于 `now - 1h` 且已结束）就掉出集合，锚点随之
/// 变成晚一步的事件 → 去重键更换 → 同一 episode 反复告警、反复通知。
///
/// 回溯不带时间下限，因此链头多老都能找到；`started_at` 严格递减保证终止。
/// 步数上限只为兜住病态库（持续抖动会让链极长），截断后取当时最老的一环。
fn episode_anchor(conn: &Connection, incident: &Incident, bucket: i64) -> anyhow::Result<i64> {
    let bucket = if bucket <= 0 { 30_000_000_000 } else { bucket };
    let mut anchor = incident.started_at;
    let mut cursor = Incident {
        id: incident.id,
        node_uuid: incident.node_uuid.clone(),
        initial: incident.initial.clone(),
        current: incident.current.clone(),
        revision: incident.revision,
        started_at: incident.started_at,
        ended_at: incident.ended_at,
    };
    for _ in 0..EPISODE_WALK_LIMIT {
        let previous: Option<Incident> = conn
            .query_row(
                "SELECT id, node_uuid, initial_classification, current_classification, revision, \
                      started_at, COALESCE(ended_at, 0) \
                 FROM availability_incidents \
                 WHERE node_uuid = ?1 AND current_classification = ?2 \
                   AND (started_at < ?3 OR (started_at = ?3 AND id < ?4)) \
                 ORDER BY started_at DESC, id DESC LIMIT 1",
                params![
                    cursor.node_uuid,
                    cursor.current,
                    cursor.started_at,
                    cursor.id
                ],
                |row| {
                    Ok(Incident {
                        id: row.get(0)?,
                        node_uuid: row.get(1)?,
                        initial: row.get(2)?,
                        current: row.get(3)?,
                        revision: row.get(4)?,
                        started_at: row.get(5)?,
                        ended_at: row.get(6)?,
                    })
                },
            )
            .optional()?;
        let Some(previous) = previous else { break };
        if !same_episode(&previous, &cursor, bucket) {
            break;
        }
        anchor = previous.started_at;
        cursor = previous;
    }
    Ok(anchor)
}

/// 单次回溯的步数上限：兜住病态库，不影响正常链（每环 ≤ bucket）。
const EPISODE_WALK_LIMIT: usize = 512;

fn same_episode(prev: &Incident, curr: &Incident, bucket: i64) -> bool {
    if prev.node_uuid != curr.node_uuid || prev.current != curr.current {
        return false;
    }
    if prev.ended_at == 0 || prev.ended_at == curr.started_at {
        return true;
    }
    if curr.started_at >= prev.started_at && curr.started_at - prev.started_at <= bucket {
        return true;
    }
    prev.ended_at > 0 && curr.started_at <= prev.ended_at + bucket
}

pub async fn run(
    db: crate::db::Db,
    conf: crate::config::ConfigHandle,
    notifier: std::sync::Arc<crate::notify::Notifier>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(15)) => {
                let policy = AlertPolicy::from_config(&conf.read().expect("config lock"));
                match db.read(|conn| scan(conn, &policy, crate::util::time::now_nanos())) {
                    Ok(notices) => {
                        for (message, mute) in format_burst(&notices) {
                            notifier.send("default", &message, Some(mute), None).await;
                        }
                    }
                    Err(err) => tracing::warn!("telemetry alerts: {err:#}"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;

    fn temp_db() -> crate::db::Db {
        let dir = std::env::temp_dir().join(format!(
            "santaizi-tel-alerts-{}-{}",
            std::process::id(),
            crate::util::time::now_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        migrate::open_primary(dir.join("panel.db").to_str().unwrap()).unwrap()
    }

    pub(super) fn policy(notify: bool, suppress: bool) -> AlertPolicy {
        AlertPolicy {
            notify_host_offline: notify,
            notify_connectivity: false,
            notify_correction: false,
            notify_collector_offline: notify,
            notify_collector_online: false,
            notify_data_loss: false,
            suppress_host_offline_notify: suppress,
            collector_timeout_nanos: 90_000_000_000,
            availability_bucket_nanos: 30_000_000_000,
        }
    }

    #[test]
    fn host_offline_is_suppressed_when_history_and_notification_are_both_on() {
        let db = temp_db();
        let now = 1_700_000_000_000_000_000i64;
        db.write(|tx| {
            tx.execute(
                "INSERT INTO availability_incidents \
                 (node_uuid, initial_classification, current_classification, revision, started_at, ended_at) \
                 VALUES (?1, 'HOST_OFFLINE', 'HOST_OFFLINE', 1, ?2, 0)",
                params![vec![0x11u8; 16], now],
            )?;
            Ok(())
        })
        .unwrap();
        let notices = db
            .read(|conn| scan(conn, &policy(true, true), now))
            .unwrap();
        assert!(notices.is_empty());
        let notified: i64 = db
            .read(|conn| {
                conn.query_row(
                    "SELECT notified FROM telemetry_alerts WHERE alert_type = 'HOST_OFFLINE'",
                    [],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(notified, 0);
        let again = db
            .read(|conn| scan(conn, &policy(true, false), now))
            .unwrap();
        assert!(
            again.is_empty(),
            "dedup keeps the suppressed row from notifying later"
        );
    }

    #[test]
    fn collector_offline_notifies_once() {
        let db = temp_db();
        let now = 1_700_000_000_000_000_000i64;
        db.write(|tx| {
            tx.execute(
                "INSERT INTO collectors (collector_uuid, name, address, token_hash, token_ciphertext, kind, tcp_ports, revoked, deleted, created_at) \
                 VALUES ('edge', 'edge', '10.0.0.8:5556', X'00', X'00', 'observer', '', 0, 0, '2020-01-01 00:00:00')",
                [],
            )?;
            tx.execute(
                "INSERT INTO collector_runtimes (collector_uuid, status, last_seen) VALUES ('edge', 'offline', ?1)",
                [now - 120_000_000_000i64],
            )?;
            Ok(())
        })
        .unwrap();
        let first = db
            .read(|conn| scan(conn, &policy(true, false), now))
            .unwrap();
        assert_eq!(first.len(), 1);
        assert!(first[0].mute_key.contains("bf::tco-edge"));
        let second = db
            .read(|conn| scan(conn, &policy(true, false), now))
            .unwrap();
        assert!(second.is_empty());
    }

    /// 重启探针会让主端按会话补空洞，component_id 全是接收端 `primary`。
    /// 同一节点同一轮只发一条，名字用绑定的主机名，不能刷出 N 行 primary。
    #[test]
    fn inferred_holes_collapse_to_one_notice_per_host() {
        let db = temp_db();
        let now = 1_700_000_000_000_000_000i64;
        let node = vec![0x33u8; 16];
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, secret_ciphertext) VALUES (8, '东京-1', X'00')",
                [],
            )?;
            tx.execute(
                "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason, created_at) \
                 VALUES (8, ?1, 0, NULL, 1, 'enroll', CURRENT_TIMESTAMP)",
                [&node],
            )?;
            for (index, session) in [vec![0x01u8; 16], vec![0x02u8; 16], vec![0x03u8; 16]]
                .into_iter()
                .enumerate()
            {
                let fact = vec![0xA0 + index as u8; 16];
                tx.execute(
                    "INSERT INTO telemetry_gaps (gap_id, node_uuid, session_id, start_sequence, end_sequence, reason, created_at_unix_nano) \
                     VALUES (?1, ?2, ?3, 2, 4, 2, ?4)",
                    params![fact, node, session, now],
                )?;
                tx.execute(
                    "INSERT INTO telemetry_data_losses (fact_id, component_id, occurred_at, reason, first_spool_id, last_spool_id, lost_records, detail) \
                     VALUES (?1, 'primary', ?2, 2, 2, 4, 3, ?3)",
                    params![
                        fact,
                        now + index as i64,
                        format!("receiver inferred compacted hole 2-4 session {index}")
                    ],
                )?;
            }
            // 从端自己报的丢失仍按组件名逐条发，不并进主机。
            tx.execute(
                "INSERT INTO telemetry_data_losses (fact_id, component_id, occurred_at, reason, lost_records, detail) \
                 VALUES (?1, 'collector-east', ?2, 1, 2, 'spool overflow')",
                params![vec![0xB0u8; 16], now],
            )?;
            Ok(())
        })
        .unwrap();
        let mut policy = policy(true, false);
        policy.notify_data_loss = true;
        let notices = db.read(|conn| scan(conn, &policy, now)).unwrap();
        let mut names: Vec<&str> = notices.iter().map(|notice| notice.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["collector-east", "东京-1"]);
        assert!(
            notices.iter().all(|notice| notice.name != "primary"),
            "不得把接收端 ID 当成丢失对象：{names:?}"
        );
        let again = db
            .read(|conn| scan(conn, &policy, now + 60_000_000_000))
            .unwrap();
        assert!(again.is_empty(), "同一轮不得再发：{again:?}");
    }
}

#[cfg(test)]
mod episode_anchor_tests {
    use super::tests::policy;
    use super::*;
    use crate::db::migrate;

    const NODE: [u8; 16] = [0x11; 16];
    const OTHER: [u8; 16] = [0x22; 16];
    const BUCKET: i64 = 30_000_000_000;

    fn db(tag: &str) -> crate::db::Db {
        let dir = std::env::temp_dir().join(format!(
            "santaizi-episode-anchor-{tag}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        migrate::open_primary(dir.join("panel.db").to_str().unwrap()).unwrap()
    }

    fn incident(
        db: &crate::db::Db,
        node: &[u8],
        classification: &str,
        started_at: i64,
        ended_at: i64,
    ) {
        db.write(|tx| {
            tx.execute(
                "INSERT INTO availability_incidents \
                 (node_uuid, initial_classification, current_classification, revision, started_at, ended_at) \
                 VALUES (?1, ?2, ?2, 1, ?3, ?4)",
                params![node, classification, started_at, ended_at],
            )?;
            Ok(())
        })
        .unwrap();
    }

    fn load(db: &crate::db::Db, node: &[u8], started_at: i64) -> Incident {
        db.read(|conn| {
            Ok(conn.query_row(
                "SELECT id, node_uuid, initial_classification, current_classification, revision, \
                        started_at, COALESCE(ended_at, 0) \
                 FROM availability_incidents WHERE node_uuid = ?1 AND started_at = ?2",
                params![node, started_at],
                |row| {
                    Ok(Incident {
                        id: row.get(0)?,
                        node_uuid: row.get(1)?,
                        initial: row.get(2)?,
                        current: row.get(3)?,
                        revision: row.get(4)?,
                        started_at: row.get(5)?,
                        ended_at: row.get(6)?,
                    })
                },
            )?)
        })
        .unwrap()
    }

    fn anchor(db: &crate::db::Db, incident: &Incident) -> i64 {
        db.read(|conn| episode_anchor(conn, incident, BUCKET))
            .unwrap()
    }

    /// 核心回归：链头事件已结束且早于 1h 扫描窗，锚点仍须是链头。
    /// 旧实现按窗口内集合分组，链头一掉出窗口锚点就后移 → 换 key → 重复告警。
    #[test]
    fn anchor_ignores_the_one_hour_scan_window() {
        let db = db("window");
        let head = 1_000_000_000_000i64;
        incident(&db, &NODE, HOST_OFFLINE, head, head + 10_000_000_000);
        // 与链头相隔 20s（≤ bucket）⇒ 同一 episode；且远早于 now - 1h。
        let tail = head + 20_000_000_000;
        incident(&db, &NODE, HOST_OFFLINE, tail, 0);

        // now 取 1h + 60s 后：`now - 1h = head + 60s`，晚于 tail(head+20s)。
        let now = head + 3_600_000_000_000 + 60_000_000_000;
        assert!(
            head < now - 3_600_000_000_000,
            "构造前提：链头必须已在 1h 扫描窗外"
        );
        // 晚到事件靠 ended_at = 0 留在窗内（它的 started_at 也早已过窗）。
        assert_eq!(
            anchor(&db, &load(&db, &NODE, tail)),
            head,
            "锚点必须锚在链头，与扫描窗无关"
        );
        // 顺带确认扫描窗确实把链头排除在外（否则测试空转）。
        let in_window = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM availability_incidents \
                     WHERE current_classification = 'HOST_OFFLINE' \
                       AND (COALESCE(ended_at, 0) = 0 OR started_at >= ?1)",
                    [now - 3_600_000_000_000],
                    |r| r.get::<_, i64>(0),
                )?)
            })
            .unwrap();
        assert_eq!(in_window, 1, "扫描窗内应只剩晚到事件");
    }

    /// 超过 bucket 的间隔 ⇒ 断开，各自成 episode。
    #[test]
    fn anchor_breaks_when_gap_exceeds_bucket() {
        let db = db("gap");
        let first = 1_000_000_000_000i64;
        incident(&db, &NODE, HOST_OFFLINE, first, first + 1_000_000_000);
        // 间隔须超过 `ended_at + bucket`（1s + 30s = 31s），取 32s。
        let second = first + 32_000_000_000;
        incident(&db, &NODE, HOST_OFFLINE, second, 0);
        assert_eq!(anchor(&db, &load(&db, &NODE, second)), second);
    }

    /// 多环链条：锚点一路回到最老的一环。
    #[test]
    fn anchor_walks_back_through_a_long_chain() {
        let db = db("chain");
        let mut at = 1_000_000_000_000i64;
        let head = at;
        for _ in 0..5 {
            incident(&db, &NODE, HOST_OFFLINE, at, at + 1_000_000_000);
            at += BUCKET;
        }
        let last = at - BUCKET;
        assert_eq!(anchor(&db, &load(&db, &NODE, last)), head);
    }

    /// 别的节点 / 别的分类不得串环。
    #[test]
    fn anchor_is_scoped_to_node_and_classification() {
        let db = db("scope");
        let start = 1_000_000_000_000i64;
        incident(&db, &NODE, HOST_OFFLINE, start, start + 1_000_000_000);
        incident(&db, &OTHER, HOST_OFFLINE, start + 1, 0);
        incident(&db, &NODE, CONNECTIVITY_DEGRADED, start + 2, 0);
        assert_eq!(anchor(&db, &load(&db, &NODE, start)), start);
        assert_eq!(anchor(&db, &load(&db, &OTHER, start + 1)), start + 1);
        assert_eq!(
            anchor(&db, &load(&db, &NODE, start + 2)),
            start + 2,
            "同节点不同分类不得并入同一 episode"
        );
    }

    /// 端到端：同一 episode 在窗口滑动后不得产生第二条告警。
    #[test]
    fn sliding_window_does_not_duplicate_the_episode_alert() {
        let db = db("dup");
        let head = 1_000_000_000_000i64;
        incident(&db, &NODE, HOST_OFFLINE, head, head + 10_000_000_000);
        let tail = head + 20_000_000_000;
        incident(&db, &NODE, HOST_OFFLINE, tail, 0);

        let early = head + 30_000_000_000;
        let late = head + 3_600_000_000_000 + 30_000_000_000;
        // 窗口滑动前：episode 首次告警，一条通知。
        let first = db
            .read(|conn| scan(conn, &policy(true, false), early))
            .unwrap();
        assert_eq!(first.len(), 1, "episode 首次应告警一次");

        // 窗口滑过链头之后：不得再为同一 episode 发第二条。
        let second = db
            .read(|conn| scan(conn, &policy(true, false), late))
            .unwrap();
        assert!(
            second.is_empty(),
            "同一 episode 不得因扫描窗滑动重复告警：{second:?}"
        );
        let keys = db
            .read(|conn| {
                let mut stmt =
                    conn.prepare("SELECT dedup_key FROM telemetry_alerts ORDER BY id")?;
                let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
                Ok(rows.collect::<Result<Vec<_>, _>>()?)
            })
            .unwrap();
        assert_eq!(keys.len(), 1, "去重键必须稳定：{keys:?}");
        assert!(
            keys[0].ends_with(&format!("/{head}")),
            "锚点应是链头：{keys:?}"
        );
    }
}
