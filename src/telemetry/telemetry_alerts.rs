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

struct Incident {
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
        "SELECT node_uuid, initial_classification, current_classification, revision, started_at, \
         COALESCE(ended_at, 0) FROM availability_incidents \
         WHERE current_classification IN ('HOST_OFFLINE', 'CONNECTIVITY_DEGRADED') \
           AND (COALESCE(ended_at, 0) = 0 OR started_at >= ?1)",
    )?;
    let rows = stmt.query_map([cutoff], |row| {
        Ok(Incident {
            node_uuid: row.get(0)?,
            initial: row.get(1)?,
            current: row.get(2)?,
            revision: row.get(3)?,
            started_at: row.get(4)?,
            ended_at: row.get(5)?,
        })
    })?;
    let mut incidents = Vec::new();
    for row in rows {
        incidents.push(row?);
    }
    drop(stmt);
    let episodes = episode_starts(&incidents, policy.availability_bucket_nanos);
    for (index, incident) in incidents.iter().enumerate() {
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
            episodes[index]
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
    for (fact_id, component_id, occurred_at) in losses {
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
    Ok(())
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

fn episode_starts(incidents: &[Incident], bucket: i64) -> Vec<i64> {
    let bucket = if bucket <= 0 { 30_000_000_000 } else { bucket };
    let mut order: Vec<usize> = (0..incidents.len()).collect();
    order.sort_by(|&left, &right| {
        let a = &incidents[left];
        let b = &incidents[right];
        a.node_uuid
            .cmp(&b.node_uuid)
            .then_with(|| a.current.cmp(&b.current))
            .then_with(|| a.started_at.cmp(&b.started_at))
    });
    let mut starts = vec![0; incidents.len()];
    let mut prev: Option<usize> = None;
    let mut prev_start = 0i64;
    for index in order {
        let incident = &incidents[index];
        if let Some(previous) = prev
            && same_episode(&incidents[previous], incident, bucket)
        {
            starts[index] = prev_start;
        } else {
            starts[index] = incident.started_at;
            prev_start = incident.started_at;
        }
        prev = Some(index);
    }
    starts
}

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

    fn policy(notify: bool, suppress: bool) -> AlertPolicy {
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
}
