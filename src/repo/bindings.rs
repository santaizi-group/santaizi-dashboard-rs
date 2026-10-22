//! 服务器节点绑定与观察者指派，对齐 Go `service/singleton/telemetry_runtime.go`。
//!
//! 绑定是 P0 生命周期事实：换绑只终结未来指派，历史观测与可用性证据不可变。

use rand::Rng;
use rusqlite::{Connection, OptionalExtension, Transaction};

use crate::repo::collectors;

pub const PRIMARY_OBSERVER_ID: &str = "primary";
const V2_BINDING_REASON: &str = "authenticated_v2_control_binding";
const PLAIN_BINDING_REASON: &str = "authenticated_control_binding";

#[derive(Debug, thiserror::Error)]
pub enum BindingError {
    #[error("node UUID must be 16 bytes")]
    NodeUuidLength,
    #[error("server is already bound to another node")]
    BoundToOtherNode,
}

pub struct BindingRow {
    pub server_id: i64,
    pub node_uuid: Vec<u8>,
    pub reason: String,
}

pub fn current_binding_for_server(
    conn: &Connection,
    server_id: i64,
) -> anyhow::Result<Option<BindingRow>> {
    conn.query_row(
        "SELECT server_id, node_uuid, reason FROM server_node_bindings WHERE server_id = ? AND current = 1",
        [server_id],
        |row| {
            Ok(BindingRow {
                server_id: row.get(0)?,
                node_uuid: row.get(1)?,
                reason: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

pub fn server_id_from_node_uuid(
    conn: &Connection,
    node_uuid: &[u8],
) -> anyhow::Result<Option<i64>> {
    if node_uuid.len() != 16 {
        return Err(BindingError::NodeUuidLength.into());
    }
    conn.query_row(
        "SELECT server_id FROM server_node_bindings WHERE node_uuid = ? AND current = 1",
        [node_uuid],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

/// `EnsureServerNodeAvailableForEnroll`：服务器当前绑到别的节点时拒绝换绑。
pub fn ensure_server_node_available_for_enroll(
    conn: &Connection,
    server_id: i64,
    node_uuid: &[u8],
) -> anyhow::Result<()> {
    if node_uuid.len() != 16 {
        return Err(BindingError::NodeUuidLength.into());
    }
    match current_binding_for_server(conn, server_id)? {
        None => Ok(()),
        Some(binding) if binding.node_uuid == node_uuid => Ok(()),
        Some(_) => Err(BindingError::BoundToOtherNode.into()),
    }
}

/// `BindServerNodeForProtocol`：换绑时终结旧绑定与未来指派，写入新绑定与 primary 指派。
/// 返回 `changed`：已绑定同一节点时为 false（幂等）。
pub fn bind_server_node_for_protocol(
    tx: &Transaction<'_>,
    server_id: i64,
    node_uuid: &[u8],
    now_nanos: i64,
    v2_source: bool,
) -> anyhow::Result<bool> {
    if node_uuid.len() != 16 {
        return Err(BindingError::NodeUuidLength.into());
    }
    let reason = if v2_source {
        V2_BINDING_REASON
    } else {
        PLAIN_BINDING_REASON
    };
    let current = current_binding_for_server_tx(tx, server_id)?;
    if let Some(binding) = &current {
        if binding.node_uuid == node_uuid {
            return Ok(false);
        }
        let previous_node_uuid = binding.node_uuid.clone();
        tx.execute(
            "UPDATE server_node_bindings SET current = 0, valid_to = ? WHERE server_id = ? AND current = 1",
            [&now_nanos.to_string(), &server_id.to_string()],
        )?;
        close_observer_assignments(tx, &previous_node_uuid, now_nanos)?;
        close_open_incidents(tx, &previous_node_uuid, now_nanos)?;
        if binding.reason == V2_BINDING_REASON && v2_source {
            record_identity_change(tx, node_uuid, &previous_node_uuid, now_nanos)?;
        }
    }
    tx.execute(
        "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason, created_at) VALUES (?, ?, ?, NULL, 1, ?, CURRENT_TIMESTAMP)",
        rusqlite::params![server_id, node_uuid, now_nanos.to_string(), reason],
    )?;
    let config_version = current_telemetry_config_version(tx);
    tx.execute(
        "INSERT INTO observer_assignments (node_uuid, observer_id, valid_from, valid_to, config_version, generation, created_at) VALUES (?, ?, ?, 0, ?, 1, CURRENT_TIMESTAMP)",
        rusqlite::params![node_uuid, PRIMARY_OBSERVER_ID, now_nanos.to_string(), config_version.to_string()],
    )?;
    sync_collector_assignments_for_server(tx, server_id, node_uuid, now_nanos)?;
    Ok(true)
}

fn current_binding_for_server_tx(
    tx: &Transaction<'_>,
    server_id: i64,
) -> anyhow::Result<Option<BindingRow>> {
    tx.query_row(
        "SELECT server_id, node_uuid, reason FROM server_node_bindings WHERE server_id = ? AND current = 1",
        [server_id],
        |row| {
            Ok(BindingRow { server_id: row.get(0)?, node_uuid: row.get(1)?, reason: row.get(2)? })
        },
    )
    .optional()
    .map_err(Into::into)
}

/// `closeObserverAssignments`：终结某节点的开放指派，并推高受影响 collector 的配置版本。
fn close_observer_assignments(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    now_nanos: i64,
) -> anyhow::Result<()> {
    let mut stmt = tx.prepare(
        "SELECT DISTINCT observer_id FROM observer_assignments WHERE node_uuid = ? AND valid_to = 0",
    )?;
    let observers: Vec<String> = stmt
        .query_map([node_uuid], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    drop(stmt);
    tx.execute(
        "UPDATE observer_assignments SET valid_to = ? WHERE node_uuid = ? AND valid_to = 0",
        rusqlite::params![now_nanos.to_string(), node_uuid],
    )?;
    let mut seen = std::collections::HashSet::new();
    for observer_id in observers {
        if observer_id == PRIMARY_OBSERVER_ID || !seen.insert(observer_id.clone()) {
            continue;
        }
        match collectors::bump_config_version(tx, &observer_id) {
            Ok(_) => {}
            Err(e) if is_not_found(&e) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Go `RefreshObserverAssignmentsForServer`：按当前绑定与 collector scopes
/// 重同步某服务器的观测指派（管理端改 tag / 批量分组 / 重命名后调用）。
pub fn refresh_observer_assignments_for_server(
    tx: &Transaction<'_>,
    server_id: i64,
    now_nanos: i64,
) -> anyhow::Result<()> {
    let Some(binding) = current_binding_for_server_tx(tx, server_id)? else {
        return Ok(());
    };
    sync_collector_assignments_for_server(tx, server_id, &binding.node_uuid, now_nanos)
}

/// `CloseOpenIncidents`：把节点名下未结束的可用性事件关闭到 `ended_at`。
pub fn close_open_incidents(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    ended_at_nanos: i64,
) -> anyhow::Result<()> {
    let mut stmt = tx.prepare(
        "SELECT id, started_at FROM availability_incidents WHERE node_uuid = ? AND ended_at = 0",
    )?;
    let incidents: Vec<(i64, i64)> = stmt
        .query_map([node_uuid], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    drop(stmt);
    for (id, started_at) in incidents {
        let mut closed = ended_at_nanos;
        if closed < started_at {
            closed = started_at;
        }
        if closed == 0 {
            closed = 1;
        }
        let recalculated = if ended_at_nanos <= 0 {
            closed
        } else {
            ended_at_nanos
        };
        tx.execute(
            "UPDATE availability_incidents SET ended_at = ?, recalculated_at = ? WHERE id = ?",
            rusqlite::params![closed.to_string(), recalculated.to_string(), id],
        )?;
    }
    Ok(())
}

/// `syncCollectorAssignmentsForServer`：按服务器 group/tag 与 collector 作用域对齐开放指派。
fn sync_collector_assignments_for_server(
    tx: &Transaction<'_>,
    server_id: i64,
    node_uuid: &[u8],
    now_nanos: i64,
) -> anyhow::Result<()> {
    let server_tag: String = tx
        .query_row(
            "SELECT COALESCE(tag, '') FROM servers WHERE id = ?",
            [server_id],
            |row| row.get(0),
        )
        .optional()?
        .ok_or_else(|| anyhow::anyhow!("server {server_id} not found"))?;
    let server_id_text = server_id.to_string();
    let collectors = collectors::list_active_tx(tx)?;
    let mut desired: Vec<(String, u64)> = Vec::new();
    for collector in collectors {
        if collector.is_probe() {
            continue;
        }
        let mut stmt = tx.prepare(
            "SELECT scope_type, scope_value FROM collector_scopes WHERE collector_uuid = ?",
        )?;
        let scopes: Vec<(String, String)> = stmt
            .query_map([&collector.collector_uuid], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<Result<_, _>>()?;
        drop(stmt);
        if scopes_include_server(&scopes, &server_id_text, &server_tag) {
            desired.push((collector.collector_uuid, collector.generation));
        }
    }
    let mut stmt = tx.prepare(
        "SELECT observer_id FROM observer_assignments WHERE node_uuid = ? AND observer_id != ? AND valid_to = 0",
    )?;
    let active: Vec<String> = stmt
        .query_map(rusqlite::params![node_uuid, PRIMARY_OBSERVER_ID], |row| {
            row.get(0)
        })?
        .collect::<Result<_, _>>()?;
    drop(stmt);
    for observer_id in &active {
        if desired.iter().any(|(uuid, _)| uuid == observer_id) {
            continue;
        }
        tx.execute(
            "UPDATE observer_assignments SET valid_to = ? WHERE node_uuid = ? AND observer_id = ? AND valid_to = 0",
            rusqlite::params![now_nanos.to_string(), node_uuid, observer_id],
        )?;
        match collectors::bump_config_version(tx, observer_id) {
            Ok(_) => {}
            Err(e) if is_not_found(&e) => {}
            Err(e) => return Err(e),
        }
    }
    for (observer_id, generation) in &desired {
        if active.iter().any(|id| id == observer_id) {
            continue;
        }
        let version = collectors::bump_config_version(tx, observer_id)?;
        tx.execute(
            "INSERT INTO observer_assignments (node_uuid, observer_id, valid_from, valid_to, config_version, generation, created_at) VALUES (?, ?, ?, 0, ?, ?, CURRENT_TIMESTAMP)",
            rusqlite::params![node_uuid, observer_id, now_nanos.to_string(), version.to_string(), generation.to_string()],
        )?;
    }
    Ok(())
}

fn scopes_include_server(scopes: &[(String, String)], server_id: &str, server_tag: &str) -> bool {
    for (scope_type, scope_value) in scopes {
        match scope_type.as_str() {
            "all" => return true,
            "server" if scope_value == server_id => return true,
            "group" | "tag" if scope_value == server_tag => return true,
            _ => {}
        }
    }
    false
}

/// `currentTelemetryConfigVersion`：collector 配置版本的最大值，空表取 1。
pub fn current_telemetry_config_version(tx: &Transaction<'_>) -> u64 {
    tx.query_row(
        "SELECT COALESCE(MAX(config_version), 0) FROM collectors",
        [],
        |row| row.get(0),
    )
    .optional()
    .ok()
    .flatten()
    .map(|version: u64| if version == 0 { 1 } else { version })
    .unwrap_or(1)
}

/// `sendCollectorConfig` 的指派清单：某 observer 的全部指派。
/// 与 Go 一致不过滤 `valid_to`——历史区间对 collector 审计有效。
pub struct AssignmentRow {
    pub node_uuid: Vec<u8>,
    pub observer_id: String,
    pub valid_from: i64,
    pub valid_to: i64,
    pub config_version: i64,
    pub generation: i64,
}

pub fn assignments_for_observer(
    conn: &Connection,
    observer_id: &str,
) -> anyhow::Result<Vec<AssignmentRow>> {
    let mut stmt = conn.prepare(
        "SELECT node_uuid, observer_id, valid_from, COALESCE(valid_to, 0), config_version, generation
         FROM observer_assignments WHERE observer_id = ?",
    )?;
    let rows = stmt
        .query_map([observer_id], |row| {
            Ok(AssignmentRow {
                node_uuid: row.get(0)?,
                observer_id: row.get(1)?,
                valid_from: row.get(2)?,
                valid_to: row.get(3)?,
                config_version: row.get(4)?,
                generation: row.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// 取该观测点名下已终结（`valid_to` 已设）的节点 UUID，去重。
///
/// 从端 `IsNodeAuthorized` 先查吊销缓存再查指派：只靠「新配置里不再出现该指派」
/// 也能判越权，但那是隐式的。显式推送终结节点让吊销成为正向信号——
/// 尤其在指派被提前终结、而 `valid_from/valid_to` 尚未翻过的窗口里。
/// 此前这里恒返回空表，从端的 `collector_cached_revocations` 与
/// `is_node_authorized` 的吊销分支都是死代码。
pub fn revoked_node_uuids_for_observer(
    conn: &Connection,
    observer_id: &str,
) -> anyhow::Result<Vec<Vec<u8>>> {
    let mut stmt = conn.prepare(
        // `valid_to = 0` 是全库通用的「未终结」哨兵（见 assignments_for_observer
        // 的 `COALESCE(valid_to, 0)`），所以「已终结」是 `<> 0` 而不是 `IS NOT NULL`。
        "SELECT DISTINCT node_uuid FROM observer_assignments \
         WHERE observer_id = ? AND valid_to IS NOT NULL AND valid_to <> 0",
    )?;
    let rows = stmt.query_map([observer_id], |row| row.get::<_, Vec<u8>>(0))?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

/// 该观测点名下的**在效**节点 UUID 集合（去重）。
///
/// 归属校验用：从端凭一条 collector 凭据就能推任意节点的数据，
/// 等于把「谁能代表这个节点上报」降级成「谁有凭据」。
pub fn assigned_node_uuids(
    conn: &Connection,
    observer_id: &str,
) -> anyhow::Result<std::collections::HashSet<Vec<u8>>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT node_uuid FROM observer_assignments \
         WHERE observer_id = ? AND (valid_to = 0 OR valid_to = '')",
    )?;
    let rows = stmt.query_map([observer_id], |row| row.get::<_, Vec<u8>>(0))?;
    Ok(rows.collect::<Result<std::collections::HashSet<_>, _>>()?)
}

/// `recordIdentityChange`：V2 换绑留下 P0 生命周期事件，证明节点 UUID 变更。
fn record_identity_change(
    tx: &Transaction<'_>,
    node_uuid: &[u8],
    previous_node_uuid: &[u8],
    now_nanos: i64,
) -> anyhow::Result<()> {
    use prost::Message;
    use sha2::{Digest, Sha256};

    let mut session_id = [0u8; 16];
    rand::rng().fill_bytes(&mut session_id);
    let mut input = [0u8; 40];
    input[..16].copy_from_slice(node_uuid);
    input[16..32].copy_from_slice(&session_id);
    input[32..].copy_from_slice(&1u64.to_be_bytes());
    let event_id = Sha256::digest(input)[..16].to_vec();

    let event = crate::proto::TelemetryEvent {
        event_id: event_id.clone(),
        node_uuid: node_uuid.to_vec(),
        session_id: session_id.to_vec(),
        sequence: 1,
        event_type: 4, // TELEMETRY_EVENT_TYPE_LIFECYCLE
        priority: 1,   // TELEMETRY_PRIORITY_P0_CRITICAL
        collected_at_unix_nano: now_nanos,
        source_protocol: 1, // SOURCE_PROTOCOL_SANTAIZI_V2
        reliability: 2,     // RELIABILITY_BEST_EFFORT
        protocol_version: 2,
        payload: Some(crate::proto::telemetry_event::Payload::Lifecycle(
            crate::proto::LifecyclePayload {
                kind: 4, // LIFECYCLE_KIND_AGENT_IDENTITY_CHANGED
                reason: "authenticated node UUID changed".to_string(),
                previous_node_uuid: previous_node_uuid.to_vec(),
            },
        )),
        ..Default::default()
    };
    let payload = event.encode_to_vec();
    tx.execute(
        "INSERT INTO telemetry_events (event_id, node_uuid, session_id, sequence, event_type, priority, collected_at, clock_untrusted, source_protocol, reliability, payload, payload_retained, created_at) VALUES (?, ?, ?, 1, 4, 1, ?, 0, 1, 2, ?, 1, CURRENT_TIMESTAMP)",
        rusqlite::params![&event_id, node_uuid, &session_id.to_vec(), now_nanos.to_string(), &payload],
    )?;
    tx.execute(
        "INSERT INTO telemetry_observations (event_id, observer_id, node_uuid, received_at, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
        rusqlite::params![&event_id, PRIMARY_OBSERVER_ID, node_uuid, now_nanos.to_string()],
    )?;
    Ok(())
}

fn is_not_found(err: &anyhow::Error) -> bool {
    err.downcast_ref::<rusqlite::Error>()
        .map(|e| matches!(e, rusqlite::Error::QueryReturnedNoRows))
        .unwrap_or(false)
        || err.to_string().contains("not found")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;
    use crate::repo::collectors;

    fn temp_db(tag: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "santaizi-repo-bindings-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path.to_string_lossy().into_owned()
    }

    fn seed(db: &crate::db::Db) {
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) VALUES (1, 's1', 'tag1', X'00', 1, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO collectors (collector_uuid, name, address, token_hash, token_ciphertext, kind, tcp_ports, revoked, deleted) VALUES ('col1', 'c', 'addr', X'00', X'00', 'observer', '', 0, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO collector_scopes (collector_uuid, scope_type, scope_value) VALUES ('col1', 'all', '')",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn bind_creates_binding_and_primary_assignment() {
        let path = temp_db("bind");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db);
        let node = [1u8; 16];
        assert!(
            db.read(|conn| current_binding_for_server(conn, 1))
                .unwrap()
                .is_none()
        );
        let changed = db
            .write(|tx| bind_server_node_for_protocol(tx, 1, &node, 1_000, true))
            .unwrap();
        assert!(changed);

        let binding = db
            .read(|conn| current_binding_for_server(conn, 1))
            .unwrap()
            .unwrap();
        assert_eq!(binding.node_uuid, node.to_vec());
        assert_eq!(binding.reason, V2_BINDING_REASON);
        assert_eq!(
            db.read(|conn| server_id_from_node_uuid(conn, &node))
                .unwrap(),
            Some(1)
        );

        // 幂等：同一节点再绑定不变更
        let again = db
            .write(|tx| bind_server_node_for_protocol(tx, 1, &node, 2_000, true))
            .unwrap();
        assert!(!again);

        // collector 作用域 all → 生成指派并推高版本
        let assignments: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM observer_assignments WHERE node_uuid = ? AND observer_id = 'col1' AND valid_to = 0",
                    [&node.to_vec()],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(assignments, 1);
        let version = db
            .read(|conn| collectors::find_by_uuid(conn, "col1"))
            .unwrap()
            .unwrap()
            .config_version;
        assert_eq!(version, 2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rebind_closes_previous_assignments_and_incidents() {
        let path = temp_db("rebind");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db);
        let first = [1u8; 16];
        let second = [2u8; 16];
        db.write(|tx| {
            bind_server_node_for_protocol(tx, 1, &first, 1_000, true)?;
            tx.execute(
                "INSERT INTO availability_incidents (node_uuid, initial_classification, current_classification, revision, started_at, ended_at) VALUES (?, 'OFFLINE', 'OFFLINE', 1, 500, 0)",
                [&first.to_vec()],
            )?;
            Ok(())
        })
        .unwrap();

        db.write(|tx| bind_server_node_for_protocol(tx, 1, &second, 2_000, true))
            .unwrap();
        let binding = db
            .read(|conn| current_binding_for_server(conn, 1))
            .unwrap()
            .unwrap();
        assert_eq!(binding.node_uuid, second.to_vec());
        // 旧节点的开放指派与事件被关闭
        let open: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM observer_assignments WHERE node_uuid = ? AND valid_to = 0",
                    [&first.to_vec()],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(open, 0);
        let ended: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT ended_at FROM availability_incidents WHERE node_uuid = ?",
                    [&first.to_vec()],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(ended, 2_000);
        // V2→V2 换绑记录身份变更事件
        let events: i64 = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM telemetry_events WHERE event_type = 4 AND node_uuid = ?",
                    [&second.to_vec()],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(events, 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn ensure_available_rejects_other_node() {
        let path = temp_db("ensure");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db);
        let first = [1u8; 16];
        db.write(|tx| bind_server_node_for_protocol(tx, 1, &first, 1_000, true))
            .unwrap();
        assert!(
            db.read(|conn| ensure_server_node_available_for_enroll(conn, 1, &first))
                .is_ok()
        );
        let err = db
            .read(|conn| ensure_server_node_available_for_enroll(conn, 1, &[9u8; 16]))
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<BindingError>(),
            Some(BindingError::BoundToOtherNode)
        ));
        let _ = std::fs::remove_file(&path);
    }

    /// `revoked_node_uuids` 恒空 → 从端吊销缓存与 `IsNodeAuthorized` 吊销分支
    /// 全是死代码。终结绑定必须显式推给从端。
    #[test]
    fn terminated_bindings_are_reported_as_revoked() {
        let path = temp_db("revoked");
        let db = migrate::open_primary(&path).unwrap();
        seed(&db);
        let live_node = [2u8; 16];
        let terminated = [3u8; 16];
        db.write(|tx| {
            tx.execute(
                // secret_ciphertext 上有 UNIQUE 索引，不能与 server 1 复用同一个值。
                "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) VALUES (2, 's2', 'tag1', X'01', 1, 0)",
                [],
            )?;
            bind_server_node_for_protocol(tx, 1, &live_node, 1_000, true)?;
            bind_server_node_for_protocol(tx, 2, &terminated, 1_000, true)?;
            // 终结第二个绑定：与生产代码一致，valid_to 存 TEXT 纳秒串。
            tx.execute(
                "UPDATE observer_assignments SET valid_to = ?2 WHERE node_uuid = ?1 AND valid_to = 0",
                rusqlite::params![terminated.to_vec(), 2_000_i64.to_string()],
            )?;
            Ok(())
        })
        .unwrap();

        // 顺带验证：终结行的 valid_to 是 TEXT，assignments_for_observer 不得因此崩。
        let all = db
            .read(|conn| assignments_for_observer(conn, "col1"))
            .expect("assignments_for_observer 必须在 TEXT valid_to 下仍可用");
        assert_eq!(all.len(), 2, "两条指派都应返回");

        let revoked = db
            .read(|conn| revoked_node_uuids_for_observer(conn, "col1"))
            .unwrap();
        assert_eq!(revoked, vec![terminated.to_vec()], "只应报终结节点");
        // 换个观测点：不应看到别人的终结节点。
        let other = db
            .read(|conn| revoked_node_uuids_for_observer(conn, "col-other"))
            .unwrap();
        assert!(other.is_empty());

        // 在效指派集合只含未终结节点（归属校验的另一半）。
        let live = db.read(|conn| assigned_node_uuids(conn, "col1")).unwrap();
        assert_eq!(live.len(), 1, "只应含在效节点：{live:?}");
        assert!(live.contains(live_node.as_slice()), "在效节点应命中");
        assert!(
            !live.contains(terminated.as_slice()),
            "已终结节点不得仍在在效集合里"
        );
        assert!(
            db.read(|conn| assigned_node_uuids(conn, "col-other"))
                .unwrap()
                .is_empty(),
            "别的观测点看不到这些节点"
        );
        let _ = std::fs::remove_file(&path);
    }
}
