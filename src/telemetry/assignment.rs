//! 端点指派：对齐 Go `singleton.EndpointAssignmentForNode` 与
//! `CurrentTelemetryConfigVersion`。
//!
//! 探针连上 Control 后按指派决定往哪些观测点上报：primary 恒在，
//! 观测型从端按开放 observer_assignment 下发；探测型从端不进指派。

use rusqlite::{OptionalExtension, Transaction};

use crate::config::Config;
use crate::proto;
use crate::repo::bindings::PRIMARY_OBSERVER_ID;
use crate::repo::collectors;

/// `CurrentTelemetryConfigVersion`：collectors 的 MAX(config_version)，下限 1。
pub fn current_config_version(tx: &Transaction<'_>) -> u64 {
    tx.query_row(
        "SELECT COALESCE(MAX(config_version), 0) FROM collectors",
        [],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .ok()
    .flatten()
    .filter(|version| *version > 0)
    .map(|version| version as u64)
    .unwrap_or(1)
}

/// `EndpointAssignmentForNode`：primary + 该节点的开放观测型从端指派。
pub fn endpoint_assignment_for_node(
    tx: &Transaction<'_>,
    conf: &Config,
    node_uuid: &[u8],
    session_id: &[u8],
    activation_sequence: u64,
) -> anyhow::Result<proto::EndpointAssignment> {
    if node_uuid.len() != 16 || session_id.len() != 16 {
        anyhow::bail!("node UUID and session ID must be 16 bytes");
    }
    let activation = if activation_sequence == 0 {
        1
    } else {
        activation_sequence
    };
    let version = current_config_version(tx);
    let primary_address = if conf.telemetry.primary_endpoint.is_empty() {
        format!("{}:{}", conf.grpc_host, conf.grpc_port)
    } else {
        conf.telemetry.primary_endpoint.clone()
    };
    let mut endpoints = vec![proto::TelemetryEndpoint {
        endpoint_id: PRIMARY_OBSERVER_ID.to_string(),
        kind: proto::EndpointKind::Primary as i32,
        address: primary_address,
        reliable: true,
        tls: conf.grpc_tls.enabled,
        insecure_tls: false,
        generation: 1,
        activation_session_id: session_id.to_vec(),
        activation_sequence: 1,
    }];

    let mut stmt = tx.prepare(
        "SELECT observer_id, generation FROM observer_assignments \
         WHERE node_uuid = ? AND observer_id != ? AND valid_to = 0",
    )?;
    let observers: Vec<(String, u64)> = stmt
        .query_map(rusqlite::params![node_uuid, PRIMARY_OBSERVER_ID], |row| {
            Ok((row.get(0)?, row.get::<_, i64>(1)?.max(1) as u64))
        })?
        .collect::<Result<_, _>>()?;
    drop(stmt);
    for (observer_id, _) in observers {
        let collector = collectors::find_by_uuid_tx(tx, &observer_id)?;
        let Some(collector) = collector else { continue };
        if collector.is_probe() {
            continue;
        }
        endpoints.push(proto::TelemetryEndpoint {
            endpoint_id: collector.collector_uuid,
            kind: proto::EndpointKind::Collector as i32,
            address: collector.address,
            reliable: true,
            tls: collector.tls,
            insecure_tls: collector.insecure_tls,
            generation: collector.generation,
            activation_session_id: session_id.to_vec(),
            activation_sequence: activation,
        });
    }
    Ok(proto::EndpointAssignment {
        config_version: version,
        endpoints,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;

    #[test]
    fn assignment_includes_primary_and_open_observer() {
        let dir = std::env::temp_dir().join(format!("santaizi-assign-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("panel.db").to_string_lossy().into_owned();
        let db = migrate::open_primary(&path).unwrap();
        let node = [9u8; 16];
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) \
                 VALUES (1, 'n', 't', X'00', 1, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO collectors (collector_uuid, name, address, generation, kind, tcp_ports, token_hash, token_ciphertext, revoked, deleted) \
                 VALUES ('obs-1', 'Obs', 'obs:5556', 3, 'observer', '', X'00', X'00', 0, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO collectors (collector_uuid, name, address, generation, kind, tcp_ports, token_hash, token_ciphertext, revoked, deleted) \
                 VALUES ('probe-1', 'Probe', 'probe:5556', 3, 'probe', '', X'00', X'00', 0, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO observer_assignments (node_uuid, observer_id, valid_from, valid_to, config_version, generation) \
                 VALUES (?, 'obs-1', 1, 0, 1, 3)",
                [&node],
            )?;
            tx.execute(
                "INSERT INTO observer_assignments (node_uuid, observer_id, valid_from, valid_to, config_version, generation) \
                 VALUES (?, 'probe-1', 1, 0, 1, 3)",
                [&node],
            )?;
            Ok(())
        })
        .unwrap();
        let conf = Config::default();
        let assignment = db
            .read(|conn| {
                // read 路径只有 &Connection；这里借用 write 事务语义由调用方保证，测试直接读。
                let tx = conn.unchecked_transaction()?;
                let out = endpoint_assignment_for_node(&tx, &conf, &node, &[9u8; 16], 5)?;
                tx.commit()?;
                Ok(out)
            })
            .unwrap();
        assert_eq!(assignment.endpoints.len(), 2);
        assert_eq!(assignment.endpoints[0].endpoint_id, "primary");
        assert_eq!(assignment.endpoints[1].endpoint_id, "obs-1");
        assert_eq!(assignment.endpoints[1].activation_sequence, 5);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
