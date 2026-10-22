//! schema DDL。全部幂等（IF NOT EXISTS），一次建全量终态，
//! 版本号记入 `schema_migrations` / `collector_schema_migrations`。

/// 主库终态 schema（对齐 Go v20，去掉 bot_*、ddns、nats 与 servers 的 DDNS 列）。
pub const PRIMARY_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    applied_at datetime NOT NULL
);

CREATE TABLE IF NOT EXISTS servers (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at             datetime,
    updated_at             datetime,
    deleted_at             datetime,
    name                   TEXT,
    tag                    TEXT,
    secret_ciphertext      BLOB NOT NULL,
    note                   TEXT,
    public_note            TEXT,
    display_index          INTEGER,
    hide_for_guest         INTEGER,
    monitoring_options_raw TEXT DEFAULT '{}',
    probe_target           TEXT,
    probe_tcp_ports        TEXT,
    probe_enable_icmp      INTEGER DEFAULT 1,
    probe_enable_tcp       INTEGER DEFAULT 1,
    probe_enable_mtr       INTEGER DEFAULT 1
);
CREATE UNIQUE INDEX IF NOT EXISTS uni_servers_secret_ciphertext ON servers (secret_ciphertext);
CREATE INDEX IF NOT EXISTS idx_servers_created_at ON servers (created_at);
CREATE INDEX IF NOT EXISTS idx_servers_deleted_at ON servers (deleted_at);

CREATE TABLE IF NOT EXISTS server_runtimes (
    server_id            INTEGER PRIMARY KEY,
    status               TEXT,
    first_seen_at        datetime,
    last_seen_at         datetime,
    last_online_at       datetime,
    last_offline_at      datetime,
    last_boot_time       INTEGER,
    last_uptime          INTEGER,
    last_ip              TEXT,
    last_agent_version   TEXT,
    current_offline_id   INTEGER,
    current_node_uuid    BLOB,
    current_session_id   BLOB,
    current_sequence     INTEGER,
    protocol             TEXT,
    host_state           TEXT,
    connectivity_state  TEXT,
    last_collected_at    INTEGER,
    last_received_at     INTEGER,
    state_payload        BLOB,
    host_payload         BLOB,
    created_at           datetime,
    updated_at           datetime
);
CREATE INDEX IF NOT EXISTS idx_server_runtimes_status ON server_runtimes (status);
CREATE INDEX IF NOT EXISTS idx_server_runtimes_current_offline_id ON server_runtimes (current_offline_id);
CREATE INDEX IF NOT EXISTS idx_server_runtimes_current_node_uuid ON server_runtimes (current_node_uuid);
CREATE INDEX IF NOT EXISTS idx_server_runtimes_protocol ON server_runtimes (protocol);
CREATE INDEX IF NOT EXISTS idx_server_runtimes_host_state ON server_runtimes (host_state);
CREATE INDEX IF NOT EXISTS idx_server_runtimes_connectivity_state ON server_runtimes (connectivity_state);
CREATE INDEX IF NOT EXISTS idx_server_runtimes_last_collected_at ON server_runtimes (last_collected_at);
CREATE INDEX IF NOT EXISTS idx_server_runtimes_last_received_at ON server_runtimes (last_received_at);

CREATE TABLE IF NOT EXISTS server_offline_histories (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at         datetime,
    updated_at         datetime,
    deleted_at         datetime,
    server_id          INTEGER,
    started_at         datetime,
    detected_at        datetime,
    ended_at           datetime,
    duration_seconds   INTEGER,
    reason             TEXT,
    status             TEXT,
    threshold_seconds  INTEGER,
    last_seen_at       datetime,
    last_boot_time     INTEGER,
    last_uptime        INTEGER,
    last_ip            TEXT,
    recovered_at       datetime,
    recovered_boot_time INTEGER,
    recovered_uptime   INTEGER,
    recovered_ip       TEXT,
    note               TEXT
);
CREATE INDEX IF NOT EXISTS idx_server_offline_histories_created_at ON server_offline_histories (created_at);
CREATE INDEX IF NOT EXISTS idx_server_offline_histories_deleted_at ON server_offline_histories (deleted_at);
CREATE INDEX IF NOT EXISTS idx_server_offline_histories_server_id ON server_offline_histories (server_id);
CREATE INDEX IF NOT EXISTS idx_server_offline_histories_started_at ON server_offline_histories (started_at);
CREATE INDEX IF NOT EXISTS idx_server_offline_histories_detected_at ON server_offline_histories (detected_at);
CREATE INDEX IF NOT EXISTS idx_server_offline_histories_ended_at ON server_offline_histories (ended_at);
CREATE INDEX IF NOT EXISTS idx_server_offline_histories_reason ON server_offline_histories (reason);
CREATE INDEX IF NOT EXISTS idx_server_offline_histories_status ON server_offline_histories (status);

CREATE TABLE IF NOT EXISTS server_node_bindings (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    server_id  INTEGER NOT NULL,
    node_uuid  BLOB NOT NULL,
    valid_from INTEGER NOT NULL,
    valid_to   INTEGER,
    current    INTEGER NOT NULL,
    reason     TEXT NOT NULL,
    created_at datetime
);
CREATE INDEX IF NOT EXISTS idx_server_node_binding ON server_node_bindings (server_id, node_uuid);
CREATE INDEX IF NOT EXISTS idx_node_binding ON server_node_bindings (node_uuid);
CREATE INDEX IF NOT EXISTS idx_server_node_bindings_valid_from ON server_node_bindings (valid_from);
CREATE INDEX IF NOT EXISTS idx_server_node_bindings_valid_to ON server_node_bindings (valid_to);
CREATE INDEX IF NOT EXISTS idx_server_node_bindings_current ON server_node_bindings (current);

CREATE TABLE IF NOT EXISTS transfers (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at datetime,
    updated_at datetime,
    deleted_at datetime,
    server_id  INTEGER,
    "in"       INTEGER,
    "out"      INTEGER
);
CREATE INDEX IF NOT EXISTS idx_transfers_created_at ON transfers (created_at);
CREATE INDEX IF NOT EXISTS idx_transfers_deleted_at ON transfers (deleted_at);

CREATE TABLE IF NOT EXISTS monitors (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at             datetime,
    updated_at             datetime,
    deleted_at             datetime,
    name                   TEXT,
    type                   INTEGER,
    target                 TEXT,
    skip_servers_raw       TEXT,
    duration               INTEGER,
    notify                 INTEGER,
    notification_tag       TEXT,
    cover                  INTEGER,
    enable_show_in_service INTEGER DEFAULT 0,
    min_latency            REAL,
    max_latency            REAL,
    latency_notify         INTEGER
);
CREATE INDEX IF NOT EXISTS idx_monitors_created_at ON monitors (created_at);
CREATE INDEX IF NOT EXISTS idx_monitors_deleted_at ON monitors (deleted_at);

CREATE TABLE IF NOT EXISTS monitor_histories (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at datetime,
    updated_at datetime,
    deleted_at datetime,
    monitor_id INTEGER,
    server_id  INTEGER,
    avg_delay  REAL,
    up         INTEGER,
    down       INTEGER,
    data       TEXT
);
CREATE INDEX IF NOT EXISTS idx_monitor_histories_created_at ON monitor_histories (created_at);
CREATE INDEX IF NOT EXISTS idx_monitor_histories_deleted_at ON monitor_histories (deleted_at);
CREATE INDEX IF NOT EXISTS idx_server_id_created_at_monitor_id_avg_delay ON monitor_histories (created_at, monitor_id, server_id, avg_delay);
-- 监控历史按 monitor_id / server_id 过滤 + created_at 排序：既有三个索引前导列
-- 都不是这两个字段，列表查询只能全表扫。T-0273 补；存量库经 migrate 的幂等
-- DDL 重跑获得，故不 bump schema version。
CREATE INDEX IF NOT EXISTS idx_monitor_histories_monitor_created ON monitor_histories (monitor_id, created_at);
CREATE INDEX IF NOT EXISTS idx_monitor_histories_server_created ON monitor_histories (server_id, created_at);

CREATE TABLE IF NOT EXISTS notifications (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at     datetime,
    updated_at     datetime,
    deleted_at     datetime,
    name           TEXT,
    tag            TEXT,
    url            TEXT,
    request_method INTEGER,
    request_type   INTEGER,
    request_header TEXT,
    request_body   TEXT,
    verify_ssl     INTEGER
);
CREATE INDEX IF NOT EXISTS idx_notifications_created_at ON notifications (created_at);
CREATE INDEX IF NOT EXISTS idx_notifications_deleted_at ON notifications (deleted_at);

CREATE TABLE IF NOT EXISTS alert_rules (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at       datetime,
    updated_at       datetime,
    deleted_at       datetime,
    name             TEXT,
    rules_raw        TEXT,
    enable           INTEGER,
    trigger_mode     INTEGER DEFAULT 0,
    notification_tag TEXT
);
CREATE INDEX IF NOT EXISTS idx_alert_rules_created_at ON alert_rules (created_at);
CREATE INDEX IF NOT EXISTS idx_alert_rules_deleted_at ON alert_rules (deleted_at);

CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at    datetime,
    updated_at    datetime,
    deleted_at    datetime,
    login         TEXT,
    avatar_url    TEXT,
    name          TEXT,
    blog          TEXT,
    email         TEXT,
    hireable      INTEGER,
    bio           TEXT,
    token         TEXT,
    token_expired datetime,
    super_admin   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_users_created_at ON users (created_at);
CREATE INDEX IF NOT EXISTS idx_users_deleted_at ON users (deleted_at);

CREATE TABLE IF NOT EXISTS api_tokens (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at       datetime,
    updated_at       datetime,
    deleted_at       datetime,
    user_id          INTEGER,
    token_ciphertext BLOB NOT NULL,
    token_hash       BLOB NOT NULL,
    note             TEXT,
    permission       TEXT NOT NULL DEFAULT 'write',
    expires_at       datetime,
    enabled          INTEGER NOT NULL DEFAULT 1
);
CREATE UNIQUE INDEX IF NOT EXISTS uni_api_tokens_token_hash ON api_tokens (token_hash);
CREATE INDEX IF NOT EXISTS idx_api_tokens_created_at ON api_tokens (created_at);
CREATE INDEX IF NOT EXISTS idx_api_tokens_deleted_at ON api_tokens (deleted_at);

CREATE TABLE IF NOT EXISTS traffic_policies (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at       datetime,
    updated_at       datetime,
    deleted_at       datetime,
    server_id        INTEGER NOT NULL,
    name             TEXT NOT NULL,
    direction        TEXT NOT NULL,
    mode             TEXT NOT NULL,
    cycle_start      datetime,
    cycle_interval   INTEGER NOT NULL DEFAULT 1,
    cycle_unit       TEXT,
    quota_bytes      INTEGER NOT NULL,
    warning_percent  REAL NOT NULL DEFAULT 80,
    notification_tag TEXT,
    enabled          INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_traffic_policy_server ON traffic_policies (server_id);
CREATE INDEX IF NOT EXISTS idx_traffic_policies_direction ON traffic_policies (direction);
CREATE INDEX IF NOT EXISTS idx_traffic_policies_mode ON traffic_policies (mode);
CREATE INDEX IF NOT EXISTS idx_traffic_policies_cycle_start ON traffic_policies (cycle_start);
CREATE INDEX IF NOT EXISTS idx_traffic_policies_enabled ON traffic_policies (enabled);
CREATE INDEX IF NOT EXISTS idx_traffic_policies_created_at ON traffic_policies (created_at);
CREATE INDEX IF NOT EXISTS idx_traffic_policies_deleted_at ON traffic_policies (deleted_at);

CREATE TABLE IF NOT EXISTS traffic_policy_states (
    policy_id            INTEGER PRIMARY KEY,
    window_start         INTEGER NOT NULL,
    window_end           INTEGER NOT NULL,
    used_bytes           INTEGER NOT NULL,
    warning_notified_at  INTEGER,
    exceeded_notified_at INTEGER,
    last_evaluated_at    INTEGER NOT NULL,
    updated_at           datetime
);
CREATE INDEX IF NOT EXISTS idx_traffic_policy_states_window_start ON traffic_policy_states (window_start);
CREATE INDEX IF NOT EXISTS idx_traffic_policy_states_window_end ON traffic_policy_states (window_end);
CREATE INDEX IF NOT EXISTS idx_traffic_policy_states_last_evaluated_at ON traffic_policy_states (last_evaluated_at);

CREATE TABLE IF NOT EXISTS telemetry_events (
    event_id             BLOB PRIMARY KEY,
    node_uuid            BLOB NOT NULL,
    session_id           BLOB NOT NULL,
    sequence             INTEGER NOT NULL,
    event_type           INTEGER NOT NULL,
    priority             INTEGER NOT NULL,
    collected_at         INTEGER NOT NULL,
    agent_uptime_nano    INTEGER,
    session_elapsed_nano INTEGER,
    clock_untrusted      INTEGER NOT NULL,
    source_protocol      INTEGER NOT NULL,
    reliability          INTEGER NOT NULL,
    payload              BLOB,
    payload_retained     INTEGER NOT NULL DEFAULT 1,
    created_at           datetime
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_node_session_sequence ON telemetry_events (node_uuid, session_id, sequence);
CREATE INDEX IF NOT EXISTS idx_node_collected ON telemetry_events (node_uuid, collected_at);
CREATE INDEX IF NOT EXISTS idx_event_type_collected ON telemetry_events (event_type, collected_at);
CREATE INDEX IF NOT EXISTS idx_telemetry_events_priority ON telemetry_events (priority);
CREATE INDEX IF NOT EXISTS idx_telemetry_events_clock_untrusted ON telemetry_events (clock_untrusted);
CREATE INDEX IF NOT EXISTS idx_telemetry_events_source_protocol ON telemetry_events (source_protocol);
CREATE INDEX IF NOT EXISTS idx_telemetry_events_payload_retained ON telemetry_events (payload_retained);
CREATE INDEX IF NOT EXISTS idx_telemetry_events_created_at ON telemetry_events (created_at);

CREATE TABLE IF NOT EXISTS telemetry_observations (
    event_id       BLOB NOT NULL,
    observer_id    TEXT NOT NULL,
    node_uuid      BLOB NOT NULL,
    received_at    INTEGER NOT NULL,
    replicated_at  INTEGER,
    metadata       BLOB,
    created_at     datetime,
    PRIMARY KEY (event_id, observer_id)
);
CREATE INDEX IF NOT EXISTS idx_observer_received ON telemetry_observations (observer_id, received_at);
CREATE INDEX IF NOT EXISTS idx_observation_node_received ON telemetry_observations (node_uuid, received_at);
CREATE INDEX IF NOT EXISTS idx_telemetry_observations_replicated_at ON telemetry_observations (replicated_at);

CREATE TABLE IF NOT EXISTS telemetry_gaps (
    gap_id               BLOB PRIMARY KEY,
    node_uuid            BLOB NOT NULL,
    session_id           BLOB NOT NULL,
    start_sequence       INTEGER NOT NULL,
    end_sequence         INTEGER NOT NULL,
    reason               INTEGER NOT NULL,
    replacement_event_id BLOB,
    created_at_unix_nano INTEGER NOT NULL,
    created_at           datetime
);
CREATE INDEX IF NOT EXISTS idx_gap_session ON telemetry_gaps (node_uuid, session_id, start_sequence);
CREATE INDEX IF NOT EXISTS idx_telemetry_gaps_reason ON telemetry_gaps (reason);
CREATE INDEX IF NOT EXISTS idx_telemetry_gaps_created_at_unix_nano ON telemetry_gaps (created_at_unix_nano);

CREATE TABLE IF NOT EXISTS telemetry_ingest_cursors (
    receiver_id   TEXT NOT NULL,
    node_uuid     BLOB NOT NULL,
    session_id    BLOB NOT NULL,
    ack_through   INTEGER NOT NULL,
    hole_sequence INTEGER,
    hole_since    INTEGER,
    updated_at    datetime,
    PRIMARY KEY (receiver_id, node_uuid, session_id)
);

-- HEARTBEAT / STATE / STATE_ROLLUP 不落 telemetry_events（见 persist_event_type），
-- 但它们的 sequence 同样占位。游标推进只查 telemetry_events 时会把这些序列
-- 当成空洞，宽限一到就误报「探测丢失」。这里给不落表的事件类型留一份序号回执，
-- 只保存 ack_through 之上的行（推进成功后即清理），表体积天然有界。
CREATE TABLE IF NOT EXISTS telemetry_sequence_receipts (
    receiver_id TEXT NOT NULL,
    node_uuid   BLOB NOT NULL,
    session_id  BLOB NOT NULL,
    sequence    INTEGER NOT NULL,
    PRIMARY KEY (receiver_id, node_uuid, session_id, sequence)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS agent_telemetry_runtimes (
    node_uuid        BLOB PRIMARY KEY,
    wal_pressure     INTEGER NOT NULL,
    wal_bytes        INTEGER,
    pending_events   INTEGER,
    oldest_pending   INTEGER,
    sink_cursors     BLOB,
    clock_untrusted  INTEGER,
    protocol_version TEXT,
    agent_version    TEXT,
    updated_at       datetime
);

CREATE TABLE IF NOT EXISTS state_rollups (
    node_uuid     BLOB NOT NULL,
    resolution    TEXT NOT NULL,
    window_start  INTEGER NOT NULL,
    window_end    INTEGER NOT NULL,
    sample_count  INTEGER NOT NULL,
    payload       BLOB NOT NULL,
    net_in_total  INTEGER,
    net_out_total INTEGER,
    created_at    datetime,
    PRIMARY KEY (node_uuid, resolution, window_start)
);
CREATE INDEX IF NOT EXISTS idx_state_rollups_window_end ON state_rollups (window_end);

CREATE TABLE IF NOT EXISTS telemetry_data_losses (
    fact_id       BLOB PRIMARY KEY,
    component_id  TEXT NOT NULL,
    occurred_at   INTEGER NOT NULL,
    reason        INTEGER NOT NULL,
    first_spool_id INTEGER,
    last_spool_id  INTEGER,
    lost_records  INTEGER,
    detail        TEXT,
    created_at    datetime
);
CREATE INDEX IF NOT EXISTS idx_telemetry_data_losses_component_id ON telemetry_data_losses (component_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_data_losses_occurred_at ON telemetry_data_losses (occurred_at);
CREATE INDEX IF NOT EXISTS idx_telemetry_data_losses_reason ON telemetry_data_losses (reason);

CREATE TABLE IF NOT EXISTS telemetry_alerts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    dedup_key    TEXT NOT NULL,
    alert_type   TEXT NOT NULL,
    severity     TEXT NOT NULL,
    node_uuid    BLOB,
    component_id TEXT,
    occurred_at  INTEGER NOT NULL,
    message      TEXT NOT NULL,
    notified     INTEGER NOT NULL,
    created_at   datetime
);
CREATE UNIQUE INDEX IF NOT EXISTS uni_telemetry_alerts_dedup_key ON telemetry_alerts (dedup_key);
CREATE INDEX IF NOT EXISTS idx_telemetry_alerts_alert_type ON telemetry_alerts (alert_type);
CREATE INDEX IF NOT EXISTS idx_telemetry_alerts_severity ON telemetry_alerts (severity);
CREATE INDEX IF NOT EXISTS idx_telemetry_alerts_node_uuid ON telemetry_alerts (node_uuid);
CREATE INDEX IF NOT EXISTS idx_telemetry_alerts_component_id ON telemetry_alerts (component_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_alerts_occurred_at ON telemetry_alerts (occurred_at);

CREATE TABLE IF NOT EXISTS collector_replication_receipts (
    collector_uuid          TEXT NOT NULL,
    replication_session     BLOB NOT NULL,
    batch_sequence          INTEGER NOT NULL,
    committed_spool_through INTEGER NOT NULL,
    created_at              datetime,
    PRIMARY KEY (collector_uuid, replication_session, batch_sequence)
);

CREATE TABLE IF NOT EXISTS availability_buckets (
    node_uuid           BLOB NOT NULL,
    bucket_start        INTEGER NOT NULL,
    window_end          INTEGER NOT NULL DEFAULT 0,
    resolution          TEXT NOT NULL DEFAULT '30s',
    host_state          TEXT NOT NULL,
    connectivity_state  TEXT NOT NULL,
    expected_observers  INTEGER NOT NULL,
    healthy_observers   INTEGER NOT NULL,
    seen_observers      INTEGER NOT NULL,
    observer_summary    BLOB,
    revision            INTEGER NOT NULL DEFAULT 1,
    finalized           INTEGER NOT NULL,
    recalculated_at     INTEGER,
    created_at          datetime,
    updated_at          datetime,
    PRIMARY KEY (node_uuid, bucket_start)
);
CREATE INDEX IF NOT EXISTS idx_availability_node_window ON availability_buckets (node_uuid, window_end, bucket_start);
CREATE INDEX IF NOT EXISTS idx_availability_buckets_resolution ON availability_buckets (resolution);
CREATE INDEX IF NOT EXISTS idx_availability_buckets_host_state ON availability_buckets (host_state);
CREATE INDEX IF NOT EXISTS idx_availability_buckets_connectivity_state ON availability_buckets (connectivity_state);
CREATE INDEX IF NOT EXISTS idx_availability_buckets_finalized ON availability_buckets (finalized);

CREATE TABLE IF NOT EXISTS availability_incidents (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    node_uuid             BLOB NOT NULL,
    initial_classification TEXT NOT NULL,
    current_classification TEXT NOT NULL,
    revision              INTEGER NOT NULL DEFAULT 1,
    started_at            INTEGER NOT NULL,
    ended_at              INTEGER,
    recalculated_at       INTEGER,
    reason                TEXT,
    observer_evidence     BLOB,
    created_at            datetime,
    updated_at            datetime
);
CREATE INDEX IF NOT EXISTS idx_incident_node_time ON availability_incidents (node_uuid, started_at);
CREATE INDEX IF NOT EXISTS idx_availability_incidents_initial_classification ON availability_incidents (initial_classification);
CREATE INDEX IF NOT EXISTS idx_availability_incidents_current_classification ON availability_incidents (current_classification);
CREATE INDEX IF NOT EXISTS idx_availability_incidents_ended_at ON availability_incidents (ended_at);

CREATE TABLE IF NOT EXISTS incident_revisions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id    INTEGER NOT NULL,
    revision       INTEGER NOT NULL,
    classification TEXT NOT NULL,
    reason         TEXT,
    evidence       BLOB,
    recalculated_at INTEGER NOT NULL,
    created_at     datetime
);
CREATE INDEX IF NOT EXISTS idx_incident_revisions_incident_id ON incident_revisions (incident_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_incident_revision ON incident_revisions (incident_id, revision);
CREATE INDEX IF NOT EXISTS idx_incident_revisions_recalculated_at ON incident_revisions (recalculated_at);

CREATE TABLE IF NOT EXISTS availability_recompute_queues (
    node_uuid    BLOB NOT NULL,
    bucket_start INTEGER NOT NULL,
    reason       TEXT NOT NULL,
    created_at   datetime,
    updated_at   datetime,
    PRIMARY KEY (node_uuid, bucket_start)
);

CREATE TABLE IF NOT EXISTS observer_assignments (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    node_uuid     BLOB NOT NULL,
    observer_id   TEXT NOT NULL,
    valid_from    INTEGER NOT NULL,
    valid_to      INTEGER,
    config_version INTEGER NOT NULL,
    generation    INTEGER NOT NULL,
    created_at    datetime
);
CREATE INDEX IF NOT EXISTS idx_assignment_range ON observer_assignments (node_uuid, observer_id, valid_from, valid_to);

CREATE TABLE IF NOT EXISTS observer_health_buckets (
    observer_id     TEXT NOT NULL,
    bucket_start    INTEGER NOT NULL,
    healthy         INTEGER NOT NULL,
    process_session TEXT,
    revision        INTEGER NOT NULL DEFAULT 1,
    updated_at      datetime,
    PRIMARY KEY (observer_id, bucket_start)
);
CREATE INDEX IF NOT EXISTS idx_observer_health_buckets_healthy ON observer_health_buckets (healthy);

CREATE TABLE IF NOT EXISTS observer_path_buckets (
    node_uuid    BLOB NOT NULL,
    observer_id  TEXT NOT NULL,
    bucket_start INTEGER NOT NULL,
    seen         INTEGER NOT NULL,
    last_seen_at INTEGER,
    revision     INTEGER NOT NULL DEFAULT 1,
    updated_at   datetime,
    PRIMARY KEY (node_uuid, observer_id, bucket_start)
);

CREATE TABLE IF NOT EXISTS collectors (
    collector_uuid      TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    address             TEXT NOT NULL,
    listen_port         INTEGER NOT NULL DEFAULT 0,
    token_hash          BLOB NOT NULL,
    token_ciphertext    BLOB NOT NULL,
    generation          INTEGER NOT NULL DEFAULT 1,
    config_version      INTEGER NOT NULL DEFAULT 1,
    tls                 INTEGER,
    insecure_tls        INTEGER,
    location            TEXT,
    kind                TEXT NOT NULL DEFAULT 'observer',
    probe_interval_sec  INTEGER NOT NULL DEFAULT 30,
    mtr_interval_sec    INTEGER NOT NULL DEFAULT 300,
    mtr_probes          INTEGER NOT NULL DEFAULT 10,
    tcp_ports           TEXT NOT NULL,
    enable_icmp         INTEGER NOT NULL DEFAULT 1,
    enable_tcp          INTEGER NOT NULL DEFAULT 1,
    enable_mtr          INTEGER NOT NULL DEFAULT 1,
    enable_ipv4         INTEGER NOT NULL DEFAULT 1,
    enable_ipv6         INTEGER NOT NULL DEFAULT 1,
    probe_notify        INTEGER,
    notification_tag    TEXT,
    latency_notify      INTEGER,
    min_latency_ms      REAL,
    max_latency_ms      REAL,
    fail_threshold      INTEGER NOT NULL DEFAULT 3,
    route_interval_sec  INTEGER NOT NULL DEFAULT 86400,
    route_keep          INTEGER NOT NULL DEFAULT 10,
    revoked             INTEGER NOT NULL,
    deleted             INTEGER NOT NULL,
    created_at          datetime,
    updated_at          datetime
);
CREATE INDEX IF NOT EXISTS idx_collectors_kind ON collectors (kind);
CREATE INDEX IF NOT EXISTS idx_collectors_revoked ON collectors (revoked);
CREATE INDEX IF NOT EXISTS idx_collectors_deleted ON collectors (deleted);

CREATE TABLE IF NOT EXISTS collector_scopes (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    collector_uuid TEXT NOT NULL,
    scope_type     TEXT NOT NULL,
    scope_value    TEXT NOT NULL,
    created_at     datetime
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_collector_scope ON collector_scopes (collector_uuid, scope_type, scope_value);

CREATE TABLE IF NOT EXISTS collector_runtimes (
    collector_uuid            TEXT PRIMARY KEY,
    status                    TEXT NOT NULL,
    last_seen                 INTEGER,
    last_sync                 INTEGER,
    last_primary_seen         INTEGER,
    spool_size                INTEGER,
    pending_records           INTEGER,
    oldest_pending            INTEGER,
    replication_cursor        INTEGER,
    connected_agents          INTEGER,
    protocol_version          TEXT,
    software_version          TEXT,
    heartbeat_rtt_ms          REAL,
    heartbeat_rtt_sampled_at  INTEGER,
    replication_rtt_ms        REAL,
    replication_rtt_sampled_at INTEGER,
    updated_at                datetime
);
CREATE INDEX IF NOT EXISTS idx_collector_runtimes_status ON collector_runtimes (status);
CREATE INDEX IF NOT EXISTS idx_collector_runtimes_last_seen ON collector_runtimes (last_seen);
CREATE INDEX IF NOT EXISTS idx_collector_runtimes_last_sync ON collector_runtimes (last_sync);
CREATE INDEX IF NOT EXISTS idx_collector_runtimes_last_primary_seen ON collector_runtimes (last_primary_seen);

CREATE TABLE IF NOT EXISTS connection_latency_buckets (
    kind           TEXT NOT NULL,
    collector_uuid TEXT NOT NULL,
    node_uuid      BLOB NOT NULL,
    observer_id    TEXT NOT NULL,
    bucket_start   INTEGER NOT NULL,
    min_ms         REAL,
    max_ms         REAL,
    sum_ms         REAL,
    count          INTEGER NOT NULL,
    updated_at     datetime,
    PRIMARY KEY (kind, collector_uuid, node_uuid, observer_id, bucket_start)
);

CREATE TABLE IF NOT EXISTS connection_latency_cursors (
    kind            TEXT NOT NULL,
    collector_uuid  TEXT NOT NULL,
    node_uuid       BLOB NOT NULL,
    observer_id     TEXT NOT NULL,
    last_sampled_at INTEGER NOT NULL,
    updated_at      datetime,
    PRIMARY KEY (kind, collector_uuid, node_uuid, observer_id)
);

CREATE TABLE IF NOT EXISTS probe_sample_buckets (
    collector_uuid TEXT NOT NULL,
    server_id      INTEGER NOT NULL,
    kind           TEXT NOT NULL,
    port           INTEGER NOT NULL,
    bucket_start   INTEGER NOT NULL,
    min_ms         REAL,
    max_ms         REAL,
    sum_ms         REAL,
    count          INTEGER NOT NULL,
    loss_sum       REAL,
    success_count  INTEGER,
    fail_count     INTEGER,
    updated_at     datetime,
    PRIMARY KEY (collector_uuid, server_id, kind, port, bucket_start)
);

CREATE TABLE IF NOT EXISTS probe_latests (
    collector_uuid TEXT NOT NULL,
    server_id      INTEGER NOT NULL,
    sampled_at     INTEGER,
    reachable      INTEGER,
    display_rtt_ms REAL,
    last_error     TEXT,
    icmp_ok        INTEGER,
    icmp_rtt_ms    REAL,
    icmp_loss      REAL,
    icmp_sent      INTEGER,
    icmp_recv      INTEGER,
    tcp_json       BLOB,
    has_trace      INTEGER,
    updated_at     datetime,
    PRIMARY KEY (collector_uuid, server_id)
);

CREATE TABLE IF NOT EXISTS probe_traces (
    collector_uuid  TEXT NOT NULL,
    server_id       INTEGER NOT NULL,
    sampled_at      INTEGER,
    destination     TEXT,
    hops_json       BLOB,
    tcp_sampled_at  INTEGER,
    tcp_destination TEXT,
    tcp_hops_json   BLOB,
    tcp_port        INTEGER,
    updated_at      datetime,
    PRIMARY KEY (collector_uuid, server_id)
);

CREATE TABLE IF NOT EXISTS probe_routes (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    collector_uuid TEXT NOT NULL,
    server_id      INTEGER NOT NULL,
    protocol       TEXT NOT NULL,
    sampled_at     INTEGER NOT NULL,
    destination    TEXT,
    port           INTEGER,
    hops_json      BLOB,
    error          TEXT,
    job_id         INTEGER,
    updated_at     datetime
);
CREATE INDEX IF NOT EXISTS idx_probe_route_hist ON probe_routes (collector_uuid, server_id, protocol, sampled_at);

CREATE TABLE IF NOT EXISTS probe_route_jobs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    collector_uuid TEXT NOT NULL,
    server_id      INTEGER NOT NULL,
    protocol       TEXT NOT NULL,
    port           INTEGER,
    status         TEXT NOT NULL,
    requested_at   INTEGER NOT NULL,
    finished_at    INTEGER,
    error          TEXT,
    updated_at     datetime
);
CREATE INDEX IF NOT EXISTS idx_probe_route_job_pending ON probe_route_jobs (collector_uuid, server_id, protocol);
CREATE INDEX IF NOT EXISTS idx_probe_route_jobs_status ON probe_route_jobs (status);

CREATE TABLE IF NOT EXISTS probe_alert_states (
    collector_uuid    TEXT NOT NULL,
    server_id         INTEGER NOT NULL,
    consecutive_fails INTEGER,
    down_notified     INTEGER,
    latency_alert     INTEGER,
    updated_at        datetime,
    PRIMARY KEY (collector_uuid, server_id)
);
"#;

/// 从端库终态 schema（对齐 Go collector store v7）。
pub const COLLECTOR_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS collector_schema_migrations (
    version    INTEGER PRIMARY KEY,
    applied_at datetime
);

CREATE TABLE IF NOT EXISTS collector_metas (
    key        TEXT PRIMARY KEY,
    value      BLOB NOT NULL,
    updated_at datetime
);

CREATE TABLE IF NOT EXISTS collector_stored_events (
    event_id     BLOB PRIMARY KEY,
    node_uuid    BLOB NOT NULL,
    session_id   BLOB NOT NULL,
    sequence     INTEGER NOT NULL,
    event_type   INTEGER NOT NULL,
    priority     INTEGER NOT NULL,
    collected_at INTEGER NOT NULL,
    payload      BLOB NOT NULL,
    created_at   datetime
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_collector_node_session_sequence ON collector_stored_events (node_uuid, session_id, sequence);
CREATE INDEX IF NOT EXISTS idx_collector_event_collected ON collector_stored_events (node_uuid, collected_at);
CREATE INDEX IF NOT EXISTS idx_collector_stored_events_event_type ON collector_stored_events (event_type);

CREATE TABLE IF NOT EXISTS collector_stored_observations (
    event_id      BLOB NOT NULL,
    observer_id   TEXT NOT NULL,
    node_uuid     BLOB NOT NULL,
    received_at   INTEGER NOT NULL,
    replicated_at INTEGER,
    metadata      BLOB,
    created_at    datetime,
    PRIMARY KEY (event_id, observer_id)
);
CREATE INDEX IF NOT EXISTS idx_collector_observation_received ON collector_stored_observations (observer_id, received_at);
CREATE INDEX IF NOT EXISTS idx_collector_stored_observations_node_uuid ON collector_stored_observations (node_uuid);

CREATE TABLE IF NOT EXISTS collector_stored_gaps (
    gap_id              BLOB PRIMARY KEY,
    node_uuid           BLOB NOT NULL,
    session_id          BLOB NOT NULL,
    start_sequence      INTEGER NOT NULL,
    end_sequence        INTEGER NOT NULL,
    reason              INTEGER NOT NULL,
    payload             BLOB NOT NULL,
    created_at_unix_nano INTEGER NOT NULL,
    created_at          datetime
);
CREATE INDEX IF NOT EXISTS idx_collector_gap_session ON collector_stored_gaps (node_uuid, session_id, start_sequence);
CREATE INDEX IF NOT EXISTS idx_collector_stored_gaps_created_at_unix_nano ON collector_stored_gaps (created_at_unix_nano);

CREATE TABLE IF NOT EXISTS collector_agent_cursors (
    node_uuid   BLOB NOT NULL,
    session_id  BLOB NOT NULL,
    ack_through INTEGER NOT NULL,
    updated_at  datetime,
    PRIMARY KEY (node_uuid, session_id)
);

CREATE TABLE IF NOT EXISTS collector_outboxes (
    spool_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    record_type TEXT NOT NULL,
    payload     BLOB NOT NULL,
    created_at  datetime
);
CREATE INDEX IF NOT EXISTS idx_collector_outboxes_record_type ON collector_outboxes (record_type);

CREATE TABLE IF NOT EXISTS collector_replication_cursors (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    committed_spool_through_id INTEGER NOT NULL,
    next_batch_sequence       INTEGER NOT NULL,
    updated_at                datetime
);

CREATE TABLE IF NOT EXISTS collector_authorization_caches (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    collector_uuid          TEXT NOT NULL,
    primary_public_key      BLOB NOT NULL,
    key_id                  BLOB NOT NULL,
    config_version          INTEGER NOT NULL,
    last_primary_seen_nano  INTEGER NOT NULL,
    agent_ca_certificate_pem TEXT,
    kind                    TEXT,
    probe_config_json       BLOB,
    updated_at              datetime
);

CREATE TABLE IF NOT EXISTS collector_cached_probe_targets (
    server_id          INTEGER PRIMARY KEY,
    server_name        TEXT,
    ipv4               TEXT,
    ipv6               TEXT,
    hostname           TEXT,
    tcp_ports          TEXT,
    enable_icmp        INTEGER,
    enable_tcp         INTEGER,
    enable_mtr         INTEGER,
    enable_ipv4        INTEGER,
    enable_ipv6        INTEGER,
    interval_sec       INTEGER,
    mtr_interval_sec   INTEGER,
    mtr_probes         INTEGER,
    route_interval_sec INTEGER,
    updated_at         datetime
);

CREATE TABLE IF NOT EXISTS collector_cached_assignments (
    node_uuid      BLOB NOT NULL,
    observer_id    TEXT NOT NULL,
    valid_from     INTEGER NOT NULL,
    valid_to       INTEGER,
    generation     INTEGER NOT NULL,
    config_version INTEGER NOT NULL,
    updated_at     datetime,
    PRIMARY KEY (node_uuid, observer_id, valid_from)
);
CREATE INDEX IF NOT EXISTS idx_collector_cached_assignments_valid_to ON collector_cached_assignments (valid_to);
CREATE INDEX IF NOT EXISTS idx_collector_cached_assignments_config_version ON collector_cached_assignments (config_version);

CREATE TABLE IF NOT EXISTS collector_cached_revocations (
    node_uuid      BLOB PRIMARY KEY,
    config_version INTEGER NOT NULL,
    updated_at     datetime
);
CREATE INDEX IF NOT EXISTS idx_collector_cached_revocations_config_version ON collector_cached_revocations (config_version);

CREATE TABLE IF NOT EXISTS collector_health_evidences (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    observer_id     TEXT NOT NULL,
    sampled_at      INTEGER NOT NULL,
    healthy         INTEGER NOT NULL,
    process_session TEXT NOT NULL,
    replicated      INTEGER NOT NULL,
    created_at      datetime
);
CREATE INDEX IF NOT EXISTS idx_collector_health_time ON collector_health_evidences (observer_id, sampled_at);
CREATE INDEX IF NOT EXISTS idx_collector_health_evidences_replicated ON collector_health_evidences (replicated);

CREATE TABLE IF NOT EXISTS collector_data_losses (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    fact_id       BLOB NOT NULL,
    occurred_at   INTEGER NOT NULL,
    reason        TEXT NOT NULL,
    first_spool_id INTEGER,
    last_spool_id  INTEGER,
    lost_records  INTEGER,
    detail        TEXT,
    replicated    INTEGER NOT NULL,
    created_at    datetime
);
CREATE UNIQUE INDEX IF NOT EXISTS uni_collector_data_losses_fact_id ON collector_data_losses (fact_id);
CREATE INDEX IF NOT EXISTS idx_collector_data_losses_occurred_at ON collector_data_losses (occurred_at);
CREATE INDEX IF NOT EXISTS idx_collector_data_losses_replicated ON collector_data_losses (replicated);
"#;
