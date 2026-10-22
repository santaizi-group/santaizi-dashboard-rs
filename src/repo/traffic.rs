//! 流量策略窗口与用量：对齐 Go `service/traffic/policy.go` 的
//! `Window` / `addCycle` / `Calculate` / `usageResult`（前台 cycle-transfer 用）。
//!
//! 全库统一 UTC：Go `time.Now()` 取本地时区，这里按 UTC 计算周期窗
//! （容器部署下二者通常一致）；`AddDate` 的月末归一化在 `util::time::add_months` 手工复刻。

use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension};

use crate::repo::bindings;
use crate::telemetry::rollup;
use crate::util::time as util_time;

pub const TRAFFIC_MODE_CUMULATIVE: &str = "cumulative";
pub const TRAFFIC_MODE_RECURRING: &str = "recurring";

const DAY_NANOS: i64 = 24 * 3600 * 1_000_000_000;
const HOUR_NANOS: i64 = 3600 * 1_000_000_000;

/// `traffic_policies` 行（只取窗口/用量计算所需列）。
#[derive(Debug, Clone)]
pub struct TrafficPolicyRow {
    pub id: i64,
    pub server_id: i64,
    pub name: String,
    pub direction: String,
    pub mode: String,
    /// `cycle_start`（datetime 文本 → 纳秒）；空值/零值即 `None`。
    pub cycle_start_nanos: Option<i64>,
    pub cycle_interval: u64,
    pub cycle_unit: String,
    pub quota_bytes: u64,
    pub warning_percent: f64,
    pub notification_tag: String,
    /// `created_at`：cumulative 模式在 `cycle_start` 缺省时的窗口起点。
    pub created_at_nanos: i64,
}

/// `Find(&rows)` + 可选 server_id 过滤（Go `v2PublicCycleTransfer` 的取数）。
/// gorm 软删除语义对齐：排除 `deleted_at` 非空行。
pub fn list_enabled(
    conn: &Connection,
    server_id: Option<i64>,
) -> anyhow::Result<Vec<TrafficPolicyRow>> {
    let sql = "SELECT id, server_id, name, direction, mode, cycle_start, \
               COALESCE(cycle_interval, 1), COALESCE(cycle_unit, ''), quota_bytes, \
               COALESCE(warning_percent, 80), COALESCE(notification_tag, ''), \
               COALESCE(created_at, '') \
               FROM traffic_policies WHERE enabled = 1 AND deleted_at IS NULL \
               AND (?1 IS NULL OR server_id = ?1) ORDER BY server_id ASC, id ASC";
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt
        .query_map([server_id], |row| {
            Ok(TrafficPolicyRow {
                id: row.get(0)?,
                server_id: row.get(1)?,
                name: row.get(2)?,
                direction: row.get(3)?,
                mode: row.get(4)?,
                cycle_start_nanos: row
                    .get::<_, Option<String>>(5)?
                    .as_deref()
                    .and_then(util_time::sqlite_to_unix_nanos),
                cycle_interval: row.get::<_, i64>(6)?.max(0) as u64,
                cycle_unit: row.get(7)?,
                quota_bytes: row.get::<_, i64>(8)?.max(0) as u64,
                warning_percent: row.get(9)?,
                notification_tag: row.get(10)?,
                created_at_nanos: row
                    .get::<_, String>(11)
                    .ok()
                    .as_deref()
                    .and_then(util_time::sqlite_to_unix_nanos)
                    .unwrap_or(0),
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Go `Window`：返回 `(start, end)`；cumulative 的 `end` 为 `None`。
/// recurring 参数非法（缺 `cycle_start`、interval 为 0、unit 不支持）时返回
/// `None`——对应 Go `Calculate` 报错、行不携带 usage 字段。
pub fn policy_window(policy: &TrafficPolicyRow, now_nanos: i64) -> Option<(i64, Option<i64>)> {
    if policy.mode == TRAFFIC_MODE_CUMULATIVE {
        let start = policy
            .cycle_start_nanos
            .filter(|nanos| *nanos != 0)
            .unwrap_or(policy.created_at_nanos);
        let start = if start == 0 { now_nanos } else { start };
        return Some((start, None));
    }
    if policy.mode != TRAFFIC_MODE_RECURRING {
        return None;
    }
    let anchor = policy.cycle_start_nanos.filter(|nanos| *nanos != 0)?;
    // 读侧同样要防：库里可能已有超限的历史行（写侧校验是后加的）。
    let interval = i64::try_from(policy.cycle_interval).ok()?;
    if interval <= 0 {
        return None;
    }
    let unit = policy.cycle_unit.as_str();
    if !matches!(unit, "hour" | "day" | "week" | "month" | "year") {
        return None;
    }
    if now_nanos < anchor {
        return Some((anchor, add_cycle(anchor, interval, unit)));
    }
    let start = match unit {
        "hour" => {
            // Go 用 Unix 秒差取整再加回 anchor，保留亚秒部分。
            // `checked_mul`：回绕成 0 会让下一行除零 panic。
            let step_secs = interval.checked_mul(3600)?;
            let elapsed = (now_nanos - anchor) / 1_000_000_000;
            let step_nanos = step_secs.checked_mul(1_000_000_000)?;
            anchor + elapsed / step_secs * step_nanos
        }
        "day" => {
            let days = (now_nanos - anchor) / DAY_NANOS;
            let step = days / interval * interval;
            anchor + step * DAY_NANOS
        }
        "week" => {
            let days = (now_nanos - anchor) / DAY_NANOS;
            let step_days = 7 * interval;
            let step = days / step_days * step_days;
            anchor + step * DAY_NANOS
        }
        // 月/年按日历推进：月末与闰年走 AddDate 归一化语义。
        "month" | "year" => {
            // 负数步长会让 add_months 永远推不过 now → 死循环。
            let step_months = if unit == "year" {
                interval.checked_mul(12)?
            } else {
                interval
            };
            if step_months <= 0 {
                return None;
            }
            let mut start = anchor;
            loop {
                let next = util_time::add_months(start, step_months)?;
                if next > now_nanos {
                    break;
                }
                start = next;
            }
            start
        }
        _ => return None,
    };
    Some((start, add_cycle(start, interval, unit)))
}

/// Go `addCycle`：按周期单位推进一个步长。
fn add_cycle(nanos: i64, interval: i64, unit: &str) -> Option<i64> {
    match unit {
        "year" => util_time::add_months(nanos, interval.checked_mul(12)?),
        "month" => util_time::add_months(nanos, interval),
        "week" => nanos.checked_add(interval.checked_mul(7)?.checked_mul(DAY_NANOS)?),
        "day" => nanos.checked_add(interval.checked_mul(DAY_NANOS)?),
        "hour" => nanos.checked_add(interval.checked_mul(HOUR_NANOS)?),
        _ => None,
    }
}

/// 单条策略的用量结果（Go `Usage` 的前台消费子集）。
#[derive(Debug, Clone)]
pub struct TrafficUsage {
    pub window_start: i64,
    /// cumulative 为 `None`。
    pub window_end: Option<i64>,
    pub used_bytes: u64,
    pub quota_bytes: u64,
    pub warning_percent: f64,
    pub usage_percent: f64,
    pub status: String,
}

/// Go `Calculate`：窗口 + 当前节点绑定 + rollup 方向求和。
/// 无绑定的服务器计 0（Go 同）；recurring 参数非法返回 `Ok(None)`。
pub fn usage(
    conn: &Connection,
    policy: &TrafficPolicyRow,
    now_nanos: i64,
) -> anyhow::Result<Option<TrafficUsage>> {
    let Some((start, end)) = policy_window(policy, now_nanos) else {
        return Ok(None);
    };
    let used = match bindings::current_binding_for_server(conn, policy.server_id)? {
        None => 0,
        Some(binding) => {
            let upper = end.map_or(now_nanos, |end| end.min(now_nanos));
            rollup::sum_direction_usage(conn, &binding.node_uuid, &policy.direction, start, upper)?
        }
    };
    // quota 为 0 只在绕过 Validate 的脏数据下出现：Go 会算出 NaN 导致 JSON
    // 编码失败，这里取 0 保证响应可序列化。
    let percent = if policy.quota_bytes == 0 {
        0.0
    } else {
        used as f64 * 100.0 / policy.quota_bytes as f64
    };
    let status = if percent >= 100.0 {
        "exceeded"
    } else if percent >= policy.warning_percent {
        "warning"
    } else {
        "normal"
    };
    Ok(Some(TrafficUsage {
        window_start: start,
        window_end: end,
        used_bytes: used,
        quota_bytes: policy.quota_bytes,
        warning_percent: policy.warning_percent,
        usage_percent: percent,
        status: status.to_string(),
    }))
}

/// admin 服务器列表的流量策略概要（Go `trafficservice.Summary`）。
#[derive(Debug, Clone)]
pub struct TrafficSummary {
    pub policy_id: i64,
    pub name: String,
    pub used_bytes: u64,
    pub quota_bytes: u64,
    pub usage_percent: f64,
    pub status: String,
}

/// Go `Summaries`：按服务器聚合启用策略的当前用量（附在 admin 列表行上）。
pub fn summaries(
    conn: &Connection,
    server_ids: &[i64],
    now_nanos: i64,
) -> anyhow::Result<HashMap<i64, Vec<TrafficSummary>>> {
    let mut out: HashMap<i64, Vec<TrafficSummary>> = HashMap::new();
    for server_id in server_ids {
        let mut rows = Vec::new();
        for policy in list_enabled(conn, Some(*server_id))? {
            let Some(usage) = usage(conn, &policy, now_nanos).unwrap_or(None) else {
                continue;
            };
            rows.push(TrafficSummary {
                policy_id: policy.id,
                name: policy.name,
                used_bytes: usage.used_bytes,
                quota_bytes: usage.quota_bytes,
                usage_percent: usage.usage_percent,
                status: usage.status,
            });
        }
        out.insert(*server_id, rows);
    }
    Ok(out)
}

/// 管理端策略写载荷（Go `trafficPolicyWriteDTO` 落库前的最终值）。
#[derive(Debug, Clone)]
pub struct PolicyWrite {
    pub name: String,
    pub direction: String,
    pub mode: String,
    /// datetime 列的纳秒值；`None`/0 = 未提供。
    pub cycle_start_nanos: Option<i64>,
    pub cycle_interval: u64,
    pub cycle_unit: String,
    pub quota_bytes: u64,
    pub warning_percent: f64,
    pub notification_tag: String,
    pub enabled: bool,
}

/// Go `Validate`：归一化并校验；cumulative 强制 interval=1、unit=""。
pub fn validate(write: &mut PolicyWrite) -> Result<(), String> {
    write.name = write.name.trim().to_string();
    write.direction = write.direction.trim().to_lowercase();
    write.mode = write.mode.trim().to_lowercase();
    write.cycle_unit = write.cycle_unit.trim().to_lowercase();
    if write.name.is_empty() || write.name.len() > 100 {
        return Err("traffic policy name is required and must not exceed 100 characters".into());
    }
    if write.direction != rollup::TRAFFIC_DIRECTION_INBOUND
        && write.direction != rollup::TRAFFIC_DIRECTION_OUTBOUND
        && write.direction != rollup::TRAFFIC_DIRECTION_TOTAL
    {
        return Err("traffic direction must be inbound, outbound, or total".into());
    }
    if write.mode != TRAFFIC_MODE_CUMULATIVE && write.mode != TRAFFIC_MODE_RECURRING {
        return Err("traffic mode must be cumulative or recurring".into());
    }
    if write.quota_bytes == 0 {
        return Err("traffic quota must be greater than zero".into());
    }
    if write.warning_percent <= 0.0 || write.warning_percent >= 100.0 {
        return Err("warning percent must be greater than zero and less than 100".into());
    }
    if write.mode == TRAFFIC_MODE_RECURRING {
        match write.cycle_start_nanos {
            Some(start) if start != 0 => {}
            _ => return Err("recurring traffic policy requires cycle_start".into()),
        }
        if write.cycle_interval == 0 {
            return Err("recurring traffic policy requires a positive cycle_interval".into());
        }
        if !matches!(
            write.cycle_unit.as_str(),
            "hour" | "day" | "week" | "month" | "year"
        ) {
            return Err("cycle unit must be hour, day, week, month, or year".into());
        }
        // 上限保证 `interval * 单位跨度` 永不回绕：hour 的回绕会得到 step_secs = 0
        // → 除零 panic；month/year 的回绕会得到负数步长 → add_months 死循环。
        let max_interval = match write.cycle_unit.as_str() {
            "hour" => 100_000,
            "day" => 100_000,
            "week" => 10_000,
            "month" => 1_200,
            "year" => 100,
            _ => 1,
        };
        if write.cycle_interval > max_interval {
            return Err(format!(
                "cycle_interval for unit {} must not exceed {max_interval}",
                write.cycle_unit
            ));
        }
    } else {
        write.cycle_interval = 1;
        write.cycle_unit = String::new();
    }
    Ok(())
}

/// 全列策略行（管理端 CRUD / 历史序列用）。
#[derive(Debug, Clone)]
pub struct TrafficPolicyFullRow {
    pub id: i64,
    pub server_id: i64,
    pub name: String,
    pub direction: String,
    pub mode: String,
    pub cycle_start_nanos: Option<i64>,
    pub cycle_interval: u64,
    pub cycle_unit: String,
    pub quota_bytes: u64,
    pub warning_percent: f64,
    pub notification_tag: String,
    pub enabled: bool,
    pub created_at_nanos: Option<i64>,
    pub updated_at_nanos: Option<i64>,
}

impl TrafficPolicyFullRow {
    /// 窗口/用量计算所需的子集（`usage` / `policy_window` 入参）。
    pub fn window_row(&self) -> TrafficPolicyRow {
        TrafficPolicyRow {
            id: self.id,
            server_id: self.server_id,
            name: self.name.clone(),
            direction: self.direction.clone(),
            mode: self.mode.clone(),
            notification_tag: self.notification_tag.clone(),
            cycle_start_nanos: self.cycle_start_nanos,
            cycle_interval: self.cycle_interval,
            cycle_unit: self.cycle_unit.clone(),
            quota_bytes: self.quota_bytes,
            warning_percent: self.warning_percent,
            created_at_nanos: self.created_at_nanos.unwrap_or(0),
        }
    }
}

const FULL_COLUMNS: &str = "id, server_id, name, direction, mode, cycle_start, \
     COALESCE(cycle_interval, 1), COALESCE(cycle_unit, ''), quota_bytes, \
     COALESCE(warning_percent, 80), COALESCE(notification_tag, ''), COALESCE(enabled, 0), \
     created_at, updated_at";

fn full_row_from(row: &rusqlite::Row<'_>) -> rusqlite::Result<TrafficPolicyFullRow> {
    Ok(TrafficPolicyFullRow {
        id: row.get(0)?,
        server_id: row.get(1)?,
        name: row.get(2)?,
        direction: row.get(3)?,
        mode: row.get(4)?,
        cycle_start_nanos: row
            .get::<_, Option<String>>(5)?
            .as_deref()
            .and_then(util_time::sqlite_to_unix_nanos),
        cycle_interval: row.get::<_, i64>(6)?.max(0) as u64,
        cycle_unit: row.get(7)?,
        quota_bytes: row.get::<_, i64>(8)?.max(0) as u64,
        warning_percent: row.get(9)?,
        notification_tag: row.get(10)?,
        enabled: row.get::<_, i64>(11)? != 0,
        created_at_nanos: row
            .get::<_, Option<String>>(12)?
            .as_deref()
            .and_then(util_time::sqlite_to_unix_nanos),
        updated_at_nanos: row
            .get::<_, Option<String>>(13)?
            .as_deref()
            .and_then(util_time::sqlite_to_unix_nanos),
    })
}

/// 服务器的全部策略（Go `Where("server_id = ?").Order("id ASC")`；
/// 含停用与软删——管理端要看到停用策略，软删行 gorm 默认排除）。
pub fn list_for_server(
    conn: &Connection,
    server_id: i64,
) -> anyhow::Result<Vec<TrafficPolicyFullRow>> {
    let sql = format!(
        "SELECT {FULL_COLUMNS} FROM traffic_policies \
         WHERE server_id = ?1 AND deleted_at IS NULL ORDER BY id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map([server_id], full_row_from)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// 单查策略（归属校验在 SQL 里：id + server_id 双条件）。
pub fn get_policy(
    conn: &Connection,
    server_id: i64,
    policy_id: i64,
) -> anyhow::Result<Option<TrafficPolicyFullRow>> {
    let sql = format!(
        "SELECT {FULL_COLUMNS} FROM traffic_policies \
         WHERE id = ?1 AND server_id = ?2 AND deleted_at IS NULL"
    );
    conn.query_row(&sql, [policy_id, server_id], full_row_from)
        .optional()
        .map_err(Into::into)
}

pub fn server_exists(conn: &Connection, server_id: i64) -> anyhow::Result<bool> {
    Ok(conn
        .query_row(
            "SELECT COUNT(*) FROM servers WHERE id = ?1 AND deleted_at IS NULL",
            [server_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .unwrap_or(0)
        > 0)
}

/// 新建策略，返回新 ID（Go `tx.Create`）。
pub fn insert_policy(
    tx: &rusqlite::Transaction<'_>,
    server_id: i64,
    write: &PolicyWrite,
) -> anyhow::Result<i64> {
    let cycle_start = write
        .cycle_start_nanos
        .filter(|nanos| *nanos != 0)
        .map(util_time::unix_nanos_to_sqlite);
    tx.execute(
        "INSERT INTO traffic_policies (created_at, updated_at, server_id, name, direction, mode, \
         cycle_start, cycle_interval, cycle_unit, quota_bytes, warning_percent, notification_tag, enabled) \
         VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        rusqlite::params![
            server_id,
            write.name,
            write.direction,
            write.mode,
            cycle_start,
            write.cycle_interval as i64,
            write.cycle_unit,
            write.quota_bytes as i64,
            write.warning_percent,
            write.notification_tag,
            write.enabled as i64,
        ],
    )?;
    Ok(tx.last_insert_rowid())
}

/// 更新策略（保留 created_at），返回是否命中（Go `Save` + NotFound 404）。
pub fn update_policy(
    tx: &rusqlite::Transaction<'_>,
    server_id: i64,
    policy_id: i64,
    write: &PolicyWrite,
) -> anyhow::Result<bool> {
    let cycle_start = write
        .cycle_start_nanos
        .filter(|nanos| *nanos != 0)
        .map(util_time::unix_nanos_to_sqlite);
    let changed = tx.execute(
        "UPDATE traffic_policies SET updated_at = CURRENT_TIMESTAMP, name = ?3, direction = ?4, \
         mode = ?5, cycle_start = ?6, cycle_interval = ?7, cycle_unit = ?8, quota_bytes = ?9, \
         warning_percent = ?10, notification_tag = ?11, enabled = ?12 \
         WHERE id = ?1 AND server_id = ?2 AND deleted_at IS NULL",
        rusqlite::params![
            policy_id,
            server_id,
            write.name,
            write.direction,
            write.mode,
            cycle_start,
            write.cycle_interval as i64,
            write.cycle_unit,
            write.quota_bytes as i64,
            write.warning_percent,
            write.notification_tag,
            write.enabled as i64,
        ],
    )?;
    Ok(changed > 0)
}

/// 硬删策略与其评估状态（Go `v2DeleteTrafficPolicy`），返回是否命中。
pub fn delete_policy(
    tx: &rusqlite::Transaction<'_>,
    server_id: i64,
    policy_id: i64,
) -> anyhow::Result<bool> {
    let changed = tx.execute(
        "DELETE FROM traffic_policies WHERE id = ?1 AND server_id = ?2",
        [policy_id, server_id],
    )?;
    if changed == 0 {
        return Ok(false);
    }
    tx.execute(
        "DELETE FROM traffic_policy_states WHERE policy_id = ?1",
        [policy_id],
    )?;
    Ok(true)
}

/// Go `Replace`：服务器保存时的策略整体替换。`None` id = 新建；已有 id 必须
/// 属于该服务器且不得重复；不在提交列表里的旧行连同评估状态一并硬删。
pub fn replace_policies(
    tx: &rusqlite::Transaction<'_>,
    server_id: i64,
    writes: Vec<(i64, PolicyWrite)>,
) -> anyhow::Result<()> {
    // Go 在 upsert 之前取存量快照：新建行不在快照里，不会被当成「未保留」删掉。
    let existing = list_for_server(tx, server_id)?;
    let mut keep: Vec<i64> = Vec::new();
    for (policy_id, mut write) in writes {
        validate(&mut write).map_err(anyhow::Error::msg)?;
        if policy_id == 0 {
            insert_policy(tx, server_id, &write)?;
            continue;
        }
        if keep.contains(&policy_id) {
            anyhow::bail!("duplicate traffic policy id {policy_id}");
        }
        if get_policy_tx(tx, server_id, policy_id)?.is_none() {
            anyhow::bail!("traffic policy {policy_id} does not belong to this server");
        }
        update_policy(tx, server_id, policy_id, &write)?;
        keep.push(policy_id);
    }
    for row in existing {
        if keep.contains(&row.id) {
            continue;
        }
        tx.execute(
            "DELETE FROM traffic_policies WHERE id = ?1 AND server_id = ?2",
            [row.id, server_id],
        )?;
        tx.execute(
            "DELETE FROM traffic_policy_states WHERE policy_id = ?1",
            [row.id],
        )?;
    }
    Ok(())
}

fn get_policy_tx(
    tx: &rusqlite::Transaction<'_>,
    server_id: i64,
    policy_id: i64,
) -> anyhow::Result<Option<TrafficPolicyFullRow>> {
    let sql = format!(
        "SELECT {FULL_COLUMNS} FROM traffic_policies \
         WHERE id = ?1 AND server_id = ?2 AND deleted_at IS NULL"
    );
    tx.query_row(&sql, [policy_id, server_id], full_row_from)
        .optional()
        .map_err(Into::into)
}

/// 历史序列点（Go `trafficservice.Point`）。
#[derive(Debug, Clone)]
pub struct TrafficPoint {
    pub start_nanos: i64,
    pub end_nanos: i64,
    pub bytes: u64,
}

/// 单策略历史（Go `trafficservice.PolicyHistory`）：当前用量 + 近 24 小时点 +
/// 窗口内日点（UTC 对齐；Go 支持本地时区，这里暂按 UTC——IANA 时区库未引入）。
#[derive(Debug, Clone)]
pub struct PolicyHistory {
    pub policy: TrafficPolicyFullRow,
    pub usage: Option<TrafficUsage>,
    pub hourly: Vec<TrafficPoint>,
    pub daily: Vec<TrafficPoint>,
}

const HOURLY_BUCKETS: usize = 24;
const MAX_DAILY_LOOKBACK_NANOS: i64 = 90 * DAY_NANOS;

fn direction_sum(net_in: u64, net_out: u64, direction: &str) -> u64 {
    if direction == rollup::TRAFFIC_DIRECTION_OUTBOUND {
        net_out
    } else if direction == rollup::TRAFFIC_DIRECTION_TOTAL {
        net_in.saturating_add(net_out)
    } else {
        net_in
    }
}

/// Go `emptyHourly`：24 个小时桶骨架（最后一桶到 now）。
fn empty_hourly(now_nanos: i64) -> Vec<TrafficPoint> {
    let current_hour = now_nanos - now_nanos.rem_euclid(HOUR_NANOS);
    let start = current_hour - (HOURLY_BUCKETS as i64 - 1) * HOUR_NANOS;
    (0..HOURLY_BUCKETS)
        .map(|i| {
            let bucket = start + i as i64 * HOUR_NANOS;
            let end = if i == HOURLY_BUCKETS - 1 {
                now_nanos
            } else {
                bucket + HOUR_NANOS
            };
            TrafficPoint {
                start_nanos: bucket,
                end_nanos: end,
                bytes: 0,
            }
        })
        .collect()
}

/// Go `hourlySeries`：24 个小时桶（最后一桶 [整点, now) 用 1m 行现算）。
fn hourly_series(
    conn: &Connection,
    node_uuid: &[u8],
    direction: &str,
    now_nanos: i64,
) -> anyhow::Result<Vec<TrafficPoint>> {
    let mut points = empty_hourly(now_nanos);
    let current_hour = now_nanos - now_nanos.rem_euclid(HOUR_NANOS);
    let start = points[0].start_nanos;
    let mut stmt = conn.prepare(
        "SELECT window_start, net_in_total, net_out_total FROM state_rollups \
         WHERE node_uuid = ?1 AND resolution = '1h' AND window_start >= ?2 AND window_start < ?3",
    )?;
    let rows = stmt.query_map(rusqlite::params![node_uuid, start, current_hour], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?.max(0) as u64,
            row.get::<_, i64>(2)?.max(0) as u64,
        ))
    })?;
    let mut by_start: HashMap<i64, u64> = HashMap::new();
    for row in rows {
        let (window_start, net_in, net_out) = row?;
        let bytes = direction_sum(net_in, net_out, direction);
        *by_start.entry(window_start).or_insert(0) = by_start[&window_start].saturating_add(bytes);
    }
    for point in points.iter_mut().take(HOURLY_BUCKETS - 1) {
        point.bytes = by_start.get(&point.start_nanos).copied().unwrap_or(0);
    }
    points[HOURLY_BUCKETS - 1].bytes =
        rollup::sum_direction_usage(conn, node_uuid, direction, current_hour, now_nanos)
            .unwrap_or(0);
    Ok(points)
}

/// Go `emptyDaily`：窗口起点（钳 90 天回看）到当天的逐日桶骨架，UTC 对齐。
fn empty_daily(window_start: i64, now_nanos: i64) -> Vec<TrafficPoint> {
    let today_start = now_nanos - now_nanos.rem_euclid(DAY_NANOS);
    let lookback = now_nanos - MAX_DAILY_LOOKBACK_NANOS;
    let mut from = window_start - window_start.rem_euclid(DAY_NANOS);
    if from < lookback - lookback.rem_euclid(DAY_NANOS) {
        from = lookback - lookback.rem_euclid(DAY_NANOS);
    }
    if window_start > now_nanos {
        return Vec::new();
    }
    let mut points: Vec<TrafficPoint> = Vec::new();
    let mut day = from;
    while day <= today_start {
        points.push(TrafficPoint {
            start_nanos: day,
            end_nanos: day + DAY_NANOS,
            bytes: 0,
        });
        day += DAY_NANOS;
    }
    points
}

/// Go `dailySeries`：窗口起点（钳 90 天回看）到当天的逐日桶，UTC 对齐。
fn daily_series(
    conn: &Connection,
    node_uuid: &[u8],
    direction: &str,
    window_start: i64,
    now_nanos: i64,
) -> anyhow::Result<Vec<TrafficPoint>> {
    let today_start = now_nanos - now_nanos.rem_euclid(DAY_NANOS);
    let lookback = now_nanos - MAX_DAILY_LOOKBACK_NANOS;
    let mut from = window_start - window_start.rem_euclid(DAY_NANOS);
    if from < lookback - lookback.rem_euclid(DAY_NANOS) {
        from = lookback - lookback.rem_euclid(DAY_NANOS);
    }
    if window_start > now_nanos {
        return Ok(Vec::new());
    }
    let mut points: Vec<TrafficPoint> = Vec::new();
    let mut day = from;
    while day <= today_start {
        points.push(TrafficPoint {
            start_nanos: day,
            end_nanos: day + DAY_NANOS,
            bytes: 0,
        });
        day += DAY_NANOS;
    }
    if points.is_empty() {
        return Ok(points);
    }
    let current_hour = now_nanos - now_nanos.rem_euclid(HOUR_NANOS);
    let mut stmt = conn.prepare(
        "SELECT window_start, net_in_total, net_out_total FROM state_rollups \
         WHERE node_uuid = ?1 AND resolution = '1h' AND window_start >= ?2 AND window_start < ?3",
    )?;
    let rows = stmt.query_map(rusqlite::params![node_uuid, from, current_hour], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?.max(0) as u64,
            row.get::<_, i64>(2)?.max(0) as u64,
        ))
    })?;
    let mut sums: HashMap<i64, u64> = HashMap::new();
    for row in rows {
        let (window_start, net_in, net_out) = row?;
        let day_start = window_start - window_start.rem_euclid(DAY_NANOS);
        let bytes = direction_sum(net_in, net_out, direction);
        *sums.entry(day_start).or_insert(0) = sums[&day_start].saturating_add(bytes);
    }
    for point in points.iter_mut() {
        point.bytes = sums.get(&point.start_nanos).copied().unwrap_or(0);
    }
    let used = rollup::sum_direction_usage(conn, node_uuid, direction, current_hour, now_nanos)
        .unwrap_or(0);
    if used > 0
        && let Some(last) = points.last_mut()
    {
        last.bytes = last.bytes.saturating_add(used);
    }
    Ok(points)
}

/// Go `Histories`：服务器全部策略的当前用量 + 小时/日序列。
pub fn histories(
    conn: &Connection,
    server_id: i64,
    now_nanos: i64,
) -> anyhow::Result<Vec<PolicyHistory>> {
    let policies = list_for_server(conn, server_id)?;
    let binding = bindings::current_binding_for_server(conn, server_id)?;
    let mut items = Vec::new();
    for policy in policies {
        let usage = usage(conn, &policy.window_row(), now_nanos).unwrap_or(None);
        // Go：无绑定也返回零值骨架（emptyHourly / emptyDaily），只有有绑定时才填充。
        let (hourly, daily) = match &binding {
            Some(binding) => {
                let window = policy_window(&policy.window_row(), now_nanos);
                let window_start = window.map_or(now_nanos, |(start, _)| start);
                (
                    hourly_series(conn, &binding.node_uuid, &policy.direction, now_nanos)?,
                    daily_series(
                        conn,
                        &binding.node_uuid,
                        &policy.direction,
                        window_start,
                        now_nanos,
                    )?,
                )
            }
            None => (
                empty_hourly(now_nanos),
                empty_daily(
                    usage.as_ref().map_or(now_nanos, |value| value.window_start),
                    now_nanos,
                ),
            ),
        };
        items.push(PolicyHistory {
            policy,
            usage,
            hourly,
            daily,
        });
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;

    const HOUR: i64 = HOUR_NANOS;
    const DAY: i64 = DAY_NANOS;
    /// 2026-10-16T00:00:00Z。
    const BASE: i64 = 1_792_108_800_000_000_000;

    pub(super) fn policy(
        mode: &str,
        cycle_start: Option<i64>,
        interval: u64,
        unit: &str,
    ) -> TrafficPolicyRow {
        TrafficPolicyRow {
            id: 1,
            server_id: 1,
            name: "p".into(),
            direction: rollup::TRAFFIC_DIRECTION_TOTAL.into(),
            mode: mode.into(),
            cycle_start_nanos: cycle_start,
            cycle_interval: interval,
            cycle_unit: unit.into(),
            quota_bytes: 1000,
            warning_percent: 80.0,
            notification_tag: String::new(),
            created_at_nanos: BASE,
        }
    }

    pub(super) fn write(name: &str) -> PolicyWrite {
        PolicyWrite {
            name: name.into(),
            direction: rollup::TRAFFIC_DIRECTION_TOTAL.into(),
            mode: TRAFFIC_MODE_CUMULATIVE.into(),
            cycle_start_nanos: None,
            cycle_interval: 1,
            cycle_unit: String::new(),
            quota_bytes: 100,
            warning_percent: 80.0,
            notification_tag: String::new(),
            enabled: true,
        }
    }

    #[test]
    fn replace_policies_upserts_and_prunes() {
        let path = std::env::temp_dir().join(format!(
            "santaizi-repo-traffic-replace-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let db = migrate::open_primary(&path.to_string_lossy()).unwrap();
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) VALUES (1, 's1', 't', X'11', 1, 0)",
                [],
            )?;
            replace_policies(tx, 1, vec![(0, write("a")), (0, write("b"))])?;
            let rows = list_for_server(tx, 1)?;
            assert_eq!(rows.len(), 2);

            // 保留 a、改写 b、新增 c → 共 3 行，a 的 id 不变。
            let a_id = rows.iter().find(|row| row.name == "a").unwrap().id;
            replace_policies(
                tx,
                1,
                vec![
                    (a_id, write("a2")),
                    (0, write("b2")),
                    (0, write("c")),
                ],
            )?;
            let rows = list_for_server(tx, 1)?;
            assert_eq!(rows.len(), 3);
            assert!(
                rows.iter().any(|row| row.id == a_id && row.name == "a2"),
                "保留行原位更新"
            );

            // 不属于该服务器的 id 拒绝。
            let err = replace_policies(tx, 1, vec![(999, write("x"))]);
            assert!(err.is_err());
            Ok(())
        })
        .unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn cumulative_window_prefers_cycle_start_then_created_at() {
        // 带 cycle_start：用它，end 为空。
        assert_eq!(
            policy_window(
                &policy(TRAFFIC_MODE_CUMULATIVE, Some(BASE - 3 * DAY), 1, ""),
                BASE
            ),
            Some((BASE - 3 * DAY, None))
        );
        // cycle_start 缺省 → created_at。
        assert_eq!(
            policy_window(&policy(TRAFFIC_MODE_CUMULATIVE, None, 1, ""), BASE),
            Some((BASE, None))
        );
        // 都缺 → now。
        let mut bare = policy(TRAFFIC_MODE_CUMULATIVE, None, 1, "");
        bare.created_at_nanos = 0;
        assert_eq!(policy_window(&bare, BASE), Some((BASE, None)));
    }

    #[test]
    fn recurring_hour_day_week_windows_floor_to_step() {
        // hour：anchor 09:00 + 2h 步长，now=次日 13:20 → 本步 11:00-13:00。
        let anchor = BASE + 9 * HOUR;
        let now = anchor + 28 * HOUR + 20 * 60_000_000_000;
        assert_eq!(
            policy_window(
                &policy(TRAFFIC_MODE_RECURRING, Some(anchor), 2, "hour"),
                now
            ),
            Some((anchor + 28 * HOUR, Some(anchor + 30 * HOUR)))
        );
        // day：anchor 00:00，3 天步长，now=第 7 天 → [6 天, 9 天)。
        assert_eq!(
            policy_window(
                &policy(TRAFFIC_MODE_RECURRING, Some(BASE), 3, "day"),
                BASE + 7 * DAY
            ),
            Some((BASE + 6 * DAY, Some(BASE + 9 * DAY)))
        );
        // week：2 周步长，now=第 5 周 → [4 周, 6 周)。
        assert_eq!(
            policy_window(
                &policy(TRAFFIC_MODE_RECURRING, Some(BASE), 2, "week"),
                BASE + 5 * 7 * DAY
            ),
            Some((BASE + 4 * 7 * DAY, Some(BASE + 6 * 7 * DAY)))
        );
        // 整步落点：now 恰在步边界 → 窗口从 now 开始。
        assert_eq!(
            policy_window(
                &policy(TRAFFIC_MODE_RECURRING, Some(BASE), 3, "day"),
                BASE + 6 * DAY
            ),
            Some((BASE + 6 * DAY, Some(BASE + 9 * DAY)))
        );
    }

    #[test]
    fn recurring_month_year_windows_advance_calendar() {
        // month：anchor 2026-01-31（月末），步长 2 个月。Go AddDate 溢出进位：
        // 链 01-31 → 03-31 → 05-31 → 07-31 → 09-31→10-01（> now 09-15）停。
        // 终点窗 [07-31, 10-01)，窗末同样落在进位后的 10-01。
        let anchor = util_time::days_from_civil(2026, 1, 31).unwrap() * DAY;
        let now = util_time::days_from_civil(2026, 9, 15).unwrap() * DAY;
        assert_eq!(
            policy_window(
                &policy(TRAFFIC_MODE_RECURRING, Some(anchor), 2, "month"),
                now
            ),
            Some((
                util_time::days_from_civil(2026, 7, 31).unwrap() * DAY,
                Some(util_time::days_from_civil(2026, 10, 1).unwrap() * DAY),
            ))
        );
        // year：anchor 2024-02-29（闰日），步长 1 年。非闰年 2-29 进位 3-01：
        // 链 2024-02-29 → 2025-03-01 → 2026-03-01（≤ now）→ 2027-03-01 停。
        let leap = util_time::days_from_civil(2024, 2, 29).unwrap() * DAY;
        let now = util_time::days_from_civil(2026, 6, 1).unwrap() * DAY;
        assert_eq!(
            policy_window(&policy(TRAFFIC_MODE_RECURRING, Some(leap), 1, "year"), now),
            Some((
                util_time::days_from_civil(2026, 3, 1).unwrap() * DAY,
                Some(util_time::days_from_civil(2027, 3, 1).unwrap() * DAY),
            ))
        );
    }

    #[test]
    fn recurring_window_before_anchor_and_invalid_params() {
        // now 早于 anchor：窗口就是 [anchor, anchor+step)。
        assert_eq!(
            policy_window(
                &policy(TRAFFIC_MODE_RECURRING, Some(BASE), 1, "day"),
                BASE - DAY
            ),
            Some((BASE, Some(BASE + DAY)))
        );
        // now == anchor：第一个窗口从 anchor 开始。
        assert_eq!(
            policy_window(&policy(TRAFFIC_MODE_RECURRING, Some(BASE), 1, "day"), BASE),
            Some((BASE, Some(BASE + DAY)))
        );
        // 非法参数一律 None（Go Validate 报错 → 行无 usage 字段）。
        let no_start = policy(TRAFFIC_MODE_RECURRING, None, 1, "day");
        assert_eq!(policy_window(&no_start, BASE), None);
        let zero_interval = policy(TRAFFIC_MODE_RECURRING, Some(BASE), 0, "day");
        assert_eq!(policy_window(&zero_interval, BASE), None);
        let bad_unit = policy(TRAFFIC_MODE_RECURRING, Some(BASE), 1, "fortnight");
        assert_eq!(policy_window(&bad_unit, BASE), None);
        let unknown_mode = policy("bogus", Some(BASE), 1, "day");
        assert_eq!(policy_window(&unknown_mode, BASE), None);
    }

    #[test]
    fn usage_sums_rollups_and_ranks_status() {
        let path =
            std::env::temp_dir().join(format!("santaizi-repo-traffic-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let db = migrate::open_primary(&path.to_string_lossy()).unwrap();
        let node = [7u8; 16];
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) VALUES (1, 's1', 't', X'00', 1, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO server_node_bindings (server_id, node_uuid, valid_from, valid_to, current, reason, created_at) VALUES (1, ?1, 0, NULL, 1, 'r', CURRENT_TIMESTAMP)",
                [&node.to_vec()],
            )?;
            let payload = proto_rollup(BASE, BASE + HOUR, 300, 700);
            rollup::upsert_state_rollup(tx, &node, rollup::RESOLUTION_1H, &payload)?;
            Ok(())
        })
        .unwrap();

        // cumulative：窗口 [BASE-DAY, now)，1h 行的 300+700 全计入 → total 1000 → 100% exceeded。
        let cumulative = policy(TRAFFIC_MODE_CUMULATIVE, Some(BASE - DAY), 1, "");
        let usage_now = db
            .read(|conn| usage(conn, &cumulative, BASE + HOUR))
            .unwrap()
            .unwrap();
        assert_eq!(usage_now.used_bytes, 1000);
        assert_eq!(usage_now.usage_percent, 100.0);
        assert_eq!(usage_now.status, "exceeded");
        assert_eq!(usage_now.window_end, None);

        // 只算 outbound：700/1000 = 70% < 80% → normal。
        let mut outbound = policy(TRAFFIC_MODE_CUMULATIVE, Some(BASE - DAY), 1, "");
        outbound.direction = rollup::TRAFFIC_DIRECTION_OUTBOUND.into();
        let usage_out = db
            .read(|conn| usage(conn, &outbound, BASE + HOUR))
            .unwrap()
            .unwrap();
        assert_eq!(usage_out.used_bytes, 700);
        assert_eq!(usage_out.status, "normal");

        // warning 带宽：80% 整。
        let mut warn = policy(TRAFFIC_MODE_CUMULATIVE, Some(BASE - DAY), 1, "");
        warn.direction = rollup::TRAFFIC_DIRECTION_OUTBOUND.into();
        warn.quota_bytes = 875; // 700/875 = 80%
        let usage_warn = db
            .read(|conn| usage(conn, &warn, BASE + HOUR))
            .unwrap()
            .unwrap();
        assert_eq!(usage_warn.status, "warning");

        // 无绑定的服务器计 0。
        db.write(|tx| {
            tx.execute(
                "INSERT INTO servers (id, name, tag, secret_ciphertext, display_index, hide_for_guest) VALUES (2, 's2', 't2', X'01', 2, 0)",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        let mut unbound = policy(TRAFFIC_MODE_CUMULATIVE, Some(BASE - DAY), 1, "");
        unbound.server_id = 2;
        let usage_zero = db
            .read(|conn| usage(conn, &unbound, BASE + HOUR))
            .unwrap()
            .unwrap();
        assert_eq!(usage_zero.used_bytes, 0);

        // recurring 参数非法 → None。
        let invalid = policy(TRAFFIC_MODE_RECURRING, None, 1, "day");
        assert!(
            db.read(|conn| usage(conn, &invalid, BASE))
                .unwrap()
                .is_none()
        );
        let _ = std::fs::remove_file(&path);
    }

    fn proto_rollup(
        start: i64,
        end: i64,
        net_in: u64,
        net_out: u64,
    ) -> crate::proto::StateRollupPayload {
        crate::proto::StateRollupPayload {
            window_start_unix_nano: start,
            window_end_unix_nano: end,
            sample_count: 1,
            net_in_total: net_in,
            net_out_total: net_out,
            ..Default::default()
        }
    }

    #[test]
    fn list_enabled_filters_and_orders() {
        let path =
            std::env::temp_dir().join(format!("santaizi-repo-traffic-list-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let db = migrate::open_primary(&path.to_string_lossy()).unwrap();
        db.write(|tx| {
            for (id, server, enabled, deleted) in [
                (1, 2, 1, None),
                (2, 1, 1, None),
                (3, 1, 0, None),   // disabled
                (4, 1, 1, Some("2026-01-01 00:00:00+00:00")), // soft deleted
            ] {
                tx.execute(
                    "INSERT INTO traffic_policies (id, created_at, updated_at, deleted_at, server_id, name, direction, mode, cycle_start, cycle_interval, cycle_unit, quota_bytes, warning_percent, enabled) \
                     VALUES (?1, '2026-01-01 00:00:00+00:00', NULL, ?4, ?2, 'p', 'inbound', 'cumulative', NULL, 1, '', 100, 80, ?3)",
                    rusqlite::params![id, server, enabled, deleted],
                )?;
            }
            Ok(())
        })
        .unwrap();

        let all = db.read(|conn| list_enabled(conn, None)).unwrap();
        assert_eq!(all.iter().map(|row| row.id).collect::<Vec<_>>(), vec![2, 1]);
        assert_eq!(all[0].server_id, 1);
        assert_eq!(all[0].created_at_nanos, 1_767_225_600_000_000_000);
        let filtered = db.read(|conn| list_enabled(conn, Some(2))).unwrap();
        assert_eq!(
            filtered.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![1]
        );
        let _ = std::fs::remove_file(&path);
    }
}

#[cfg(test)]
mod cycle_interval_guard_tests {
    use super::tests::{policy, write};
    use super::validate;
    use super::{DAY_NANOS as DAY, HOUR_NANOS as HOUR, TRAFFIC_MODE_RECURRING, policy_window};

    /// 2026-10-16T00:00:00Z。
    const BASE: i64 = 1_792_108_800_000_000_000;

    /// `interval * 3600` 回绕成 0 → 除零 panic（hour）。
    /// 这里用远超上限的 interval 触发回绕路径，确认返回 None 而不是 panic。
    #[test]
    fn oversized_hour_interval_does_not_panic_or_wrap() {
        // 2^63 / 3600 ≈ 2.56e15；再大就会让 `interval * 3600` 回绕成负数或 0。
        let interval = u64::MAX / 3_600 + 10;
        let row = policy(TRAFFIC_MODE_RECURRING, Some(BASE), interval, "hour");
        assert_eq!(policy_window(&row, BASE + DAY), None);
    }

    /// `interval * 12` 回绕成负数 → add_months 死循环（month/year）。
    #[test]
    fn oversized_year_interval_does_not_hang() {
        let interval = u64::MAX / 6 + 10;
        let row = policy(TRAFFIC_MODE_RECURRING, Some(BASE), interval, "year");
        assert_eq!(policy_window(&row, BASE + DAY), None);
    }

    /// 读侧同样要防超限（写侧校验是后加的，库里可能已有历史行）。
    #[test]
    fn oversized_interval_is_rejected_on_read_side() {
        let row = policy(TRAFFIC_MODE_RECURRING, Some(BASE), u64::MAX, "month");
        assert_eq!(policy_window(&row, BASE + DAY), None);
    }

    /// 写侧按单位设上限。
    #[test]
    fn write_side_caps_interval_per_unit() {
        let mut w = write("p");
        w.mode = TRAFFIC_MODE_RECURRING.into();
        w.cycle_start_nanos = Some(BASE);
        w.cycle_unit = "hour".into();
        w.cycle_interval = 100_001;
        assert!(validate(&mut w).is_err(), "hour 超过 100000 应拒");

        w.cycle_interval = 100_000;
        assert!(validate(&mut w).is_ok(), "hour 100000 应放行");

        w.cycle_unit = "year".into();
        w.cycle_interval = 101;
        assert!(validate(&mut w).is_err(), "year 超过 100 应拒");

        w.cycle_interval = 100;
        assert!(validate(&mut w).is_ok(), "year 100 应放行");
    }

    /// 上限之内的正常策略行为不变（回归护栏）。
    #[test]
    fn sane_interval_still_computes_windows() {
        assert_eq!(
            policy_window(
                &policy(TRAFFIC_MODE_RECURRING, Some(BASE), 6, "hour"),
                BASE + 13 * HOUR
            ),
            Some((BASE + 12 * HOUR, Some(BASE + 18 * HOUR)))
        );
        // 3 个月步长：BASE=2026-10-16，+90 天 = 2027-01-14，再 +3 个月 = 2027-04-14。
        // 期望值按日历月推进（add_months），不是固定的 90 天。
        let (start, end) = policy_window(
            &policy(TRAFFIC_MODE_RECURRING, Some(BASE), 3, "month"),
            BASE + 100 * DAY,
        )
        .expect("3 个月步长应算出窗口");
        assert!(start > BASE, "窗口起点应晚于锚点");
        assert!(end.expect("recurring 应有窗口终点") > start);
    }
}
