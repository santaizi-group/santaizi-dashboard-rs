//! 可用性摘要：对齐 Go `service/singleton/availability.go` 的
//! `GetServerAvailabilitySummaries` / `SummarizeOfflineIntervals` /
//! `FormatAvailabilityPercent`（单服务器版，前台 `/availability` 用）。

use rusqlite::{Connection, OptionalExtension};

use crate::util::time as util_time;

/// 摘要查询天数上限（Go `maxOfflineSummaryDays`：10 年，防大查询拖垮服务）。
pub const MAX_SUMMARY_DAYS: i64 = 3660;

/// 服务器可用性聚合摘要（Go `singleton.ServerAvailability`）。
/// `availability_percent` 为 `None` 表示该服务器从未上报过数据（不可统计）。
#[derive(Debug, Clone)]
pub struct AvailabilitySummary {
    pub server_id: i64,
    pub days: i64,
    pub offline_count: i64,
    pub total_offline_seconds: u64,
    pub longest_offline_seconds: u64,
    pub availability_percent: Option<f64>,
}

/// 可用率格式化：保留两位小数并向下取整（Go `FormatAvailabilityPercent`），
/// 避免少量离线后四舍五入显示为 100.00%。
pub fn format_availability_percent(percent: f64) -> f64 {
    if percent <= 0.0 {
        return 0.0;
    }
    if percent >= 100.0 {
        return 100.0;
    }
    (percent * 100.0).floor() / 100.0
}

/// 离线区间在统计窗 `[period_start, period_end)` 内的并集时长与最长单段。
/// 输入单位为纳秒；重叠区间只计一次（Go `SummarizeOfflineIntervals`）。
/// 返回 `(总秒数, 最长秒数)`。
pub fn summarize_offline_intervals(
    intervals: &[(i64, i64)],
    period_start: i64,
    period_end: i64,
) -> (u64, u64) {
    let mut clamped: Vec<(i64, i64)> = intervals
        .iter()
        .filter_map(|(start, end)| {
            let start = (*start).max(period_start);
            let end = (*end).min(period_end);
            (end > start).then_some((start, end))
        })
        .collect();
    if clamped.is_empty() {
        return (0, 0);
    }
    clamped.sort_unstable();

    let mut total: u64 = 0;
    let mut longest: u64 = 0;
    let (mut cur_start, mut cur_end) = clamped[0];
    for &(start, end) in &clamped[1..] {
        if start > cur_end {
            total += flush_len(cur_start, cur_end, &mut longest);
            cur_start = start;
            cur_end = end;
        } else if end > cur_end {
            cur_end = end;
        }
    }
    total += flush_len(cur_start, cur_end, &mut longest);
    (total, longest)
}

fn flush_len(start: i64, end: i64, longest: &mut u64) -> u64 {
    let seconds = ((end - start) / 1_000_000_000).max(0) as u64;
    if seconds > *longest {
        *longest = seconds;
    }
    seconds
}

/// 单服务器最近 `days` 天可用性摘要（Go `GetServerAvailabilitySummaries` 单点版）。
/// 从未上报（`server_runtimes.last_seen_at` 为空）的服务器 `availability_percent`
/// 保持 `None`，而不是 100%。
pub fn summarize(
    conn: &Connection,
    server_id: i64,
    days: i64,
    now_nanos: i64,
) -> anyhow::Result<AvailabilitySummary> {
    let days = days.clamp(1, MAX_SUMMARY_DAYS);
    let period_start = now_nanos - days * 24 * 3600 * 1_000_000_000;
    let period_end = now_nanos;

    let mut summary = AvailabilitySummary {
        server_id,
        days,
        offline_count: 0,
        total_offline_seconds: 0,
        longest_offline_seconds: 0,
        availability_percent: None,
    };

    let last_seen: Option<Option<String>> = conn
        .query_row(
            "SELECT last_seen_at FROM server_runtimes WHERE server_id = ?1",
            [server_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?;
    let reported = matches!(last_seen, Some(Some(_)));
    if !reported {
        return Ok(summary);
    }
    summary.availability_percent = Some(100.0);

    let mut stmt = conn.prepare(
        "SELECT started_at, ended_at FROM server_offline_histories \
         WHERE server_id = ?1 AND deleted_at IS NULL AND started_at < ?2 \
         AND (ended_at IS NULL OR ended_at > ?3) \
         ORDER BY started_at DESC",
    )?;
    let rows = stmt.query_map(
        rusqlite::params![
            server_id,
            util_time::unix_nanos_to_sqlite(period_end),
            util_time::unix_nanos_to_sqlite(period_start),
        ],
        |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
            ))
        },
    )?;
    let mut intervals = Vec::new();
    for row in rows {
        let (started_at, ended_at) = row?;
        let Some(start) = started_at
            .as_deref()
            .and_then(util_time::sqlite_to_unix_nanos)
        else {
            continue;
        };
        // Go：`ended_at IS NULL OR ended_at > period_start`；未结束的离线按窗口末端计。
        let end = ended_at
            .as_deref()
            .and_then(util_time::sqlite_to_unix_nanos)
            .unwrap_or(period_end);
        intervals.push((start, end));
    }
    summary.offline_count = intervals.len() as i64;
    let (total, longest) = summarize_offline_intervals(&intervals, period_start, period_end);
    summary.total_offline_seconds = total;
    summary.longest_offline_seconds = longest;

    let period_seconds = ((period_end - period_start) / 1_000_000_000).max(0) as u64;
    if period_seconds == 0 {
        return Ok(summary);
    }
    if total >= period_seconds {
        summary.availability_percent = Some(0.0);
    } else {
        let percent = (1.0 - total as f64 / period_seconds as f64) * 100.0;
        summary.availability_percent = Some(format_availability_percent(percent));
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 3600 * 1_000_000_000;
    const DAY: i64 = 24 * HOUR;

    #[test]
    fn format_percent_floors_to_two_decimals() {
        assert_eq!(format_availability_percent(0.0), 0.0);
        assert_eq!(format_availability_percent(-5.0), 0.0);
        assert_eq!(format_availability_percent(100.0), 100.0);
        assert_eq!(format_availability_percent(150.0), 100.0);
        // 99.999% 必须向下取整为 99.99，不得四舍五入成 100。
        assert_eq!(format_availability_percent(99.999), 99.99);
        assert_eq!(format_availability_percent(87.654321), 87.65);
    }

    #[test]
    fn offline_interval_union_counts_overlap_once() {
        let start = 10 * DAY;
        let end = 11 * DAY;
        // 两段重叠：[0,2h) 与 [1h,3h) → 并集 3h，最长 3h。
        let (total, longest) = summarize_offline_intervals(
            &[(start, start + 2 * HOUR), (start + HOUR, start + 3 * HOUR)],
            start,
            end,
        );
        assert_eq!(total, 3 * 3600);
        assert_eq!(longest, 3 * 3600);

        // 两段不相交：1h + 2h，最长 2h。
        let (total, longest) = summarize_offline_intervals(
            &[(start, start + HOUR), (start + 5 * HOUR, start + 7 * HOUR)],
            start,
            end,
        );
        assert_eq!(total, 3 * 3600);
        assert_eq!(longest, 2 * 3600);
    }

    #[test]
    fn offline_interval_clamped_to_window() {
        let start = 10 * DAY;
        let end = 11 * DAY;
        // 跨窗区间按窗口裁剪；窗口外的忽略。
        let (total, longest) = summarize_offline_intervals(
            &[
                (start - 5 * HOUR, start + HOUR), // 裁成 [start, start+1h)
                (end + HOUR, end + 2 * HOUR),     // 完全在窗外
            ],
            start,
            end,
        );
        assert_eq!(total, 3600);
        assert_eq!(longest, 3600);

        // 未结束（end == start 的退化区间）不计。
        assert_eq!(
            summarize_offline_intervals(&[(start, start)], start, end),
            (0, 0)
        );
        assert_eq!(summarize_offline_intervals(&[], start, end), (0, 0));
    }
}
