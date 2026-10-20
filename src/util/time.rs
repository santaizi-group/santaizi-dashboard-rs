//! 时间助手。全库统一用 i64 Unix 纳秒；遗留 `datetime` 列用 Go time.Time
//! 驱动的文本格式（UTC，`YYYY-MM-DD HH:MM:SS[.frac]+00:00`），保证与 Go 版数据可对账。

use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

pub fn now_millis() -> i64 {
    now_nanos() / 1_000_000
}

/// Unix 纳秒 → Go `time.Time` 驱动写入 SQLite 的文本格式（UTC）。
pub fn unix_nanos_to_sqlite(nanos: i64) -> String {
    let (secs, nsec) = split_nanos(nanos);
    let civil = civil_from_secs(secs);
    let (y, m, d) = (civil.0, civil.1, civil.2);
    let (hh, mm, ss) = (civil.3, civil.4, civil.5);
    if nsec == 0 {
        format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}+00:00")
    } else {
        // Go 去掉末尾零：.999999999 → 最少一位
        let mut frac = format!("{nsec:09}");
        while frac.ends_with('0') {
            frac.pop();
        }
        format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}.{frac}+00:00")
    }
}

/// Unix 纳秒 → RFC3339 UTC（`…Z`），维护记录 / 状态时间戳用；与 Go `time.RFC3339` 对齐。
pub fn unix_nanos_to_rfc3339(nanos: i64) -> String {
    let (secs, _) = split_nanos(nanos);
    let (y, m, d, hh, mm, ss) = civil_from_secs(secs);
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

/// 解析 Go 驱动格式（容忍缺小数、缺偏移，偏移非零时按给定偏移换算到 UTC）。
pub fn sqlite_to_unix_nanos(text: &str) -> Option<i64> {
    let text = text.trim();
    if text.len() < 19 {
        return None;
    }
    let date = &text[0..10];
    let time = &text[11..19];
    let y: i64 = date.get(0..4)?.parse().ok()?;
    let m: u32 = date.get(5..7)?.parse().ok()?;
    let d: u32 = date.get(8..10)?.parse().ok()?;
    let hh: u32 = time.get(0..2)?.parse().ok()?;
    let mm: u32 = time.get(3..5)?.parse().ok()?;
    let ss: u32 = time.get(6..8)?.parse().ok()?;
    let mut rest = &text[19..];
    let mut nanos: i64 = 0;
    if let Some(stripped) = rest.strip_prefix('.') {
        let end = stripped
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(stripped.len());
        let digits = &stripped[..end];
        if digits.is_empty() || digits.len() > 9 {
            return None;
        }
        let mut padded = String::with_capacity(9);
        padded.push_str(digits);
        while padded.len() < 9 {
            padded.push('0');
        }
        nanos = padded.parse().ok()?;
        rest = &stripped[end..];
    }
    let mut offset_secs: i64 = 0;
    if !rest.is_empty() {
        let rest = rest.trim();
        if let Some(body) = rest.strip_prefix('+').or_else(|| rest.strip_prefix('-')) {
            let sign = if rest.starts_with('-') { -1 } else { 1 };
            let body = body.trim_start_matches(':');
            let (h, m) = body.split_once(':')?;
            offset_secs = sign * (h.parse::<i64>().ok()? * 3600 + m.parse::<i64>().ok()? * 60);
        }
    }
    if !(1..=12).contains(&m) || d == 0 || d > 31 || hh > 23 || mm > 59 || ss > 60 {
        return None;
    }
    let days = days_from_civil(y, m, d)?;
    let secs = days * 86400 + (hh as i64) * 3600 + (mm as i64) * 60 + ss as i64 - offset_secs;
    Some(secs * 1_000_000_000 + nanos)
}

fn split_nanos(nanos: i64) -> (i64, i32) {
    let secs = nanos.div_euclid(1_000_000_000);
    let nsec = nanos.rem_euclid(1_000_000_000);
    (secs, nsec as i32)
}

/// (年, 月, 日, 时, 分, 秒) —— Hinnant 算法。
fn civil_from_secs(secs: i64) -> (i64, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (
        y,
        m,
        d,
        (rem / 3600) as u32,
        ((rem % 3600) / 60) as u32,
        (rem % 60) as u32,
    )
}

/// 反函数：civil → days since epoch。
pub fn days_from_civil(y: i64, m: u32, d: u32) -> Option<i64> {
    if !(1..=12).contains(&m) || d == 0 || d > 31 {
        return None;
    }
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64;
    let doy = (153 * mp + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146097 + doe as i64 - 719468)
}

/// (年, 月, 日) —— 日历周期（月/年）运算用。
pub fn ymd_from_nanos(nanos: i64) -> (i64, u32, u32) {
    let (y, m, d, ..) = civil_from_secs(nanos.div_euclid(1_000_000_000));
    (y, m, d)
}

fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// 当月天数（公历）。
pub fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(y) => 29,
        2 => 28,
        _ => 0,
    }
}

/// Go `AddDate(0, months, 0)`：月字段平移，日溢出按 Go `time.Date` 归一化
/// 进位（1 月 31 日 + 1 个月 = 3 月 3 日，不是钳到月末），时分秒与亚秒保留。
pub fn add_months(nanos: i64, months: i64) -> Option<i64> {
    let (y, m, d) = ymd_from_nanos(nanos);
    let total = y * 12 + (m as i64 - 1) + months;
    let new_y = total.div_euclid(12);
    let new_m = (total.rem_euclid(12) + 1) as u32;
    let day_nanos = 86_400_000_000_000i64;
    let old_day_start = nanos.div_euclid(day_nanos) * day_nanos;
    let time_of_day = nanos - old_day_start;
    let month_start = days_from_civil(new_y, new_m, 1)? * day_nanos;
    Some(month_start + (d as i64 - 1) * day_nanos + time_of_day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_known_values() {
        // 2026-10-15T13:40:00Z
        let nanos = 1_792_071_600_i64 * 1_000_000_000;
        let text = unix_nanos_to_sqlite(nanos);
        assert_eq!(text, "2026-10-15 13:40:00+00:00");
        assert_eq!(sqlite_to_unix_nanos(&text), Some(nanos));
    }

    #[test]
    fn roundtrip_fraction_and_offset() {
        let nanos = 1_792_071_600_123_456_789;
        let text = unix_nanos_to_sqlite(nanos);
        assert_eq!(text, "2026-10-15 13:40:00.123456789+00:00");
        assert_eq!(sqlite_to_unix_nanos(&text), Some(nanos));
        // +08:00 偏移换算回 UTC
        assert_eq!(
            sqlite_to_unix_nanos("2026-10-15 21:40:00.123456789+08:00"),
            Some(nanos)
        );
        // 容忍无小数、无偏移（Go 早期格式）
        assert_eq!(
            sqlite_to_unix_nanos("2026-10-15 13:40:00"),
            Some(1_792_071_600_000_000_000)
        );
    }

    #[test]
    fn epoch_and_negative() {
        assert_eq!(unix_nanos_to_sqlite(0), "1970-01-01 00:00:00+00:00");
        assert_eq!(sqlite_to_unix_nanos("1970-01-01 00:00:00+00:00"), Some(0));
        assert_eq!(
            sqlite_to_unix_nanos("1969-12-31 23:59:59+00:00"),
            Some(-1_000_000_000)
        );
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(sqlite_to_unix_nanos(""), None);
        assert_eq!(sqlite_to_unix_nanos("not-a-time"), None);
        assert_eq!(sqlite_to_unix_nanos("2026-13-01 00:00:00+00:00"), None);
    }

    #[test]
    fn add_months_normalizes_month_end_like_go() {
        let hour = 3_600_000_000_000i64;
        // 2026-01-31T10:30Z + 1 个月 → 2026-03-03T10:30Z（Go AddDate 溢出进位）。
        let jan31 = 1_769_817_600_i64 * 1_000_000_000 + 10 * hour + 30 * 60_000_000_000;
        let added = add_months(jan31, 1).unwrap();
        assert_eq!(ymd_from_nanos(added), (2026, 3, 3));
        assert_eq!(added % (24 * hour), jan31 % (24 * hour));
        // +12 个月落在目标月同日：2027-01-31。
        assert_eq!(
            ymd_from_nanos(add_months(jan31, 12).unwrap()),
            (2027, 1, 31)
        );
        // 闰年：2024-01-31 + 1 个月 → 2024-03-02（2-29 再进 1 天）。
        let jan31_2024 = 1_706_659_200_i64 * 1_000_000_000;
        assert_eq!(
            ymd_from_nanos(add_months(jan31_2024, 1).unwrap()),
            (2024, 3, 2)
        );
        // 日不溢出：2026-03-15 + 1 个月 → 2026-04-15。
        let mar15 = 1_773_532_800_i64 * 1_000_000_000;
        assert_eq!(ymd_from_nanos(add_months(mar15, 1).unwrap()), (2026, 4, 15));
    }
}
