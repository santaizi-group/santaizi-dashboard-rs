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

/// 把任意本仓写出的时间文本归一成 `YYYY-MM-DD HH:MM:SS`（UTC，无偏移后缀）。
///
/// 用于比较两侧：`unix_nanos_to_sqlite` 产 `…+00:00`，`CURRENT_TIMESTAMP` 产
/// 无后缀串。同秒内无后缀串恒「小于」带后缀串（字典序），直接混比会漏掉
/// 窗口边界那一秒的行。
pub fn sqlite_to_datetime_text(text: &str) -> String {
    match sqlite_to_unix_nanos(text) {
        Some(nanos) => {
            let secs = nanos.div_euclid(1_000_000_000);
            let civil = civil_from_secs(secs);
            format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                civil.0, civil.1, civil.2, civil.3, civil.4, civil.5
            )
        }
        None => text.trim().to_string(),
    }
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

/// 按给定 `location` 日历推进 `months` 个月，月末钳制为目标月最大天数
/// （1 月 31 日 + 1 月 = 2 月 28/29 日，1 月 31 日 + 2 月 = 3 月 31 日），
/// 保留本地时分秒与亚秒；未知或空时区回退 UTC。
pub fn add_months_in_location(nanos: i64, months: i64, location: &str) -> Option<i64> {
    let (utc_secs, sub_nanos) = split_nanos(nanos);
    let off0 = calendar_offset_seconds(location, utc_secs) as i64;
    let local_secs = utc_secs.checked_add(off0)?;
    let (y, m, d, hh, mm, ss) = civil_from_secs(local_secs);
    let total = y
        .checked_mul(12)?
        .checked_add(m as i64 - 1)?
        .checked_add(months)?;
    let new_y = total.div_euclid(12);
    let new_m = (total.rem_euclid(12) + 1) as u32;
    let max_d = days_in_month(new_y, new_m);
    if max_d == 0 {
        return None;
    }
    let new_d = d.min(max_d);
    let new_local_days = days_from_civil(new_y, new_m, new_d)?;
    let time_of_day_secs = (hh as i64) * 3600 + (mm as i64) * 60 + (ss as i64);
    let new_local_secs = new_local_days
        .checked_mul(86400)?
        .checked_add(time_of_day_secs)?;

    // 两步不动点逼近：兼容夏令时时区跨冬夏令边界时的偏移变化。
    let guess0 = new_local_secs.checked_sub(off0)?;
    let off1 = calendar_offset_seconds(location, guess0) as i64;
    let guess1 = new_local_secs.checked_sub(off1)?;
    let off2 = calendar_offset_seconds(location, guess1) as i64;
    let target_utc_secs = new_local_secs.checked_sub(off2)?;
    target_utc_secs
        .checked_mul(1_000_000_000)?
        .checked_add(sub_nanos as i64)
}

/// UTC 日历月推进（月末钳制）。
pub fn add_months(nanos: i64, months: i64) -> Option<i64> {
    add_months_in_location(nanos, months, "UTC")
}

fn calendar_offset_seconds(location: &str, unix_secs: i64) -> i32 {
    let trimmed = location.trim();
    if trimmed.is_empty() {
        return 0;
    }
    if let Some(zone) = load_system_tzif(trimmed) {
        return zone.offset_at(unix_secs);
    }
    parse_explicit_or_fallback_offset(trimmed).unwrap_or(0)
}

/// 按面板配置 `location` 取给定纳秒时间戳对应的本地 `(年, 月, 日)`。
pub fn local_ymd_in_location(nanos: i64, location: &str) -> (i64, u32, u32) {
    let (secs, _) = split_nanos(nanos);
    let offset = offset_seconds_for_location(location, secs) as i64;
    let (y, m, d, ..) = civil_from_secs(secs.saturating_add(offset));
    (y, m, d)
}

/// 按面板配置 `location` 取给定纳秒时间戳对应的本地日期串 `YYYY-MM-DD`。
pub fn local_date_in_location(nanos: i64, location: &str) -> String {
    let (y, m, d) = local_ymd_in_location(nanos, location);
    format!("{y:04}-{m:02}-{d:02}")
}

#[derive(Clone)]
struct TzifZone {
    transitions: Vec<i64>,
    type_indices: Vec<u8>,
    offsets: Vec<i32>,
    initial_offset: i32,
}

impl TzifZone {
    fn offset_at(&self, unix_secs: i64) -> i32 {
        if self.transitions.is_empty() {
            return self.initial_offset;
        }
        match self.transitions.binary_search(&unix_secs) {
            Ok(idx) => self
                .type_indices
                .get(idx)
                .and_then(|&ti| self.offsets.get(ti as usize))
                .copied()
                .unwrap_or(self.initial_offset),
            Err(0) => self.initial_offset,
            Err(ins) => self
                .type_indices
                .get(ins - 1)
                .and_then(|&ti| self.offsets.get(ti as usize))
                .copied()
                .unwrap_or(self.initial_offset),
        }
    }
}

fn read_be_u32(bytes: &[u8], pos: usize) -> Option<u32> {
    let slice: [u8; 4] = bytes.get(pos..pos + 4)?.try_into().ok()?;
    Some(u32::from_be_bytes(slice))
}

fn read_be_i32(bytes: &[u8], pos: usize) -> Option<i32> {
    let slice: [u8; 4] = bytes.get(pos..pos + 4)?.try_into().ok()?;
    Some(i32::from_be_bytes(slice))
}

fn read_be_i64(bytes: &[u8], pos: usize) -> Option<i64> {
    let slice: [u8; 8] = bytes.get(pos..pos + 8)?.try_into().ok()?;
    Some(i64::from_be_bytes(slice))
}

/// 剩余字节是否容纳 `needed`：不足即整表作废。
/// 头字段来自文件本体，不能信；先校验再 `Vec::with_capacity`，
/// 否则畸形 zoneinfo 会让一个 u32 计数字段触发 GB 级分配直接 OOM。
fn tzif_room(bytes: &[u8], cursor: usize, needed: usize) -> bool {
    cursor
        .checked_add(needed)
        .is_some_and(|end| bytes.len() >= end)
}

/// 解析 `/usr/share/zoneinfo` 的 TZif v1/v2/v3 文件（纯安全切片，支持夏令时跳变）。
fn parse_tzif(bytes: &[u8]) -> Option<TzifZone> {
    if bytes.get(0..4)? != b"TZif" {
        return None;
    }
    let version = *bytes.get(4)?;
    let parse_header = |base: usize| -> Option<(usize, usize, usize, usize, usize, usize)> {
        if bytes.get(base..base + 4)? != b"TZif" {
            return None;
        }
        let isut = read_be_u32(bytes, base + 20)? as usize;
        let isstd = read_be_u32(bytes, base + 24)? as usize;
        let leap = read_be_u32(bytes, base + 28)? as usize;
        let timecnt = read_be_u32(bytes, base + 32)? as usize;
        let typecnt = read_be_u32(bytes, base + 36)? as usize;
        let charcnt = read_be_u32(bytes, base + 40)? as usize;
        Some((isut, isstd, leap, timecnt, typecnt, charcnt))
    };

    let (isut1, isstd1, leap1, timecnt1, typecnt1, charcnt1) = parse_header(0)?;
    let v1_data_len = timecnt1
        .checked_mul(5)?
        .checked_add(typecnt1.checked_mul(6)?)?
        .checked_add(charcnt1)?
        .checked_add(leap1.checked_mul(8)?)?
        .checked_add(isstd1)?
        .checked_add(isut1)?;

    if version == b'2' || version == b'3' {
        let v2_base = 44usize.checked_add(v1_data_len)?;
        // v2 头必须整体落在文件内：`parse_header` 的偏移加法在超长 v1_data_len 下会溢出。
        if bytes.len() >= v2_base
            && let Some((_, _, _, timecnt, typecnt, _)) = parse_header(v2_base)
            && typecnt > 0
        {
            let mut cursor = v2_base + 44;
            if !tzif_room(bytes, cursor, timecnt.saturating_mul(8)) {
                return None;
            }
            let mut transitions = Vec::with_capacity(timecnt);
            for _ in 0..timecnt {
                transitions.push(read_be_i64(bytes, cursor)?);
                cursor += 8;
            }
            let type_indices = bytes.get(cursor..cursor + timecnt)?.to_vec();
            cursor += timecnt;
            if !tzif_room(bytes, cursor, typecnt.saturating_mul(6)) {
                return None;
            }
            let mut offsets = Vec::with_capacity(typecnt);
            let mut first_std: Option<i32> = None;
            for _ in 0..typecnt {
                let utoff = read_be_i32(bytes, cursor)?;
                let is_dst = *bytes.get(cursor + 4)?;
                if is_dst == 0 && first_std.is_none() {
                    first_std = Some(utoff);
                }
                offsets.push(utoff);
                cursor += 6;
            }
            let initial_offset = first_std.or_else(|| offsets.first().copied()).unwrap_or(0);
            return Some(TzifZone {
                transitions,
                type_indices,
                offsets,
                initial_offset,
            });
        }
    }

    if typecnt1 == 0 {
        return None;
    }
    let mut cursor = 44;
    if !tzif_room(bytes, cursor, timecnt1.saturating_mul(4)) {
        return None;
    }
    let mut transitions = Vec::with_capacity(timecnt1);
    for _ in 0..timecnt1 {
        transitions.push(read_be_i32(bytes, cursor)? as i64);
        cursor += 4;
    }
    let type_indices = bytes.get(cursor..cursor + timecnt1)?.to_vec();
    cursor += timecnt1;
    if !tzif_room(bytes, cursor, typecnt1.saturating_mul(6)) {
        return None;
    }
    let mut offsets = Vec::with_capacity(typecnt1);
    let mut first_std: Option<i32> = None;
    for _ in 0..typecnt1 {
        let utoff = read_be_i32(bytes, cursor)?;
        let is_dst = *bytes.get(cursor + 4)?;
        if is_dst == 0 && first_std.is_none() {
            first_std = Some(utoff);
        }
        offsets.push(utoff);
        cursor += 6;
    }
    let initial_offset = first_std.or_else(|| offsets.first().copied()).unwrap_or(0);
    Some(TzifZone {
        transitions,
        type_indices,
        offsets,
        initial_offset,
    })
}

fn valid_zone_relpath(location: &str) -> bool {
    !location.is_empty()
        && !location.starts_with('/')
        && !location.contains("..")
        && !location.contains('\\')
        && location
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'_' | b'-' | b'+'))
}

fn load_system_tzif(location: &str) -> Option<TzifZone> {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    static CACHE: OnceLock<Mutex<HashMap<String, Option<TzifZone>>>> = OnceLock::new();
    if !valid_zone_relpath(location) {
        return None;
    }
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(cached) = cache
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .get(location)
        .cloned()
    {
        return cached;
    }
    let parsed = ["/usr/share/zoneinfo", "/usr/lib/zoneinfo"]
        .iter()
        .find_map(|dir| {
            let path = format!("{dir}/{location}");
            std::fs::read(path)
                .ok()
                .and_then(|bytes| parse_tzif(&bytes))
        });
    let mut guard = cache.lock().unwrap_or_else(|err| err.into_inner());
    if guard.len() < 128 {
        guard.insert(location.to_string(), parsed.clone());
    }
    parsed
}

fn parse_explicit_or_fallback_offset(location: &str) -> Option<i32> {
    let trimmed = location.trim();
    if trimmed.is_empty() {
        return None;
    }
    if matches!(
        trimmed.to_ascii_uppercase().as_str(),
        "UTC" | "GMT" | "ETC/UTC" | "ETC/GMT" | "Z"
    ) {
        return Some(0);
    }
    let rest = trimmed
        .strip_prefix("UTC")
        .or_else(|| trimmed.strip_prefix("utc"))
        .or_else(|| trimmed.strip_prefix("GMT"))
        .or_else(|| trimmed.strip_prefix("gmt"))
        .unwrap_or(trimmed);
    if let Some(sign) = rest.chars().next().filter(|c| *c == '+' || *c == '-') {
        let mult = if sign == '-' { -1 } else { 1 };
        let body = &rest[1..];
        let (h_str, m_str) = body.split_once(':').unwrap_or((body, "0"));
        if let (Ok(h), Ok(m)) = (h_str.parse::<i32>(), m_str.parse::<i32>())
            && (0..=14).contains(&h)
            && (0..60).contains(&m)
        {
            return Some(mult * (h * 3600 + m * 60));
        }
    }
    // 无 zoneinfo 的精简容器兜底常见时区。
    match trimmed {
        "Asia/Shanghai" | "Asia/Chongqing" | "Asia/Harbin" | "Asia/Urumqi" | "PRC"
        | "Asia/Hong_Kong" | "Asia/Macau" | "Asia/Taipei" | "Asia/Singapore"
        | "Asia/Kuala_Lumpur" | "Asia/Manila" | "Australia/Perth" => Some(8 * 3600),
        "Asia/Tokyo" | "Asia/Seoul" => Some(9 * 3600),
        "Asia/Bangkok" | "Asia/Jakarta" | "Asia/Ho_Chi_Minh" => Some(7 * 3600),
        "Asia/Kolkata" | "Asia/Colombo" => Some(5 * 3600 + 1800),
        "Asia/Dubai" => Some(4 * 3600),
        "Europe/Moscow" | "Europe/Istanbul" => Some(3 * 3600),
        "Europe/Berlin" | "Europe/Paris" | "Europe/Madrid" | "Europe/Rome" | "Europe/Amsterdam"
        | "Europe/Zurich" => Some(3600),
        "Europe/London" => Some(0),
        "America/Sao_Paulo" => Some(-3 * 3600),
        "America/New_York" | "America/Toronto" => Some(-5 * 3600),
        "America/Chicago" => Some(-6 * 3600),
        "America/Denver" => Some(-7 * 3600),
        "America/Los_Angeles" | "America/Vancouver" => Some(-8 * 3600),
        "Australia/Sydney" | "Australia/Melbourne" => Some(10 * 3600),
        "Pacific/Auckland" => Some(12 * 3600),
        _ => None,
    }
}

/// 按面板配置 `location`（如 `Asia/Shanghai`、`America/New_York`、`+08:00`）求给定时刻的 UTC 秒偏移。
pub fn offset_seconds_for_location(location: &str, unix_secs: i64) -> i32 {
    let trimmed = location.trim();
    if trimmed.is_empty() {
        return 8 * 3600;
    }
    if let Some(zone) = load_system_tzif(trimmed) {
        return zone.offset_at(unix_secs);
    }
    parse_explicit_or_fallback_offset(trimmed).unwrap_or(0)
}

/// 面向通知正文的可读时间戳：`YYYY-MM-DD HH:MM:SS ±HH:MM`（按面板配置时区换算）。
pub fn format_nanos_in_location(nanos: i64, location: &str) -> String {
    if nanos <= 0 {
        return "—".to_string();
    }
    let (secs, _) = split_nanos(nanos);
    format_unix_secs_in_location(secs, location)
}

/// Unix 秒按配置时区格式化为 `YYYY-MM-DD HH:MM:SS ±HH:MM`。
pub fn format_unix_secs_in_location(secs: i64, location: &str) -> String {
    let offset = offset_seconds_for_location(location, secs);
    let local_secs = secs.saturating_add(offset as i64);
    let (y, m, d, hh, mm, ss) = civil_from_secs(local_secs);
    let sign = if offset < 0 { '-' } else { '+' };
    let abs_off = offset.unsigned_abs();
    let off_h = abs_off / 3600;
    let off_m = (abs_off % 3600) / 60;
    format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02} {sign}{off_h:02}:{off_m:02}")
}

/// 中文可读时长（如 `45秒`、`2分钟`、`1分30秒`、`2小时5分`、`1天3小时`）。
pub fn format_duration_zh(seconds: u64) -> String {
    if seconds < 60 {
        return format!("{seconds}秒");
    }
    if seconds < 3600 {
        let mins = seconds / 60;
        let secs = seconds % 60;
        return if secs == 0 {
            format!("{mins}分钟")
        } else {
            format!("{mins}分{secs}秒")
        };
    }
    if seconds < 86_400 {
        let hours = seconds / 3600;
        let mins = (seconds % 3600) / 60;
        let secs = seconds % 60;
        let mut out = format!("{hours}小时");
        if mins > 0 {
            out.push_str(&format!("{mins}分"));
        }
        if secs > 0 && hours < 6 {
            if mins == 0 {
                out.push_str("0分");
            }
            out.push_str(&format!("{secs}秒"));
        }
        return out;
    }
    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3600;
    let mins = (seconds % 3600) / 60;
    let mut out = format!("{days}天");
    if hours > 0 {
        out.push_str(&format!("{hours}小时"));
    }
    if mins > 0 && days < 7 {
        out.push_str(&format!("{mins}分"));
    }
    out
}

/// 紧凑多语言时长（如 `45s`、`1m 30s`、`2h 5m`、`1d 3h`）。
pub fn format_duration_compact(seconds: u64) -> String {
    if seconds < 60 {
        return format!("{seconds}s");
    }
    if seconds < 3600 {
        let mins = seconds / 60;
        let secs = seconds % 60;
        return if secs == 0 {
            format!("{mins}m")
        } else {
            format!("{mins}m {secs}s")
        };
    }
    if seconds < 86_400 {
        let hours = seconds / 3600;
        let mins = (seconds % 3600) / 60;
        return if mins == 0 {
            format!("{hours}h")
        } else {
            format!("{hours}h {mins}m")
        };
    }
    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3600;
    if hours == 0 {
        format!("{days}d")
    } else {
        format!("{days}d {hours}h")
    }
}

/// 字节量可读格式（1024 进制：B / KB / MB / GB / TB）。
pub fn format_bytes_human(bytes: f64) -> String {
    if !bytes.is_finite() || bytes <= 0.0 {
        return "0 B".to_string();
    }
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    let mut value = bytes;
    let mut idx = 0usize;
    while value >= 1024.0 && idx + 1 < UNITS.len() {
        value /= 1024.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{value:.0} B")
    } else if value >= 100.0 {
        format!("{value:.0} {}", UNITS[idx])
    } else if value >= 10.0 {
        format!("{value:.1} {}", UNITS[idx])
    } else {
        format!("{value:.2} {}", UNITS[idx])
    }
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
    fn datetime_text_normalizes_both_formats() {
        // 两侧同秒：无后缀串字典序恒「小于」带后缀串，混比会漏边界行。
        let nanos = 1_792_071_600_i64 * 1_000_000_000;
        assert_eq!(
            sqlite_to_datetime_text(&unix_nanos_to_sqlite(nanos)),
            "2026-10-15 13:40:00"
        );
        assert_eq!(
            sqlite_to_datetime_text("2026-10-15 13:40:00"),
            "2026-10-15 13:40:00"
        );
        assert_eq!(
            sqlite_to_datetime_text("2026-10-15 13:40:00.500+00:00"),
            "2026-10-15 13:40:00"
        );
        assert_eq!(
            sqlite_to_datetime_text("2026-10-15 13:40:00.500"),
            "2026-10-15 13:40:00"
        );
        // 非零偏移按给定偏移换算到 UTC。
        assert_eq!(
            sqlite_to_datetime_text("2026-10-15 21:40:00+08:00"),
            "2026-10-15 13:40:00"
        );
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
    fn add_months_clamps_month_end() {
        let hour = 3_600_000_000_000i64;
        // 2026-01-31T10:30Z + 1 个月 → 2026-02-28T10:30Z（月末钳制，不溢出到 3 月 3 日）。
        let jan31 = 1_769_817_600_i64 * 1_000_000_000 + 10 * hour + 30 * 60_000_000_000;
        let added = add_months(jan31, 1).unwrap();
        assert_eq!(ymd_from_nanos(added), (2026, 2, 28));
        assert_eq!(added % (24 * hour), jan31 % (24 * hour));
        // +2 个月按锚点推进回到 2026-03-31。
        assert_eq!(ymd_from_nanos(add_months(jan31, 2).unwrap()), (2026, 3, 31));
        // +12 个月落在目标月同日：2027-01-31。
        assert_eq!(
            ymd_from_nanos(add_months(jan31, 12).unwrap()),
            (2027, 1, 31)
        );
        // 闰年：2024-01-31 + 1 个月 → 2024-02-29。
        let jan31_2024 = 1_706_659_200_i64 * 1_000_000_000;
        assert_eq!(
            ymd_from_nanos(add_months(jan31_2024, 1).unwrap()),
            (2024, 2, 29)
        );
        // 日不溢出：2026-03-15 + 1 个月 → 2026-04-15。
        let mar15 = 1_773_532_800_i64 * 1_000_000_000;
        assert_eq!(ymd_from_nanos(add_months(mar15, 1).unwrap()), (2026, 4, 15));
    }

    #[test]
    fn add_months_in_location_respects_panel_timezone_and_clamps_month_end() {
        // 2026-02-01T00:00:00+08:00 == 2026-01-31T16:00:00Z
        // 若错误按 UTC 日历计算会把 1 月 31 日钳制成 2 月 28 日；按 Asia/Shanghai 必须落在每月 1 日 00:00+08:00。
        let feb01_cst = sqlite_to_unix_nanos("2026-02-01 00:00:00+08:00").unwrap();
        let mar01_cst = add_months_in_location(feb01_cst, 1, "Asia/Shanghai").unwrap();
        let apr01_cst = add_months_in_location(feb01_cst, 2, "Asia/Shanghai").unwrap();
        assert_eq!(
            format_nanos_in_location(mar01_cst, "Asia/Shanghai"),
            "2026-03-01 00:00:00 +08:00"
        );
        assert_eq!(
            format_nanos_in_location(apr01_cst, "Asia/Shanghai"),
            "2026-04-01 00:00:00 +08:00"
        );

        // 东八区 1 月 31 日 → 2 月 28 日 → 3 月 31 日
        let jan31_cst = sqlite_to_unix_nanos("2026-01-31 00:00:00+08:00").unwrap();
        assert_eq!(
            format_nanos_in_location(
                add_months_in_location(jan31_cst, 1, "Asia/Shanghai").unwrap(),
                "Asia/Shanghai"
            ),
            "2026-02-28 00:00:00 +08:00"
        );
        assert_eq!(
            format_nanos_in_location(
                add_months_in_location(jan31_cst, 2, "Asia/Shanghai").unwrap(),
                "Asia/Shanghai"
            ),
            "2026-03-31 00:00:00 +08:00"
        );

        // 未知时区回退 UTC
        assert_eq!(
            add_months_in_location(jan31_cst, 1, "Mars/Olympus_Mons"),
            add_months(jan31_cst, 1)
        );
        assert_eq!(
            local_date_in_location(feb01_cst, "Asia/Shanghai"),
            "2026-02-01"
        );
        assert_eq!(local_date_in_location(feb01_cst, "UTC"), "2026-01-31");
    }

    #[test]
    fn formats_in_configured_location_and_rejects_path_traversal() {
        // 2026-10-15T13:40:00Z → Asia/Shanghai (+08:00) = 2026-10-15 21:40:00 +08:00
        let nanos = 1_792_071_600_i64 * 1_000_000_000;
        assert_eq!(
            format_nanos_in_location(nanos, "Asia/Shanghai"),
            "2026-10-15 21:40:00 +08:00"
        );
        assert_eq!(
            format_nanos_in_location(nanos, "UTC"),
            "2026-10-15 13:40:00 +00:00"
        );
        assert_eq!(
            format_nanos_in_location(nanos, "-05:00"),
            "2026-10-15 08:40:00 -05:00"
        );
        assert_eq!(format_nanos_in_location(0, "Asia/Shanghai"), "—");
        assert!(!valid_zone_relpath("../etc/passwd"));
        assert!(!valid_zone_relpath("/etc/passwd"));
    }

    #[test]
    fn human_duration_and_bytes_formatting() {
        assert_eq!(format_duration_zh(0), "0秒");
        assert_eq!(format_duration_zh(45), "45秒");
        assert_eq!(format_duration_zh(60), "1分钟");
        assert_eq!(format_duration_zh(95), "1分35秒");
        assert_eq!(format_duration_zh(3725), "1小时2分5秒");
        assert_eq!(format_duration_zh(90_000), "1天1小时");
        assert_eq!(format_bytes_human(512.0), "512 B");
        assert_eq!(format_bytes_human(1536.0), "1.50 KB");
        assert_eq!(format_bytes_human(1024.0 * 1024.0 * 128.0), "128 MB");
    }

    /// 畸形 TZif：头字段声明海量 transitions/types 但字节不够，必须整表作废。
    /// 旧实现先 `Vec::with_capacity(timecnt)` 再逐字节校验，一个 u32 就能 OOM。
    #[test]
    fn malformed_tzif_is_rejected_without_preallocating() {
        // v2 形态：v1 头 timecnt=0 / typecnt=1 / charcnt=1 → v1 数据区 7 字节，
        // v2 头因此落在 51；v2 头声明 0xffffffff 个 transition，而文件在
        // v2 数据区之前就结束。
        let mut raw = Vec::new();
        raw.extend_from_slice(b"TZif");
        raw.push(b'2');
        raw.extend_from_slice(&[0u8; 15]);
        for value in [0u32, 0, 0, 0, 1, 1] {
            raw.extend_from_slice(&value.to_be_bytes());
        }
        assert_eq!(raw.len(), 44);
        // v1 数据区（typecnt*6 + charcnt = 7 字节）。
        raw.extend_from_slice(&[0u8; 7]);
        assert_eq!(raw.len(), 51);
        // v2 头（44 字节），timecnt = u32::MAX。
        raw.extend_from_slice(b"TZif");
        raw.push(b'2');
        raw.extend_from_slice(&[0u8; 15]);
        for value in [0u32, 0, 0, u32::MAX, 1, 0] {
            raw.extend_from_slice(&value.to_be_bytes());
        }
        assert_eq!(raw.len(), 51 + 44, "v2 头必须完整落在文件内才会被解析");
        assert!(parse_tzif(&raw).is_none(), "字节不足的计数必须整表作废");

        // v1 形态同样拒绝：typecnt 声明巨大但正文为空。
        let mut raw_v1 = Vec::new();
        raw_v1.extend_from_slice(b"TZif");
        raw_v1.push(b'1');
        raw_v1.extend_from_slice(&[0u8; 15]);
        for value in [0u32, 0, 0, 0, u32::MAX, 0] {
            raw_v1.extend_from_slice(&value.to_be_bytes());
        }
        assert!(parse_tzif(&raw_v1).is_none());
        // 不是 TZif。
        assert!(parse_tzif(b"not a zone file at all").is_none());
    }
}
