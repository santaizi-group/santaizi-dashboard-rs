//! 分页参数解析：三处（admin_servers / admin_resources / admin_insights）曾经
//! 各抄一份 `parse_page`，且 offset 一律写成 `(page - 1) * size`。
//!
//! `(page - 1) * size` 在 page/size 都被放开成 i64 后会溢出：debug 构建直接
//! panic，release 构建回绕成负 offset，SQLite 把负数 LIMIT/OFFSET 当「不限」
//! 处理 → 一次拉全表。这里统一收敛到一个函数，并改 `saturating_mul`。

/// 解析 `page` / `page_size`。越界/非数字一律回落默认值，不报错（Go 同款）。
pub fn parse_page(params: &std::collections::HashMap<String, String>) -> (i64, i64) {
    let page = params
        .get("page")
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|page| *page >= 1)
        .unwrap_or(1);
    let size = params
        .get("page_size")
        .and_then(|raw| raw.parse::<i64>().ok())
        .filter(|size| *size >= 1)
        .unwrap_or(20)
        .min(MAX_PAGE_SIZE);
    (page, size)
}

/// 单页上限：与 Go `pageSizeMax` 一致。
pub const MAX_PAGE_SIZE: i64 = 200;

/// 默认单页大小。
pub const DEFAULT_PAGE_SIZE: i64 = 20;

/// `(page - 1) * size`，饱和乘：page 已经 clamp 到 ≥1，size clamp 到
/// `[1, MAX_PAGE_SIZE]`，正常路径不可能溢出；饱和分支只在绕过 `parse_page`
/// 直传极端值时兜底，绝不让它回绕成负数（负数 offset 在 SQLite 里等于不限制）。
#[inline]
pub fn offset_of(page: i64, size: i64) -> i64 {
    let page = page.max(1);
    let size = size.clamp(1, MAX_PAGE_SIZE);
    (page - 1).saturating_mul(size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn defaults_when_missing_or_garbage() {
        assert_eq!(parse_page(&params(&[])), (1, DEFAULT_PAGE_SIZE));
        assert_eq!(
            parse_page(&params(&[("page", "abc")])),
            (1, DEFAULT_PAGE_SIZE)
        );
        assert_eq!(
            parse_page(&params(&[("page", "0"), ("page_size", "-5")])),
            (1, DEFAULT_PAGE_SIZE)
        );
    }

    #[test]
    fn size_is_capped() {
        assert_eq!(
            parse_page(&params(&[("page_size", "99999")])).1,
            MAX_PAGE_SIZE
        );
    }

    #[test]
    fn offset_does_not_wrap() {
        assert_eq!(offset_of(1, 20), 0);
        assert_eq!(offset_of(3, 20), 40);
        // 极端值必须饱和而不是回绕成负数。
        assert_eq!(offset_of(i64::MAX, 200), i64::MAX);
        assert_eq!(offset_of(2, i64::MAX), MAX_PAGE_SIZE);
        assert!(offset_of(i64::MAX, 200) >= 0);
    }
}
