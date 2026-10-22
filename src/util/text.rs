//! 文本工具：与 SQL 语义相关的转义。

/// 把用户输入包装成 `LIKE` 的包含匹配串：转义 `\` `%` `_` 三个元字符。
///
/// 调用方必须同时在 SQL 里写 `ESCAPE '\'`，否则转义符本身不被识别。
/// 配套的 `COUNT` 与 `SELECT` 要用同一个值，否则计数与行数不一致。
///
/// Go `api_v2_spa.go:571-574` 直接 `LIKE %?%` 拼原始值，同样未转义——
/// Rust 侧这是加固，行为差异只体现在「含 `%`/`_` 的搜索词不再匹配无关行」。
pub fn like_contains(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 2);
    for ch in raw.chars() {
        match ch {
            '\\' | '%' | '_' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_text_passes_through() {
        assert_eq!(like_contains("node-7"), "node-7");
        assert_eq!(like_contains(""), "");
        assert_eq!(like_contains("三太子"), "三太子");
    }

    #[test]
    fn wildcards_are_escaped() {
        assert_eq!(like_contains("100%"), "100\\%");
        assert_eq!(like_contains("a_b"), "a\\_b");
        assert_eq!(like_contains("back\\slash"), "back\\\\slash");
        assert_eq!(like_contains("%_\\"), "\\%\\_\\\\");
    }

    #[test]
    fn escaped_value_matches_literally() {
        // 语义验证：转义后 `%` 只匹配字面 `%`，不再当通配符。
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (name TEXT)").unwrap();
        for name in ["a%b", "axb", "a_b", "axb_y"] {
            conn.execute("INSERT INTO t (name) VALUES (?1)", [name])
                .unwrap();
        }
        let pattern = format!("%{}%", like_contains("a%b"));
        let hits: Vec<String> = conn
            .prepare("SELECT name FROM t WHERE name LIKE ?1 ESCAPE '\\'")
            .unwrap()
            .query_map([pattern], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(hits, vec!["a%b".to_string()]);
    }
}
