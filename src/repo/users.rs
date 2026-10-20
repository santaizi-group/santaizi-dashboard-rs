//! 用户与会话：对齐 Go `model.User` 的 cookie 会话查询与 OAuth2 登录落地。
//!
//! 会话令牌存 `users.token`，过期时间 `users.token_expired`。

use rusqlite::OptionalExtension;

use crate::util::time as util_time;

/// 已认证用户（对应 Go `CtxKeyAuthorizedUser` 的 `model.User` 子集）。
#[derive(Debug, Clone)]
pub struct UserRow {
    pub id: i64,
    pub login: String,
    pub name: String,
    pub avatar_url: String,
    pub super_admin: bool,
}

/// 按 ID 查用户（Bearer 令牌命中后取用户档案）。
pub fn find_by_id(conn: &rusqlite::Connection, id: i64) -> anyhow::Result<Option<UserRow>> {
    let row = conn
        .query_row(
            "SELECT id, login, IFNULL(name, ''), IFNULL(avatar_url, ''), IFNULL(super_admin, 0) \
             FROM users WHERE id = ?1 AND deleted_at IS NULL",
            [id],
            |row| {
                Ok(UserRow {
                    id: row.get(0)?,
                    login: row.get(1)?,
                    name: row.get(2)?,
                    avatar_url: row.get(3)?,
                    super_admin: row.get::<_, i64>(4)? != 0,
                })
            },
        )
        .optional()?;
    Ok(row)
}

/// 按会话令牌查用户；要求未删除且 `token_expired` 晚于 `now_nanos`。
/// 过期时间解析失败按已过期处理（Go 零值 `time.Time` 同理）。
pub fn find_by_token(
    conn: &rusqlite::Connection,
    token: &str,
    now_nanos: i64,
) -> anyhow::Result<Option<UserRow>> {
    let row = conn
        .query_row(
            "SELECT id, login, IFNULL(name, ''), IFNULL(avatar_url, ''), IFNULL(super_admin, 0), token_expired \
             FROM users WHERE token = ?1 AND deleted_at IS NULL",
            [token],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)? != 0,
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((id, login, name, avatar_url, super_admin, token_expired)) = row else {
        return Ok(None);
    };
    let expired = match token_expired
        .as_deref()
        .map(util_time::sqlite_to_unix_nanos)
    {
        Some(Some(nanos)) => nanos <= now_nanos,
        _ => true,
    };
    if expired {
        return Ok(None);
    }
    Ok(Some(UserRow {
        id,
        login,
        name,
        avatar_url,
        super_admin,
    }))
}

/// OAuth2 登录档案：api 层把 provider userinfo 映射成这个中性结构。
#[derive(Debug, Clone, Default)]
pub struct OauthProfile {
    pub login: String,
    pub name: String,
    pub avatar_url: String,
    pub blog: String,
    pub email: String,
    pub bio: String,
    pub hireable: bool,
    pub super_admin: bool,
}

/// OAuth2 登录落地：按 `login` upsert 会话令牌，返回用户 ID。
///
/// Go 用 `DB.Save(&user)`：provider 映射器带上外部数字 ID，GORM 据此定位行
/// （无行则插入）。本仓用户表无外部 ID 列，以 `login` 为自然键——同一登录名
/// 再次登录即刷新令牌，语义与 Go 对已登录用户等价。
pub fn upsert_oauth_user(
    tx: &rusqlite::Transaction<'_>,
    profile: &OauthProfile,
    token: &str,
    token_expired_nanos: i64,
) -> anyhow::Result<i64> {
    let now = util_time::unix_nanos_to_sqlite(util_time::now_nanos());
    let expired = util_time::unix_nanos_to_sqlite(token_expired_nanos);
    let existing: Option<i64> = tx
        .query_row(
            "SELECT id FROM users WHERE login = ?1 AND deleted_at IS NULL",
            [profile.login.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(id) = existing {
        tx.execute(
            "UPDATE users SET token = ?1, token_expired = ?2, name = ?3, avatar_url = ?4, \
             blog = ?5, email = ?6, bio = ?7, hireable = ?8, super_admin = ?9, updated_at = ?10 \
             WHERE id = ?11",
            rusqlite::params![
                token,
                expired,
                profile.name,
                profile.avatar_url,
                profile.blog,
                profile.email,
                profile.bio,
                profile.hireable as i64,
                profile.super_admin as i64,
                now,
                id
            ],
        )?;
        return Ok(id);
    }
    tx.execute(
        "INSERT INTO users (login, name, avatar_url, blog, email, bio, hireable, token, \
         token_expired, super_admin, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11)",
        rusqlite::params![
            profile.login,
            profile.name,
            profile.avatar_url,
            profile.blog,
            profile.email,
            profile.bio,
            profile.hireable as i64,
            token,
            expired,
            profile.super_admin as i64,
            now
        ],
    )?;
    Ok(tx.last_insert_rowid())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;

    fn temp_db(tag: &str) -> String {
        let path =
            std::env::temp_dir().join(format!("santaizi-repo-users-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        path.to_string_lossy().into_owned()
    }

    fn profile(login: &str) -> OauthProfile {
        OauthProfile {
            login: login.to_string(),
            name: login.to_string(),
            super_admin: true,
            ..Default::default()
        }
    }

    #[test]
    fn upsert_oauth_user_inserts_then_refreshes_by_login() {
        let db = migrate::open_primary(&temp_db("oauth-upsert")).expect("open db");
        let expired = util_time::now_nanos() + 60 * 24 * 3600 * 1_000_000_000;

        let id = db
            .write(|tx| upsert_oauth_user(tx, &profile("dev"), "token-1", expired))
            .expect("insert");
        assert!(id > 0);

        // 同一 login 再登录：不新增行，令牌与资料被刷新。
        let mut second = profile("dev");
        second.avatar_url = "https://example.test/a.png".into();
        second.email = "dev@example.test".into();
        let id_again = db
            .write(|tx| upsert_oauth_user(tx, &second, "token-2", expired))
            .expect("update");
        assert_eq!(id, id_again);

        let (login, name, token, email) = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT login, name, token, email FROM users WHERE id = ?1",
                    [id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )?)
            })
            .expect("read back");
        assert_eq!(login, "dev");
        assert_eq!(name, "dev");
        assert_eq!(token, "token-2");
        assert_eq!(email, "dev@example.test");

        let count: i64 = db
            .read(|conn| {
                Ok(conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get::<_, i64>(0))?)
            })
            .expect("count");
        assert_eq!(count, 1);

        // 新令牌可经会话查询路径解析出用户。
        let found = db
            .read(|conn| find_by_token(conn, "token-2", util_time::now_nanos()))
            .expect("find")
            .expect("user by new token");
        assert_eq!(found.login, "dev");
        assert!(found.super_admin);
    }
}
