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

/// 会话令牌剩余寿命超过 24h 时多端登录复用原令牌并顺延过期时间，避免多设备互踢掉线。
const REUSE_TOKEN_MIN_REMAINING_NANOS: i64 = 86_400 * 1_000_000_000;

/// OAuth2 登录落地：按 `login` upsert 会话令牌，返回用户 ID（兼容旧签名）。
pub fn upsert_oauth_user(
    tx: &rusqlite::Transaction<'_>,
    profile: &OauthProfile,
    token: &str,
    token_expired_nanos: i64,
) -> anyhow::Result<i64> {
    upsert_oauth_user_token(tx, profile, token, token_expired_nanos).map(|(id, _)| id)
}

/// OAuth2 登录落地并返回实际生效的 `(user_id, effective_token)`：
/// 若该管理员已有未过期有效 `token`（`token_expired > now + 86400s`），复用原 `token`
/// 并顺延 `token_expired`，避免电脑与手机登录同一账号互踢掉线。
pub fn upsert_oauth_user_token(
    tx: &rusqlite::Transaction<'_>,
    profile: &OauthProfile,
    token: &str,
    token_expired_nanos: i64,
) -> anyhow::Result<(i64, String)> {
    let now_nanos = util_time::now_nanos();
    let now = util_time::unix_nanos_to_sqlite(now_nanos);
    let existing: Option<(i64, String, Option<String>)> = tx
        .query_row(
            "SELECT id, COALESCE(token, ''), token_expired \
             FROM users WHERE LOWER(login) = LOWER(?1) AND deleted_at IS NULL",
            [profile.login.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    if let Some((id, old_token, old_expired_raw)) = existing {
        let old_expired_nanos = old_expired_raw
            .as_deref()
            .and_then(util_time::sqlite_to_unix_nanos)
            .unwrap_or(0);
        let reuse_old = !old_token.trim().is_empty()
            && old_expired_nanos > now_nanos.saturating_add(REUSE_TOKEN_MIN_REMAINING_NANOS);
        let effective_token = if reuse_old {
            old_token
        } else {
            token.to_string()
        };
        let effective_expired_nanos = if reuse_old {
            token_expired_nanos.max(old_expired_nanos)
        } else {
            token_expired_nanos
        };
        let expired = util_time::unix_nanos_to_sqlite(effective_expired_nanos);
        tx.execute(
            "UPDATE users SET token = ?1, token_expired = ?2, name = ?3, avatar_url = ?4, \
             blog = ?5, email = ?6, bio = ?7, hireable = ?8, super_admin = ?9, updated_at = ?10 \
             WHERE id = ?11",
            rusqlite::params![
                effective_token,
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
        return Ok((id, effective_token));
    }
    let expired = util_time::unix_nanos_to_sqlite(token_expired_nanos);
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
    Ok((tx.last_insert_rowid(), token.to_string()))
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
    fn upsert_oauth_user_reuses_valid_token_and_rotates_expiring_token() {
        let db = migrate::open_primary(&temp_db("oauth-upsert")).expect("open db");
        let now = util_time::now_nanos();
        let expired = now + 60 * 24 * 3600 * 1_000_000_000;

        let (id, tok1) = db
            .write(|tx| upsert_oauth_user_token(tx, &profile("dev"), "token-1", expired))
            .expect("insert");
        assert!(id > 0);
        assert_eq!(tok1, "token-1");

        // 同一 login 且原 token 距过期远超 24h：复用原 token-1 并顺延过期时间，避免多端互踢。
        let mut second = profile("dev");
        second.avatar_url = "https://example.test/a.png".into();
        second.email = "dev@example.test".into();
        let later_expired = expired + 7 * 24 * 3600 * 1_000_000_000;
        let (id_again, tok_reused) = db
            .write(|tx| upsert_oauth_user_token(tx, &second, "token-2", later_expired))
            .expect("update reuse");
        assert_eq!(id, id_again);
        assert_eq!(tok_reused, "token-1");

        let (login, name, token, email, exp_text) = db
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT login, name, token, email, token_expired FROM users WHERE id = ?1",
                    [id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    },
                )?)
            })
            .expect("read back");
        assert_eq!(login, "dev");
        assert_eq!(name, "dev");
        assert_eq!(token, "token-1");
        assert_eq!(email, "dev@example.test");
        assert_eq!(
            util_time::sqlite_to_unix_nanos(&exp_text),
            Some(later_expired)
        );

        // 当现有 token 剩余寿命不足 24h 时再登录：切换为新 token-3。
        let soon = util_time::unix_nanos_to_sqlite(now + 3600 * 1_000_000_000);
        db.write(|tx| {
            tx.execute(
                "UPDATE users SET token_expired = ?1 WHERE id = ?2",
                rusqlite::params![soon, id],
            )?;
            Ok(())
        })
        .unwrap();
        let (_, tok_rotated) = db
            .write(|tx| upsert_oauth_user_token(tx, &second, "token-3", later_expired))
            .expect("rotate expiring");
        assert_eq!(tok_rotated, "token-3");

        let count: i64 = db
            .read(|conn| {
                Ok(conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get::<_, i64>(0))?)
            })
            .expect("count");
        assert_eq!(count, 1);

        let found = db
            .read(|conn| find_by_token(conn, "token-3", util_time::now_nanos()))
            .expect("find")
            .expect("user by rotated token");
        assert_eq!(found.login, "dev");
        assert!(found.super_admin);
    }
}
