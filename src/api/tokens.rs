//! API Token 内存索引：对齐 Go `service/singleton` 的 `ApiTokenList`。
//!
//! 启动时把全部未删除 token 的 `token_ciphertext` 解密为明文键（Bearer
//! 直接呈现的值），请求路径只查内存、不落库。增删改由后续 admin 端点
//! 维护同一份索引。

use std::collections::HashMap;
use std::sync::RwLock;

use crate::db::Db;
use crate::util::time as util_time;

/// 一条 API Token 的运行态条目（Go `model.ApiToken` 的鉴权相关子集）。
#[derive(Debug, Clone)]
pub struct ApiTokenEntry {
    pub user_id: i64,
    /// `read` / `write`（Go `NormalizedPermission`，空值按 write）。
    pub permission: String,
    /// 过期时间（unix nanos）；0 = 永不过期。
    pub expires_at_nanos: i64,
    pub enabled: bool,
}

/// 明文 token → 条目。读写锁与 Go `ApiLock` 同角色。
#[derive(Debug, Default)]
pub struct ApiTokenIndex {
    tokens: RwLock<HashMap<String, ApiTokenEntry>>,
}

/// 令牌鉴权结果：命中的用户与权限。
#[derive(Debug, Clone)]
pub struct ApiTokenAuth {
    pub user_id: i64,
    pub permission: String,
}

/// 装载行的中间形态（解密前的原始列）。
type TokenRow = (Vec<u8>, i64, String, Option<String>, bool);

impl ApiTokenIndex {
    /// 从主库装载全部 token；解密失败的行跳过（与 Go 启动装载尽力而为一致）。
    pub fn load(db: &Db) -> anyhow::Result<Self> {
        let rows: Vec<TokenRow> = db.read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT token_ciphertext, user_id, permission, expires_at, enabled \
                 FROM api_tokens WHERE deleted_at IS NULL",
            )?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, i64>(4)? != 0,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })?;
        let mut tokens = HashMap::new();
        for (ciphertext, user_id, permission, expires_at, enabled) in rows {
            let Ok(plain) = crate::secret::decrypt_secret(&ciphertext) else {
                continue;
            };
            let permission = if permission.is_empty() {
                "write".to_string()
            } else {
                permission
            };
            let expires_at_nanos = expires_at
                .as_deref()
                .and_then(util_time::sqlite_to_unix_nanos)
                .unwrap_or(0);
            tokens.insert(
                plain,
                ApiTokenEntry {
                    user_id,
                    permission,
                    expires_at_nanos,
                    enabled,
                },
            );
        }
        Ok(Self {
            tokens: RwLock::new(tokens),
        })
    }

    /// Bearer 校验：启用且未过期才放行（Go `record.IsActive()` + 过期比较）。
    pub fn authenticate(&self, presented: &str, now_nanos: i64) -> Option<ApiTokenAuth> {
        let tokens = self.tokens.read().ok()?;
        let entry = tokens.get(presented)?;
        if !entry.enabled {
            return None;
        }
        if entry.expires_at_nanos != 0 && entry.expires_at_nanos <= now_nanos {
            return None;
        }
        Some(ApiTokenAuth {
            user_id: entry.user_id,
            permission: entry.permission.clone(),
        })
    }

    /// 插入或覆盖（创建 token 时由 admin 端点调用）。
    pub fn insert(&self, plain: String, entry: ApiTokenEntry) {
        if let Ok(mut tokens) = self.tokens.write() {
            tokens.insert(plain, entry);
        }
    }

    /// 按明文移除（删除 / 禁用 token 时调用）。
    pub fn remove(&self, plain: &str) {
        if let Ok(mut tokens) = self.tokens.write() {
            tokens.remove(plain);
        }
    }

    /// 启停热更新（admin PATCH 后调用；索引里没有该明文时静默）。
    pub fn set_enabled(&self, plain: &str, enabled: bool) {
        if let Ok(mut tokens) = self.tokens.write()
            && let Some(entry) = tokens.get_mut(plain)
        {
            entry.enabled = enabled;
        }
    }
}
