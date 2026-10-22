//! 失败节流：给「一个共享秘密 + 无认证」的公开端点兜底。
//!
//! 查看密码（`site.view_password`）是站点级单值：不按账号区分，也没有
//! 用户名可锁定。按 IP 计数对分布式爆破形同虚设（换 IP 即重置），因此这里
//! 用**全局**桶——秘密只有一个，桶也只需要一个，直接压住尝试速率。

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// 窗口内允许的失败次数。
const MAX_FAILURES: u32 = 5;
/// 计数窗口：超过这个时长的失败记录丢弃（滑动重置）。
const WINDOW: Duration = Duration::from_secs(60);
/// 触发后的锁定时长。
const LOCKOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy)]
struct Bucket {
    failures: u32,
    window_started: Instant,
    blocked_until: Option<Instant>,
}

/// 被节流时返回还需等待的秒数（向上取整，至少 1）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryAfter(pub u64);

impl RetryAfter {
    pub fn seconds(self) -> u64 {
        self.0
    }
}

/// 失败节流器。`Default` 即可用；放进 `ApiShared` 共享。
#[derive(Debug, Default)]
pub struct FailureThrottle {
    buckets: Mutex<HashMap<String, Bucket>>,
}

impl FailureThrottle {
    /// 尝试前调用：被锁定时返回 `RetryAfter`。
    pub fn check(&self, key: &str) -> Result<(), RetryAfter> {
        let now = Instant::now();
        let buckets = self.buckets.lock().unwrap_or_else(|err| err.into_inner());
        let Some(bucket) = buckets.get(key) else {
            return Ok(());
        };
        if let Some(until) = bucket.blocked_until
            && until > now
        {
            let wait = until - now;
            let seconds = wait.as_secs().max(1);
            return Err(RetryAfter(seconds));
        }
        Ok(())
    }

    /// 记录一次失败；返回本次是否刚触发锁定。
    pub fn record_failure(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut buckets = self.buckets.lock().unwrap_or_else(|err| err.into_inner());
        let bucket = buckets.entry(key.to_string()).or_insert(Bucket {
            failures: 0,
            window_started: now,
            blocked_until: None,
        });
        // 窗口过期：重新计数，但保留未到期的锁定。
        if now.duration_since(bucket.window_started) > WINDOW {
            bucket.failures = 0;
            bucket.window_started = now;
        }
        bucket.failures = bucket.failures.saturating_add(1);
        if bucket.failures >= MAX_FAILURES {
            bucket.blocked_until = Some(now + LOCKOUT);
            // 锁定即清窗：解锁后重新给满额度，而不是一解锁就又锁。
            bucket.failures = 0;
            bucket.window_started = now;
            true
        } else {
            false
        }
    }

    /// 成功时清窗（含锁定）。
    pub fn record_success(&self, key: &str) {
        let mut buckets = self.buckets.lock().unwrap_or_else(|err| err.into_inner());
        buckets.remove(key);
    }

    /// 观测用：当前被跟踪的键数量。
    pub fn tracked_keys(&self) -> usize {
        self.buckets
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_after_max_failures_then_recovers() {
        let throttle = FailureThrottle::default();
        let key = "view-password";
        for _ in 0..MAX_FAILURES {
            assert!(throttle.check(key).is_ok(), "未达阈值应放行");
            throttle.record_failure(key);
        }
        let blocked = throttle.check(key).expect_err("达到阈值必须锁");
        assert!(blocked.seconds() >= 1);

        // 锁定期内持续拒绝。
        assert!(throttle.check(key).is_err());

        // 成功后立即清窗（管理员自己输对一次即可解锁）。
        throttle.record_success(key);
        assert!(throttle.check(key).is_ok());
        assert_eq!(throttle.tracked_keys(), 0);
    }

    #[test]
    fn keys_are_isolated() {
        let throttle = FailureThrottle::default();
        for _ in 0..MAX_FAILURES {
            throttle.record_failure("a");
        }
        assert!(throttle.check("a").is_err());
        assert!(throttle.check("b").is_ok(), "不同键互不影响");
    }
}
