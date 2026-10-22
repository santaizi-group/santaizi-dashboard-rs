//! 数据库维护 worker：对齐 Go `service/telemetry/maintenance.go` 的 `DatabaseMaintainer`。
//!
//! 周期（5min）做：rollup 补卷 → retention drain → 决定 compact（整库 VACUUM）或
//! incremental_vacuum 预算回收 → 超限 WAL 截断。compact 有 6h 冷却与磁盘余量保护；
//! 运行记录 `DatabaseOptimizeRun` 供 HTTP 状态查询（Phase 5）。
//! 与 Go 的差异：策略取启动快照（Go 的 `policyFn` 服务配置热重载，Rust 侧尚未实现）。

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::db::Db;
use crate::telemetry::retention::drain_retention;
use crate::telemetry::rollup::{RetentionPolicy, RollupWorker};
use crate::util::time::{now_nanos, unix_nanos_to_rfc3339};

const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5 * 60);
const INCREMENTAL_PAGES: i64 = 4096;
const INCREMENTAL_MAX_ROUNDS: i64 = 32;
const VACUUM_BUSY_TIMEOUT_MS: i64 = 60_000;
const DEFAULT_BUSY_TIMEOUT_MS: i64 = 5000;
const COMPACT_COOLDOWN: Duration = Duration::from_secs(6 * 60 * 60);
const AUTO_COMPACT_HUGE_FACTOR: i64 = 8;
const WAL_TRUNCATE_THRESHOLD_BYTES: i64 = 256 << 20;

/// 单轮维护记录；时间戳 RFC3339 UTC，与 Go JSON 一致。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DatabaseOptimizeRun {
    pub started_at: String,
    pub ended_at: String,
    pub deleted: i64,
    pub compacted: bool,
    pub skipped: String,
    pub error: String,
}

/// 维护状态：文件尺寸 + 可回收字节 + 是否在跑 + 最近一轮记录。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DatabaseStatus {
    pub file_bytes: i64,
    pub wal_bytes: i64,
    pub reclaimable_bytes: i64,
    pub running: bool,
    pub last_run: Option<DatabaseOptimizeRun>,
}

/// 跨轮状态：运行标记、最近记录、上次 compact 时间（冷却用）。
#[derive(Default)]
struct MaintainerState {
    running: bool,
    last: Option<DatabaseOptimizeRun>,
    last_compact_at: Option<Instant>,
}

/// 一轮维护的全部输入。`Db` 与 `RollupWorker` 均为廉价 Arc 克隆，
/// 使核心逻辑可以在 `spawn_blocking` 的 `'static` 闭包里跑。
struct MaintenanceContext {
    db: Db,
    path: String,
    policy: RetentionPolicy,
    rollup: Option<Arc<RollupWorker>>,
    wal_truncate_bytes: i64,
}

pub struct DatabaseMaintainer {
    ctx: MaintenanceContext,
    state: Mutex<MaintainerState>,
}

impl DatabaseMaintainer {
    pub fn new(
        db: Db,
        path: impl Into<String>,
        policy: RetentionPolicy,
        rollup: Option<Arc<RollupWorker>>,
    ) -> Self {
        Self {
            ctx: MaintenanceContext {
                db,
                path: path.into(),
                policy: policy.normalized(),
                rollup,
                wal_truncate_bytes: WAL_TRUNCATE_THRESHOLD_BYTES,
            },
            state: Mutex::new(MaintainerState::default()),
        }
    }

    /// `Run`：启动即跑一次，随后每 5min 一轮；shutdown 唤醒退出。
    pub async fn run(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        self.execute(false).await;
        let mut ticker = tokio::time::interval(MAINTENANCE_INTERVAL);
        ticker.tick().await; // 首轮已由启动时的一次性执行覆盖
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.execute(false).await;
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        tracing::info!("数据库维护 worker 退出");
                        return;
                    }
                }
            }
        }
    }

    /// `Start`：并发保护的手动触发（HTTP 侧「立即优化」用）。已在跑返回 false。
    pub async fn start(self: &Arc<Self>, compact: bool) -> bool {
        if !self.begin() {
            return false;
        }
        let worker = Arc::clone(self);
        tokio::spawn(async move { worker.execute_locked(compact).await });
        true
    }

    /// `Status`：文件 / WAL 尺寸、可回收字节、运行标记与最近一轮记录。
    pub fn status(&self) -> DatabaseStatus {
        let (file_bytes, wal_bytes) = database_file_sizes(&self.ctx.path);
        let reclaimable = reclaimable_bytes(&self.ctx.db).unwrap_or(0);
        let state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        DatabaseStatus {
            file_bytes,
            wal_bytes,
            reclaimable_bytes: reclaimable,
            running: state.running,
            last_run: state.last.clone(),
        }
    }

    /// `RunOnce`：同步跑一轮（测试 / 一次性调用用）。
    pub fn run_once(&self, compact: bool) -> DatabaseOptimizeRun {
        let last_compact_at = self
            .state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .last_compact_at;
        let outcome = run_maintenance(&self.ctx, compact, last_compact_at);
        let run = outcome.run.clone();
        self.record(outcome);
        run
    }

    async fn execute(&self, compact: bool) {
        if !self.begin() {
            return;
        }
        self.execute_locked(compact).await;
    }

    async fn execute_locked(&self, compact: bool) {
        let ctx = self.ctx_clone();
        let last_compact_at = self
            .state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .last_compact_at;
        let outcome =
            tokio::task::spawn_blocking(move || run_maintenance(&ctx, compact, last_compact_at))
                .await
                .unwrap_or_else(|join_err| MaintenanceOutcome {
                    run: DatabaseOptimizeRun {
                        error: format!("维护任务崩溃: {join_err}"),
                        ..Default::default()
                    },
                    last_compact_at: None,
                });
        self.record(outcome);
    }

    fn record(&self, outcome: MaintenanceOutcome) {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        state.running = false;
        state.last = Some(outcome.run);
        if let Some(at) = outcome.last_compact_at {
            state.last_compact_at = Some(at);
        }
    }

    fn begin(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if state.running {
            return false;
        }
        state.running = true;
        true
    }

    fn ctx_clone(&self) -> MaintenanceContext {
        MaintenanceContext {
            db: self.ctx.db.clone(),
            path: self.ctx.path.clone(),
            policy: self.ctx.policy,
            rollup: self.ctx.rollup.clone(),
            wal_truncate_bytes: self.ctx.wal_truncate_bytes,
        }
    }
}

struct MaintenanceOutcome {
    run: DatabaseOptimizeRun,
    /// 本轮发生 compact 时回传时间戳，供冷却判断。
    last_compact_at: Option<Instant>,
}

/// Go `DatabaseMaintainer.run`：rollup → drain → compact / incremental → WAL 截断。
fn run_maintenance(
    ctx: &MaintenanceContext,
    compact: bool,
    last_compact_at: Option<Instant>,
) -> MaintenanceOutcome {
    let started_nanos = now_nanos();
    let mut result = DatabaseOptimizeRun {
        started_at: unix_nanos_to_rfc3339(started_nanos),
        ..Default::default()
    };
    let policy = ctx.policy;
    if let Some(rollup) = &ctx.rollup
        && sqlite_table_exists(&ctx.db, "telemetry_events")
        && let Err(err) = rollup.rollup_pending(started_nanos)
    {
        tracing::warn!("维护前 rollup 失败: {err:#}");
    }
    match drain_retention(&ctx.db, &policy, started_nanos) {
        Ok(deleted) => result.deleted = deleted,
        Err(err) => {
            result.error = err.to_string();
            result.ended_at = unix_nanos_to_rfc3339(now_nanos());
            return MaintenanceOutcome {
                run: result,
                last_compact_at: None,
            };
        }
    }
    let reclaimable = reclaimable_bytes(&ctx.db).unwrap_or(0);
    let mode = auto_vacuum_mode(&ctx.db).unwrap_or(0);
    let mut want_compact = compact;
    if !want_compact && policy.auto_compact && reclaimable >= policy.compact_min_bytes {
        let legacy = mode != 2;
        let huge = reclaimable >= policy.compact_min_bytes * AUTO_COMPACT_HUGE_FACTOR;
        let cooldown_ok = last_compact_at
            .map(|at| at.elapsed() >= COMPACT_COOLDOWN)
            .unwrap_or(true);
        if (legacy || huge) && cooldown_ok {
            want_compact = true;
        }
    }
    let mut compacted_at = None;
    // 手动 compact 是操作者显式下的令：不能再被 `compact_min_bytes`（默认 64MB）
    // 这道自动阈值吞掉——小库点「优化」会毫无反馈地空转。自动路径仍走原阈值。
    if want_compact && (compact || reclaimable >= policy.compact_min_bytes) {
        let (compacted, skipped, compact_err) = compact_if_possible(ctx);
        result.compacted = compacted;
        result.skipped = skipped;
        if let Err(err) = compact_err {
            result.error = err.to_string();
        }
        if compacted {
            compacted_at = Some(Instant::now());
        }
    } else if let Err(err) =
        incremental_vacuum_budget(&ctx.db, INCREMENTAL_PAGES, INCREMENTAL_MAX_ROUNDS)
    {
        tracing::debug!("incremental vacuum 跳过: {err:#}");
    }
    truncate_oversized_wal(ctx);
    result.ended_at = unix_nanos_to_rfc3339(now_nanos());
    MaintenanceOutcome {
        run: result,
        last_compact_at: compacted_at,
    }
}

/// `compactIfPossible`：磁盘余量不足则跳过（VACUUM 需约等于库文件大小的空闲空间）。
fn compact_if_possible(ctx: &MaintenanceContext) -> (bool, String, anyhow::Result<()>) {
    let (file_bytes, _) = database_file_sizes(&ctx.path);
    if !ctx.path.is_empty()
        && let Some(free) = volume_free_bytes(&ctx.path)
        && file_bytes > 0
        && free < file_bytes as u64
    {
        return (false, "insufficient_disk".into(), Ok(()));
    }
    match compact_database(&ctx.db) {
        Ok(()) => (true, String::new(), Ok(())),
        Err(err) => (false, String::new(), Err(err)),
    }
}

/// `truncateOversizedWAL`：被动 auto-checkpoint 只原地回收帧，不显式 TRUNCATE
/// 的话 -wal 会长期停在高水位；超阈值才截断。
fn truncate_oversized_wal(ctx: &MaintenanceContext) {
    if ctx.path.is_empty() || ctx.wal_truncate_bytes <= 0 {
        return;
    }
    let (_, wal_bytes) = database_file_sizes(&ctx.path);
    if wal_bytes < ctx.wal_truncate_bytes {
        return;
    }
    if let Ok(conn) = ctx.db.get() {
        let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)");
    }
}

fn sqlite_table_exists(db: &Db, table: &str) -> bool {
    db.read(|conn| {
        Ok(conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                rusqlite::params![table],
                |_| Ok(()),
            )
            .is_ok())
    })
    .unwrap_or(false)
}

/// `databaseFileSizes`：主库与 `-wal` 的字节数；任缺省返回 0。
fn database_file_sizes(path: &str) -> (i64, i64) {
    if path.is_empty() {
        return (0, 0);
    }
    let file_bytes = std::fs::metadata(path).map(|m| m.len() as i64).unwrap_or(0);
    let wal_bytes = std::fs::metadata(format!("{path}-wal"))
        .map(|m| m.len() as i64)
        .unwrap_or(0);
    (file_bytes, wal_bytes)
}

/// `reclaimableBytes`：`page_size × freelist_count`，即紧凑可回收的近似字节。
fn reclaimable_bytes(db: &Db) -> anyhow::Result<i64> {
    let page_size = pragma_i64(db, "PRAGMA page_size")?;
    let freelist = pragma_i64(db, "PRAGMA freelist_count")?;
    Ok(page_size * freelist)
}

/// `autoVacuumMode`：0=NONE / 1=FULL / 2=INCREMENTAL。
fn auto_vacuum_mode(db: &Db) -> anyhow::Result<i64> {
    pragma_i64(db, "PRAGMA auto_vacuum")
}

fn pragma_i64(db: &Db, pragma: &str) -> anyhow::Result<i64> {
    db.read(|conn| Ok(conn.query_row(pragma, [], |row| row.get(0))?))
}

/// `incrementalVacuum`：仅 INCREMENTAL 模式有意义；pages<=0 时回收全部空闲页。
fn incremental_vacuum(db: &Db, pages: i64) -> anyhow::Result<()> {
    let mode = auto_vacuum_mode(db)?;
    if mode != 2 {
        return Ok(());
    }
    let conn = db.get()?;
    if pages <= 0 {
        conn.execute_batch("PRAGMA incremental_vacuum")?;
    } else {
        conn.execute_batch(&format!("PRAGMA incremental_vacuum({pages})"))?;
    }
    Ok(())
}

/// `incrementalVacuumBudget`：每轮先看可回收量，归零即停；最多 rounds 轮。
fn incremental_vacuum_budget(db: &Db, pages: i64, rounds: i64) -> anyhow::Result<()> {
    if rounds <= 0 {
        return incremental_vacuum(db, pages);
    }
    for _ in 0..rounds {
        if reclaimable_bytes(db)? == 0 {
            return Ok(());
        }
        incremental_vacuum(db, pages)?;
    }
    Ok(())
}

/// `compactDatabase`：临时放大 busy_timeout → 切 INCREMENTAL → VACUUM → 立即截断 WAL
/// → 还原 busy_timeout。VACUUM 必须跑在事务外的连接上，故走池连接而非 `Db::write`。
fn compact_database(db: &Db) -> anyhow::Result<()> {
    let conn = db.get()?;
    conn.execute_batch(&format!("PRAGMA busy_timeout={VACUUM_BUSY_TIMEOUT_MS}"))?;
    let compacted = (|| {
        conn.execute_batch("PRAGMA auto_vacuum=INCREMENTAL")?;
        conn.execute_batch("VACUUM")?;
        // VACUUM 会把整库流经 WAL，不立刻截断的话 -wal 会停在约等于库大小直到关闭。
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        anyhow::Ok(())
    })();
    // 还原必须排在 compact **之后**：排在前面等于 VACUUM 仍跑在默认 5s
    // busy_timeout 上，放大完全白做（并发写一多就 SQLITE_BUSY 失败）。
    let restored = conn.execute_batch(&format!("PRAGMA busy_timeout={DEFAULT_BUSY_TIMEOUT_MS}"));
    restored?;
    compacted
}

/// `volumeFreeBytes`（Unix）：statfs 的 `f_bavail × f_bsize`。
#[cfg(unix)]
fn volume_free_bytes(path: &str) -> Option<u64> {
    let c_path = std::ffi::CString::new(path).ok()?;
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statfs(c_path.as_ptr(), &mut stat) };
    if rc != 0 {
        return None;
    }
    Some(stat.f_bavail as u64 * stat.f_bsize as u64)
}

#[cfg(not(unix))]
fn volume_free_bytes(_path: &str) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate;

    const NODE: [u8; 16] = [9u8; 16];

    fn db(name: &str) -> (Db, String) {
        let dir = std::env::temp_dir().join(format!(
            "santaizi-maintenance-{name}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("panel.db").to_string_lossy().into_owned();
        (migrate::open_primary(&path).unwrap(), path)
    }

    fn policy(compact_min_bytes: i64) -> RetentionPolicy {
        RetentionPolicy {
            compact_min_bytes,
            max_runtime: Duration::from_secs(60),
            batch_size: 200,
            ..Default::default()
        }
        .normalized()
    }

    fn insert_old_observation(db: &Db, index: u8, received_at: i64) {
        let mut event_id = vec![7u8; 16];
        event_id[0] = index;
        db.write(|tx| {
            tx.execute(
                "INSERT INTO telemetry_observations (event_id, observer_id, node_uuid, received_at, metadata) \
                 VALUES (?1, 'primary', ?2, ?3, ?4)",
                rusqlite::params![event_id, NODE.to_vec(), received_at, vec![0u8; 32768]],
            )?;
            Ok(())
        })
        .unwrap();
    }

    fn page_count(db: &Db) -> i64 {
        pragma_i64(db, "PRAGMA page_count").unwrap()
    }

    /// 手动 compact 不能再被 `compact_min_bytes`（默认 64MB）吞掉：
    /// 操作者显式点了「优化」，小库也必须真的跑，否则毫无反馈地空转。
    #[test]
    fn run_once_compacts_when_manual_even_below_threshold() {
        let (db, path) = db("manual-small");
        let maintainer = DatabaseMaintainer::new(db.clone(), path.clone(), policy(1 << 30), None);
        let run = maintainer.run_once(true);
        assert!(run.compacted, "手动优化不得被自动阈值吞掉：{run:?}");
        assert!(run.error.is_empty(), "{run:?}");
        assert!(!run.started_at.is_empty() && !run.ended_at.is_empty());
    }

    /// 反向对照：自动路径仍然尊重阈值（阈值以下不 compact）。
    #[test]
    fn run_once_skips_auto_compact_below_threshold() {
        let (db, path) = db("auto-small");
        let maintainer = DatabaseMaintainer::new(db, path, policy(1 << 30), None);
        let run = maintainer.run_once(false);
        assert!(!run.compacted, "自动路径在阈值以下不得 compact：{run:?}");
        assert!(run.error.is_empty(), "{run:?}");
        assert!(!run.started_at.is_empty() && !run.ended_at.is_empty());
    }

    #[test]
    fn run_once_compacts_when_reclaimable() {
        let (db, path) = db("compact");
        let old = crate::util::time::now_nanos() - 40 * 24 * 60 * 60 * 1_000_000_000;
        for index in 0..80u8 {
            insert_old_observation(&db, index, old + index as i64);
        }
        let before = page_count(&db);
        let maintainer = DatabaseMaintainer::new(db.clone(), path.clone(), policy(1), None);
        let run = maintainer.run_once(true);
        assert!(run.error.is_empty(), "{run:?}");
        assert!(run.compacted, "{run:?}");
        let after = page_count(&db);
        assert!(after < before, "before={before} after={after}");
        let (_, wal_bytes) = database_file_sizes(&path);
        assert_eq!(wal_bytes, 0);
    }

    #[test]
    fn periodic_auto_compacts_legacy_vacuum() {
        // 新库默认 auto_vacuum=NONE（legacy）：可回收超阈值即自动 compact。
        let (db, path) = db("auto");
        let old = crate::util::time::now_nanos() - 40 * 24 * 60 * 60 * 1_000_000_000;
        for index in 0..80u8 {
            insert_old_observation(&db, index, old + index as i64);
        }
        let mut auto = policy(1);
        auto.auto_compact = true;
        let maintainer = DatabaseMaintainer::new(db, path, auto, None);
        let run = maintainer.run_once(false);
        assert!(run.error.is_empty(), "{run:?}");
        assert!(run.compacted, "{run:?}");
    }

    #[test]
    fn start_rejects_concurrent_run() {
        let (db, path) = db("concurrent");
        let maintainer = DatabaseMaintainer::new(db, path, policy(1 << 30), None);
        assert!(maintainer.begin());
        assert!(!maintainer.begin());
        assert!(maintainer.status().running);
    }

    #[test]
    fn truncates_oversized_wal() {
        let (db, path) = db("wal");
        insert_old_observation(&db, 0, crate::util::time::now_nanos());
        let (_, wal_before) = database_file_sizes(&path);
        assert!(wal_before > 0);
        let mut ctx = MaintenanceContext {
            db,
            path: path.clone(),
            policy: policy(1 << 30),
            rollup: None,
            wal_truncate_bytes: 1,
        };
        truncate_oversized_wal(&ctx);
        let (_, wal_after) = database_file_sizes(&path);
        assert_eq!(wal_after, 0);
        // 阈值未超时不动作。
        ctx.wal_truncate_bytes = 1 << 30;
        insert_old_observation(&ctx.db, 1, crate::util::time::now_nanos());
        truncate_oversized_wal(&ctx);
        let (_, wal_kept) = database_file_sizes(&path);
        assert!(wal_kept > 0, "阈值未超时不应截断");
    }

    #[test]
    fn status_reports_sizes_and_last_run() {
        let (db, path) = db("status");
        let maintainer = DatabaseMaintainer::new(db, path.clone(), policy(1 << 30), None);
        let status = maintainer.status();
        assert!(!status.running);
        assert!(status.last_run.is_none());
        assert!(status.file_bytes > 0, "{status:?}");
        let run = maintainer.run_once(true);
        assert_eq!(maintainer.status().last_run, Some(run));
    }
}
