//! 进程级共享状态：配置、连接池、PKI 与内存索引。
//!
//! 对齐 Go `service/singleton`：主端持有双 CA 包、遥测签名器与
//! `client_secret → server_id` 索引；从端不签证书，`pki` 为 `None`。

use std::sync::Arc;

use crate::config::Config;
use crate::db::Db;
use crate::pki::{Bundle, load_or_create as load_pki_bundle};
use crate::repo::servers::SecretIndex;
use crate::secret;
use crate::telemetry::credential::Signer;
use crate::telemetry::store::Store;

pub struct AppState {
    conf: crate::config::ConfigHandle,
    db: Db,
    pki: Option<Bundle>,
    signer: Signer,
    secrets: Arc<SecretIndex>,
    telemetry_store: Arc<Store>,
    /// 数据库维护器（后台接线后回填；HTTP /admin/database 消费）。
    maintainer: std::sync::OnceLock<Arc<crate::telemetry::maintenance::DatabaseMaintainer>>,
    /// 通知引擎（离线/恢复/告警等触发点共用）。
    notifier: Arc<crate::notify::Notifier>,
    /// 监控派发引擎（Control 流 + 调度循环共用）。
    monitor: Arc<crate::monitor::MonitorEngine>,
}

impl AppState {
    /// 主端：加载（或创建）双 CA、遥测签名器、业务秘密密钥与秘密索引。
    pub async fn primary(conf: Config, db: Db) -> crate::Result<Arc<Self>> {
        let pki = load_pki_bundle(&crate::pki::default_dir(&conf.telemetry.data_dir))
            .map_err(|err| anyhow::anyhow!("load pki bundle: {err:#}"))?;
        secret::init_business_secret_encryption(&conf.telemetry.secret_key_path)
            .map_err(|err| anyhow::anyhow!("init business secret encryption: {err:#}"))?;
        let signer = Signer::load_or_create(&conf.telemetry.signing_key_path)
            .map_err(|err| anyhow::anyhow!("load telemetry signer: {err:#}"))?;
        let secrets = Arc::new(SecretIndex::new());
        secrets
            .rebuild(&db)
            .map_err(|err| anyhow::anyhow!("rebuild secret index: {err:#}"))?;
        let telemetry_store = Arc::new(Store::new(conf.telemetry.availability_bucket_seconds));
        let conf_handle = crate::config::new_config_handle(conf);
        let notifier = Arc::new(crate::notify::Notifier::new(
            db.clone(),
            conf_handle.clone(),
        ));
        let monitor = Arc::new(crate::monitor::MonitorEngine::new(
            db.clone(),
            conf_handle.clone(),
            Arc::clone(&notifier),
        ));
        Ok(Arc::new(Self {
            conf: conf_handle,
            db,
            pki: Some(pki),
            signer,
            secrets,
            telemetry_store,
            maintainer: std::sync::OnceLock::new(),
            notifier,
            monitor,
        }))
    }

    /// 监控派发引擎句柄（Control 流注册与调度循环共用）。
    pub fn monitor(&self) -> Arc<crate::monitor::MonitorEngine> {
        Arc::clone(&self.monitor)
    }

    /// 通知引擎句柄（HTTP test 端点与后台触发点共用）。
    pub fn notifier(&self) -> Arc<crate::notify::Notifier> {
        Arc::clone(&self.notifier)
    }

    /// 后台接线时回填维护器句柄（仅 run() 调用一次）。
    pub fn set_maintainer(
        &self,
        maintainer: Arc<crate::telemetry::maintenance::DatabaseMaintainer>,
    ) {
        let _ = self.maintainer.set(maintainer);
    }

    /// 数据库维护器（collector 模式或接线前为 None）。
    pub fn maintainer(&self) -> Option<Arc<crate::telemetry::maintenance::DatabaseMaintainer>> {
        self.maintainer.get().cloned()
    }

    /// 当前配置快照（settings 写入后拿到的是新值）。
    pub fn conf(&self) -> Arc<Config> {
        self.conf.read().expect("config lock").clone()
    }

    /// 与 HTTP 层共享的可变配置句柄。
    pub fn conf_handle(&self) -> crate::config::ConfigHandle {
        Arc::clone(&self.conf)
    }

    /// 原子替换运行时配置（settings PATCH 成功后调用）。
    pub fn update_conf(&self, conf: Config) {
        *self.conf.write().expect("config lock") = Arc::new(conf);
    }

    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn pki(&self) -> Option<&Bundle> {
        self.pki.as_ref()
    }

    pub fn signer(&self) -> &Signer {
        &self.signer
    }

    pub fn secrets(&self) -> &SecretIndex {
        &self.secrets
    }

    /// 共享给 HTTP 层（`ApiShared`）的同一索引句柄。
    pub fn secrets_arc(&self) -> Arc<SecretIndex> {
        Arc::clone(&self.secrets)
    }

    /// 摄入存储：主端 Ingest / 从端复制共用同一实例（STATE 缓冲进程级共享）。
    pub fn telemetry_store(&self) -> &Arc<Store> {
        &self.telemetry_store
    }
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("mode", &self.conf().mode)
            .field("has_pki", &self.pki.is_some())
            .finish_non_exhaustive()
    }
}
