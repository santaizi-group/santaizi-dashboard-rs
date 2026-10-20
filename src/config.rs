//! 站点配置。键名与校验规则对齐 Go 版 `model/config.go`（koanf 大小写不敏感 +
//! yaml.v3 全小写键；env `SANTAIZI_*` 优先、YAML 覆盖）。不含 Bot 段。

use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

pub const LANGUAGES: [(&str, &str); 4] = [
    ("zh-CN", "简体中文"),
    ("zh-TW", "繁體中文"),
    ("en-US", "English"),
    ("es-ES", "Español"),
];

pub const PUBLIC_THEME_SERVER_STATUS: &str = "server-status";
pub const PUBLIC_THEME_SERVER_GRID: &str = "server-grid";
pub const PUBLIC_THEME_NAZHUA: &str = "nazhua";

/// 公开站主题收敛到内置白名单。
pub fn normalize_public_theme(theme: &str) -> String {
    match theme.trim() {
        PUBLIC_THEME_NAZHUA => PUBLIC_THEME_NAZHUA.to_string(),
        PUBLIC_THEME_SERVER_GRID => PUBLIC_THEME_SERVER_GRID.to_string(),
        _ => PUBLIC_THEME_SERVER_STATUS.to_string(),
    }
}

pub const OAUTH2_GITHUB: &str = "github";
pub const OAUTH2_GITEE: &str = "gitee";
pub const OAUTH2_GITLAB: &str = "gitlab";
pub const OAUTH2_JIHULAB: &str = "jihulab";
pub const OAUTH2_GITEA: &str = "gitea";
pub const OAUTH2_CLOUDFLARE: &str = "cloudflare";
pub const OAUTH2_OIDC: &str = "oidc";
pub const OAUTH2_MOCK: &str = "mock";

pub const COVER_ALL: u8 = 0;
pub const COVER_IGNORE_ALL: u8 = 1;

/// 运行时可变配置句柄：读多写少，读侧拿当前 `Arc<Config>` 快照
/// （Go `*singleton.Conf` 的进程内可变等价物）。
pub type ConfigHandle = std::sync::Arc<std::sync::RwLock<std::sync::Arc<Config>>>;

pub fn new_config_handle(conf: Config) -> ConfigHandle {
    std::sync::Arc::new(std::sync::RwLock::new(std::sync::Arc::new(conf)))
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
#[serde(rename_all = "lowercase")]
pub struct SiteConfig {
    #[serde(rename = "brand")]
    pub brand: String,
    #[serde(rename = "cookiename")]
    pub cookie_name: String,
    pub theme: String,
    #[serde(rename = "dashboardtheme")]
    pub dashboard_theme: String,
    pub customcode: String,
    #[serde(rename = "customcodedashboard")]
    pub custom_code_dashboard: String,
    #[serde(rename = "viewpassword")]
    pub view_password: String,
    #[serde(rename = "primarycolor")]
    pub primary_color: String,
    #[serde(rename = "footertext")]
    pub footer_text: String,
    #[serde(rename = "logourl")]
    pub logo_url: String,
    #[serde(rename = "safecustomcss")]
    pub safe_custom_css: String,
    #[serde(rename = "primarylocation")]
    pub primary_location: String,
}

/// 一键安装脚本源。键名对齐 Go 的 koanf/yaml tag 混合形态。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct InstallScriptConfig {
    pub linux: String,
    pub linuxen: String,
    pub windows: String,
    pub macos: String,
    pub collector: String,
    pub dashboard: String,
    #[serde(rename = "linux_rust")]
    pub linux_rust: String,
    pub upgradecollector: String,
    #[serde(rename = "upgrade_linux")]
    pub upgrade_linux: String,
    #[serde(rename = "upgrade_linuxen")]
    pub upgrade_linux_en: String,
    #[serde(rename = "upgrade_windows")]
    pub upgrade_windows: String,
    #[serde(rename = "upgrade_macos")]
    pub upgrade_macos: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "snake_case")]
pub struct TelemetryConfig {
    pub data_dir: String,
    pub signing_key_path: String,
    pub secret_key_path: String,
    pub primary_endpoint: String,
    pub state_interval_seconds: u64,
    pub heartbeat_interval_seconds: u64,
    pub offline_threshold_seconds: u64,
    pub ingest_batch_size: usize,
    pub ingest_queue_size: usize,
    pub sequence_hole_grace_seconds: i64,
    pub credential_validity_days: u64,
    pub credential_refresh_days: u64,
    pub credential_grace_days: u64,
    pub availability_bucket_seconds: u64,
    pub min_observers: u32,
    pub enable_connectivity_notification: bool,
    pub enable_correction_notification: bool,
    pub enable_collector_offline_notification: bool,
    pub enable_collector_online_notification: bool,
    pub enable_data_loss_notification: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "snake_case")]
pub struct CollectorModeConfig {
    pub primary_endpoint: String,
    pub primary_tls: bool,
    pub primary_insecure_tls: bool,
    pub registration_token: String,
    pub database_path: String,
    pub spool_max_bytes: u64,
    pub spool_max_age_days: u64,
    pub status_authorization: String,
    pub nexttrace_path: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "snake_case")]
pub struct GrpcTlsConfig {
    pub enabled: bool,
    pub cert_file: String,
    pub key_file: String,
    pub client_ca_file: String,
    pub require_agent_mtls: bool,
    pub require_collector_mtls: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "snake_case")]
pub struct RollupConfig {
    pub enabled: bool,
    pub batch_size: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "snake_case")]
pub struct RetentionConfig {
    pub state_raw_hours: u64,
    pub state_one_minute_days: u64,
    pub state_one_hour_days: u64,
    pub observation_days: u64,
    pub evidence_hours: u64,
    pub lifecycle_days: u64,
    pub batch_size: usize,
    pub max_runtime_ms: u64,
    pub receipt_days: u64,
    pub compact_min_bytes: i64,
    pub auto_compact: Option<bool>,
}

impl RetentionConfig {
    pub fn auto_compact(&self) -> bool {
        self.auto_compact.unwrap_or(true)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "snake_case")]
pub struct WebConfig {
    pub delivery: String,
    pub static_dir: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "lowercase")]
pub struct Oauth2Config {
    #[serde(rename = "type")]
    pub type_: String,
    pub admin: String,
    pub admingroups: String,
    pub clientid: String,
    pub clientsecret: String,
    pub endpoint: String,
    pub oidcdisplayname: String,
    pub oidcissuer: String,
    pub oidclogouturl: String,
    pub oidcregisterurl: String,
    pub oidcloginclaim: String,
    pub oidcgroupclaim: String,
    pub oidcscopes: String,
    pub oidcautocreate: bool,
    pub oidcautologin: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub debug: bool,
    pub language: String,
    pub mode: String,
    #[serde(default)]
    pub site: SiteConfig,
    #[serde(default, rename = "installscript")]
    pub install_script: InstallScriptConfig,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    #[serde(default)]
    pub collector: CollectorModeConfig,
    #[serde(default, rename = "grpc_tls")]
    pub grpc_tls: GrpcTlsConfig,
    #[serde(default)]
    pub rollup: RollupConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
    #[serde(default)]
    pub web: WebConfig,
    #[serde(default)]
    pub oauth2: Oauth2Config,
    #[serde(rename = "httpport")]
    pub http_port: u16,
    #[serde(rename = "grpcport")]
    pub grpc_port: u16,
    #[serde(rename = "grpchost")]
    pub grpc_host: String,
    #[serde(rename = "proxygrpcport")]
    pub proxy_grpc_port: u16,
    pub tls: bool,

    #[serde(rename = "enableplainipinnotification")]
    pub enable_plain_ip_in_notification: bool,
    #[serde(rename = "disableswitchtemplateinfrontend")]
    pub disable_switch_template_in_frontend: bool,

    #[serde(rename = "enableipchangenotification")]
    pub enable_ip_change_notification: bool,
    #[serde(rename = "ipchangenotificationtag")]
    pub ip_change_notification_tag: String,
    pub cover: u8,
    #[serde(rename = "ignoredipnotification")]
    pub ignored_ip_notification: String,

    pub location: String,

    #[serde(skip)]
    pub ignored_ip_notification_server_ids: HashSet<u64>,
    #[serde(rename = "maxtcppingvalue")]
    pub max_tcp_ping_value: i32,
    #[serde(rename = "avgpingcount")]
    pub avg_ping_count: i32,
    #[serde(rename = "dnsservers")]
    pub dns_servers: String,

    #[serde(rename = "enableofflinehistory")]
    pub enable_offline_history: bool,
    #[serde(rename = "offlinethresholdseconds")]
    pub offline_threshold_seconds: u64,
    #[serde(rename = "offlinecheckintervalseconds")]
    pub offline_check_interval_seconds: u64,
    #[serde(rename = "offlinemergegapseconds")]
    pub offline_merge_gap_seconds: u64,
    #[serde(rename = "offlinehistoryretentiondays")]
    pub offline_history_retention_days: u64,
    #[serde(rename = "enableofflinenotification")]
    pub enable_offline_notification: bool,
    #[serde(rename = "enablerecoverynotification")]
    pub enable_recovery_notification: bool,
    #[serde(rename = "showavailabilitytoguest")]
    pub show_availability_to_guest: bool,

    #[serde(skip)]
    pub(crate) file_path: String,
}

impl Default for Config {
    fn default() -> Self {
        let mut conf = Self {
            debug: false,
            language: String::new(),
            mode: String::new(),
            site: SiteConfig::default(),
            install_script: InstallScriptConfig::default(),
            telemetry: TelemetryConfig::default(),
            collector: CollectorModeConfig::default(),
            grpc_tls: GrpcTlsConfig::default(),
            rollup: RollupConfig {
                enabled: false,
                batch_size: 0,
            },
            retention: RetentionConfig::default(),
            web: WebConfig {
                delivery: String::new(),
                static_dir: String::new(),
            },
            oauth2: Oauth2Config::default(),
            http_port: 0,
            grpc_port: 0,
            grpc_host: String::new(),
            proxy_grpc_port: 0,
            tls: false,
            enable_plain_ip_in_notification: false,
            disable_switch_template_in_frontend: false,
            enable_ip_change_notification: false,
            ip_change_notification_tag: String::new(),
            cover: 0,
            ignored_ip_notification: String::new(),
            location: String::new(),
            ignored_ip_notification_server_ids: HashSet::new(),
            max_tcp_ping_value: 0,
            avg_ping_count: 0,
            dns_servers: String::new(),
            enable_offline_history: false,
            offline_threshold_seconds: 0,
            offline_check_interval_seconds: 0,
            offline_merge_gap_seconds: 0,
            offline_history_retention_days: 0,
            enable_offline_notification: false,
            enable_recovery_notification: false,
            show_availability_to_guest: false,
            file_path: String::new(),
        };
        conf.apply_defaults();
        conf
    }
}

impl Config {
    /// 读取配置：env `SANTAIZI_*` 打底，YAML 覆盖，随后补默认值并校验。
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let env_tree = env_tree();
        let file_tree = if Path::new(path).exists() {
            let text = std::fs::read_to_string(path)
                .map_err(|err| anyhow::anyhow!("read config {path}: {err}"))?;
            let value: serde_yaml::Value = serde_yaml::from_str(&text)
                .map_err(|err| anyhow::anyhow!("parse config {path}: {err}"))?;
            yaml_to_json(value)
        } else {
            serde_json::Value::Object(serde_json::Map::new())
        };
        let merged = merge(file_tree, env_tree);
        let normalized = lowercase_keys(merged);
        let mut conf: Config = serde_json::from_value(normalized)
            .map_err(|err| anyhow::anyhow!("decode config: {err}"))?;
        conf.file_path = path.to_string();
        conf.apply_defaults();
        conf.validate()?;
        Ok(conf)
    }

    pub fn apply_defaults(&mut self) {
        if self.mode.is_empty() {
            self.mode = "primary".into();
        }
        if self.site.brand.is_empty() {
            self.site.brand = "Santaizi Monitoring".into();
        }
        if self.site.cookie_name.is_empty() {
            self.site.cookie_name = "santaizi-dashboard".into();
        }
        if self.site.theme.is_empty() || self.site.theme == "default" {
            self.site.theme = PUBLIC_THEME_SERVER_STATUS.into();
        } else {
            self.site.theme = normalize_public_theme(&self.site.theme);
        }
        if self.site.dashboard_theme.is_empty() || self.site.dashboard_theme == "default" {
            self.site.dashboard_theme = "spa".into();
        }
        if self.site.primary_color.is_empty() {
            self.site.primary_color = "#2563eb".into();
        }
        if self.site.logo_url.is_empty() {
            self.site.logo_url = "/static/logo.svg".into();
        }
        if self.web.delivery.is_empty() {
            self.web.delivery = "embedded".into();
        }
        if self.language.is_empty() {
            self.language = "zh-CN".into();
        }
        if self.http_port == 0 {
            self.http_port = 80;
        }
        if self.grpc_port == 0 {
            self.grpc_port = 5555;
        }
        if self.telemetry.data_dir.is_empty() {
            self.telemetry.data_dir = "/var/lib/santaizi-dashboard".into();
        }
        if self.telemetry.signing_key_path.is_empty() {
            self.telemetry.signing_key_path =
                join(&self.telemetry.data_dir, "telemetry-signing.key");
        }
        if self.telemetry.secret_key_path.is_empty() {
            self.telemetry.secret_key_path = join(&self.telemetry.data_dir, "business-secrets.key");
        }
        if self.telemetry.primary_endpoint.is_empty() {
            self.telemetry.primary_endpoint = self.grpc_host.clone();
        }
        let t = &mut self.telemetry;
        if t.state_interval_seconds == 0 {
            t.state_interval_seconds = 5;
        }
        if t.heartbeat_interval_seconds == 0 {
            t.heartbeat_interval_seconds = 10;
        }
        if t.offline_threshold_seconds == 0 {
            t.offline_threshold_seconds = 30;
        }
        if t.ingest_batch_size == 0 {
            t.ingest_batch_size = 256;
        }
        if t.ingest_queue_size == 0 {
            t.ingest_queue_size = 4096;
        }
        if t.credential_validity_days == 0 {
            t.credential_validity_days = 30;
        }
        if t.credential_refresh_days == 0 {
            t.credential_refresh_days = 7;
        }
        if t.credential_grace_days == 0 {
            t.credential_grace_days = 7;
        }
        if t.availability_bucket_seconds == 0 {
            t.availability_bucket_seconds = 30;
        }
        if t.min_observers == 0 {
            t.min_observers = 1;
        }
        if self.collector.database_path.is_empty() {
            self.collector.database_path = join(&self.telemetry.data_dir, "collector.db");
        }
        if self.collector.spool_max_bytes == 0 {
            self.collector.spool_max_bytes = 5 << 30;
        }
        if self.collector.spool_max_age_days == 0 {
            self.collector.spool_max_age_days = 30;
        }
        if !self.rollup.enabled {
            self.rollup.enabled = true;
        }
        if self.rollup.batch_size == 0 {
            self.rollup.batch_size = 1000;
        }
        let r = &mut self.retention;
        if r.state_raw_hours == 0 {
            r.state_raw_hours = 6;
        }
        if r.state_one_minute_days == 0 {
            r.state_one_minute_days = 30;
        }
        if r.state_one_hour_days == 0 {
            r.state_one_hour_days = 365;
        }
        if r.observation_days == 0 {
            r.observation_days = 30;
        }
        if r.evidence_hours == 0 {
            r.evidence_hours = 48;
        }
        if r.lifecycle_days == 0 {
            r.lifecycle_days = 3650;
        }
        if r.batch_size == 0 {
            r.batch_size = 5000;
        }
        if r.max_runtime_ms == 0 {
            r.max_runtime_ms = 20_000;
        }
        if r.receipt_days == 0 {
            r.receipt_days = 7;
        }
        if r.compact_min_bytes == 0 {
            r.compact_min_bytes = 64 << 20;
        }
        if self.enable_ip_change_notification && self.ip_change_notification_tag.is_empty() {
            self.ip_change_notification_tag = "default".into();
        }
        if self.location.is_empty() {
            self.location = "Asia/Shanghai".into();
        }
        if self.max_tcp_ping_value == 0 {
            self.max_tcp_ping_value = 1000;
        }
        if self.avg_ping_count == 0 {
            self.avg_ping_count = 2;
        }
        if self.install_script.linux.is_empty() {
            self.install_script.linux = "https://raw.githubusercontent.com/santaizi-group/santanzi-dashboard/main/script/install_agent.sh".into();
        }
        if self.install_script.linux_rust.is_empty() {
            self.install_script.linux_rust = "https://raw.githubusercontent.com/santaizi-group/santanzi-dashboard/main/script/install_agent_rs.sh".into();
        }
        if self.install_script.linuxen.is_empty() {
            self.install_script.linuxen = "https://raw.githubusercontent.com/santaizi-group/santanzi-dashboard/main/script/install_agent_en.sh".into();
        }
        if self.install_script.windows.is_empty() {
            self.install_script.windows = "https://raw.githubusercontent.com/santaizi-group/santanzi-dashboard/main/script/install.ps1".into();
        }
        if self.install_script.macos.is_empty() {
            self.install_script.macos = "https://raw.githubusercontent.com/santaizi-group/santanzi-dashboard/main/script/install.command".into();
        }
        if self.install_script.collector.is_empty() {
            self.install_script.collector = "https://raw.githubusercontent.com/santaizi-group/santanzi-dashboard/main/script/install_collector.sh".into();
        }
        if self.install_script.dashboard.is_empty() {
            self.install_script.dashboard = "https://raw.githubusercontent.com/santaizi-group/santanzi-dashboard/main/script/install_dashboard.sh".into();
        }
        if self.install_script.upgradecollector.is_empty() {
            self.install_script.upgradecollector = "https://raw.githubusercontent.com/santaizi-group/santanzi-dashboard/main/script/upgrade_collector.sh".into();
        }
        if self.install_script.upgrade_linux.is_empty() {
            self.install_script.upgrade_linux = "https://raw.githubusercontent.com/santaizi-group/santanzi-dashboard/main/script/upgrade_agent.sh".into();
        }
        if self.install_script.upgrade_linux_en.is_empty() {
            self.install_script.upgrade_linux_en = "https://raw.githubusercontent.com/santaizi-group/santanzi-dashboard/main/script/upgrade_agent_en.sh".into();
        }
        if self.install_script.upgrade_windows.is_empty() {
            self.install_script.upgrade_windows = "https://raw.githubusercontent.com/santaizi-group/santanzi-dashboard/main/script/upgrade.ps1".into();
        }
        if self.install_script.upgrade_macos.is_empty() {
            self.install_script.upgrade_macos = "https://raw.githubusercontent.com/santaizi-group/santanzi-dashboard/main/script/upgrade.command".into();
        }
        if self.oauth2.oidcscopes.is_empty() {
            self.oauth2.oidcscopes = "openid,profile,email".into();
        }
        if self.oauth2.oidcloginclaim.is_empty() {
            self.oauth2.oidcloginclaim = "sub".into();
        }
        if self.oauth2.oidcdisplayname.is_empty() {
            self.oauth2.oidcdisplayname = "OIDC".into();
        }
        if self.oauth2.oidcgroupclaim.is_empty() {
            self.oauth2.oidcgroupclaim = "groups".into();
        }

        // 离线历史边界（对齐 NormalizeOfflineConfig）
        if self.offline_threshold_seconds == 0 {
            self.offline_threshold_seconds = 30;
        }
        if self.offline_threshold_seconds < 10 {
            self.offline_threshold_seconds = 10;
        }
        if self.offline_check_interval_seconds == 0 {
            self.offline_check_interval_seconds = 10;
        }
        if self.offline_check_interval_seconds < 5 {
            self.offline_check_interval_seconds = 5;
        }
        if self.offline_check_interval_seconds > self.offline_threshold_seconds {
            self.offline_check_interval_seconds = self.offline_threshold_seconds;
        }
        if self.offline_merge_gap_seconds == 0 {
            self.offline_merge_gap_seconds = 10;
        }
        if self.offline_merge_gap_seconds > 3600 {
            self.offline_merge_gap_seconds = 3600;
        }
        if self.offline_history_retention_days == 0 {
            self.offline_history_retention_days = 365;
        }
        if self.offline_history_retention_days < 1 {
            self.offline_history_retention_days = 1;
        }

        self.update_ignored_ip_notification_ids();
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.mode != "primary" && self.mode != "collector" {
            anyhow::bail!("unsupported mode {:?}", self.mode);
        }
        if self.mode == "primary" && (self.oauth2.type_.is_empty() || self.oauth2.admin.is_empty())
        {
            anyhow::bail!("missing oauth2 config");
        }
        if self.mode == "primary" && self.oauth2.type_ == OAUTH2_MOCK && !self.debug {
            anyhow::bail!("mock oauth2 can only be used in debug mode");
        }
        if self.mode == "primary"
            && self.oauth2.type_ != OAUTH2_MOCK
            && (self.oauth2.clientid.is_empty() || self.oauth2.clientsecret.is_empty())
        {
            anyhow::bail!("missing oauth2 config");
        }
        if self.mode == "collector" && self.collector.primary_endpoint.is_empty() {
            anyhow::bail!("collector mode requires collector.primary_endpoint");
        }
        if self.mode == "collector" && self.collector.registration_token.is_empty() {
            anyhow::bail!("collector mode requires collector.registration_token");
        }
        if self.web.delivery != "embedded" && self.web.delivery != "external" {
            anyhow::bail!("unsupported web delivery {:?}", self.web.delivery);
        }
        if self.grpc_tls.require_agent_mtls && !self.grpc_tls.enabled {
            anyhow::bail!("grpc_tls.require_agent_mtls requires grpc_tls.enabled");
        }
        if self.grpc_tls.require_collector_mtls && !self.grpc_tls.enabled {
            anyhow::bail!("grpc_tls.require_collector_mtls requires grpc_tls.enabled");
        }
        if self.grpc_tls.enabled
            && (self.grpc_tls.cert_file.is_empty() || self.grpc_tls.key_file.is_empty())
        {
            anyhow::bail!("grpc_tls.enabled requires cert_file and key_file");
        }
        Ok(())
    }

    fn update_ignored_ip_notification_ids(&mut self) {
        self.ignored_ip_notification_server_ids = self
            .ignored_ip_notification
            .split(',')
            .filter_map(|part| part.trim().parse::<u64>().ok())
            .filter(|id| *id > 0)
            .collect();
    }

    /// 运行时外部 GeoIP 库路径（env `SANTAIZI_GEOIP_DB`，非 YAML 配置项）。
    pub fn geoip_db_path(&self) -> Option<String> {
        std::env::var("SANTAIZI_GEOIP_DB")
            .ok()
            .filter(|value| !value.trim().is_empty())
    }

    /// 站点展示名（供公开站/管理后台共用）。
    pub fn brand(&self) -> &str {
        &self.site.brand
    }

    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// 注入配置文件路径（`load` 之外的场景：测试 / 嵌入式构造）。
    pub fn set_file_path(&mut self, path: &str) {
        self.file_path = path.to_string();
    }

    /// 全量写回 yaml 配置文件（Go `Conf.Save`：0600）。
    pub fn save(&self) -> anyhow::Result<()> {
        if self.file_path.is_empty() {
            anyhow::bail!("config file path is empty");
        }
        let text = serde_yaml::to_string(self)?;
        #[cfg(unix)]
        {
            use std::io::Write as _;
            use std::os::unix::fs::OpenOptionsExt as _;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&self.file_path)?;
            file.write_all(text.as_bytes())?;
        }
        #[cfg(not(unix))]
        std::fs::write(&self.file_path, text.as_bytes())?;
        Ok(())
    }
}

fn join(dir: &str, name: &str) -> String {
    let trimmed = dir.trim_end_matches('/');
    if trimmed.is_empty() {
        name.to_string()
    } else {
        format!("{trimmed}/{name}")
    }
}

/// env `SANTAIZI_*` → JSON 树。键转换对齐 Go `configEnvKey`：
/// 命中段前缀（grpc_tls/telemetry/collector/rollup/retention/web/site/oauth2/installscript）
/// 转为 `段.其余`，其余下划线替换为点。值按 YAML 标量解析以获得类型。
fn env_tree() -> serde_json::Value {
    env_tree_from(std::env::vars())
}

fn env_tree_from<I: Iterator<Item = (String, String)>>(vars: I) -> serde_json::Value {
    const SECTIONS: [&str; 9] = [
        "grpc_tls",
        "telemetry",
        "collector",
        "rollup",
        "retention",
        "web",
        "site",
        "oauth2",
        "installscript",
    ];
    let mut root = serde_json::Map::new();
    for (key, value) in vars {
        let Some(rest) = key.strip_prefix("SANTAIZI_") else {
            continue;
        };
        if rest.is_empty() {
            continue;
        }
        let lower = rest.to_lowercase();
        let dotted = if let Some(section) = SECTIONS
            .iter()
            .find(|section| lower.starts_with(&format!("{section}_")))
        {
            format!("{section}.{}", &lower[section.len() + 1..])
        } else {
            lower.replace('_', ".")
        };
        let scalar: serde_yaml::Value = match serde_yaml::from_str(&value) {
            Ok(parsed) => parsed,
            Err(_) => serde_yaml::Value::String(value.clone()),
        };
        insert_dotted(&mut root, &dotted, yaml_to_json(scalar));
    }
    serde_json::Value::Object(root)
}

fn insert_dotted(
    root: &mut serde_json::Map<String, serde_json::Value>,
    dotted: &str,
    value: serde_json::Value,
) {
    let mut parts: Vec<&str> = dotted.split('.').collect();
    let Some(last) = parts.pop() else { return };
    let mut current = root;
    for part in parts {
        let entry = current
            .entry(part.to_string())
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        if !entry.is_object() {
            *entry = serde_json::Value::Object(serde_json::Map::new());
        }
        current = entry.as_object_mut().expect("object after ensure");
    }
    current.insert(last.to_string(), value);
}

/// YAML → JSON。YAML mapping 保留为 JSON object。
fn yaml_to_json(value: serde_yaml::Value) -> serde_json::Value {
    match value {
        serde_yaml::Value::Null => serde_json::Value::Null,
        serde_yaml::Value::Bool(b) => serde_json::Value::Bool(b),
        serde_yaml::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_json::Value::Number(i.into())
            } else if let Some(u) = n.as_u64() {
                serde_json::Value::Number(u.into())
            } else {
                serde_json::Value::Number(
                    serde_json::Number::from_f64(n.as_f64().unwrap_or(0.0))
                        .unwrap_or_else(|| 0.into()),
                )
            }
        }
        serde_yaml::Value::String(s) => serde_json::Value::String(s),
        serde_yaml::Value::Sequence(seq) => {
            serde_json::Value::Array(seq.into_iter().map(yaml_to_json).collect())
        }
        serde_yaml::Value::Mapping(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                let key = match k {
                    serde_yaml::Value::String(s) => s,
                    serde_yaml::Value::Bool(b) => b.to_string(),
                    serde_yaml::Value::Number(n) => n.to_string(),
                    _ => continue,
                };
                out.insert(key, yaml_to_json(v));
            }
            serde_json::Value::Object(out)
        }
        serde_yaml::Value::Tagged(tagged) => yaml_to_json(tagged.value),
    }
}

/// 深合并：file 覆盖 env。
fn merge(file: serde_json::Value, env: serde_json::Value) -> serde_json::Value {
    match (file, env) {
        (serde_json::Value::Object(mut f), serde_json::Value::Object(e)) => {
            for (key, env_value) in e {
                match f.remove(&key) {
                    Some(file_value) => {
                        f.insert(key, merge(file_value, env_value));
                    }
                    None => {
                        f.insert(key, env_value);
                    }
                }
            }
            serde_json::Value::Object(f)
        }
        (file, _) => file,
    }
}

/// 所有 mapping 键转小写——复现 koanf/mapstructure 的大小写不敏感匹配。
fn lowercase_keys(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::new();
            let mut sorted: BTreeMap<String, serde_json::Value> = BTreeMap::new();
            for (k, v) in map {
                sorted.insert(k.to_lowercase(), lowercase_keys(v));
            }
            for (k, v) in sorted {
                out.insert(k, v);
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(lowercase_keys).collect())
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_for_empty_file() {
        let dir = std::env::temp_dir().join(format!("santaizi-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dashboard.yaml");
        // primary 模式必须配 oauth2，否则校验失败；这里只验证默认值逻辑。
        std::fs::write(
            &path,
            "mode: primary\noauth2:\n  type: mock\n  admin: u1\ndebug: true\n",
        )
        .unwrap();
        let conf = Config::load(path.to_str().unwrap()).unwrap();
        assert_eq!(conf.mode, "primary");
        assert_eq!(conf.http_port, 80);
        assert_eq!(conf.grpc_port, 5555);
        assert_eq!(conf.site.brand, "Santaizi Monitoring");
        assert_eq!(conf.site.cookie_name, "santaizi-dashboard");
        assert_eq!(conf.site.theme, "server-status");
        assert_eq!(conf.telemetry.availability_bucket_seconds, 30);
        assert_eq!(conf.telemetry.ingest_batch_size, 256);
        assert_eq!(conf.retention.evidence_hours, 48);
        assert!(conf.retention.auto_compact());
        assert_eq!(conf.offline_history_retention_days, 365);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn normalize_public_theme_whitelist() {
        assert_eq!(normalize_public_theme("server-status"), "server-status");
        assert_eq!(normalize_public_theme("server-grid"), "server-grid");
        assert_eq!(normalize_public_theme(" nazhua "), "nazhua");
        assert_eq!(normalize_public_theme("unknown"), "server-status");
        assert_eq!(normalize_public_theme(""), "server-status");
    }

    #[test]
    fn env_key_transform() {
        let vars = vec![
            ("SANTAIZI_HTTPPORT".to_string(), "8080".to_string()),
            ("SANTAIZI_SITE_BRAND".to_string(), "EnvBrand".to_string()),
            (
                "SANTAIZI_TELEMETRY_AVAILABILITY_BUCKET_SECONDS".to_string(),
                "15".to_string(),
            ),
            (
                "SANTAIZI_INSTALLSCRIPT_LINUX_RUST".to_string(),
                "https://x/y.sh".to_string(),
            ),
            (
                "SANTAIZI_GRPC_TLS_REQUIRE_AGENT_MTLS".to_string(),
                "true".to_string(),
            ),
            ("OTHER_VAR".to_string(), "ignored".to_string()),
        ];
        let tree = env_tree_from(vars.into_iter());
        assert_eq!(tree["httpport"], serde_json::json!(8080));
        assert_eq!(tree["site"]["brand"], serde_json::json!("EnvBrand"));
        assert_eq!(
            tree["telemetry"]["availability_bucket_seconds"],
            serde_json::json!(15)
        );
        assert_eq!(
            tree["installscript"]["linux_rust"],
            serde_json::json!("https://x/y.sh")
        );
        assert_eq!(
            tree["grpc_tls"]["require_agent_mtls"],
            serde_json::json!(true)
        );
        assert!(tree.get("other").is_none());
    }

    #[test]
    fn merge_file_wins_and_keeps_env_siblings() {
        let file = serde_json::json!({"site": {"brand": "File"}, "mode": "primary"});
        let env =
            serde_json::json!({"site": {"brand": "Env", "cookiename": "ck"}, "httpport": 8080});
        let merged = merge(file, env);
        assert_eq!(merged["site"]["brand"], serde_json::json!("File"));
        assert_eq!(merged["site"]["cookiename"], serde_json::json!("ck"));
        assert_eq!(merged["httpport"], serde_json::json!(8080));
    }

    #[test]
    fn rejects_bad_mode_and_mock_without_debug() {
        let dir = std::env::temp_dir().join(format!("santaizi-cfg-bad-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dashboard.yaml");
        std::fs::write(&path, "mode: wat\noauth2:\n  type: mock\n  admin: u1\n").unwrap();
        assert!(Config::load(path.to_str().unwrap()).is_err());
        std::fs::write(&path, "mode: primary\noauth2:\n  type: mock\n  admin: u1\n").unwrap();
        assert!(Config::load(path.to_str().unwrap()).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
