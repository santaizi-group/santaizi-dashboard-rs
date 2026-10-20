use tracing_subscriber::EnvFilter;

/// 与 Go 版一致的日志初始化：`SANTAIZI_LOG` 控制过滤器，默认 info。
pub fn init() {
    let filter = EnvFilter::try_from_env("SANTAIZI_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}
