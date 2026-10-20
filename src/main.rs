use clap::Parser;
use santaizi_dashboard::config::Config;

/// 三太子面板（Rust）。
#[derive(Debug, Parser)]
#[command(
    name = "santaizi-dashboard",
    version,
    about,
    disable_version_flag = true
)]
struct Cli {
    /// 当前版本号
    #[arg(short = 'v', long = "version", conflicts_with_all = ["config", "db"])]
    show_version: bool,

    /// 配置文件路径
    #[arg(
        short = 'c',
        long = "config",
        default_value = "/opt/santaizi/dashboard-rs/config/dashboard.yaml"
    )]
    config: String,

    /// SQLite 数据库文件路径
    #[arg(
        long = "db",
        default_value = "/opt/santaizi/dashboard-rs/data/sqlite.db"
    )]
    db: String,
}

fn main() {
    let cli = Cli::parse();
    if cli.show_version {
        println!("{}", santaizi_dashboard::VERSION);
        return;
    }

    santaizi_dashboard::util::logging::init();

    let conf = match Config::load(&cli.config) {
        Ok(conf) => conf,
        Err(err) => {
            eprintln!("SANTAIZI>> config: {err:#}");
            std::process::exit(1);
        }
    };

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("santaizi")
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("SANTAIZI>> runtime: {err:#}");
            std::process::exit(1);
        }
    };
    if let Err(err) = rt.block_on(santaizi_dashboard::run(conf, &cli.db)) {
        eprintln!("SANTAIZI>> ERROR: {err:#}");
        std::process::exit(1);
    }
}
