# 三太子 Santaizi · 面板（Rust）

三太子监控的 Rust 面板（主力）。同一二进制可作主面板（`mode: primary`）或从端（`mode: collector`），线协议为 `SantaiziService`。

仓库：[`santaizi-group/santaizi-dashboard-rs`](https://github.com/santaizi-group/santaizi-dashboard-rs)。Go 版面板 [`santaizi-dashboard`](https://github.com/santaizi-group/santaizi-dashboard) 已归档，仅安全/阻断性维护。

## 与 Go 版的边界

- 不做内网穿透（NAT）、DDNS、Telegram 机器人。
- 无 `/api/v1/*`、无 Bot 管理面。探针主通路为 V2（[`santaizi-agent`](https://github.com/santaizi-group/santaizi-agent) / [`santaizi-agent-rs`](https://github.com/santaizi-group/santaizi-agent-rs)）；另挂 V1 unary 互通，供仍用 `client_secret` 的旧探针上报。
- 数据库为全新 SQLite，不迁移 Go 版库。schema 与 Go 版 v20 对齐，去掉 bot / ddns / nat 表。
- **proto 权威在本仓** [`proto/santaizi.proto`](./proto/santaizi.proto)。Go / Rust 探针须与之一致；CI 用 [`script/proto-diff.sh`](./script/proto-diff.sh) 防漂移。

## 安装

仅支持 Linux amd64 / arm64。步骤、OAuth2 回调和反向代理见 [部署](docs/deployment.md)。一键安装（交互选择二进制 + systemd 或 Docker）：

```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/santaizi-group/santaizi-dashboard-rs/main/script/install_dashboard.sh)"
```

升级（沿用现有安装形态与配置）：

```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/santaizi-group/santaizi-dashboard-rs/main/script/install_dashboard.sh)" -- --upgrade
```

默认目录布局（程序、配置、数据都在工作目录下一块）：

- 程序：`/opt/santaizi/dashboard-rs/santaizi-dashboard`
- 配置：`/opt/santaizi/dashboard-rs/config/dashboard.yaml`（0600）
- 数据：`/opt/santaizi/dashboard-rs/data`

Docker 形态的 compose 以 `./data`、`./config/dashboard.yaml` 相对挂载；容器内固定为 `/var/lib/santaizi-dashboard` 与 `/etc/santaizi/dashboard.yaml`（宿主端口靠 compose 映射）。

Docker 镜像：`ghcr.io/santaizi-group/santaizi-dashboard-rs`。

## 构建

需要稳定版 Rust（edition 2024）、`protoc`、pnpm 10 与 Node 24。

```bash
pnpm install --frozen-lockfile && pnpm build
cargo build --release
```

前端产物写入 `resource/web/`（已 gitignore）。目录缺失时 `cargo` 无法编译。

## 本地开发

```bash
./script/start-local.sh
```

首次启动把 [`script/config.local.yaml`](./script/config.local.yaml) 写到 `data/dashboard.yaml`，使用 mock OAuth2：

- Web：`http://127.0.0.1:8000`
- 登录：`http://127.0.0.1:8000/oauth2/login`（以 `admin` 进入后台）
- gRPC：`127.0.0.1:5555`
- 数据、密钥与 SQLite：`data/`（已 gitignore）

`--reset-config` 重新生成配置。`HTTP_PORT` / `GRPC_PORT` 覆盖端口。mock OAuth 仅在 `debug: true` 下可用，不能用于生产。

## 运行与配置

```bash
./target/release/santaizi-dashboard -c /opt/santaizi/dashboard-rs/config/dashboard.yaml
```

`-c` 默认 `/opt/santaizi/dashboard-rs/config/dashboard.yaml`，`--db` 默认 `/opt/santaizi/dashboard-rs/data/sqlite.db`。环境变量 `SANTAIZI_*`（下划线换成点）打底，YAML 覆盖。

### 从端（collector）

`mode: collector` 时本进程为从端（观测 / 探测），向主面板注册并复制遥测。安装脚本、TLS、证书自愈与探测型从端的内存边界见 [部署 · 从端](docs/deployment.md#从端)。

探测路由依赖本机 `nexttrace`（`collector.nexttrace_path`）。可选在本机 YAML 或环境变量打开 IP 数据源：

- `collector.nexttrace_ipinfo: true` / `SANTAIZI_COLLECTOR_NEXTTRACE_IPINFO=true` → 每次追踪追加 `-d ipinfo`（默认关闭；`nexttrace-tiny` 等不支持时会报错）。

该开关不进管理后台表单，改完须重启从端进程。

## GeoIP

国家码与探测跳点 ASN 查询开箱即用：

- **内嵌**：发版构建时 [`script/fetch-geoip.sh`](./script/fetch-geoip.sh) 用真库替换 `resource/geoip.db` 占位并编进二进制。源码构建拿到占位 stub 时，走下面的自愈下载。
- **外部指定**：`SANTAIZI_GEOIP_DB=/path/to/geoip.dat`（或 `.mmdb`）。支持 v2ray 系列 `geoip.dat` 与 MaxMind MMDB，按内容识别。
- **自愈下载**：无可用库时按源表从 GitHub release 拉取并校验，缓存到 `--db` 同目录的 `geoip.db`。`SANTAIZI_GEOIP_DOWNLOAD=0` 关闭下载。
- 缺库时国家码与 ASN 为空，上报继续。

## 测试与发版

前端构建之后，本地与 CI 相同：

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
```

tag `vMAJOR.MINOR.PATCH` 发布 linux amd64 / arm64 zip 与 `ghcr.io/santaizi-group/santaizi-dashboard-rs` 多架构镜像。安全问题通过 [`SECURITY.md`](./SECURITY.md) 中的 GitHub 私密公告提交。

## 版权

本项目派生自 [哪吒监控 Nezha Monitoring](https://github.com/naiba/nezha)（Apache-2.0，原作者 naiba）。品牌「三太子 / Santaizi」为再品牌化，不主张原商标。致谢与许可见 [`NOTICE`](./NOTICE) 与 [`LICENSE`](./LICENSE)，二者不得删除或削弱。
