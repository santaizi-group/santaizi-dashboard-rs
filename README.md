# 三太子 Santaizi · 面板（Rust）

`Santaizi Dashboard（Rust）` 是 Go 版面板（[santanzi-dashboard](https://github.com/santaizi-group/santanzi-dashboard)）的 Rust 重写：主面板（primary）与从端（collector）双模式，同一套 `SantaiziService` 线协议，面向高性能探测摄入。

**当前状态：v0.1.0，Phase 0–7 完成（存储 / PKI / gRPC 双模式 / HTTP 全套 / 通知 / 监控派发 / 流量评估 / 前端内嵌），真机 smoke 通过；提交与发版等拍板。**

## 与 Go 版的差异

* 不做内网穿透（NAT）、DDNS、Telegram 机器人。
* 不对 Go 版探针做兼容适配：不提供 V1 `SantaiziService`（`ReportSystemState` 等）与 `/api/v1/*`、`/api/*` 遗留 HTTP API。探针请使用 `santaizi-agent-rs`（默认 v2 通路）。
* 数据库为全新 SQLite，不迁移 Go 版库；schema 与 Go 版 v20 对齐（去掉 bot/ddns/nat 表）。

## 构建

```bash
# 前端（产物输出 resource/web/，rust-embed 内嵌；CI 同款步骤）
pnpm install --frozen-lockfile && pnpm build

# 后端
cargo build --release
```

`build.rs` 用 `tonic-prost-build` 由 `proto/santaizi.proto` 生成 gRPC 代码；proto 以 `santaizi-dashboard`（Go 仓）为权威，CI 会 diff 防漂移。`resource/web/` 缺失时 cargo 编译失败——本地开发先跑上面的 pnpm 步骤（web/ 源码自 santaizi-dashboard vendor 并裁掉 DDNS / NAT / Bot）。

## 本地开发

```bash
./script/start-local.sh
```

首次启动会把 `script/config.local.yaml` 复制到 `data/dashboard.yaml`，走 mock OAuth2：

* Web：`http://127.0.0.1:8000`
* 登录：`http://127.0.0.1:8000/oauth2/login`（直接以 `admin` 进入后台，无需外部 OAuth 应用）
* gRPC：`127.0.0.1:5555`
* 数据、密钥与 SQLite：`data/`（已 gitignore）

`--reset-config` 重新生成配置；`HTTP_PORT` / `GRPC_PORT` 覆盖端口。mock 仅在 `debug: true` 下可用，切勿用于生产。

## 配置

```bash
./target/release/santaizi-dashboard -c /etc/santaizi/dashboard.yaml
```

环境变量前缀 `SANTAIZI_`（下划线替换为点）打底，YAML 覆盖。默认库路径 `/var/lib/santaizi-dashboard/sqlite.db`（`--db` 可改）。

## GeoIP

国家码与探测跳点 ASN 查询开箱即用：

* **内嵌**：release 构建期 `script/fetch-geoip.sh` 从 GitHub 拉真库替换 `resource/geoip.db` 占位并编进二进制；源码构建拿占位 stub 时自动走下面的自愈下载。
* **外部指定**：`SANTAIZI_GEOIP_DB=/path/to/geoip.dat`（或 `.mmdb` / 其他 `geoip.db`），支持 v2ray 系列 `GeoIPList`（`geoip.dat`）与 MaxMind MMDB，按内容自动识别。
* **自愈下载**：无可用库时按源表顺序从 GitHub release 拉取（Loyalsoldier → MetaCubeX → v2fly → P3TERX GeoLite2），带 `.sha256sum` 校验，缓存在 `--db` 同目录 `geoip.db`；`SANTAIZI_GEOIP_DOWNLOAD=0` 可关闭。
* 缺库静默降级：国家码/ASN 返回空，不影响上报。

## 版权

本项目派生自 [Nezha Monitoring](https://github.com/naiba/nezha)（Apache-2.0），品牌「三太子 / Santaizi」为再品牌化，不主张原商标。致谢与许可声明见 [`NOTICE`](./NOTICE) 与 [`LICENSE`](./LICENSE)，二者不得删除或削弱。
