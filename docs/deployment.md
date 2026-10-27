# 部署

本文只覆盖 Rust 版面板 [`santaizi-dashboard-rs`](https://github.com/santaizi-group/santaizi-dashboard-rs)。同一二进制可作主面板（`mode: primary`）或从端（`mode: collector`）。

不做内网穿透、DDNS、Telegram 机器人。没有 OIDC：`oauth2.type: oidc` 登录会返回 501。`mock` 只在 `debug: true` 时能登录，不能用于生产。数据库是全新 SQLite，不迁移 Go 版库。

## 安全

管理后台、OAuth 回调、API 与 WebSocket 不要直接暴露在公网。Web 放在私有网络或反向代理访问控制之后；gRPC（默认 `5555`）只对探针开放，与 Web 入口分开。

本进程不挂 TLS。回调的协议只看反向代理头 `X-Forwarded-Proto`、`X-Forwarded-Ssl`，或 Referer 是否以 `https://` 开头。没有这些头时，回调会被拼成 `http://...`，和提供商控制台不一致。

## 要求与端口

- Linux amd64 / arm64
- Web 默认 `80`，gRPC 默认 `5555`

| 路径 | 二进制 + systemd | Docker（容器内） |
| --- | --- | --- |
| 程序 | `/opt/santaizi/dashboard-rs/santaizi-dashboard` | 镜像 `ghcr.io/santaizi-group/santaizi-dashboard-rs` |
| 配置 | `/opt/santaizi/dashboard-rs/config/dashboard.yaml`（0600） | `/etc/santaizi/dashboard.yaml` |
| 数据 | `/opt/santaizi/dashboard-rs/data` | `/var/lib/santaizi-dashboard` |

Docker 的 compose 用工作目录下的 `./data`、`./config/dashboard.yaml` 相对挂载。宿主端口写在 compose 里；容器内配置的 `httpport` / `grpcport` 固定为 `80` / `5555`。

## 一键安装

非 root 时先 `sudo -i`、`doas sh` 或 `su -`。不要用 `curl | bash`，脚本需要交互输入。

```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/santaizi-group/santaizi-dashboard-rs/main/script/install_dashboard.sh)"
```

脚本会询问安装形态、工作目录、Web / gRPC 端口、OAuth2 类型，以及面板对外地址。选定提供商后会打印该控制台的回调字段名和可粘贴的回调 URL。对外地址可留空；留空时结束语里的 `http://IP:端口` 只用于本机核对，不能登记为回调。

升级沿用现有形态和配置，只更新程序：

```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/santaizi-group/santaizi-dashboard-rs/main/script/install_dashboard.sh)" -- --upgrade
```

## 手动：二进制 + systemd

从 [Release](https://github.com/santaizi-group/santaizi-dashboard-rs/releases) 下载 `santaizi-dashboard-linux-amd64.zip` 或 `santaizi-dashboard-linux-arm64.zip`，用同目录 `checksums.txt` 核对 SHA256。zip 内文件名带平台后缀，安装时改名为 `santaizi-dashboard`。

```bash
sudo mkdir -p /opt/santaizi/dashboard-rs/config /opt/santaizi/dashboard-rs/data
sudo install -m 0755 santaizi-dashboard-linux-amd64 /opt/santaizi/dashboard-rs/santaizi-dashboard
sudo install -m 0600 config.example.yaml /opt/santaizi/dashboard-rs/config/dashboard.yaml
```

`/etc/systemd/system/santaizi-dashboard-rs.service`：

```ini
[Unit]
Description=Santaizi Dashboard (Rust)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/santaizi/dashboard-rs
ExecStart=/opt/santaizi/dashboard-rs/santaizi-dashboard --config /opt/santaizi/dashboard-rs/config/dashboard.yaml --db /opt/santaizi/dashboard-rs/data/sqlite.db
Restart=always
RestartSec=5
ProtectSystem=full
PrivateTmp=yes
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now santaizi-dashboard-rs
```

## 手动：Docker

在 `/opt/santaizi/dashboard-rs` 放置 `config/dashboard.yaml` 与 `docker-compose.yml`。配置里的端口和数据目录用容器内值：

```yaml
httpport: 80
grpcport: 5555
telemetry:
  data_dir: '/var/lib/santaizi-dashboard'
```

```yaml
services:
  santaizi-dashboard:
    image: ghcr.io/santaizi-group/santaizi-dashboard-rs:latest
    container_name: santaizi-dashboard-rs
    restart: unless-stopped
    ports:
      - "80:80"
      - "5555:5555"
    volumes:
      - /etc/timezone:/etc/timezone:ro
      - /etc/localtime:/etc/localtime:ro
      - ./data:/var/lib/santaizi-dashboard
      - ./config/dashboard.yaml:/etc/santaizi/dashboard.yaml:ro
    environment:
      - TZ=Asia/Shanghai
```

```bash
docker compose up -d
```

## 从端

从端与主面板是同一二进制，`mode: collector`。登录后台 → 从端 → 添加从端，在安装弹窗复制一键命令，到观测点主机执行。命令由面板按当前配置生成（端点、端口、TLS 旗标），注册令牌只在弹窗里显示，丢失就轮换。

```bash
curl -fsSL https://raw.githubusercontent.com/santaizi-group/santaizi-dashboard-rs/main/script/install_collector.sh | bash -s -- --primary-endpoint primary.example.com:5555 --token <注册令牌> --grpc-port 5556 --primary-tls false --primary-insecure-tls false
```

默认装到 `/opt/santaizi/collector-rs`，容器名 `santaizi-collector-rs`，与 Go 版从端（`/opt/santaizi/collector`）可在同一主机共存。`--primary-tls` 跟随主端 `grpc_tls.enabled`；主端证书不在公开信任链（自签）时加 `--primary-insecure-tls true`。

升级不传 `--token`（保留配置与数据，pull 并重建）：

```bash
curl -fsSL https://raw.githubusercontent.com/santaizi-group/santaizi-dashboard-rs/main/script/install_collector.sh | bash
```

日志与卸载：

```bash
docker logs -f santaizi-collector-rs
cd /opt/santaizi/collector-rs && docker compose down
sudo rm -rf /opt/santaizi/collector-rs
```

主端 CA 轮换或重装后，盘上的从端证书会被主端在 TLS 层拒绝（`UnknownCA`）。从端检测到后自动丢弃旧证书、用注册令牌重新注册，无需手工删数据目录；日志里「主端拒绝了从端证书」即这条自愈路径。持续出现且不恢复，检查主端是否已吊销该从端。

### 探测型从端的内存边界

探测型从端（`kind=probe`）的执行并发与等待队列上限与目标数无关：ICMP/TCP 连通性固定 2 个 worker；MTR、周期路由与主端下发的按需路由共用 1 个 nexttrace 执行器，同时最多 1 个 nexttrace 子进程。每类等待队列 32 项（连通性为 2 个 worker × 16），满时周期任务保留到下一轮重试（不产伪样本），按需任务立即回失败、主端据此标记失败。目标状态本身仍按目标数线性增长，但每项很小。

45s 超时或从端取消时，nexttrace 子进程会被杀掉、输出读取任务同时中止，不会等孙进程释放管道。因此没有 swap 的 2G 级主机也能安全启动。代价是大规模 MTR/路由的尾延迟变长——串行追踪是以延迟换安全，不要为此调大队列。也不要用 Docker 硬内存限额去掩盖：掩盖之后进程被 cgroup OOM 杀掉，表现同样是面板显示从端离线、容器每几秒重启。

排障时若看到「nexttrace 输出超过 4194304 字节上限」，说明该次追踪的 stdout 超出 4 MiB 硬上限被主动判失败（stderr 上限 64 KiB），不是进程崩溃。

## 最小配置

二进制形态的示例见 [`script/config.example.yaml`](../script/config.example.yaml)。至少填写 `oauth2.type`、`oauth2.admin`、`oauth2.clientid`、`oauth2.clientsecret`。自建 Gitea 与 Cloudflare Access 还要填 `oauth2.endpoint`。

回调不写入配置。面板用浏览器这次请求的 Host 拼出 `https://<地址栏域名和端口>/oauth2/callback`。

## OAuth2 回调

先在提供商创建应用，再把密钥写入配置。规则：

- 路径固定 `/oauth2/callback`。不要只填首页，不要加尾斜杠，不要改成 `/admin/` 或 `/oauth2/login`。
- 协议、域名、端口必须和浏览器地址栏一致。`www` 与裸域名不能混用。
- 反向代理必须把 `/oauth2` 转到面板，并带上 `Host` 与 `X-Forwarded-Proto: https`。

示例：面板是 `https://dash.example.com`，回调填 `https://dash.example.com/oauth2/callback`。

| 类型 | 控制台字段 | `oauth2.admin` |
| --- | --- | --- |
| `github` | OAuth App（不是 GitHub App）的 **Authorization callback URL**。不要勾 Device Flow。创建页：<https://github.com/settings/applications/new> | GitHub 登录名 |
| `gitee` | **应用回调地址**。创建页：<https://gitee.com/oauth/applications> | Gitee 登录名 |
| `gitlab` / `jihulab` | **Redirect URI**。权限勾选 `read_user`、`read_api` | username |
| `gitea` | **Redirect URIs**。`oauth2.endpoint` 只填源站，例如 `https://git.example.com`，不要带路径 | 登录名 |
| `cloudflare` | Access 应用的 **Redirect URLs**。`oauth2.endpoint` 填团队域名，例如 `https://<team>.cloudflareaccess.com` | 用户 `sub`，不是邮箱 |

多个管理员用半角逗号隔开，匹配时忽略大小写。

## 反向代理

Caddy 会带上 `Host` 与 `X-Forwarded-Proto`。下面只代理 Web；探针仍直接连 gRPC 端口。

```caddy
dash.example.com {
	reverse_proxy 127.0.0.1:80
}
```

Web 不在 `80` 时，把 `127.0.0.1:80` 换成实际端口。Nginx 须自行设置：

```nginx
proxy_set_header Host $host;
proxy_set_header X-Forwarded-Proto $scheme;
```

`/oauth2` 与 `/admin/`、`/api/`、`/ws` 走同一上游，不要拆到别的域名。

## 防火墙

- Web：不对公网直接放行，只对本机、内网或反向代理出口开放。
- gRPC：按探针所在网络放行。

## 升级、日志、卸载

升级：

```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/santaizi-group/santaizi-dashboard-rs/main/script/install_dashboard.sh)" -- --upgrade
```

日志：

```bash
journalctl -u santaizi-dashboard-rs -f
docker logs -f santaizi-dashboard-rs
```

卸载会删掉程序、配置和 SQLite。二进制：

```bash
sudo systemctl disable --now santaizi-dashboard-rs
sudo rm -f /etc/systemd/system/santaizi-dashboard-rs.service
sudo rm -rf /opt/santaizi/dashboard-rs
sudo systemctl daemon-reload
```

Docker：

```bash
cd /opt/santaizi/dashboard-rs
docker compose down
sudo rm -rf /opt/santaizi/dashboard-rs
```

## 探针

登录后台 → 服务器 → 添加服务器，在卡片上复制一键安装命令，到被监控主机执行。Rust 探针仅 Linux amd64 / arm64。

## 故障

- **`redirect_uri` 不一致**：控制台回调不是 `https://<地址栏域名和端口>/oauth2/callback`，或反代没把 `Host` 和 `X-Forwarded-Proto` 传到面板。
- **登录了但进不了后台**：`oauth2.admin` 与提供商返回的登录名（Cloudflare 为 `sub`）不一致。
- **探针连不上**：防火墙未放行 gRPC 端口，或探针命令里的地址、端口与 `grpcport` 不一致。
