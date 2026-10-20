#!/bin/sh

#========================================================
#   Santaizi Dashboard (Rust) 一键安装脚本
#   默认从 santaizi-group/santaizi-dashboard-rs 下载，可用 SANTAIZI_DASHBOARD_RS_REPO 覆盖
#   仅 Linux amd64 / arm64。两种安装形态：二进制 + systemd（默认）或 Docker compose。
#   安装位置 /opt/santaizi/dashboard-rs，程序、配置、数据都在该目录下一块：
#   config/dashboard.yaml（0600）与 data/。--upgrade 沿用现有安装形态与配置，只更新程序。
#   Docker 形态的 compose 以 ./data、./config/dashboard.yaml 相对挂载，
#   容器内固定为 /var/lib/santaizi-dashboard 与 /etc/santaizi/dashboard.yaml。
#   SANTAIZI_DASHBOARD_RS_PARSE_ONLY=1 时只写配置与 compose（路径可用环境变量覆盖），
#   不碰 systemd/docker、不要 root，供一次性验证。
#========================================================

SANTAIZI_BASE_PATH="/opt/santaizi"
SANTAIZI_DASHBOARD_PATH="${SANTAIZI_DASHBOARD_PATH:-${SANTAIZI_BASE_PATH}/dashboard-rs}"
# 配置与数据默认跟随工作目录放一块；env 显式指定时不跟随（YAML_FROM_ENV/DATA_FROM_ENV）。
YAML_FROM_ENV="${SANTAIZI_DASHBOARD_YAML:-}"
DATA_FROM_ENV="${SANTAIZI_DASHBOARD_DATA:-}"
SANTAIZI_DASHBOARD_YAML="${YAML_FROM_ENV:-${SANTAIZI_DASHBOARD_PATH}/config/dashboard.yaml}"
SANTAIZI_DASHBOARD_DATA="${DATA_FROM_ENV:-${SANTAIZI_DASHBOARD_PATH}/data}"
SANTAIZI_DASHBOARD_BIN="${SANTAIZI_DASHBOARD_PATH}/santaizi-dashboard"
SANTAIZI_DASHBOARD_UNIT="${SANTAIZI_DASHBOARD_UNIT:-/etc/systemd/system/santaizi-dashboard-rs.service}"

red='\033[0;31m'
green='\033[0;32m'
yellow='\033[0;33m'
plain='\033[0m'

SANTAIZI_DASHBOARD_RS_REPO="${SANTAIZI_DASHBOARD_RS_REPO:-santaizi-group/santaizi-dashboard-rs}"
GHCR_IMAGE="ghcr.io/santaizi-group/santaizi-dashboard-rs"
INSTALL_SCRIPT_URL="https://raw.githubusercontent.com/${SANTAIZI_DASHBOARD_RS_REPO}/main/script/install_dashboard.sh"

PARSE_ONLY="${SANTAIZI_DASHBOARD_RS_PARSE_ONLY:-0}"
UPGRADE=0
INSTALL_MODE=""
WEB_PORT=""
GRPC_PORT=""
OAUTH2_TYPE=""
OAUTH2_ADMIN=""
OAUTH2_CLIENTID=""
OAUTH2_CLIENTSECRET=""
OAUTH2_ENDPOINT=""
SITE_BRAND=""

err() {
    printf "${red}%s${plain}\n" "$*" >&2
}

info() {
    printf "${yellow}%s${plain}\n" "$*"
}

success() {
    printf "${green}%s${plain}\n" "$*"
}

sudo() {
    myEUID=$(id -ru)
    if [ "$myEUID" -ne 0 ]; then
        if command -v sudo > /dev/null 2>&1; then
            command sudo "$@"
        elif command -v doas > /dev/null 2>&1; then
            command doas "$@"
        else
            err "ERROR: 当前非 root，且未安装 sudo/doas，无法继续。"
            err "请先切换到 root 后运行："
            err "  su -"
            err "  sh -c \"\$(curl -fsSL ${INSTALL_SCRIPT_URL})\""
            exit 1
        fi
    else
        "$@"
    fi
}

as_root() {
    if [ "$PARSE_ONLY" = 1 ]; then
        "$@"
    else
        sudo "$@"
    fi
}

yaml_escape() {
    printf "%s" "$1" | sed "s/'/''/g"
}

prompt() {
    message=$1
    default=$2
    printf "%s" "$message" >&2
    if [ -n "$default" ]; then
        printf " (默认: %s)" "$default" >&2
    fi
    printf ": " >&2
    if ! read -r val; then
        printf "\n" >&2
        err "输入已结束（EOF），无法继续交互。"
        return 1
    fi
    if [ -z "$val" ]; then
        val=$default
    fi
    printf "%s\n" "$val"
}

prompt_required() {
    while true; do
        val=$(prompt "$1" "$2") || return 1
        if [ -n "$val" ]; then
            printf "%s\n" "$val"
            return 0
        fi
        err "该项不能为空，请重新输入。"
    done
}

prompt_port() {
    while true; do
        val=$(prompt "$1" "$2") || return 1
        case "$val" in
            ''|*[!0-9]*)
                err "端口必须是 1-65535 之间的数字。"
                continue
                ;;
        esac
        if [ "$val" -ge 1 ] 2>/dev/null && [ "$val" -le 65535 ] 2>/dev/null; then
            printf "%s\n" "$val"
            return 0
        fi
        err "端口必须是 1-65535 之间的数字。"
    done
}

deps_check() {
    deps="curl"
    if [ "$1" = "binary" ]; then
        deps="curl unzip sha256sum"
    fi
    missing=""
    for dep in $deps; do
        if ! command -v "$dep" >/dev/null 2>&1; then
            missing="${missing} ${dep}"
        fi
    done
    if [ -n "$missing" ]; then
        err "缺少依赖:${missing}，请先安装后再试。"
        exit 1
    fi
}

detect_os() {
    system=$(uname)
    case "$system" in
        *Linux*) echo "linux" ;;
        *) echo "unknown" ;;
    esac
}

detect_arch() {
    mach=$(uname -m)
    case "$mach" in
        amd64|x86_64) echo "amd64" ;;
        aarch64|arm64) echo "arm64" ;;
        *) echo "unknown" ;;
    esac
}

get_latest_version() {
    version=$(curl -fsSL -m 10 "https://api.github.com/repos/${SANTAIZI_DASHBOARD_RS_REPO}/releases/latest" | grep '"tag_name":' | head -n 1 | sed 's/.*"tag_name": "\(.*\)",.*/\1/')
    if [ -z "$version" ]; then
        err "获取 Rust 版 Dashboard 版本失败，请检查网络是否能访问 https://api.github.com/repos/${SANTAIZI_DASHBOARD_RS_REPO}/releases/latest"
        err "需要该仓库已发布 v* Release（santaizi-dashboard-linux-amd64/arm64.zip）。"
        exit 1
    fi
    echo "$version"
}

verify_checksum() {
    zipfile=$1
    name=$2
    sumsfile=$3
    expected=$(awk -v f="$name" '$2 == f {print $1; exit}' "$sumsfile")
    if [ -z "$expected" ]; then
        err "checksums.txt 中没有 ${name} 的校验和。"
        return 1
    fi
    actual=$(sha256sum "$zipfile" | awk '{print $1}')
    if [ "$expected" != "$actual" ]; then
        err "SHA256 校验失败：期望 ${expected}，实际 ${actual}"
        return 1
    fi
    success "SHA256 校验通过。"
}

# zip 内二进制带平台后缀（santaizi-dashboard-linux-<arch>），统一改名安装。
install_binary() {
    deps_check binary

    os=$(detect_os)
    arch=$(detect_arch)
    if [ "$os" != "linux" ] || [ "$arch" = "unknown" ]; then
        err "Rust 版 Dashboard 安装脚本仅支持 Linux amd64 / arm64，当前: $(uname) / $(uname -m)"
        exit 1
    fi

    info "正在获取 Rust 版 Dashboard 最新版本..."
    version=$(get_latest_version)
    success "最新版本: ${version}"

    base="https://github.com/${SANTAIZI_DASHBOARD_RS_REPO}/releases/download/${version}"
    zipname="santaizi-dashboard-linux-${arch}.zip"
    tmpzip=$(mktemp)
    tmpsums=$(mktemp)

    info "正在下载 ${base}/${zipname} ..."
    if ! curl -fsSL -m 180 -o "$tmpzip" "${base}/${zipname}"; then
        err "下载 Rust 版 Dashboard 失败，请检查网络连接，并确认该版本已发布 linux ${arch} 产物。"
        rm -f "$tmpzip" "$tmpsums"
        exit 1
    fi
    if ! curl -fsSL -m 60 -o "$tmpsums" "${base}/checksums.txt"; then
        err "下载 checksums.txt 失败，无法校验产物完整性。"
        rm -f "$tmpzip" "$tmpsums"
        exit 1
    fi
    if ! verify_checksum "$tmpzip" "$zipname" "$tmpsums"; then
        rm -f "$tmpzip" "$tmpsums"
        exit 1
    fi

    stage=$(mktemp -d)
    if ! unzip -qo "$tmpzip" -d "$stage"; then
        err "解压 Rust 版 Dashboard 失败。"
        rm -rf "$stage" "$tmpzip" "$tmpsums"
        exit 1
    fi
    rm -f "$tmpzip" "$tmpsums"
    inner="${stage}/santaizi-dashboard-linux-${arch}"
    if [ ! -f "$inner" ]; then
        err "解压后未找到 ${inner}"
        rm -rf "$stage"
        exit 1
    fi
    as_root mkdir -p "$SANTAIZI_DASHBOARD_PATH"
    as_root mv "$inner" "$SANTAIZI_DASHBOARD_BIN"
    as_root chmod +x "$SANTAIZI_DASHBOARD_BIN"
    rm -rf "$stage"
}

write_owned_file() {
    dest=$1
    src=$2
    mode=$3
    as_root mkdir -p "$(dirname "$dest")"
    as_root mv "$src" "$dest"
    as_root chmod "$mode" "$dest"
}

write_config() {
    oauth2_type=$(yaml_escape "$OAUTH2_TYPE")
    oauth2_admin=$(yaml_escape "$OAUTH2_ADMIN")
    oauth2_clientid=$(yaml_escape "$OAUTH2_CLIENTID")
    oauth2_clientsecret=$(yaml_escape "$OAUTH2_CLIENTSECRET")
    oauth2_endpoint=$(yaml_escape "$OAUTH2_ENDPOINT")
    site_brand=$(yaml_escape "$SITE_BRAND")
    if [ "$INSTALL_MODE" = "docker" ]; then
        # 配置挂进容器使用，端口固定容器内 80/5555，宿主端口靠 compose 映射；
        # data_dir 写容器内路径（compose 把宿主 ./data 挂到 /var/lib/santaizi-dashboard）。
        cfg_http_port=80
        cfg_grpc_port=5555
        escaped_data=$(yaml_escape "/var/lib/santaizi-dashboard")
    else
        cfg_http_port=$WEB_PORT
        cfg_grpc_port=$GRPC_PORT
        escaped_data=$(yaml_escape "$SANTAIZI_DASHBOARD_DATA")
    fi
    # 数据目录与配置放一块；docker 形态它就是 compose 的 ./data 挂载源。
    as_root mkdir -p "$SANTAIZI_DASHBOARD_DATA"
    tmp=$(mktemp)
    cat > "$tmp" <<EOF
mode: primary
debug: false
httpport: ${cfg_http_port}
grpcport: ${cfg_grpc_port}
language: zh-CN
oauth2:
  type: '${oauth2_type}'
  admin: '${oauth2_admin}'
  clientid: '${oauth2_clientid}'
  clientsecret: '${oauth2_clientsecret}'
  endpoint: '${oauth2_endpoint}'
site:
  brand: '${site_brand}'
  cookiename: 'santaizi-dashboard-rs'
web:
  delivery: 'embedded'
telemetry:
  data_dir: '${escaped_data}'
EOF
    write_owned_file "$SANTAIZI_DASHBOARD_YAML" "$tmp" 0600
}

write_compose() {
    # 相对路径按 compose 文件所在目录（安装工作目录）解析，配置与数据放一块。
    tmp=$(mktemp)
    cat > "$tmp" <<EOF
services:
  santaizi-dashboard:
    image: ${GHCR_IMAGE}:latest
    container_name: santaizi-dashboard-rs
    restart: unless-stopped
    ports:
      - "${WEB_PORT}:80"
      - "${GRPC_PORT}:5555"
    volumes:
      - /etc/timezone:/etc/timezone:ro
      - /etc/localtime:/etc/localtime:ro
      - ./data:/var/lib/santaizi-dashboard
      - ./config/dashboard.yaml:/etc/santaizi/dashboard.yaml:ro
    environment:
      - TZ=Asia/Shanghai
EOF
    write_owned_file "${SANTAIZI_DASHBOARD_PATH}/docker-compose.yml" "$tmp" 0644
}

write_unit() {
    tmp=$(mktemp)
    cat > "$tmp" <<EOF
[Unit]
Description=Santaizi Dashboard (Rust)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${SANTAIZI_DASHBOARD_PATH}
ExecStart=${SANTAIZI_DASHBOARD_BIN} --config ${SANTAIZI_DASHBOARD_YAML} --db ${SANTAIZI_DASHBOARD_DATA}/sqlite.db
Restart=always
RestartSec=5
ProtectSystem=full
PrivateTmp=yes
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF
    write_owned_file "$SANTAIZI_DASHBOARD_UNIT" "$tmp" 0644
}

# 升级时从既有单元读回二进制路径，兼容安装时改过工作目录的机器。
detect_unit_bin() {
    [ -f "$SANTAIZI_DASHBOARD_UNIT" ] || return 1
    bin=$(sed -n 's/^ExecStart=\([^ ]*\).*$/\1/p' "$SANTAIZI_DASHBOARD_UNIT" | head -n 1)
    [ -n "$bin" ] || return 1
    printf '%s\n' "$bin"
}

check_docker() {
    if ! command -v docker >/dev/null 2>&1; then
        err "未检测到 Docker，请先安装 Docker 后重新运行脚本（或改用二进制安装方式）。"
        exit 1
    fi
    if ! docker compose version >/dev/null 2>&1 && ! command -v docker-compose >/dev/null 2>&1; then
        err "未检测到 Docker Compose 插件，请先安装后重新运行脚本。"
        exit 1
    fi
}

run_compose() {
    compose_file="${SANTAIZI_DASHBOARD_PATH}/docker-compose.yml"
    if docker compose version >/dev/null 2>&1; then
        sudo docker compose -f "$compose_file" "$@"
    else
        sudo docker-compose -f "$compose_file" "$@"
    fi
}

collect_answers() {
    info "请选择 Dashboard 安装方式"
    info "  1) 二进制 + systemd（默认）"
    info "  2) Docker（docker compose）"
    while true; do
        val=$(prompt "安装方式" "1") || exit 1
        case "$val" in
            1|binary) INSTALL_MODE="binary"; break ;;
            2|docker) INSTALL_MODE="docker"; break ;;
            *) err "请输入 1 或 2。" ;;
        esac
    done

    SANTAIZI_DASHBOARD_PATH=$(prompt "请输入 Dashboard 工作目录" "$SANTAIZI_DASHBOARD_PATH") || exit 1
    SANTAIZI_DASHBOARD_BIN="${SANTAIZI_DASHBOARD_PATH}/santaizi-dashboard"
    # 配置/数据跟着工作目录放一块。Docker 形态的 compose 用 ./config、./data 相对挂载，
    # 宿主路径必须就在工作目录下，不接受 env 覆盖；env 覆盖仅对二进制形态生效。
    if [ "$INSTALL_MODE" = "docker" ] || [ -z "$YAML_FROM_ENV" ]; then
        SANTAIZI_DASHBOARD_YAML="${SANTAIZI_DASHBOARD_PATH}/config/dashboard.yaml"
    fi
    if [ "$INSTALL_MODE" = "docker" ] || [ -z "$DATA_FROM_ENV" ]; then
        SANTAIZI_DASHBOARD_DATA="${SANTAIZI_DASHBOARD_PATH}/data"
    fi

    WEB_PORT=$(prompt_port "请输入面板 Web 端口" "80") || exit 1
    GRPC_PORT=$(prompt_port "请输入 Agent gRPC 端口" "5555") || exit 1

    info "请配置 OAuth2 登录（首次登录必须使用 OAuth2）"
    info "请先在提供商创建应用。回调地址必须填：https://<你访问面板的域名>/oauth2/callback"
    info "GitHub 字段名是 Authorization callback URL；不要只填首页。"
    OAUTH2_TYPE=$(prompt "OAuth2 类型 (github/gitlab/jihulab/gitee/gitea)" "github") || exit 1
    OAUTH2_TYPE=${OAUTH2_TYPE:-github}
    OAUTH2_ADMIN=$(prompt_required "管理员账号（多个用半角逗号隔开）") || exit 1
    OAUTH2_CLIENTID=$(prompt_required "OAuth2 Client ID") || exit 1
    OAUTH2_CLIENTSECRET=$(prompt_required "OAuth2 Client Secret") || exit 1
    OAUTH2_ENDPOINT=$(prompt "OAuth2 Endpoint（仅自建 Gitea 需要）" "") || exit 1

    SITE_BRAND=$(prompt "站点标题" "三太子监控") || exit 1
    SITE_BRAND=${SITE_BRAND:-三太子监控}
}

get_server_ip() {
    ip=$(ip route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src") {print $(i+1); exit}}')
    if [ -z "$ip" ]; then
        ip=$(hostname -I 2>/dev/null | awk '{print $1}')
    fi
    if [ -z "$ip" ]; then
        ip="<服务器IP>"
    fi
    printf "%s\n" "$ip"
}

finish() {
    SERVER_IP=$(get_server_ip)
    success "Santaizi Dashboard (Rust) 安装完成！"
    success "访问地址: http://${SERVER_IP}:${WEB_PORT}"
    success "工作目录: ${SANTAIZI_DASHBOARD_PATH}"
    success "配置文件: ${SANTAIZI_DASHBOARD_YAML}"
    success "数据目录: ${SANTAIZI_DASHBOARD_DATA}"
    info "安装 Agent：进入后台 → 服务器 → 添加服务器 → 使用一键安装命令。"
}

upgrade_flow() {
    if [ "$(uname -s)" != "Linux" ]; then
        err "本脚本目前仅支持 Linux 系统。"
        exit 1
    fi

    if [ -f "$SANTAIZI_DASHBOARD_UNIT" ]; then
        info "检测到 systemd 安装，正在升级二进制..."
        installed_bin=$(detect_unit_bin) || true
        if [ -n "$installed_bin" ]; then
            SANTAIZI_DASHBOARD_BIN="$installed_bin"
            SANTAIZI_DASHBOARD_PATH=$(dirname "$installed_bin")
        fi
        install_binary
        sudo systemctl daemon-reload
        if ! sudo systemctl restart santaizi-dashboard-rs; then
            err "重启 santaizi-dashboard-rs 失败，请用 journalctl -u santaizi-dashboard-rs 查看日志。"
            exit 1
        fi
        success "Santaizi Dashboard (Rust) 升级完成。"
        return
    fi

    if [ -f "${SANTAIZI_DASHBOARD_PATH}/docker-compose.yml" ]; then
        info "检测到 Docker 安装，正在拉取最新镜像..."
        check_docker
        if ! run_compose pull || ! run_compose up -d; then
            err "升级失败，请检查 Docker 与网络。"
            exit 1
        fi
        success "Santaizi Dashboard (Rust) 升级完成。"
        return
    fi

    err "未检测到现有安装（${SANTAIZI_DASHBOARD_UNIT} 或 ${SANTAIZI_DASHBOARD_PATH}/docker-compose.yml）。"
    err "如安装在非默认工作目录，请设置 SANTAIZI_DASHBOARD_PATH 后重试。"
    exit 1
}

usage() {
    echo "用法: $0 [--upgrade]"
    echo "  （无参数）交互式安装：二进制 + systemd 或 Docker，默认装到 /opt/santaizi/dashboard-rs"
    echo "  --upgrade  非交互升级：沿用现有安装形态与配置，只更新程序"
    echo "环境变量: SANTAIZI_DASHBOARD_RS_REPO / SANTAIZI_DASHBOARD_PATH / SANTAIZI_DASHBOARD_YAML"
    echo "          SANTAIZI_DASHBOARD_DATA / SANTAIZI_DASHBOARD_RS_PARSE_ONLY=1"
    echo "          （Docker 形态配置/数据固定在工作目录下的 ./config、./data，后两个变量仅对二进制形态生效）"
}

case "${1:-}" in
    "") ;;
    --upgrade) UPGRADE=1 ;;
    --help|-h) usage; exit 0 ;;
    *) err "不支持的参数: $1"; usage; exit 1 ;;
esac

if [ "$PARSE_ONLY" = 1 ]; then
    # 只生成配置与 compose：允许管道输入，不校验 TTY、不要 root、不装服务。
    collect_answers
    write_config
    if [ "$INSTALL_MODE" = "docker" ]; then
        write_compose
    fi
    success "PARSE_ONLY: 已写入 ${SANTAIZI_DASHBOARD_YAML}"
    exit 0
fi

if [ "$UPGRADE" = 1 ]; then
    upgrade_flow
    exit 0
fi

info "欢迎使用 Santaizi Dashboard (Rust) 一键安装脚本"

if [ "$(uname -s)" != "Linux" ]; then
    err "本脚本目前仅支持 Linux 系统。"
    exit 1
fi

if ! [ -t 0 ]; then
    err "检测到脚本通过管道执行（如 curl ... | bash），但本脚本需要交互式输入。"
    err "请改用以下一键运行方式："
    err "  sh -c \"\$(curl -fsSL ${INSTALL_SCRIPT_URL})\""
    err "如果当前不是 root，请先使用 sudo、doas 或 su - 提权后再运行上面的命令。"
    exit 1
fi

collect_answers

as_root mkdir -p "$SANTAIZI_DASHBOARD_PATH" "$SANTAIZI_DASHBOARD_DATA" "$(dirname "$SANTAIZI_DASHBOARD_YAML")"

case "$INSTALL_MODE" in
    binary)
        write_config
        install_binary
        write_unit
        sudo systemctl daemon-reload
        sudo systemctl enable santaizi-dashboard-rs >/dev/null 2>&1
        if ! sudo systemctl restart santaizi-dashboard-rs; then
            err "启动 santaizi-dashboard-rs 失败，请用 journalctl -u santaizi-dashboard-rs 查看日志。"
            exit 1
        fi
        ;;
    docker)
        check_docker
        write_config
        write_compose
        info "正在拉取镜像并启动 Dashboard..."
        if ! run_compose up -d; then
            err "启动 Dashboard 失败，请检查 Docker 与网络。"
            exit 1
        fi
        ;;
esac

finish
