#!/usr/bin/env bash
# Santaizi Dashboard（Rust）本地 Primary 开发启动：debug + mock OAuth2。
#
# 用法:
#   ./script/start-local.sh                 # 增量构建 release 后启动
#   ./script/start-local.sh --reset-config  # 重新生成 data/dashboard.yaml
#   HTTP_PORT=8080 GRPC_PORT=5556 ./script/start-local.sh
#
# 数据与配置落在仓库根目录 data/（已 gitignore）。
# 登录：http://127.0.0.1:8000/oauth2/login 直接以 admin 身份进入后台。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DATA_DIR="${SANTAIZI_LOCAL_DATA:-$ROOT/data}"
CONFIG_PATH="${SANTAIZI_LOCAL_CONFIG:-$DATA_DIR/dashboard.yaml}"
DB_PATH="${SANTAIZI_LOCAL_DB:-$DATA_DIR/sqlite.db}"
TEMPLATE="$ROOT/script/config.local.yaml"
BIN_PATH="${SANTAIZI_LOCAL_BIN:-$ROOT/target/release/santaizi-dashboard}"

HTTP_PORT="${HTTP_PORT:-8000}"
GRPC_PORT="${GRPC_PORT:-5555}"

RESET_CONFIG=0
PASS_ARGS=()

usage() {
  sed -n '2,10p' "$0" | sed 's/^# \?//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --reset-config)
      RESET_CONFIG=1
      shift
      ;;
    --)
      shift
      PASS_ARGS+=("$@")
      break
      ;;
    *)
      PASS_ARGS+=("$1")
      shift
      ;;
  esac
done

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: 需要已安装 Rust 工具链" >&2
  exit 1
fi

if [[ ! -f "$ROOT/resource/web/status/index.html" ]]; then
  echo "error: resource/web 缺失，先构建前端：" >&2
  echo "  pnpm install --frozen-lockfile && pnpm build" >&2
  exit 1
fi

mkdir -p "$DATA_DIR"

seed_config() {
  if [[ ! -f "$TEMPLATE" ]]; then
    echo "error: 缺少模板 $TEMPLATE" >&2
    exit 1
  fi
  # 配置加载时 YAML 覆盖 SANTAIZI_* 环境变量，端口与数据目录必须写进文件。
  sed \
    -e "s|^  data_dir:.*|  data_dir: \"${DATA_DIR}\"|" \
    -e "s|^httpport:.*|httpport: ${HTTP_PORT}|" \
    -e "s|^grpcport:.*|grpcport: ${GRPC_PORT}|" \
    "$TEMPLATE" >"$CONFIG_PATH"
  echo "已生成配置: $CONFIG_PATH"
}

if [[ "$RESET_CONFIG" -eq 1 || ! -f "$CONFIG_PATH" ]]; then
  seed_config
else
  # 已有配置以文件为准，避免打印的端口和实际监听不一致。
  cfg_http="$(awk '/^httpport:/{print $2; exit}' "$CONFIG_PATH" || true)"
  cfg_grpc="$(awk '/^grpcport:/{print $2; exit}' "$CONFIG_PATH" || true)"
  [[ -n "$cfg_http" ]] && HTTP_PORT="$cfg_http"
  [[ -n "$cfg_grpc" ]] && GRPC_PORT="$cfg_grpc"
fi

echo "SANTAIZI local Primary"
echo "  config : $CONFIG_PATH"
echo "  db     : $DB_PATH"
echo "  web    : http://127.0.0.1:${HTTP_PORT}"
echo "  grpc   : 127.0.0.1:${GRPC_PORT}"
echo "  login  : http://127.0.0.1:${HTTP_PORT}/oauth2/login  (mock admin)"
echo

cargo build --release
exec "$BIN_PATH" -c "$CONFIG_PATH" --db "$DB_PATH" "${PASS_ARGS[@]}"
