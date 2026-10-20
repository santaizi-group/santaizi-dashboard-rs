#!/usr/bin/env bash
# GeoIP 库自助下载：按源表顺序拉 GitHub release 资产，首个成功即用（release CI 与本地构建共用）。
# 源表与 src/geoip/download.rs 的 SOURCES 保持一致；带 .sha256sum 伴生的源会校验后再落盘。
# 用法：script/fetch-geoip.sh [-o 输出路径]   默认 resource/geoip.db
set -euo pipefail

OUT="resource/geoip.db"
while getopts "o:h" opt; do
  case "$opt" in
    o) OUT="$OPTARG" ;;
    h)
      sed -n '2,4p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      exit 1
      ;;
  esac
done

SOURCES=(
  "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat|https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat.sha256sum"
  "https://github.com/MetaCubeX/meta-rules-dat/releases/latest/download/geoip.db|"
  "https://github.com/v2fly/geoip/releases/latest/download/geoip.dat|https://github.com/v2fly/geoip/releases/latest/download/geoip.dat.sha256sum"
  "https://github.com/P3TERX/GeoLite.mmdb/releases/latest/download/GeoLite2-Country.mmdb|"
)

verify() {
  local file="$1" sum_url="$2"
  [ -n "$sum_url" ] || return 0
  local expected actual
  expected="$(curl -fsSL --retry 2 "$sum_url" | awk 'NR==1{print $1}')"
  if [ "${#expected}" -ne 64 ]; then
    echo "checksum malformed: $sum_url" >&2
    return 1
  fi
  actual="$(sha256sum "$file" | awk '{print $1}')"
  if [ "$actual" != "$expected" ]; then
    echo "sha256 mismatch for $file" >&2
    return 1
  fi
}

mkdir -p "$(dirname "$OUT")"
TMP="${OUT}.tmp"

for entry in "${SOURCES[@]}"; do
  url="${entry%%|*}"
  sum_url="${entry##*|}"
  echo ">> GeoIP 下载 $url"
  if ! curl -fsSL --retry 2 --connect-timeout 15 --max-time 180 -o "$TMP" "$url"; then
    echo "   失败，换下一个源" >&2
    continue
  fi
  if [ ! -s "$TMP" ]; then
    echo "   空文件，换下一个源" >&2
    continue
  fi
  if ! verify "$TMP" "$sum_url"; then
    continue
  fi
  mv "$TMP" "$OUT"
  echo ">> 已写入 $OUT（$(wc -c <"$OUT") bytes）"
  exit 0
done

rm -f "$TMP"
echo "GeoIP 全部源下载失败" >&2
exit 1
