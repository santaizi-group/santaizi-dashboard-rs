#!/usr/bin/env bash
# 校验 vendor 的 proto 与权威版（santaizi-dashboard）一致。
# 用法：script/proto-diff.sh [权威版 proto 路径]
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
upstream="${1:-${SANTAIZI_PROTO_UPSTREAM:-}}"

if [[ -z "${upstream}" ]]; then
  echo "usage: $0 <path-to-santaizi-dashboard/proto/santaizi.proto>" >&2
  echo "  或设置 SANTAIZI_PROTO_UPSTREAM" >&2
  exit 2
fi

if [[ ! -f "${upstream}" ]]; then
  echo "upstream proto not found: ${upstream}" >&2
  exit 2
fi

if ! diff -u "${upstream}" "${repo_root}/proto/santaizi.proto"; then
  echo "proto/santaizi.proto drifted from the authoritative dashboard proto" >&2
  echo "copy the upstream file into proto/ and commit" >&2
  exit 1
fi
echo "proto in sync"
