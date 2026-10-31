# shellcheck shell=sh
# Shared helpers for install/upgrade scripts (source with `.`, do not execute).
# Expects err/info/success from the caller.

ARTIFACT_BIN_PATH="${ARTIFACT_BIN_PATH:-}"
ARTIFACT_BIN_URL="${ARTIFACT_BIN_URL:-}"
ARTIFACT_BIN_VERSION="${ARTIFACT_BIN_VERSION:-}"
ARTIFACT_SKIP_CHECKSUM="${ARTIFACT_SKIP_CHECKSUM:-0}"
ARTIFACT_IMAGE_REF="${ARTIFACT_IMAGE_REF:-}"
ARTIFACT_IMAGE_TAR="${ARTIFACT_IMAGE_TAR:-}"

artifact_merge_bin_env() {
    prefix=$1
    eval "from_path=\${${prefix}_BIN_PATH:-}"
    eval "from_url=\${${prefix}_BIN_URL:-}"
    eval "from_ver=\${${prefix}_BIN_VERSION:-}"
    eval "from_skip=\${${prefix}_SKIP_CHECKSUM:-}"
    if [ -z "$ARTIFACT_BIN_PATH" ] && [ -n "$from_path" ]; then
        ARTIFACT_BIN_PATH=$from_path
    fi
    if [ -z "$ARTIFACT_BIN_URL" ] && [ -n "$from_url" ]; then
        ARTIFACT_BIN_URL=$from_url
    fi
    if [ -z "$ARTIFACT_BIN_VERSION" ] && [ -n "$from_ver" ]; then
        ARTIFACT_BIN_VERSION=$from_ver
    fi
    if [ "$ARTIFACT_SKIP_CHECKSUM" != 1 ] && [ "$from_skip" = 1 ]; then
        ARTIFACT_SKIP_CHECKSUM=1
    fi
}

artifact_merge_image_env() {
    prefix=$1
    eval "from_image=\${${prefix}_IMAGE:-}"
    eval "from_tar=\${${prefix}_IMAGE_TAR:-}"
    if [ -z "$ARTIFACT_IMAGE_REF" ] && [ -n "$from_image" ]; then
        ARTIFACT_IMAGE_REF=$from_image
    fi
    if [ -z "$ARTIFACT_IMAGE_TAR" ] && [ -n "$from_tar" ]; then
        ARTIFACT_IMAGE_TAR=$from_tar
    fi
}

artifact_assert_bin_sources_exclusive() {
    if [ -n "$ARTIFACT_BIN_PATH" ] && [ -n "$ARTIFACT_BIN_URL" ]; then
        err "不能同时指定 --bin-path 与 --bin-url。"
        exit 1
    fi
}

artifact_require_flag_value() {
    flag=$1
    value=${2:-}
    if [ -z "$value" ]; then
        err "参数 ${flag} 缺少取值"
        exit 1
    fi
    case $value in
        --*)
            err "参数 ${flag} 缺少取值"
            exit 1
            ;;
    esac
}

artifact_verify_checksum_file() {
    zipfile=$1
    zipname=$2
    sumsfile=$3
    expected=$(awk -v f="$zipname" '$2 == f {print $1; exit}' "$sumsfile")
    if [ -z "$expected" ]; then
        err "checksums 中没有 ${zipname} 的校验和。"
        return 1
    fi
    actual=$(sha256sum "$zipfile" | awk '{print $1}')
    if [ "$expected" != "$actual" ]; then
        err "SHA256 校验失败：期望 ${expected}，实际 ${actual}"
        return 1
    fi
    success "SHA256 校验通过。"
    return 0
}

artifact_checksum_url_from_zip_url() {
    url=$1
    case $url in
        */*)
            base=${url%/*}
            zipbase=${url##*/}
            case $zipbase in
                *.zip) printf '%s/checksums.txt\n' "$base" ;;
                *) printf '\n' ;;
            esac
            ;;
        *) printf '\n' ;;
    esac
}

artifact_local_checksum_file() {
    path=$1
    zipname=$2
    dir=$(dirname "$path")
    sums="${dir}/checksums.txt"
    if [ -f "$sums" ]; then
        artifact_verify_checksum_file "$path" "$zipname" "$sums"
        return $?
    fi
    return 0
}

artifact_github_release_version() {
    repo=$1
    if [ -n "$ARTIFACT_BIN_VERSION" ]; then
        case "$ARTIFACT_BIN_VERSION" in
            v*) printf '%s\n' "$ARTIFACT_BIN_VERSION" ;;
            *) printf 'v%s\n' "$ARTIFACT_BIN_VERSION" ;;
        esac
        return 0
    fi
    art_version=$(curl -fsSL -m 10 "https://api.github.com/repos/${repo}/releases/latest" | grep '"tag_name":' | head -n 1 | sed 's/.*"tag_name": "\(.*\)",.*/\1/')
    if [ -z "$art_version" ]; then
        err "获取版本失败，请检查网络是否能访问 GitHub API：${repo}"
        return 1
    fi
    printf '%s\n' "$art_version"
}

artifact_extract_zip_member() {
    art_zipfile=$1
    art_inner_name=$2
    art_dest=$3
    art_stage=$(mktemp -d)
    if ! unzip -qo "$art_zipfile" -d "$art_stage"; then
        err "解压失败: ${art_zipfile}"
        rm -rf "$art_stage"
        return 1
    fi
    art_inner="${art_stage}/${art_inner_name}"
    if [ ! -f "$art_inner" ]; then
        err "压缩包中未找到 ${art_inner_name}"
        rm -rf "$art_stage"
        return 1
    fi
    if [ "$art_inner" = "$art_dest" ]; then
        err "解压目标与压缩包内文件是同一路径: ${art_dest}"
        rm -rf "$art_stage"
        return 1
    fi
    if ! mv "$art_inner" "$art_dest"; then
        err "无法写入 ${art_dest}"
        rm -rf "$art_stage"
        return 1
    fi
    rm -rf "$art_stage"
    chmod +x "$art_dest" || return 1
    return 0
}

artifact_stage_from_path() {
    art_src=$1
    art_inner_name=$2
    art_zip_basename=$3
    art_dest=$4
    if [ ! -e "$art_src" ]; then
        err "本地路径不存在: ${art_src}"
        return 1
    fi
    case $art_src in
        *.zip)
            if ! artifact_local_checksum_file "$art_src" "$art_zip_basename"; then
                return 1
            fi
            artifact_extract_zip_member "$art_src" "$art_inner_name" "$art_dest"
            ;;
        *)
            if [ ! -f "$art_src" ]; then
                err "本地路径不是文件: ${art_src}"
                return 1
            fi
            if [ "$art_src" = "$art_dest" ]; then
                chmod +x "$art_dest" || return 1
                return 0
            fi
            cp "$art_src" "$art_dest" || return 1
            chmod +x "$art_dest" || return 1
            ;;
    esac
}

artifact_stage_from_url() {
    art_url=$1
    art_inner_name=$2
    art_zip_basename=$3
    art_dest=$4
    art_tmpzip=$(mktemp)
    art_tmpsums=$(mktemp)
    if ! curl -fsSL -m 180 -o "$art_tmpzip" "$art_url"; then
        err "下载失败: ${art_url}"
        rm -f "$art_tmpzip" "$art_tmpsums"
        return 1
    fi
    art_sums_url=$(artifact_checksum_url_from_zip_url "$art_url")
    if [ -n "$art_sums_url" ]; then
        if curl -fsSL -m 60 -o "$art_tmpsums" "$art_sums_url"; then
            if ! artifact_verify_checksum_file "$art_tmpzip" "$art_zip_basename" "$art_tmpsums"; then
                rm -f "$art_tmpzip" "$art_tmpsums"
                return 1
            fi
        elif [ "$ARTIFACT_SKIP_CHECKSUM" = 1 ]; then
            info "未获取 checksums.txt，已按 SANTAIZI_*_SKIP_CHECKSUM=1 跳过校验。"
        else
            err "无法下载 checksums.txt（${art_sums_url}）。若信任该 URL，请设置 SANTAIZI_*_SKIP_CHECKSUM=1。"
            rm -f "$art_tmpzip" "$art_tmpsums"
            return 1
        fi
    elif [ "$ARTIFACT_SKIP_CHECKSUM" = 1 ]; then
        info "未校验下载产物（SANTAIZI_*_SKIP_CHECKSUM=1）。"
    else
        err "第三方 URL 须能推导 checksums.txt，或设置 SANTAIZI_*_SKIP_CHECKSUM=1。"
        rm -f "$art_tmpzip" "$art_tmpsums"
        return 1
    fi
    rm -f "$art_tmpsums"
    if ! artifact_extract_zip_member "$art_tmpzip" "$art_inner_name" "$art_dest"; then
        rm -f "$art_tmpzip"
        return 1
    fi
    rm -f "$art_tmpzip"
    return 0
}

artifact_stage_from_github() {
    art_repo=$1
    art_os=$2
    art_arch=$3
    art_zip_basename=$4
    art_inner_name=$5
    art_dest=$6
    art_require_checksum=$7
    art_version=$(artifact_github_release_version "$art_repo") || return 1
    success "目标版本: ${art_version}"
    art_base="https://github.com/${art_repo}/releases/download/${art_version}"
    art_tmpzip=$(mktemp)
    art_tmpsums=$(mktemp)
    info "正在下载 ${art_base}/${art_zip_basename} ..."
    if ! curl -fsSL -m 180 -o "$art_tmpzip" "${art_base}/${art_zip_basename}"; then
        err "下载失败，请确认该版本已发布 ${art_zip_basename}。"
        rm -f "$art_tmpzip" "$art_tmpsums"
        return 1
    fi
    if [ "$art_require_checksum" = 1 ]; then
        if ! curl -fsSL -m 60 -o "$art_tmpsums" "${art_base}/checksums.txt"; then
            err "下载 checksums.txt 失败，无法校验产物完整性。"
            rm -f "$art_tmpzip" "$art_tmpsums"
            return 1
        fi
        if ! artifact_verify_checksum_file "$art_tmpzip" "$art_zip_basename" "$art_tmpsums"; then
            rm -f "$art_tmpzip" "$art_tmpsums"
            return 1
        fi
    fi
    rm -f "$art_tmpsums"
    if ! artifact_extract_zip_member "$art_tmpzip" "$art_inner_name" "$art_dest"; then
        rm -f "$art_tmpzip"
        return 1
    fi
    rm -f "$art_tmpzip"
    return 0
}

artifact_stage_dashboard() {
    art_repo=$1
    art_os=$2
    art_arch=$3
    art_dest=$4
    art_zip_basename="santaizi-dashboard-linux-${art_arch}.zip"
    art_inner_name="santaizi-dashboard-linux-${art_arch}"
    artifact_assert_bin_sources_exclusive
    if [ -n "$ARTIFACT_BIN_PATH" ]; then
        artifact_stage_from_path "$ARTIFACT_BIN_PATH" "$art_inner_name" "$art_zip_basename" "$art_dest"
        return $?
    fi
    if [ -n "$ARTIFACT_BIN_URL" ]; then
        artifact_stage_from_url "$ARTIFACT_BIN_URL" "$art_inner_name" "$art_zip_basename" "$art_dest"
        return $?
    fi
    artifact_stage_from_github "$art_repo" "$art_os" "$art_arch" "$art_zip_basename" "$art_inner_name" "$art_dest" 1
}

artifact_stage_agent_asset() {
    art_repo=$1
    art_os=$2
    art_arch=$3
    art_asset_name=$4
    art_dest=$5
    art_zip_basename="${art_asset_name}_${art_os}_${art_arch}.zip"
    art_inner_name=$art_asset_name
    artifact_assert_bin_sources_exclusive
    if [ -n "$ARTIFACT_BIN_PATH" ]; then
        artifact_stage_from_path "$ARTIFACT_BIN_PATH" "$art_inner_name" "$art_zip_basename" "$art_dest"
        return $?
    fi
    if [ -n "$ARTIFACT_BIN_URL" ]; then
        artifact_stage_from_url "$ARTIFACT_BIN_URL" "$art_inner_name" "$art_zip_basename" "$art_dest"
        return $?
    fi
    artifact_stage_from_github "$art_repo" "$art_os" "$art_arch" "$art_zip_basename" "$art_inner_name" "$art_dest" 0
}

artifact_verify_dashboard() {
    bin=$1
    output=$("$bin" --version 2>&1)
    if [ $? -eq 0 ]; then
        return 0
    fi
    err "面板二进制无法执行：${output:-无输出}"
    exit 1
}

artifact_verify_agent_rs() {
    bin=$1
    output=$("$bin" --version 2>&1)
    if [ $? -eq 0 ]; then
        return 0
    fi
    case $output in
        *GLIBC_*not\ found*)
            err "Rust 探针二进制无法启动：$(printf '%s' "$output" | head -n 1)"
            err "发布产物为静态 musl；若手动替换过二进制，请改用官方 Release 或 musl 构建产物。"
            ;;
        *)
            err "Rust 探针二进制无法执行：${output:-无输出}"
            err "请检查文件是否完整、架构是否匹配（仅 linux amd64 / arm64）。"
            ;;
    esac
    exit 1
}

compose_set_image_tag() {
    file=$1
    tag=$2
    tmp="${file}.tmp.$$"
    if ! sed -e "s|^\\([[:space:]]*image:[[:space:]]*.*\\):[^:[:space:]]*[[:space:]]*$|\\1:${tag}|" "$file" > "$tmp"; then
        rm -f "$tmp"
        err "更新 compose 镜像标签失败。"
        return 1
    fi
    if ! grep -Eq "^[[:space:]]*image:[[:space:]]*.*:${tag}[[:space:]]*$" "$tmp"; then
        rm -f "$tmp"
        err "未能在 compose 中写入镜像标签: ${tag}"
        return 1
    fi
    mv "$tmp" "$file"
    return 0
}

compose_set_image_ref() {
    file=$1
    ref=$2
    tmp="${file}.tmp.$$"
    escaped=$(printf '%s' "$ref" | sed 's/[&/\]/\\&/g')
    if ! sed -e "s|^[[:space:]]*image:.*|    image: ${escaped}|" "$file" > "$tmp"; then
        rm -f "$tmp"
        err "更新 compose 镜像引用失败。"
        return 1
    fi
    if ! grep -Fq "$ref" "$tmp"; then
        rm -f "$tmp"
        err "未能在 compose 中写入镜像: ${ref}"
        return 1
    fi
    mv "$tmp" "$file"
    return 0
}

artifact_docker_load_tar() {
    tarpath=$1
    if [ ! -f "$tarpath" ]; then
        err "镜像 tar 不存在: ${tarpath}"
        return 1
    fi
    info "正在加载镜像: ${tarpath} ..."
    if ! docker load -i "$tarpath"; then
        err "docker load 失败。"
        return 1
    fi
    return 0
}
