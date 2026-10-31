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
    version=$(curl -fsSL -m 10 "https://api.github.com/repos/${repo}/releases/latest" | grep '"tag_name":' | head -n 1 | sed 's/.*"tag_name": "\(.*\)",.*/\1/')
    if [ -z "$version" ]; then
        err "获取版本失败，请检查网络是否能访问 GitHub API：${repo}"
        return 1
    fi
    printf '%s\n' "$version"
}

artifact_extract_zip_member() {
    zipfile=$1
    inner_name=$2
    dest=$3
    stage=$(mktemp -d)
    if ! unzip -qo "$zipfile" -d "$stage"; then
        err "解压失败: ${zipfile}"
        rm -rf "$stage"
        return 1
    fi
    inner="${stage}/${inner_name}"
    if [ ! -f "$inner" ]; then
        err "压缩包中未找到 ${inner_name}"
        rm -rf "$stage"
        return 1
    fi
    mv "$inner" "$dest"
    rm -rf "$stage"
    return 0
}

artifact_stage_from_path() {
    src=$1
    inner_name=$2
    zip_basename=$3
    dest=$4
    if [ ! -e "$src" ]; then
        err "本地路径不存在: ${src}"
        return 1
    fi
    case $src in
        *.zip)
            if ! artifact_local_checksum_file "$src" "$zip_basename"; then
                return 1
            fi
            artifact_extract_zip_member "$src" "$inner_name" "$dest"
            ;;
        *)
            if [ ! -f "$src" ]; then
                err "本地路径不是文件: ${src}"
                return 1
            fi
            cp "$src" "$dest"
            chmod +x "$dest"
            ;;
    esac
}

artifact_stage_from_url() {
    url=$1
    inner_name=$2
    zip_basename=$3
    dest=$4
    tmpzip=$(mktemp)
    tmpsums=$(mktemp)
    if ! curl -fsSL -m 180 -o "$tmpzip" "$url"; then
        err "下载失败: ${url}"
        rm -f "$tmpzip" "$tmpsums"
        return 1
    fi
    sums_url=$(artifact_checksum_url_from_zip_url "$url")
    if [ -n "$sums_url" ]; then
        if curl -fsSL -m 60 -o "$tmpsums" "$sums_url"; then
            if ! artifact_verify_checksum_file "$tmpzip" "$zip_basename" "$tmpsums"; then
                rm -f "$tmpzip" "$tmpsums"
                return 1
            fi
        elif [ "$ARTIFACT_SKIP_CHECKSUM" = 1 ]; then
            info "未获取 checksums.txt，已按 SANTAIZI_*_SKIP_CHECKSUM=1 跳过校验。"
        else
            err "无法下载 checksums.txt（${sums_url}）。若信任该 URL，请设置 SANTAIZI_*_SKIP_CHECKSUM=1。"
            rm -f "$tmpzip" "$tmpsums"
            return 1
        fi
    elif [ "$ARTIFACT_SKIP_CHECKSUM" = 1 ]; then
        info "未校验下载产物（SANTAIZI_*_SKIP_CHECKSUM=1）。"
    else
        err "第三方 URL 须能推导 checksums.txt，或设置 SANTAIZI_*_SKIP_CHECKSUM=1。"
        rm -f "$tmpzip" "$tmpsums"
        return 1
    fi
    rm -f "$tmpsums"
    if ! artifact_extract_zip_member "$tmpzip" "$inner_name" "$dest"; then
        rm -f "$tmpzip"
        return 1
    fi
    rm -f "$tmpzip"
    chmod +x "$dest"
    return 0
}

artifact_stage_from_github() {
    repo=$1
    os=$2
    arch=$3
    zip_basename=$4
    inner_name=$5
    dest=$6
    require_checksum=$7
    version=$(artifact_github_release_version "$repo") || return 1
    success "目标版本: ${version}"
    base="https://github.com/${repo}/releases/download/${version}"
    tmpzip=$(mktemp)
    tmpsums=$(mktemp)
    info "正在下载 ${base}/${zip_basename} ..."
    if ! curl -fsSL -m 180 -o "$tmpzip" "${base}/${zip_basename}"; then
        err "下载失败，请确认该版本已发布 ${zip_basename}。"
        rm -f "$tmpzip" "$tmpsums"
        return 1
    fi
    if [ "$require_checksum" = 1 ]; then
        if ! curl -fsSL -m 60 -o "$tmpsums" "${base}/checksums.txt"; then
            err "下载 checksums.txt 失败，无法校验产物完整性。"
            rm -f "$tmpzip" "$tmpsums"
            return 1
        fi
        if ! artifact_verify_checksum_file "$tmpzip" "$zip_basename" "$tmpsums"; then
            rm -f "$tmpzip" "$tmpsums"
            return 1
        fi
    fi
    rm -f "$tmpsums"
    if ! artifact_extract_zip_member "$tmpzip" "$inner_name" "$dest"; then
        rm -f "$tmpzip"
        return 1
    fi
    rm -f "$tmpzip"
    chmod +x "$dest"
    return 0
}

artifact_stage_dashboard() {
    repo=$1
    os=$2
    arch=$3
    dest=$4
    zip_basename="santaizi-dashboard-linux-${arch}.zip"
    inner_name="santaizi-dashboard-linux-${arch}"
    artifact_assert_bin_sources_exclusive
    if [ -n "$ARTIFACT_BIN_PATH" ]; then
        artifact_stage_from_path "$ARTIFACT_BIN_PATH" "$inner_name" "$zip_basename" "$dest"
        return $?
    fi
    if [ -n "$ARTIFACT_BIN_URL" ]; then
        artifact_stage_from_url "$ARTIFACT_BIN_URL" "$inner_name" "$zip_basename" "$dest"
        return $?
    fi
    artifact_stage_from_github "$repo" "$os" "$arch" "$zip_basename" "$inner_name" "$dest" 1
}

artifact_stage_agent_asset() {
    repo=$1
    os=$2
    arch=$3
    asset_name=$4
    dest=$5
    zip_basename="${asset_name}_${os}_${arch}.zip"
    inner_name=$asset_name
    artifact_assert_bin_sources_exclusive
    if [ -n "$ARTIFACT_BIN_PATH" ]; then
        artifact_stage_from_path "$ARTIFACT_BIN_PATH" "$inner_name" "$zip_basename" "$dest"
        return $?
    fi
    if [ -n "$ARTIFACT_BIN_URL" ]; then
        artifact_stage_from_url "$ARTIFACT_BIN_URL" "$inner_name" "$zip_basename" "$dest"
        return $?
    fi
    tmpdir=$(mktemp -d 2>/dev/null || mktemp -d -t santaizi-agent-stage)
    staged="${tmpdir}/${inner_name}"
    if ! artifact_stage_from_github "$repo" "$os" "$arch" "$zip_basename" "$inner_name" "$staged" 0; then
        rm -rf "$tmpdir"
        return 1
    fi
    mv "$staged" "$dest"
    rm -rf "$tmpdir"
    chmod +x "$dest"
    return 0
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
