fn main() {
    let proto = "proto/santaizi.proto";
    println!("cargo:rerun-if-changed={proto}");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto], &["proto"])
        .expect("compile santaizi.proto");
    // 版本号跟随 git tag：仓库内构建报告最近 tag 的版本号，避免「tag 已发
    // v0.1.4、二进制还报 Cargo.toml 里的 0.1.2」。CI 发版另在 checkout 后
    // sed 同步 Cargo.toml（release.yml "Sync crate version from tag"），
    // 两条路径互为兜底；Docker 构建上下文不含 .git，用 SANTAIZI_VERSION
    // build-arg 显式传入。
    if let Some(version) = git_tag_version() {
        println!("cargo:rustc-env=SANTAIZI_BUILD_VERSION={version}");
    }
    if std::path::Path::new(".git").exists() {
        // tag 变化（新打 tag）要能触发重链，否则版本号会停在首次构建的值。
        println!("cargo:rerun-if-changed=.git/HEAD");
        println!("cargo:rerun-if-changed=.git/refs/tags");
    }
}

/// `git describe --tags --abbrev=0` 去掉 `v` 前缀。
/// 无 git、无 tag 或非 git 检出（Docker 上下文）时返回 `None`，
/// 由 `CARGO_PKG_VERSION` 兜底。
fn git_tag_version() -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["describe", "--tags", "--abbrev=0"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let tag = String::from_utf8(output.stdout).ok()?;
    let version = tag.trim().strip_prefix('v').unwrap_or_else(|| tag.trim());
    if version.is_empty() {
        None
    } else {
        Some(version.to_string())
    }
}
