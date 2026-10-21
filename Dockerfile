# syntax=docker/dockerfile:1
# 多阶段：pnpm 构建前端 → 拉 GeoIP 真库 → cargo release。运行层只留二进制 + ca-certificates
# （GeoIP 自愈下载与通知外呼需要 TLS 根证书）。
# 构建阶段固定 $BUILDPLATFORM 原生执行并交叉编译目标架构，避免 QEMU 模拟 cargo。

FROM --platform=$BUILDPLATFORM node:24-bookworm AS web
WORKDIR /src
RUN npm install -g pnpm@10.15.1
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.base.json ./
COPY web/ web/
RUN pnpm install --frozen-lockfile \
    && pnpm build

FROM --platform=$BUILDPLATFORM rust:1-bookworm AS build
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler curl ca-certificates \
    && if [ "$TARGETARCH" = "arm64" ]; then \
         apt-get install -y --no-install-recommends gcc-aarch64-linux-gnu g++-aarch64-linux-gnu; \
         rustup target add aarch64-unknown-linux-gnu; \
       fi \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
# 版本号：默认取 Cargo.toml（CI 在 checkout 里同步成 tag 版本）；本地构建可用
# --build-arg SANTAIZI_VERSION=v0.1.4 覆盖。空值即回退，见 build.rs。
ARG SANTAIZI_VERSION=""
ENV SANTAIZI_BUILD_VERSION=${SANTAIZI_VERSION}
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto/ proto/
COPY src/ src/
COPY resource/ resource/
COPY script/ script/
# resource/web/ 不入库，用构建产物覆盖占位。
COPY --from=web /src/resource/web/ resource/web/
RUN bash script/fetch-geoip.sh -o resource/geoip.db
RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) triple=x86_64-unknown-linux-gnu ;; \
      arm64) \
        triple=aarch64-unknown-linux-gnu; \
        export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
               CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
               CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++ ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    cargo build --release --locked --target "$triple"; \
    cp "target/${triple}/release/santaizi-dashboard" /santaizi-dashboard

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /santaizi-dashboard /usr/local/bin/santaizi-dashboard
EXPOSE 80 5555
ENTRYPOINT ["santaizi-dashboard", "--config", "/etc/santaizi/dashboard.yaml", "--db", "/var/lib/santaizi-dashboard/sqlite.db"]
