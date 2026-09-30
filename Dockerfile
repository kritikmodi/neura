# syntax=docker/dockerfile:1
FROM rust:1.98.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo .cargo
COPY src src
ARG CARGO_FEATURES=""
ARG CARGO_BUILD_JOBS=2
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked --jobs "$CARGO_BUILD_JOBS" --features "$CARGO_FEATURES" && cp target/release/neura /neura
FROM debian:bookworm-slim
ARG NEURA_VERSION=dev
ARG NEURA_REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/kritikmodi/neura" \
    org.opencontainers.image.title="Neura" \
    org.opencontainers.image.description="Read-only federated SQL engine" \
    org.opencontainers.image.licenses="Apache-2.0" \
    org.opencontainers.image.version="$NEURA_VERSION" \
    org.opencontainers.image.revision="$NEURA_REVISION"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 neura && useradd --uid 10001 --gid 10001 --no-create-home neura \
    && mkdir -p /var/lib/neura/spill && chown -R neura:neura /var/lib/neura
COPY --from=build /neura /usr/local/bin/neura
USER 10001:10001
WORKDIR /var/lib/neura
EXPOSE 8080
ENTRYPOINT ["neura"]
STOPSIGNAL SIGINT
CMD ["--config","/etc/neura/neura.toml","serve"]
