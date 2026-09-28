# syntax=docker/dockerfile:1
FROM rust:1.98.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo .cargo
COPY src src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked && cp target/release/neura /neura
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 neura && useradd --uid 10001 --gid 10001 --no-create-home neura \
    && mkdir -p /var/lib/neura/spill && chown -R neura:neura /var/lib/neura
COPY --from=build /neura /usr/local/bin/neura
USER 10001:10001
WORKDIR /var/lib/neura
EXPOSE 8080 5433
ENTRYPOINT ["neura"]
STOPSIGNAL SIGINT
CMD ["--config","/etc/neura/neura.toml","serve"]
