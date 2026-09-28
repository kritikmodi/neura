#!/usr/bin/env bash
# Keep large compiler/cache outputs in this repository, outside Docker's VM disk.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p target/linux-cargo target/linux-build
docker build -f deploy/Dockerfile.toolchain -t neura-build:1.98.1 .
docker run --rm --name neura-build --cpus 4 --memory 8g \
 -v "$PWD:/work" -v "$PWD/target/linux-cargo:/cargo-cache" -v "$PWD/target/linux-build:/build-target" \
 -w /work -e RUSTUP_TOOLCHAIN=1.98.1 -e CARGO_HOME=/cargo-cache -e CARGO_TARGET_DIR=/build-target \
 neura-build:1.98.1 cargo build --release --locked -j 4 --features "${NEURA_CARGO_FEATURES:-}"
docker build -f deploy/Dockerfile.prebuilt -t "${NEURA_IMAGE:-neura:0.1.0}" .
