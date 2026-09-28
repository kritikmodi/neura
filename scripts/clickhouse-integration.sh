#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.cargo/bin:$PATH"
docker compose -f deploy/compose.test.yml up -d --wait clickhouse
source scripts/test-env.sh
cargo test --test clickhouse -- --ignored --test-threads=1
