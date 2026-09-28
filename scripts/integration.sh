#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.cargo/bin:$PATH"
docker compose -f deploy/compose.test.yml up -d --wait
uv run --with pyarrow==21.0.0 --with boto3==1.40.40 scripts/seed.py
source scripts/test-env.sh
cargo test --test federation -- --ignored --skip cloud_connectors
cargo test --test clickhouse -- --ignored --test-threads=1
