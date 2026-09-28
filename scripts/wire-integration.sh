#!/usr/bin/env bash
# Optional Cube/pgwire compatibility. Run scripts/integration.sh first.
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.cargo/bin:$PATH"
source scripts/test-env.sh
export NEURA_TEST_PG_PORT=55433
cargo build --locked --features postgres-wire
python3 - <<'PY'
from pathlib import Path
import socket
for port in (55433, 58080):
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', port))  # Refuse to test somebody else's running server.
s=Path('data/test.toml').read_text().replace('[server]', '[server]\npg_addr="127.0.0.1:55433"')
Path('data/wire-test.toml').write_text(s)
PY
npm install --prefix target/cube-test --no-audit --no-fund @cubejs-backend/postgres-driver@1.6.57 pg@8.16.3
target/debug/neura -c data/wire-test.toml serve >data/wire-test.log 2>&1 &
neura_test_pid=$!
trap 'kill -INT "$neura_test_pid" 2>/dev/null || true; wait "$neura_test_pid" 2>/dev/null || true' EXIT
python3 - <<'PY'
import time, urllib.request
for _ in range(100):
    try:
        urllib.request.urlopen('http://127.0.0.1:58080/healthz',timeout=1).read()
        break
    except OSError: time.sleep(.1)
else: raise RuntimeError('Neura did not become ready')
PY
kill -0 "$neura_test_pid"
NODE_PATH="$PWD/target/cube-test/node_modules" node scripts/wire-test.cjs
kill -0 "$neura_test_pid"
