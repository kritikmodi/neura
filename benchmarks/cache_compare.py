#!/usr/bin/env python3
"""A separately labelled DuckDB default-file-cache profile, after the base suite."""

import json
import os
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]
os.chdir(ROOT)
name = "neura-cache-duckdb"
for base in ("neura", "spark", "duckdb"):
    running = subprocess.check_output(["docker", "inspect", "--format", "{{.State.Running}}",
        "neura-compare-" + base], text=True).strip()
    assert running == "false", "Finish the main suite before the cache profile"
original = json.loads(subprocess.check_output(["docker", "inspect", "neura-compare-duckdb"]))[0]
temp = ROOT / "data/cache-runtime/duckdb"
temp.mkdir(parents=True, exist_ok=True)
temp.chmod(0o1777)
exists = subprocess.run(["docker", "inspect", name], capture_output=True).returncode == 0
if not exists:
    args = ["docker", "create", "--name", name, "--network", "neura-test_default", "--cpus", "2",
        "--memory", "2g", "--memory-swap", "2g", "--user", "10001:10001", "-p", "127.0.0.1:58104:8080"]
    for bind in original["HostConfig"]["Binds"]:
        if bind.endswith(":/tmp"):
            bind = str(temp) + ":/tmp"
        args += ["-v", bind]
    for env in original["Config"]["Env"]:
        if not env.startswith("BENCH_DUCKDB_FILE_CACHE="):
            args += ["-e", env]
    args += ["-e", "BENCH_DUCKDB_FILE_CACHE=true", "--entrypoint", "python3",
        original["Config"]["Image"], "/bench/compare_server.py"]
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL)
environment = dict(os.environ, NEURA_BENCH_PREFIX="neura-cache-", NEURA_BENCH_DUCKDB_PORT="58104",
    NEURA_BENCH_TMP_ROOT=str(ROOT / "data/cache-runtime"))
subprocess.run(["python3", "benchmarks/compare.py", "--engines", "duckdb", "--workloads",
    "parquet_count", "federated_group", "federated_group_coarse", "local_high_cardinality",
    "--repeats", "5", "--throughput-queries", "20", "--output", "results/duckdb-file-cache.json"],
    env=environment, check=True)
report_path = ROOT / "results/duckdb-file-cache.json"
report = json.loads(report_path.read_text())
assert report["engines"]["duckdb"]["metadata"]["settings"]["enable_external_file_cache"] == "true"
report["profile"] = "DuckDB default external file cache enabled; separate from the no-file-cache baseline"
report_path.write_text(json.dumps(report, indent=2))
