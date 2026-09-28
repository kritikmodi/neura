#!/usr/bin/env python3
"""Prepare isolated Neura/Spark/DuckDB containers; never replace existing containers."""

import hashlib
import json
import os
import pathlib
import platform
import subprocess
import urllib.request
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
os.chdir(ROOT)
assert platform.machine() in ("arm64", "aarch64"), "Pinned Linux arm64 runtime required"
assert len(os.environ.get("NEURA_API_TOKEN", "")) >= 24, "Set NEURA_API_TOKEN"


def run(*args):
    subprocess.run(args, check=True)


names = ["neura-compare-" + e for e in ("neura", "spark", "duckdb")]
for name in names:
    if subprocess.run(["docker", "inspect", name], capture_output=True).returncode == 0:
        raise SystemExit(f"{name} exists; refusing to replace it. Reuse it or remove only this benchmark container explicitly.")
runtime = "neura-spark-runtime:3.5.6"
if subprocess.run(["docker", "image", "inspect", runtime], capture_output=True).returncode:
    run("python3", "benchmarks/prepare.py", "--runtime-only")

wheel = ROOT / "target/compare/duckdb-1.5.5-cp311-cp311-manylinux_2_26_aarch64.manylinux_2_28_aarch64.whl"
wheel.parent.mkdir(parents=True, exist_ok=True)
expected = "2e72f9e1a4f90a5c8483ad4d540e495bf0834ba61c360b52499a573d7ed62a3f"
if not wheel.exists():
    meta = json.load(urllib.request.urlopen("https://pypi.org/pypi/duckdb/1.5.5/json"))
    selected = next(f for f in meta["urls"] if f["filename"] == wheel.name)
    assert selected["digests"]["sha256"] == expected
    urllib.request.urlretrieve(selected["url"], wheel)
assert hashlib.sha256(wheel.read_bytes()).hexdigest() == expected
with zipfile.ZipFile(wheel) as archive:
    archive.extractall(ROOT / "target/duckdb-vendor")
(ROOT / "target/duckdb-extensions").mkdir(exist_ok=True)
run("docker", "run", "--rm", "-v", f"{ROOT}/target/duckdb-vendor:/python:ro",
    "-v", f"{ROOT}/target/duckdb-extensions:/extensions", "-e", "PYTHONPATH=/python",
    "--entrypoint", "python3", runtime, "-c",
    "import duckdb; c=duckdb.connect(config={'extension_directory':'/extensions'}); "
    "c.execute('INSTALL httpfs FROM core; INSTALL postgres FROM core')")
run("docker", "run", "--rm", "-v", f"{ROOT}/benchmarks:/bench:ro",
    "-v", f"{ROOT}/target/compare:/output", "--entrypoint", "gcc", runtime,
    "-O2", "-Wall", "-Wextra", "-Werror", "-std=c11", "/bench/memmon.c", "-o", "/output/memmon")
run("uv", "run", "--with", "pyarrow==21.0.0", "--with", "boto3==1.40.40", "benchmarks/compare_fixture.py")

for engine, port in [("neura", 58102), ("spark", 58101), ("duckdb", 58103)]:
    temp = ROOT / f"data/compare-runtime/{engine}"
    temp.mkdir(parents=True, exist_ok=True)
    temp.chmod(0o1777)
    args = ["docker", "create", "--name", "neura-compare-" + engine,
        "--network", "neura-test_default", "--cpus", "2", "--memory", "2g", "--memory-swap", "2g",
        "--user", "65534:65534" if engine == "spark" else "10001:10001", "-p", f"127.0.0.1:{port}:8080",
        "-v", f"{ROOT}/benchmarks:/bench:ro", "-v", f"{ROOT}/data/compare:/fixture:ro",
        "-v", f"{ROOT}/target/compare/memmon:/memmon:ro", "-v", f"{temp}:/tmp",
        "-e", "NEURA_API_TOKEN"]
    if engine == "neura":
        args += ["-e", "NEURA_TEST_POSTGRES_DSN=host=postgres port=5432 user=neura_reader password=neura-test-reader-only dbname=neura sslmode=disable",
            "-e", "AWS_ACCESS_KEY_ID=neura-reader", "-e", "AWS_SECRET_ACCESS_KEY=neura-test-readonly-storage",
            "-e", "AWS_REGION=us-east-1", "neura:0.1.0", "-c", "/fixture/neura.toml", "serve"]
    else:
        args += ["-e", f"BENCH_ENGINE={engine}", "--entrypoint", "python3"]
        if engine == "spark":
            args += ["-v", f"{ROOT}/target/benchmark-java/jdk-17.0.16+8-jre:/runtime/java:ro",
                "-v", f"{ROOT}/target/spark-vendor:/runtime/python:ro",
                "-v", f"{ROOT}/target/benchmark-jars:/jars:ro",
                "-e", "PYSPARK_SUBMIT_ARGS=--driver-memory 512m pyspark-shell", "-e", "SPARK_LOCAL_IP=127.0.0.1"]
        else:
            args += ["-v", f"{ROOT}/target/duckdb-vendor:/python:ro",
                "-v", f"{ROOT}/target/duckdb-extensions:/extensions:ro", "-e", "PYTHONPATH=/python"]
        args += [runtime, "/bench/compare_server.py"]
    run(*args)
print("Prepared stopped benchmark containers. Run benchmarks/compare.py to measure them sequentially.")
