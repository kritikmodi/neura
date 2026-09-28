#!/usr/bin/env python3
"""Prepare only isolated Neura benchmark containers. Credentials are fixture-only."""

import hashlib, json, os, pathlib, platform, subprocess, sys, urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
os.chdir(ROOT)


def run(*args):
    subprocess.run(args, check=True)


def download(url, path, digest):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        urllib.request.urlretrieve(url, path)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest, (
        f"Checksum failed: {path}"
    )


# The measured baseline is arm64. Refuse a silent architecture change.
assert platform.machine() in ("arm64", "aarch64"), (
    "This baseline setup is pinned to arm64 Linux containers."
)
run("uv", "pip", "install", "--target", "target/spark-vendor", "pyspark==3.5.6")
jre = ROOT / "target/benchmark-java/jre.tar.gz"
download(
    "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.16%2B8/OpenJDK17U-jre_aarch64_linux_hotspot_17.0.16_8.tar.gz",
    jre,
    "98f9d60be880b6ec551f5f1fcd784971aa573e8d8f5b9923fb0148c25afcd161",
)
if not (jre.parent / "jdk-17.0.16+8-jre").exists():
    run("tar", "-xzf", str(jre), "-C", str(jre.parent))
for name, suffix, digest in [
    (
        "postgresql",
        "org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar",
        "157963d60ae66d607e09466e8c0cdf8087e9cb20d0159899ffca96bca2528460",
    ),
    (
        "hadoop-aws",
        "org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
        "53f9ae03c681a30a50aa17524bd9790ab596b28481858e54efd989a826ed3a4a",
    ),
    (
        "aws-java-sdk-bundle",
        "com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
        "873fe7cf495126619997bec21c44de5d992544aea7e632fdc77adb1a0915bae5",
    ),
]:
    download(
        "https://repo.maven.apache.org/maven2/" + suffix,
        ROOT / f"target/benchmark-jars/{name}.jar",
        digest,
    )
run(
    "docker",
    "build",
    "-f",
    "benchmarks/Dockerfile.spark-runtime",
    "-t",
    "neura-spark-runtime:3.5.6",
    ".",
)
if "--runtime-only" in sys.argv:
    raise SystemExit(0)
config = (
    (ROOT / "data/test.toml")
    .read_text()
    .replace("memory_mb=256", "memory_mb=512")
    .replace("127.0.0.1:58080", "0.0.0.0:8080")
    .replace("[server]", "[server]\nallow_insecure_network=true")
    .replace("http://127.0.0.1:59000", "http://s3:8333")
)
(ROOT / "data/neura-benchmark.toml").write_text(config)
qe_config = {
    "connectors": {
        "pg": {
            "type": "postgresql",
            "connection": "postgresql://neura_reader:neura-test-reader-only@postgres:5432/neura",
            "pool_min": 1,
            "pool_max": 4,
        },
        "lake": {
            "type": "parquet",
            "base_path": "s3://neura-fixture/",
            "endpoint": "http://s3:8333",
            "region": "us-east-1",
            "access_key": "neura-reader",
            "secret_key": "neura-test-readonly-storage",
        },
    }
}
(ROOT / "data/qe-benchmark.json").write_text(json.dumps(qe_config))


def start(name, args):
    # No replacement/deletion of an existing container, even one with our name.
    exists = (
        subprocess.run(["docker", "inspect", name], capture_output=True).returncode == 0
    )
    if exists:
        raise SystemExit(
            f"{name} exists. Stop/remove that benchmark container explicitly before preparing a new run."
        )
    run(
        "docker",
        "run",
        "-d",
        "--name",
        name,
        "--network",
        "neura-test_default",
        "--cpus",
        "2",
        "--memory",
        "2g",
        "--memory-swap",
        "2g",
        *args,
    )


start(
    "neura-bench-neura",
    [
        "-p",
        "127.0.0.1:58082:8080",
        "-v",
        f"{ROOT}/data/neura-benchmark.toml:/config/neura.toml:ro",
        "-e",
        "NEURA_TEST_POSTGRES_DSN=host=postgres port=5432 user=neura_reader password=neura-test-reader-only dbname=neura sslmode=disable",
        "-e",
        "AWS_ACCESS_KEY_ID=neura-reader",
        "-e",
        "AWS_SECRET_ACCESS_KEY=neura-test-readonly-storage",
        "-e",
        "AWS_REGION=us-east-1",
        "-e",
        "NEURA_API_TOKEN",
        "neura:0.1.0",
        "-c",
        "/config/neura.toml",
        "serve",
    ],
)
start(
    "neura-bench-qe",
    [
        "-p",
        "127.0.0.1:56433:5433",
        "-p",
        "127.0.0.1:58888:8088",
        "-v",
        f"{ROOT}/data/qe-benchmark.json:/config/qe.json:ro",
        os.environ.get("QE_IMAGE", "datatailr/query-engine:0.1.0-pre300-arm64"),
        "--config",
        "/config/qe.json",
        "--rest-auth",
        "none",
        "--threads",
        "2",
        "--pool-memory-mb",
        "512",
        "--data-cache-mb",
        "0",
        "--result-cache-mb",
        "0",
        "--auto-optimize",
        "off",
    ],
)
start(
    "neura-bench-spark",
    [
        "-p",
        "127.0.0.1:58081:8080",
        "-v",
        f"{ROOT}/target/benchmark-java/jdk-17.0.16+8-jre:/runtime/java:ro",
        "-v",
        f"{ROOT}/target/spark-vendor:/runtime/python:ro",
        "-v",
        f"{ROOT}/target/benchmark-jars:/jars:ro",
        "-v",
        f"{ROOT}/benchmarks/spark_server.py:/bench/spark_server.py:ro",
        "-e",
        "PYSPARK_SUBMIT_ARGS=--driver-memory 512m pyspark-shell",
        "-e",
        "SPARK_LOCAL_IP=127.0.0.1",
        "neura-spark-runtime:3.5.6",
    ],
)
