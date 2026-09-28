#!/usr/bin/env python3
"""Create isolated, equal-resource fixtures/profiles on the selected Linux host.

Never modifies an existing container or connects to an existing source database.
Run fixtures first, then engines after building Neura natively on this host.
"""
import argparse
import hashlib
import json
import os
import pathlib
import platform
import secrets
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data/qe-host"
PREFIX = "neura-hz-20260928"


def run(*args, capture=False):
    return subprocess.check_output(args, text=True).strip() if capture else subprocess.run(args, check=True)


def inspect(name):
    return json.loads(run("docker", "inspect", name, capture=True))[0]


def absent(name):
    assert subprocess.run(["docker", "inspect", name], capture_output=True).returncode != 0, f"Refusing to replace {name}"


def snapshot():
    names = run("docker", "ps", "--format", "{{.Names}}", capture=True).splitlines()
    names = [name for name in names if not name.startswith(PREFIX + "-") and name != "neura-hetzner-build-20260928"]
    return {name: {"image_id": (d := inspect(name))["Image"], "pid": d["State"]["Pid"],
        "started_at": d["State"]["StartedAt"], "restart_count": d["RestartCount"]} for name in names}


def fixtures():
    import boto3
    assert platform.system() == "Linux", "Run on the selected Linux benchmark host"
    assert not (DATA / "state.json").exists(), "Fixture already prepared; never reseed active fixtures"
    for suffix in ("postgres", "s3"):
        absent(PREFIX + "-" + suffix)
    assert subprocess.run(["docker", "network", "inspect", PREFIX + "-net"], capture_output=True).returncode != 0
    DATA.mkdir(parents=True, exist_ok=True)
    before = snapshot()
    token = DATA / "api-token"
    token.write_text(secrets.token_hex(32))
    token.chmod(0o600)
    run("docker", "network", "create", PREFIX + "-net")
    for name, image in (("postgres", "postgres:17.6"), ("s3", "chrislusf/seaweedfs:3.94")):
        run("docker", "pull", image)
        directory = DATA / name
        directory.mkdir()
        memory = "512m" if name == "postgres" else "1g"
        args = ["docker", "run", "-d", "--name", PREFIX + "-" + name, "--network", PREFIX + "-net",
            "--network-alias", name, "--cpus", "1", "--memory", memory, "--memory-swap", memory]
        if name == "postgres":
            args += ["-e", "POSTGRES_PASSWORD=neura-test-admin-only", "-e", "POSTGRES_DB=neura",
                "-v", f"{ROOT}/deploy/init.sql:/docker-entrypoint-initdb.d/01-init.sql:ro",
                "-v", f"{directory}:/var/lib/postgresql/data", image]
        else:
            args += ["-e", "GOMEMLIMIT=512MiB", "-p", "127.0.0.1:58400:8333", "-v", f"{ROOT}/deploy/s3.test.json:/etc/neura-s3.json:ro",
                "-v", f"{directory}:/data", image, "server", "-s3", "-s3.port=8333",
                "-s3.config=/etc/neura-s3.json", "-ip=s3", "-ip.bind=0.0.0.0", "-dir=/data",
                "-volume.max=20", "-master.volumeSizeLimitMB=64", "-master.volumePreallocate=false",
                "-volume.minFreeSpace=10MiB"]
        run(*args)
    s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:58400", region_name="us-east-1",
        aws_access_key_id="neura-test", aws_secret_access_key="neura-test-storage-only")
    for _ in range(90):
        try:
            s3.list_buckets()
            p = subprocess.run(["docker", "exec", PREFIX + "-postgres", "psql", "-U", "postgres", "-d", "neura",
                "-Atc", "SELECT COUNT(*) FROM customers"], capture_output=True, text=True)
            if p.returncode == 0 and p.stdout.strip() == "200":
                break
        except Exception:
            pass
        time.sleep(1)
    else:
        raise RuntimeError("Dedicated sources did not become ready")
    s3.create_bucket(Bucket="neura-fixture")
    original = json.loads((ROOT / "data/compare/manifest.json").read_text())
    manifest = {"rows": original["rows"], "small": [], "large": []}
    for layout, table in (("small", "orders"), ("large", "orders_coarse")):
        for i, entry in enumerate(original[layout]):
            path = ROOT / f"data/compare/{layout}-{i}.parquet"
            body = path.read_bytes()
            assert hashlib.sha256(body).hexdigest() == entry["sha256"], "Fixture bytes changed during transfer"
            key = f"{table}/part-{i}.parquet"
            s3.put_object(Bucket="neura-fixture", Key=key, Body=body)
            etag = s3.head_object(Bucket="neura-fixture", Key=key)["ETag"]
            manifest[layout].append({**entry, "path": key, "etag": etag})
    local = DATA / "local/orders"
    local.mkdir(parents=True)
    for i in range(2):
        os.link(ROOT / f"data/compare/large-{i}.parquet", local / f"part-{i}.parquet")
    (DATA / "manifest.json").write_text(json.dumps(manifest, indent=2))
    state = {"prefix": PREFIX, "sources": [PREFIX + "-postgres", PREFIX + "-s3"], "existing_services_before": before,
        "fixture": manifest, "profiles": {}}
    (DATA / "state.json").write_text(json.dumps(state, indent=2))
    print("Dedicated fixtures prepared; exact transferred Parquet hashes verified.")


def engines(args):
    state = json.loads((DATA / "state.json").read_text())
    assert not set(args.engines) & state["profiles"].keys(), "Requested profiles already exist; refusing replacement"
    run("gcc", "-O2", "-static", "-Wall", "-Wextra", "-Werror", "-std=c11",
        str(ROOT / "benchmarks/memmon.c"), "-o", str(DATA / "memmon"))
    manifest = state["fixture"]
    for name, image, port in [("neura", args.neura_image, 58402),
        ("qe305", args.qe305_image, 58403), ("qe341", args.qe341_image, 58404)]:
        if name not in args.engines:
            continue
        absent(PREFIX + "-" + name)
        image_id = json.loads(run("docker", "image", "inspect", image, capture=True))[0]["Id"]
        if name == "qe305":
            reference = state["existing_services_before"].get("phase1-qe")
            assert reference and reference["image_id"] == image_id, "Primary QE image must match the running reference service"
        temp = DATA / f"runtime/{name}"
        temp.mkdir(parents=True, exist_ok=True)
        temp.chmod(0o1777)
        command = ["docker", "create", "--name", PREFIX + "-" + name, "--network", PREFIX + "-net",
            "--cpus", "2", "--memory", "2g", "--memory-swap", "2g", "-v", f"{temp}:/tmp",
            "-v", f"{DATA}/local:/fixture/local:ro", "-v", f"{DATA}/memmon:/memmon:ro"]
        if name == "neura":
            text = (ROOT / "data/compare/neura.toml").read_text()
            for layout in ("small", "large"):
                for old, new in zip(json.loads((ROOT / "data/compare/manifest.json").read_text())[layout], manifest[layout]):
                    text = text.replace(old["path"], new["path"]).replace(json.dumps(old["etag"]), json.dumps(new["etag"]))
            text = text.replace('/fixture/large-', '/fixture/local/orders/part-')
            text = text.replace("[limits]", "[limits]\nfile_cache_mb=0\ns3_read_ahead_kb=1024\ns3_read_ahead_budget_mb=2\nadaptive_partitions=true")
            config = DATA / "neura.toml"
            config.write_text(text)
            command += ["--user", "10001:10001", "-p", f"127.0.0.1:{port}:8080", "-v", f"{config}:/neura.toml:ro",
                "-e", "NEURA_API_TOKEN=" + (DATA / "api-token").read_text(),
                "-e", "NEURA_TEST_POSTGRES_DSN=host=postgres port=5432 user=neura_reader password=neura-test-reader-only dbname=neura sslmode=disable",
                "-e", "AWS_ACCESS_KEY_ID=neura-reader", "-e", "AWS_SECRET_ACCESS_KEY=neura-test-readonly-storage",
                "-e", "AWS_REGION=us-east-1", image_id, "-c", "/neura.toml", "serve"]
            options = {"memory_mb": 512, "file_cache_mb": 0, "partitions": 2, "adaptive_partitions": True}
        else:
            config = DATA / (name + ".json")
            config.write_text(json.dumps({"connectors": {
                "pg": {"type": "postgresql", "connection": "postgresql://neura_reader:neura-test-reader-only@postgres:5432/neura", "pool_min": 1, "pool_max": 2},
                "lake": {"type": "parquet", "base_path": "s3://neura-fixture/", "endpoint": "http://s3:8333",
                    "region": "us-east-1", "access_key": "neura-reader", "secret_key": "neura-test-readonly-storage"},
                "local": {"type": "parquet", "base_path": "/fixture/local/"}}}, indent=2))
            # QE has a separate per-query estimate guard in addition to its pool.
            # Retain its documented 1 GiB default; the common hard RSS cap is 2 GiB.
            options = {"threads": 2, "memory-limit": args.qe_query_memory_mb, "pool-memory-mb": 512, "data-cache-mb": 0,
                "data-cache-pct": 0, "result-cache-mb": 0, "auto-optimize": "off", "catalog-refresh": 0,
                "pool-concurrency": 2, "query-timeout-seconds": 120, "max-spill-bytes": 4096}
            command += ["-p", f"127.0.0.1:{port}:5433", "-p", f"127.0.0.1:{port + 10}:8088",
                "-v", f"{config}:/qe.json:ro", "--entrypoint", "/opt/datatailr/sbin/dt-query-engine",
                image_id, "--config", "/qe.json", "--port", "5433", "--rest-port", "8088", "--rest-auth", "standalone",
                "--admin-password", (DATA / "api-token").read_text(), "--auth-db", "/tmp/qe_auth.db",
                "--jwt-secret", "/tmp/qe_jwt.key", "--spill-dir", "/tmp/spill"]
            for key, value in options.items():
                command += ["--" + key, str(value)]
        # Capture create output only: the command includes the private API token.
        if subprocess.run(command, stdout=subprocess.DEVNULL).returncode:
            raise RuntimeError(f"Container creation failed for {name}")
        state["profiles"][name] = {"container": PREFIX + "-" + name, "image": image, "image_id": image_id,
            "port": port, "config_path": str(config.relative_to(ROOT)), "config_sha256": hashlib.sha256(config.read_bytes()).hexdigest(),
            "options": options, "runtime": str(temp.relative_to(ROOT))}
        (DATA / "state.json").write_text(json.dumps(state, indent=2))
        print("Created stopped profile", name, image_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["fixtures", "engines"])
    parser.add_argument("--engines", nargs="+", choices=["neura", "qe305", "qe341"], default=["neura", "qe305", "qe341"])
    parser.add_argument("--neura-image", default="neura:hetzner-5bcc99c")
    parser.add_argument("--qe305-image", default="datatailr/query-engine:0.1.0-pre305-amd64-pinned")
    parser.add_argument("--qe341-image", default="datatailr/query-engine:0.1.0-pre341")
    parser.add_argument("--qe-query-memory-mb", type=int, default=1024)
    args = parser.parse_args()
    fixtures() if args.phase == "fixtures" else engines(args)
