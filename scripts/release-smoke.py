#!/usr/bin/env python3
"""Test the actual release image against disposable PostgreSQL/S3 fixtures.

Requires deploy/compose.test.yml and scripts/seed.py first. Uses only test secrets.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def run(*command, capture=False, **kwargs):
    if capture:
        return subprocess.check_output(command, text=True, **kwargs).strip()
    return subprocess.run(command, check=True, **kwargs)


def query(sql):
    request = urllib.request.Request("http://127.0.0.1:58080/v1/query",
        data=json.dumps({"sql": sql}).encode(),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + os.environ["NEURA_API_TOKEN"]})
    with urllib.request.urlopen(request, timeout=90) as response:
        events = [json.loads(line) for line in response]
    errors = [e for e in events if e["type"] == "error"]
    if errors:
        raise RuntimeError(str(errors))
    assert events[-1]["type"] == "complete", "Incomplete query output"
    return [e["values"] for e in events if e["type"] == "row"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("image")
    parser.add_argument("version")
    parser.add_argument("--cube", action="store_true")
    args = parser.parse_args()
    assert run("docker", "run", "--rm", args.image, "--version", capture=True) == "neura " + args.version
    info = json.loads(run("docker", "image", "inspect", args.image, capture=True))[0]
    assert info["Config"]["User"] == "10001:10001"
    assert info["Config"]["Labels"]["org.opencontainers.image.version"] == args.version
    assert info["Config"]["Labels"]["org.opencontainers.image.revision"] == os.environ["GITHUB_SHA"]
    assert info["Config"]["Labels"]["org.opencontainers.image.source"] == "https://github.com/kritikmodi/neura"
    name = "neura-release-smoke"
    existing = subprocess.run(["docker", "inspect", name], capture_output=True)
    assert existing.returncode != 0, "Refusing to replace an existing test container"
    config = (ROOT / "data/test.toml").read_text()
    config = config.replace('http_addr="127.0.0.1:58080"', 'http_addr="0.0.0.0:8080"\nallow_insecure_network=true')
    config = config.replace('endpoint="http://127.0.0.1:59000"', 'endpoint="http://s3:8333"')
    wire = config.replace("[server]", '[server]\npg_addr="0.0.0.0:5433"')
    path = ROOT / "data/release-smoke.toml"
    path.write_text(wire if args.cube else config)
    mount = f"{path}:/etc/neura/neura.toml:ro"
    cli = run("docker", "run", "--rm", "-v", mount, args.image, "-c", "/etc/neura/neura.toml", "query", "SELECT 1 AS n", capture=True,
              env=os.environ)
    events = [json.loads(line) for line in cli.splitlines()]
    assert [e["type"] for e in events] == ["schema", "row", "complete"]
    assert events[1]["values"] == ["1"]
    env = {**os.environ,
        "NEURA_TEST_POSTGRES_DSN": "host=postgres port=5432 user=neura_reader password=neura-test-reader-only dbname=neura sslmode=disable"}
    command = ["docker", "run", "-d", "--name", name, "--network", "neura-test_default",
        "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges:true",
        "--memory=512m", "--memory-swap=512m", "--cpus=2", "--pids-limit=256",
        "--tmpfs", "/tmp:size=128m,mode=1777", "-p", "127.0.0.1:58080:8080", "-v", mount]
    if args.cube:
        command += ["-p", "127.0.0.1:55433:5433"]
    for key in ("NEURA_TEST_POSTGRES_DSN", "NEURA_API_TOKEN", "NEURA_PG_PASSWORD",
                "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION"):
        command += ["-e", key]
    try:
        run(*command, args.image, env=env)
        for _ in range(120):
            try:
                with urllib.request.urlopen("http://127.0.0.1:58080/healthz", timeout=1):
                    break
            except OSError:
                time.sleep(.25)
        else:
            raise RuntimeError("Release image did not become healthy")
        run("python3", "scripts/api-test.py", cwd=ROOT)
        assert query("SELECT COUNT(*) FROM pg.customers") == [["100"]], "Source RLS changed"
        sql = "SELECT c.region, SUM(o.amount) AS total FROM pg.customers c JOIN lake.orders o ON c.id=o.customer_id GROUP BY c.region ORDER BY c.region"
        assert query(sql) == json.loads((ROOT / "data/expected.json").read_text())["grouped"]
        if args.cube:
            run("node", "scripts/wire-test.cjs", cwd=ROOT,
                env={**os.environ, "NODE_PATH": str(ROOT / "target/cube-test/node_modules")})
        else:
            rejected = ROOT / "data/release-wire-rejected.toml"
            rejected.write_text(wire)
            result = subprocess.run(["docker", "run", "--rm", "-v", f"{rejected}:/etc/neura/neura.toml:ro",
                args.image, "-c", "/etc/neura/neura.toml", "check-config"], capture_output=True, text=True)
            assert result.returncode != 0 and "postgres-wire" in result.stderr, "Default image must reject an unavailable PG listener"
        assert json.loads(run("docker", "inspect", name, capture=True))[0]["State"]["Running"]
        print("PASS: packaged version, labels, CLI, non-root REST, auth, RLS, federation" + (", Cube" if args.cube else ", feature isolation"))
    finally:
        subprocess.run(["docker", "logs", "--tail", "30", name], check=False)
        subprocess.run(["docker", "rm", "-f", name], check=False)


if __name__ == "__main__":
    main()
