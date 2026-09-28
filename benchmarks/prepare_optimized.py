#!/usr/bin/env python3
"""Prepare isolated revised Neura profiles without replacing prior benchmark containers."""

import argparse
import json
import pathlib
import re
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]


def inspect(name):
    return json.loads(subprocess.check_output(["docker", "inspect", name]))[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True, help="Explicit image containing the revised engine")
    parser.add_argument("--prefix", default="neura-optimized", help="Unique names for a new experiment")
    parser.add_argument("--port-start", type=int, default=58112)
    parser.add_argument("--read-ahead-kb", type=int, default=1024)
    args = parser.parse_args()
    assert re.fullmatch(r"[a-z][a-z0-9-]{1,48}", args.prefix)
    assert 1024 <= args.port_start <= 65533 and 0 <= args.read_ahead_kb <= 2048
    subprocess.run(["docker", "image", "inspect", args.image], check=True, stdout=subprocess.DEVNULL)
    original = inspect("neura-compare-neura")
    assert not original["State"]["Running"], "Finish the earlier benchmark before preparing profiles"
    base_config = (ROOT / "data/compare/neura.toml").read_text()
    assert base_config.count("[limits]") == 1
    for profile, port, cache_mb in [(args.prefix, args.port_start, 0), (args.prefix + "-cache", args.port_start + 2, 32)]:
        name = f"{profile}-neura"
        if subprocess.run(["docker", "inspect", name], capture_output=True).returncode == 0:
            raise SystemExit(f"{name} already exists; refusing to replace it")
        directory = ROOT / "data" / profile.removeprefix("neura-")
        directory.mkdir(parents=True, exist_ok=True)
        config = directory / "neura.toml"
        config.write_text(base_config.replace("[limits]", f"[limits]\nfile_cache_mb={cache_mb}\n"
            f"file_cache_max_object_mb=8\ns3_read_ahead_kb={args.read_ahead_kb}\n"
            "s3_read_ahead_budget_mb=2\nadaptive_partitions=true", 1))
        temp = directory / "runtime/neura"
        temp.mkdir(parents=True, exist_ok=True)
        temp.chmod(0o1777)
        command = ["docker", "create", "--name", name, "--network", "neura-test_default",
            "--cpus", "2", "--memory", "2g", "--memory-swap", "2g", "--user", "10001:10001",
            "-p", f"127.0.0.1:{port}:8080"]
        for bind in original["HostConfig"]["Binds"]:
            if bind.endswith(":/tmp"):
                bind = f"{temp}:/tmp"
            command += ["-v", bind]
        command += ["-v", f"{config}:/optimized.toml:ro"]
        # Reuse only explicitly configured source/auth settings, not image defaults.
        for setting in original["Config"]["Env"]:
            if setting.startswith(("NEURA_", "AWS_")):
                command += ["-e", setting]
        command += [args.image, "-c", "/optimized.toml", "serve"]
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL)
        print(f"Prepared {name} on localhost:{port}; reusable file cache {cache_mb} MiB")
        print(f"NEURA_BENCH_PREFIX={profile}- NEURA_BENCH_NEURA_PORT={port} "
            f"NEURA_BENCH_TMP_ROOT={temp.parent} python3 benchmarks/compare.py --engines neura "
            f"--output results/{profile.removeprefix('neura-')}.json")


if __name__ == "__main__":
    main()
