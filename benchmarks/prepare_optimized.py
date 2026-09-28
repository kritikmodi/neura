#!/usr/bin/env python3
"""Prepare isolated revised Neura profiles without replacing prior benchmark containers."""

import argparse
import json
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]


def inspect(name):
    return json.loads(subprocess.check_output(["docker", "inspect", name]))[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True, help="Explicit image containing the revised engine")
    args = parser.parse_args()
    subprocess.run(["docker", "image", "inspect", args.image], check=True, stdout=subprocess.DEVNULL)
    original = inspect("neura-compare-neura")
    assert not original["State"]["Running"], "Finish the earlier benchmark before preparing profiles"
    base_config = (ROOT / "data/compare/neura.toml").read_text()
    assert base_config.count("[limits]") == 1
    for profile, port, cache_mb in [("optimized", 58112, 0), ("optimized-cache", 58114, 32)]:
        name = f"neura-{profile}-neura"
        if subprocess.run(["docker", "inspect", name], capture_output=True).returncode == 0:
            raise SystemExit(f"{name} already exists; refusing to replace it")
        directory = ROOT / "data" / profile
        directory.mkdir(parents=True, exist_ok=True)
        config = directory / "neura.toml"
        config.write_text(base_config.replace("[limits]", f"[limits]\nfile_cache_mb={cache_mb}\n"
            "file_cache_max_object_mb=8\ns3_read_ahead_kb=256\ns3_read_ahead_budget_mb=2", 1))
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
        print(f"NEURA_BENCH_PREFIX=neura-{profile}- NEURA_BENCH_NEURA_PORT={port} "
            f"NEURA_BENCH_TMP_ROOT={temp.parent} python3 benchmarks/compare.py --engines neura "
            f"--output results/{profile}.json")


if __name__ == "__main__":
    main()
