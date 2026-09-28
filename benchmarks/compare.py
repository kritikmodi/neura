#!/usr/bin/env python3
"""Equal-resource service comparison with independent correctness and memory passes."""

import argparse
import concurrent.futures
import datetime
import hashlib
import json
import os
import pathlib
import platform
import subprocess
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
PORTS = {"neura": int(os.environ.get("NEURA_BENCH_NEURA_PORT", "58102")), "spark": 58101,
    "duckdb": int(os.environ.get("NEURA_BENCH_DUCKDB_PORT", "58103"))}
PREFIX = os.environ.get("NEURA_BENCH_PREFIX", "neura-compare-")
TEMP_ROOT = pathlib.Path(os.environ.get("NEURA_BENCH_TMP_ROOT", str(ROOT / "data/compare-runtime")))


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def http(engine, path, body=None):
    req = urllib.request.Request(f"http://127.0.0.1:{PORTS[engine]}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": "Bearer " + os.environ["NEURA_API_TOKEN"], "Content-Type": "application/json"})
    return urllib.request.urlopen(req, timeout=130)


def execute(engine, sql):
    try:
        with http(engine, "/v1/query" if engine == "neura" else "/query", {"sql": sql}) as response:
            if engine != "neura":
                body = json.load(response)
                if "error" in body:
                    raise RuntimeError(body["error"])
                return body["rows"], None
            rows, complete, diagnostic = [], False, None
            for line in response:
                item = json.loads(line)
                if item["type"] == "error":
                    raise RuntimeError(item["error"])
                if item["type"] == "row":
                    rows.append(item["values"])
                if item["type"] == "complete":
                    complete, diagnostic = True, item["diagnostic"]
            if not complete or diagnostic["status"] != "succeeded":
                raise RuntimeError("Neura response did not complete successfully")
            return rows, diagnostic
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"HTTP {error.code}: {error.read().decode()}") from error


def start(engine):
    started = time.perf_counter()
    docker("start", PREFIX + engine)
    deadline, last, checked = time.monotonic() + 180, None, 0
    while time.monotonic() < deadline:
        try:
            with http(engine, "/healthz") as response:
                return json.load(response), (time.perf_counter() - started) * 1000
        except Exception as error:
            last = error
            if time.monotonic() - checked > 1:
                checked = time.monotonic()
                if docker("inspect", "--format", "{{.State.Running}}", PREFIX + engine) == "false":
                    raise RuntimeError(f"{engine} exited during startup: " + docker("logs", "--tail", "15", PREFIX + engine))
            time.sleep(0.2)
    raise RuntimeError(f"{engine} readiness failed: {last}")


def stop(engine):
    docker("stop", "--time", "3", PREFIX + engine)


def canonical(rows):
    return sorted(rows, key=lambda row: json.dumps(row))


def oracle(n):
    total = sum(i % 17 + 1 for i in range(n))
    groups, selective = {"east": 0, "west": 0}, {"east": [0, 0], "west": [0, 0]}
    for i in range(n):
        customer = i % 200 + 1
        region = "east" if customer % 2 == 0 else "west"
        if customer <= 100:
            groups[region] += i % 17 + 1
        if customer < 10 and i < 10000:
            selective[region][0] += 1
            selective[region][1] += i % 17 + 1
    return {"parquet_count": [[str(n), str(total)]],
        "parquet_filter": [[str(min(n, 1000)), str(sum(i % 17 + 1 for i in range(min(n, 1000))))]],
        "postgres_rls": [["east", "50"], ["west", "50"]],
        "federated_group": [[key, str(value)] for key, value in groups.items()],
        "federated_selective": [[key, str(value[0]), str(value[1])] for key, value in selective.items()],
        "local_high_cardinality": [[str(n), str(total), str(n)]]}


def measure(engine, workload, expected):
    started = time.perf_counter()
    result = {"engine": engine, "workload": workload["name"]}
    try:
        rows, diagnostic = execute(engine, workload["sql"])
        result.update(latency_ms=(time.perf_counter() - started) * 1000,
            correct=canonical(rows) == canonical(expected), rows=rows,
            result_sha256=hashlib.sha256(json.dumps(canonical(rows)).encode()).hexdigest())
        if diagnostic:
            result["diagnostic"] = diagnostic
    except Exception as error:
        result.update(correct=False, error=str(error), latency_ms=(time.perf_counter() - started) * 1000)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--engines", nargs="+", choices=list(PORTS), default=list(PORTS))
    parser.add_argument("--workloads", nargs="+", help="Optional workload names")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--throughput-queries", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--output", type=pathlib.Path, default=ROOT / "results/spark-duckdb.json")
    parser.add_argument("--smoke", action="store_true", help="Only source permission probes and one of each workload")
    args = parser.parse_args()
    assert args.repeats > 0 and args.throughput_queries > 0 and 1 <= args.concurrency <= 2
    workloads = json.loads((ROOT / "benchmarks/workloads/compare.json").read_text())
    if args.workloads:
        assert set(args.workloads) <= {w["name"] for w in workloads}, "Unknown workload"
        workloads = [w for w in workloads if w["name"] in args.workloads]
    fixture = json.loads((ROOT / "data/compare/manifest.json").read_text())
    expected = oracle(fixture["rows"])
    report = {"started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "fixture": fixture, "workloads": workloads, "engines": {}, "samples": [], "primes": [],
        "throughput": [], "memory": [], "governance": [], "startup": [], "errors": [],
        "host": {"platform": platform.platform(), "machine": platform.machine()},
        "docker": json.loads(docker("info", "--format", "{{json .}}")),
        "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted((ROOT / "src").rglob("*.rs")) + [ROOT / "Cargo.toml", ROOT / "Cargo.lock"]},
        "harness_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted((ROOT / "benchmarks").glob("compare*.py")) + [ROOT / "benchmarks/memmon.c",
                ROOT / "benchmarks/prepare_compare.py", ROOT / "benchmarks/workloads/compare.json"]},
        "notes": ["2 CPU, 2 GiB cgroup RAM, no extra swap; 512 MiB internal budgets have different accounting.",
            "Cold is process-cold after readiness, excluding startup; source/OS/storage caches retained.",
            "Spark and DuckDB discover schemas during startup; Neura does so during queries.",
            "Authenticated HTTP end-to-end full-fetch latency; Neura NDJSON, other adapters JSON; adapters differ.",
            "No explicit SQL result/table caches; Neura file-cache limits and DuckDB file-cache settings are recorded. Connection/metadata/OS caches remain.",
            "Engines execute sequentially; their order rotates per workload. Shared host is not exclusive.",
            "Memory measured separately from latency at roughly 50ms intervals; short-lived peaks may be missed.",
            "RSS sums may double-count shared pages; PSS apportions them. Sum of process HWM is not a simultaneous peak.",
            "Cgroup peak includes startup, page cache and the small monitor process; shared pages may be charged elsewhere.",
            "Fixture remains static. Permission probes do not certify equivalent concurrent-mutation snapshot semantics."]}
    # Avoid dumping unrelated Docker configuration into the published report.
    report["docker"] = {k: report["docker"].get(k) for k in
        ["ServerVersion", "KernelVersion", "OperatingSystem", "Architecture", "NCPU", "MemTotal"]}
    report["source_containers"] = {}
    for name in ("neura-test-postgres-1", "neura-test-s3-1"):
        info = json.loads(docker("inspect", name))[0]
        report["source_containers"][name] = {"image": info["Config"]["Image"], "image_id": info["Image"],
            "cpu_limit": info["HostConfig"]["NanoCpus"] / 1e9,
            "memory_limit_bytes": info["HostConfig"]["Memory"], "restart_count_at_start": info["RestartCount"]}
    report["duckdb_extension_sha256"] = {str(p.relative_to(ROOT / "target/duckdb-extensions")):
        hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT / "target/duckdb-extensions").rglob("*.duckdb_extension")}
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def save():
        args.output.write_text(json.dumps(report, indent=2))

    def boot(engine, phase, workload=None):
        metadata, startup = start(engine)
        report["engines"][engine]["metadata"] = metadata
        report["startup"].append({"engine": engine, "phase": phase, "workload": workload, "startup_ms": startup})

    def sample(engine, workload, mode, repeat=0):
        value = measure(engine, workload, expected[workload.get("oracle", workload["name"])])
        value.update(mode=mode, repeat=repeat)
        print(engine, workload["name"], mode, repeat, "PASS" if value["correct"] else "FAIL",
            round(value["latency_ms"], 2), flush=True)
        return value

    for engine in args.engines:
        info = json.loads(docker("inspect", PREFIX + engine))[0]
        host = info["HostConfig"]
        assert host["NanoCpus"] == 2_000_000_000 and host["Memory"] == 2 * 1024**3 and host["MemorySwap"] == 2 * 1024**3
        assert not info["State"]["Running"], f"Stop {engine}'s benchmark container before a new run"
        report["engines"][engine] = {"image": info["Config"]["Image"], "image_id": info["Image"],
            "cpu_limit": 2, "memory_limit_bytes": host["Memory"], "memory_swap_bytes": host["MemorySwap"]}
        if engine == "neura":
            command = info["Config"]["Cmd"]
            flag = next((flag for flag in ("-c", "--config") if flag in command), None)
            if flag:
                config_path = pathlib.PurePosixPath(command[command.index(flag) + 1])
                for mount in sorted(info["Mounts"], key=lambda m: len(m["Destination"]), reverse=True):
                    try:
                        relative = config_path.relative_to(mount["Destination"])
                    except ValueError:
                        continue
                    local_config = pathlib.Path(mount["Source"]) / relative
                    if local_config.is_file():
                        content = local_config.read_bytes()
                        report["engines"][engine]["config_sha256"] = hashlib.sha256(content).hexdigest()
                        # Only limits are recorded; connector config may refer to credentials.
                        limits = content.decode().split("[limits]", 1)
                        if len(limits) == 2:
                            report["engines"][engine]["limits_toml"] = limits[1].split("[", 1)[0].strip()
                    break
    save()
    active = None
    try:
        # Fail before timing if the real source authorization is not preserved.
        for engine in args.engines:
            active = engine
            boot(engine, "governance")
            rows, _ = execute(engine, "SELECT COUNT(*) FROM pg.customers WHERE tenant_id <> 1")
            assert rows == [["0"]], f"{engine} bypassed the tenant row policy"
            since = datetime.datetime.now(datetime.timezone.utc).isoformat()
            try:
                execute(engine, "SELECT * FROM pg.forbidden")
            except RuntimeError as error:
                # Neura intentionally sanitizes source errors; verify the actual
                # PostgreSQL rejection instead of accepting an arbitrary failure.
                logs = subprocess.check_output(["docker", "logs", "--since", since,
                    "neura-test-postgres-1"], stderr=subprocess.STDOUT, text=True)
                denied = [line for line in logs.splitlines() if "permission denied for table forbidden" in line]
                assert denied, str(error)
                report["governance"].append({"engine": engine, "rls_passed": True, "denied_table_passed": True,
                    "denied_table_error": str(error), "source_log_evidence": denied})
            else:
                raise AssertionError(f"{engine} returned a forbidden table")
            if args.smoke:
                for workload in workloads:
                    report["samples"].append(sample(engine, workload, "smoke"))
                    save()
            stop(engine); active = None; save()
        if args.smoke:
            return
        for index, workload in enumerate(workloads):
            order = args.engines[index % len(args.engines):] + args.engines[:index % len(args.engines)]
            for engine in order:
                active = engine
                boot(engine, "latency", workload["name"])
                report["samples"].append(sample(engine, workload, "process_cold_storage_uncontrolled")); save()
                prime = sample(engine, workload, "prime")
                report["primes"].append(prime); save()
                if prime["correct"]:
                    for repeat in range(args.repeats):
                        report["samples"].append(sample(engine, workload, "warm", repeat)); save()
                stop(engine); active = None
        for name in (n for n in ("federated_group", "federated_group_coarse", "local_high_cardinality")
                if n in {w["name"] for w in workloads}):
            workload = next(w for w in workloads if w["name"] == name)
            for engine in args.engines:
                active = engine
                boot(engine, "throughput", name)
                prime = sample(engine, workload, "throughput_prime")
                report["primes"].append(prime); save()
                if prime["correct"]:
                    started = time.perf_counter()
                    with concurrent.futures.ThreadPoolExecutor(args.concurrency) as pool:
                        samples = list(pool.map(lambda _: measure(engine, workload,
                            expected[workload.get("oracle", name)]), range(args.throughput_queries)))
                    elapsed = time.perf_counter() - started
                    successful = sum(s["correct"] for s in samples)
                    report["throughput"].append({"engine": engine, "workload": name,
                        "concurrency": args.concurrency, "samples": samples, "elapsed_secs": elapsed,
                        "successful_queries": successful, "successful_queries_per_second": successful / elapsed})
                    print(engine, name, "THROUGHPUT", successful, "/", len(samples), round(successful / elapsed, 3), flush=True)
                stop(engine); active = None; save()
        for name in (n for n in ("federated_group", "federated_group_coarse", "local_high_cardinality")
                if n in {w["name"] for w in workloads}):
            workload = next(w for w in workloads if w["name"] == name)
            for engine in args.engines:
                active = engine
                boot(engine, "memory", name)
                file = TEMP_ROOT / engine / f"memory-{name}.jsonl"
                if file.exists():
                    file.unlink()
                monitor = subprocess.Popen(["docker", "exec", PREFIX + engine, "/memmon", "/tmp/" + file.name],
                    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                deadline = time.monotonic() + 10
                while not file.exists() or file.stat().st_size == 0:
                    if monitor.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError("memory monitor did not start")
                    time.sleep(0.05)
                baseline = json.loads(file.read_text().splitlines()[-1])
                begin = time.time()
                with concurrent.futures.ThreadPoolExecutor(args.concurrency) as pool:
                    checks = list(pool.map(lambda _: measure(engine, workload,
                        expected[workload.get("oracle", name)]), range(args.concurrency)))
                end = time.time()
                time.sleep(0.1)
                stop(engine); active = None
                monitor.communicate(timeout=10)
                observations = [json.loads(line) for line in file.read_text().splitlines()]
                if not all(o["smaps_readable"] == o["processes"] for o in observations):
                    report["errors"].append(f"Incomplete PSS access for {engine} {name}")
                report["memory"].append({"engine": engine, "workload": name, "concurrency": args.concurrency,
                    "baseline": baseline, "query_start_time": begin, "query_end_time": end,
                    "checks": checks, "observations": observations,
                    "peaks": {key: max(o[key] for o in observations if o["time"] <= end + 0.1)
                        for key in ["rss_bytes", "pss_bytes", "private_bytes", "sum_process_hwm_bytes",
                            "cgroup_current_bytes", "cgroup_peak_bytes"]}})
                print(engine, name, "MEMORY RSS MiB", round(report["memory"][-1]["peaks"]["rss_bytes"] / 1024**2, 1), flush=True)
                save()
    except Exception as error:
        report["errors"].append(str(error)); save()
        raise
    finally:
        if active:
            stop(active)
        report["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        save()
        checks = report["samples"] + report["primes"] + [s for t in report["throughput"] for s in t["samples"]] + [s for m in report["memory"] for s in m["checks"]]
        if any(not s["correct"] for s in checks) or report["errors"]:
            raise SystemExit("Benchmark errors or incorrect results; inspect raw report. Failures are not speed wins.")
    print("Wrote", args.output, flush=True)


if __name__ == "__main__":
    main()
