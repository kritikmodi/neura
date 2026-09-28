#!/usr/bin/env python3
"""Native authenticated HTTP comparison against isolated QE images on one host."""
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

from compare import canonical, oracle

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data/qe-host"


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def inspect(name):
    return json.loads(docker("inspect", name))[0]


def service_id(name):
    # Preserve restart/image evidence without publishing unrelated service names.
    return "service-" + hashlib.sha256(name.encode()).hexdigest()[:12]


def execute(profile, token, sql):
    neura = profile["kind"] == "neura"
    port = profile["port"] if neura else profile["port"] + 10
    endpoint = "/v1/query" if neura else "/api/query"
    req = urllib.request.Request(f"http://127.0.0.1:{port}{endpoint}",
        data=json.dumps({"sql": sql, **({} if neura else {"limit": 100})}).encode(),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + profile.get("_query_token", token)})
    try:
        with urllib.request.urlopen(req, timeout=135) as response:
            if not neura:
                payload = json.load(response)
                if payload.get("error") or payload.get("success") is False:
                    raise RuntimeError(str(payload))
                result = payload["result"]
                if result.get("truncated") or result.get("has_more"):
                    raise RuntimeError("QE returned a truncated result")
                rows = result["rows"]
                assert len(rows) < 100, "Unexpected result limit reached"
                return [[None if v is None else str(int(v)) if isinstance(v, float) and v.is_integer() else str(v)
                    for v in row] for row in rows], {k: v for k, v in result.items() if k != "rows"}
            rows, diagnostic = [], None
            for line in response:
                item = json.loads(line)
                if item["type"] == "error":
                    raise RuntimeError(item["error"])
                if item["type"] == "row":
                    rows.append(item["values"])
                if item["type"] == "complete":
                    diagnostic = item["diagnostic"]
            if diagnostic is None or diagnostic["status"] != "succeeded":
                raise RuntimeError("Incomplete Neura result")
            return rows, diagnostic
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"HTTP {error.code}: {error.read().decode()}") from error


def start(profile, token):
    started = time.perf_counter()
    docker("start", profile["container"])
    deadline, last = time.monotonic() + 120, None
    profile.pop("_query_token", None)
    while time.monotonic() < deadline:
        try:
            if profile["kind"] == "qe" and "_query_token" not in profile:
                req = urllib.request.Request(f"http://127.0.0.1:{profile['port'] + 10}/api/auth/login",
                    data=json.dumps({"username": "admin", "password": token}).encode(),
                    headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=10) as response:
                    auth = json.load(response)
                profile["_query_token"] = auth["result"]["token"]
            rows, metadata = execute(profile, token, "SELECT 1")
            assert rows == [["1"]], rows
            return {"elapsed_ms": (time.perf_counter() - started) * 1000, "readiness_result": rows}
        except (KeyError, TypeError, AssertionError) as error:
            raise RuntimeError(f"Unexpected readiness response shape: {type(error).__name__}") from error
        except Exception as error:
            last = str(error).replace(token, "<redacted>")
            if not inspect(profile["container"])["State"]["Running"]:
                raise RuntimeError(f"{profile['container']} exited; inspect its benchmark-only logs")
            time.sleep(0.25)
    raise RuntimeError("Readiness timeout: " + str(last))


def stop(profile):
    docker("stop", "--time", "5", profile["container"])


def measure(name, profile, token, workload, expected, mode, repeat=0):
    begin = time.perf_counter()
    sample = {"engine": name, "workload": workload["name"], "mode": mode, "repeat": repeat}
    try:
        rows, diagnostic = execute(profile, token, workload["sql"])
        sample.update(latency_ms=(time.perf_counter() - begin) * 1000, rows=rows,
            correct=canonical(rows) == canonical(expected), diagnostic=diagnostic,
            result_sha256=hashlib.sha256(json.dumps(canonical(rows)).encode()).hexdigest())
    except Exception as error:
        sample.update(latency_ms=(time.perf_counter() - begin) * 1000, correct=False,
            error=str(error).replace(token, "<redacted>"))
    return sample


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--engines", nargs="+", default=["neura", "qe305", "qe341"])
    parser.add_argument("--source-commit", required=True, help="Commit of the archived Rust source used to build Neura")
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--throughput-queries", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--output", type=pathlib.Path, default=ROOT / "results/hetzner-qe.json")
    args = parser.parse_args()
    assert args.repeats > 0 and args.throughput_queries > 0 and args.concurrency in (1, 2)
    state = json.loads((DATA / "state.json").read_text())
    profiles = {name: {**state["profiles"][name], "kind": "neura" if name.startswith("neura") else "qe"} for name in args.engines}
    token = (DATA / "api-token").read_text()
    workloads = json.loads((ROOT / "benchmarks/workloads/compare.json").read_text())
    workloads += json.loads((ROOT / "benchmarks/workloads/qe-local-control.json").read_text())
    expected = oracle(state["fixture"]["rows"])
    report = {"started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "fixture": state["fixture"], "host": {"platform": platform.platform(), "cpu_count": os.cpu_count(),
            "cpu_model": next((line.split(":", 1)[1].strip() for line in pathlib.Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')), None),
            "mem_total_kib": next(line for line in pathlib.Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemTotal:')),
            "load_at_start": os.getloadavg()},
        "source_commit": args.source_commit,
        "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted((ROOT / "src").rglob("*.rs")) + [ROOT / "Cargo.toml", ROOT / "Cargo.lock"]},
        "harness_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in [ROOT / "benchmarks/compare_qe_host.py", ROOT / "benchmarks/prepare_qe_host.py",
                ROOT / "benchmarks/compare.py", ROOT / "benchmarks/memmon.c", ROOT / "benchmarks/workloads/compare.json",
                ROOT / "benchmarks/workloads/qe-local-control.json"]},
        "workloads": workloads, "engines": {}, "sources": {}, "samples": [], "primes": [], "throughput": [],
        "memory": [], "startup": [], "governance": [], "errors": [], "engine_events": [],
        "existing_services_before": {service_id(n): v for n, v in state["existing_services_before"].items()},
        "running_qe_reference_image_id": state["existing_services_before"]["phase1-qe"]["image_id"],
        "notes": ["Same x86_64 host, immutable Parquet bytes and restricted PostgreSQL/S3 source accounts.",
            "2 CPU and 2 GiB hard cgroup cap per engine; 512 MiB internal budgets have different accounting.",
            "QE also has a separate per-query estimate guard, recorded in options. Neura has no directly equivalent separate guard.",
            "Primary profiles disable reusable data/result caching and auto-optimized result reuse in QE; Neura file cache off.",
            "Native authenticated HTTP; client runs on server host. Neura uses a static bearer token/NDJSON; QE uses standalone JWT auth/buffered JSON. Full result consumption.",
            "QE login occurs during readiness, outside query timing. Authentication implementations and catalog startup work differ.",
            "QE result limit 100; all workloads return at most three rows. A result reaching the limit fails validation.",
            "Cold means fresh engine process after SELECT 1 readiness; startup excluded. Source/OS/storage caches uncontrolled.",
            "Engine order rotates per workload. Existing services stay running; this is a shared-host experiment.",
            "Memory is a separate two-query pass, approximately 50 ms samples; brief peaks may be missed.",
            "Per-process high-water sums need not coincide; PSS apportions shared pages. Cgroup includes startup and page cache.",
            "Timing excludes memory-observer work; finite-batch throughput is not a saturation test.",
            "Static fixture tests source grants/RLS, not equivalent snapshot/cache behavior under concurrent changes."]}
    for name, profile in profiles.items():
        d = inspect(profile["container"])
        h = d["HostConfig"]
        assert not d["State"]["Running"], f"{name} must be stopped before measurement"
        assert h["NanoCpus"] == 2_000_000_000 and h["Memory"] == h["MemorySwap"] == 2 * 1024**3
        assert d["Image"] == profile["image_id"], "Engine image changed"
        report["engines"][name] = {**profile, "cpu_limit": 2, "memory_limit_bytes": h["Memory"],
            "client_protocol": "authenticated native HTTP", "memory_swap_bytes": h["MemorySwap"]}
    for name in state["sources"]:
        d = inspect(name)
        report["sources"][name] = {"image": d["Config"]["Image"], "image_id": d["Image"],
            "cpu_limit": d["HostConfig"]["NanoCpus"] / 1e9, "memory_limit_bytes": d["HostConfig"]["Memory"],
            "started_at": d["State"]["StartedAt"], "restart_count": d["RestartCount"],
            "go_memory_limit": next((e.split("=", 1)[1] for e in d["Config"]["Env"] if e.startswith("GOMEMLIMIT=")), None)}
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def save():
        args.output.write_text(json.dumps(report, indent=2))

    context = {}

    def verify_sources():
        for name, source in report["sources"].items():
            d = inspect(name)
            if (not d["State"]["Running"] or d["State"]["OOMKilled"] or d["State"]["StartedAt"] != source["started_at"]
                    or d["RestartCount"] != source["restart_count"]):
                raise RuntimeError(f"Dedicated source stopped/restarted: {name}; engine comparisons are invalid")

    def boot(name, phase, workload=None):
        context.update(engine=name, phase=phase, workload=workload)
        verify_sources()
        result = start(profiles[name], token)
        verify_sources()
        report["startup"].append({"engine": name, "phase": phase, "workload": workload, **result})

    def finish(name):
        d = inspect(profiles[name]["container"])
        report["engine_events"].append({**context, "state_before_stop": {
            k: d["State"][k] for k in ("Running", "OOMKilled", "ExitCode", "Error", "StartedAt", "FinishedAt")}})
        stop(profiles[name])

    def sample(name, workload, mode, repeat=0):
        value = measure(name, profiles[name], token, workload, expected[workload.get("oracle", workload["name"])], mode, repeat)
        print(name, workload["name"], mode, repeat, "PASS" if value["correct"] else "FAIL", round(value["latency_ms"], 2), flush=True)
        return value

    active = None
    try:
        for name in args.engines:
            active = name
            boot(name, "governance")
            rows, _ = execute(profiles[name], token, "SELECT COUNT(*) FROM pg.customers WHERE tenant_id <> 1")
            assert rows == [["0"]], f"{name} did not preserve RLS"
            since = datetime.datetime.now(datetime.timezone.utc).isoformat()
            try:
                execute(profiles[name], token, "SELECT * FROM pg.forbidden")
            except RuntimeError as error:
                logs = subprocess.check_output(["docker", "logs", "--since", since, state["prefix"] + "-postgres"], text=True, stderr=subprocess.STDOUT)
                denied = [line for line in logs.splitlines() if "permission denied for table forbidden" in line]
                assert denied, f"Missing source denial evidence: {str(error).replace(token, '<redacted>')}"
                report["governance"].append({"engine": name, "rls_passed": True, "denied_table_passed": True,
                    "source_log_evidence": denied, "error": str(error).replace(token, "<redacted>")})
            else:
                raise AssertionError(f"{name} exposed forbidden data")
            if args.smoke:
                for workload in workloads:
                    report["samples"].append(sample(name, workload, "smoke")); save()
            finish(name); active = None; save()
        if not args.smoke:
            for index, workload in enumerate(workloads):
                order = args.engines[index % len(args.engines):] + args.engines[:index % len(args.engines)]
                for name in order:
                    active = name; boot(name, "latency", workload["name"])
                    report["samples"].append(sample(name, workload, "process_cold_storage_uncontrolled")); save()
                    prime = sample(name, workload, "prime"); report["primes"].append(prime); save()
                    if prime["correct"]:
                        for i in range(args.repeats):
                            report["samples"].append(sample(name, workload, "warm", i)); save()
                    finish(name); active = None
            measured = [w for w in workloads if w["name"] in ("federated_group", "federated_group_coarse", "local_high_cardinality", "local_distinct_control")]
            for workload in measured:
                for name in args.engines:
                    active = name; boot(name, "throughput", workload["name"])
                    prime = sample(name, workload, "throughput_prime"); report["primes"].append(prime); save()
                    if prime["correct"]:
                        begin = time.perf_counter()
                        with concurrent.futures.ThreadPoolExecutor(args.concurrency) as pool:
                            samples = list(pool.map(lambda i: sample(name, workload, "throughput", i), range(args.throughput_queries)))
                        elapsed = time.perf_counter() - begin
                        good = sum(s["correct"] for s in samples)
                        report["throughput"].append({"engine": name, "workload": workload["name"], "concurrency": args.concurrency,
                            "elapsed_secs": elapsed, "successful_queries_per_second": good / elapsed, "samples": samples})
                    finish(name); active = None; save()
            for workload in measured:
                for name in args.engines:
                    active = name; boot(name, "memory", workload["name"])
                    file = ROOT / profiles[name]["runtime"] / f"memory-{workload['name']}.jsonl"
                    file.unlink(missing_ok=True)
                    monitor = subprocess.Popen(["docker", "exec", profiles[name]["container"], "/memmon", "/tmp/" + file.name],
                        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                    deadline = time.monotonic() + 10
                    while not file.exists() or not file.stat().st_size:
                        if monitor.poll() is not None or time.monotonic() > deadline:
                            raise RuntimeError("Memory observer did not start")
                        time.sleep(.05)
                    baseline = json.loads(file.read_text().splitlines()[-1])
                    begin = time.time()
                    with concurrent.futures.ThreadPoolExecutor(args.concurrency) as pool:
                        checks = list(pool.map(lambda i: sample(name, workload, "memory", i), range(args.concurrency)))
                    end = time.time(); time.sleep(.1)
                    finish(name); active = None
                    monitor.communicate(timeout=10)
                    observations = [json.loads(line) for line in file.read_text().splitlines()]
                    eligible = [o for o in observations if o["time"] <= end + .1]
                    pss_complete = all(o["smaps_readable"] == o["processes"] for o in eligible)
                    report["memory"].append({"engine": name, "workload": workload["name"], "concurrency": args.concurrency,
                        "baseline": baseline, "query_start_time": begin, "query_end_time": end, "checks": checks,
                        "pss_complete": pss_complete,
                        "observations": observations, "peaks": {key: max(o[key] for o in eligible)
                            for key in ("rss_bytes", "pss_bytes", "private_bytes", "sum_process_hwm_bytes", "cgroup_current_bytes", "cgroup_peak_bytes")}})
                    if not pss_complete:
                        report["memory"][-1]["peaks"]["pss_bytes"] = None
                    save()
            verify_sources()
    except Exception as error:
        report["errors"].append(str(error).replace(token, "<redacted>")); save()
        raise
    finally:
        if active:
            finish(active)
        report["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        report["host"]["load_at_end"] = os.getloadavg()
        report["existing_services_after"] = {}
        for name, before in state["existing_services_before"].items():
            d = inspect(name)
            after = {"image_id": d["Image"], "pid": d["State"]["Pid"], "started_at": d["State"]["StartedAt"], "restart_count": d["RestartCount"]}
            report["existing_services_after"][service_id(name)] = after
            if before != after:
                report["errors"].append(f"Existing service changed during experiment: {service_id(name)}")
        save()
    checks = report["samples"] + report["primes"] + [s for t in report["throughput"] for s in t["samples"]] + [s for m in report["memory"] for s in m["checks"]]
    if report["errors"] or any(not s["correct"] for s in checks):
        raise SystemExit("Incorrect results or harness errors; inspect saved evidence")
    print(f"PASS {len(checks)}/{len(checks)} correctness checks; saved {args.output}", flush=True)


if __name__ == "__main__":
    main()
