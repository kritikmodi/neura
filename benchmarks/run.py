#!/usr/bin/env python3
"""Reproducible comparison. No invented results; failed queries are preserved as failures."""

import argparse, concurrent.futures, datetime, hashlib, json, os, pathlib, statistics, subprocess, time, urllib.request
import psycopg
import platform

ROOT = pathlib.Path(__file__).resolve().parents[1]


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def request(url, body=None):
    data = None if body is None else json.dumps(body).encode()
    with urllib.request.urlopen(
        urllib.request.Request(
            url, data=data, headers={"Content-Type": "application/json"}
        ),
        timeout=120,
    ) as r:
        return json.load(r)


def execute(engine, sql):
    if engine == "spark":
        response = request("http://127.0.0.1:58081/query", {"sql": sql})
        if "error" in response:
            raise RuntimeError(response["error"])
        return response["rows"]
    port = 55434 if engine == "neura" else 56433
    password = (
        os.environ.get("NEURA_PG_PASSWORD", "") if engine == "neura" else "unused"
    )
    with psycopg.connect(
        host="127.0.0.1",
        port=port,
        user="neura",
        password=password,
        dbname="neura",
        connect_timeout=10,
        autocommit=True,
    ) as conn:
        rows = conn.execute(sql, prepare=False).fetchall()
    return [[None if v is None else str(v) for v in row] for row in rows]


def ready(engine):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            if engine == "spark":
                request("http://127.0.0.1:58081/healthz")
            else:
                execute(engine, "SELECT 1")
            return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError(f"{engine} did not become ready")


def memory(container):
    # Cumulative high-water mark since this container restart, includes runtime startup
    # and filesystem cache. This is not per-query allocator usage or process-only RSS.
    raw = docker(
        "exec",
        container,
        "sh",
        "-c",
        "cat /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.peak",
    )
    current, peak = map(int, raw.split())
    return {"cgroup_current_bytes": current, "cgroup_peak_since_restart_bytes": peak}


def canonical(rows):
    return sorted(rows, key=lambda row: json.dumps(row, separators=(",", ":")))


def expected(n):
    total = sum(i % 17 + 1 for i in range(n))
    cut = min(1000, n)
    grouped = {"east": 0, "west": 0}
    selected = {"east": [0, 0], "west": [0, 0]}
    for i in range(n):
        cid = i % 200 + 1
        region = "east" if cid % 2 == 0 else "west"
        if cid <= 100:
            grouped[region] += i % 17 + 1
        if cid < 10 and i < 10000:
            selected[region][0] += 1
            selected[region][1] += i % 17 + 1
    return {
        "parquet_count": [[str(n), str(total)]],
        "parquet_filter": [[str(cut), str(sum(i % 17 + 1 for i in range(cut)))]],
        "postgres_rls": [["east", "50"], ["west", "50"]],
        "federated_group": [[k, str(v)] for k, v in sorted(grouped.items())],
        "federated_selective": [
            [k, str(v[0]), str(v[1])] for k, v in sorted(selected.items())
        ],
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--engines", nargs="+", default=["neura", "qe", "spark"])
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument("--concurrency", type=int, default=2)
    p.add_argument("--output", default="results/comparison.json")
    a = p.parse_args()
    workloads = json.loads((ROOT / "benchmarks/workloads/queries.json").read_text())
    n = json.loads((ROOT / "data/expected.json").read_text())["rows"]
    oracle = expected(n)
    docker_info = json.loads(docker("info", "--format", "{{json .}}"))
    source_hash = hashlib.sha256()
    for file in sorted((ROOT / "src").rglob("*.rs")) + [
        ROOT / "Cargo.toml",
        ROOT / "Cargo.lock",
    ]:
        source_hash.update(str(file.relative_to(ROOT)).encode())
        source_hash.update(file.read_bytes())
    report = {
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "fixture_rows": n,
        "source_fixture": "scripts/seed.py deterministic v1",
        "neura_source_sha256": source_hash.hexdigest(),
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "logical_cpus": os.cpu_count(),
        },
        "docker": {
            k: docker_info.get(k)
            for k in [
                "ServerVersion",
                "KernelVersion",
                "OperatingSystem",
                "Architecture",
                "NCPU",
                "MemTotal",
            ]
        },
        "fixture_sha256": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted((ROOT / "data").glob("part-*.parquet"))
        },
        "notes": [
            "Cold means engine process restarted, health checked; source processes and OS/storage caches retained.",
            "Warm samples follow a full successful priming query; no engine result caching enabled.",
            "Latency includes client connect/authenticate, query and complete result fetch.",
            "Cgroup memory includes runtime startup and file cache; cumulative high-water mark since restart.",
            "No claim of storage-cold measurement or universal performance advantage.",
        ],
        "engines": {},
        "samples": [],
        "throughput": [],
    }
    out = ROOT / a.output
    out.parent.mkdir(parents=True, exist_ok=True)

    def save():
        out.write_text(json.dumps(report, indent=2))

    for engine in a.engines:
        container = f"neura-bench-{engine}"
        info = json.loads(docker("inspect", container))[0]
        host = info["HostConfig"]
        assert (
            host["NanoCpus"] == 2_000_000_000
            and host["Memory"] == 2 * 1024**3
            and host["MemorySwap"] == 2 * 1024**3
        ), "unequal resource limits; benchmark refused"
        report["engines"][engine] = {
            "image": info["Config"]["Image"],
            "image_id": info["Image"],
            "cpu_limit": 2,
            "memory_limit_bytes": host["Memory"],
            "memory_swap_bytes": host["MemorySwap"],
        }
        for workload in workloads:
            docker("restart", "--signal", "SIGINT", container)
            ready(engine)
            for mode, count in [
                ("process_cold_storage_uncontrolled", 1),
                ("warm", a.repeats),
            ]:
                if mode == "warm":
                    try:
                        assert canonical(execute(engine, workload["sql"])) == canonical(
                            oracle[workload["name"]]
                        )
                    except Exception as e:
                        report["samples"].append(
                            {
                                "engine": engine,
                                "workload": workload["name"],
                                "mode": "prime",
                                "error": str(e),
                            }
                        )
                        save()
                        break
                for repeat in range(count):
                    sample = {
                        "engine": engine,
                        "workload": workload["name"],
                        "mode": mode,
                        "repeat": repeat,
                    }
                    started = time.perf_counter()
                    try:
                        rows = execute(engine, workload["sql"])
                        sample["latency_ms"] = (time.perf_counter() - started) * 1000
                        sample["correct"] = canonical(rows) == canonical(
                            oracle[workload["name"]]
                        )
                        sample["rows"] = rows
                        sample["result_sha256"] = hashlib.sha256(
                            json.dumps(canonical(rows)).encode()
                        ).hexdigest()
                    except Exception as e:
                        sample.update(
                            error=str(e),
                            correct=False,
                            latency_ms=(time.perf_counter() - started) * 1000,
                        )
                    sample.update(memory(container))
                    report["samples"].append(sample)
                    save()
                    print(
                        engine,
                        workload["name"],
                        mode,
                        repeat,
                        "PASS" if sample.get("correct") else "FAIL",
                        round(sample["latency_ms"], 2),
                        flush=True,
                    )
        w = next(w for w in workloads if w["name"] == "federated_group")
        execute(engine, w["sql"])

        def one(_):
            t = time.perf_counter()
            try:
                r = execute(engine, w["sql"])
                return {
                    "latency_ms": (time.perf_counter() - t) * 1000,
                    "correct": canonical(r) == canonical(oracle[w["name"]]),
                }
            except Exception as e:
                return {"error": str(e), "correct": False}

        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(a.concurrency) as pool:
            samples = list(pool.map(one, range(a.repeats * a.concurrency)))
        elapsed = time.perf_counter() - start
        report["throughput"].append(
            {
                "engine": engine,
                "workload": w["name"],
                "concurrency": a.concurrency,
                "elapsed_secs": elapsed,
                "successful_queries": sum(s["correct"] for s in samples),
                "successful_queries_per_second": sum(s["correct"] for s in samples)
                / elapsed,
                "samples": samples,
                **memory(container),
            }
        )
        save()
    print("Wrote", out)
    if any(not s.get("correct", False) for s in report["samples"]) or any(
        not s["correct"] for t in report["throughput"] for s in t["samples"]
    ):
        raise SystemExit(
            "Some benchmark queries failed; inspect report. Failures are not performance wins."
        )


if __name__ == "__main__":
    main()
