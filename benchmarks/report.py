#!/usr/bin/env python3
"""Render a factual summary from a completed run, retaining failure counts."""

import argparse
import collections
import json
import pathlib
import statistics

p = argparse.ArgumentParser()
p.add_argument("input", type=pathlib.Path)
p.add_argument("output", type=pathlib.Path)
p.add_argument("--memory", type=pathlib.Path)
a = p.parse_args()
r = json.loads(a.input.read_text())
groups = collections.defaultdict(list)
for sample in r["samples"]:
    groups[(sample["engine"], sample["workload"])].append(sample)
failed = [s for s in r["samples"] if not s.get("correct", False)]
throughput_failed = sum(not s["correct"] for t in r["throughput"] for s in t["samples"])
lines = [
    "# Measured local comparison",
    "",
    f"Started: {r['started_at']}. Fixture: {r['fixture_rows']:,} Parquet rows plus 200 PostgreSQL customers (100 visible under RLS).",
    "",
    f"Correctness: {len(r['samples']) - len(failed)}/{len(r['samples'])} latency samples passed; {throughput_failed} throughput requests failed. Errors remain in the raw JSON.",
    "",
    "Each engine: 2 CPUs, 2 GiB RAM, no extra swap. Cold is process-cold after readiness, with source/OS/storage caches retained. Warm follows a successful full priming query. Latency includes connection and full fetch. Peak memory includes startup/cache and is cumulative since restart. These measurements do not establish universal superiority.",
    "",
    "| Engine | Workload | Cold ms (one sample) | Warm median ms | Warm min–max ms | Cgroup peak MiB |",
    "|---|---|---:|---:|---:|---:|",
]
for (engine, workload), samples in groups.items():
    cold = [
        s for s in samples if s["mode"].startswith("process_cold") and s.get("correct")
    ]
    warm = [s for s in samples if s["mode"] == "warm" and s.get("correct")]
    cs = f"{cold[0]['latency_ms']:.2f}" if cold else "FAILED"
    if warm:
        values = [s["latency_ms"] for s in warm]
        ws = f"{statistics.median(values):.2f}"
        bounds = f"{min(values):.2f}–{max(values):.2f}"
    else:
        ws = bounds = "FAILED"
    peak = (
        max((s.get("cgroup_peak_since_restart_bytes", 0) for s in samples), default=0)
        / 1024**2
    )
    lines.append(f"| {engine} | {workload} | {cs} | {ws} | {bounds} | {peak:.1f} |")
lines += [
    "",
    "Warm federated-group throughput:",
    "",
    "| Engine | Concurrency | Successful / attempted | Correct queries/s | Cgroup peak MiB |",
    "|---|---:|---:|---:|---:|",
]
for t in r["throughput"]:
    lines.append(
        f"| {t['engine']} | {t['concurrency']} | {t['successful_queries']}/{len(t['samples'])} | {t['successful_queries_per_second']:.2f} | {t['cgroup_peak_since_restart_bytes'] / 1024**2:.1f} |"
    )
lines += [
    "",
    "Internal memory settings differ in accounting: Neura and QE use 512 MiB operator pools; Spark uses a 512 MiB JVM heap. QE data/result caches are disabled. Spark does not cache tables. Spark is accessed through an HTTP adapter, whereas Neura/QE use PostgreSQL wire; protocol overhead differs. Engines run sequentially, so run-order and shared-cache effects remain. Five warm samples and ten throughput requests per engine are a smoke comparison, not a statistically robust capacity study.",
    "",
    "See [methodology](../../benchmarks/README.md), [validation and blocked cloud tests](../VALIDATION.md), and [limitations](../LIMITATIONS.md). Exact image identities, raw samples and checksums are in the adjacent JSON.",
]
lines += [
    "",
    "Neura uses SCRAM for each measured client connection; QE has no client password configured and the local Spark adapter is unauthenticated. All source credentials/policies are identical. Short-query numbers include different authentication overhead and do not isolate execution time. The 1,024-row Parquet groups exercise pruning but amplify small-range request overhead on full scans. Other user containers remained running on the shared Docker host; no exclusive-machine claim is made.",
]
if a.memory:
    memory = json.loads(a.memory.read_text())
    lines += [
        "",
        "Separate idle process-RSS observation after the suite:",
        "",
        "| Engine | Sum of process RSS MiB |",
        "|---|---:|",
    ]
    for engine, value in memory["engines"].items():
        lines.append(f"| {engine} | {value['summed_rss_bytes'] / 1024**2:.1f} |")
    lines += [
        "",
        "This is an idle snapshot, not peak RSS. Summing process RSS can double-count shared pages. Cgroup accounting can charge shared mapped pages elsewhere, so its smaller values must not be interpreted as total process footprint.",
        "",
        f"[Raw samples]({a.input.name}) · [RSS observations]({a.memory.name})",
    ]
a.output.parent.mkdir(parents=True, exist_ok=True)
a.output.write_text("\n".join(lines) + "\n")
