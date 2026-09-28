#!/usr/bin/env python3
"""Produce tables from recorded observations; never substitute missing results."""

import argparse
import json
import math
import pathlib
import statistics

p = argparse.ArgumentParser()
p.add_argument("input", type=pathlib.Path)
p.add_argument("output", type=pathlib.Path)
p.add_argument("--cache", type=pathlib.Path, help="Separate DuckDB default-file-cache observations")
a = p.parse_args()
r = json.loads(a.input.read_text())
engines = list(r["engines"])
all_checks = r["samples"] + r["primes"] + [s for t in r["throughput"] for s in t["samples"]] + [s for m in r["memory"] for s in m["checks"]]
good = sum(s["correct"] for s in all_checks)
lines = ["# Neura, Spark and DuckDB: measured comparison", "",
    f"Started: {r['started_at']}. Finished: {r.get('finished_at', 'incomplete')}.", "",
    f"Correctness: **{good}/{len(all_checks)}** query results matched an independent Python oracle. "
    f"Recorded harness errors: **{len(r['errors'])}**. Failed queries are retained in the raw data.", "",
    f"Fixture: {r['fixture']['rows']:,} orders, 200 PostgreSQL customers, 100 visible to the restricted reader. "
    "All engines read the same source objects with the same source credentials. "
    "RLS and forbidden-table probes must pass before measurement.", "",
    "## Versions and resource limits", "",
    "| Engine | Version | CPU limit | Container RAM / swap | Internal budget |",
    "|---|---|---:|---|---|",
]
for engine, info in r["engines"].items():
    budget = "512 MiB JVM heap" if engine == "spark" else "512 MiB tracked operator/buffer memory"
    lines.append(f"| {engine} | {info.get('metadata', {}).get('version', 'unknown')} | 2 | 2 GiB / no extra swap | {budget} |")
lines += ["", "These internal budgets have different accounting. The common cgroup limit is the hard boundary. "
    "Spark 3.5.6 is the pinned existing comparison runtime; this is not a latest-Spark comparison. "
    "Neura's engine source was unchanged for this experiment.", "",
    "## Warm latency", "", "End-to-end milliseconds, median of successful warm samples; min–max in parentheses. "
    "Each pair has one process-cold query, a separate successful priming query, then warm repetitions.", "",
    "| Workload | " + " | ".join(engines) + " |", "|---|" + "---:|" * len(engines)]
for workload in r["workloads"]:
    row = [workload["name"]]
    for engine in engines:
        samples = [s for s in r["samples"] if s["engine"] == engine and s["workload"] == workload["name"] and s["mode"] == "warm"]
        values = [s["latency_ms"] for s in samples if s["correct"]]
        text = f"{statistics.median(values):.2f} ({min(values):.2f}–{max(values):.2f})" if values else "NO SUCCESSFUL SAMPLES"
        if len(values) != len(samples):
            text += f"; {len(samples)-len(values)} failed"
        row.append(text)
    lines.append("| " + " | ".join(row) + " |")
lines += ["", "## Process-cold latency", "", "One sample per engine/workload, after service readiness. "
    "Startup is excluded. Source, operating-system and storage caches remain uncontrolled; **not storage-cold**.", "",
    "| Workload | " + " | ".join(engines) + " |", "|---|" + "---:|" * len(engines)]
for workload in r["workloads"]:
    row = [workload["name"]]
    for engine in engines:
        samples = [s for s in r["samples"] if s["engine"] == engine and s["workload"] == workload["name"] and s["mode"].startswith("process_cold")]
        row.append(f"{samples[0]['latency_ms']:.2f}" if samples and samples[0]["correct"] else "FAILED / MISSING")
    lines.append("| " + " | ".join(row) + " |")
lines += ["", "## Throughput", "", "A finite batch at concurrency two, after priming; not an open-loop saturation study.", "",
    "| Workload | Engine | Correct / attempted | Queries/s | Request median ms | Request p95 ms* |",
    "|---|---|---:|---:|---:|---:|"]
for item in r["throughput"]:
    values = sorted(s["latency_ms"] for s in item["samples"] if s["correct"])
    median = f"{statistics.median(values):.2f}" if values else "FAILED"
    p95 = f"{values[max(0, math.ceil(len(values)*.95)-1)]:.2f}" if values else "FAILED"
    lines.append(f"| {item['workload']} | {item['engine']} | {item['successful_queries']}/{len(item['samples'])} | "
        f"{item['successful_queries_per_second']:.3f} | {median} | {p95} |")
lines += ["", "*Nearest-rank p95 over this small batch; not a reliable production tail-latency estimate.", "",
    "## Memory, measured separately", "",
    "Two simultaneous queries after a fresh engine start. No full-query priming. "
    "The native observer samples about every 50 ms and is excluded from process RSS/PSS, but included in cgroup usage. "
    "These runs are separate from timing runs. RSS can double-count shared pages; PSS apportions them. "
    "Sampled peaks are lower bounds on instantaneous peaks. Lifetime per-process high-water marks are also retained in JSON.", "",
    "| Workload | Engine | Baseline RSS MiB | Sampled peak RSS MiB | Sum process HWM MiB* | Sampled peak PSS MiB | Cgroup peak since restart MiB |",
    "|---|---|---:|---:|---:|---:|---:|"]
for item in r["memory"]:
    lines.append(f"| {item['workload']} | {item['engine']} | {item['baseline']['rss_bytes']/1024**2:.1f} | "
        f"{item['peaks']['rss_bytes']/1024**2:.1f} | {item['peaks']['sum_process_hwm_bytes']/1024**2:.1f} | {item['peaks']['pss_bytes']/1024**2:.1f} | {item['peaks']['cgroup_peak_bytes']/1024**2:.1f} |")
lines += ["", "*HWM is the kernel's lifetime resident high-water mark for each process, summed across engine processes. "
    "For a multi-process engine those individual peaks need not coincide. It is additional evidence, not a simultaneous total peak."]
lines += ["", "Cgroup accounting includes startup and filesystem cache, and shared mapped pages can be charged to other cgroups. "
    "It is not interchangeable with RSS or PSS. Baseline and query resident memory can also reflect earlier readiness work.", "",
    "RSS is the approximate Linux VmRSS counter; PSS comes from smaps_rollup. They are read sequentially, "
    "so small RSS/PSS discrepancies can occur. Brief allocation peaks may exceed sampled RSS/PSS; "
    "consult the recorded process high-water marks as well.", "",
    "## Layout and source diagnostics", "",
    f"Small layout: {sum(f['row_groups'] for f in r['fixture']['small'])} row groups across two objects, "
    f"{sum(f['bytes'] for f in r['fixture']['small']):,} compressed bytes. "
    f"Large layout: {sum(f['row_groups'] for f in r['fixture']['large'])} row groups, "
    f"{sum(f['bytes'] for f in r['fixture']['large']):,} compressed bytes. "
    "Both contain the same logical rows. The local high-cardinality workload reads the large-layout files from a read-only bind mount "
    "and groups by one million distinct order IDs before reducing to three checked scalar results.", "",
    "| Neura workload | Source | Requests | Response bytes | Rows |",
    "|---|---|---:|---:|---:|"]
for workload in r["workloads"]:
    samples = [s for s in r["samples"] if s["engine"] == "neura" and s["workload"] == workload["name"] and s["mode"] == "warm" and s["correct"]]
    if samples:
        for source, metric in samples[-1].get("diagnostic", {}).get("sources", {}).items():
            lines.append(f"| {workload['name']} | {source} | {metric['requests']} | {metric.get('response_bytes')} | {metric['rows']} |")
lines += ["", "Neura records source-specific payload bytes, not uniform network/egress billing bytes. "
    "Plans and operator metrics are retained in the raw results. Both Parquet tables are configured under one S3 source; "
    "Neura currently resolves their metadata during each referenced-source setup.", "",
    "## Interpretation limits", ""]
lines += ["- " + note for note in r["notes"]]
lines += ["- Eight workloads on a synthetic million-row fixture do not establish universal superiority, distributed scaling, "
    "production sizing, full SQL/type equivalence or cloud warehouse behavior.",
    "- PostgreSQL grants/RLS are tested. Source data stays unchanged during measurement; "
    "this does not prove equal snapshot guarantees under concurrent writes. No governance shortcuts or privileged reader are used.", ""]
if a.cache:
    cached = json.loads(a.cache.read_text())
    cached_checks = cached["samples"] + cached["primes"] + [s for t in cached["throughput"] for s in t["samples"]] + [s for m in cached["memory"] for s in m["checks"]]
    lines += ["", "## DuckDB with its default external file cache", "",
        f"Separate profile: {sum(s['correct'] for s in cached_checks)}/{len(cached_checks)} result checks passed, "
        f"{len(cached['errors'])} harness errors. Same limits, source identities and HTTP adapter. "
        "Only the external file cache is enabled. This is cached file data, not precomputed query results. "
        "These warm numbers must not be silently substituted into the baseline above.", "",
        "| Workload | DuckDB cache off warm median ms | DuckDB cache on warm median ms | Cache on process-cold ms |",
        "|---|---:|---:|---:|"]
    for workload in cached["workloads"]:
        values = []
        for source in (r, cached):
            samples = [s["latency_ms"] for s in source["samples"] if s["engine"] == "duckdb" and
                s["workload"] == workload["name"] and s["mode"] == "warm" and s["correct"]]
            values.append(f"{statistics.median(samples):.2f}" if samples else "FAILED / MISSING")
        cold = [s["latency_ms"] for s in cached["samples"] if s["workload"] == workload["name"] and s["mode"].startswith("process_cold") and s["correct"]]
        values.append(f"{cold[0]:.2f}" if cold else "FAILED / MISSING")
        lines.append("| " + workload["name"] + " | " + " | ".join(values) + " |")
    lines += ["", "| Workload | Cache on throughput queries/s | Observed peak RSS MiB | Observed peak PSS MiB |",
        "|---|---:|---:|---:|"]
    for item in cached["throughput"]:
        memory = next(m for m in cached["memory"] if m["workload"] == item["workload"])
        lines.append(f"| {item['workload']} | {item['successful_queries_per_second']:.3f} | "
            f"{memory['peaks']['rss_bytes']/1024**2:.1f} | {memory['peaks']['pss_bytes']/1024**2:.1f} |")
    lines += ["", "The same separate memory-pass caveats apply. The cache profile remains a static-fixture test; "
        "permission revocation, cache invalidation under updates, large working sets, and equal end-user governance have not been certified.", "",
        f"[Cache-profile raw observations]({a.cache.name})", ""]
lines += ["",
    f"[Raw observations]({a.input.name}) · [Reproduction](../../benchmarks/COMPARE.md)", "",
    "[Warm-latency chart](spark-duckdb-latency.png) · [Memory chart](spark-duckdb-memory.png)", ""]
a.output.parent.mkdir(parents=True, exist_ok=True)
a.output.write_text("\n".join(lines))
print(a.output)
