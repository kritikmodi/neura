#!/usr/bin/env python3
"""Compare saved historical observations with independently measured revised profiles."""

import argparse
import json
import os
import pathlib
import re
import statistics


def warm(report, engine, workload):
    values = [s["latency_ms"] for s in report["samples"]
        if s["engine"] == engine and s["workload"] == workload and s["mode"] == "warm" and s["correct"]]
    return statistics.median(values) if values else None


def fmt(value):
    return "—" if value is None else f"{value:,.2f}"


def checks(report):
    return report["samples"] + report["primes"] + [s for t in report["throughput"] for s in t["samples"]] + [
        s for m in report["memory"] for s in m["checks"]]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("baseline", type=pathlib.Path)
    parser.add_argument("optimized", type=pathlib.Path)
    parser.add_argument("cached", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--duckdb-cache", type=pathlib.Path, help="Historical DuckDB default-cache reference")
    parser.add_argument("--chart", type=pathlib.Path, help="Optional generated chart to link")
    parser.add_argument("--controls", type=pathlib.Path, help="Optional tuning-control record to link")
    args = parser.parse_args()
    old, revised, cached = [json.loads(path.read_text()) for path in (args.baseline, args.optimized, args.cached)]
    duckdb_cache = json.loads(args.duckdb_cache.read_text()) if args.duckdb_cache else None
    for report in (revised, cached):
        assert not report["errors"] and all(c["correct"] for c in checks(report)), "Failed runs must not become a success report"
        assert report["fixture"] == old["fixture"], "Fixture mismatch"
    def link(label, path):
        return f"[{label}]({os.path.relpath(path.resolve(), args.output.parent.resolve())})"

    def memory_hwm(report):
        return next(m["peaks"]["sum_process_hwm_bytes"] / 1024**2 for m in report["memory"]
            if m["engine"] == "neura" and m["workload"] == "local_high_cardinality")

    def join_metric(report):
        return [s for s in report["samples"] if s["engine"] == "neura" and s["mode"] == "warm"
            and s["workload"] == "federated_group"][-1]["diagnostic"]["sources"]["lake"]

    before, after, reused = map(join_metric, (old, revised, cached))
    lines = ["# Neura optimization measurements", "",
        "These are real measurements using the same million-row fixture and 2 CPU / 2 GiB container limits as the earlier comparison. "
        "The earlier Neura, Spark and DuckDB observations are historical references, not simultaneous reruns. "
        "The revised profiles were measured separately on the shared host. No claim of statistical significance or universal superiority is made.", "",
        "Both revised profiles use adaptive S3 read-ahead, admission-aware partition targets and PostgreSQL pooling. The second enables a 32 MiB reusable S3 file cache, "
        "deducted from the 512 MiB execution budget. Read-ahead also reserves from that execution pool. "
        "Cache hits require a current conditional GetObject request. This authorization cost is included.", "",
        "## Interpretation", "",
        f"The default, cache-off small-row-group join changed from {fmt(warm(old, 'neura', 'federated_group'))} ms "
        f"to {fmt(warm(revised, 'neura', 'federated_group'))} ms warm. "
        f"Its final warm source request count changed from {before['requests']} to {after['requests']}, "
        f"while origin response-body bytes changed from {before['response_bytes']:,} to {after['response_bytes']:,}. "
        "Read-ahead trades additional bytes for fewer requests; this can matter for cloud egress and selective workloads.", "",
        f"**Faster does not imply lower peak memory.** The local aggregation's recorded process high-water RSS "
        f"was {fmt(memory_hwm(old))} MiB before and {fmt(memory_hwm(revised))} MiB with the revised default. "
        "The lower sampled RSS shown below misses brief peaks and must not be presented as an overall peak-memory improvement. "
        "The internal memory pool is not a limit on all process allocations.", "",
        f"**Caching remains opt-in.** The cached warm join transferred {reused['response_bytes']:,} origin body bytes "
        "for live authorization probes; HTTP/TCP headers are excluded. Cache-enabled latency was not consistently lower in this run. "
        "The profiles run sequentially, and local-only aggregation does not read the S3 cache: its throughput difference "
        "cannot be attributed to caching alone. Execution budget, scheduling, source conditions and shared-host noise also differ.", "",
        "PostgreSQL pooling improves warm reuse; process-cold PostgreSQL setup still costs approximately 43 ms here. "
        "The earlier DuckDB default-cache join remains much faster on this small repeated-read fixture. "
        "These observations do not justify a fastest-engine or lowest-memory claim.", "",
        "Raw measurements: " + link("revised, cache off", args.optimized) + " · " +
        link("revised, cache 32 MiB", args.cached) + " · " + link("historical baseline", args.baseline) +
        (" · " + link("historical DuckDB default cache", args.duckdb_cache) if args.duckdb_cache else "") + ".", ""]
    if args.controls:
        lines += [link("Tuning controls and environment record", args.controls) + ".", ""]
    if args.chart:
        lines += ["!" + link("Measured Neura latency and high-water RSS", args.chart), ""]
    lines += ["## Correctness and provenance", ""]
    for label, report in [("Revised, cache off", revised), ("Revised, cache 32 MiB", cached)]:
        observed = checks(report)
        lines += [f"- {label}: {sum(c['correct'] for c in observed)}/{len(observed)} result checks passed; "
            f"{len(report['governance'])} live governance probe sets passed. "
            f"Started {report['started_at']}; finished {report['finished_at']}.",
            f"  Image `{report['engines']['neura']['image_id']}`; source commit `{report['git_commit']}`. "
            "Raw files additionally record exact source/configuration hashes."]
    lines += ["", "## Warm median latency (ms)", "",
        "Five warm measurements after a full priming query; lower is better. DuckDB cache profiles are labelled separately. Missing entries were not measured.", "",
        "| Workload | Earlier Neura | Revised, cache off | Revised, cache 32 MiB | Earlier Spark | Earlier DuckDB, cache off | Earlier DuckDB, default cache |",
        "|---|---:|---:|---:|---:|---:|---:|"]
    for workload in revised["workloads"]:
        name = workload["name"]
        values = [warm(old, "neura", name), warm(revised, "neura", name), warm(cached, "neura", name),
            warm(old, "spark", name), warm(old, "duckdb", name), warm(duckdb_cache, "duckdb", name) if duckdb_cache else None]
        lines.append(f"| {name} | " + " | ".join(map(fmt, values)) + " |")
    lines += ["", "## Process-cold latency (ms)", "",
        "One sample after each engine restart and readiness; startup excluded. Source/OS/storage caches are retained and uncontrolled. "
        "This is **not storage-cold**.", "", "| Workload | Revised, cache off | Revised, cache 32 MiB |", "|---|---:|---:|"]
    for workload in revised["workloads"]:
        name = workload["name"]
        values = [next((s["latency_ms"] for s in r["samples"] if s["workload"] == name and
            s["mode"] == "process_cold_storage_uncontrolled"), None) for r in (revised, cached)]
        lines.append(f"| {name} | " + " | ".join(map(fmt, values)) + " |")
    for field, title, extract in [
        ("throughput", "Throughput, queries/sec at concurrency two", lambda r: r["successful_queries_per_second"]),
        ("memory", "Separate memory pass: sampled process peak RSS (MiB)", lambda r: r["peaks"]["rss_bytes"] / 1024**2),
        ("memory", "Separate memory pass: process high-water mark sum (MiB)", lambda r: r["peaks"]["sum_process_hwm_bytes"] / 1024**2),
    ]:
        lines += ["", "## " + title, "", "| Workload | Earlier Neura | Revised, cache off | Revised, cache 32 MiB |",
            "|---|---:|---:|---:|"]
        for name in ("federated_group", "federated_group_coarse", "local_high_cardinality"):
            values = [next((extract(s) for s in r[field] if s["engine"] == "neura" and s["workload"] == name), None)
                for r in (old, revised, cached)]
            lines.append(f"| {name} | " + " | ".join(map(fmt, values)) + " |")
    lines += ["", "## Concurrent local aggregation diagnostics", "",
        "Spill counts below mean queries whose final physical-plan metrics include a nonzero spill count. "
        "Partition targets are chosen at admission; already-running plans retain their target.", "",
        "| Profile | Queries that spilled | Admission targets observed |", "|---|---:|---|"]
    for label, report in [("Earlier Neura", old), ("Revised, cache off", revised), ("Revised, cache 32 MiB", cached)]:
        batch = next(t for t in report["throughput"] if t["engine"] == "neura" and t["workload"] == "local_high_cardinality")
        spills = sum(any(int(n) > 0 for n in re.findall(r"spill_count=(\d+)", s["diagnostic"]["physical_plan"])) for s in batch["samples"])
        targets = sorted({s["diagnostic"].get("selected_partitions") for s in batch["samples"]} - {None})
        lines.append(f"| {label} | {spills}/{len(batch['samples'])} | {', '.join(map(str, targets)) or 'not recorded'} |")
    lines += ["", "## Warm S3 diagnostics", "",
        "Each row is the final successful warm query. Source request counters include instrumented scan operations, "
        "as in the original suite. `response_bytes` counts origin payload bytes; cache bytes are reported separately.", "",
        "| Profile | Workload | Source requests | Origin bytes | Read-ahead hits | Cache hits | Cache bytes served |",
        "|---|---|---:|---:|---:|---:|---:|"]
    for label, report in [("Cache off", revised), ("Cache 32 MiB", cached)]:
        for workload in report["workloads"]:
            samples = [s for s in report["samples"] if s["workload"] == workload["name"] and s["mode"] == "warm"]
            if samples:
                metric = samples[-1]["diagnostic"]["sources"].get("lake")
                if metric:
                    lines.append(f"| {label} | {workload['name']} | {metric['requests']} | {metric['response_bytes']} | "
                        f"{metric.get('read_ahead_hits', 0)} | {metric.get('cache_hits', 0)} | {metric.get('cache_bytes', 0)} |")
    lines += ["", "## Limits of this experiment", "",
        "- Small static fixture, local S3-compatible storage, shared host; source CPU and memory limits remain unchanged. "
        "This is not an AWS/cloud-warehouse or distributed benchmark.",
        "- Throughput is a finite batch of twenty queries, not a saturation or stable tail-latency estimate.",
        "- Memory is a separate two-query pass with approximately 50 ms sampling, including service runtime; short spikes may be missed. "
        "Per-process high-water sums are not simultaneous peaks. Cgroup counters are retained separately in raw data.",
        "- File caching is opt-in and may overread small objects on first use. It does not cache SQL results, metadata permissions, PostgreSQL rows or Iceberg/Glue data.",
        "- The earlier DuckDB default-cache profile remains in the original report and must also be considered when comparing repeated reads. "
        "The no-cache reference table does not represent DuckDB's best warm performance.",
        "- Missing warehouse credentials still block real Databricks, Snowflake, BigQuery and AWS S3/Glue validation. "
        "Lake Formation governed tables are unsupported and rejected.", ""]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(lines))
    print(args.output)


if __name__ == "__main__":
    main()
