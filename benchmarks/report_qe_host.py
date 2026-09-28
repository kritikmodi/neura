#!/usr/bin/env python3
"""Render the measured same-host QE comparison without inferring missing results."""
import argparse
import json
import os
import pathlib
import statistics

ROOT = pathlib.Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    args = parser.parse_args()
    r = json.loads(args.input.read_text())
    def relative(path):
        return pathlib.Path(os.path.relpath(path.resolve(), args.output.parent.resolve())).as_posix()

    def evidence(filename):
        return relative(ROOT / "docs/benchmark-results" / filename)

    engines = list(r["engines"])
    checks = r["samples"] + r["primes"] + [s for t in r["throughput"] for s in t["samples"]] + [s for m in r["memory"] for s in m["checks"]]
    passed = sum(s["correct"] for s in checks)
    clean = passed == len(checks) and not r["errors"]
    status = "PASS" if clean else "COMPLETED WITH QUERY FAILURES" if not r["errors"] else "INCOMPLETE / HARNESS ERRORS"
    names = {"neura": "Neura", "qe305": "QE pre305", "qe341": "QE pre341"}
    def warm(engine, workload):
        values = [s["latency_ms"] for s in r["samples"] if s["engine"] == engine and s["workload"] == workload and s["mode"] == "warm" and s["correct"]]
        return statistics.median(values) if values else None

    lines = ["# Neura versus Query Engine on Hetzner", "",
        "This experiment runs the current Neura engine and actual Datatailr Query Engine images on the same x86_64 machine. "
        "The primary QE image is the exact image of the host's running pre305 service; pre341 is an additional, newer image "
        "already installed on that host. Dedicated benchmark copies and dedicated sources are used. Existing services remain running.", "",
        f"Started `{r['started_at']}`; finished `{r['finished_at']}`. "
        f"Host: {r['host']['cpu_model']}, {r['host']['cpu_count']} logical CPUs. "
        "The host is shared; unrelated workload activity was not controlled.", "",
        f"**{status}: {passed}/{len(checks)} result checks matched the independent Python oracle.** "
        f"{len(r['governance'])} source governance probe sets completed. "
        f"Harness errors: {len(r['errors'])}.", ""]
    if not r["errors"] and {"neura", "qe305", "qe341"} <= set(engines) and all(warm(e, w) is not None for e in engines for w in ("federated_group", "postgres_rls", "local_distinct_control")):
        s3_workloads = [w["name"] for w in r["workloads"] if w["name"].startswith(("parquet_", "federated_"))]
        wins = {e: sum(warm(e, w) is not None and warm("neura", w) is not None and warm(e, w) < warm("neura", w)
            for w in s3_workloads) for e in ("qe305", "qe341")}
        lines += ["## Observations", "",
            f"The primary QE image completed the warm small-row-group join in {warm('qe305', 'federated_group'):,.2f} ms "
            f"versus Neura's {warm('neura', 'federated_group'):,.2f} ms. "
            f"QE pre305 had lower warm medians on {wins['qe305']}/{len(s3_workloads)} S3 workloads; "
            f"pre341 did so on {wins['qe341']}/{len(s3_workloads)}.", "",
            f"Warm PostgreSQL aggregation: Neura {warm('neura', 'postgres_rls'):.2f} ms, "
            f"QE pre305 {warm('qe305', 'postgres_rls'):.2f} ms, QE pre341 {warm('qe341', 'postgres_rls'):.2f} ms. "
            f"Local distinct-count control: Neura {warm('neura', 'local_distinct_control'):.2f} ms, "
            f"QE pre305 {warm('qe305', 'local_distinct_control'):.2f} ms, QE pre341 {warm('qe341', 'local_distinct_control'):.2f} ms. "
            "The original grouped-subquery failures, concurrent-batch failures and memory observations are reported below. "
            "These are specific workload/resource observations, not universal superiority claims.", "",
            "![Measured latency and high-water RSS](hetzner-qe.png)", ""]
    lines += ["## Configuration", "",
        "Each engine has **2 CPUs, 2 GiB container RAM and no additional swap**, with a nominal **512 MiB internal budget**. "
        "The tracked memory boundaries differ; the cgroup limit is the common hard boundary. PostgreSQL has 1 CPU / 512 MiB; "
        "S3 has 1 CPU / 1 GiB, with Go's soft memory limit configured at 512 MiB. These same source instances serve all engines. "
        "All engines use the same restricted PostgreSQL role, RLS policy and read-only S3 identity. No production credentials or tables are used.", "",
        "Reusable data/result caching is disabled in this primary comparison, matching the existing pre305 service's cache settings. "
        "QE auto-optimized result reuse is off. Neura keeps its default bounded read-ahead and admission-aware parallelism. "
        "QE uses two worker threads and pool concurrency two. This does not represent every engine's fastest cached configuration.", "",
        "QE additionally retains its documented **1 GiB per-query estimate guard**, separate from its 512 MiB shared pool. "
        "Neura has no directly equivalent separate guard. An initial smoke control with QE's guard reduced to 512 MiB "
        "rejected the million-group query at a reported 526 MiB; the 1 GiB guard also rejected it at a reported 1,029 MiB. "
        "These failures are retained. They are resource-limit "
        "errors, not incorrect returned results or OS OOMs. Equal container limits do not make internal memory accounting identical.", "",
        "An initial timing attempt was invalidated when the S3 fixture was OOM-killed at a 512 MiB container limit. "
        "That run is retained as failure evidence and excluded from comparative performance claims. "
        "The final run uses the larger, bounded S3 fixture above; engine limits are unchanged.", "",
        "The same immutable million-row Parquet files are used in two layouts: 1,024-row groups and 131,072-row groups. "
        "PostgreSQL contains 200 customers, of which RLS exposes 100. The local aggregation groups one million distinct order IDs. "
        "Object hashes, ETags, image IDs, source hashes and configuration hashes are in the raw record.", "",
        "A ninth workload, `local_distinct_control`, computes the same checked scalar answers for this fixture using "
        "COUNT(DISTINCT) instead of the grouped subquery. It diagnoses sensitivity to query shape. Every engine receives "
        "the identical control SQL; it does not replace or hide the original grouped-query failures.", "",
        "Native HTTP requests originate on the benchmark host, excluding SSH latency. Neura uses static bearer authentication "
        "and streams NDJSON; QE uses standalone JWT authentication and returns buffered JSON. QE login is part of readiness, "
        "outside query timing. Both clients fully consume results. Authentication implementations, catalog initialization and "
        "response formats differ; these are complete-service measurements, not isolated operator timings.", "",
        "Neura uses its default REST-only build; optional PostgreSQL wire for Cube is not enabled in this profile. "
        "QE retains both its native REST and PG listeners. Existing QE services can share executable/library pages with the "
        "benchmark copies; consider RSS/high-water marks alongside PSS/cgroup accounting.", "",
        "| Profile | Image | Image ID |", "|---|---|---|"]
    for name, e in r["engines"].items():
        lines.append(f"| {names.get(name, name)} | `{e['image']}` | `{e['image_id']}` |")
    lines += ["", f"Neura source commit: `{r['source_commit']}`.", "",
        "| Engine | Valid results / attempted checks | Failed checks |", "|---|---:|---:|"]
    for engine in engines:
        subset = [s for s in checks if s["engine"] == engine]
        good = sum(s["correct"] for s in subset)
        lines.append(f"| {names.get(engine, engine)} | {good}/{len(subset)} | {len(subset)-good} |")
    failures = {}
    for s in checks:
        if not s["correct"]:
            failures.setdefault((s["engine"], s["workload"]), []).append(s.get("error", "Returned rows did not match oracle"))
    if failures:
        lines += ["", "Failed checks are retained and excluded from speed/memory wins:", ""]
        for (engine, workload), errors in failures.items():
            kinds = "memory guard" if all("memory limit" in e for e in errors) else "query failures; inspect raw errors"
            lines.append(f"- {names.get(engine, engine)} / `{workload}`: {len(errors)} failed checks ({kinds}).")
    oom = [event for event in r.get("engine_events", []) if event["state_before_stop"]["OOMKilled"]]
    if oom:
        lines += ["", "Kernel OOM evidence:", ""]
        for event in oom:
            lines.append(f"- {names.get(event['engine'], event['engine'])}: `{event['phase']}` / `{event['workload']}`, "
                f"container exit code {event['state_before_stop']['ExitCode']}.")
    lines += ["", "## Warm latency", "",
        "End-to-end median milliseconds with observed min–max in parentheses; lower is better. "
        "Each engine/workload starts fresh, executes one process-cold query and a full priming query before warm repetitions. "
        "Engine order rotates across workloads.", "",
        "| Workload | " + " | ".join(names.get(e, e) for e in engines) + " |",
        "|---|" + "---:|" * len(engines)]
    for w in r["workloads"]:
        cells = []
        for e in engines:
            samples = [s for s in r["samples"] if s["engine"] == e and s["workload"] == w["name"] and s["mode"] == "warm"]
            vals = [s["latency_ms"] for s in samples]
            previous = [s for s in r["samples"] + r["primes"] if s["engine"] == e and s["workload"] == w["name"] and not s["correct"]]
            missing = "Memory guard" if any("memory limit" in s.get("error", "") for s in previous) else "FAILED" if previous else "—"
            cells.append("FAILED" if any(not s["correct"] for s in samples) else
                f"{statistics.median(vals):,.2f} ({min(vals):,.2f}–{max(vals):,.2f})" if vals else missing)
        lines.append("| " + w["name"] + " | " + " | ".join(cells) + " |")
    lines += ["", "## Process-cold latency", "",
        "One observation per engine/workload after readiness; startup excluded. Source/OS/storage caches are not flushed. "
        "**These are not storage-cold measurements.**", "",
        "| Workload | " + " | ".join(names.get(e, e) for e in engines) + " |", "|---|" + "---:|" * len(engines)]
    for w in r["workloads"]:
        cells = []
        for e in engines:
            s = next((s for s in r["samples"] if s["engine"] == e and s["workload"] == w["name"] and s["mode"] == "process_cold_storage_uncontrolled"), None)
            cells.append("—" if s is None else f"{s['latency_ms']:,.2f}" if s["correct"] else "FAILED")
        lines.append("| " + w["name"] + " | " + " | ".join(cells) + " |")
    lines += ["", "## Throughput", "", "Finite batch, concurrency two; higher is better. This is not an open-loop saturation test.", "",
        "| Workload | Engine | Correct / attempted | Queries/sec |", "|---|---|---:|---:|"]
    for workload in dict.fromkeys(p["workload"] for p in r["primes"] if p["mode"] == "throughput_prime"):
        for engine in engines:
            t = next((t for t in r["throughput"] if t["workload"] == workload and t["engine"] == engine), None)
            if t is None:
                lines.append(f"| {workload} | {names.get(engine, engine)} | Prime failed; batch not run | — |")
                continue
            good = sum(s["correct"] for s in t["samples"])
            rate = f"{t['successful_queries_per_second']:.3f}" if good == len(t["samples"]) else "FAILED batch; see raw rates"
            lines.append(f"| {t['workload']} | {names.get(t['engine'], t['engine'])} | {good}/{len(t['samples'])} | {rate} |")
    lines += ["", "## Separate memory pass", "",
        "Two simultaneous queries after a fresh start. The observer samples at roughly 50 ms and is excluded from process totals. "
        "Short spikes may be missed; recorded process high-water marks are also shown. Summing multiple processes' high-water "
        "marks need not describe a simultaneous peak. PSS apportions shared pages. Cgroup peak includes startup, page cache and "
        "the observer, and may charge shared pages elsewhere. It is not interchangeable with process RSS.", "",
        "| Workload | Engine | Sampled RSS MiB | High-water sum MiB | Sampled PSS MiB | Cgroup peak MiB |",
        "|---|---|---:|---:|---:|---:|"]
    for m in r["memory"]:
        if not all(s["correct"] for s in m["checks"]):
            lines.append(f"| {m['workload']} | {names.get(m['engine'], m['engine'])} | FAILED query | — | — | — |")
            continue
        values = [m["peaks"][k] / 1024**2 if m["peaks"][k] is not None else None for k in ("rss_bytes", "sum_process_hwm_bytes", "pss_bytes", "cgroup_peak_bytes")]
        lines.append(f"| {m['workload']} | {names.get(m['engine'], m['engine'])} | " + " | ".join(f"{v:.2f}" if v is not None else "incomplete" for v in values) + " |")
    lines += ["", "## Interpretation limits", "",
        "- This is a small, static, synthetic, single-node PostgreSQL/S3-compatible benchmark. It does not establish production tail latency, cloud egress cost, distributed scaling or a universal ranking.",
        "- The image of the running QE service is reproduced with equal limits; this is not a timing run against the untouched live service's original 4 CPU / 8 GiB deployment.",
        "- Source permissions are checked through actual RLS results and PostgreSQL denial logs. Equal snapshot semantics under concurrent source mutations are not certified by this static test.",
        "- QE may initialize catalogs before query timing; Neura resolves referenced sources per query. Warm metadata, connection pools, JIT and OS caches remain even when reusable data/result caches are off.",
        "- Internal limits count different allocations. Use resident/cgroup observations alongside internal budgets; no global minimum-memory claim is justified.",
        "- Earlier laptop Spark/DuckDB results use another architecture and host. They must not be numerically mixed with these Hetzner observations.", "",
        "## Evidence", "", f"[Raw samples, plans and fingerprints]({relative(args.input)}). "
        f"[Reproduction instructions]({relative(ROOT / 'benchmarks/HETZNER_QE.md')}).", "",
        f"[Initial invalid timing attempt]({evidence('2026-09-28-hetzner-invalid-run.json')}) · "
        f"[S3 fixture recovery and object hashes]({evidence('2026-09-28-hetzner-source-recovery.json')}) · "
        f"[512 MiB QE guard control]({evidence('2026-09-28-qe-512-guard.json')}) · "
        f"[Cleanup and existing-service verification]({evidence('2026-09-28-hetzner-cleanup.json')}).", ""]
    if r["errors"]:
        lines += ["Recorded errors:", "", *[f"- {error}" for error in r["errors"]], ""]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(lines))
    print(args.output)


if __name__ == "__main__":
    main()
