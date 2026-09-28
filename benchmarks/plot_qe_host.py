#!/usr/bin/env python3
"""Chart measured successful queries; resource failures never become fast bars."""
import argparse
import json
import pathlib
import statistics

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("input", type=pathlib.Path)
parser.add_argument("output", type=pathlib.Path)
args = parser.parse_args()
r = json.loads(args.input.read_text())
assert not r["errors"], "Do not chart an invalid harness/source run as a performance comparison"
engines = list(r["engines"])
names = {"neura": "Neura", "qe305": "QE pre305 · running image", "qe341": "QE pre341 · installed image"}
colors = {"neura": "#126e82", "qe305": "#a55b32", "qe341": "#8373b2"}
labels = {
    "parquet_count": "S3 aggregate · small groups", "parquet_filter": "S3 selective filter",
    "postgres_rls": "PostgreSQL aggregate / RLS", "federated_group": "PG × S3 join · small groups",
    "federated_selective": "PG × S3 selective join", "parquet_count_coarse": "S3 aggregate · large groups",
    "federated_group_coarse": "PG × S3 join · large groups", "local_high_cardinality": "Local 1M groups · subquery",
    "local_distinct_control": "Local distinct-count control",
}
workloads = [w["name"] for w in r["workloads"]]
memory_workloads = ["federated_group", "federated_group_coarse", "local_high_cardinality", "local_distinct_control"]
fig, axes = plt.subplots(1, 2, figsize=(16, 7), gridspec_kw={"width_ratios": [1.6, 1]})
max_latency, max_memory = 1, 1
for i, engine in enumerate(engines):
    offset = (i - (len(engines) - 1) / 2) * .24
    values = []
    for name in workloads:
        samples = [s for s in r["samples"] if s["engine"] == engine and s["workload"] == name and s["mode"] == "warm"]
        values.append(statistics.median(s["latency_ms"] for s in samples) if samples and all(s["correct"] for s in samples) else float("nan"))
    axes[0].barh(np.arange(len(workloads)) + offset, values, height=.22, color=colors[engine], label=names[engine])
    for row, value in enumerate(values):
        if np.isnan(value):
            failures = [s for s in r["samples"] + r["primes"] if s["engine"] == engine and s["workload"] == workloads[row] and not s["correct"]]
            label = "resource limit" if any("memory limit" in s.get("error", "") for s in failures) else "query failed" if failures else "not measured"
            axes[0].text(1.1, row + offset, label, va="center", fontsize=8, color=colors[engine])
        else:
            max_latency = max(max_latency, value)
            axes[0].text(value * 1.045, row + offset, f"{value:.1f}", va="center", fontsize=8, color=colors[engine])
    memory = []
    for name in memory_workloads:
        m = next((m for m in r["memory"] if m["engine"] == engine and m["workload"] == name), None)
        memory.append(m["peaks"]["sum_process_hwm_bytes"] / 1024**2 if m and all(s["correct"] for s in m["checks"]) else float("nan"))
    axes[1].barh(np.arange(len(memory)) + offset, memory, height=.22, color=colors[engine])
    for row, value in enumerate(memory):
        if np.isnan(value):
            axes[1].text(8, row + offset, "query failed", va="center", fontsize=8, color=colors[engine])
        else:
            max_memory = max(max_memory, value)
            axes[1].text(value + 4, row + offset, f"{value:.0f}", va="center", fontsize=9, color=colors[engine])
axes[0].set_yticks(range(len(workloads)), [labels[w] for w in workloads])
axes[0].set_xscale("log")
axes[0].set_xlim(1, max_latency * 1.65)
axes[0].set_xlabel("Warm median milliseconds · log scale · lower is better")
axes[0].set_title("Latency · seven warm observations", loc="left", fontweight="bold")
axes[1].set_yticks(range(len(memory_workloads)), ["Small-group join", "Large-group join", "Local grouped subquery", "Local distinct control"])
axes[1].set_xlim(0, max_memory * 1.15)
axes[1].set_xlabel("Process high-water RSS (MiB) · lower is better")
axes[1].set_title("Memory · separate two-query pass", loc="left", fontweight="bold")
for ax in axes:
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=.18)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
fig.suptitle("Neura versus Query Engine on Hetzner", x=.025, y=.985, ha="left", fontsize=19, fontweight="bold")
fig.legend(*axes[0].get_legend_handles_labels(), loc="upper center", bbox_to_anchor=(.64, .951), ncol=3, frameon=False)
fig.text(.025, .025,
    "Same host, SQL and 1M-row fixture · 2 CPU / 2 GiB per engine · reusable data/result caches off · source permissions active.\n"
    "Shared pools: 512 MiB; QE additionally has a 1 GiB estimate guard. Shared host; uncontrolled storage caches; failed queries excluded from bars.", fontsize=9)
fig.subplots_adjust(left=.205, right=.97, top=.83, bottom=.16, wspace=.52)
args.output.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(args.output, dpi=160, facecolor="white")
plt.close(fig)
print(args.output)
