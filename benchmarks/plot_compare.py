#!/usr/bin/env python3
"""Plot recorded warm medians and separately observed proportional memory."""

import argparse
import json
import pathlib
import statistics

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

p = argparse.ArgumentParser()
p.add_argument("input", type=pathlib.Path)
p.add_argument("output_dir", type=pathlib.Path)
a = p.parse_args()
r = json.loads(a.input.read_text())
a.output_dir.mkdir(parents=True, exist_ok=True)
engines = list(r["engines"])
colors = {"neura": "#2563eb", "spark": "#f59e0b", "duckdb": "#16a34a"}
labels = {
    "parquet_count": "S3 full aggregate · small row groups",
    "parquet_filter": "S3 selective aggregate",
    "postgres_rls": "PostgreSQL aggregate with RLS",
    "federated_group": "PostgreSQL × S3 join · small groups",
    "federated_selective": "Selective PostgreSQL × S3 join",
    "parquet_count_coarse": "S3 full aggregate · large row groups",
    "federated_group_coarse": "PostgreSQL × S3 join · large groups",
    "local_high_cardinality": "Local aggregate · 1 million groups",
}
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
    "axes.spines.top": False, "axes.spines.right": False})
fig, ax = plt.subplots(figsize=(13, 8))
positions = np.arange(len(r["workloads"]))
for index, engine in enumerate(engines):
    values = []
    for workload in r["workloads"]:
        samples = [s["latency_ms"] for s in r["samples"] if s["engine"] == engine and
            s["workload"] == workload["name"] and s["mode"] == "warm" and s["correct"]]
        values.append(statistics.median(samples) if samples else float("nan"))
    y = positions + (index - (len(engines)-1)/2) * .23
    ax.barh(y, values, height=.21, label=engine.title(), color=colors[engine])
    for row, value in zip(y, values):
        if np.isfinite(value):
            ax.text(value * 1.08, row, f"{value:,.1f}", va="center", fontsize=8)
ax.set_yticks(positions, [labels[w["name"]] for w in r["workloads"]])
ax.invert_yaxis()
ax.set_xscale("log")
ax.set_xlim(.5, max(s["latency_ms"] for s in r["samples"] if s["correct"]) * 4)
ax.set_xlabel("Warm median latency in milliseconds · logarithmic scale · lower is better")
ax.set_title("Neura vs Spark 3.5.6 vs DuckDB 1.5.5", loc="left", fontsize=17, pad=20, weight="bold")
ax.legend(loc="lower right", frameon=False)
ax.grid(axis="x", alpha=.2)
ax.set_axisbelow(True)
fig.text(.02, .015, "1 million rows · 2 CPUs / 2 GiB per engine · 5 warm samples · DuckDB external file cache disabled\n"
    "HTTP full-fetch timings; adapters differ. Source/OS caches retained. Workload-specific results, not universal rankings.", fontsize=9, color="#475569")
fig.tight_layout(rect=(0, .065, 1, 1))
fig.savefig(a.output_dir / "spark-duckdb-latency.png", dpi=160)
plt.close(fig)

names = ["federated_group", "federated_group_coarse", "local_high_cardinality"]
fig, ax = plt.subplots(figsize=(12, 5))
positions = np.arange(len(names))
for index, engine in enumerate(engines):
    values = [next(m["peaks"]["pss_bytes"] / 1024**2 for m in r["memory"]
        if m["engine"] == engine and m["workload"] == name) for name in names]
    x = positions + (index - (len(engines)-1)/2) * .23
    bars = ax.bar(x, values, width=.21, label=engine.title(), color=colors[engine])
    ax.bar_label(bars, fmt="%.1f", padding=4, fontsize=9)
ax.set_xticks(positions, ["S3 join\nsmall row groups", "S3 join\nlarge row groups", "Local aggregation\n1 million groups"])
ax.set_ylabel("Observed peak proportional resident memory (MiB)")
ax.set_title("Memory during two concurrent queries", loc="left", fontsize=17, pad=20, weight="bold")
ax.set_ylim(0, ax.get_ylim()[1] * 1.15)
ax.legend(frameon=False)
ax.grid(axis="y", alpha=.2)
ax.set_axisbelow(True)
fig.text(.02, .015, "Separate memory pass after fresh engine startup; sampled about every 50 ms. Brief peaks may be missed.\n"
    "PSS apportions shared pages; this is different from RSS and container memory. Includes each engine's runtime.", fontsize=9, color="#475569")
fig.tight_layout(rect=(0, .095, 1, 1))
fig.savefig(a.output_dir / "spark-duckdb-memory.png", dpi=160)
print(a.output_dir)
