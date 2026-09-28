#!/usr/bin/env python3
"""Plot saved observations; never synthesize or extrapolate benchmark values."""
import argparse
import json
import pathlib
import statistics

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("baseline", type=pathlib.Path)
parser.add_argument("revised", type=pathlib.Path)
parser.add_argument("cached", type=pathlib.Path)
parser.add_argument("output", type=pathlib.Path)
args = parser.parse_args()
reports = [json.loads(p.read_text()) for p in (args.baseline, args.revised, args.cached)]
labels = ["Earlier Neura", "Revised · cache off", "Revised · 32 MiB cache"]
colors = ["#8c96a3", "#126e82", "#bd5e2a"]
workloads = [
    ("parquet_count", "S3 full aggregate · small groups"),
    ("parquet_filter", "S3 selective filter"),
    ("postgres_rls", "PostgreSQL aggregate / RLS"),
    ("federated_group", "PG × S3 join · small groups"),
    ("federated_selective", "PG × S3 selective join"),
    ("parquet_count_coarse", "S3 full aggregate · large groups"),
    ("federated_group_coarse", "PG × S3 join · large groups"),
    ("local_high_cardinality", "Local aggregate · 1M groups"),
]
fig, axes = plt.subplots(1, 2, figsize=(15, 6.4), gridspec_kw={"width_ratios": [1.65, 1]})
for index, (label, color, report) in enumerate(zip(labels, colors, reports)):
    values = [statistics.median(s["latency_ms"] for s in report["samples"]
        if s["engine"] == "neura" and s["mode"] == "warm" and s["workload"] == name and s["correct"])
        for name, _ in workloads]
    offset = (index - 1) * .24
    axes[0].barh(np.arange(len(values)) + offset, values, height=.22, color=color, label=label)
    for row, value in enumerate(values):
        axes[0].text(value * 1.06, row + offset, f"{value:,.0f}", va="center", fontsize=8, color=color)
    names = ["federated_group", "federated_group_coarse", "local_high_cardinality"]
    memory = [next(m["peaks"]["sum_process_hwm_bytes"] / 1024**2 for m in report["memory"]
        if m["engine"] == "neura" and m["workload"] == name) for name in names]
    axes[1].barh(np.arange(3) + offset, memory, height=.22, color=color)
    for row, value in enumerate(memory):
        axes[1].text(value + 2, row + offset, f"{value:.0f}", va="center", fontsize=9, color=color)
axes[0].set_yticks(range(len(workloads)), [label for _, label in workloads])
axes[0].set_xscale("log")
axes[0].set_xlim(1, 40000)
axes[0].set_xlabel("Warm median latency (ms, log scale) · lower is better")
axes[0].set_title("Latency · five measured warm queries", loc="left", fontweight="bold")
axes[1].set_yticks(range(3), ["Small-group join", "Large-group join", "Local 1M groups"])
axes[1].set_xlim(0, max(230, axes[1].get_xlim()[1] * 1.1))
axes[1].set_xlabel("Process high-water RSS (MiB) · lower is better")
axes[1].set_title("Memory · separate two-query pass", loc="left", fontweight="bold")
for ax in axes:
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=.18)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)
fig.suptitle("Neura: measured optimization results", x=.025, y=.99, ha="left", fontsize=19, fontweight="bold")
fig.legend(*axes[0].get_legend_handles_labels(), loc="upper center", bbox_to_anchor=(.64, .965), ncol=3, frameon=False)
fig.text(.025, .025,
    "Same 1M-row fixture · 2 CPU / 2 GiB per engine · earlier observations are historical, not simultaneous.\n"
    "Shared host and uncontrolled source caches; memory includes runtime/startup. Source authorization remains active.", fontsize=9)
fig.subplots_adjust(left=.22, right=.975, top=.83, bottom=.17, wspace=.5)
args.output.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(args.output, dpi=160, facecolor="white")
plt.close(fig)
print(args.output)
