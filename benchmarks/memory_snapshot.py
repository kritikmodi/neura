#!/usr/bin/env python3
"""Capture idle process RSS after the suite, separately from cgroup high-water marks."""

import datetime
import json
import pathlib
import subprocess

result = {
    "captured_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "notes": [
        "Idle snapshot after the entire suite; not per-query or peak process RSS.",
        "RSS is summed across engine processes; shared pages can be double-counted.",
        "Cgroup memory can charge shared mapped pages to other cgroups; it is a different measure.",
    ],
    "engines": {},
}
for engine in ["neura", "qe", "spark"]:
    output = subprocess.check_output(
        ["docker", "top", f"neura-bench-{engine}", "-eo", "pid,rss,comm"], text=True
    )
    rows = []
    for line in output.splitlines()[1:]:
        pid, rss, command = line.split(None, 2)
        rows.append({"pid": int(pid), "rss_bytes": int(rss) * 1024, "command": command})
    result["engines"][engine] = {
        "summed_rss_bytes": sum(row["rss_bytes"] for row in rows),
        "processes": rows,
    }
path = pathlib.Path(__file__).resolve().parents[1] / "results/process-memory.json"
path.parent.mkdir(exist_ok=True)
path.write_text(json.dumps(result, indent=2))
print(path)
