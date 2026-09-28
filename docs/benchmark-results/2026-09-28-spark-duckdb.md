# Neura, Spark and DuckDB: measured comparison

Started: 2026-09-28T11:56:15.259706+00:00. Finished: 2026-09-28T12:20:28.944149+00:00.

Correctness: **375/375** query results matched an independent Python oracle. Recorded harness errors: **0**. Failed queries are retained in the raw data.

Fixture: 1,000,000 orders, 200 PostgreSQL customers, 100 visible to the restricted reader. All engines read the same source objects with the same source credentials. RLS and forbidden-table probes must pass before measurement.

## Versions and resource limits

| Engine | Version | CPU limit | Container RAM / swap | Internal budget |
|---|---|---:|---|---|
| neura | 0.1.0 | 2 | 2 GiB / no extra swap | 512 MiB tracked operator/buffer memory |
| spark | 3.5.6 | 2 | 2 GiB / no extra swap | 512 MiB JVM heap |
| duckdb | 1.5.5 | 2 | 2 GiB / no extra swap | 512 MiB tracked operator/buffer memory |

These internal budgets have different accounting. The common cgroup limit is the hard boundary. Spark 3.5.6 is the pinned existing comparison runtime; this is not a latest-Spark comparison. Neura's engine source was unchanged for this experiment.

## Warm latency

End-to-end milliseconds, median of successful warm samples; min–max in parentheses. Each pair has one process-cold query, a separate successful priming query, then warm repetitions.

| Workload | neura | spark | duckdb |
|---|---:|---:|---:|
| parquet_count | 17372.67 (17119.57–17467.78) | 195.48 (181.87–224.67) | 10696.13 (10102.86–10869.25) |
| parquet_filter | 113.63 (107.23–118.94) | 218.74 (206.59–238.89) | 126.79 (115.76–150.29) |
| postgres_rls | 40.74 (40.02–42.44) | 189.33 (142.86–217.76) | 2.89 (2.76–3.42) |
| federated_group | 15900.60 (15642.28–16308.65) | 546.33 (498.60–591.48) | 20838.55 (20548.13–21938.60) |
| federated_selective | 423.95 (402.54–435.98) | 395.00 (379.87–445.08) | 740.20 (700.15–778.34) |
| parquet_count_coarse | 224.76 (195.21–263.44) | 318.83 (296.47–497.78) | 122.56 (121.64–191.25) |
| federated_group_coarse | 260.47 (239.95–316.56) | 705.06 (625.62–745.86) | 221.40 (205.92–225.76) |
| local_high_cardinality | 57.70 (55.02–63.60) | 403.39 (377.61–750.05) | 59.69 (58.71–60.56) |

## Process-cold latency

One sample per engine/workload, after service readiness. Startup is excluded. Source, operating-system and storage caches remain uncontrolled; **not storage-cold**.

| Workload | neura | spark | duckdb |
|---|---:|---:|---:|
| parquet_count | 16483.24 | 941.48 | 11052.98 |
| parquet_filter | 144.30 | 775.06 | 147.81 |
| postgres_rls | 45.42 | 782.68 | 6.17 |
| federated_group | 16331.55 | 1703.69 | 20576.01 |
| federated_selective | 491.15 | 1475.55 | 715.50 |
| parquet_count_coarse | 277.27 | 839.51 | 170.95 |
| federated_group_coarse | 297.53 | 1564.17 | 227.03 |
| local_high_cardinality | 71.30 | 1797.09 | 76.84 |

## Throughput

A finite batch at concurrency two, after priming; not an open-loop saturation study.

| Workload | Engine | Correct / attempted | Queries/s | Request median ms | Request p95 ms* |
|---|---|---:|---:|---:|---:|
| federated_group | neura | 20/20 | 0.094 | 20954.90 | 22411.61 |
| federated_group | spark | 20/20 | 1.806 | 1172.89 | 1204.55 |
| federated_group | duckdb | 20/20 | 0.053 | 37049.79 | 39611.18 |
| federated_group_coarse | neura | 20/20 | 6.197 | 320.39 | 335.94 |
| federated_group_coarse | spark | 20/20 | 1.976 | 939.07 | 1270.31 |
| federated_group_coarse | duckdb | 20/20 | 5.721 | 341.43 | 384.30 |
| local_high_cardinality | neura | 20/20 | 9.715 | 185.64 | 281.84 |
| local_high_cardinality | spark | 20/20 | 2.714 | 694.12 | 971.10 |
| local_high_cardinality | duckdb | 20/20 | 15.497 | 127.33 | 158.46 |

*Nearest-rank p95 over this small batch; not a reliable production tail-latency estimate.

## Memory, measured separately

Two simultaneous queries after a fresh engine start. No full-query priming. The native observer samples about every 50 ms and is excluded from process RSS/PSS, but included in cgroup usage. These runs are separate from timing runs. RSS can double-count shared pages; PSS apportions them. Sampled peaks are lower bounds on instantaneous peaks. Lifetime per-process high-water marks are also retained in JSON.

| Workload | Engine | Baseline RSS MiB | Sampled peak RSS MiB | Sum process HWM MiB* | Sampled peak PSS MiB | Cgroup peak since restart MiB |
|---|---|---:|---:|---:|---:|---:|
| federated_group | neura | 24.0 | 88.2 | 88.2 | 88.7 | 20.1 |
| federated_group | spark | 441.1 | 820.8 | 820.8 | 820.3 | 1208.8 |
| federated_group | duckdb | 89.8 | 124.6 | 124.9 | 125.3 | 131.4 |
| federated_group_coarse | neura | 24.1 | 84.0 | 84.7 | 83.8 | 15.9 |
| federated_group_coarse | spark | 426.7 | 776.0 | 776.0 | 775.1 | 1159.0 |
| federated_group_coarse | duckdb | 89.9 | 119.2 | 122.0 | 119.7 | 128.6 |
| local_high_cardinality | neura | 24.1 | 179.6 | 207.8 | 180.3 | 152.7 |
| local_high_cardinality | spark | 440.4 | 724.5 | 724.5 | 723.3 | 1131.7 |
| local_high_cardinality | duckdb | 90.0 | 217.1 | 254.2 | 217.6 | 265.4 |

*HWM is the kernel's lifetime resident high-water mark for each process, summed across engine processes. For a multi-process engine those individual peaks need not coincide. It is additional evidence, not a simultaneous total peak.

Cgroup accounting includes startup and filesystem cache, and shared mapped pages can be charged to other cgroups. It is not interchangeable with RSS or PSS. Baseline and query resident memory can also reflect earlier readiness work.

RSS is the approximate Linux VmRSS counter; PSS comes from smaps_rollup. They are read sequentially, so small RSS/PSS discrepancies can occur. Brief allocation peaks may exceed sampled RSS/PSS; consult the recorded process high-water marks as well.

## Layout and source diagnostics

Small layout: 978 row groups across two objects, 7,138,699 compressed bytes. Large layout: 8 row groups, 6,228,754 compressed bytes. Both contain the same logical rows. The local high-cardinality workload reads the large-layout files from a read-only bind mount and groups by one million distinct order IDs before reducing to three checked scalar results.

| Neura workload | Source | Requests | Response bytes | Rows |
|---|---|---:|---:|---:|
| parquet_count | lake | 987 | 1869124 | 1000000 |
| parquet_filter | lake | 10 | 1579808 | 1024 |
| postgres_rls | pg | 2 | 12 | 2 |
| federated_group | lake | 987 | 3000582 | 1000000 |
| federated_group | pg | 2 | 692 | 100 |
| federated_selective | lake | 19 | 1642351 | 10240 |
| federated_selective | pg | 2 | 692 | 100 |
| parquet_count_coarse | lake | 17 | 1606284 | 1000000 |
| federated_group_coarse | lake | 17 | 1667612 | 1000000 |
| federated_group_coarse | pg | 2 | 692 | 100 |
| local_high_cardinality | local | 15 | 7273422 | 1000000 |

Neura records source-specific payload bytes, not uniform network/egress billing bytes. Plans and operator metrics are retained in the raw results. Both Parquet tables are configured under one S3 source; Neura currently resolves their metadata during each referenced-source setup.

## Interpretation limits

- 2 CPU, 2 GiB cgroup RAM, no extra swap; 512 MiB internal budgets have different accounting.
- Cold is process-cold after readiness, excluding startup; source/OS/storage caches retained.
- Spark and DuckDB discover schemas during startup; Neura does so during queries.
- Authenticated HTTP end-to-end full-fetch latency; Neura NDJSON, other adapters JSON; adapters differ.
- No explicit result/table caches; DuckDB external file cache disabled. Connection/metadata/OS caches remain.
- Engines execute sequentially; their order rotates per workload. Shared host is not exclusive.
- Memory measured separately from latency at roughly 50ms intervals; short-lived peaks may be missed.
- RSS sums may double-count shared pages; PSS apportions them. Sum of process HWM is not a simultaneous peak.
- Cgroup peak includes startup, page cache and the small monitor process; shared pages may be charged elsewhere.
- Fixture remains static. Permission probes do not certify equivalent concurrent-mutation snapshot semantics.
- PostgreSQL and SeaweedFS source fixtures each have separate 1 CPU / 512 MiB limits. A spot check during DuckDB throughput observed S3 at its CPU cap and near its memory cap; this is a constrained-storage service comparison.
- The idle Neura demo was stopped during throughput and before all memory measurements to avoid a second instance sharing executable mappings. Other unrelated host workloads remained running.
- Memory includes each service runtime: native Rust HTTP in Neura, Python HTTP adapters for Spark/DuckDB. It is not an isolated execution-library footprint comparison.
- Eight workloads on a synthetic million-row fixture do not establish universal superiority, distributed scaling, production sizing, full SQL/type equivalence or cloud warehouse behavior.
- PostgreSQL grants/RLS are tested. Source data stays unchanged during measurement; this does not prove equal snapshot guarantees under concurrent writes. No governance shortcuts or privileged reader are used.


## DuckDB with its default external file cache

Separate profile: 97/97 result checks passed, 0 harness errors. Same limits, source identities and HTTP adapter. Only the external file cache is enabled. This is cached file data, not precomputed query results. These warm numbers must not be silently substituted into the baseline above.

| Workload | DuckDB cache off warm median ms | DuckDB cache on warm median ms | Cache on process-cold ms |
|---|---:|---:|---:|
| parquet_count | 10696.13 | 8.95 | 10465.75 |
| federated_group | 20838.55 | 19.10 | 20500.95 |
| federated_group_coarse | 221.40 | 11.43 | 187.27 |
| local_high_cardinality | 59.69 | 60.64 | 63.50 |

| Workload | Cache on throughput queries/s | Observed peak RSS MiB | Observed peak PSS MiB |
|---|---:|---:|---:|
| federated_group | 75.231 | 139.7 | 140.3 |
| federated_group_coarse | 131.456 | 124.8 | 125.2 |
| local_high_cardinality | 15.236 | 260.3 | 260.3 |

The same separate memory-pass caveats apply. The cache profile remains a static-fixture test; permission revocation, cache invalidation under updates, large working sets, and equal end-user governance have not been certified.

[Cache-profile raw observations](2026-09-28-duckdb-file-cache.json)


[Raw observations](2026-09-28-spark-duckdb.json) · [Reproduction](../../benchmarks/COMPARE.md)

[Warm-latency chart](spark-duckdb-latency.png) · [Memory chart](spark-duckdb-memory.png)
