# Optimization tuning controls

These intermediate experiments informed the final defaults. They are retained to show tradeoffs, including slower cases; they are not final-release profiles. All use source commit `6211223`, the same immutable million-row fixture, 2 CPU / 2 GiB container limits and a 512 MiB total execution/cache budget. Raw records contain exact image, source/configuration hashes, workload SQL, resource settings and timestamps. Earlier and later profiles ran sequentially on a shared host.

| Control / raw results | Correct checks | Warm small-group join (ms) | Warm local aggregate (ms) | Local queries/sec | Local queries spilling | Local high-water RSS (MiB) |
|---|---:|---:|---:|---:|---:|---:|
| [Initial, 256 KiB, fixed two partitions](2026-09-28-control-initial.json) | 125/125 | 413.15 | 56.79 | 11.10 | 16/20 | 187.04 |
| [Initial + 32 MiB cache](2026-09-28-control-initial-cache.json) | 125/125 | 481.98 | 58.80 | 13.05 | 11/20 | 196.80 |
| [Fixed one partition, local workload only](2026-09-28-control-one-partition.json) | 30/30 | — | 93.51 | 23.14 | 0/20 | 238.74 |
| [Fixed 1 MiB window, four S3 workloads](2026-09-28-control-wide.json) | 51/51 | 222.56 | — | — | — | — |
| [Fixed 1 MiB + 32 MiB cache](2026-09-28-control-wide-cache.json) | 51/51 | 237.81 | — | — | — | — |

**382/382 control result checks passed.** Missing cells were not measured. All five profiles also passed the source permission probes. These are separate from the 250 checks in the two final profiles.

## Decisions supported by these controls

- Fixed one-partition local aggregation removed observed concurrent spills and improved throughput, but slowed the isolated warm query from 56.79 to 93.51 ms. Its high-water RSS increased from 187.04 to 238.74 MiB. The final implementation therefore selects partition targets at admission, preserving wider execution for an isolated query. This is a throughput/latency tradeoff, not evidence of a universal memory reduction.
- Fixed 1 MiB read-ahead improved dense full scans but increased the speculative window for short/selective reads. The final reader starts at no more than 256 KiB and grows only after dense forward consumption, up to 1 MiB within the shared per-query budget.
- Small-object caching saved origin payload bytes but retained authorization requests and showed inconsistent latency. It remains opt-in; the cache budget is deducted from execution memory.
- The final implementation changes several factors together. The final versus historical comparison cannot attribute every gain to one change or establish statistical significance.

## Reproducing controls

For exact historical behavior, build commit `6211223` in a separate checkout and retain its image. Create isolated benchmark containers from the same base configuration and restricted fixture principals; set the limits listed in each raw record. The one-partition control changes only `partitions=1`; its runner uses `--workloads local_high_cardinality`. The wide controls set `s3_read_ahead_kb=1024` and select `parquet_count parquet_filter federated_group federated_selective`. Do not enable the later admission-aware partition option on this historical implementation. Use the current harness only if its recorded source/image identity is preserved separately; a checkout hash alone does not identify an externally built image.

The final profiles instead build commit `bf18760` and use the current [preparation instructions](../../benchmarks/COMPARE.md#revised-neura-profiles). Config hashes and Rust source hashes in every raw record allow mismatched builds to be detected. Never reseed or replace objects while any test/query is active.

## Environment handling

The existing demo was temporarily stopped after confirming zero active queries, then restored and checked healthy. Exact UTC times and health response are saved in the [environment record](2026-09-28-optimization-environment.json). The demo still uses its original image; the optimized image is separately available as `neura:optimized-v2`. No unrelated containers were pruned or restarted. Fixtures stayed running, and source/OS/storage caches were not flushed.

[Final results and raw profiles](2026-09-28-optimizations.md).
