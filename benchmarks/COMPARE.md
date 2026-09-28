# Neura versus Spark and DuckDB

This suite compares the unchanged Neura engine with Spark 3.5.6 and DuckDB 1.5.5,
using real PostgreSQL and S3-compatible storage. It supplements the historical
QE/Spark baseline; it does not overwrite those results.

## Reproduce

The supplied runtime is pinned to Linux arm64 containers. Start the local fixtures
and build Neura as described in [README.md](README.md). The Spark runtime needs the
`neura-build:1.98.1` image produced by the low-disk build setup. Python, `uv`, Docker,
outbound access for pinned dependencies, and adequate host disk are required.

```sh
docker compose -f deploy/compose.test.yml up -d --wait postgres s3
# Seed once; never reseed while tests or queries are running.
NEURA_FIXTURE_ROWS=1000000 uv run --with pyarrow==21.0.0 --with boto3==1.40.40 scripts/seed.py
source scripts/test-env.sh
bash scripts/build-low-disk.sh
python3 benchmarks/prepare_compare.py
python3 benchmarks/compare.py --smoke --output results/spark-duckdb-smoke.json
python3 benchmarks/compare.py --repeats 5 --throughput-queries 20 --concurrency 2
python3 benchmarks/summarize_compare.py results/spark-duckdb.json results/spark-duckdb.md
uv run --with matplotlib==3.10.6 benchmarks/plot_compare.py results/spark-duckdb.json results/plots
```

`prepare_compare.py` refuses to replace existing containers. Subsequent runs can
reuse the stopped `neura-compare-neura`, `neura-compare-spark` and
`neura-compare-duckdb` containers. The harness starts/stops only these containers
and leaves them stopped on completion. Do not prune unrelated Docker resources.
Workspace-backed `/tmp` directories avoid relying on free space in Docker's VM.

The setup downloads a checksummed DuckDB Python wheel, installs official signed
`httpfs` and `postgres` extensions, and records extension hashes in the result.
Spark uses the checksummed JRE/JDBC/S3 dependencies in `prepare.py`. All engines
use **2 CPUs, 2 GiB container RAM, no extra swap**, two execution threads/partitions
and a nominal **512 MiB** internal budget. Spark's heap and the other engines'
tracked memory pools do not account for the same allocations.

The fixture generator downloads and hashes the actual S3 objects, preserves the
existing small-row-group files, and writes equivalent 131,072-row-group files
under a separate content-addressed S3 prefix. Exact filenames, hashes, sizes and
ETags are recorded. Local operator tests read the larger-row-group files through
read-only container mounts. The same restricted PostgreSQL and S3 identities are
used throughout. A PostgreSQL row-policy probe and source-confirmed forbidden-table
rejection run before timing. No database table is copied into an engine cache.

## What is measured

Eight queries cover full/selective Parquet aggregation, PostgreSQL RLS, full and
selective PostgreSQL × S3 joins, equivalent scans/joins with larger row groups,
and local aggregation over a million distinct groups. Every result, including
priming, throughput and memory-pass queries, is checked against a separate Python
oracle. Wrong results and failures remain in the JSON and cause an unsuccessful
exit. SQL workload text is checked in; only catalog identifiers change for Spark's
temporary views. DuckDB uses its real read-only PostgreSQL attachment and S3 reader.

For each workload, engine order rotates. Each engine starts afresh, completes
readiness, executes one process-cold query, executes a full priming query, and
executes five warm queries. Startup time is recorded separately. Source, OS and
storage caches are not flushed: **there is no storage-cold claim**. Spark and DuckDB
resolve schemas during startup; Neura resolves them during query setup. This is
a service comparison, not isolated operator timing. No explicit result/table
caching is enabled; DuckDB's external file cache is explicitly disabled. Normal
metadata, connection and OS caching still occurs.

All clients use authenticated HTTP and fully fetch results. Neura streams NDJSON;
Spark/DuckDB use the same minimal Python adapter returning JSON. Adapters and auth
implementations differ, so short-query times include those differences. The adapter
accepts only exact benchmark and probe SQL, listens on a localhost-published port,
and is a fixture utility, not a Neura connector or production service.
Memory includes these runtimes too: Neura's HTTP server is native Rust, while the
Spark/DuckDB measurements include their Python adapter. This is not an isolated
execution-library footprint comparison. Source-container image identities and
limits are recorded separately; the published run used 1 CPU / 512 MiB per source.

Throughput uses a finite batch of 20 queries at concurrency two for three workloads.
It is not an open-loop saturation test; a p95 from 20 observations is not a stable
production tail estimate. The shared host is not exclusively reserved.

Memory is measured in a **separate pass** so the observer does not affect latency
samples. Following a fresh start and readiness, a native observer samples roughly
every 50 ms while two queries run simultaneously. It reads process RSS, PSS,
private pages and lifetime per-process high-water marks, plus cgroup current/peak
memory. The observer is excluded from process sums but consumes a small amount of
cgroup memory and CPU during that pass. Short-lived peaks can fall between samples.
RSS can double-count shared pages; PSS apportions them. Summing process high-water
marks is not a simultaneous peak. Cgroup accounting includes startup/file cache,
and can charge shared mappings elsewhere; do not label it process RSS. All raw
observations are retained. Local bind mounts and spill files use host storage;
the 4 GiB spill setting is per Neura query, versus DuckDB's database-wide setting.
RSS is read from `/proc/<pid>/status`, PSS from `smaps_rollup`. These counters are
read sequentially and VmRSS is approximate, so small RSS/PSS discrepancies can
occur. Use the raw high-water marks as additional evidence for brief allocation
peaks missed by the sampling interval, not as a sum of simultaneous process peaks.

Source data remains static for the whole experiment. Passing permission and
result checks does not certify identical snapshot semantics under concurrent
writes. This suite does not exercise cloud warehouses, distributed execution,
Iceberg evolution, or the full SQL/type surface.

## Separate DuckDB default-cache profile

The baseline explicitly disables DuckDB's external file cache. That is useful for
comparing repeated reads, but it is not DuckDB's default configuration. After the
three-engine suite finishes, run:

```sh
source scripts/test-env.sh
python3 benchmarks/cache_compare.py
python3 benchmarks/summarize_compare.py results/spark-duckdb.json results/spark-duckdb.md --cache results/duckdb-file-cache.json
uv run --with matplotlib==3.10.6 benchmarks/plot_compare.py results/spark-duckdb.json results/plots --cache results/duckdb-file-cache.json
```

This creates a separate `neura-cache-duckdb` container on localhost port 58104 with
the same resources, identities and adapter, enabling only the external file cache.
It measures four representative workloads and saves `results/duckdb-file-cache.json`.
The setting is asserted from live metadata. Warm cached results must be labelled
separately from the baseline; process-cold measurements and the separate memory
pass remain present. This is file caching, not precomputed query-result caching.

## Revised Neura profiles

Keep the original image and benchmark containers for historical reproduction.
Build the revised engine under a separate tag, then create separate profiles:

```sh
source scripts/test-env.sh
NEURA_IMAGE=neura:optimized scripts/build-low-disk.sh
python3 benchmarks/prepare_optimized.py --image neura:optimized
NEURA_BENCH_PREFIX=neura-optimized- NEURA_BENCH_NEURA_PORT=58112 \
  NEURA_BENCH_TMP_ROOT="$PWD/data/optimized/runtime" \
  python3 benchmarks/compare.py --engines neura --output results/optimized.json
NEURA_BENCH_PREFIX=neura-optimized-cache- NEURA_BENCH_NEURA_PORT=58114 \
  NEURA_BENCH_TMP_ROOT="$PWD/data/optimized-cache/runtime" \
  python3 benchmarks/compare.py --engines neura --output results/optimized-cache.json
python3 benchmarks/report_optimized.py \
  docs/benchmark-results/2026-09-28-spark-duckdb.json \
  results/optimized.json results/optimized-cache.json results/optimized.md \
  --duckdb-cache docs/benchmark-results/2026-09-28-duckdb-file-cache.json
uv run --with matplotlib==3.10.6 benchmarks/plot_optimized.py \
  docs/benchmark-results/2026-09-28-spark-duckdb.json \
  results/optimized.json results/optimized-cache.json results/optimized.png
```

Both profiles retain the same fixture and resource limits. The first uses the
revised defaults with no reusable file cache; the second enables a 32 MiB cache,
deducted from the 512 MiB total execution/cache budget. Cache hits perform live
conditional GetObject authorization. The harness records the actual config hash
and resource-limit section. `prepare_optimized.py` refuses to replace containers.
Finish one profile before starting the next. Existing demo processes mapping the
same binary can affect PSS accounting; record any temporary pause and restore
them after measurement.

The generated report explicitly labels earlier engine measurements as historical
references. Do not describe them as a simultaneous rerun of Spark and DuckDB.

The [published final profiles](../docs/benchmark-results/2026-09-28-optimizations.md)
used source commit `bf18760`, image `neura:optimized-v2`, prefix `neura-final` and
port 58122 (cache profile: 58124). Supply `--prefix neura-final --port-start 58122`
to preparation to recreate those names in a clean setup; retained containers must
not be replaced. All other limits/workloads remain as above. The report generator
accepts `--chart PATH --controls PATH` to link a chart and tuning record. Source
hashes and actual configuration sections in the raw JSON are authoritative.

## Official connector references

- [DuckDB PostgreSQL extension](https://duckdb.org/docs/current/core_extensions/postgres/overview)
- [DuckDB S3 support](https://duckdb.org/docs/current/core_extensions/httpfs/s3api)
- [DuckDB memory/cache configuration](https://duckdb.org/docs/current/configuration/overview)
- [Spark SQL tuning](https://spark.apache.org/docs/3.5.6/sql-performance-tuning.html)
