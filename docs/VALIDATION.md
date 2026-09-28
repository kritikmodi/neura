# Validation record

Validation performed on 2026-09-28 in the separate `/Users/kritikmodi/Neura` repository. CompanyBrain was not read or modified. Local host: Apple Silicon, macOS, 36 GiB physical RAM; Linux arm64 containers run under Docker Desktop. Rust 1.98.1, DataFusion 53.1.0, Arrow 58.4.0, Iceberg Rust 0.10.1. Dependency versions are locked in `Cargo.lock`.

## Optimization validation

The final optimization implementation is source commit `bf18760`. Its release
image is `neura:optimized-v2`, ID
`sha256:5dccf623ccc44a6066b4b7552447db193ed112366ce537acb1a5be3a45c92939`.
Subsequent changes publish reports and documentation; the raw benchmark records
retain source and configuration hashes.

| Check | Final result / scope |
|---|---|
| Default unit/config tests | 51 library tests and two connector/config tests passed; `cargo test --all-targets --locked`. Two credential-dependent PostgreSQL pool tests were skipped in this ordinary run, then run explicitly below |
| Real PostgreSQL pool | Both tests passed: bounded checkout, connection reuse with a fresh repeatable-read snapshot, live RLS/role/permission checks, reset state, cancellation and abandoned-session discard |
| Real PostgreSQL × S3 | All six local federation tests passed, including early cross-source `LIMIT` termination followed by a new PostgreSQL query; existing pruning, permissions, snapshot and cancellation cases retained |
| Real S3 cache | Pinned-object replacement/deletion invalidation test passed against SeaweedFS; a unique temporary object was cleaned up. Unit tests additionally cover GET denial despite allowed HEAD, credential/session rotation, scope isolation, concurrent fills, malformed bodies, cancellation and retained-allocation accounting |
| Real ClickHouse / Iceberg | All four ClickHouse and both Iceberg tests rerun and passed on the final implementation |
| Real spill | Four-million-row sort test passed with correct ordering/count/sum and nonzero spill metrics |
| Cube compatibility | Actual Cube PostgreSQL driver and node-postgres integration passed against the optional wire build |
| Static/build checks | Default and `postgres-wire` clippy with `-D warnings`, formatting, Python syntax, diff checks and Linux arm64 release-image build passed |
| Final benchmark profiles | **250/250** result checks passed across the two eight-workload profiles; both also passed live PostgreSQL RLS and denied-table probes with source logs |
| Tuning controls | **382/382** additional result checks passed across five separately retained control profiles; these are not final-release results |

Commands for the explicitly enabled local checks, after `source scripts/test-env.sh`:

```sh
cargo test --lib postgres_pool -- --ignored --test-threads=1
cargo test --test federation -- --ignored --skip cloud_connectors --test-threads=1
cargo test --test storage_cache -- --ignored --test-threads=1
cargo test --test clickhouse -- --ignored --test-threads=1
cargo test --test iceberg -- --ignored --test-threads=1
scripts/wire-integration.sh
```

The [final measurements](benchmark-results/2026-09-28-optimizations.md) show
substantial S3-request and warm PostgreSQL overhead improvements, but no general
peak-memory win. Default local aggregation high-water RSS rose from 208 to
237 MiB while throughput improved from 9.7 to 22.3 queries/s. The cache remains
opt-in because measured latency did not consistently improve. Historical
Spark/DuckDB measurements were not rerun alongside these final Neura profiles.
Raw tuning controls and the temporary demo pause/restoration record are linked
from the report. The original demo was restored healthy after measurement; it
still runs its original image, rather than the new optimized image.

Cloud acceptance remains blocked exactly as listed below. Local permission and
cache tests do not certify AWS IAM/STS, production network failures or all
concurrent-mutation cases.

## Initial implementation checks (before optimization)

| Check | Evidence / scope |
|---|---|
| Unit tests | Default build: 14 library tests plus two connector/config tests; optional wire adds its parameter test. Includes read-only SQL (including nested DML), parameter injection, strict Arrow decoding, decimal precision, snapshot SQL ASTs, pinned object ETags, diagnostics and stream-drop cancellation |
| Actual PostgreSQL × S3 | Five integration tests against PostgreSQL 17.6 and SeaweedFS 3.94: correct federated aggregation, remote projection/filter/aggregation, Parquet pruning (at most 1,024 scanned rows and fewer than 30 storage requests for `order_id<1000`), RLS, denied-table and denied-S3-bucket access, concurrency admission, cancellation of an active remote PostgreSQL statement and repeatable-read isolation during a concurrent update |
| Actual ClickHouse | Four live tests against ClickHouse 25.8.4.13: restricted reader and tenant row policy, denied table, remote projection/filter/aggregation, exact types/NULL/empty results, outer joins, three-source PostgreSQL/S3 join, rejection of split source snapshots, 200,000 rows streamed at the default 4,096-row batch size through a 1 MiB frame cap, active remote query cancellation, oversized-frame rejection, source write rejection and enforcement of source resource constraints |
| Actual Iceberg | Local v2 metadata, Avro manifests and Parquet data: pinned historical snapshot, append, field-ID rename, copy-on-write delete, and combined position/equality delete files; old snapshot remains unchanged |
| Actual spill | Four million generated integer rows sorted with a 64 MiB shared operator pool and 32 MiB sort merge reservation; every row's ordering, count, sum and nonzero physical-plan spill metrics checked |
| Optional Cube / PostgreSQL wire | `postgres-wire` is opt-in; actual `@cubejs-backend/postgres-driver@1.6.57` and `pg@8.16.3`: Cube `testConnection`, session settings, extended-query binding, injection resistance, read-only rejection, RLS and federated aggregation |
| REST | Actual running service: bearer authentication, NDJSON schema/rows/completion, diagnostics lookup and mutation rejection |
| Docker | Linux arm64 release binary compiled inside the pinned Rust container, packaged with the runtime Dockerfile, run as UID/GID 10001, and verified by an actual PostgreSQL × S3 CLI query. The REST-only default was also built and its five benchmark workloads checked through the HTTP client (correctness checks only, not a new timing comparison). Local Docker VM disk exhaustion required a workspace-backed temporary/spill directory for the demo. The low-Docker-disk build path was used; the main multi-stage recipe was not separately completed |
| Static checks | `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` with default and `postgres-wire` features, Python/shell syntax checks |

The PostgreSQL/S3 integration was exercised with both the original 10,000-row fixture and the later 1,000,000-row benchmark fixture. SeaweedFS is a real S3-compatible object service; these tests do **not** certify Amazon S3 IAM, STS or Glue behavior. Cube compatibility covers its driver/query path, not a deployed Cube application, pre-aggregations, every introspection endpoint or every PostgreSQL type.

macOS's linker emits an `__eh_frame`/compact-unwind size warning for the large debug executable. It did not prevent tests from running. CI configuration is supplied, but no hosted GitHub Actions run is claimed.

## Credential-blocked tests

No cloud credentials were available. `cloud_connectors` is explicitly ignored unless invoked and requires `NEURA_CLOUD_CONFIG` pointing to a configuration containing all four cloud source types. It sends real COUNT queries; it is a smoke test, not the full production acceptance matrix.

| Blocked live source | Missing inputs | Tests that remain unexecuted |
|---|---|---|
| AWS S3 + Glue | AWS credential provider/role, region, Glue database/table, IAM-only S3 warehouse, usable Iceberg snapshot | IAM denial, STS rotation, GetTable permissions, Lake Formation registered-table rejection, pinned metadata and deleted-file behavior on S3, range requests against AWS |
| Databricks | Workspace HTTPS host, warehouse ID, `DATABRICKS_TOKEN` (or file), restricted table/schema and retained Delta version | SQL execution, row/column policies, chunk pagination, truncation rejection, cancellation, expired token and time travel |
| Snowflake | Account host, warehouse, database/schema, read-only role, `SNOWFLAKE_TOKEN` (or file), timestamp within retention | SQL API execution, policy enforcement, partitions, cancellation, token expiry, numeric/timestamp decoding and time travel |
| BigQuery | Project/location, `BIGQUERY_TOKEN` (or file), dataset/table, retained timestamp, billing ceiling | Query/poll/page APIs, policy tags/row policies, cancellation, token expiry, decoded types, bytes-billed ceiling and time travel |

ClickHouse Cloud and distributed deployments are also untested; local ClickHouse has a real integration fixture and does not need cloud credentials.

The cloud connectors use real vendor APIs and compile; no cloud acceptance result is implied by that fact. Snapshot-clause and decoder unit tests are not substitutes for these live tests. Consult `examples/sources.toml`, then explicitly run:

```sh
NEURA_CLOUD_CONFIG=/secure/path/cloud.toml cargo test --test federation cloud_connectors -- --ignored --nocapture
```

Use read-only principals with deliberately denied tables and known reference results when expanding the acceptance suite. A failed permission test must never be “fixed” by increasing that principal's privileges.

## Benchmark interpretation

The earlier [Neura/Spark/DuckDB suite](benchmark-results/2026-09-28-spark-duckdb.md)
used the pre-optimization Neura engine, Spark 3.5.6 and DuckDB 1.5.5. **375/375** baseline
query-result checks passed (144 timed samples, 33 priming queries, 180 throughput
requests and 18 separate memory-pass queries). A separately labelled DuckDB
default-file-cache profile passed **97/97** checks. All engines passed real
PostgreSQL RLS and forbidden-table probes with source-log evidence. The suite
compares two Parquet layouts, eight workloads, concurrency two, and separately
sampled process RSS/PSS/high-water marks and cgroup memory. No engine implementation
was changed for this experiment. Python syntax checks and native C observer
compilation with warnings as errors passed. Charts were rendered and inspected.

The local million-group aggregate was 57.7 ms warm in Neura, 59.7 ms in uncached
DuckDB and 403.4 ms in Spark. Neura's full S3 join changed from 15.9 s to 260 ms
when the same rows used larger row groups. DuckDB's default external file cache
reduced its warm small-row-group join to 19.1 ms; its process-cold query remained
20.5 s. These cache modes are explicitly separate. Source fixtures each have a
1 CPU / 512 MiB limit; S3 saturation was observed during uncached DuckDB throughput.
This is a constrained-storage service experiment, not an isolated engine or
latest-Spark comparison. Historical results below remain unchanged.

The published baseline predates the REST-only default and used the wire compatibility path. Historical results are unchanged. The current harness uses Neura REST with bearer authentication and records client protocol metadata; no new performance or memory improvement is inferred from removing the optional listener.

The reproducible harness and exact workload definitions are in `benchmarks/`. [Published measurements](benchmark-results/2026-09-28.md) include raw samples, image/source fingerprints and a separate idle RSS observation. All 90 latency samples and 30 throughput requests passed independent correctness checks. Each engine uses 2 CPUs and 2 GiB RAM with no additional swap. The same restricted source accounts, immutable fixture files and relational SQL are used.

Warm federated-group medians: Neura 16,091.97 ms, Datatailr QE 31.36 ms, Spark 452.95 ms. At concurrency two, measured correct-query throughput was 0.095, 58.240 and 2.527 queries/s respectively. The earlier Neura was materially slower on this full-scan fixture. Its warm selective Parquet median was 83.61 ms; file/row-group pruning worked, while cross-row-group read-ahead was then an optimization gap. These are historical measurements, not a performance replacement claim.

Cold means **engine process restarted, storage caches uncontrolled**, after readiness checks. Warm means after a successful full priming query. There is no storage-cold claim. Cgroup peak memory includes startup and filesystem cache and is cumulative since each restart. This small local synthetic comparison cannot establish general superiority, production sizing, cloud egress economics or distributed scaling. See `benchmarks/README.md` for protocol and internal-memory-accounting differences.
