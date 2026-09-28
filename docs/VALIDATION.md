# Validation record

Validation performed on 2026-09-28 in the separate `/Users/kritikmodi/Neura` repository. CompanyBrain was not read or modified. Local host: Apple Silicon, macOS, 36 GiB physical RAM; Linux arm64 containers run under Docker Desktop. Rust 1.98.1, DataFusion 53.1.0, Arrow 58.4.0, Iceberg Rust 0.10.1. Dependency versions are locked in `Cargo.lock`.

## Completed checks

| Check | Evidence / scope |
|---|---|
| Unit tests | 12 tests: read-only SQL (including nested DML), parameter injection, strict Arrow decoding, decimal precision, snapshot SQL ASTs, pinned object ETags, diagnostics and stream-drop cancellation |
| Actual PostgreSQL × S3 | Five integration tests against PostgreSQL 17.6 and SeaweedFS 3.94: correct federated aggregation, remote projection/filter/aggregation, Parquet pruning (at most 1,024 scanned rows and fewer than 30 storage requests for `order_id<1000`), RLS, denied-table and denied-S3-bucket access, concurrency admission, cancellation of an active remote PostgreSQL statement and repeatable-read isolation during a concurrent update |
| Actual Iceberg | Local v2 metadata, Avro manifests and Parquet data: pinned historical snapshot, append, field-ID rename, copy-on-write delete, and combined position/equality delete files; old snapshot remains unchanged |
| Actual spill | Four million generated integer rows sorted with a 64 MiB shared operator pool and 32 MiB sort merge reservation; every row's ordering, count, sum and nonzero physical-plan spill metrics checked |
| Cube / PostgreSQL wire | Actual `@cubejs-backend/postgres-driver@1.6.57` and `pg@8.16.3`: Cube `testConnection`, session settings, extended-query binding, injection resistance, read-only rejection, RLS and federated aggregation |
| REST | Actual running service: bearer authentication, NDJSON schema/rows/completion, diagnostics lookup and mutation rejection |
| Docker | Linux arm64 release binary compiled inside the pinned Rust container, packaged with the runtime Dockerfile, run as UID/GID 10001, and verified by an actual PostgreSQL × S3 CLI query. The low-Docker-disk build path was used; the main multi-stage recipe was not separately completed |
| Static checks | `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, Python syntax compilation |

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

The connectors use real vendor APIs and compile; no cloud acceptance result is implied by that fact. Snapshot-clause and decoder unit tests are not substitutes for these live tests. Consult `examples/sources.toml`, then explicitly run:

```sh
NEURA_CLOUD_CONFIG=/secure/path/cloud.toml cargo test --test federation cloud_connectors -- --ignored --nocapture
```

Use read-only principals with deliberately denied tables and known reference results when expanding the acceptance suite. A failed permission test must never be “fixed” by increasing that principal's privileges.

## Benchmark interpretation

The reproducible harness and exact workload definitions are in `benchmarks/`. Published raw measurements and their summary are stored under `docs/benchmark-results/`. Each engine uses 2 CPUs and 2 GiB RAM with no additional swap. The same restricted source accounts, immutable fixture files and relational SQL are used. Every result is checked against an independent Python oracle.

Cold means **engine process restarted, storage caches uncontrolled**, after readiness checks. Warm means after a successful full priming query. There is no storage-cold claim. Cgroup peak memory includes startup and filesystem cache and is cumulative since each restart. This small local synthetic comparison cannot establish general superiority, production sizing, cloud egress economics or distributed scaling. See `benchmarks/README.md` for protocol and internal-memory-accounting differences.
