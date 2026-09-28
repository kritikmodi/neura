# Neura

A standalone, read-only SQL service built with Rust, Apache DataFusion 53.1 and Arrow 58.4. This repository has no CompanyBrain dependency and does not modify CompanyBrain. It is an initial implementation, with verified local federation and explicit production gaps in [the limitations report](docs/LIMITATIONS.md).

```sql
SELECT c.region, SUM(o.amount) AS revenue
FROM pg.customers c
JOIN lake.orders o ON c.id = o.customer_id
GROUP BY c.region;
```

Sources are configured by operators. PostgreSQL uses its SQL protocol, ClickHouse streams Arrow over its governed HTTP SQL interface, warehouses use their governed SQL APIs, S3 Parquet uses signed object requests, and Glue resolves Iceberg metadata. Query execution uses Arrow batches throughout the local engine. There are no substitute/fake connectors.

| Source | Implementation | Validation |
|---|---|---|
| PostgreSQL | Verified TLS, read-only repeatable-read transaction, streaming text protocol to Arrow, SQL subplan federation, EXPLAIN estimates | Actual PostgreSQL fixture: RLS, denied table, pushdown, snapshot, joins |
| ClickHouse | Read-only HTTP SQL, projection/filter/aggregate pushdown, bounded ArrowStream, source row policies, cancellation | Actual ClickHouse 25.8 fixture; see [connector contract](docs/CLICKHOUSE.md) |
| S3 Parquet | AWS credential chain, manifest of exact objects/ETags, conditional ranged reads, footer/statistics/filter pruning | Actual S3-compatible SeaweedFS fixture; **AWS S3 not credential-tested** |
| Local Parquet | Exact file manifest, file ETag checks, DataFusion Parquet scan | Same implementation as object scans |
| Iceberg | Official Apache Iceberg reader; static metadata/snapshot provider, field IDs, manifest planning, delete handling | Real local v2 snapshots, schema rename, delete rewrite, position/equality delete files; broader matrix pending |
| AWS Glue | SDK GetTable, frozen metadata pointer, Iceberg S3 FileIO; reject Lake Formation registered tables | Live AWS test blocked; compiles |
| Databricks | Statement Execution REST API, polling/chunks, cancellation, explicit Delta version | Live test blocked; snapshot AST/decoding unit tests |
| Snowflake | SQL API, role/warehouse, polling/partitions, cancellation, timestamp time travel | Live test blocked; snapshot AST/decoding unit tests |
| BigQuery | Query REST API, polling/page tokens, cancellation, billed-bytes ceiling, system-time read | Live test blocked; snapshot AST/decoding unit tests |

## Run locally

Install Rust via [rustup](https://rustup.rs/); the toolchain file pins 1.98.1. Build dependencies on Linux include a C compiler, pkg-config, OpenSSL development headers and CMake.

```sh
cargo build --locked
cargo run --locked -- -c examples/empty.toml query 'SELECT 1 AS hello'
```

Results are NDJSON: a schema record, zero or more row records, and a completion record. Values are strings or null to preserve 64-bit integers and decimals. A missing completion record means the result is incomplete. `query '...' --diagnostics` writes diagnostics to stderr. `check-config` validates structure without contacting sources.

The default build serves only the streaming REST API:

```sh
export NEURA_API_TOKEN="$(openssl rand -hex 32)"
cargo run --locked -- -c examples/empty.toml serve
curl http://127.0.0.1:8080/v1/query \
  -H "Authorization: Bearer $NEURA_API_TOKEN" -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT 1"}'
```

PostgreSQL **source reads** do not require a PostgreSQL-wire **listener**. The listener and its protocol dependency are excluded from default builds. Keep it only when using Cube’s existing PostgreSQL driver or SQL clients: build with `--features postgres-wire` and configure `server.pg_addr` ([example](examples/wire.toml)). Without that feature, a configured PG listener fails validation rather than being silently ignored. REST or the Rust API is the intended CompanyBrain integration boundary.

Copy and adapt [examples/sources.toml](examples/sources.toml) for all sources, or the standalone [PostgreSQL](examples/postgres.toml) and [ClickHouse](examples/clickhouse.toml) examples. Remote schemas are explicit contracts; unsupported types and invalid casts fail. Secrets are environment references, never literal configuration fields. For non-AWS secrets, `NAME_FILE` can reference a mounted secret file instead of `NAME`; it is read at use time. Use AWS's credential providers for AWS credentials. Do not place production secrets in Git.

## Reproduce integration tests

Docker and [uv](https://docs.astral.sh/uv/) are needed. Test credentials in `deploy/` and `scripts/test-env.sh` belong solely to disposable localhost fixtures.

```sh
scripts/integration.sh
uv run --with 'pyiceberg[sql-sqlite]==0.10.0' --with pyarrow==21.0.0 scripts/iceberg_fixture.py
uv run --with 'pyiceberg[sql-sqlite]==0.10.0' --with pyarrow==21.0.0 scripts/iceberg_delete_fixture.py
cargo test --lib
cargo test --test iceberg -- --include-ignored
source scripts/test-env.sh
cargo run -- -c data/test.toml serve
# In another shell, after sourcing scripts/test-env.sh:
python3 scripts/api-test.py
# Stop the REST server before the optional compatibility test:
scripts/wire-integration.sh
```

`cargo test` never silently substitutes cloud services. Credential-dependent tests are explicitly ignored; `cargo test --test federation cloud_connectors -- --ignored` requires `NEURA_CLOUD_CONFIG`. See [validation](docs/VALIDATION.md) for exactly what has and has not run.

## Deploy and integrate

```sh
docker build -t neura:0.1.0 .
# If Docker's VM disk is small, use scripts/build-low-disk.sh instead.
export NEURA_CONFIG=/absolute/path/neura.toml
export NEURA_ENV_FILE=/absolute/path/protected-neura.env
docker compose -f deploy/compose.yml up -d
```

The container runs as an unprivileged user. The supplied deployment has CPU, memory, PID and filesystem limits. Listener TLS terminates at an external proxy; public plaintext deployment is unsupported. Container binding requires explicit `allow_insecure_network=true`; the sample publishes only to localhost. For durable disk spilling, replace the example bounded tmpfs with a private disk volume with an enforced quota. Tmpfs counts toward the container memory limit.

[API and Cube integration](docs/API.md) · [Architecture and governance](docs/ARCHITECTURE.md) · [Benchmarks](benchmarks/README.md) · [Limitations](docs/LIMITATIONS.md)

The [Neura / Spark / DuckDB comparison](docs/benchmark-results/2026-09-28-spark-duckdb.md) passed **472/472** result checks across its baseline and separate DuckDB default-file-cache profile. With identical 2 CPU / 2 GiB engine limits, Neura's local million-group aggregate was 57.7 ms warm versus DuckDB's 59.7 ms and Spark's 403.4 ms. Neura's S3 join was 15.9 s with small row groups and 260 ms with larger groups; source requests fell from 987 to 17. DuckDB's default file cache reduced its warm join to 19.1 ms on this small static fixture. Neura had lower observed resident memory in the measured cases, but is not the fastest engine overall. [Reproduce the suite](benchmarks/COMPARE.md); cold/warm definitions, raw samples, source-resource constraints and memory caveats are included.

The [earlier QE / Spark comparison](docs/benchmark-results/2026-09-28.md) passed all 120 result checks. On its million-row, small-row-group fixture, Neura's warm federated aggregate median was 16.09 s, versus 31.36 ms for Datatailr QE and 452.95 ms for Spark under the same 2 CPU / 2 GiB container limits. Neura is substantially slower on this full-scan workload; ranged-read overhead remains a concrete optimization gap. Protocol/authentication and memory-accounting differences are documented with the raw results. No cloud performance or universal superiority claim is made.
