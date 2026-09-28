# Interfaces

REST listens on `127.0.0.1:8080` by default. No PostgreSQL listener is enabled in the default build. `GET /healthz` is unauthenticated and exposes only health/version. All `/v1` endpoints require `Authorization: Bearer <NEURA_API_TOKEN>`; comparisons use constant-time SHA-256 digest comparison.

| Endpoint | Behavior |
|---|---|
| `POST /v1/query` | Body `{"sql":"SELECT ...","id":"optional UUID"}`. Streams NDJSON schema, rows, then completion or error. |
| `GET /v1/sources` | Source names, kinds and configured table names; no credentials. |
| `GET /v1/queries` | Bounded query diagnostic history. |
| `GET /v1/queries/{id}` | Status, SQL hash, logical/physical plans, source metrics, result counts. |
| `DELETE /v1/queries/{id}` | Cancel an active query; 202 on request, 404 if absent/finished. |

Supply a UUID before submission to make cancellation possible during planning. Query IDs cannot be reused while in history. API planning failures return 400; failures after headers are sent are NDJSON error records. Consumers must require `type=complete` before accepting a result. Partial rows must be discarded on error/disconnection. Row values are strings/null and schema records carry Arrow type names. The API is streaming and does not persist result sets or expose public result URLs.

Configuration is immutable during a server run. Change configuration and restart to manage sources; dynamic source CRUD and distributed durable query history are not implemented. A single token authorizes the configured service identity. Frontend SQL is limited to one SELECT/EXPLAIN SELECT; arbitrary DDL, DML, COPY, functions loading files, and session mutation are not exposed.

Diagnostics include `phases` (`catalog_ms`, `logical_plan_ms`, `optimize_ms`,
`physical_plan_ms`, `execution_ms`, `cleanup_ms`) and `file_cache_resident_bytes`.
Execution time includes consumer backpressure. Source counters include
`read_ahead_hits`, `read_ahead_bytes`, `cache_hits`, `cache_misses`, `cache_bytes`
and `cache_evictions`; origin `response_bytes` excludes cached bytes. Memory
fields are process-wide snapshots, not per-query peak RSS. See the
[performance contracts](PERFORMANCE.md) for their limits.

The CLI offers `query`, `serve`, and `check-config`; see `neura --help`. The Rust library exposes `Config`, `Engine::query`, `Engine::collect`, cancellation, and diagnostics. CompanyBrain can later call REST or embed the crate through a separate adapter; Neura does not import CompanyBrain code.

## Optional PostgreSQL wire and Cube

The default binary does not compile the wire endpoint and opens only HTTP. PostgreSQL **source** connectivity is independent and always available. Wire is needed only to use the existing Cube PostgreSQL driver / PostgreSQL clients; it adds no query-engine or source-access capability.

Build `cargo build --release --features postgres-wire`, then add `pg_addr="127.0.0.1:5433"` under `[server]` and supply `NEURA_PG_PASSWORD` (or its `_FILE` secret). Merely enabling the build feature does not start a listener without `pg_addr`. Docker users can opt in with `deploy/compose.cube.yml`; the default image/compose file exposes only 8080. The low-disk alternative is `NEURA_CARGO_FEATURES=postgres-wire NEURA_IMAGE=neura:0.1.0-cube scripts/build-low-disk.sh`. `scripts/wire-integration.sh` exercises the optional build against the real fixtures. The default REST build does not support Cube's PostgreSQL driver directly.

Authentication is SCRAM-SHA-256. Simple queries and extended Parse/Bind/Describe/Execute are implemented using pgwire. Parameters are substituted into a parsed SQL AST as values, never interpolated SQL fragments. Basic binary scalars and text results are supported. Binary decimal/nested result formats are rejected; node-postgres and the tested Cube driver request text. Supported OIDs cover primitive strings, booleans, integers, floats, dates, timestamps and numeric values.

Protocol cancellation is supported. Sessions understand UTC timezone setup and statement_timeout (capped by the server deadline). Other session changes, SQL transactions and PostgreSQL-specific administrative features are not supported. `pg_type`/`pg_class` advertise no user-defined types; this is not a complete PostgreSQL catalog emulation. `information_schema` exposes configured provider schemas. Do not use psql administrative backslash commands as a compatibility test.

Example Cube configuration:

```js
const { PostgresDriver } = require('@cubejs-backend/postgres-driver');
module.exports = {
  dbType: 'postgres',
  driverFactory: () => new PostgresDriver({
    host: process.env.NEURA_HOST,
    port: 5433,
    user: 'neura',
    password: process.env.NEURA_PG_PASSWORD,
    database: 'neura',
    storeTimezone: 'UTC'
  })
};
```

Use fully qualified `source.table` references in Cube models. Keep Cube pre-aggregations in an external store; Neura rejects CREATE/INSERT used for internal pre-aggregation tables. Cube's driver version 1.6.57 is exercised by `scripts/wire-test.cjs` for connection checks, parameters and a federated aggregation. This does not certify every Cube SQL feature, schema introspection mode, timezone, or version.
