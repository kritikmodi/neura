# Performance controls and contracts

Neura retains DataFusion for local execution. These controls reduce avoidable
network and planning work; they do not establish a performance ranking against
Spark, DuckDB or other engines.

## Bounded S3 read-ahead

Manifest-backed S3 Parquet reads start with the requested ranges. A sequence of
nearby small forward reads activates a larger range, allowing subsequent row
groups to use bytes already fetched for the query. Isolated selective reads and
large ranges remain exact. Every origin read still uses the manifest's conditional
ETag. Wildcard and alternative ETag lists are rejected.

The default window is 256 KiB, with a 2 MiB cap shared by S3 sources within one
query. Buffers reserve the shared DataFusion pool, are evicted under pressure,
and are released on cancellation. If reservation fails, the reader falls back
to an exact range. Read-ahead can transfer unused columns or rows inside a window;
it is a bounded network-request/byte tradeoff, not a replacement for pruning.

## Optional reusable file cache

Caching is disabled by default. To enable it explicitly:

```toml
[limits]
memory_mb = 512
file_cache_mb = 32
file_cache_max_object_mb = 8
s3_read_ahead_kb = 256
s3_read_ahead_budget_mb = 2
```

The cache receives 32 MiB and the execution pool receives the remaining 480 MiB.
Read-ahead reserves from that 480 MiB pool. Payload accounting includes in-flight
fills and backing allocations retained by downstream slices after eviction.
Cache metadata is separately capped at 1,024 entries. Transport buffers, decoded
Arrow buffers and other allocations still require the container RSS boundary.
`s3_read_ahead_kb = 0` disables read-ahead; `file_cache_mb = 0` disables reuse.

Only small, explicitly pinned S3 objects qualify. A partial cold miss can fetch
the entire eligible object, so caching may increase bytes and latency for a
one-off selective query. Larger files stream normally. A busy fill or exhausted
cache budget falls back to the live read instead of queuing an unbounded fill.

The cache key includes source, endpoint, region, bucket, resolved credential and
session-token identity, object path, ETag and optional version. Credential values
are hashed and never included in diagnostics. Every cache hit must complete a
fresh conditional **GetObject** request and consume its response; HEAD alone
cannot establish permission to retrieve/decrypt the data. Denied, deleted or
changed objects fail closed. There is no cached authorization lease or SQL result
cache. Bytes already returned into an active query have ordinary read-buffer
semantics; revocation cannot retract bytes already fetched.

This cache does not apply to PostgreSQL, local Parquet, Iceberg or Glue. It does
not add Lake Formation access or bypass warehouse SQL governance.

## PostgreSQL reuse

Each configured PostgreSQL source has a connection pool capped by the query
concurrency limit. Every checkout resolves the current DSN secret and checks its
credential/TLS scope. Reused backends begin a **new read-only repeatable-read
transaction**, establish a new snapshot, and check live role flags. Superuser,
BYPASSRLS, NOLOGIN and changed session identity are rejected. Table and row
permissions are still evaluated by PostgreSQL.

After a fully consumed successful query, Neura rolls back the read transaction
and executes `DISCARD ALL` before returning the socket. Errors, cancellation,
abandoned streams and failed cleanup discard it. A cancellation watcher is
stopped and joined before reuse so an old query cannot cancel a later borrower.
Reuse is limited to a 60-second connection lifetime; idle connections older than
that are closed on checkout. Pooling follows PostgreSQL's authenticated-session
semantics: password rotation does not itself terminate a source backend.

Queries sharing one source snapshot continue to serialize on that source
connection. This change does not introduce independent snapshots for parallel
pieces of a single source query.

## Measurement

Source diagnostics separate origin response bytes, reusable-cache bytes,
cache hit/miss/eviction counts and read-ahead hits/extra bytes. Request counters
also include instrumented source scan/estimate operations; they are not exact
S3 billing counters. Engine phase timings separate setup, logical planning,
optimization, physical planning, execution and cleanup. Execution includes client
backpressure. Memory reservation and cache occupancy are process-wide snapshots,
not per-query peak RSS.

The reproducible suite uses identical logical data, engine resource limits,
correctness checks, separate process-cold/warm samples, concurrency tests and an
independent memory pass. See [benchmark instructions](../benchmarks/COMPARE.md)
and the raw result files linked from each report. True storage-cold behavior,
large changing datasets and cloud credentials remain separate validation work.
