# Limitations and release gates

This is an initial working engine, not a production equivalence claim for PostgreSQL, ClickHouse, Databricks, Snowflake, BigQuery, Spark or QE.

* **Cloud acceptance is blocked.** No Databricks, Snowflake, BigQuery or AWS credentials/catalog/table configuration were available. Their implementations compile and call real APIs, but cloud authorization, pagination, remote cancellation, token expiry, time travel and types remain unverified against live accounts. The ignored cloud smoke test must not be reported as passed.
* **Governance scope is service identity.** There is no end-user credential delegation, multi-tenant policy engine, per-user source allowlist or identity exchange. Run a separate instance for each trust domain. Only direct IAM-authorized raw S3 datasets are allowed. Glue explicitly rejects tables marked Lake Formation registered, and configuration rejects Lake Formation governance. Vended credentials/row-filtered lake access are unimplemented.
* **Snapshot scope is per source/table.** PostgreSQL uses one read-only repeatable-read transaction per query/source. Parquet uses pinned file manifests/ETags. Iceberg metadata is frozen and explicit snapshots are supported. Cloud time travel must be configured. There is no atomic cross-source snapshot or automatic shared clock. Expired snapshots/files fail. Local files must not be overwritten during a scan; immutable paths are the safe deployment contract.
* **ClickHouse consistency and types are explicit.** `consistency="statement"` opts into native source semantics, not a transaction shared with other systems. Multiple remote statements per configured ClickHouse source in a single Neura query are rejected. Views/Distributed tables retain native source guarantees; no `FINAL`, snapshot, or deduplication is silently added. Arrow IPC frames and converted batches have a size cap; compressed IPC and dictionaries are refused. Nested, binary, decimal256 and lossy conversions require a source view. Complete dialect/function equivalence is unproven. Local cancellation passed; load-balanced/cloud cancellation remains untested. See [details](CLICKHOUSE.md).
* **Pushdown has dialect limits.** The federation library is alpha. Basic SQL projection/filter/aggregation/subplan pushdown is tested on PostgreSQL and ClickHouse. Cross-source subplans can retain unused remote columns and apply the final projection locally. There is no exhaustive cross-dialect semantic proof for collation, decimal arithmetic/rounding, overflow, NaN, timezone arithmetic or functions. Unsupported operations fail; arbitrary remote functions and remote SQL passthrough are not exposed. Use tested expressions and explicit source views/types.
* **Planning is statistics based, not a full federated cost model.** DataFusion uses Parquet statistics and PostgreSQL estimates. ClickHouse and cloud estimates are unknown. There is no network-price model, dynamic semijoin shipping, adaptive cross-source repartitioning, distributed execution, or automatic source parallelism. PostgreSQL scans sharing a snapshot serialize on one source connection.
* **Memory has multiple boundaries.** The shared DataFusion pool covers tracked operator memory. JSON pages, PostgreSQL batches, ClickHouse IPC/cast buffers, reader metadata and some Iceberg delete structures sit outside it. Pages/output/concurrency have separate bounds; use the Docker/cgroup RSS limit. Some operators cannot spill and return resource errors. Sorts also need merge headroom (`sort_merge_reservation_mb`); wide-key sorts at small budgets can exhaust memory even with spilling enabled. Output accounting conservatively sums Arrow batch buffer sizes, including shared buffers, and can reject a result before its serialized payload reaches the nominal limit. Spill quotas are per query; the deployment must impose aggregate disk limits. The sample tmpfs spill volume counts against RSS.
* **Warehouse output is intentionally bounded.** Databricks uses INLINE JSON, limited to 25 MiB by the vendor, and rejects truncated results. Large Arrow/external-link results are not implemented. Snowflake pages have vendor-chosen sizes; a page over the configured cap fails. BigQuery uses REST result pages rather than the Storage Read API. No retry of ambiguous statement submissions is performed. A request lost before its job handle arrives can leave remote work running; server-side timeouts and billing limits remain necessary.
* **Type support is a subset.** Remote configured columns support booleans, signed integers, float32/64, UTF-8, dates, microsecond timestamps and decimal128 up to 38 digits. Nested/binary/interval/geospatial/decimal256 vendor decoding is not implemented. Precision-loss decimal encodings fail. PostgreSQL arrays, JSON objects and user-defined types need explicit source views. Raw Parquet/Iceberg support is wider internally, but PG output rejects unimplemented encodings.
* **Iceberg acceptance is incomplete.** Tests cover real v2 snapshots, append, schema rename, copy-on-write delete rewrite, and a snapshot containing both position and equality delete files. Neura's multi-file delete/sequence/partition-evolution matrix is not yet certified. These cases do not prove every delete-file mode. Glue support covers Iceberg tables, not partitioned Hive Parquet/CSV tables, Glue views, S3 Tables or Iceberg REST catalogs.
* **Metrics are partial.** Response bytes are source-specific payload measures, not uniform TCP/egress billing counters. Iceberg FileIO bytes are null. BigQuery processed bytes are vendor-reported when present; Snowflake/Databricks scanned bytes are not reported. Timings may include backpressure. Pool reserved bytes are a snapshot, not RSS peak. Benchmark cgroup measurements are separate.
* **API/PG compatibility is bounded.** One static configuration, in-memory bounded diagnostics, shared authentication identity, no transaction protocol, no broad PostgreSQL catalog emulation, no binary numeric encoding, and no native listener TLS. Put the service behind a private TLS boundary. HTTP planning failures currently share status 400, including concurrency admission failures.
* **Performance results are workload-specific.** The benchmark fixture is synthetic, single-node and local S3-compatible storage. Adaptive bounded read-ahead reduces successive small-range requests, but sparse accesses can still issue many requests. The optional small-object cache performs live conditional GET authorization on hits and may overread eligible files on cold selective queries. It does not apply to Iceberg/Glue; see [controls and contracts](PERFORMANCE.md). Row-level late materialization is opt-in because it can increase request counts on low-selectivity scans. Process-cold runs retain source/OS caches; true storage-cold runs were not performed. It does not establish cloud egress economics, distributed scaling, production latency, or universal superiority. Cgroup memory and the separately reported idle process-RSS snapshot have different accounting, and neither is a per-query unique-memory peak.

Before a production release: complete cloud tests with restricted principals; validate source schema/type contracts; certify Iceberg delete/partition-evolution cases; add delegated identity and TLS deployment tests; load-test hard memory/disk bounds and slow/disconnected clients; broaden dialect correctness workloads; and independently reproduce benchmark results at representative scale.

The [Spark/DuckDB comparison](benchmark-results/2026-09-28-spark-duckdb.md) reinforces
these performance limits: Neura is competitive on the measured local aggregation,
but the pre-optimization engine made excessive small-row-group S3 requests and had
no reusable file cache. The fixture fits easily in the
cache budget. Lower observed Neura RSS in these cases is not a universal memory
claim: measurements include different service runtimes, sample at roughly 50 ms,
and the recorded process high-water marks can exceed sampled peaks. Spark 3.5.6
was used, not the latest Spark release. The S3-compatible source itself reached
its CPU cap in an observed throughput interval. Static-fixture permission checks
do not establish equivalent cache invalidation or concurrent-write snapshots.

The [subsequent optimizations](benchmark-results/2026-09-28-optimizations.md)
reduced small-row-group request overhead and improved concurrent throughput.
They did **not** establish lower peak memory: default local aggregation high-water
RSS increased from 208 to 237 MiB despite lower sampled RSS. Admission-aware
partitions are a heuristic; running plans keep their original width, and waiting
queries count toward the selection. It is not a global CPU or memory scheduler.
The optional cache reduced body bytes but did not consistently improve latency.
Read-ahead increased origin bytes on the measured full join while reducing
requests; cloud request/egress cost remains unmeasured.

In the [native Hetzner comparison against QE](benchmark-results/2026-09-28-hetzner-qe.md),
the optimized Neura engine remained slower on all six S3 workloads, despite fewer
range requests. It had lower warm PostgreSQL/local-control latency and lower
recorded high-water RSS in the successful comparisons. QE's memory-guard
rejections and pre305's concurrent local-control OOM are workload-specific; they
do not establish a general correctness or memory ranking. The benchmark's first
S3 fixture OOM invalidated that attempt; the final run used a 1 GiB S3 cap and
512 MiB Go soft memory limit shared identically by all engines. Full evidence is retained.
