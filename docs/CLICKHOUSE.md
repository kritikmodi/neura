# ClickHouse connector

ClickHouse is a source in the default REST-only build. PostgreSQL remains a separate, always-enabled source connector. Neither needs Neura's optional PostgreSQL-wire listener.

Configure an HTTPS origin, a restricted username, a password environment/file reference, explicit columns and `database.table` references, as in [the example](../examples/clickhouse.toml). Secrets are sent as HTTP Basic authentication over verified TLS; redirects are disabled. `allow_http=true` is an explicit exception for private test networks. The connector never reads ClickHouse's backing files and never switches credentials to bypass policies.

## Source contract

`consistency="statement"` is required. ClickHouse's native consistency depends on the table engine and operation. Neura executes at most one remote statement per configured ClickHouse source in a query. A plan requiring another statement fails; use one source view/subplan or immutable materialized data if a query needs stronger guarantees. There is no time-travel pin, multi-statement transaction, distributed snapshot or atomic snapshot shared with PostgreSQL/S3. The connector does not silently add `FINAL` to Replacing/CollapsingMergeTree tables. Declare a source view when those semantics are required.

The SQL federation optimizer pushes supported projections, predicates, aggregates and subplans to ClickHouse. Standard outer-join NULL behavior is requested with `join_use_nulls=1`. ClickHouse statistics are currently unknown to Neura's cost model. Collation, overflow, date arithmetic and arbitrary functions are not certified equivalent across dialects. Use explicit source views for unsupported types/expressions.

Results use [ArrowStream](https://clickhouse.com/docs/interfaces/formats/ArrowStream). No full-result JSON or text conversion is used. `max_block_size` requests `limits.batch_rows`; the local stream yields slices of at most that many rows. `max_remote_page_mb` bounds each IPC frame and the unique backing buffers of each converted batch, not the total result size. Shared IPC allocations are counted once across columns. Oversized frames fail before body allocation. Compression and dictionary messages are rejected to avoid decompression expansion and accumulating dictionaries. Compatible Arrow arrays are reused; differing scalar types are checked by a strict cast and round trip to reject overflow/truncation. Decimal and timestamp precision must be exact. Nested/binary/decimal256 columns require source views.

One frame, its decoded batch, and temporary cast buffers may coexist. These buffers are outside DataFusion's operator pool and are proportional to the frame cap and active queries, not total result rows. Cgroup limits remain the process boundary. There is no claim that the configured operator memory limit equals process RSS. The connector does not prefetch a queue of decoded batches. HTTP response bytes, Arrow bytes, rows, requests and elapsed source time are measured; source storage-read bytes are unknown (null).

HTTP 200 is insufficient evidence of success. The IPC decoder requires a valid schema, complete messages, the explicit EOS marker and clean HTTP EOF. Truncation or an appended server exception fails the stream. REST consumers must also require the final completion record and discard partial results after errors.

## Restricted settings profile

Create a dedicated identity with SELECT only on approved tables/views and your source row policies. Do not grant broad privileges to resolve a permission failure. The connector requests execution limits and format settings; a stricter incompatible policy causes a query error. It never retries after relaxing source policy. A `readonly=1` profile must explicitly allow the execution settings it changes. The fixture demonstrates the required profile syntax for ClickHouse 25.8:

```sql
CREATE SETTINGS PROFILE neura_reader_profile SETTINGS
  readonly=1,
  max_block_size=4096 MAX 65536 CHANGEABLE_IN_READONLY,
  preferred_block_size_bytes=1048576 MAX 33554432 CHANGEABLE_IN_READONLY,
  max_execution_time=120 MAX 120 CHANGEABLE_IN_READONLY,
  max_memory_usage=268435456 MAX 536870912 CHANGEABLE_IN_READONLY,
  max_threads=2 MAX 4 CHANGEABLE_IN_READONLY,
  join_use_nulls=1 CONST,
  session_timezone='UTC' CONST,
  cancel_http_readonly_queries_on_client_close=1 CONST,
  output_format_arrow_low_cardinality_as_dictionary=0 CONST,
  output_format_arrow_string_as_string=1 CONST,
  output_format_arrow_compression_method='none' CONST,
  enable_http_compression=0 CONST
TO neura_reader;
```

Choose upper bounds for your deployment; align Neura's requested limits with them. `readonly` alone is not a substitute for restricted grants. No raw SQL passthrough or source table-function entrypoint is exposed.

Each remote statement has a fresh UUID. Cancellation/drop closes the response and sends `KILL QUERY WHERE query_id='<uuid>' ASYNC` using the same identity, with a five-second request deadline. It cannot cancel arbitrary users' work. Disconnect cancellation is also enabled. KILL delivery is best effort during network failures or load-balancer routing to a different node; retain server-side deadlines. No automatic SELECT retry changes the snapshot. See the official [HTTP](https://clickhouse.com/docs/interfaces/http), [query permissions](https://clickhouse.com/docs/operations/settings/permissions-for-queries), [KILL](https://clickhouse.com/docs/sql-reference/statements/kill), and [transaction guarantees](https://clickhouse.com/docs/guides/developer/transactional) references.

## Reproduce tests

```sh
scripts/integration.sh
# Or, after PostgreSQL/S3 fixtures already exist:
scripts/clickhouse-integration.sh
```

The pinned ClickHouse 25.8.4.13 Docker fixture uses a SELECT-only reader, a tenant row policy, a deliberately forbidden table, a typed view and generated stream/slow-query views. Tests exercise source filtering/aggregation/projection, exact decimal/date/timestamp/null handling, empty results, outer-join NULLs, ClickHouse × PostgreSQL × S3 results, rejection of split snapshots, streaming more than 20 MiB through a 1 MiB frame budget, and termination of an active remote query. A 2 MiB scalar is rejected under the 1 MiB frame cap, a request above the source memory-policy ceiling fails, and the source identity independently rejects INSERT. Unit tests cover bad/truncated/trailing IPC and conversion overflow/rounding. Managed ClickHouse Cloud, TLS-specific deployment behavior and distributed clusters remain untested.
