-- Disposable integration identities; no production credentials.
CREATE DATABASE IF NOT EXISTS neura;
CREATE TABLE IF NOT EXISTS neura.customers (id Int64, region String, tenant_id Int64) ENGINE=MergeTree ORDER BY id;
INSERT INTO neura.customers SELECT number+1, if((number+1)%2=0,'east','west'), if(number<100,1,2) FROM numbers(200) WHERE (SELECT count() FROM neura.customers)=0;
CREATE TABLE IF NOT EXISTS neura.forbidden (id Int64) ENGINE=MergeTree ORDER BY id;
CREATE VIEW IF NOT EXISTS neura.stream_rows AS SELECT toInt64(number) AS id, repeat('x',128) AS payload FROM numbers(200000);
CREATE VIEW IF NOT EXISTS neura.slow_rows AS SELECT toInt64(number) AS id, sleepEachRow(0.01) AS pause FROM numbers(100000);
CREATE VIEW IF NOT EXISTS neura.types AS SELECT toInt64(7) AS id, toDecimal128('12345678901234567890.12',2) AS amount, toDate('2026-01-02') AS day, toDateTime64('2026-01-02 03:04:05.123456',6,'UTC') AS at, CAST(NULL AS Nullable(Int64)) AS missing;
CREATE VIEW IF NOT EXISTS neura.oversized AS SELECT repeat('xxxx',524288) AS payload;
CREATE USER IF NOT EXISTS neura_reader IDENTIFIED WITH sha256_password BY 'neura-test-clickhouse-reader';
GRANT SELECT ON neura.customers TO neura_reader;
GRANT SELECT ON neura.stream_rows TO neura_reader;
GRANT SELECT ON neura.slow_rows TO neura_reader;
GRANT SELECT ON neura.types TO neura_reader;
GRANT SELECT ON neura.oversized TO neura_reader;
CREATE ROW POLICY IF NOT EXISTS tenant_one ON neura.customers FOR SELECT USING tenant_id=1 TO neura_reader;
CREATE SETTINGS PROFILE IF NOT EXISTS neura_reader_profile SETTINGS
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
