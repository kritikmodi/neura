#!/usr/bin/env python3
"""Deterministic fixtures. Only connects to Neura's localhost test services."""

import io, json, os, pathlib, time
import boto3
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)
N = int(os.environ.get("NEURA_FIXTURE_ROWS", "10000"))
orders = pa.table(
    {
        "order_id": pa.array(range(N), pa.int64()),
        "customer_id": pa.array([i % 200 + 1 for i in range(N)], pa.int64()),
        "amount": pa.array([i % 17 + 1 for i in range(N)], pa.int64()),
    }
)
s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:59000",
    region_name="us-east-1",
    aws_access_key_id="neura-test",
    aws_secret_access_key="neura-test-storage-only",
)
for attempt in range(60):
    try:
        s3.list_buckets()
        break
    except Exception:
        time.sleep(1)
else:
    raise RuntimeError("test S3 service unavailable")
try:
    s3.create_bucket(Bucket="neura-fixture")
except (s3.exceptions.BucketAlreadyOwnedByYou, s3.exceptions.BucketAlreadyExists):
    s3.head_bucket(Bucket="neura-fixture")
try:
    s3.create_bucket(Bucket="neura-private")
except (s3.exceptions.BucketAlreadyOwnedByYou, s3.exceptions.BucketAlreadyExists):
    s3.head_bucket(Bucket="neura-private")
private = io.BytesIO()
pq.write_table(pa.table({"secret": [42]}), private)
s3.put_object(Bucket="neura-private", Key="private.parquet", Body=private.getvalue())
(DATA / "private-etag.json").write_text(
    json.dumps(s3.head_object(Bucket="neura-private", Key="private.parquet")["ETag"])
)
files = []
for i in range(2):
    part = orders.slice(i * (N // 2), (N // 2 if i == 0 else N - N // 2))
    buf = io.BytesIO()
    pq.write_table(part, buf, row_group_size=1024, compression="snappy")
    key = f"orders/part-{i}.parquet"
    s3.put_object(Bucket="neura-fixture", Key=key, Body=buf.getvalue())
    files.append(
        {"path": key, "etag": s3.head_object(Bucket="neura-fixture", Key=key)["ETag"]}
    )
    pq.write_table(part, DATA / f"part-{i}.parquet", row_group_size=1024)
expected = {}
for i in range(N):
    c = i % 200 + 1
    if c <= 100:
        region = "east" if c % 2 == 0 else "west"
        expected[region] = expected.get(region, 0) + i % 17 + 1
(DATA / "expected.json").write_text(
    json.dumps(
        {"rows": N, "grouped": [[k, str(v)] for k, v in sorted(expected.items())]},
        indent=2,
    )
)
config = """[limits]
memory_mb=256
partitions=2
max_concurrent_queries=2
query_timeout_secs=60
spill_dir="/tmp/neura-test-spill"
[server]
http_addr="127.0.0.1:58080"
[[sources]]
name="pg"
kind="postgres"
dsn_env="NEURA_TEST_POSTGRES_DSN"
allow_insecure=true
[[sources.tables]]
name="customers"
remote="public.customers"
columns=[{name="id",data_type="int64",nullable=false},{name="region",data_type="utf8",nullable=false},{name="tenant_id",data_type="int64",nullable=false}]
[[sources.tables]]
name="forbidden"
remote="public.forbidden"
columns=[{name="secret",data_type="utf8"}]
[[sources]]
name="lake"
kind="s3"
bucket="neura-fixture"
region="us-east-1"
endpoint="http://127.0.0.1:59000"
allow_http=true
governance="iam_only"
[[sources.tables]]
name="orders"
"""
config += (
    "files=["
    + ",".join(
        "{path=" + json.dumps(f["path"]) + ",etag=" + json.dumps(f["etag"]) + "}"
        for f in files
    )
    + "]\n"
)
(DATA / "test.toml").write_text(config)
print(f"Seeded {N} Parquet rows; wrote data/test.toml and independent expected result.")
