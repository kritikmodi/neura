#!/usr/bin/env python3
"""Add benchmark-only files without overwriting the existing integration fixtures."""

import hashlib
import json
import pathlib
import tomllib

import boto3
import pyarrow.parquet as pq

ROOT = pathlib.Path(__file__).resolve().parents[1]
DEST = ROOT / "data/compare"
DEST.mkdir(parents=True, exist_ok=True)
config = tomllib.loads((ROOT / "data/test.toml").read_text())
lake = next(s for s in config["sources"] if s["name"] == "lake")
s3 = boto3.client(
    "s3", endpoint_url="http://127.0.0.1:59000", region_name="us-east-1",
    aws_access_key_id="neura-test", aws_secret_access_key="neura-test-storage-only",
)
manifest = {"rows": 0, "small": [], "large": []}
for i, original in enumerate(lake["tables"][0]["files"]):
    # Download the actual S3 bytes, rather than hashing a differently encoded local copy.
    obj = s3.get_object(Bucket="neura-fixture", Key=original["path"], IfMatch=original["etag"])
    body = obj["Body"].read()
    local = DEST / f"small-{i}.parquet"
    local.write_bytes(body)
    table = pq.read_table(local)
    manifest["rows"] += table.num_rows
    manifest["small"].append({**original, "bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(), "row_groups": pq.read_metadata(local).num_row_groups})
    large = DEST / f"large-{i}.parquet"
    pq.write_table(table, large, row_group_size=131072, compression="snappy")
    encoded = large.read_bytes()
    digest = hashlib.sha256(encoded).hexdigest()
    key = f"benchmark-large/{digest}/part-{i}.parquet"
    s3.put_object(Bucket="neura-fixture", Key=key, Body=encoded)
    manifest["large"].append({"path": key,
        "etag": s3.head_object(Bucket="neura-fixture", Key=key)["ETag"],
        "bytes": len(encoded), "sha256": digest, "row_groups": pq.read_metadata(large).num_row_groups})
assert manifest["rows"] == json.loads((ROOT / "data/expected.json").read_text())["rows"]
(DEST / "manifest.json").write_text(json.dumps(manifest, indent=2))

base = (ROOT / "data/test.toml").read_text().split("[[sources]]", 1)[1]
base = "[[sources]]" + base.replace("http://127.0.0.1:59000", "http://s3:8333")
header = '''[limits]
memory_mb=512
max_remote_page_mb=32
max_concurrent_queries=2
partitions=2
batch_rows=4096
query_timeout_secs=120
max_spill_mb=4096
spill_dir="/tmp/spill"
[server]
allow_insecure_network=true
http_addr="0.0.0.0:8080"
'''
coarse = '''
[[sources.tables]]
name="orders_coarse"
files=[''' + ",".join("{path=" + json.dumps(f["path"]) + ",etag=" + json.dumps(f["etag"]) + "}"
    for f in manifest["large"]) + "]\n"
local = '''
[[sources]]
name="local"
kind="parquet"
[[sources.tables]]
name="orders"
files=[{path="/fixture/large-0.parquet"},{path="/fixture/large-1.parquet"}]
'''
(DEST / "neura.toml").write_text(header + base + coarse + local)
print(json.dumps(manifest, indent=2))
