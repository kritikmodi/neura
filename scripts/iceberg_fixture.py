#!/usr/bin/env python3
"""Real Iceberg commits, snapshots, delete rewrite and field-ID schema evolution."""

import pathlib, json
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

root = pathlib.Path(__file__).resolve().parents[1]
path = root / "data" / "iceberg"
path.mkdir(parents=True, exist_ok=True)
cat = SqlCatalog(
    "test", uri=f"sqlite:///{path}/catalog.db", warehouse=f"file://{path}/warehouse"
)
cat.create_namespace_if_not_exists("test")
if cat.table_exists("test.events"):
    cat.drop_table("test.events")
t = cat.create_table(
    "test.events",
    schema=pa.schema([("id", pa.int64()), ("amount", pa.int64())]),
    properties={"format-version": "2"},
)
t.append(pa.table({"id": [1, 2, 3], "amount": [100, 200, 300]}))
first = t.current_snapshot().snapshot_id
t.append(pa.table({"id": [4], "amount": [400]}))
t.delete("id = 2")
with t.update_schema() as update:
    update.rename_column("amount", "total")
# A new data commit records the renamed schema in a snapshot.
t.append(pa.table({"id": [5], "total": [500]}))
metadata = t.metadata_location
(root / "data" / "iceberg.toml").write_text(f"""[[sources]]
name="ice"
kind="iceberg"
[[sources.tables]]
name="old"
metadata_location={json.dumps(metadata)}
snapshot_id={first}
[[sources.tables]]
name="current"
metadata_location={json.dumps(metadata)}
""")
print("Created actual Iceberg v2 snapshots and schema evolution fixture.")
