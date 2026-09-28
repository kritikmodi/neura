#!/usr/bin/env python3
"""Create real v2 position/equality-delete Avro manifests and Parquet delete files.

PyIceberg's public append API supplies the data snapshot. Its Avro writers create
the delete manifests; the static metadata commit is assembled explicitly because
PyIceberg's delete API performs copy-on-write instead of emitting equality deletes.
"""

import json, pathlib, time
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestWriterV2,
    write_manifest_list,
)
from pyiceberg.typedef import Record

root = pathlib.Path(__file__).resolve().parents[1]
path = root / "data/iceberg-deletes"
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
t.append(pa.table({"id": [1, 2, 3, 4], "amount": [100, 200, 300, 400]}))
old = t.current_snapshot()
data_path = next(iter(t.scan().plan_files())).file.file_path
metadata = json.loads(
    pathlib.Path(t.metadata_location.removeprefix("file://")).read_text()
)
snapshot_id = old.snapshot_id + 1
sequence = old.sequence_number + 1


class DeleteWriter(ManifestWriterV2):
    def content(self):
        return ManifestContent.DELETES

    @property
    def _meta(self):
        return {**super()._meta, "content": "deletes"}


pos = path / "position.parquet"
schema = pa.schema(
    [
        pa.field(
            "file_path",
            pa.string(),
            nullable=False,
            metadata={b"PARQUET:field_id": b"2147483546"},
        ),
        pa.field(
            "pos",
            pa.int64(),
            nullable=False,
            metadata={b"PARQUET:field_id": b"2147483545"},
        ),
    ]
)
pq.write_table(
    pa.Table.from_arrays(
        [pa.array([data_path]), pa.array([1], type=pa.int64())], schema=schema
    ),
    pos,
)
eq = path / "equality.parquet"
eq_schema = pa.schema(
    [pa.field("id", pa.int64(), metadata={b"PARQUET:field_id": b"1"})]
)
pq.write_table(
    pa.Table.from_arrays([pa.array([3], type=pa.int64())], schema=eq_schema), eq
)
manifest_path = (path / "delete-manifest.avro").as_uri()
with DeleteWriter(
    t.spec(), t.schema(), t.io.new_output(manifest_path), snapshot_id, "null"
) as writer:
    for file, content, equality_ids in [
        (pos, DataFileContent.POSITION_DELETES, None),
        (eq, DataFileContent.EQUALITY_DELETES, [1]),
    ]:
        data = DataFile.from_args(
            content=content,
            file_path=file.as_uri(),
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=1,
            file_size_in_bytes=file.stat().st_size,
            column_sizes=None,
            value_counts=None,
            null_value_counts=None,
            nan_value_counts=None,
            lower_bounds=None,
            upper_bounds=None,
            key_metadata=None,
            split_offsets=None,
            equality_ids=equality_ids,
            sort_order_id=None,
        )
        writer.add_entry(
            ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=snapshot_id,
                sequence_number=sequence,
                file_sequence_number=sequence,
                data_file=data,
            )
        )
    deletes = writer.to_manifest_file()
manifest_list = (path / "snapshot.avro").as_uri()
with write_manifest_list(
    2, t.io.new_output(manifest_list), snapshot_id, old.snapshot_id, sequence, "null"
) as writer:
    writer.add_manifests([*old.manifests(t.io), deletes])
timestamp = int(time.time() * 1000)
metadata["snapshots"].append(
    {
        "snapshot-id": snapshot_id,
        "parent-snapshot-id": old.snapshot_id,
        "sequence-number": sequence,
        "timestamp-ms": timestamp,
        "manifest-list": manifest_list,
        "summary": {"operation": "delete"},
        "schema-id": t.schema().schema_id,
    }
)
metadata.update(
    {
        "current-snapshot-id": snapshot_id,
        "last-sequence-number": sequence,
        "last-updated-ms": timestamp,
    }
)
metadata["refs"]["main"]["snapshot-id"] = snapshot_id
metadata.setdefault("snapshot-log", []).append(
    {"timestamp-ms": timestamp, "snapshot-id": snapshot_id}
)
out = path / "with-deletes.metadata.json"
out.write_text(json.dumps(metadata))
(root / "data/iceberg-deletes.toml").write_text(f"""[[sources]]
name="ice"
kind="iceberg"
[[sources.tables]]
name="old"
metadata_location={json.dumps(out.as_uri())}
snapshot_id={old.snapshot_id}
[[sources.tables]]
name="current"
metadata_location={json.dumps(out.as_uri())}
""")
print(
    "Created real v2 position/equality delete files: current IDs [1,4], previous [1,2,3,4]."
)
