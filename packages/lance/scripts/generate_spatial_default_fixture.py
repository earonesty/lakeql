"""Reproduce issue #24's GeoParquet-to-Lance conversion with unchanged writer defaults."""
from __future__ import annotations
import argparse
import hashlib
import json
import shutil
import struct
from pathlib import Path

import geoarrow.pyarrow as ga
import lance
import pyarrow as pa
import pyarrow.parquet as pq

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[1] / "fixtures" / "spatial-default")
parser.add_argument("--rows", type=int, default=131072)
parser.add_argument("--width", type=int, default=512)
args = parser.parse_args()
root = args.output
if root.exists():
    shutil.rmtree(root)
root.mkdir(parents=True)
N = args.rows
source = root / "source.parquet"
destination = root / "dataset.lance"
geometry = [None if i in {0, N-1} else struct.pack("<BIdd", 1, 1, float(i % args.width), float(i // args.width)) for i in range(N)]
table = pa.table({"serial": range(N), "label": [f"point-{i}" for i in range(N)],
                  "note": [None if i % 17 == 0 else f"雪🙂 note-{i}" for i in range(N)],
                  "score": [i - N // 2 for i in range(N)],
                  "category": [None if i % 7 == 0 else ("north" if i % 2 == 0 else "south") for i in range(N)],
                  "geometry": pa.array(geometry, type=pa.binary())})
geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": None}}}
pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), source, row_group_size=100000)

# This is the conversion in issue #24, including batch size and all default
# storage, manifest, row-ID and compression settings of the official writer.
BATCH_SIZE = 100_000

def convert(src: str, dst: str) -> None:
    def to_geoarrow(batch: pa.RecordBatch) -> pa.RecordBatch:
        wkb = ga.wkb().wrap_array(batch.column("geometry"))
        points = ga.as_geoarrow(wkb, type=ga.point())
        idx = batch.schema.get_field_index("geometry")
        return batch.set_column(idx, "geometry", points).replace_schema_metadata(None)

    batches = (to_geoarrow(b) for b in pq.ParquetFile(src).iter_batches(batch_size=BATCH_SIZE))
    first = next(batches)

    def all_batches():
        yield first
        yield from batches

    reader = pa.RecordBatchReader.from_batches(first.schema, all_batches())
    ds = lance.write_dataset(reader, dst, mode="overwrite")
    ds.create_scalar_index("geometry", "RTREE")

convert(str(source), str(destination))
ds = lance.dataset(destination)
queries = {
    "intersects": "ST_Intersects(geometry, ST_GeomFromText('POLYGON ((10 10, 14 10, 14 14, 10 14, 10 10))'))",
    "within": "ST_Within(geometry, ST_GeomFromText('POLYGON ((10 10, 14 10, 14 14, 10 14, 10 10))'))",
    "hole": "ST_Intersects(geometry, ST_GeomFromText('POLYGON ((10 10, 14 10, 14 14, 10 14, 10 10), (11 11, 11 13, 13 13, 13 11, 11 11))'))",
}
expected = {"producer": {"package": "pylance", "version": lance.__version__, "pyarrow": pa.__version__},
            "datasetPath": "dataset.lance", "storageVersion": ds.data_storage_version,
            "datasetVersion": ds.version, "rowCount": N, "batchSize": BATCH_SIZE,
            "queries": {}, "take": ds.take([0, 10*args.width+10, N-1]).to_pylist()}
for name, expression in queries.items():
    matches = ds.scanner(columns=["serial", "label", "note", "score", "category"], filter=expression, with_row_id=True).to_table().to_pylist()
    expected["queries"][name] = sorted(matches, key=lambda row: row["serial"])
# Supplementary default-writer snapshots exercise physical row addresses across
# fragments and layouts that coexist with the same generic projection engine.
small_path = root / "fragmented.lance"
small = lance.write_dataset(ds.take([10*args.width+10, 10*args.width+11]), small_path)
small = lance.write_dataset(ds.take([11*args.width+10, 11*args.width+11]), small_path, mode="append")
small.create_scalar_index("geometry", "RTREE")
small = lance.dataset(small_path)
expected["fragmented"] = small.scanner(with_row_id=True).to_table().to_pylist()

layouts = pa.table({
    "id": [1,2,3,4],
    "text": ["abc"*1000, None, "雪🙂"*500, "short"],
    "value": [1.234, 4.32, None, -0.7],
    "none": pa.array([None]*4, type=pa.float64()),
    "flag": [True, False, None, True],
    "blob": pa.array([b"\x00\xff", None, b"", b"\xff\x01"], type=pa.binary()),
    "large": pa.array([2**63+1, 0, None, 2**64-1], type=pa.uint64()),
    "left": pa.array([{"x":1,"y":None},None,{"x":None,"y":3},{"x":4,"y":5}], type=pa.struct([("x",pa.int32()),("y",pa.int32())])),
    "right": pa.array([None,{"x":6,"y":7},{"x":8,"y":None},{"x":9,"y":10}], type=pa.struct([("x",pa.int32()),("y",pa.int32())])),
})
other = lance.write_dataset(layouts, root / "layouts.lance")
# Binary and uint64 values need portable JSON representations for checked-in truth.
expected["layouts"] = [{**row, "blob": None if row["blob"] is None else list(row["blob"]),
    "large": None if row["large"] is None else str(row["large"])} for row in other.to_table().to_pylist()]
deep_type = pa.int32()
for _ in range(6):
    deep_type = pa.struct([("v", deep_type)])

def deep_value(index):
    # Nulls at every depth require three-bit definition levels.
    value = index
    null_depth = index % 8
    for depth in range(6, -1, -1):
        if null_depth == depth:
            value = None
        if depth > 0:
            value = {"v": value}
    return value

numeric = pa.table({"tiny": pa.array([i%127 for i in range(2048)],type=pa.int8()),
                    "small": pa.array([i%2048 for i in range(2048)],type=pa.int16()),
                    "f32": pa.array([i*0.125 for i in range(2048)],type=pa.float32()),
                    "deep": pa.array([deep_value(i) for i in range(2048)],type=deep_type)})
nums = lance.write_dataset(numeric, root / "numeric.lance")
expected["numeric"] = nums.take(list(range(8))+[1023,1024,2047]).to_pylist()
expected["numericOffsets"] = list(range(8))+[1023,1024,2047]
expected["sha256"] = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in sorted(root.rglob("*")) if p.is_file()}
(root / "expected.json").write_text(json.dumps(expected, indent=2, sort_keys=True) + "\n")
