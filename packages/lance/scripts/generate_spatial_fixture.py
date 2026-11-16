"""Official-writer spatial compatibility fixture. Run with pylance==8.0.0, geoarrow-pyarrow."""
import argparse
import hashlib
import json
import shutil
from pathlib import Path

import geoarrow.pyarrow as ga
import lance
import pyarrow as pa

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[1] / "fixtures" / "spatial-rtree-v2.0.lance")
parser.add_argument("--rows", type=int, default=16384)
parser.add_argument("--width", type=int, default=128)
args = parser.parse_args()
ROOT = args.output
if ROOT.exists():
    shutil.rmtree(ROOT)
# A grid forces multiple R-tree leaves. Duplicate points and a polygon hole
# distinguish exact geometry tests from envelope-only filtering.
N = args.rows
x = [float(i % args.width) for i in range(N)]
y = [float(i // args.width) for i in range(N)]
table = pa.table({"serial": range(N), "label": [f"point-{i}" for i in range(N)],
                  "geometry": ga.make_point(pa.array(x), pa.array(y)),
                  "location": pa.StructArray.from_arrays([pa.array(x), pa.array(y)], names=["x", "y"])})
ds = lance.write_dataset(table, ROOT, data_storage_version="2.0",
                         enable_stable_row_ids=True, enable_v2_manifest_paths=True,
                         max_rows_per_file=4096)
ds.create_scalar_index("geometry", "RTREE")
queries = {
    "intersects": "ST_Intersects(geometry, ST_GeomFromText('POLYGON ((10 10, 14 10, 14 14, 10 14, 10 10))'))",
    "within": "ST_Within(geometry, ST_GeomFromText('POLYGON ((10 10, 14 10, 14 14, 10 14, 10 10))'))",
    "hole": "ST_Intersects(geometry, ST_GeomFromText('POLYGON ((10 10, 14 10, 14 14, 10 14, 10 10), (11 11, 11 13, 13 13, 13 11, 11 11))'))",
}
expected = {"producer": {"package": "pylance", "version": lance.__version__},
            "storageVersion": ds.data_storage_version, "datasetVersion": ds.version,
            "rowCount": N, "queries": {}}
for name, expr in queries.items():
    expected["queries"][name] = sorted(ds.to_table(columns=["serial"], filter=expr).column("serial").to_pylist())
# Keep the indexed snapshot, then append a fragment to verify incomplete-index rejection.
lance.write_dataset(table.slice(0, 1), ROOT, mode="append", data_storage_version="2.0")
expected["unindexedAppendVersion"] = 3
expected["sha256"] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in sorted(ROOT.rglob("*")) if p.is_file()}
(ROOT / "expected.json").write_text(json.dumps(expected, indent=2, sort_keys=True) + "\n")
