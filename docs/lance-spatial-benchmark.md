# Lance R-tree point queries

`LanceDataset.spatialRows()` reads official packed R-tree indexes and evaluates
exact `intersects`, `within`, and `contains` predicates on native GeoArrow XY
points. Query geometries can be Points, LineStrings, or Polygons, including holes.
A point's indexed envelope uniquely identifies its geometry, so reconstructing
that point for refinement is exact. Other indexed geometry shapes require their
own geometry decoder and are rejected.

```ts
const result = await dataset.spatialRows({
  snapshotId: dataset.snapshotId,
  index: "geometry_idx",
  query: {
    predicate: "within",
    geometry: {
      type: "Polygon",
      coordinates: [[[10, 10], [14, 10], [14, 14], [10, 14], [10, 10]]],
    },
  },
  select: ["serial", "geometry"],
  limits: { maxPagesVisited: 1024, maxCandidatesExamined: 100_000 },
});
```

`spatialIndexes()` discovers indexes. Results include snapshot-pinned row IDs,
projected rows, visited tree pages, examined leaf entries, and cumulative Lance
read statistics. Results follow packed index traversal order. Projected points
retain their Arrow struct representation (`{ x, y }`); null points remain null.
No CRS transformation is performed.

## Unchanged default-writer compatibility

The conversion in [issue #24](https://github.com/earonesty/lakeql/issues/24) works
without additional writer options. The checked-in fixture generator includes its
conversion function: GeoParquet WKB is wrapped with `ga.wkb()`, converted with
`ga.as_geoarrow(..., type=ga.point())`, and the obsolete schema-level GeoParquet
metadata is removed. Batches contain 100,000 rows. The official writer call is:

```py
ds = lance.write_dataset(reader, destination, mode="overwrite")
ds.create_scalar_index("geometry", "RTREE")
```

With pylance 8.0.0 this produces storage 2.1, V2 manifests, default compression,
and physical row addresses. LakeQL supports that output as well as the existing
storage 2.0/stable-row-ID contract. `dataset.storageVersion` reports the opened
format; `dataset.rowIdMode` is `"address"` or `"stable"`.

In address mode, a row ID contains the fragment ID in the upper 32 bits and the
physical row offset in the lower 32 bits. It is meaningful only for its pinned
snapshot. Store `snapshotId` with external IDs in either mode. Snapshot mismatch
is rejected before data reads. Default-address resolution uses fragment metadata
and does not scan or manufacture stable-ID columns.

The storage 2.1 reader projects primitive and struct leaves through mini-block,
full-zip, and all-null layouts. It supports flat values, inline/out-of-line
FastLanes bitpacking, RLE, variable binary/string values, FSST, and page
dictionaries used by the default writer. Definition levels preserve nullable
leaves and parent structs, including repeated child names under different
parents. Full-zip variable values use selected offset pairs rather than loading
the complete index or data column. Unsupported compression/layout variants,
list-based nested values, and newer storage versions fail explicitly.

R-tree version 0 is supported, including its physical-address interpretation on
default datasets. Newer index versions with a different row-ID domain are
rejected. Portable Roaring coverage must include every active fragment; append
or rewrite operations leaving unindexed fragments require rebuilding the index.
Deletion filtering retains the materializer's supported deletion encodings.
SQL table-provider integration remains separate from this dataset API.

## Bounded execution

Tree levels require O(log N) descriptors. Traversal loads intersecting branches
and candidate leaves. The generic numeric range operator supports contiguous
non-null primitive numeric spans. Storage 2.0 leases fixed-width spans directly;
storage 2.1 admits typed output vectors and fetches only the required mini-blocks.
Compressed pages are not expanded into JavaScript row objects or full columns.
Sparse payload projection decodes only requested values from those blocks.

Admission covers active traversal state, retained match IDs, numeric vectors,
mini-block planning/RLE workspaces, encoded ranges, and retained primitive/binary
payload values. Byte, request, memory, decoded-row, output-row, cancellation,
concurrency, and elapsed-time budgets are shared through payload materialization.
Page and candidate limits also bound traversal work for low-selectivity queries.
Budget failures never fall back to a scan or return truncated results.

These are modeled operation limits, not an allocator or whole-heap ceiling.
Metadata object graphs, runtime objects, geometry parsing, allocator behavior,
and caller-held results also require headroom. The benchmark separately applies
`--max-old-space-size=64`, which limits V8 old space, and reports heap/RSS deltas.
Those deltas are not peak-heap measurements.

## Fixtures and reproduction

The LFS fixture is `packages/lance/fixtures/spatial-default/`. Its source
GeoParquet and binary Lance objects are stored through Git LFS; its JSON ground
truth and hashes stay readable in Git. The main dataset contains 131,072 rows,
so the unchanged conversion crosses a 100,000-row input-batch boundary. It
includes null geometries, Unicode and nullable attributes, signed numbers, and
low-cardinality dictionary strings. Additional official default-writer datasets
exercise addresses across fragments, long strings, binary/boolean/uint64 values,
all-null columns, narrow numeric words, and nullable structs at every depth.

```sh
uv run --with pylance==8.0.0 --with geoarrow-pyarrow \
  python packages/lance/scripts/generate_spatial_default_fixture.py

uv run --with pylance==8.0.0 --with geoarrow-pyarrow \
  python packages/lance/scripts/generate_spatial_default_fixture.py \
    --output /tmp/lakeql-spatial-default-1m --rows 1048576 --width 1024
pnpm build
node --expose-gc --max-old-space-size=64 bench/lance-spatial.mjs /tmp/lakeql-spatial-default-1m
```

Ground truth comes from official pylance spatial predicates and row projections.
Each recorded object has a SHA-256 digest. Producer-generated object names and
transaction IDs can change on regeneration. The earlier 16,384-point storage 2.0
fixture remains a regression control, including rejection of an unindexed append.

Recorded on 2026-11-16, Node 24.18.1, local filesystem ranges, five trials per size,
selecting `serial` for a rectangle intersection:

| Default-writer points | Matches | Tree pages | Leaf entries examined | Bytes read | Requests | Peak accounted memory | First trial | Repeated trials |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 131,072 | 25 | 2 | 4,096 | 274,369 | 25 | 461,248 B | 145.28 ms | 23.71–37.53 ms |
| 1,048,576 | 25 | 2 | 4,096 | 237,838 | 25 | 463,812 B | 58.19 ms | 18.54–24.11 ms |

Both runs enforced 512 KiB operation memory/byte limits, 32 range requests,
10,000 decoded rows, and a 64 MiB V8 old-space limit. Every trial checked results
against pylance. Per-trial heap deltas were approximately 0.4–3.9 MB; RSS deltas
were approximately -1.2–5.7 MB. Different compression and clustering account for
byte differences between sizes. Local synthetic timings establish bounded work
and compatibility; they do not establish object-storage throughput or peak heap.
