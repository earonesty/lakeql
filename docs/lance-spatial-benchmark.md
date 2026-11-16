# Lance R-tree point queries

`LanceDataset.spatialRows()` reads the official packed R-tree and evaluates exact
`intersects`, `within`, or `contains` predicates on native GeoArrow XY points.
The query geometry can be a Point, LineString, or Polygon, including polygon holes.
The point envelope stored in the index uniquely identifies the indexed geometry;
there is no approximation in reconstructing that point for exact refinement.
Other indexed geometry shapes require their own geometry decoder and are rejected.

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

`spatialIndexes()` discovers indexes. Results include stable row IDs, projected
rows, pages visited, leaf entries examined, and the usual cumulative Lance read
statistics. Results follow packed index traversal order. Projected native point
columns retain their Arrow struct representation (`{ x, y }`); ordinary structs
are also reconstructed recursively. No CRS transformation is performed.

## Writer compatibility

This path uses LakeQL's existing storage 2.0, stable-row-ID, V2-manifest contract.
With `pylance==8.0.0`, supply these options explicitly:

```py
ds = lance.write_dataset(
    reader, destination,
    data_storage_version="2.0",
    enable_stable_row_ids=True,
    enable_v2_manifest_paths=True,
)
ds.create_scalar_index("geometry", "RTREE")
```

The default pylance 8 writer uses storage 2.1, which the LakeQL reader does not
support. This change does not add storage 2.1 decoding or SQL table-provider
integration. R-tree index version 0 stores stable IDs and is supported; newer
physical-address index versions are rejected. The index's portable Roaring
fragment coverage must include every active fragment. Appending or rewriting
unindexed fragments fails explicitly; rebuild the index to restore coverage.
Deletion filtering uses the existing snapshot-aware materializer and its supported
deletion encodings.

## Bounded execution

The packed tree layout is calculated with O(log N) level descriptors. Traversal
loads only intersecting branches and their candidate leaves. The generic numeric
range operator leases fixed-width column spans directly, without expanding an
entire page into JavaScript row objects. Its explicit capability is contiguous
ranges of non-null numeric primitive columns, with independent physical page
boundaries per column. Other projections use the existing cell materializer.

Active traversal state is bounded by page fanout and tree depth. Child IDs and
transient refinement work receive memory admission before page reads. Matching
IDs stay charged through payload materialization. Byte, request, memory,
decoded-row, output-row, cancellation, concurrency, and elapsed-time budgets are
shared across traversal and payload reads. The additional page/candidate limits
bound traversal work even for low-selectivity queries. Budget failures never
fall back to a dataset scan or return truncated results.

Accounted memory covers admitted index state and encoded/decoded buffers; it is
not a hard limit on total JavaScript heap. Runtime objects, geometry parsing,
allocator behavior, and retained dataset metadata also consume heap. The benchmark
therefore runs under a separate Node heap limit and reports process-memory deltas.

## Reproduce

```sh
uv run --with pylance==8.0.0 --with geoarrow-pyarrow \
  python packages/lance/scripts/generate_spatial_fixture.py

uv run --with pylance==8.0.0 --with geoarrow-pyarrow \
  python packages/lance/scripts/generate_spatial_fixture.py \
    --output /tmp/lakeql-spatial-1m.lance --rows 1048576 --width 1024
pnpm build
node --expose-gc --max-old-space-size=64 bench/lance-spatial.mjs /tmp/lakeql-spatial-1m.lance
```

The checked-in 16,384-point fixture has four data fragments, four R-tree leaves,
and a root. It records official pylance predicate results, producer/storage
versions, and file digests. A later appended fragment verifies rejection of
incomplete indexes while version 2 preserves the indexed snapshot.

Recorded on 2026-11-15, Node 24.18.1, local filesystem ranges, five trials per size:

| Dataset points | Matches | Tree pages | Leaf entries examined | Bytes read | Requests | Peak accounted memory | First trial | Repeated trials |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16,384 | 25 | 2 | 4,096 | 264,716 | 14 | 427,200 B | 69.44 ms | 9.76–15.53 ms |
| 1,048,576 | 25 | 2 | 4,096 | 336,570 | 15 | 443,328 B | 57.66 ms | 9.05–19.57 ms |

Both runs enforced 512 KiB operation memory/byte limits, 32 range requests,
10,000 decoded rows, and a 64 MiB Node heap limit. Every trial checked results
against pylance. Observed per-trial heap deltas were 0.3–3.9 MB; RSS deltas were
approximately -0.8–5.0 MB. These deltas include runtime allocation and GC noise;
they are not peak-heap measurements. These synthetic local-file timings isolate
traversal and projection costs; object-storage latency and irregular spatial
clustering require separate measurements.
