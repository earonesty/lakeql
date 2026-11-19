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

## Network latency regression contract

The local-file bounds above did not detect serialized remote reads. A page count
is a tree-work metric, not an HTTP request count, and neither alone bounds latency.
`spatial-performance.test.ts` now runs the official storage 2.1 fixture through
`tests/support/network-waves.ts`. This reusable store completes all pending ranges
in one event-loop turn; dependent reads require another wave. It measures work
and dependency depth without wall-clock thresholds, timers, or a public bucket in CI.
The tests run with the ordinary `pnpm test` / `pnpm check` gate.

The selective rectangle returns the official 25 rows and must satisfy all of:

| Metric | Ceiling / required behavior |
| --- | --- |
| Logical tree pages / leaf candidates | 2 / 4,096 |
| Store range reads, excluding dataset open | 32 |
| Object-size probes after open | 0 |
| Cumulative reported range reads, including open | 34 |
| Physical column pages / decoded rows | 10 / 4,153 |
| Requested bytes / peak accounted operation memory | each below 512 KiB |
| Network waves at concurrency 1 / 2 / 8 / 32 | 32 / 20 / 10 / 9 |
| Index and payload overlap | both exceed one when concurrency exceeds one |
| Active range reads | never exceed the configured concurrency |
| Repeated and nearby viewports | no index reads with only the decoded cache |

A wider rectangle returns 955 points, visits six tree pages, examines 20,480 leaf
entries, and must stay within 242 store reads, 35 total waves, seven index waves,
and 3 MiB accounted memory at concurrency eight. It checks the full row IDs and
projected rows against concurrency one and independently checks grid coordinates.
Current wave counts are 242 serial versus 34 concurrent. Output order remains
packed traversal order even when page reads complete out of order.

Tests also cover disabled/evicted caches, storage 2.0 page reuse, and concurrent
request/byte/memory/decoded-row/output budget failures. Failed queries must drain
all started reads and fetch no payload. A transport-failure test verifies that
admitted buffers remain charged until the other in-flight reads finish. Exact
one-byte admission boundaries use a single read worker, because I/O completion
order can legitimately change the peak live memory of parallel execution.

Running this new spatial performance suite against unmodified main (`5f60155`)
fails six of its original seven cases: it performs 37 rather than 32 selective
reads, 267 rather than 242 wide-query reads, and refetches the tree on repeats.
The limits are reviewable contracts, not automatically updated snapshots. Separate
mutations also confirm that serializing only sibling traversal fails the index-wave
ceiling (8 > 7), and serializing only numeric page reads fails the selective wave
ceilings (21 > 10 at concurrency eight; 20 > 9 at concurrency 32).

The reader now schedules independent structural columns/pages concurrently and
walks R-tree frontiers in bounded waves. All network ranges share an operation's
read semaphore (default four workers). Wave width uses remaining modeled memory
as a planning estimate; every actual modeled allocation still requires admission
and can fail the budget. Mini-block directories, including pending reads, are
shared within an operation and released at operation cleanup. Completed numeric
spans and inspected R-tree metadata use the bounded snapshot-scoped decoded
index cache. Cached vectors are admitted while in use; raw payload caching remains
an independent `cachedObjectStore` choice. Pending decoded loads are not shared
between operations, preserving independent cancellation and budget ownership.

## Public USGS viewport benchmark

`bench/lance-spatial-remote.mjs` runs the workload from
[C-Loftus/cloud-native-geo-lance](https://github.com/C-Loftus/cloud-native-geo-lance)
against its public GCS dataset. It tests concurrency eight and 32, with/without the
demo's 256 MiB raw byte cache, using a 64 MiB decoded cache. There is no background
warm-up and no range gap coalescing. It measures full range completion rather
than only response-header arrival, checks expected row counts, and emits hashes
of row IDs plus projected rows. It records actual transport requests separately
from cumulative reader stats, which include open work and byte-cache hits.

```sh
pnpm build
node --expose-gc bench/lance-spatial-remote.mjs
# Run the identical workload with a separately built baseline reader:
LANCE_MODULE=file:///absolute/baseline/dist/index.js \
  node --expose-gc bench/lance-spatial-remote.mjs
```

The dataset snapshot in the recorded run is version 5 with manifest SHA-256
`bc1b0ac1408990caab70d24679ddb54f379f5b986134e15d688bcb931366cab6`.
The viewport is `[-77.29, 38.76, -76.81, 39.04]`; the nearby pan shifts longitude
by 0.1 degrees. The projection is `site_no`, `feature_name`, `mainstem_id`, `lon`,
and `lat`. Both revisions return 1,117 viewport rows and 693 pan rows with matching
full-result hashes, five visited tree pages, and 13,258 candidates.

The final run on 2026-11-19 (milliseconds rounded):

| Concurrency / raw cache | Phase | Before ms | After ms | Store requests before → after | Peak active ranges before → after |
| --- | --- | ---: | ---: | --- | --- |
| 8 / none | cold | 9,715 | 2,890 | 67 → 47 | 2 → 8 |
| 8 / none | repeat | 9,498 | 699 | 67 → 16 | 2 → 8 |
| 8 / none | nearby | 9,697 | 791 | 68 → 17 | 3 → 8 |
| 32 / 256 MiB | cold | 6,254 | 1,853 | 47 → 47 | 2 → 20 |
| 32 / 256 MiB | repeat | 77 | 80 | 0 → 0 | 0 → 0 |
| 32 / 256 MiB | nearby | 784 | 195 | 6 → 6 | 3 → 6 |

Cold operation memory rose from 512,128 to 1,245,188 accounted bytes with the
benchmark's generous memory allowance. The selective local fixture retains its
512 KiB ceiling; the wider concurrency fixture retains a 3 MiB ceiling. This
trades bounded live buffers for fewer serialized network dependencies, without
increasing cold bytes fetched (829,490 → 820,434 without a raw cache).

Recorded before/after data and environment details are in
`benchmarks/lance-spatial-network-2026-11-19.json`. These are individual Node/GCS
observations, not browser measurements or CI latency thresholds. The hosted demo
still runs its published dependency until its owner updates it. Remote latency,
CDN state, and runtime allocation timing vary. Concurrent cold reads use more
operation memory than serial reads; the artifact reports that tradeoff, separately
from retained cache capacity and heap/RSS deltas. Heap/RSS deltas are not peaks.
