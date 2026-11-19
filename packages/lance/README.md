# lakeql-lance

`lakeql-lance` performs bounded, projection-aware random reads from immutable
[Lance](https://lancedb.github.io/lance/) datasets in Cloudflare Workers, browsers, and Node.js.
It materializes snapshot-pinned row IDs from search indexes without scanning a key column,
and supports indexed scalar, vector, and spatial queries. Both stable IDs and the default
physical row addresses are supported.

```ts
import { openLanceDataset } from "lakeql-lance";

const dataset = await openLanceDataset({
  store,
  path: "catalog.lance",
  indexCacheBytes: 8 * 1024 * 1024,
  budget: {
    maxBytes: 8 * 1024 * 1024,
    maxRangeRequests: 128,
    maxMemoryBytes: 16 * 1024 * 1024,
    maxOutputRows: 32,
    maxConcurrentReads: 4,
    maxElapsedMs: 3_000,
  },
});

const result = await dataset.takeRows({
  snapshotId: dataset.snapshotId,
  rowIds: [42n, 7n, 42n],
  select: ["serial", "mark_text", "owner_name"],
});
```

Official Lance BTree indexes can produce those stable IDs directly without a key-column scan:

```ts
const indexes = await dataset.scalarIndexes();
const result = await dataset.lookupRows({
  snapshotId: dataset.snapshotId,
  index: indexes[0].name,
  values: [1005, 1031],
  select: ["mark_text", "owner_name"],
});
```

`lookupRows` performs exact equality lookup, preserves the caller's key order and duplicate keys,
returns every indexed duplicate in stable index order, and composes matches with the same
snapshot-safe projected materializer. It retains the BTree lookup state, loads each candidate
4,096-row leaf once, and binary-searches its sorted keys locally. BTree page lookup, stable-ID
retrieval, and result materialization share one cumulative byte, request, memory, decoded-row,
concurrency, cancellation, and elapsed-time budget.

Decoded BTree lookup state/leaves and R-tree metadata/numeric pages use a separate
snapshot-scoped LRU cache. `indexCacheBytes`
sets its byte ceiling and defaults to 8 MiB; set it to `0` to disable decoded-index caching. Supply
an explicitly sized `SharedMemoryCache` as `indexCache` to share immutable index state across
separately opened handles for the same snapshot. Cache population remains subject to the query
budgets, while hits consume neither range/byte budget nor decoded-row budget. Active cached
numeric vectors are charged against operation memory. Spatial traversal and structural
column/page reads execute concurrently under one operation-wide read limit; page directories
are shared within the operation. See [network performance contracts](../../docs/lance-spatial-benchmark.md#network-latency-regression-contract).

Ordered range retrieval uses the same index without decoding intervening key pages:

```ts
const result = await dataset.rangeRows({
  snapshotId: dataset.snapshotId,
  index: "serial_btree",
  range: {
    lower: 1000,
    lowerInclusive: true,
    upper: 2000,
    upperInclusive: false,
  },
  select: ["mark_text", "owner_name"],
});
```

Either bound may be omitted. Results remain in BTree order and output-row budgets are checked from
the binary-search bounds before stable IDs or projected data are fetched.

Official Lance IVF_FLAT indexes support bounded vector search without native bindings:

```ts
const dataset = await openLanceDataset({
  store,
  path: "catalog.lance",
  budget,
  vectorLimits: {
    maxDimension: 1_536,
    maxPartitionsSearched: 16,
    maxCandidatesScored: 50_000,
  },
});

const result = await dataset.nearest({
  snapshotId: dataset.snapshotId,
  index: "embedding_ivf_flat",
  vector: queryEmbedding,
  k: 20,
  nprobes: 8,
  select: ["title", "url"],
});
```

The reader selects IVF partitions from the official centroid tensor, reads only their auxiliary
vector and stable-ID rows, scores candidates in memory-bounded chunks, retains a deterministic
top-k, and materializes projected fields in distance order. L2, cosine, and Lance dot-distance
semantics are supported. `nprobes` is explicit: searching every partition is exact, while fewer
partitions trades recall for object-storage I/O.

Official R-tree indexes support exact spatial queries over native GeoArrow XY points:

```ts
const indexes = await dataset.spatialIndexes();
const result = await dataset.spatialRows({
  snapshotId: dataset.snapshotId,
  index: indexes[0].name,
  query: {
    predicate: "within", // also "intersects" and "contains"
    geometry: {
      type: "Polygon",
      coordinates: [[[10, 10], [14, 10], [14, 14], [10, 14], [10, 10]]],
    },
  },
  select: ["serial", "geometry"],
});
```

Traversal prunes branches by envelope, refines the indexed point against the exact
query geometry, and materializes only matches. Polygon holes and boundary semantics
are preserved. Numeric range buffers and active traversal state obey the same
operation budgets as payload reads. The index must cover all active fragments.
Native XY points and R-tree version 0 are supported; other indexed geometry shapes
and newer physical-address index versions fail explicitly. Writer options, memory
accounting, reproducible fixtures, and million-point results are documented in
[`docs/lance-spatial-benchmark.md`](../../docs/lance-spatial-benchmark.md).

Store `dataset.snapshotId` with every external index generation. `takeRows` requires it and returns
a typed `LAKEQL_LANCE_SNAPSHOT_MISMATCH` error before reading data if it does not match. Duplicate
IDs and caller order are preserved. Missing and snapshot-deleted IDs throw by default; use
`onMissing: "null"` to retain their position as `null`. `deletedRowIds` distinguishes deleted IDs
from IDs that never belonged to the snapshot.

## Compatibility

The reader supports Lance storage versions 2.0 and 2.1 with V2 manifest paths.
The official `pylance 8.0.0` fixtures cover both stable IDs and default physical
row addresses. `dataset.storageVersion` and `dataset.rowIdMode` report these
properties. Physical addresses remain meaningful only for the pinned snapshot.

Supported projected leaf types are UTF-8 strings, binary values, booleans, signed
and unsigned integers, 32/64-bit floats, dates, and timestamps. Storage 2.0 supports
uncompressed flat/nullable pages, dictionary strings, and SimpleStruct parents.
Storage 2.1 supports primitive/struct mini-block, full-zip, and all-null layouts;
flat values, inline/out-of-line bitpacking, RLE, FSST, variable strings/binary, and
page dictionaries are decoded with bounded range reads. Definition levels preserve
null leaves and parent structs. Sparse Arrow deletion files, including
Zstandard-compressed buffers, retain their existing support.

The exact GeoParquet conversion from issue #24 works with unchanged
`lance.write_dataset(reader, destination, mode="overwrite")` defaults and
`ds.create_scalar_index("geometry", "RTREE")`. Its source and generated objects
are checked in through Git LFS under `fixtures/spatial-default`. See the spatial
benchmark document for reproduction, supported shapes, and budget semantics.

Official version-0 BTree indexes over the supported scalar key types are supported for exact
equality and bounded range lookup. Null-key lookup, bitmap/label-list index variants, and index
versions other than 0 remain explicit unsupported boundaries.

Official vector index format V3 IVF_FLAT indexes (`index_version` 1) are supported for float32
fixed-size-list columns. IVF_PQ, IVF_SQ, IVF_RQ, and HNSW layouts remain explicit unsupported
boundaries; there is no brute-force dataset fallback when an unsupported index is selected.

The reader rejects unsupported storage versions, compression/layout variants, list-based
nested fields, Roaring-bitmap deletion files, and unknown encodings. It never silently scans a column,
index, Lance data file, or dataset, and it rejects a data range that would read an entire Lance
data file. Arrow deletion vectors are resolved only for fragments containing requested row IDs;
the reader range-loads their footer, record-batch metadata, and value buffer without downloading
the complete deletion object. Because the official Arrow-array producer writes deletion offsets
in hash-set order, membership requires checking that touched fragment's value buffer. Its encoded
and decoded bytes remain subject to the same request, byte, elapsed-time, and memory budgets.

All metadata and data access flows through LakeQL's `ObjectStore`, cancellation, concurrency,
cache, and query-budget contracts. The result reports snapshot/data metadata bytes, logical and
physical bytes, range requests, fragments/pages touched, cache activity, requested, decoded, and
materialized row counts, peak memory, and elapsed time. Vector results additionally report selected
partition IDs and candidates scored. Opening and each public dataset operation receive independent
elapsed-time windows, while compound index lookup and row materialization share one operation
window.

## Reproducing the fixture

Create an isolated Python environment with `pylance==8.0.0` and run:

```sh
python packages/lance/scripts/generate_fixture.py
```

The generator records producer and storage versions, expected projections, stable row IDs, and
SHA-256 hashes in each fixture's `expected.json`. The generated suite includes single- and
multi-page BTree datasets so compatibility tests exercise page boundaries and bounded page-local
search, a dictionary-encoded UTF-8 dataset, plus L2, cosine, and dot IVF_FLAT indexes checked
against official Lance ground truth.

## Reproducing the public-data benchmark

Convert the public MarkWatch USPTO Parquet fixture with official Lance tooling, then exercise 32
scattered stable row IDs through HTTP ranges:

```sh
uv run --with pylance==8.0.0 --with pyarrow --with numpy \
  python bench/lance-prepare-uspto.py --output /tmp/uspto.lance
pnpm bench:lance -- --dataset /tmp/uspto.lance --trials 3
```

The converter records source and dataset hashes, producer/storage versions, stable IDs, and
expected projections in `benchmark.json`. The runner also accepts `--base-url`, `--path`, and
`--manifest` for a dataset hosted on R2 or another HTTP range server. See
[`docs/lance-random-read-benchmark.md`](../../docs/lance-random-read-benchmark.md) for the recorded
results and interpretation.

For the million-row exact BTree lookup benchmark:

```sh
pnpm bench:lance-btree -- \
  --dataset /tmp/parcels-1m.lance \
  --version 3 \
  --key 50000:000000550000 \
  --trials 3
```

The runner enforces a 27-request cold ceiling by default and reports cold and shared-cache trials.
See [`docs/lance-btree-exact-benchmark.md`](../../docs/lance-btree-exact-benchmark.md).
