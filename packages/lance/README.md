# lakeql-lance

`lakeql-lance` performs bounded, projection-aware random reads from immutable
[Lance](https://lancedb.github.io/lance/) datasets in Cloudflare Workers, browsers, and Node.js.
It is intended for applications that already have stable Lance row IDs from a search index and
need to materialize the corresponding records without scanning a key column.

```ts
import { openLanceDataset } from "lakeql-lance";

const dataset = await openLanceDataset({
  store,
  path: "catalog.lance",
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
snapshot-safe projected materializer. BTree page lookup, binary search, stable-ID retrieval, and
result materialization share one cumulative byte, request, memory, decoded-row, concurrency,
cancellation, and elapsed-time budget.

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

Store `dataset.snapshotId` with every external index generation. `takeRows` requires it and returns
a typed `LAKEQL_LANCE_SNAPSHOT_MISMATCH` error before reading data if it does not match. Duplicate
IDs and caller order are preserved. Missing and snapshot-deleted IDs throw by default; use
`onMissing: "null"` to retain their position as `null`. `deletedRowIds` distinguishes deleted IDs
from IDs that never belonged to the snapshot.

## Compatibility

The current reader supports stable-row-ID datasets written with Lance storage version 2.0 and V2
manifest paths. The checked-in compatibility fixtures were produced by the official
`pylance 8.0.0` writer. Supported projected leaf types are UTF-8 strings, binary values, booleans,
signed and unsigned integers, 32/64-bit floats, dates, and second/millisecond/microsecond/nanosecond
timestamps using uncompressed flat and nullable encodings. Low-cardinality UTF-8 columns using
Lance's dictionary encoding are supported, including its null sentinel. Sparse Arrow-array
deletion files are supported, including Zstandard-compressed buffers.

Official version-0 BTree indexes over the supported scalar key types are supported for exact
equality and bounded range lookup. Null-key lookup, bitmap/label-list index variants, and index
versions other than 0 remain explicit unsupported boundaries.

Official vector index format V3 IVF_FLAT indexes (`index_version` 1) are supported for float32
fixed-size-list columns. IVF_PQ, IVF_SQ, IVF_RQ, and HNSW layouts remain explicit unsupported
boundaries; there is no brute-force dataset fallback when an unsupported index is selected.

The reader deliberately rejects unsupported storage versions, compressed data pages, nested
fields, Roaring-bitmap deletion files, and unknown encodings. It never silently scans a column,
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
multi-page BTree datasets so compatibility tests exercise page boundaries and bounded logarithmic
reads, a dictionary-encoded UTF-8 dataset, plus L2, cosine, and dot IVF_FLAT indexes checked
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
