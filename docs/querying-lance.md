# Querying Lance

LakeQL reads selected records and supported indexes from immutable Lance
datasets in object storage without native Lance bindings. Use `lakeql-lance`
when a browser, Worker, Node.js service, or Lambda already has stable row IDs or
needs a bounded lookup through an official Lance index.

The Lance reader is a separate package so applications that only query Parquet
or Iceberg do not include Lance metadata and index decoding.

## Install

```sh
npm install lakeql lakeql-lance
```

Supply any LakeQL `ObjectStore`. The same dataset API works with HTTP, S3, R2,
and in-memory stores:

```ts
import { httpStore } from "lakeql/node";
import { openLanceDataset } from "lakeql-lance";

const dataset = await openLanceDataset({
  store: httpStore({ baseUrl: "https://example.com/data/" }),
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
```

`openLanceDataset` resolves one immutable manifest. `dataset.snapshotId` is the
identity callers must store beside any row IDs or external index generation.

## Materialize Stable Row IDs

```ts
const result = await dataset.takeRows({
  snapshotId: dataset.snapshotId,
  rowIds: [42n, 7n, 42n],
  select: ["serial", "mark_text", "owner_name", "source_url"],
});

console.log(result.rows);
console.log(result.stats.physicalBytesRequested);
```

The reader groups IDs by fragment and page, reads only selected columns,
coalesces bounded nearby ranges, and restores caller order. Duplicate IDs are
preserved. Missing and snapshot-deleted IDs throw by default; set
`onMissing: "null"` to preserve their positions as `null`.

Passing a snapshot identity from another dataset version fails with
`LAKEQL_LANCE_SNAPSHOT_MISMATCH` before data-page reads. LakeQL never treats a
stable row ID as valid outside the snapshot that produced it.

## Scalar Index Lookup

Official Lance version-0 BTree indexes can locate stable IDs without rescanning
the key column:

```ts
const indexes = await dataset.scalarIndexes();

const exact = await dataset.lookupRows({
  snapshotId: dataset.snapshotId,
  index: indexes[0].name,
  values: [1005, 1031, 1005],
  select: ["mark_text", "owner_name"],
});

const range = await dataset.rangeRows({
  snapshotId: dataset.snapshotId,
  index: indexes[0].name,
  range: {
    lower: 1000,
    lowerInclusive: true,
    upper: 2000,
    upperInclusive: false,
  },
  select: ["mark_text", "owner_name"],
});
```

Exact lookup retains caller key order and duplicate keys. Every indexed
duplicate is returned in stable index order. Range results remain in BTree
order, and LakeQL checks output budgets from the binary-search bounds before
fetching stable IDs or projected data.

## Vector Index Search

Official vector-index V3 IVF_FLAT indexes over float32 fixed-size-list columns
support bounded nearest-neighbor search:

```ts
const vectors = await openLanceDataset({
  store,
  path: "catalog.lance",
  budget,
  vectorLimits: {
    maxDimension: 1_536,
    maxPartitionsSearched: 16,
    maxCandidatesScored: 50_000,
  },
});

const nearest = await vectors.nearest({
  snapshotId: vectors.snapshotId,
  index: "embedding_ivf_flat",
  vector: queryEmbedding,
  k: 20,
  nprobes: 8,
  select: ["title", "source_url"],
});
```

LakeQL selects partitions from official centroid metadata, reads only those
partition-local vectors and stable IDs, scores bounded chunks, retains a
deterministic top-k, and materializes the requested fields in distance order.
L2, cosine, and Lance dot-distance semantics are supported. Searching all
partitions is exact for IVF_FLAT; a smaller `nprobes` trades recall for I/O.

There is no brute-force dataset fallback when a requested vector index or
encoding is unsupported.

## Supported Lance Contract

| Area | Supported |
| --- | --- |
| Dataset storage | Stable row-ID datasets using Lance storage version 2.0 and V2 manifest paths |
| Fixture producer | Official `pylance 8.0.0` |
| Projected leaves | UTF-8, binary, booleans, signed/unsigned integers, float32/64, dates, and timestamps |
| Encodings | Uncompressed flat and nullable values, low-cardinality UTF-8 dictionaries, supported fixed-size-list vector values |
| Deletions | Sparse Arrow-array deletion files, including Zstandard-compressed buffers |
| Scalar indexes | Official version-0 BTree equality and bounded range lookup |
| Vector indexes | Official vector-index V3 IVF_FLAT, index version 1, float32 vectors |
| Runtimes | Browsers, Cloudflare Workers/workerd, Node.js, and other `ObjectStore` hosts |

The reader explicitly rejects unknown storage/index versions, compressed data
pages, unsupported nested fields, Roaring-bitmap deletion files, BTree null-key
lookup, bitmap/label-list scalar indexes, and IVF_PQ, IVF_SQ, IVF_RQ, or HNSW
vector layouts. Rejection is preferable to silently returning incomplete or
misinterpreted data.

For the evidence behind each supported boundary, see the
[compatibility matrix](./compatibility.md). Contributor-level storage and index
details are in the [Lance implementation plan](./lance-edge-vector-search.md).

## Budgets And Physical Statistics

Opening and every operation enforce LakeQL byte, request, memory, decoded-row,
output-row, concurrency, elapsed-time, cache, and cancellation contracts. A
compound index lookup and row materialization share one cumulative operation
budget.

Results report:

- snapshot and data metadata bytes;
- metadata and total elapsed time;
- logical and physical bytes requested;
- range-request count;
- fragments and pages touched;
- requested, decoded, and materialized rows;
- selected columns;
- cache hits and misses;
- peak decoded memory;
- vector partitions and candidates where applicable.

The reader refuses a planned range that would fetch a complete Lance data file.

## Recorded Random-Read Benchmark

The public-data benchmark converts 682,517 USPTO trademark rows with official
Lance tooling, selects 32 widely scattered stable IDs, and materializes five
scalar columns through an HTTP server that rejects full-object reads.

| Measurement | Recorded range |
| --- | ---: |
| Total elapsed | 229.33–603.94 ms |
| Physical bytes | 20,877 B |
| Range requests | 346 |
| Fragments touched | 11 |
| Pages touched | 55 |
| Full-object GETs | 0 |

All 32 rows matched projections recorded by official tooling. This is a
loopback HTTP result and demonstrates proportional I/O, not public-R2 latency.
The exact environment, hashes, commands, and per-trial measurements are in the
[Lance benchmark report](./lance-random-read-benchmark.md).

The repository includes a
[runnable HTTP example](../examples/lance-http.ts).
