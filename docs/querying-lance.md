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

## Full-Text Search

Official Lance `INVERTED` indexes (pylance 8.0.0, partitioned layout with FST token sets and
block-compressed posting lists) can be searched directly from browsers and workers. Results are
ranked with the same index-global BM25 statistics Lance uses and are returned in descending score
order together with the requested columns.

```ts
const [index] = await dataset.ftsIndexes(); // name, column, withPosition, analyzer, supported

const result = await dataset.searchRows({
  snapshotId: dataset.snapshotId,
  index: index.name,
  query: { kind: "match", text: "quick brown fox", operator: "or" },
  select: ["id", "title"],
  limit: 20,
});
// result.rows, result.rowIds, result.scores, result.totalMatches, result.search, result.stats
```

Queries are plain data and compose:

| Query | Meaning |
| --- | --- |
| `{ kind: "match", text, operator?, fuzziness?, prefixLength?, maxExpansions? }` | Analyzed terms combined with `or` (default) or `and`; `fuzziness` is 0 (default), 1, or 2 and expands each term through the token dictionary (`maxExpansions` caps the expansions accumulated across a partition's terms, `prefixLength` is measured in UTF-8 bytes) |
| `{ kind: "phrase", text, slop? }` | Terms in order within `slop` positions; needs an index created with `with_position=True` |
| `{ kind: "boolean", must?, should?, mustNot? }` | `must` clauses narrow, `should` clauses add score, `mustNot` clauses remove documents |

The query text is analyzed with the pipeline persisted in the index (base tokenizer, token length
limit, lowercasing, English stemming, stop words, ASCII folding), so it matches exactly what Lance
indexed. Analyzer settings LakeQL cannot reproduce exactly (n-gram, ICU, JSON, and dictionary
segmenters; stemming or default stop words for languages other than English) are rejected when
the index is searched, never approximated.

Cost model and memory: opening an index keeps only file directories and corpus totals (document
lengths are summed in bounded chunks and dropped). A search resolves terms against each
partition's token set (an FST: one blob in the format, so it is resident only while its
partition group resolves, and partitions are grouped to fit the remaining memory budget; the
shared cache may keep it). It then reads posting-list offsets and lengths, and streams postings
`postingBlocksPerBatch` blocks per term at a time, merging terms document by document and
scoring `documentsPerBatch` documents per batch with their lengths read per batch. Ranking keeps
a bounded top-`limit` heap. A warm search whose postings fit one batch costs five dependent
round trips (offsets, block offsets, block bytes, document lengths, `_rowid`), regardless of
dataset size; larger postings add batches instead of memory. Phrase queries read position blocks
per candidate batch, validating and admitting each position list before decoding it.

Every buffer, read plan and map is admitted against the operation memory budget before it is
allocated, including empty result containers. Two structures grow with the number of index
partitions and are reserved before they are built: the decoded file directories (charged to the
operation that opens the index, then owned by the shared cache) and the query plan (one posting
location per scoring term per partition). `search.directoryBytes` and `search.planBytes` report
both. `limits` bounds posting entries decoded, documents scored, phrase candidates, token-set
bytes, and the batch sizes; work limits stop at the first document past the limit.

`maxBufferedRows` must cover `limit`, and nothing ever buffers more. Rows deleted after indexing
stay in the index, so they are filtered while ranking: for each batch, the best candidates that
could still enter the top-`limit` have their `_rowid` read and checked against the dataset's
deletion files before they may take a heap slot. One scan therefore returns the final ranking;
nothing is rescored, so deleted hits cannot push a search past `maxCandidates` or
`maxPostingEntries`. A batch with no deleted candidates costs one row-id read, and a run of
deleted hits costs doubling rounds (capped at `maxBufferedRows` rows a round).
`search.rowIdsResolved` counts the candidates checked. Each fragment's deletion file is loaded
once per search and shared with the final row take.

BM25 needs the corpus token total, which the format does not store, so the first search that
has to score anything on an index sums every document's length once: in chunks of
`limits.documentLengthChunkRows`, up to `maxConcurrentReads` at a time, alongside the posting
lookup. Those rows count against `maxRowsDecoded`, and the total is cached with the opened index
(`search.documentLengthsSummed` is 0 afterwards). A query whose terms are all absent from the
dictionary returns without reading any lengths. Query analysis is charged as it runs, so a very
wide boolean query or a very long clause fails the memory budget instead of outgrowing it.

The [full-text benchmark report](./lance-fts-benchmark.md) records loopback measurements and the
network-wave contracts enforced in CI.

Details to know:

- The index must cover every active fragment. After appending data, rebuild or optimize the
  index; searching an index with uncovered fragments is rejected.
- Rows deleted after indexing are skipped while ranking; `totalMatches` still counts them.
  Like Lance, corpus statistics still include deleted documents until the index is rebuilt.
- An `and` query that contains a term absent from the index matches nothing. Lance ignores such
  a term and returns documents without it.
- A boolean query with several `must` clauses scores a document by the sum of its clause scores,
  so it ranks exactly like the equivalent `and` match query. Lance joins `must` clauses on row ID
  and keeps only the first clause's score.
- Document frequencies are index-global: a term's frequency counts every partition that holds it,
  including partitions an `and` query or a fuzzy expansion cutoff excludes from scoring.
- Index format versions 1 and 2 are searched. Version 2 indexes (varint posting tails, shared
  position streams) support match and boolean queries; phrase queries on them are rejected until
  shared position streams are decoded. Arrow-format token sets and legacy single-partition
  layouts are rejected with explicit errors.
- `ftsIndexes()` lists every inverted index. One this reader cannot search (for example a French
  analyzer) is flagged `supported: false` with an `unsupportedReason`; searching it fails.
- An `and` fuzzy query requires a document to appear in every deduplicated expansion posting list
  of a partition, exactly as Lance does.

## Supported Lance Contract

| Area | Supported |
| --- | --- |
| Dataset storage | Stable row-ID datasets using Lance storage version 2.0 and V2 manifest paths |
| Fixture producer | Official `pylance 8.0.0` |
| Projected leaves | UTF-8, binary, booleans, signed/unsigned integers, float32/64, dates, and timestamps |
| Encodings | Uncompressed flat and nullable values, low-cardinality UTF-8 dictionaries, supported fixed-size-list vector values |
| Deletions | Sparse Arrow-array deletion files, including Zstandard-compressed buffers |
| Scalar indexes | Official version-0 BTree equality and bounded range lookup |
| Full-text indexes | Official `INVERTED` indexes: BM25 match (OR/AND, fuzzy), phrase, and boolean queries with the Lance analyzer pipeline for English |
| Vector indexes | Official vector-index V3 IVF_FLAT, index version 1, float32 vectors |
| Runtimes | Browsers, Cloudflare Workers/workerd, Node.js, and other `ObjectStore` hosts |

The reader explicitly rejects unknown storage/index versions, compressed data
pages, unsupported nested fields, Roaring-bitmap deletion files, BTree null-key
lookup, bitmap/label-list scalar indexes, unsupported full-text analyzers, and IVF_PQ, IVF_SQ, IVF_RQ, or HNSW
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
- vector partitions and candidates where applicable;
- full-text partitions, matched terms, posting entries decoded, and documents scored where applicable.

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
