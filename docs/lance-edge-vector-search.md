# Lance Vector Search at the Edge

Status: design proposal
Research date: 2026-07-20

## Decision

Add Lance dataset and vector-index reading as a separate `lakeql-lance` package.
The package should query Lance datasets through LakeQL's existing `ObjectStore`
contract and run in Cloudflare Workers, browsers, and other low-memory JavaScript
runtimes without a native service.

The durable product boundary is:

- Official Lance implementations build, update, compact, and validate datasets.
- `lakeql-lance` opens and searches supported stable Lance storage versions.
- LakeQL store adapters provide R2, S3-compatible, HTTP, memory, and other byte-range
  transports.
- LakeQL budgets bound memory, bytes, range requests, elapsed time, concurrency, and
  returned rows.

This is format compatibility, not a TypeScript rewrite of the complete Lance
engine. A read-only edge engine has independent value: applications can keep one
open Lance dataset in object storage and query it from an isolate without loading a
native binary or maintaining a resident vector database.

## Motivation

R2 can efficiently return byte ranges, but a byte-range API does not know which
vectors are close to a query vector. An approximate-nearest-neighbor index supplies
that routing layer:

```text
query vector
    -> compare with IVF centroids
    -> select the nearest partitions
    -> range-read their compact vector codes
    -> score candidates
    -> optionally read exact vectors and rerank
    -> materialize selected result columns
```

This access pattern is a strong match for object storage. It replaces a full vector
scan with a small metadata read and several bounded, parallel range reads. It is a
poor match for an unpartitioned graph such as HNSW because graph traversal causes
dependent, unpredictable reads. Lance's IVF layouts provide the locality an edge
runtime needs.

LakeQL already owns the environmental contract needed by such a reader:

```ts
export interface ObjectStore {
  get(path: string): Promise<Uint8Array | null>;
  getRange(
    path: string,
    range: { offset: number; length: number },
  ): Promise<Uint8Array>;
  head(path: string): Promise<ObjectHead | null>;
  // Writes, deletes, and listing omitted here.
}
```

The existing `QueryBudget` also covers `maxBytes`, `maxRangeRequests`,
`maxElapsedMs`, `maxMemoryBytes`, `maxConcurrentReads`, and `maxOutputRows`.
`lakeql-lance` should reuse these meanings and enforcement patterns.

## Prior Art

### Lance

[Lance](https://github.com/lance-format/lance) is an open lakehouse format designed
for fast random access, multimodal data, vector search, and object storage. Its core
implementation is Rust, with Python and Java bindings. The repository does not
currently present a TypeScript edge reader.

The published [Lance vector-index format](https://lance.org/format/index/vector/)
separates an index into clustering, a sub-index, and quantization. Its documented V3
layout stores a vector index as two regular Lance files:

- An index file containing the search structure and IVF metadata.
- An auxiliary file containing row IDs and flat, PQ, SQ, or RaBitQ vector codes.
- IVF centroids plus an offset and length for every partition.
- Quantizer codebooks and configuration in global buffers.

The IVF-PQ example is especially relevant. It stores centroids, partition offsets,
partition lengths, a PQ codebook, row IDs, and fixed-size PQ codes. A reader can
choose partitions in memory and retrieve only their physical ranges.

Lance also publishes a stable `data_storage_version` compatibility contract. The
reader must accept named stable versions explicitly and reject unknown or unstable
versions rather than guessing.

### Rottnest

[Rottnest: Indexing Data Lakes for
Search](https://theory.stanford.edu/~aiken/publications/papers/icde25.pdf) describes
external indexes stored alongside Parquet data in object storage. It covers
high-cardinality lookup, full-text search, and vector embedding search. The paper
identifies the same two limitations that motivate this work:

- Parquet min/max statistics cannot prune high-dimensional similarity search.
- Ordinary Parquet column chunks are too coarse for highly selective retrieval.

Rottnest uses lightweight index files to locate selected records and then reads the
underlying data lake through object-store byte ranges. Its
[package documentation](https://pypi.org/project/rottnest/) describes PQ-IVF as the
vector-index direction.

### Apache Paimon Vector Index

Apache Paimon's [vector-index
documentation](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/)
supports IVF-flat, IVF-PQ, IVF-HNSW variants, and a DiskANN-based index over data-lake
storage.

Its [binary storage-format
specification](https://apache.googlesource.com/paimon-vector-index/+/9a6e09d6587ef7d2b6336a425e8425ac5da2e15a/STORAGE_FORMAT.md)
is a particularly direct validation of the proposed range-read design. An IVF-PQ
file contains:

```text
header
optional OPQ rotation matrix
IVF coarse centroids
PQ centroids
partition offset table
contiguous partition payloads
```

Each payload contains encoded row IDs and transposed PQ codes. The format is
deliberately organized so a reader can identify and retrieve selected posting lists.

### DiskANN

[DiskANN](https://papers.nips.cc/paper_files/paper/2019/hash/09853c7fb1d3f8ee67a61b6bf4a7f8e6-Abstract.html)
demonstrated billion-point nearest-neighbor search with compressed vectors in memory
and a graph plus full vectors on SSD. It establishes the broader principle that ANN
storage need not be memory-resident.

DiskANN is adjacent rather than directly applicable. Local SSD tolerates the random
reads produced by graph traversal much better than remote object storage. Its
quantization and reranking ideas remain useful, while its physical read pattern
should not define the first edge implementation.

## Package Boundary

The package should live at `packages/lance` and publish as `lakeql-lance`.

```text
lakeql-core
  ObjectStore
  QueryBudget
  typed errors and cancellation
        ^
        |
lakeql-lance
  dataset metadata and manifest reader
  Lance file reader
  vector-index reader
  IVF planner
  distance and quantization kernels
  row-ID materialization
        ^
        |
  lakeql-r2 | lakeql-s3 | lakeql-http | caller store
```

Keeping the package separate has concrete runtime benefits:

- Parquet-only applications do not pay for Lance metadata, protobuf, or ANN code.
- Lance compatibility can be versioned independently from the SQL engine.
- The package can state exactly which storage and index versions it reads.
- Search kernels and fixtures can evolve without introducing query-specific branches
  into `lakeql-core`.
- All storage adapters remain reusable because Lance depends on `ObjectStore`, not on
  a provider API.

The umbrella `lakeql` package may expose `lakeql-lance` through a deliberate export
after bundle-size measurements show that unused Lance code tree-shakes completely.
Direct package imports are the default until then.

## Supported Contract

### Included

- Read-only access to explicitly supported stable Lance dataset versions.
- Dataset manifest and fragment resolution needed for indexed search.
- Vector-index discovery by column and index identifier.
- IVF-PQ search with cosine, dot-product, and L2 distance where the stored index
  declares that metric.
- IVF-flat as an exact/reference path within selected partitions and for conformance.
- Candidate reranking from exact stored vectors when requested and available.
- Projection of selected scalar columns for matched row IDs.
- Ranged object-store I/O with cancellation and hard budgets.
- Deterministic query statistics suitable for production telemetry and benchmarks.
- Cloudflare Workers and browser execution without Node-only APIs.

### Excluded

- Creating, training, or mutating vector indexes in an edge request.
- Dataset commits, compaction, deletion processing, schema evolution, or transactions.
- Transparent support for unstable Lance storage aliases.
- Loading an entire index, vector column, or data object as a fallback.
- HNSW or DiskANN traversal until an object-storage benchmark demonstrates a bounded
  and competitive physical read plan.
- Silent brute-force fallback when an index or format feature is unsupported.

The exclusion of writers and index builders is an architectural boundary rather
than deferred completeness. Those operations are batch compute workloads. Official
Lance tooling is the interoperable producer; `lakeql-lance` is the bounded serving
reader.

## Public API

The package should use explicit resource and query objects rather than adding Lance
special cases to the SQL parser.

```ts
import { openLanceDataset } from "lakeql-lance";
import { r2Store } from "lakeql-r2";

const dataset = await openLanceDataset({
  store: r2Store(env.DATA),
  path: "catalog/products.lance",
  budget: {
    maxBytes: 8 * 1024 * 1024,
    maxRangeRequests: 32,
    maxElapsedMs: 500,
    maxMemoryBytes: 16 * 1024 * 1024,
    maxConcurrentReads: 4,
    maxOutputRows: 20,
  },
});

const result = await dataset.nearest({
  column: "embedding",
  vector: queryEmbedding,
  k: 20,
  nprobes: 8,
  refineFactor: 2,
  select: ["id", "title", "url"],
});

for (const match of result.matches) {
  console.log(match.score, match.row);
}

console.log(result.stats.bytesRequested);
console.log(result.stats.rangeRequests);
console.log(result.stats.partitionsSearched);
console.log(result.stats.candidatesScored);
```

The concrete API can change during implementation, but it must preserve these
properties:

- The caller selects the vector column, `k`, and projected result columns.
- Approximation controls are explicit and validated.
- Every search has a hard resource budget.
- Scores are returned with the metric and row identity unambiguous.
- Search statistics distinguish planning, candidate scoring, reranking, and row
  materialization.
- The returned result is bounded; no API implies an unbounded vector-result stream.

SQL integration can follow through a general table-function or physical-operator
contract, for example `lance_vector_search(...)`. It should not precede a stable
direct API because the direct API establishes the physical behavior and error model.

## Query Execution

### 1. Resolve a stable snapshot

Open the dataset path, read the version metadata needed to select a stable snapshot,
and resolve fragments and indexes from that snapshot. All subsequent reads in the
query must refer to the resolved snapshot. If the store exposes ETags, metadata and
index reads should be keyed by path, size, and ETag.

The reader must reject:

- Unknown storage versions.
- The unstable `next` alias.
- Missing or inconsistent fragment/index references.
- Integer offsets or lengths that exceed JavaScript's safe integer range.
- Overlapping, out-of-bounds, or truncated buffers.

### 2. Read index metadata

Read the smallest suffix or footer range necessary to locate Lance global buffers and
schema metadata. Decode only the structures required by the selected vector index.

Cacheable metadata includes:

- Dataset snapshot identity.
- Index type and metric.
- Vector dimension.
- IVF centroids.
- Partition offsets and lengths.
- PQ configuration and codebooks.
- Fragment and row-ID mapping metadata.

Metadata cache entries must be byte-accounted and keyed by immutable snapshot/index
identity. A cache miss must remain correct and bounded.

### 3. Select IVF partitions

Validate the query dimension and numeric values, then calculate its distance from
each coarse centroid. Retain the nearest `nprobes` centroids with a bounded top-k
structure.

The centroid table is expected to be small relative to the corpus. It may be cached,
but query execution must account for its memory. Implementations must not retain a
second boxed-number copy of large `Float32Array` data.

For cosine indexes, follow Lance's stored metric semantics exactly. Do not assume
vectors were normalized unless the format/index contract guarantees it.

### 4. Plan physical ranges

Map selected partitions to auxiliary-file ranges. Coalesce adjacent ranges when the
extra bytes are cheaper than another object-store request and remain within the byte
budget. Apply `maxConcurrentReads` to the resulting request schedule.

Range planning should expose enough information for diagnostics:

- Selected partition IDs.
- Logical bytes required.
- Physical bytes after coalescing.
- Number of range requests.
- Bytes fetched but not decoded.

### 5. Score compact candidates

Decode row IDs and fixed-size PQ codes directly from the ranged buffers. Score codes
in bounded chunks and retain only the best `k * refineFactor` candidates.

The hot path should use typed arrays and structure-of-arrays layouts. Avoid one
JavaScript object per candidate. A candidate heap or equivalent bounded selection
structure should retain primitive row IDs, approximate scores, and partition
locations.

Pure TypeScript is the portability baseline. A later optional WebAssembly or SIMD
kernel is acceptable only if it preserves the same API, budget accounting,
deterministic scoring tolerance, and Worker compatibility. Native bindings are not
part of the package contract.

### 6. Rerank and materialize

When refinement is enabled, retrieve exact vectors only for retained candidates,
compute the declared distance metric, and reduce to `k`. Then retrieve the requested
scalar columns for those row IDs.

Physical row retrieval is likely the hardest Lance-format integration point. It must
preserve random-access efficiency: group row IDs by fragment and page/block, coalesce
nearby reads, decode only selected columns, and restore score order after
materialization.

If exact vectors or selected columns cannot be read under the budget, return a typed
budget error. Do not return an unlabelled mix of approximate and exact scores.

## Resource Model

Edge safety is part of correctness. Every allocation and read should be attributable
to one of these bounded categories:

- Metadata and centroid bytes.
- Quantizer/codebook bytes.
- In-flight physical range buffers.
- Decoded candidate-code chunks.
- Retained candidate state.
- Exact reranking vectors.
- Materialized result rows.
- Cache entries owned outside the query.

The implementation should reuse the existing `QueryBudget` fields where their
meaning applies. Vector-specific shape limits belong in the search options or a
generic vector operator capability contract:

- `k`
- `nprobes`
- `refineFactor`
- maximum vector dimension
- maximum candidates scored
- maximum partitions searched

Defaults may be provided for convenience, but deployment policy must be able to set
hard upper bounds independently of caller input.

No error path may fall back to a full-object read. `ObjectStore.get` is appropriate
only for objects proven smaller than the remaining byte and memory budgets; format
readers should normally use `head` plus `getRange`.

## Format Implementation Strategy

Implement the published stable format, using the Rust source as behavioral reference
where the specification is ambiguous. Do not translate the Rust engine line by line.

The reader will need narrowly scoped modules for:

```text
src/
  dataset/       snapshot, manifest, fragment, and index discovery
  file/          footer, schema metadata, global buffers, ranged reads
  proto/         required Lance metadata messages only
  vector/        metrics, typed-array kernels, bounded top-k
  index/         IVF metadata, PQ codebooks, partition planning and decoding
  rows/          row-ID mapping and projected random access
  errors.ts      stable typed compatibility and corruption errors
  stats.ts       search planning and execution counters
```

Generated protobuf code is acceptable if it is tree-shakeable and limited to the
messages required by supported stable versions. A handwritten decoder is justified
only when conformance tests demonstrate a material bundle or allocation benefit.

Lance is Apache-2.0 licensed. A clean implementation from the public specification
is preferred. Any copied or adapted source must retain the required license and
attribution notices, and the repository should record provenance at the module level.

## Correctness and Conformance

Compatibility must be established against files produced by official Lance, not
files produced by the TypeScript reader itself.

Maintain small, deterministic external fixtures covering:

- Stable supported storage versions.
- IVF-PQ for cosine, dot product, and L2.
- Different vector dimensions and PQ subvector counts.
- Multiple fragments and multiple IVF partitions.
- Empty partitions and short final partitions.
- Exact reranking and projected scalar columns.
- Nullability or schema features permitted by the supported format.
- Deleted or remapped row IDs if required by the selected stable version.
- Corrupt footers, truncated buffers, invalid offsets, unknown versions, and
  unsupported index types.

For each positive fixture, official Lance should generate:

- The dataset and vector index.
- A fixed set of query vectors.
- Expected top-k row IDs and exact scores.
- Approximate-search recall expectations for specified `nprobes` and refinement.
- A manifest containing file hashes and the Lance producer version.

Tests should include:

- Unit tests for binary decoding and each distance/quantization kernel.
- Cross-implementation result tests against the official expected output.
- Range-guard tests proving that unselected partitions are not read.
- Budget tests for bytes, ranges, time, concurrency, memory, candidates, and output.
- Workerd tests with no Node-only imports or globals.
- Fuzz/property tests for malformed offsets, lengths, dimensions, and code counts.
- Cache tests keyed by immutable snapshot and ETag identity.

External fixture generation may depend on Python/Rust Lance. Normal unit and workerd
test lanes must run without network access or a native Lance installation.

## Benchmarks

Performance claims should be reported as physical behavior as well as wall time.
Each benchmark records:

- Dataset vector count and dimension.
- Index type and parameters.
- `k`, `nprobes`, and refinement factor.
- Recall@k against exact official-Lance ground truth.
- Cold and warm latency percentiles.
- Range-request count.
- Bytes requested.
- Candidates scored and reranked.
- Peak or bounded memory attributable to the query.
- Bundle size and cold module initialization time.

Benchmark lanes should include:

- In-memory object store for deterministic CPU/kernel measurement.
- Local HTTP range server for request planning and coalescing.
- Cloudflare Worker plus R2 for the target deployment behavior.
- Official native Lance as a correctness and local CPU reference.
- Cloudflare Vectorize as a managed-service product comparison where practical.

Native Lance's sub-millisecond local SSD result is not the edge target. The useful
question is whether cold and warm Worker queries achieve acceptable recall and
latency while reading a small, predictable number of R2 bytes.

Large benchmark corpora should be generated or stored outside Git. Checked-in
fixtures remain small and deterministic.

## Compatibility Milestones

### Snapshot-safe row materialization foundation

- Package, `ObjectStore`, budget, error, and stats contracts.
- Immutable snapshot resolution and stable row-ID validation.
- Fragment-aware row-ID-to-physical-location resolution.
- Projection-aware page/block reads, physical-read coalescing, and caller-order
  restoration.
- Explicit behavior for duplicate, missing, deleted, invalid, and stale-snapshot row
  IDs.
- Official fixture generator and workerd conformance lane.
- Public object-storage benchmarks for scattered known-positive rows.

This is the shared terminal operator for scalar, full-text, and vector indexes. It
must be useful with externally produced row IDs before LakeQL adds another index
reader.

### Scalar indexed retrieval

- Stable scalar-index discovery and compatibility validation.
- Exact/range lookup returning snapshot-coupled stable row IDs.
- Composition with the row-materialization operator without rescanning key columns.
- Index-aware range planning, cache keys, and physical-I/O statistics.

The first scalar slice supports official version-0 BTree exact equality lookup over
supported scalar types. It reads page summaries, performs batched binary searches
inside only candidate pages, fetches stable IDs for matching bounds, and composes
them with projected materialization under one cumulative budget. Range predicates
and other scalar-index layouts remain later extensions of this milestone, not scan
fallbacks.

### Vector indexed retrieval

- IVF-PQ metadata and codebook decoding.
- Bounded centroid selection and partition range planning.
- Cosine, dot-product, and L2 approximate scoring.
- IVF-flat reference scoring for correctness comparisons.

### Complete vector search result

- Exact-vector refinement.
- Composition with the existing row-materialization operator for projected scalar
  columns.
- Search-specific cache policy and candidate/refinement statistics.

### Query-engine integration

- A general row-ID materialization physical operator.
- Scalar-index lookup rules that produce stable row-ID vectors.
- A general vector-search physical operator or table-function contract.
- Explain output containing index choice and range plan.
- Optional umbrella-package exports supported by bundle measurements.

Each milestone should land with official-Lance fixtures, workerd coverage, explicit
budgets, and byte/range benchmarks. Compatibility claims should name exact stable
storage and index versions.

## Risks and Open Questions

### Row materialization may dominate

Finding approximate row IDs is only half the operation. Retrieving scattered exact
vectors and result columns may produce more object-store work than reading IVF codes.
That path is therefore the first compatibility milestone and a separately useful
public contract, rather than a late vector-search completion step.

### Format breadth can obscure the useful subset

Lance includes table transactions, schema evolution, blobs, scalar indexes, full-text
indexes, deletion handling, and several vector-index generations. The package should
be strict and explicit: a supported stable matrix is safer than partial best-effort
decoding.

### JavaScript numeric and allocation behavior

Boxed arrays and per-candidate objects will exhaust memory and CPU long before the
underlying algorithm does. Kernels require typed arrays, chunked decoding, bounded
heaps, and allocation benchmarks. Float32 versus JavaScript-number accumulation must
be checked against official score tolerances for every metric.

### Object-store latency changes index tradeoffs

The best native Lance parameters are not necessarily the best R2 parameters. More
partitions reduce candidate computation but can increase request count. Range
coalescing, cached centroids, partition size, and `nprobes` must be tuned together
against recall and physical I/O.

### Upstream compatibility

Lance evolves quickly while maintaining stable storage-version contracts. CI should
periodically generate fixtures with the newest official Lance release while retaining
pinned fixtures for every supported stable version. New versions are opt-in after
conformance, never accepted by loose parsing.

## Release Proposition

The package should be described narrowly:

> `lakeql-lance` queries standard Lance vector indexes directly from Cloudflare
> Workers, browsers, and other JavaScript runtimes using bounded object-store range
> reads—without a resident vector service or native runtime.

This complements Lance instead of competing with it. Lance remains the producer and
canonical format implementation; LakeQL supplies the portable edge read path.
