# lakeql

## 0.9.1

### Patch Changes

- e4c7a0a: Reuse covering cached and in-flight Parquet byte ranges, merge adjacent known
  column chunks without padding, and avoid overfetch for metadata tail probes.
  Unify local and shared range-cache retention while preserving byte/entry limits,
  priority, expiry and object-version isolation. Reuse versioned planned object
  identities instead of repeating head probes during deferred scans.

  Reject incomplete physical range responses before serving owners or waiters.
  Revalidate cached plan identities at query boundaries, and invalidate byte-store
  cache entries when fresh head/list observations identify an overwritten object.

  Isolate unversioned raw, metadata and decoded cache products by read observation;
  fence late byte-read completions across invalidation; stop validation workers
  from scheduling more I/O after a peer fails.

## 0.9.0

### Minor Changes

- 17646ef: Add shared running counters and bounded `SharedMemoryCache.getOrCompute` memoization.
  Replace repeated buffer and row/vector aggregate accounting scans with retained totals.
  Share scalar and correlated subquery preparation between SQL library and CLI,
  deduplicate equivalent plans within query lifetime, and expose `memoMaxBytes`.
  Preserve existing serialized-size budget semantics, cache priority and TTL behavior.

### Patch Changes

- e70ccd1: Enforce existing operator byte and row limits while retaining deferred payloads for
  ordered top-k and filtered LIMIT queries. Account for selected references and
  materialized result entries incrementally, and release discarded offset references
  before payload reads. Oversized retained payloads now raise a budget error.
- eb5afd6: Stream deferred top-k and filtered LIMIT payloads through bounded, measured output
  windows with consumer backpressure. Charge remaining references, projected results
  and overlapping modeled decoded batches; release completed state between windows.
  Preserve result order and cancellation, shrink unexpectedly wide windows, and allow
  streaming results larger than the live operator allowance.
- a15f6f6: Share priority-queue ordering across bounded row selection, deferred ranking and
  sort-run merging. Preserve earlier equal-key rows, reuse the same selector for
  restored checkpoints, and remove redundant sorting and ordering-term copies.
  Candidate retention remains bounded by offset + limit; existing serialized-size
  and buffered-row budget semantics are preserved.

## 0.8.4

### Patch Changes

- 31f73ed: Keep buffering aggregates (count_distinct, approx_count_distinct, mode, median, quantile) linear under a memory budget by tracking their exact serialised size as values arrive instead of re-serialising the whole buffer per value, and stop mode copying its keys per row without a budget.

## 0.8.3

### Patch Changes

- 764fcd2: Improve vector top-k sorting performance by periodically sorting and trimming candidates while preserving row-index tie breaking. Keep retained row indices bounded by top-k, avoid per-term batch-sized key caches, and deduplicate equivalent sort terms.

## 0.8.2

### Patch Changes

- a012291: Preserve all source columns for SQL wildcard projections, apply exact value-checked INT64 decimal decoding across scan and window execution paths, and support policy-aware DESCRIBE through the public lake.sql API.

## 0.8.1

### Patch Changes

- 01ee02c: Keep HTTP range reads active when an unencoded response merely varies on
  `Accept-Encoding`, and read `INT64 DECIMAL` columns with declared precision above
  15 when every decoded unscaled value remains exactly representable in JavaScript.

## 0.8.0

### Minor Changes

- f7e1c58: Add snapshot-safe projected materialization for stable Lance row IDs through bounded
  object-store range reads, a broad scalar/binary/date/timestamp type matrix, sparse
  deletion vectors, bounded official BTree exact-key and range lookup, and typed Lance
  compatibility and snapshot errors. Add bounded IVF_FLAT vector search for L2,
  cosine, and dot metrics with explicit dimension, partition, and candidate limits.
  Support Lance dictionary-encoded UTF-8 projections and add a reproducible public
  USPTO scattered-row HTTP range benchmark with physical I/O reporting.
- f8a4d39: Add accelerator-neutral physical planning, type-preserving numeric vectors, and
  the optional `lakeql-webgpu` backend. Support nullable selection, exact
  count/min/max and bounded grouped reductions, exact f32 vector scoring with
  stable bounded top-k, immutable device-resident vector candidates, accelerator
  budgets, explain statistics, device-loss handling, and bounded CPU replay.

## 0.7.0

### Minor Changes

- Add a portable Fetch-runtime entry point for HTTP and S3-compatible storage,
  and expose those adapters through the Node.js and Cloudflare runtime entry
  points. Document the correct imports for Web Workers, edge functions, AWS
  Lambda, and Cloudflare Workers.

## 0.6.0

### Minor Changes

- Expose query analysis measurements, include them in budget failures, optimize
  Parquet `isIn` row-group pruning, and use explicitly configured concurrent reads
  to prefetch bounded selected-column ranges. Cache parsed Parquet metadata within
  each lake and add an encoded adapter for persistent byte caches.

## 0.5.2

### Patch Changes

- Include the Parquet scan range coalescing release, reducing object-store range
  request counts for selective Parquet scans over R2/S3/HTTP stores.

## 0.5.1

### Minor Changes

- Publish the public `lakeql` package with the product release line that includes
  multi-file Parquet planning, SQL `read_parquet(...)` sources, JavaScript SQL
  helpers, window function execution, JSON window/QUALIFY queries, browser
  compare-page fixes for DuckDB-WASM spatial and timestamp examples, and the
  priority-1 DuckDB-WASM parity work for common subquery forms.

## 0.2.0

### Minor Changes

- df9cc3c: Add `lake.sql(...)` and `querySql(...)` so JavaScript applications can execute LakeQL SQL strings directly, including implicit `input` paths, `read_parquet(...)` sources, parameters, and named table joins.

## 0.1.9

### Patch Changes

- Add SQL window function execution across in-memory and Parquet scans, including partition/order frames, ranking and analytic functions, aggregate windows, `QUALIFY`, and browser benchmark coverage.

## 0.1.8

### Patch Changes

- Add durable object-store cache adapters, filesystem-backed Iceberg reads, delegated Iceberg REST store support, streamed manifest and Parquet page planning, bounded predicate scan late materialization, and refreshed compare-page cache controls.

## 0.1.7

### Patch Changes

- Add nested Parquet vector batch support, schema-aware MAP vector normalization, broader vector execution coverage, and cache-aware column batch reads.

## 0.1.6

### Patch Changes

- Improve cached Parquet scan performance with decoded page-vector reuse, lower-copy vector slicing, and faster grouped aggregate selection paths.

## 0.1.2

### Patch Changes

- 08c94d5: Advance BUILD_PLAN implementation across resource controls, object-store hardening, Iceberg and Parquet contracts, compatibility docs, examples, and benchmark scaffolding.
