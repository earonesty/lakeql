# lakeql

## 0.8.0

### Minor Changes

- 4de4c1f: Add snapshot-safe projected materialization for stable Lance row IDs through bounded
  object-store range reads, a broad scalar/binary/date/timestamp type matrix, sparse
  deletion vectors, bounded official BTree exact-key and range lookup, and typed Lance
  compatibility and snapshot errors. Add bounded IVF_FLAT vector search for L2,
  cosine, and dot metrics with explicit dimension, partition, and candidate limits.
  Support Lance dictionary-encoded UTF-8 projections and add a reproducible public
  USPTO scattered-row HTTP range benchmark with physical I/O reporting.

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
