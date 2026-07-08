# lakeql

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
