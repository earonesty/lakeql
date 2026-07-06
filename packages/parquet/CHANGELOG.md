# lakeql-parquet

## 0.2.0

### Minor Changes

- Add SQL window function execution across in-memory and Parquet scans, including partition/order frames, ranking and analytic functions, aggregate windows, `QUALIFY`, and browser benchmark coverage.

### Patch Changes

- Updated dependencies
  - lakeql-core@0.3.0

## 0.1.0

### Minor Changes

- 87aec8a: Add WKB geometry values, GeoParquet geometry byte ingestion, and vectorized `st_contains`/`st_within` spatial predicates for browser/R2 benchmark parity.

### Patch Changes

- Updated dependencies [87aec8a]
  - lakeql-core@0.2.0

## 0.0.5

### Patch Changes

- Add durable object-store cache adapters, filesystem-backed Iceberg reads, delegated Iceberg REST store support, streamed manifest and Parquet page planning, bounded predicate scan late materialization, and refreshed compare-page cache controls.
- Updated dependencies
  - lakeql-core@0.1.3

## 0.0.4

### Patch Changes

- Add nested Parquet vector batch support, schema-aware MAP vector normalization, broader vector execution coverage, and cache-aware column batch reads.
- Updated dependencies
  - lakeql-core@0.1.2

## 0.0.3

### Patch Changes

- Improve cached Parquet scan performance with decoded page-vector reuse, lower-copy vector slicing, and faster grouped aggregate selection paths.
- Updated dependencies
  - lakeql-core@0.1.1

## 0.0.2

### Patch Changes

- 08c94d5: Advance BUILD_PLAN implementation across resource controls, object-store hardening, Iceberg and Parquet contracts, compatibility docs, examples, and benchmark scaffolding.
- Updated dependencies [08c94d5]
- Updated dependencies [6547014]
  - lakeql-core@0.1.0
