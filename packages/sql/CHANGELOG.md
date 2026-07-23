# lakeql-sql

## 0.2.2

### Patch Changes

- Updated dependencies [4de4c1f]
  - lakeql-core@0.5.1

## 0.2.1

### Patch Changes

- Updated dependencies
  - lakeql-core@0.5.0

## 0.2.0

### Minor Changes

- 5e21864: Add multi-file Parquet planning for prefixes and globs, including bounded file expansion, Hive partition pruning, schema compatibility checks, missing-column null fill, and SQL `read_parquet('...')` sources. Empty glob and prefix matches now fail with `LAKEQL_NO_FILES_MATCHED`, and `*` is segment-local; use `**` for recursive matches.

### Patch Changes

- Updated dependencies [5e21864]
  - lakeql-core@0.4.0

## 0.1.0

### Minor Changes

- Add SQL window function execution across in-memory and Parquet scans, including partition/order frames, ranking and analytic functions, aggregate windows, `QUALIFY`, and browser benchmark coverage.

### Patch Changes

- Updated dependencies
  - lakeql-core@0.3.0

## 0.0.6

### Patch Changes

- Updated dependencies [87aec8a]
  - lakeql-core@0.2.0

## 0.0.5

### Patch Changes

- Updated dependencies
  - lakeql-core@0.1.3

## 0.0.4

### Patch Changes

- Updated dependencies
  - lakeql-core@0.1.2

## 0.0.3

### Patch Changes

- Updated dependencies
  - lakeql-core@0.1.1

## 0.0.2

### Patch Changes

- 08c94d5: Advance BUILD_PLAN implementation across resource controls, object-store hardening, Iceberg and Parquet contracts, compatibility docs, examples, and benchmark scaffolding.
- Updated dependencies [08c94d5]
- Updated dependencies [6547014]
  - lakeql-core@0.1.0
