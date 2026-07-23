# lakeql-core

## 0.6.0

### Minor Changes

- f8a4d39: Add accelerator-neutral physical planning, type-preserving numeric vectors, and
  the optional `lakeql-webgpu` backend. Support nullable selection, exact
  count/min/max and bounded grouped reductions, exact f32 vector scoring with
  stable bounded top-k, immutable device-resident vector candidates, accelerator
  budgets, explain statistics, device-loss handling, and bounded CPU replay.

### Patch Changes

- f7e1c58: Add snapshot-safe projected materialization for stable Lance row IDs through bounded
  object-store range reads, a broad scalar/binary/date/timestamp type matrix, sparse
  deletion vectors, bounded official BTree exact-key and range lookup, and typed Lance
  compatibility and snapshot errors. Add bounded IVF_FLAT vector search for L2,
  cosine, and dot metrics with explicit dimension, partition, and candidate limits.
  Support Lance dictionary-encoded UTF-8 projections and add a reproducible public
  USPTO scattered-row HTTP range benchmark with physical I/O reporting.

## 0.5.0

### Minor Changes

- Expose query analysis measurements, include them in budget failures, optimize
  Parquet `isIn` row-group pruning, and use explicitly configured concurrent reads
  to prefetch bounded selected-column ranges. Cache parsed Parquet metadata within
  each lake and add an encoded adapter for persistent byte caches.

## 0.4.0

### Minor Changes

- 5e21864: Add multi-file Parquet planning for prefixes and globs, including bounded file expansion, Hive partition pruning, schema compatibility checks, missing-column null fill, and SQL `read_parquet('...')` sources. Empty glob and prefix matches now fail with `LAKEQL_NO_FILES_MATCHED`, and `*` is segment-local; use `**` for recursive matches.

## 0.3.0

### Minor Changes

- Add SQL window function execution across in-memory and Parquet scans, including partition/order frames, ranking and analytic functions, aggregate windows, `QUALIFY`, and browser benchmark coverage.

## 0.2.0

### Minor Changes

- 87aec8a: Add WKB geometry values, GeoParquet geometry byte ingestion, and vectorized `st_contains`/`st_within` spatial predicates for browser/R2 benchmark parity.

## 0.1.3

### Patch Changes

- Add durable object-store cache adapters, filesystem-backed Iceberg reads, delegated Iceberg REST store support, streamed manifest and Parquet page planning, bounded predicate scan late materialization, and refreshed compare-page cache controls.

## 0.1.2

### Patch Changes

- Add nested Parquet vector batch support, schema-aware MAP vector normalization, broader vector execution coverage, and cache-aware column batch reads.

## 0.1.1

### Patch Changes

- Improve cached Parquet scan performance with decoded page-vector reuse, lower-copy vector slicing, and faster grouped aggregate selection paths.

## 0.1.0

### Minor Changes

- 6547014: Spatial predicates (`st_intersects`, `st_contains`, `st_within`, `st_disjoint`)
  now return exact geometry answers via Turf instead of bounding-box
  approximations. Bounding boxes remain a cheap prefilter — and the sidecar bbox
  index still prunes files — but the final predicate is computed on the real
  geometry, so polygons whose envelopes overlap without their shapes touching are
  correctly reported as disjoint. `st_distance` remains envelope-based.

### Patch Changes

- 08c94d5: Advance BUILD_PLAN implementation across resource controls, object-store hardening, Iceberg and Parquet contracts, compatibility docs, examples, and benchmark scaffolding.
