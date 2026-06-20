# lakeql-core

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
