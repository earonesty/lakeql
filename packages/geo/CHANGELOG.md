# lakeql-geo

## 0.1.1

### Patch Changes

- Updated dependencies
  - lakeql-core@0.1.1

## 0.1.0

### Minor Changes

- 6547014: Spatial predicates (`st_intersects`, `st_contains`, `st_within`, `st_disjoint`)
  now return exact geometry answers via Turf instead of bounding-box
  approximations. Bounding boxes remain a cheap prefilter — and the sidecar bbox
  index still prunes files — but the final predicate is computed on the real
  geometry, so polygons whose envelopes overlap without their shapes touching are
  correctly reported as disjoint. `st_distance` remains envelope-based.

### Patch Changes

- Updated dependencies [08c94d5]
- Updated dependencies [6547014]
  - lakeql-core@0.1.0
