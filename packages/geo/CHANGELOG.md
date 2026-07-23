# lakeql-geo

## 0.1.7

### Patch Changes

- Updated dependencies
  - lakeql-core@0.5.0

## 0.1.6

### Patch Changes

- Updated dependencies [5e21864]
  - lakeql-core@0.4.0

## 0.1.5

### Patch Changes

- Updated dependencies
  - lakeql-core@0.3.0

## 0.1.4

### Patch Changes

- Updated dependencies [87aec8a]
  - lakeql-core@0.2.0

## 0.1.3

### Patch Changes

- Updated dependencies
  - lakeql-core@0.1.3

## 0.1.2

### Patch Changes

- Updated dependencies
  - lakeql-core@0.1.2

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
