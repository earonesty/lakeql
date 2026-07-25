# lakeql-lance

## 0.2.3

### Patch Changes

- Make a zero-byte decoded index cache disable cache allocation, and support package-specific trusted-publishing release tags.

## 0.2.2

### Patch Changes

- 8f30aef: Reduce object-storage requests for exact B-tree lookups by materializing candidate pages once, searching their sorted keys locally, reusing index metadata already fetched with the manifest, coalescing bounded Lance metadata tails, and retaining decoded lookup/page state in a configurable snapshot-scoped byte cache.

## 0.2.1

### Patch Changes

- 02ce9da: Resolve storage-version-2.0 rows through ordered physical column page spans, enabling bounded reads across multi-page columns including official B-tree index data.

## 0.2.0

### Minor Changes

- f7e1c58: Add snapshot-safe projected materialization for stable Lance row IDs through bounded
  object-store range reads, a broad scalar/binary/date/timestamp type matrix, sparse
  deletion vectors, bounded official BTree exact-key and range lookup, and typed Lance
  compatibility and snapshot errors. Add bounded IVF_FLAT vector search for L2,
  cosine, and dot metrics with explicit dimension, partition, and candidate limits.
  Support Lance dictionary-encoded UTF-8 projections and add a reproducible public
  USPTO scattered-row HTTP range benchmark with physical I/O reporting.

### Patch Changes

- Depend on the public `lakeql` host package instead of the private, unpublished
  `lakeql-core` workspace so the Lance and WebGPU plugins install from npm with a
  single compatible LakeQL runtime.
- Updated dependencies [f7e1c58]
- Updated dependencies [f8a4d39]
  - lakeql@0.8.0

## 0.1.0

### Minor Changes

- Add snapshot-coupled, projected materialization of stable Lance row IDs from
  storage-version 2.0 datasets through bounded object-store range reads.
- Support the official fixed-width scalar, binary, date, timestamp, nullable,
  and sparse Arrow deletion-file representations.
- Discover official version-0 Lance BTree indexes and perform bounded exact-key
  lookup, including duplicate keys and multi-page binary search, before composing
  matched stable IDs with projected row materialization.
- Read inclusive, exclusive, and one-sided BTree ranges in index order with
  pre-materialization output-budget enforcement.
- Search official vector-index V3 IVF_FLAT layouts with bounded centroid
  selection and chunked L2, cosine, or dot scoring, then materialize projected
  rows in distance order.
- Materialize low-cardinality UTF-8 columns stored with Lance dictionary encoding,
  including null sentinels, with official Node and workerd fixtures.
- Add a reproducible public-USPTO conversion and scattered-row HTTP range benchmark
  with physical I/O statistics and full-object-read enforcement.
