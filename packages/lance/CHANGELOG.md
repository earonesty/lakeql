# lakeql-lance

## 0.2.0

### Minor Changes

- 4de4c1f: Add snapshot-safe projected materialization for stable Lance row IDs through bounded
  object-store range reads, a broad scalar/binary/date/timestamp type matrix, sparse
  deletion vectors, bounded official BTree exact-key and range lookup, and typed Lance
  compatibility and snapshot errors. Add bounded IVF_FLAT vector search for L2,
  cosine, and dot metrics with explicit dimension, partition, and candidate limits.
  Support Lance dictionary-encoded UTF-8 projections and add a reproducible public
  USPTO scattered-row HTTP range benchmark with physical I/O reporting.

### Patch Changes

- Updated dependencies [4de4c1f]
  - lakeql-core@0.5.1

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
