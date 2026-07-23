# lakeql-lance

## 0.1.0

### Minor Changes

- Add snapshot-coupled, projected materialization of stable Lance row IDs from
  storage-version 2.0 datasets through bounded object-store range reads.
- Support the official fixed-width scalar, binary, date, timestamp, nullable,
  and sparse Arrow deletion-file representations.
- Discover official version-0 Lance BTree indexes and perform bounded exact-key
  lookup, including duplicate keys and multi-page binary search, before composing
  matched stable IDs with projected row materialization.
