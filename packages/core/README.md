# lakeql-core

Core runtime package for LakeQL. It contains expression builders, query
planning, streaming execution, budgets, typed errors, object-store interfaces,
bookmarks, joins, manifests, and sidecar indexes.

Most applications should import from `lakeql`, `lakeql/node`, or
`lakeql/cloudflare`. Use `lakeql-core` when you are building a new storage
adapter, scan adapter, or lower-level integration.

## Main Exports

- Expression builders and types: `col`, `lit`, `eq`, `gt`, `and`, `or`, `not`,
  `fn`, and related AST types.
- Query execution: `Lake`, `QueryBuilder`, `ScanAdapter`, `ScanOptions`,
  `QueryBudget`, and runtime stats.
- Object storage: `ObjectStore`, `ConditionalObjectStore`, `memoryStore`, and
  read-control helpers.
- Resource controls: `maxBytes`, `maxFiles`, `maxRowsDecoded`,
  `maxRangeRequests`, `maxBufferedRows`, `maxMemoryBytes`,
  `maxConcurrentReads`, `maxElapsedMs`, and `signal`.
- Manifests and resumability: output manifests, task checkpoints, bookmarks,
  and sidecar indexes.
- Errors: `LakeqlError`, stable error codes, and `isLakeqlError`.
