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

Allocation-aware adapters receive a query child budget as `ScanOptions.memoryBudget`.
`LakeConfig.memoryPool` can share a `MemoryBudget` across concurrent jobs or Lakes;
`budget.maxAllocatedMemoryBytes` sets each query's ceiling. The default internal
pool uses that limit, then `maxMemoryBytes`, then 64 MiB. A supplied pool provides
the default child ceiling. Use `MemoryScope` for admitted allocations and release
scratch in iterator cleanup; explicitly retain ownership for backing that escapes.
These limits currently govern participating allocations, not all decoded objects,
operator state, returned rows, or the process heap. `maxMemoryBytes` continues to
have its separate serialized operator-state meaning.
