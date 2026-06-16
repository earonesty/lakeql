# @laql/core

Core LaQL types and runtime: expressions, evaluation, planning, streaming query execution, manifests, bookmarks, joins, and sidecar indexes.

## Ownership

This package owns the package-neutral query engine and object-store contracts. It does not know how
to decode Parquet or load Iceberg metadata; adapters implement those pieces.

## Public Surface

- Expression builders and types: `col`, `lit`, `eq`, `gt`, `and`, `or`, `not`, `fn`, and related AST types.
- Query execution: `Lake`, `QueryBuilder`, `ScanAdapter`, `ScanOptions`, `QueryBudget`, and runtime stats.
- Object storage: `ObjectStore`, `ConditionalObjectStore`, `memoryStore`, and read-control helpers.
- Resource controls: `maxBytes`, `maxFiles`, `maxRowsDecoded`, `maxRangeRequests`, `maxBufferedRows`, `maxMemoryBytes`, `maxConcurrentReads`, `maxElapsedMs`, and `signal`.
- Manifests and resumability: output manifests, task checkpoints, bookmarks, and sidecar indexes.
- Errors: `LaQLError`, stable error codes, and `isLaQLError`.

Use this package when building a new storage or scan adapter. Application code usually imports the
aggregate `laql` package unless it needs a lower-level contract.
