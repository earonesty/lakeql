# @laql/parquet

Parquet integration for LaQL, including object-store reads, row-group pruning, projection, metadata inspection, and Parquet writes.

## Ownership

This package owns Parquet file access for LaQL. It bridges `@laql/core` object stores to
`hyparquet`, exposes row-group planning, validates supported schema posture, and writes Parquet
output through `hyparquet-writer`.

## Public Surface

- `readParquetObjects` and `readParquetObjectBatches` read object-store backed Parquet files.
- `readParquetMetadata` reads footer metadata through ranged object-store access.
- `planRowGroups` and `planRowGroupsFromMetadata` expose row-group pruning as a first-class plan with selected row-group indexes and byte ranges where footer metadata provides them.
- `rejectUnsupportedParquetSchema` rejects unsupported nested schema features before rows are returned.
- `parquetScanner` and `createParquetLake` connect Parquet files to the core query engine.
- `writeParquet`, `writePartitionedParquet`, and task/checkpoint helpers write Parquet output and manifests.

See `docs/parquet-types.md` in the repository root for the current type and nested-column posture.
