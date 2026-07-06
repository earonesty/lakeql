# lakeql-parquet

Parquet support for LakeQL. This package reads Parquet files from object stores,
plans row groups, validates supported schemas, fills missing columns for
compatible multi-file reads, and writes Parquet output.

Most applications should import from `lakeql`, `lakeql/node`, or
`lakeql/cloudflare`. Use `lakeql-parquet` directly when you need lower-level
Parquet metadata, row-group planning, or writer APIs.

## Main Exports

- `createParquetLake` creates a queryable lake over Parquet files.
- `parquetScanner` connects Parquet files to the core query engine.
- `readParquetObjects` and `readParquetObjectBatches` read Parquet rows from an
  object store.
- `readParquetMetadata` reads footer metadata through ranged object-store
  access.
- `planRowGroups` and `planRowGroupsFromMetadata` expose row-group pruning.
- `rejectUnsupportedParquetSchema` rejects unsupported nested schema features
  before rows are returned.
- `writeParquet`, `writePartitionedParquet`, and task/checkpoint helpers write
  Parquet output and manifests.

See [Parquet types](../../docs/parquet-types.md) for supported Parquet types and
nested-column behavior.
