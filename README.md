# LaQL

LaQL is a lightweight TypeScript query engine for Parquet and Iceberg-style lake data on object storage.

It provides composable packages for query planning and evaluation, Parquet reads/writes, object-store adapters, SQL parsing, geospatial/H3 helpers, and Iceberg table planning/append workflows.

## Packages

- `laql`: aggregate entrypoint for Node and Cloudflare-oriented usage.
- `@laql/core`: expressions, planning, execution, manifests, bookmarks, joins, and sidecar indexes.
- `@laql/parquet`: Parquet reader/writer integration with row-group pruning.
- `@laql/iceberg`: Iceberg metadata loading, JSON manifest hydration, planning, delete application, and append commits.
- `@laql/http`, `@laql/s3`, `@laql/r2`: object-store adapters.
- `@laql/sql`: small SQL parser and formatter.
- `@laql/geo`: expression builders and geospatial/H3 helper APIs.

## Compatibility

LaQL aims to read supported Parquet and Iceberg features correctly and reject unsupported table
semantics explicitly. See [Compatibility Matrix](./docs/compatibility.md) and
[Unsupported But Detected](./docs/unsupported.md). Catalog adapter contracts are documented in
[Iceberg Catalogs](./docs/catalogs.md), and Parquet type coverage is documented in
[Parquet Types](./docs/parquet-types.md).

## Development

```sh
pnpm install
pnpm typecheck
pnpm lint
pnpm test
```
