# laql

Aggregate LaQL package that re-exports the core query engine and object-store integrations.

## Ownership

This package is the application-facing umbrella entrypoint. It re-exports the core, Parquet, and
Iceberg surfaces, plus runtime adapter subpaths for Node and Cloudflare-oriented usage.

## Unified engine surface

- `loadTable({ format: "parquet", store, path })` creates a one-file Parquet table handle.
- `loadTable({ format: "iceberg", store, metadataPath })` loads an Iceberg table handle.
- `planFiles(table, options)` returns a facade-owned plan for either handle.
- `scanBatches(plan, options)` yields row batches from Parquet files or delete-aware Iceberg plans.
- `scanRows(plan, options)` yields rows one at a time over the same plans.

Lower-level package APIs remain available as re-exports when a caller needs direct Parquet, Iceberg,
HTTP, R2, S3, or core object-store control.

## Subpaths

- `laql` exports the core query engine, Parquet, Iceberg, and unified engine helpers.
- `laql/node` also exports `httpStore` and `s3Store`.
- `laql/cloudflare` also exports `r2Store`.
