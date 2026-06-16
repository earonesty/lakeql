# @laql/iceberg

Iceberg table utilities for LaQL: metadata loading, JSON/Avro manifest hydration, snapshot/file planning, delete application, and append commit helpers.

## Ownership

This package owns Iceberg metadata loading, manifest hydration, snapshot selection, data-file
planning, delete application, append commit boundaries, and catalog contracts. Parquet file decoding
is delegated to `@laql/parquet`.

## Public Surface

- `loadIcebergTable`, `loadIcebergTableFromObjectStore`, and `loadIcebergTableFromRest` load Iceberg metadata.
- `planFiles(table, options)` is the stable standalone file-planning function.
- `IcebergTable.planFiles(options)` remains available as a thin alias.
- `IcebergTable.projectRow(options)` maps decoded physical rows into the selected Iceberg schema.
- `scanPlannedIcebergRows` applies decoded delete files while streaming planned rows.
- `applyIcebergDeletes` filters one decoded batch when callers already have delete rows.
- `IcebergCatalog` and `IcebergCommitCatalog` define catalog adapter contracts.
- `icebergRestCatalog` and `ObjectStoreIcebergCommitCatalog` provide catalog integrations.
- `icebergGlueCatalog` and `icebergNessieCatalog` are typed stubs that satisfy the catalog contract
  and fail loudly until live adapters are implemented.

See `docs/catalogs.md` in the repository root for the catalog contract and current adapter status.
