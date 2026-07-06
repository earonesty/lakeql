# Iceberg Catalogs

Lakeql keeps Iceberg catalog integration small and explicit. Catalogs are responsible for locating
the current metadata file and, for appends, committing the next snapshot with compare-and-swap
semantics. Data-file reads still go through the configured `ObjectStore`.

## Interfaces

`IcebergCatalog` is the adapter interface for catalogs that can load, list, and commit:

```ts
interface IcebergCatalog {
  loadTable(store: ObjectStore): Promise<IcebergTable>;
  listTables(): Promise<{ namespace: string[]; name: string }[]>;
  commitAppend(input: IcebergCommitInput): Promise<boolean | IcebergCommitResult>;
}
```

`IcebergCommitCatalog` is the smaller append-only interface accepted by
`IcebergTable.appendFiles()` and `IcebergTable.appendOutputManifest()`:

```ts
interface IcebergCommitCatalog {
  commitAppend(input: IcebergCommitInput): Promise<boolean | IcebergCommitResult>;
}
```

Returning `false` or `{ committed: false }` means the catalog rejected the commit because the
base snapshot is stale. Lakeql converts that result into `LAKEQL_ICEBERG_COMMIT_CONFLICT`.

## Object-Store Metadata

`loadIcebergTableFromObjectStore({ store, tableLocation })` reads
`metadata/version-hint.text` when present and otherwise lists `metadata/vN.metadata.json`
files to choose the highest version. This path is supported and covered by tests.

The default `ObjectStoreIcebergCommitCatalog` writes the next manifest and metadata object, then
updates `metadata/version-hint.text` with `conditionalPut`. Stores used for object-store commits
must implement `conditionalPut`; otherwise Lakeql rejects the append with `LAKEQL_CATALOG_ERROR`.

## REST Catalog

`icebergRestCatalog(options)` implements `IcebergCatalog` for these REST calls:

- `loadTable(store)` sends `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}` and hydrates the returned metadata.
- `listTables()` sends `GET /v1/{prefix}/namespaces/{namespace}/tables` and returns table identifiers.
- `commitAppend(input)` writes the next manifest and metadata objects to the `ObjectStore`, then sends a table update with `assert-ref-snapshot-id` for the current `main` snapshot.

HTTP `409` commit responses are treated as stale-snapshot conflicts. Other non-2xx REST responses
raise `LAKEQL_CATALOG_ERROR`.

The provider lane can run against Apache's `iceberg-rest-fixture` and currently proves namespace
creation plus table create/list/load against that reference service. REST append request shape and
409 conflict handling are covered by unit tests with a mocked REST endpoint.

## Planned Adapters

`icebergGlueCatalog(options)` and `icebergNessieCatalog(options)` are exported stubs that satisfy
the `IcebergCatalog` interface and reject `loadTable`, `listTables`, and `commitAppend` with
`LAKEQL_CATALOG_ERROR`. They are intentionally explicit placeholders: callers can wire against the
same interface today, while live Glue and Nessie API implementations remain planned.

Glue and Nessie require live adapters before those catalogs become compatibility promises.
