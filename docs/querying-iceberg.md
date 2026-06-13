# Querying Iceberg

Iceberg support plans deterministic data files from metadata JSON fixtures and applies snapshot selection, schema projection, partition pruning, and strict delete-file handling.

```ts
import { eq, memoryStore } from "@laql/core";
import { loadIcebergTable, scanPlannedIcebergRows } from "@laql/iceberg";
import { readIcebergParquetDeletes, readParquetObjects } from "@laql/parquet";

const table = await loadIcebergTable({
  store,
  metadataPath: "iceberg/warehouse/places/metadata/v2.metadata.json",
});

const plan = table.planFiles({
  ref: "main",
  select: ["id", "country"],
  where: eq("country", "US"),
  readMode: "strict",
});
```

For object-store table layouts, load by table location. LaQL first reads
`metadata/version-hint.text`, then falls back to listing `metadata/vN.metadata.json`
files and choosing the highest version:

```ts
import { loadIcebergTableFromObjectStore } from "@laql/iceberg";

const table = await loadIcebergTableFromObjectStore({
  store,
  tableLocation: "iceberg/warehouse/places",
});
```

REST catalogs can load the current table metadata by identifier:

```ts
import { loadIcebergTableFromRest } from "@laql/iceberg";

const table = await loadIcebergTableFromRest({
  store,
  url: "https://catalog.example",
  prefix: "warehouse",
  namespace: ["prod", "analytics"],
  table: "places",
  token: process.env.ICEBERG_CATALOG_TOKEN,
});
```

Use `projectRow()` to map decoded physical rows into the selected Iceberg schema.
For renamed fields, `sourceId` maps old physical column names to the current field
name:

```ts
const row = table.projectRow({ id: 1, country: "US" }, { select: ["id", "nation"] });
```

Strict mode includes known Iceberg delete files in the plan and throws `LAQL_UNSUPPORTED_DELETE_FILES` for unknown delete formats. Use `ignore-deletes` only when raw scans are acceptable; use `ignore-unsupported-deletes` to carry known delete metadata while dropping future formats.

`planFiles()` reports `deleteFilesPlanned` and `deleteFilesIgnored` so callers can audit delete handling without walking every planned file.

When a reader has decoded delete files, `scanPlannedIcebergRows` applies them while streaming planned data-file batches:

```ts
for await (const rows of scanPlannedIcebergRows({
  plan,
  readDataFile: async (file) => readParquetObjects(store, file.path),
  readDeleteFile: async (deleteFile) => readIcebergParquetDeletes(store, deleteFile),
})) {
  // rows are visible after position, equality, and deletion-vector deletes.
}
```

`readIcebergParquetDeletes` decodes Iceberg position-delete and equality-delete
Parquet files. Deletion vectors are planned by `@laql/iceberg`, but they are not
Parquet delete files and require a caller-provided deletion-vector decoder.

`applyIcebergDeletes` is also available when a caller already has decoded delete rows and wants to filter one data-file batch directly:

```ts
const visibleRows = applyIcebergDeletes({
  dataFilePath: file.path,
  rows,
  positionDeletes,
  equalityDeletes,
  deletionVectors,
});
```
