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
