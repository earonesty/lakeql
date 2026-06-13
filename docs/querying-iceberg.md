# Querying Iceberg

Iceberg support plans deterministic data files from metadata JSON fixtures and applies snapshot selection, schema projection, partition pruning, and strict delete-file handling.

```ts
import { eq, memoryStore } from "@laql/core";
import { loadIcebergTable } from "@laql/iceberg";

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

When a reader has decoded delete files, `applyIcebergDeletes` filters data-file rows with Iceberg position deletes, equality deletes, and deletion vectors:

```ts
const visibleRows = applyIcebergDeletes({
  dataFilePath: file.path,
  rows,
  positionDeletes,
  equalityDeletes,
  deletionVectors,
});
```
