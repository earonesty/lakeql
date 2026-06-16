# Recipe: Unified Engine API

Use the aggregate `laql` package when an application should not depend on package internals:

```ts
import { eq, loadTable, planFiles, scanRows } from "laql";

const table = await loadTable({
  format: "iceberg",
  store,
  metadataPath: "iceberg/warehouse/places/metadata/v2.metadata.json",
});

const plan = planFiles(table, {
  where: eq("country", "US"),
  select: ["id", "nation"],
});

for await (const row of scanRows(plan, {
  batchSize: 256,
  maxConcurrentReads: 4,
  signal: request.signal,
})) {
  console.log(row);
}
```

The same flow works for a single Parquet object:

```ts
const parquetTable = await loadTable({
  format: "parquet",
  store,
  path: "sales.parquet",
});

for await (const row of scanRows(planFiles(parquetTable))) {
  console.log(row);
}
```
