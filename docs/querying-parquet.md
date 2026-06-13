# Querying Parquet

Create a Parquet lake from an ObjectStore, then query paths or globs.

```ts
import { eq, memoryStore } from "@laql/core";
import { createParquetLake } from "@laql/parquet";

const store = memoryStore();
const lake = createParquetLake({ store });

const rows = await lake
  .path("data/*.parquet")
  .select(["store_id", "amount"])
  .where(eq("region", "west"))
  .limit(10)
  .toArray();
```

The query API supports `rows()`, `batches()`, `toArray()`, `first()`, `count()`, `streamJson()`, `streamNdjson()`, `streamCsv()`, `explain()`, and sliced `run({ slice })`.
