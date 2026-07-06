# Querying Parquet

Create a Parquet lake from an ObjectStore, then query paths or globs.

```ts
import { eq, memoryStore } from "lakeql-core";
import { createParquetLake } from "lakeql-parquet";

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

Window functions are available through both SQL and the builder API. Use
`.window(alias, expr)` to attach ranking, value, or exact aggregate window
expressions, then `.qualify(expr)` to filter on window results before the final
projection, outer sort, and limit:

```ts
import { col, lte } from "lakeql-core";

const rows = await lake
  .path("sales.parquet")
  .window("rank_in_region", {
    fn: "row_number",
    args: [],
    over: {
      partitionBy: [col("region")],
      orderBy: [{ expr: col("amount"), direction: "desc" }],
    },
  })
  .qualify(lte("rank_in_region", 3))
  .select(["region", "store_id", "amount", "rank_in_region"])
  .toArray();
```

Use `windowWithState({ spill, spillId })` when a window query must persist or
resume its buffered operator state across slices. The state is versioned and
validated against the window plan before it is reused.

The browser site includes executable examples for partition top-N, moving `ROWS`
frames, and timestamp `RANGE` interval frames. Use `compare.html?kind=window` to
run the same window cases against lakeql and DuckDB-WASM in the browser.
