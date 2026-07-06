# Querying Parquet

LakeQL can query one Parquet file, a directory prefix, or a glob of files. The
same query engine runs in Node.js, browsers, and Cloudflare Workers.

## Run SQL from the CLI

```sh
npx lakeql query \
  --path sales.parquet \
  --sql "select store_id, amount from input where region = 'west' order by amount desc limit 10"
```

The CLI uses `input` as the table name for `--path` queries. You can omit
`from input` for short one-file queries:

```sh
npx lakeql query \
  --path sales.parquet \
  --sql "select store_id, amount where region = 'west' limit 10"
```

For joins, name each table:

```sh
npx lakeql query \
  --table sales=sales.parquet \
  --table stores=stores.parquet \
  --sql "select s.store_id, d.segment from sales s join stores d using (store_id) limit 10"
```

## Query from JavaScript

Use `lake.sql(...)` when the query starts as SQL:

```ts
import { createLake, httpStore } from "lakeql/node";

const lake = createLake({
  store: httpStore({ baseUrl: "https://example.com/data" }),
});

const rows = await lake
  .sql("select store_id, amount from input where region = $1 order by amount desc limit 10", {
    path: "sales.parquet",
    parameters: ["west"],
  })
  .toArray();
```

`read_parquet(...)` paths are resolved through the lake's store:

```ts
const rows = await lake
  .sql("select store_id, amount from read_parquet('events/*.parquet') limit 10")
  .toArray();
```

Use the builder API when you want structured expressions:

```ts
import { createLake, eq, httpStore } from "lakeql/node";

const lake = createLake({
  store: httpStore({ baseUrl: "https://example.com/data" }),
});

const rows = await lake
  .path("sales.parquet")
  .select(["store_id", "amount"])
  .where(eq("region", "west"))
  .orderBy([{ column: "amount", direction: "desc" }])
  .limit(10)
  .toArray();
```

Common result methods:

| Method | Use it for |
| --- | --- |
| `toArray()` | Small bounded results. |
| `rows()` | Async iteration over individual rows. |
| `batches()` | Async iteration over row batches. |
| `first()` | The first matching row. |
| `count()` | Count matching rows. |
| `streamJson()` | JSON response bodies. |
| `streamNdjson()` | Streaming line-delimited JSON. |
| `streamCsv()` | CSV response bodies. |
| `explain()` | Planned files, projected columns, and pruning details for builder queries. |

## Query More Than One File

Use a glob:

```ts
const rows = await lake.path("events/2026-*/part-*.parquet").limit(100).toArray();
```

Use `**` when the match should cross directory levels:

```ts
const rows = await lake.path("events/**/*.parquet").limit(100).toArray();
```

Use a prefix ending in `/` to scan every Parquet file under that prefix:

```ts
const rows = await lake.path("events/").limit(100).toArray();
```

Use Hive partition parsing when partition values are stored in the path:

```ts
const rows = await lake
  .hive("events/year=2026/month=*/")
  .select(["event_id", "year", "month"])
  .where(eq("year", "2026"))
  .limit(100)
  .toArray();
```

For multi-file reads, LakeQL checks that shared columns have compatible Parquet
types. If one file is missing a column that exists in another file, the missing
value is returned as `null`.

## Use Budgets

Budgets protect serverless and user-facing workloads from unexpectedly large
scans:

```ts
const lake = createLake({
  store: httpStore({ baseUrl: "https://example.com/data" }),
  budget: {
    maxFiles: 1000,
    maxOutputRows: 10_000,
    maxConcurrentReads: 4,
    maxElapsedMs: 10_000,
  },
});
```

## Window Queries

Window functions are available through SQL and the JavaScript builder.

```ts
import { col, lte } from "lakeql/node";

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

Use `windowWithState({ spill, spillId })` when a sliced window query needs to
persist buffered operator state between runs.

## Named Table Joins From JavaScript

```ts
const rows = await lake
  .sql("select s.store_id, d.segment from sales s join stores d using (store_id)", {
    tables: {
      sales: "sales.parquet",
      stores: "stores.parquet",
    },
  })
  .toArray();
```
