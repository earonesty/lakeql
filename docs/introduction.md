# Introduction

LakeQL queries Parquet and Iceberg data from JavaScript. It is built for object
storage, edge runtimes, browser tools, and serverless jobs where loading a full
database runtime is awkward.

The main package is `lakeql`:

```ts
import { createLake, gt, httpStore } from "lakeql/node";

const lake = createLake({
  store: httpStore({ baseUrl: "https://example.com/data" }),
});

const rows = await lake
  .path("sales.parquet")
  .select(["store_id", "amount"])
  .where(gt("amount", 100))
  .limit(10)
  .toArray();
```

## The Three Ways to Query

Use the CLI when you want SQL over local Parquet files:

```sh
npx lakeql query \
  --path sales.parquet \
  --sql "select store_id, amount from input where amount > 100 limit 10"
```

Use SQL from JavaScript applications:

```ts
const rows = await lake
  .sql("select store_id, amount from input where amount > $1 limit 10", {
    path: "sales.parquet",
    parameters: [100],
  })
  .toArray();
```

Use the JavaScript query builder when you want structured expressions:

```ts
const rows = await lake
  .path("sales.parquet")
  .select(["store_id", "amount"])
  .where(gt("amount", 100))
  .limit(10)
  .toArray();
```

Use the JSON query API when you need to send a query over the network or store it
as data:

```ts
const rows = await lake
  .query({
    version: 1,
    from: "sales.parquet",
    select: ["store_id", "amount"],
    where: { gt: ["amount", 100] },
    limit: 10,
  })
  .toArray();
```

## Reading Data

LakeQL reads from an `ObjectStore`. The main package includes in-memory stores
for tests, HTTP range reads for public or signed URLs, S3 support for Node.js,
and R2 support for Cloudflare Workers.

```ts
import { createLake, httpStore } from "lakeql/node";

const lake = createLake({
  store: httpStore({ baseUrl: "https://example.com/data" }),
});
```

```ts
import { createLake, r2Store } from "lakeql/cloudflare";

const lake = createLake({ store: r2Store(env.DATA) });
```

## Where to Go Next

- [Examples](./examples.md)
- [Querying Parquet](./querying-parquet.md)
- [SQL guide](./sql-dialect.md)
- [Querying Iceberg](./querying-iceberg.md)
- [Cloudflare Workers](./cloudflare-workers.md)
- [Writing Parquet](./writing-parquet.md)
- [Compatibility matrix](./compatibility.md)
