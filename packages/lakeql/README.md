# lakeql

[![npm](https://img.shields.io/npm/v/lakeql.svg)](https://www.npmjs.com/package/lakeql)
[![license](https://img.shields.io/npm/l/lakeql.svg)](https://github.com/earonesty/lakeql/blob/main/LICENSE)

LakeQL is a JavaScript query engine for Parquet and Iceberg data in object
storage. It runs in Node.js, browsers, Cloudflare Workers, and other JavaScript
runtimes.

Use it to query lake data without running a database server or shipping native
runtime dependencies. LakeQL reads with range requests, streams results, and
keeps execution bounded for serverless and edge workloads.

## Install

```sh
npm install lakeql
```

## Run a SQL Query

```sh
npx lakeql query \
  --path sales.parquet \
  --sql "select region, sum(amount) as revenue from input group by region order by revenue desc limit 10"
```

Use `--format json`, `--format ndjson`, or `--format csv` to choose the output.

## Query from JavaScript

```ts
import { createLake, gt, httpStore } from "lakeql/node";

const lake = createLake({
  store: httpStore({ baseUrl: "https://example.com/data" }),
});

const rows = await lake
  .path("sales.parquet")
  .select(["store_id", "region", "amount"])
  .where(gt("amount", 100))
  .limit(100)
  .toArray();
```

Inside a Cloudflare Worker, read from R2:

```ts
import { createLake, r2Store } from "lakeql/cloudflare";

export default {
  async fetch(_req: Request, env: { DATA: R2Bucket }) {
    const lake = createLake({ store: r2Store(env.DATA) });
    return Response.json(await lake.path("sales.parquet").limit(100).toArray());
  },
};
```

## Entry Points

| Import | Use it for |
| --- | --- |
| `lakeql` | Query builder, Parquet, Iceberg, in-memory stores, and shared types. |
| `lakeql/node` | Node.js apps that need HTTP, S3, or filesystem cache helpers. |
| `lakeql/cloudflare` | Cloudflare Workers apps that read from R2. |

## Documentation

- [Full README](https://github.com/earonesty/lakeql#readme)
- [Querying Parquet](https://github.com/earonesty/lakeql/blob/main/docs/querying-parquet.md)
- [SQL guide](https://github.com/earonesty/lakeql/blob/main/docs/sql-dialect.md)
- [Querying Iceberg](https://github.com/earonesty/lakeql/blob/main/docs/querying-iceberg.md)
- [Cloudflare Workers](https://github.com/earonesty/lakeql/blob/main/docs/cloudflare-workers.md)
- [Compatibility matrix](https://github.com/earonesty/lakeql/blob/main/docs/compatibility.md)

## Current DX Gaps

The CLI has a simple SQL path today. JavaScript app code does not yet have a
single exported helper such as `lake.sql("select ...").toArray()` or
`querySql(lake, sql)`. The repository tracks these in
[DX follow-ups](https://github.com/earonesty/lakeql/blob/main/docs/dx-followups.md).

## License

MIT
