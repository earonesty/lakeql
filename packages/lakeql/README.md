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

Use SQL strings from application code:

```ts
import { createLake, httpStore } from "lakeql/node";

const lake = createLake({
  store: httpStore({ baseUrl: "https://example.com/data" }),
});

const rows = await lake
  .sql("select store_id, amount from input where amount > $1 limit 100", {
    path: "sales.parquet",
    parameters: [100],
  })
  .toArray();
```

Or use the builder API:

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

Choose an entry point by runtime. The runtime entry points include the query
engine and the storage adapters available in that environment.

| Import | Use it for |
| --- | --- |
| `lakeql` | Runtime-neutral query APIs, Parquet, Iceberg, in-memory stores, and shared types. |
| `lakeql/fetch` | Browsers, Web Workers, and Fetch-compatible edge functions using HTTP or S3-compatible storage. |
| `lakeql/node` | Node.js servers and AWS Lambda functions using HTTP, S3-compatible storage, or filesystem caches. |
| `lakeql/cloudflare` | Cloudflare Workers using HTTP, S3-compatible storage, R2 bindings, or D1-backed caches. |

`lakeql/fetch` is the portable Fetch/Web Crypto contract. `lakeql/node` extends
it with filesystem support, while `lakeql/cloudflare` extends it with native R2
and D1 adapters. A Cloudflare Worker reading AWS S3 can import `s3Store` from
either `lakeql/fetch` or `lakeql/cloudflare`.

## Optional WebGPU Execution

Install the separate `lakeql-webgpu` package to register an opt-in accelerator
backend in a browser or another runtime that explicitly provides WebGPU.
CPU-only applications do not load WebGPU or a native dependency. See the
[package guide](https://github.com/earonesty/lakeql/blob/main/packages/webgpu/README.md)
for supported physical fragments, budgets, residency, and lifecycle behavior.

## Documentation

- [Full README](https://github.com/earonesty/lakeql#readme)
- [Docs index](https://github.com/earonesty/lakeql/blob/main/docs/README.md)
- [Examples](https://github.com/earonesty/lakeql/blob/main/docs/examples.md)
- [Querying Parquet](https://github.com/earonesty/lakeql/blob/main/docs/querying-parquet.md)
- [SQL guide](https://github.com/earonesty/lakeql/blob/main/docs/sql-dialect.md)
- [Querying Iceberg](https://github.com/earonesty/lakeql/blob/main/docs/querying-iceberg.md)
- [Cloudflare Workers](https://github.com/earonesty/lakeql/blob/main/docs/cloudflare-workers.md)
- [Compatibility matrix](https://github.com/earonesty/lakeql/blob/main/docs/compatibility.md)

## License

MIT
