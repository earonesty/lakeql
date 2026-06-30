# lakeql

[![npm](https://img.shields.io/npm/v/lakeql.svg)](https://www.npmjs.com/package/lakeql)
[![license](https://img.shields.io/npm/l/lakeql.svg)](https://github.com/earonesty/lakeql/blob/main/LICENSE)

LakeQL is a pure JavaScript analytical query engine for Parquet and Iceberg, designed for
edge runtimes such as Cloudflare Workers. It requires no WASM, no native modules, and
streams large datasets with low memory usage.

LakeQL runs anywhere JavaScript runs: browsers, Node.js, Cloudflare Workers, and other
edge/serverless environments. It reads directly from object storage, uses bounded
streaming execution, and avoids the startup and deployment cost of DuckDB-WASM, native
modules, or a JVM.

Why care:

- Pure JavaScript: no WASM startup cost, no native modules.
- Edge runtime friendly: built for constrained JavaScript runtimes.
- Low-memory streaming execution over Parquet and Iceberg.
- Strict correctness: supported data is read correctly, unsupported semantics are rejected with typed errors.

Although LakeQL is optimized for portability and memory efficiency rather than raw
throughput, it is competitive with DuckDB-WASM and is faster on several common workloads.
Compare them in-browser at https://lakeql.com/compare.html.

▶ **Try it live in your browser:** https://lakeql.com/

```sh
npm install lakeql
```

## Quick start

Read a Parquet file over HTTP — no credentials, runs in Node or on the edge:

```ts
import { createLake, httpStore, gt } from "lakeql/node";

const lake = createLake({ store: httpStore({ baseUrl: "https://example.com/data" }) });

const rows = await lake
  .path("sales.parquet")
  .select(["store_id", "amount"])
  .where(gt("amount", 100))
  .limit(100)
  .toArray();
```

Inside a Cloudflare Worker, reading from R2:

```ts
import { createLake, r2Store } from "lakeql/cloudflare";

export default {
  async fetch(_req: Request, env: { DATA: R2Bucket }) {
    const lake = createLake({
      store: r2Store(env.DATA),
      budget: { maxOutputRows: 1000, maxConcurrentReads: 4 },
    });
    const rows = await lake.path("sales.parquet").limit(100).toArray();
    return Response.json(rows);
  },
};
```

Plan an Iceberg table (snapshot selection, partition- and delete-aware pruning):

```ts
import { loadIcebergTable, eq } from "lakeql/node";

const table = await loadIcebergTable({
  store: httpStore({ baseUrl: "https://example.com/warehouse" }),
  metadataPath: "places/metadata/v2.metadata.json",
});

const plan = table.planFiles({ ref: "main", where: eq("country", "US") });
```

## Entry points

| Import | Adds |
| --- | --- |
| `lakeql` | Core query engine, Parquet, Iceberg, and the unified `loadTable` / `planFiles` / `scanRows` / `scanBatches` helpers. |
| `lakeql/node` | Everything in `lakeql`, plus `httpStore` and `s3Store`. |
| `lakeql/cloudflare` | Everything in `lakeql`, plus `r2Store`. |

## CLI

A global install adds a `lakeql` command for quick local queries:

```sh
npm install -g lakeql
lakeql query --path sales.parquet --sql "select region, sum(amount) as revenue from input group by region order by revenue desc"
```

## What it supports

lakeql aims to read supported Parquet and Iceberg features correctly and reject
unsupported table semantics explicitly. See the
[compatibility matrix](https://github.com/earonesty/lakeql/blob/main/docs/compatibility.md)
and [unsupported-but-detected](https://github.com/earonesty/lakeql/blob/main/docs/unsupported.md).
Object-store adapters (`httpStore`, `s3Store`, `r2Store`) use HTTP range reads by
default; Iceberg writes are append-only.

## Documentation

Full docs, recipes, and the engine contract live in the
[repository](https://github.com/earonesty/lakeql#readme):
[querying Parquet](https://github.com/earonesty/lakeql/blob/main/docs/querying-parquet.md) ·
[querying Iceberg](https://github.com/earonesty/lakeql/blob/main/docs/querying-iceberg.md) ·
[Cloudflare Workers](https://github.com/earonesty/lakeql/blob/main/docs/cloudflare-workers.md) ·
[error codes](https://github.com/earonesty/lakeql/blob/main/docs/errors.md) ·
[why not DuckDB-WASM?](https://github.com/earonesty/lakeql/blob/main/docs/why-not-duckdb-wasm.md)

## License

MIT
