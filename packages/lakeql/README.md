# lakeql

[![npm](https://img.shields.io/npm/v/lakeql.svg)](https://www.npmjs.com/package/lakeql)
[![license](https://img.shields.io/npm/l/lakeql.svg)](https://github.com/earonesty/lakeql/blob/main/LICENSE)

**Query Parquet and Iceberg tables directly from object storage, in TypeScript.**

lakeql is small and dependency-light enough to run in **Cloudflare Workers and
other edge/serverless runtimes**, where DuckDB-WASM or a JVM engine is too heavy.
It streams with HTTP range reads and bounded memory, and either reads a table
correctly or rejects it with a typed error — it won't return quietly-wrong rows.

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
