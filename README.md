# lakeql

LakeQL is a JavaScript query engine for Parquet and Iceberg data in object storage.
It runs in Node.js, browsers, Cloudflare Workers, and other JavaScript runtimes.

Use it when you want to query lake data without a database server, native module,
JVM, or WASM runtime. LakeQL reads with HTTP range requests, streams results, and
keeps memory use bounded for serverless and edge workloads.

## Install

```sh
npm install lakeql
```

## Query with SQL

For local Parquet files, the fastest way to try LakeQL is the CLI:

```sh
npx lakeql query \
  --path sales.parquet \
  --sql "select region, sum(amount) as revenue from input group by region order by revenue desc limit 10"
```

Use `--format json`, `--format ndjson`, or `--format csv` to choose the output
format.

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
  .orderBy([{ column: "amount", direction: "desc" }])
  .limit(100)
  .toArray();
```

Use globs or prefixes to scan more than one Parquet file:

```ts
const rows = await lake
  .hive("events/year=2026/month=*/")
  .select(["event_id", "year", "month"])
  .limit(100)
  .toArray();
```

Inside a Cloudflare Worker, read from R2:

```ts
import { createLake, r2Store } from "lakeql/cloudflare";

export default {
  async fetch(_req: Request, env: { DATA: R2Bucket }) {
    const lake = createLake({
      store: r2Store(env.DATA),
      budget: { maxOutputRows: 1000, maxConcurrentReads: 4 },
    });

    return Response.json(await lake.path("sales.parquet").limit(100).toArray());
  },
};
```

## Query Iceberg

```ts
import { eq, loadIcebergTable, r2Store } from "lakeql/cloudflare";

const table = await loadIcebergTable({
  store: r2Store(env.DATA),
  metadataPath: "warehouse/places/metadata/v2.metadata.json",
});

const plan = table.planFiles({ ref: "main", where: eq("country", "US") });
```

The Iceberg reader handles snapshot selection, partition pruning, schema
projection, and delete files for supported table features.

## What LakeQL Supports

LakeQL supports SQL through the CLI, a JavaScript query builder, and a JSON query
format. It can query Parquet paths, prefixes, and globs, including compatible
multi-file schemas with missing columns filled as `null`.

The detailed reference pages are still useful when you need exact behavior:

- [SQL guide](./docs/sql-dialect.md)
- [Querying Parquet](./docs/querying-parquet.md)
- [Querying Iceberg](./docs/querying-iceberg.md)
- [Cloudflare Workers](./docs/cloudflare-workers.md)
- [Compatibility matrix](./docs/compatibility.md)
- [Unsupported features](./docs/unsupported.md)
- [Error codes](./docs/errors.md)

## Packages

Most applications should import from `lakeql`, `lakeql/node`, or
`lakeql/cloudflare`.

| Import | Use it for |
| --- | --- |
| `lakeql` | Query builder, Parquet, Iceberg, in-memory stores, and shared types. |
| `lakeql/node` | Node.js apps that need HTTP, S3, or filesystem cache helpers. |
| `lakeql/cloudflare` | Cloudflare Workers apps that read from R2. |

The repository also contains internal workspace packages such as
`lakeql-core`, `lakeql-parquet`, `lakeql-iceberg`, and `lakeql-sql`. Use those
only when you are building adapters, tooling, or lower-level integrations.

## Current DX Gaps

The CLI has a simple SQL path today. JavaScript app code does not yet have a
single exported helper such as `lake.sql("select ...").toArray()` or
`querySql(lake, sql)`. See [DX follow-ups](./docs/dx-followups.md) for the API
gaps that should become product work.

## Confidence

LakeQL is checked in CI against Spark and PyIceberg reference warehouses,
DuckDB result comparisons, S3 and Iceberg REST catalog round trips, and coverage
gates. See [conformance](./docs/conformance.md) and the
[benchmark report](./bench/REPORT.md).

## Development

```sh
pnpm install
pnpm check
```

## License

MIT
