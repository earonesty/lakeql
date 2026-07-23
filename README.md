# lakeql

LakeQL is a JavaScript query engine for Parquet, Iceberg, and Lance data in
object storage. It runs in Node.js, browsers, Cloudflare Workers, and other
JavaScript runtimes.

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

LakeQL supports SQL through the CLI and JavaScript, a JavaScript query builder,
and a JSON query format. It can query Parquet paths, prefixes, and globs,
including compatible multi-file schemas with missing columns filled as `null`.

| Format or backend | Package | Supported use |
| --- | --- | --- |
| Parquet | `lakeql` | Projected scans, filtering, SQL, aggregation, and bounded object-storage reads. |
| Iceberg | `lakeql` | Snapshot-aware planning and reads over supported table versions, partitions, and delete files. |
| Lance | `lakeql-lance` | Snapshot-safe row-ID materialization, BTree equality/range lookup, and IVF_FLAT vector search without native bindings. |
| WebGPU | `lakeql-webgpu` | Optional acceleration for supported physical selection, reduction, grouped-reduction, and exact-vector top-k fragments. |

The detailed reference pages are still useful when you need exact behavior:

- [Docs index](./docs/README.md)
- [Examples](./docs/examples.md)
- [SQL guide](./docs/sql-dialect.md)
- [Querying Parquet](./docs/querying-parquet.md)
- [Querying Iceberg](./docs/querying-iceberg.md)
- [Querying Lance](./docs/querying-lance.md)
- [WebGPU execution](./docs/webgpu.md)
- [Cloudflare Workers](./docs/cloudflare-workers.md)
- [Compatibility matrix](./docs/compatibility.md)
- [Unsupported features](./docs/unsupported.md)
- [Error codes](./docs/errors.md)

## Packages

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
and D1 adapters. For example, a Cloudflare Worker reading AWS S3 can import
`s3Store` from either `lakeql/fetch` or `lakeql/cloudflare`; using the
runtime-specific entry point leaves room to add R2 or D1 without changing
imports.

The repository also contains workspace packages for adapter, format, and parser
development. Most application code should use the entry points above.

## Query Lance

Install `lakeql-lance` when an application needs bounded reads from an immutable
Lance dataset:

```sh
npm install lakeql lakeql-lance
```

The reader supports stable row-ID materialization, official BTree index lookup,
and official IVF_FLAT vector search through the same object-store, budget,
cache, and cancellation contracts used elsewhere in LakeQL. It never silently
falls back to a dataset scan. See [Querying Lance](./docs/querying-lance.md) for
the supported storage version, API examples, explicit limitations, and the
recorded 32-row benchmark.

## Optional WebGPU Execution

Install `lakeql-webgpu` alongside `lakeql` to add an accelerator backend in
browsers or other runtimes that provide WebGPU:

```sh
npm install lakeql lakeql-webgpu
```

The backend is opt-in and receives its runtime explicitly, so CPU-only
applications do not load WebGPU or a native dependency. It currently supports
nullable scalar selection, bounded count/min/max reductions, and exact `f32`
vector scoring with stable top-k and bounded device residency. See the
[WebGPU execution guide](./docs/webgpu.md), the
[runnable browser example](./examples/webgpu-browser.ts), and the
[package reference](./packages/webgpu/README.md).

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
