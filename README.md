# lakeql

LakeQL is a pure JavaScript analytical query engine for Parquet and Iceberg, designed for
edge runtimes such as Cloudflare Workers. It requires no WASM, no native modules, and
streams large datasets with low memory usage.

LakeQL exists for the places DuckDB-WASM and native analytical engines do not fit well:
edge functions, serverless jobs, browser-hosted tools, and JavaScript-only deployments.
It runs anywhere JavaScript runs, reads directly from object storage, and keeps execution
bounded with streaming HTTP range reads instead of loading whole datasets into memory.

Why care:

- Pure JavaScript: no WASM startup cost, no native modules, no JVM.
- Edge runtime friendly: works in Cloudflare Workers and other constrained JavaScript runtimes.
- Low-memory streaming execution: scan Parquet and plan Iceberg tables from object storage.
- Strict correctness: LakeQL reads supported data correctly or rejects unsupported semantics with typed errors.

Although LakeQL is optimized for portability and memory efficiency rather than raw
throughput, it is competitive with DuckDB-WASM and is faster on several common workloads.
Try the live [LakeQL vs DuckDB-WASM browser/R2 comparison](https://lakeql.com/compare.html)
or read [why not DuckDB-WASM?](./docs/why-not-duckdb-wasm.md).

LakeQL supports SQL, JavaScript builder expressions, and a JSON query API over Parquet and
Iceberg. Every supported feature, and every feature it detects and refuses, is enumerated
in the [compatibility matrix](./docs/compatibility.md) and
[unsupported features](./docs/unsupported.md).

## Install

```sh
npm install lakeql
```

## Use it

Read a Parquet file over HTTP — no credentials, runs in Node or on the edge:

```ts
import { createLake, httpStore } from "lakeql/node";

const lake = createLake({ store: httpStore({ baseUrl: "https://example.com/data" }) });

const rows = await lake
  .path("sales.parquet")
  .select(["id", "amount"])
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

Plan an Iceberg table (snapshot selection, partition + delete-aware file pruning):

```ts
import { eq, loadIcebergTable, r2Store } from "lakeql/cloudflare";

const table = await loadIcebergTable({
  store: r2Store(env.DATA),
  metadataPath: "warehouse/places/metadata/v2.metadata.json",
});

const plan = table.planFiles({ ref: "main", where: eq("country", "US") });
```

## Packages

`lakeql` is the published package — one install, with `lakeql`, `lakeql/node`, and
`lakeql/cloudflare` entry points. It bundles the internal `lakeql-*` modules below, which are
kept as workspace source (not separately published):

| Module | Owns |
| --- | --- |
| `lakeql` | Aggregate entry points (`lakeql/node`, `lakeql/cloudflare`) and the unified `loadTable`/`planFiles`/`scanRows` contract. |
| [`lakeql-core`](./packages/core) | Expressions, planning, execution, budgets/limits, manifests, joins, sidecar indexes, object-store interface, typed errors. |
| [`lakeql-parquet`](./packages/parquet) | Parquet read/write with row-group pruning. |
| [`lakeql-iceberg`](./packages/iceberg) | Iceberg metadata loading, planning, delete application, and append commits. |
| [`lakeql-http`](./packages/http), [`lakeql-s3`](./packages/s3), [`lakeql-r2`](./packages/r2) | Object-store adapters (range reads by default). |
| [`lakeql-sql`](./packages/sql) | Small, bounded SQL parser/formatter (CLI-only). |
| [`lakeql-geo`](./packages/geo) | Geospatial / H3 expression helpers. |

## Documentation

- [Introduction](./docs/introduction.md), [query language](./docs/query-language.md), and [JSON query API](./docs/json-query-api.md)
- Querying: [Parquet](./docs/querying-parquet.md), [Iceberg](./docs/querying-iceberg.md), [partitioning](./docs/partitioning.md)
- Writing: [Parquet](./docs/writing-parquet.md), [Iceberg (append-only)](./docs/writing-iceberg.md)
- [Compatibility matrix](./docs/compatibility.md) and [unsupported-but-detected](./docs/unsupported.md)
- [Iceberg catalogs](./docs/catalogs.md), [Parquet types](./docs/parquet-types.md), [error codes](./docs/errors.md)
- [Cloudflare Workers](./docs/cloudflare-workers.md), [performance](./docs/performance.md), [caching](./docs/cache.md), [security](./docs/security.md)
- [SQL dialect](./docs/sql-dialect.md), [CLI](./docs/cli.md), [geospatial](./docs/geospatial.md), [H3](./docs/h3.md)
- [Recipes](./docs/recipes) and runnable [examples](./examples)

## Trust

lakeql is checked in CI against real engine output, not just self-generated fixtures:
conformance against Spark/PyIceberg reference warehouses, row-for-row comparison against
DuckDB, real S3 (MinIO) and Iceberg REST catalog round-trips, a 90% coverage gate, and a
[reproducible benchmark report](./bench/REPORT.md). See [conformance](./docs/conformance.md).

## Development

```sh
pnpm install
pnpm check   # lint, build, typecheck, tests, conformance, reference, coverage
```

## License

MIT
