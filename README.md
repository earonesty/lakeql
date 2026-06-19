# lakeql

lakeql reads Parquet files and plans Iceberg tables directly from object storage, in
TypeScript. It is small and dependency-light enough to run in constrained runtimes —
Cloudflare Workers, edge functions, serverless — where DuckDB-WASM or a JVM engine is too
heavy. See [why not DuckDB-WASM?](./docs/why-not-duckdb-wasm.md).

It is strict about correctness: it streams with HTTP range reads and bounded memory, and
either reads a table correctly or rejects it with a typed error rather than guessing. Every
supported feature, and every feature it detects and refuses, is enumerated in the
[compatibility matrix](./docs/compatibility.md) and [unsupported features](./docs/unsupported.md).

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
