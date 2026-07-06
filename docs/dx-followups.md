# DX Follow-Ups

This contributor note records the product work that came out of the public docs
overhaul. The items below are implemented and covered by code, docs, examples,
or validation.

## Priority 1: SQL From JavaScript

Status: implemented.

Application code can run SQL through `lake.sql(...)` and `querySql(...)`.

```ts
const rows = await lake
  .sql("select * from input where amount > $1 limit 10", {
    path: "sales.parquet",
    parameters: [100],
  })
  .toArray();

const rows = await querySql(lake, "select * from read_parquet('events/*.parquet') limit 10");
```

Implemented behavior:

- Single Parquet paths with the implicit table name `input`.
- `read_parquet('path-or-glob')` resolved through the lake store.
- Prefix and glob scans.
- Named table bindings for joins.
- Positional parameters.
- Budgets, policies, stats, typed errors, and cancellation behavior through the
  configured lake.
- Result helpers for arrays, row iteration, batches, JSON, NDJSON, and CSV.
- Typed unsupported-SQL errors for parsed shapes outside the helper contract.

Evidence:

- `packages/lakeql/src/sql.ts`
- `packages/lakeql/src/index.test.ts`
- `README.md`
- `packages/lakeql/README.md`
- `docs/introduction.md`
- `docs/querying-parquet.md`
- `docs/sql-dialect.md`

## Priority 2: Complete First-Run Examples

Status: implemented.

The public examples page starts from `npm install lakeql`, shows complete code,
and includes output shapes for:

- Query a local Parquet file with SQL.
- Query a local Parquet file from JavaScript.
- Query a remote HTTP Parquet file from Node.js.
- Query R2 from a Cloudflare Worker.
- Query a prefix or glob of Parquet files.
- Query an Iceberg table.
- Write query results to Parquet.

Evidence:

- `docs/examples.md`
- `examples/local-sql.ts`
- `examples/local-js.ts`
- `examples/http-parquet.ts`
- `examples/r2-parquet.ts`
- `examples/glob-parquet.ts`
- `examples/r2-iceberg.ts`
- `examples/write-parquet.ts`
- `packages/lakeql/src/recipes.test.ts`

## Priority 3: Guide and Reference Split

Status: implemented.

The docs now separate first-run guides, reference pages, recipes, and contributor
plans.

Evidence:

- `docs/README.md`
- `README.md`
- `packages/lakeql/README.md`
- `docs/compatibility.md`
- `docs/unsupported.md`
- `docs/errors.md`
- `docs/parquet-types.md`
- `docs/catalogs.md`
- `scripts/generate-compatibility.mjs`

## Priority 4: Import and Package Story

Status: implemented.

Public docs now lead with the three application entry points:

- `lakeql` for shared browser-safe APIs.
- `lakeql/node` for Node.js stores such as HTTP and S3.
- `lakeql/cloudflare` for Workers and R2.

Beginner guides use those entry points and do not require direct workspace
package imports.

Evidence:

- `README.md`
- `packages/lakeql/README.md`
- `docs/introduction.md`
- `docs/querying-parquet.md`
- `docs/examples.md`

## Priority 5: Public Language Pass

Status: implemented.

The README and first-run guides now describe user tasks first, keep adapter
terminology out of beginner flows, and link comparison material outside the
first-run path.

Evidence:

- `README.md`
- `packages/lakeql/README.md`
- `docs/README.md`
- `docs/introduction.md`
- `docs/querying-parquet.md`
- `docs/sql-dialect.md`
- `docs/examples.md`
