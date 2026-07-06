# DX Follow-Ups

These are product and documentation gaps found while cleaning up the public
docs. They should become implementation work rather than wording work.

## 1. First-Class SQL Execution in JavaScript

The CLI can run SQL directly:

```sh
npx lakeql query --path sales.parquet --sql "select * from input limit 10"
```

Application code does not yet have an equally simple API. Today users can use
the JavaScript builder, and `lakeql-sql` can parse SQL, but there is no exported
helper such as:

```ts
await lake.sql("select * from sales.parquet limit 10").toArray();
await querySql(lake, "select * from input limit 10", { path: "sales.parquet" });
```

A production API should cover:

- one-file queries with an implicit `input` table
- `read_parquet('path-or-glob')` queries
- named table bindings for joins
- parameter binding
- the same budgets, stats, and typed errors as builder queries

## 2. Public Examples That Start From Zero

The docs need runnable examples for common first tasks:

- query a local Parquet file with SQL
- query a remote HTTP Parquet file from Node.js
- query R2 from a Worker
- query a prefix or glob of Parquet files
- query an Iceberg table
- write query results to Parquet

Each example should include install commands, imports, a complete snippet, and
expected output shape.

## 3. Clear Split Between Guides and Reference

Some docs are reference material and should stay that way:

- compatibility matrix
- unsupported features
- error codes
- Parquet type support
- catalog adapter details

The README should link first to guides and recipes, then to reference pages.
Reference pages should say that they are reference pages at the top.

## 4. Package Story

The public docs should explain imports in terms of user tasks:

- `lakeql` for shared browser-safe APIs
- `lakeql/node` for Node.js stores such as HTTP and S3
- `lakeql/cloudflare` for Workers and R2

Workspace package names such as `lakeql-core` and `lakeql-parquet` should appear
only in lower-level adapter and contributor docs.

## 5. Consistent Language

Avoid internal implementation phrasing in public docs. User-facing pages should
describe what the user can do, which import to use, and what behavior to expect.
Detailed adapter terminology belongs in adapter reference pages only.
