# lakeql-sql

SQL parser and formatter for LakeQL.

The main `lakeql` package does not yet expose a one-call SQL execution helper
for JavaScript app code. The CLI uses this package internally to run SQL, and
tools can use it directly when they need to parse or format LakeQL SQL.

## Main Exports

- `parseSql(sql)` parses the supported SQL guide into a `SqlQueryAst`.
- `parseSqlStatement(sql)` parses `SELECT` queries and metadata statements such
  as `DESCRIBE`.
- `formatSql(ast)` formats a `SqlQueryAst`.
- `SqlQueryAst` extends the core path-query shape with optional `groupBy`,
  aggregates, windows, and `having`.

Invalid SQL throws `LAKEQL_PARSE_ERROR`. SQL outside the supported execution
subset throws `LAKEQL_SQL_UNSUPPORTED`. See the
[SQL guide](../../docs/sql-dialect.md) for examples and supported syntax.
