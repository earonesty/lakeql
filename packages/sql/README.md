# @laql/sql

Small SQL dialect parser and formatter for LaQL query ASTs.

## Ownership

This package owns LaQL's intentionally small SQL subset. It converts documented SQL strings into
core `PathQueryInit` query ASTs and formats those ASTs back to SQL.

## Public Surface

- `parseSql(sql)` parses the supported dialect into a `SqlQueryAst`.
- `formatSql(ast)` formats a `SqlQueryAst`.
- `SqlQueryAst` extends the core path-query shape with optional `groupBy`, aggregates, and `having`.

The parser is deliberately bounded and rejects unsupported joins, CTEs, subqueries, and broader SQL
grammar with `LAQL_PARSE_ERROR`. See `docs/sql-dialect.md` for the supported syntax.
