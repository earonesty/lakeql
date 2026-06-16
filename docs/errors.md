# Errors

LaQL throws `LaQLError` with stable `code`, `message`, and optional `details`.

Common codes include:

- `LAQL_PARSE_ERROR`
- `LAQL_TYPE_ERROR`
- `LAQL_VALIDATION_ERROR`
- `LAQL_OBJECT_NOT_FOUND`
- `LAQL_BUDGET_EXCEEDED`
- `LAQL_UNSUPPORTED_ICEBERG_FEATURE`
- `LAQL_UNSUPPORTED_PARQUET_FEATURE`
- `LAQL_BOOKMARK_STALE`
- `LAQL_BOOKMARK_INVALID`
- `LAQL_UNSUPPORTED_DELETE_FILES`
- `LAQL_ABORTED`

Catch `LaQLError` at API boundaries and return both `code` and `message` to clients.
