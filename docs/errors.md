# Errors

Reference page: use this page when you need stable error codes for application
handling or tests.

Lakeql throws `LakeqlError` with stable `code`, `message`, and optional `details`.

Common codes include:

- `LAKEQL_PARSE_ERROR`
- `LAKEQL_SQL_UNSUPPORTED`
- `LAKEQL_TYPE_ERROR`
- `LAKEQL_VALIDATION_ERROR`
- `LAKEQL_OBJECT_NOT_FOUND`
- `LAKEQL_BUDGET_EXCEEDED`
- `LAKEQL_UNSUPPORTED_ICEBERG_FEATURE`
- `LAKEQL_UNSUPPORTED_PARQUET_FEATURE`
- `LAKEQL_BOOKMARK_STALE`
- `LAKEQL_BOOKMARK_INVALID`
- `LAKEQL_UNSUPPORTED_DELETE_FILES`
- `LAKEQL_ABORTED`

Catch `LakeqlError` at API boundaries and return both `code` and `message` to clients.
