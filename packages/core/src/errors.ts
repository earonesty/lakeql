export const ERROR_CODES = [
  "LAKEQL_PARSE_ERROR",
  "LAKEQL_SQL_UNSUPPORTED",
  "LAKEQL_TYPE_ERROR",
  "LAKEQL_UNKNOWN_TABLE",
  "LAKEQL_UNKNOWN_COLUMN",
  "LAKEQL_UNSUPPORTED_PUSHDOWN",
  "LAKEQL_BUDGET_EXCEEDED",
  "LAKEQL_GROUP_LIMIT_EXCEEDED",
  "LAKEQL_OBJECT_NOT_FOUND",
  "LAKEQL_NO_FILES_MATCHED",
  "LAKEQL_SCHEMA_CONFLICT",
  "LAKEQL_CATALOG_ERROR",
  "LAKEQL_UNSUPPORTED_ICEBERG_FEATURE",
  "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
  "LAKEQL_ICEBERG_COMMIT_CONFLICT",
  "LAKEQL_UNSUPPORTED_DELETE_FILES",
  "LAKEQL_PARQUET_READ_ERROR",
  "LAKEQL_PARQUET_WRITE_ERROR",
  "LAKEQL_VALIDATION_ERROR",
  "LAKEQL_BOOKMARK_STALE",
  "LAKEQL_BOOKMARK_INVALID",
  "LAKEQL_ABORTED",
  "LAKEQL_GEO_BACKEND_MISSING",
] as const;

export type LakeqlErrorCode = (typeof ERROR_CODES)[number];

export type ErrorDetails = Record<string, unknown>;

export class LakeqlError extends Error {
  readonly code: LakeqlErrorCode;
  readonly details: ErrorDetails;

  constructor(code: LakeqlErrorCode, message: string, details: ErrorDetails = {}) {
    super(message);
    this.name = "LakeqlError";
    this.code = code;
    this.details = details;
  }
}

export function isLakeqlError(value: unknown): value is LakeqlError {
  return value instanceof LakeqlError;
}
