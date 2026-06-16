export const ERROR_CODES = [
  "LAQL_PARSE_ERROR",
  "LAQL_TYPE_ERROR",
  "LAQL_UNKNOWN_TABLE",
  "LAQL_UNKNOWN_COLUMN",
  "LAQL_UNSUPPORTED_PUSHDOWN",
  "LAQL_BUDGET_EXCEEDED",
  "LAQL_GROUP_LIMIT_EXCEEDED",
  "LAQL_OBJECT_NOT_FOUND",
  "LAQL_CATALOG_ERROR",
  "LAQL_UNSUPPORTED_ICEBERG_FEATURE",
  "LAQL_UNSUPPORTED_PARQUET_FEATURE",
  "LAQL_ICEBERG_COMMIT_CONFLICT",
  "LAQL_UNSUPPORTED_DELETE_FILES",
  "LAQL_PARQUET_READ_ERROR",
  "LAQL_PARQUET_WRITE_ERROR",
  "LAQL_VALIDATION_ERROR",
  "LAQL_BOOKMARK_STALE",
  "LAQL_BOOKMARK_INVALID",
  "LAQL_ABORTED",
] as const;

export type LaQLErrorCode = (typeof ERROR_CODES)[number];

export type ErrorDetails = Record<string, unknown>;

export class LaQLError extends Error {
  readonly code: LaQLErrorCode;
  readonly details: ErrorDetails;

  constructor(code: LaQLErrorCode, message: string, details: ErrorDetails = {}) {
    super(message);
    this.name = "LaQLError";
    this.code = code;
    this.details = details;
  }
}

export function isLaQLError(value: unknown): value is LaQLError {
  return value instanceof LaQLError;
}
