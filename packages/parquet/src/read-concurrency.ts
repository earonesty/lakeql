import { LakeqlError } from "lakeql-core";

/** Validate before routing, cache lookup, prefetch, or demand transport. */
export function validateParquetReadConcurrency(options: {
  maxConcurrentReads?: number;
  maxConcurrentPageReads?: number;
}): void {
  for (const name of ["maxConcurrentReads", "maxConcurrentPageReads"] as const) {
    const value = options[name];
    if (value !== undefined && (!Number.isSafeInteger(value) || value < 1))
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", `${name} must be a positive safe integer`);
  }
}
