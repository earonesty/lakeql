import { LakeqlError } from "lakeql-core";

/** One row-count contract shared by owned, direct, and fallback readers. */
export function parquetBatchSize(requested: number | undefined): number {
  const size = requested ?? 4096;
  if (!Number.isSafeInteger(size) || size < 1) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Parquet batchSize must be a positive safe integer",
    );
  }
  return size;
}
