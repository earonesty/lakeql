import { LakeqlError } from "./errors.js";

/** Row bounds must remain exact before selection capacity or slice ends are computed. */
export function validateRowLimits(
  options: { limit?: number; offset?: number },
  prefix = "",
  requireLimit = false,
): void {
  const { limit, offset } = options;
  if ((requireLimit || limit !== undefined) && (!Number.isSafeInteger(limit) || (limit ?? -1) < 0))
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      `${prefix}limit must be a non-negative safe integer`,
      { limit },
    );
  if (offset !== undefined && (!Number.isSafeInteger(offset) || offset < 0))
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      `${prefix}offset must be a non-negative safe integer`,
      { offset },
    );
  if (limit !== undefined && offset !== undefined && !Number.isSafeInteger(limit + offset))
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `${prefix}offset + limit must be a safe integer`, {
      limit,
      offset,
    });
}
