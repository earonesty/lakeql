import { LakeqlError } from "./errors.js";
import type { MemoryReservation } from "./memory-budget.js";
import type { MemoryScope } from "./memory-scope.js";

/** Grow exclusive reusable workspace without consuming a fixed stage twice.
 * Success consumes the previous handle; failure leaves it owned by the caller.
 * Only the additional capacity is admitted, including under shared pool pressure.
 */
export function reserveMemoryCapacity(
  scope: MemoryScope,
  previous: MemoryReservation | undefined,
  bytes: number,
  purpose: string,
): MemoryReservation {
  if (!Number.isSafeInteger(bytes) || bytes < 0)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid workspace capacity");
  if (previous !== undefined && bytes <= previous.bytes) return previous;
  const additional = scope.reserve(bytes - (previous?.bytes ?? 0), purpose);
  try {
    return previous === undefined ? additional : previous.merge(additional);
  } catch (error) {
    additional.release();
    throw error;
  }
}
