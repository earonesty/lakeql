import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Scalar } from "./expr.js";
import type { MemoryScope } from "./memory-scope.js";
import { decimalStringAllocationBytes } from "./scalar-allocation.js";
import { isTimestampValue } from "./timestamp.js";

/** Bounds native scalar string conversion without constructing the string first.
 * Runtime conversion/GC internals are not a portable heap measurement.
 */
export function scalarStringWithAdmission(value: Scalar, scope?: MemoryScope): string {
  if (scope === undefined || typeof value === "string") return String(value);
  return scope.allocate(scalarStringAllocationBytes(value), "scalar string conversion", () =>
    String(value),
  ).value;
}

/** Conservative storage for standard scalar string formatting, before conversion. */
export function scalarStringAllocationBytes(value: Scalar): number {
  let bytes: number = c.string + 64;
  if (typeof value === "bigint") bytes = decimalStringAllocationBytes(value);
  else if (value instanceof Uint8Array)
    bytes = c.string + 8 * value.length + c.array + c.arraySlot * value.length;
  else if (isTimestampValue(value))
    bytes = c.timestamp + 4 * (decimalStringAllocationBytes(value.epochNanoseconds) + 128);
  return bytes;
}
