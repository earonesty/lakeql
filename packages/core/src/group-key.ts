import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import type { Scalar } from "./expr.js";
import { isIntervalValue } from "./interval.js";
import type { MemoryScope } from "./memory-scope.js";
import { decimalStringAllocationBytes } from "./scalar-allocation.js";
import { isTimestampValue } from "./timestamp.js";

/** Lookup encoding and its transient concatenation/decimal-conversion storage.
 * Input scalar storage is caller-owned; no encoded parts are created to forecast.
 */
export function groupKeyAllocationBytes(values: readonly Scalar[]): number {
  if (values.length === 0) return 0;
  let units = 0;
  for (const input of values) {
    const value = input ?? null;
    let part: number;
    if (value === null) part = 5;
    else if (typeof value === "string") part = 7 + value.length;
    else if (typeof value === "number") part = 7 + 32;
    else if (typeof value === "boolean") part = 8 + 5;
    else if (typeof value === "bigint")
      part = 7 + (decimalStringAllocationBytes(value) - c.string) / c.stringCodeUnit;
    else if (isTimestampValue(value))
      part =
        10 + (decimalStringAllocationBytes(value.epochNanoseconds) - c.string) / c.stringCodeUnit;
    else throw new LakeqlError("LAKEQL_TYPE_ERROR", "Value is not a scalar group key");
    // Composite keys prefix each part with its length and a colon. Sixteen digits
    // cover every safe integer extent without creating a decimal length string.
    units += part + (values.length === 1 ? 0 : 17);
  }
  const bytes = c.string + values.length * (3 * c.string + c.object) + 3 * c.stringCodeUnit * units;
  if (!Number.isSafeInteger(bytes))
    throw new LakeqlError("LAKEQL_BUDGET_EXCEEDED", "Group key extent overflow");
  return bytes;
}

export function groupKey(values: readonly Scalar[], scope?: MemoryScope): string {
  if (scope !== undefined) {
    if (values.length === 0) return "";
    return scope.allocate(groupKeyAllocationBytes(values), "group lookup key", () =>
      groupKey(values),
    ).value;
  }
  if (values.length === 1) return scalarGroupKeyPart(values[0] ?? null);
  let key = "";
  for (const value of values) {
    const part = scalarGroupKeyPart(value ?? null);
    key += `${part.length}:${part}`;
  }
  return key;
}

export function scalarGroupKeyPart(value: Scalar): string {
  if (value === null) return "null:";
  if (typeof value === "bigint") return `bigint:${value}`;
  if (value instanceof Uint8Array) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Binary values are not group keys");
  }
  if (isIntervalValue(value)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Interval values are not group keys");
  }
  if (isTimestampValue(value)) return `timestamp:${value.epochNanoseconds}`;
  return `${typeof value}:${value}`;
}
