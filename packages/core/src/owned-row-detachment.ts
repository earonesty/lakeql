import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { decimalValueAllocationBytes, isDecimalValue } from "./decimal-value.js";
import { LakeqlError } from "./errors.js";
import type { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import { decimalStringAllocationBytes } from "./scalar-allocation.js";
import { TimestampValue } from "./timestamp.js";
import type { Row } from "./types.js";

/** Detach borrowed scanner data before its next yield. Mutable containers and
 * byte views are copied; immutable scalar backing receives an independent charge.
 * Input data properties must remain stable during this synchronous traversal.
 * Cycles, accessors and custom prototypes have no scanner data-copy contract.
 */
export function detachOwnedRow(
  row: Row,
  budget: MemoryBudget,
  immutableSource?: MemoryScope,
): OwnedMemory<Row> {
  const scope = new MemoryScope(budget);
  try {
    const ancestors = scope.allocate(c.array, "row traversal stack", () => [] as object[]).value;
    const copy = (value: unknown): unknown => {
      if (
        value === null ||
        value === undefined ||
        typeof value === "boolean" ||
        typeof value === "number"
      )
        return value;
      // Projected immutable payload can transfer its existing charge. Mutable
      // containers and byte views still detach before the scanner advances.
      if (typeof value === "string" || typeof value === "bigint") {
        const retained = immutableSource?.tryRetainAllocation(value);
        if (retained !== undefined) {
          try {
            return scope.adopt(retained).value;
          } catch (error) {
            retained.release();
            throw error;
          }
        }
      }
      if (typeof value === "string")
        return scope.allocate(
          c.string + c.stringCodeUnit * value.length,
          "retained row text",
          () => value,
        ).value;
      if (typeof value === "bigint")
        return scope.allocate(
          c.bigint + decimalStringAllocationBytes(value),
          "retained row integer",
          () => value,
        ).value;
      if (isDecimalValue(value))
        return scope.allocate(
          decimalValueAllocationBytes(value.coefficient),
          "retained row decimal",
          () => value,
        ).value;
      if (value instanceof Uint8Array)
        return scope.allocate(
          c.view + value.byteLength,
          "detached row bytes",
          () => new Uint8Array(value),
        ).value;
      if (typeof value !== "object") throw unsupported();
      const prototype = Object.getPrototypeOf(value);
      if (
        !Array.isArray(value) &&
        prototype !== Object.prototype &&
        prototype !== null &&
        prototype !== TimestampValue.prototype
      )
        throw unsupported();
      if (ancestors.includes(value))
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Cyclic scanner row cannot be detached");
      const frame = scope.reserve(c.arraySlot, "row traversal frame");
      ancestors.push(value);
      try {
        const output = scope.allocate(
          Array.isArray(value) ? c.array + value.length * c.arraySlot : c.object,
          "detached row container",
          () => (Array.isArray(value) ? new Array(value.length) : Object.create(prototype)),
        ).value as Record<string, unknown>;
        for (const key in value) {
          if (!Object.hasOwn(value, key)) continue;
          const scratch = scope.reserve(c.object + 6 * c.property, "row property descriptor");
          try {
            const descriptor = Object.getOwnPropertyDescriptor(value, key);
            if (!descriptor || !("value" in descriptor)) throw unsupported();
            scope.allocate(
              c.property + c.string + key.length * c.stringCodeUnit,
              "detached row property",
              () => {
                Object.defineProperty(output, key, {
                  value: copy(descriptor.value),
                  enumerable: true,
                  writable: true,
                  configurable: true,
                });
                return undefined;
              },
            );
          } finally {
            scratch.release();
          }
        }
        return output;
      } finally {
        ancestors.pop();
        frame.release();
      }
    };
    const output = copy(row) as Row;
    scope.releaseAllocation(ancestors);
    return scope.retainAllocations(output);
  } finally {
    scope.dispose();
  }
}

function unsupported(): LakeqlError {
  return new LakeqlError(
    "LAKEQL_TYPE_ERROR",
    "Scanner row detachment requires plain data properties and supported scalar values",
  );
}
