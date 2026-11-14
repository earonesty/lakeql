import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import { jsonStringUtf8Bytes } from "./json-string-size.js";
import { stableStringify } from "./manifest.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";
import { decimalStringAllocationBytes } from "./scalar-allocation.js";

/** Stable checkpoint JSON preserves the existing serializer's byte format.
 * The input must be an immutable JSON-shaped checkpoint, not arbitrary objects.
 * Admit normalization/sort metadata, complete text, and output overlap first.
 */
export function encodeOwnedCheckpoint(value: unknown, scope: MemoryScope): OwnedMemory<Uint8Array> {
  return withStableJsonAdmission(value, scope, false, (bytes) =>
    scope
      .allocate(bytes + c.view, "checkpoint encoded bytes", () =>
        new TextEncoder().encode(stableStringify(value)),
      )
      .retain(),
  );
}

/** Stable data keys retain legacy bigint/date/byte and nested-record semantics.
 * Input data must remain immutable throughout synchronous forecast/formatting.
 */
export function stringifyOwnedStableData(value: unknown, scope: MemoryScope): OwnedMemory<string> {
  return withStableJsonAdmission(value, scope, true, (bytes) =>
    scope
      .allocate(c.string + bytes * c.stringCodeUnit, "stable data key", () =>
        stableStringify(value),
      )
      .retain(),
  );
}

function withStableJsonAdmission<T>(
  value: unknown,
  scope: MemoryScope,
  extended: boolean,
  produce: (bytes: number) => T,
): T {
  const traversal = scope.reserve(c.object, "checkpoint traversal");
  const active = new Set<object>();
  let normalization = 0;
  const measure = (value: unknown): number => {
    if (value === null) return 4;
    if (typeof value === "string") return jsonStringUtf8Bytes(value);
    if (typeof value === "number") {
      if (!Number.isFinite(value)) normalization += c.string + 32;
      return 32;
    }
    if (typeof value === "boolean") return 5;
    if (value === undefined) return 11;
    if (extended && typeof value === "bigint") {
      const storage = decimalStringAllocationBytes(value);
      normalization += storage;
      return storage / c.stringCodeUnit + 2;
    }
    if (extended && value instanceof Date) {
      normalization += c.string + 64;
      return 34;
    }
    if (extended && value instanceof Uint8Array) {
      normalization += 5 * c.string + value.length * (c.string + 16);
      return 4 * Math.ceil(value.length / 3) + 2;
    }
    if (extended && typeof value === "symbol") {
      const length = (value.description?.length ?? 0) + 8;
      normalization += c.string + length * c.stringCodeUnit;
      return length * 6 + 2;
    }
    if (
      typeof value !== "object" ||
      (!extended &&
        !Array.isArray(value) &&
        Object.getPrototypeOf(value) !== Object.prototype &&
        Object.getPrototypeOf(value) !== null)
    )
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Checkpoint encoding requires JSON-shaped data");
    if (active.has(value)) throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Cyclic checkpoint");
    const frame = scope.reserve(c.property, "checkpoint traversal frame");
    active.add(value);
    try {
      let bytes = 2;
      if (Array.isArray(value)) {
        normalization += c.array + value.length * c.arraySlot;
        bytes += Math.max(0, value.length - 1);
        for (const item of value) bytes += measure(item);
      } else {
        normalization += c.object + c.array;
        let count = 0;
        for (const key in value) {
          if (!Object.hasOwn(value, key)) continue;
          // Object.entries plus stable-sort workspace and normalized properties.
          normalization +=
            c.array +
            3 * c.arraySlot +
            c.object +
            c.property +
            c.string +
            key.length * c.stringCodeUnit;
          const item = (value as Record<string, unknown>)[key];
          if (item === undefined) continue;
          const itemBytes = measure(item);
          if (key === "__proto__") continue;
          bytes += (count++ ? 1 : 0) + jsonStringUtf8Bytes(key) + 1 + itemBytes;
        }
      }
      return bytes;
    } finally {
      active.delete(value);
      frame.release();
    }
  };
  try {
    const bytes = measure(value);
    const scratch = scope.reserve(
      normalization + c.string + bytes * c.stringCodeUnit,
      "checkpoint normalization and text",
    );
    try {
      return produce(bytes);
    } finally {
      scratch.release();
    }
  } finally {
    traversal.release();
  }
}
