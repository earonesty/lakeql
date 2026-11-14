import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import { jsonStringUtf8Bytes } from "./json-string-size.js";
import { stableStringify } from "./manifest.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";

/** Stable checkpoint JSON preserves the existing serializer's byte format.
 * The input must be an immutable JSON-shaped checkpoint, not arbitrary objects.
 * Admit normalization/sort metadata, complete text, and output overlap first.
 */
export function encodeOwnedCheckpoint(value: unknown, scope: MemoryScope): OwnedMemory<Uint8Array> {
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
    if (
      typeof value !== "object" ||
      (!Array.isArray(value) &&
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
      const output = scope.allocate(bytes + c.view, "checkpoint encoded bytes", () =>
        new TextEncoder().encode(stableStringify(value)),
      );
      return output.retain();
    } finally {
      scratch.release();
    }
  } finally {
    traversal.release();
  }
}
