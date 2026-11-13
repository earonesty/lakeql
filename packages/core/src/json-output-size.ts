import { LakeqlError } from "./errors.js";
import { intervalToString, isIntervalValue } from "./interval.js";
import { jsonStringUtf8Bytes } from "./json-string-size.js";
import { isTimestampValue } from "./timestamp.js";

/** UTF8 extent of JSON.stringify(jsonSafeValue(value)), not stableStringify.
 * Undefined means JSON.stringify has no root output. This avoids the cloned
 * JSON tree and output string/buffer, but scalar formatting and traversal state
 * still allocate. It is an extent calculator, not complete heap admission.
 * Inputs must remain immutable through encoding; stateful accessors are outside
 * this planning contract. Custom toJSON functions and cycles are rejected.
 */
export function jsonOutputUtf8Bytes(value: unknown): number | undefined {
  const ancestors = new Set<object>();
  const measure = (value: unknown): number | undefined => {
    if (value === null) return 4;
    if (isTimestampValue(value)) return jsonStringUtf8Bytes(value.toJSON());
    if (isIntervalValue(value)) return jsonStringUtf8Bytes(intervalToString(value));
    if (value instanceof Uint8Array) return 2 + 2 * value.byteLength;
    switch (typeof value) {
      case "string":
        return jsonStringUtf8Bytes(value);
      case "boolean":
        return value ? 4 : 5;
      case "number":
        return Number.isFinite(value) ? String(value).length : 4;
      case "bigint": {
        const number = Number(value);
        return Number.isSafeInteger(number)
          ? String(number).length
          : jsonStringUtf8Bytes(String(value));
      }
      case "function":
        if (typeof (value as { toJSON?: unknown }).toJSON === "function")
          throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Custom JSON output cannot be forecast");
        return undefined;
      case "undefined":
      case "symbol":
        return undefined;
      case "object": {
        if (ancestors.has(value))
          throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Cyclic JSON output");
        ancestors.add(value);
        try {
          let bytes = 2;
          if (Array.isArray(value)) {
            bytes += Math.max(0, value.length - 1);
            for (let index = 0; index < value.length; index++) bytes += measure(value[index]) ?? 4;
          } else {
            let count = 0;
            for (const key in value) {
              if (!Object.hasOwn(value, key)) continue;
              const inner = (value as Record<string, unknown>)[key];
              if (key === "toJSON" && typeof inner === "function")
                throw new LakeqlError(
                  "LAKEQL_VALIDATION_ERROR",
                  "Custom JSON output cannot be forecast",
                );
              const size = measure(inner);
              if (size !== undefined)
                bytes += (count++ ? 1 : 0) + jsonStringUtf8Bytes(key) + 1 + size;
            }
          }
          if (!Number.isSafeInteger(bytes))
            throw new LakeqlError(
              "LAKEQL_VALIDATION_ERROR",
              "JSON output extent exceeds safe capacity",
            );
          return bytes;
        } finally {
          ancestors.delete(value);
        }
      }
    }
  };
  return measure(value);
}
