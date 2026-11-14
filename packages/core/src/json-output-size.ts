import { LakeqlError } from "./errors.js";
import { isIntervalValue } from "./interval.js";
import { jsonIntegerBytes } from "./json-integer-output.js";
import { JsonScalarFormatter } from "./json-scalar-output.js";
import { jsonStringUtf8Bytes } from "./json-string-size.js";
import type { MemoryScope } from "./memory-scope.js";
import { isTimestampValue } from "./timestamp.js";

/** UTF8 extent of JSON.stringify(jsonSafeValue(value)), not stableStringify.
 * Undefined means JSON.stringify has no root output. This avoids the cloned
 * JSON tree and output string/buffer. An optional scope admits scalar formatting;
 * traversal state and runtime workspace remain outside complete heap admission.
 * Inputs must remain immutable through encoding; stateful accessors are outside
 * this planning contract. Custom toJSON functions and cycles are rejected.
 */
export function jsonOutputUtf8Bytes(value: unknown, scope?: MemoryScope): number | undefined {
  const scalar = new JsonScalarFormatter(scope);
  const ancestors = new Set<object>();
  const measure = (value: unknown): number | undefined => {
    if (value === null) return 4;
    if (isTimestampValue(value)) return scalar.consume(value, jsonStringUtf8Bytes);
    if (isIntervalValue(value)) return scalar.consume(value, jsonStringUtf8Bytes);
    if (value instanceof Uint8Array) return 2 + 2 * value.byteLength;
    switch (typeof value) {
      case "string":
        return jsonStringUtf8Bytes(value);
      case "boolean":
        return value ? 4 : 5;
      case "number":
        if (Number.isSafeInteger(value)) return jsonIntegerBytes(value);
        return Number.isFinite(value) ? scalar.consume(value, (text) => text.length) : 4;
      case "bigint": {
        const number = Number(value);
        return Number.isSafeInteger(number)
          ? jsonIntegerBytes(number)
          : scalar.consume(value, jsonStringUtf8Bytes);
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
  try {
    return measure(value);
  } finally {
    scalar.dispose();
  }
}
