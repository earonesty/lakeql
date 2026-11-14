import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { isDecimalValue } from "./decimal-value.js";
import { LakeqlError } from "./errors.js";
import { isIntervalValue } from "./interval.js";
import { JsonContainerCursor } from "./json-container-entries.js";
import { jsonIntegerBytes } from "./json-integer-output.js";
import { JsonScalarFormatter } from "./json-scalar-output.js";
import { jsonStringUtf8Bytes } from "./json-string-size.js";
import { JsonTraversalBudget } from "./json-traversal-budget.js";
import type { MemoryScope } from "./memory-scope.js";
import { isTimestampValue } from "./timestamp.js";

/** UTF8 extent of JSON.stringify(jsonSafeValue(value)), not stableStringify.
 * Undefined means JSON.stringify has no root output. This avoids the cloned
 * JSON tree and output string/buffer. An optional scope admits scalar formatting;
 * traversal frames and ancestor entries are admitted per active depth. Runtime
 * iterator internals remain outside a portable heap measurement.
 * Inputs must remain immutable through encoding; stateful accessors are outside
 * this planning contract. Custom toJSON functions and cycles are rejected.
 */
export function jsonOutputUtf8Bytes(value: unknown, scope?: MemoryScope): number | undefined {
  const scalar = new JsonScalarFormatter(scope);
  const traversal = new JsonTraversalBudget(
    scope,
    5 * c.object + 12 * c.property + c.arraySlot,
    2 * c.object + c.array,
  );
  const ancestors = new Set<object>();
  type Frame = {
    value: object;
    entries: JsonContainerCursor;
    bytes: number;
    count: number;
    key: string | undefined;
  };
  const frames: Frame[] = [];
  const measure = (value: unknown): number | undefined => {
    if (value === null) return 4;
    if (isDecimalValue(value)) return scalar.consume(value, jsonStringUtf8Bytes);
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
        traversal.admit(frames.length + 1);
        frames.push({
          value,
          entries: new JsonContainerCursor(value),
          bytes: 2,
          count: 0,
          key: undefined,
        });
        ancestors.add(value);
        return undefined;
      }
    }
  };
  const accumulate = (frame: Frame, size: number | undefined) => {
    if (Array.isArray(frame.value)) frame.bytes += (frame.count++ ? 1 : 0) + (size ?? 4);
    else if (size !== undefined)
      frame.bytes += (frame.count++ ? 1 : 0) + jsonStringUtf8Bytes(frame.key as string) + 1 + size;
    if (!Number.isSafeInteger(frame.bytes))
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "JSON output extent exceeds safe capacity");
  };
  try {
    const root = measure(value);
    if (frames.length === 0) return root;
    while (frames.length > 0) {
      const frame = frames[frames.length - 1] as Frame;
      if (!frame.entries.advance()) {
        frames.pop();
        ancestors.delete(frame.value);
        if (frames.length === 0) return frame.bytes;
        accumulate(frames[frames.length - 1] as Frame, frame.bytes);
      } else {
        frame.key = frame.entries.key;
        const depth = frames.length;
        const size = measure(frame.entries.value);
        if (frames.length === depth) accumulate(frame, size);
      }
    }
    return undefined;
  } finally {
    traversal.dispose();
    scalar.dispose();
  }
}
