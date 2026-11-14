import { type Batch, type Vector, vectorLength, vectorValue } from "./batch.js";
import { decimalToString, isDecimalValue } from "./decimal-value.js";
import { LakeqlError } from "./errors.js";
import { intervalToString, isIntervalValue } from "./interval.js";
import { jsonStringUtf8Bytes } from "./json-string-size.js";
import type { MemoryScope } from "./memory-scope.js";
import { StringKeyWorkspace } from "./string-key-workspace.js";
import { isTimestampValue } from "./timestamp.js";

/** Byte count of stableStringify(jsonSafeValue(value)), without copying the value
 * tree or constructing its serialized string and UTF8 buffer. This preserves the
 * existing serialized-size metric; it does not estimate JavaScript heap usage.
 */
export function serializedValueBytes(value: unknown): number {
  if (value === null) return 4;
  if (isDecimalValue(value)) return jsonStringUtf8Bytes(decimalToString(value));
  if (isTimestampValue(value)) return jsonStringUtf8Bytes(value.toJSON());
  if (isIntervalValue(value)) return jsonStringUtf8Bytes(intervalToString(value));
  if (value instanceof Uint8Array) return 2 + value.byteLength * 2;
  switch (typeof value) {
    case "string":
      return jsonStringUtf8Bytes(value);
    case "boolean":
      return value ? 4 : 5;
    case "number":
      return Number.isFinite(value) ? String(value).length : jsonStringUtf8Bytes(String(value));
    case "bigint": {
      const number = Number(value);
      return Number.isSafeInteger(number)
        ? String(number).length
        : jsonStringUtf8Bytes(String(value));
    }
    case "object": {
      if (Array.isArray(value)) {
        let bytes = 2 + Math.max(0, value.length - 1);
        for (let index = 0; index < value.length; index++)
          bytes += index in value ? serializedValueBytes(value[index]) : 4;
        return bytes;
      }
      let bytes = 2;
      let fields = 0;
      for (const key in value) {
        if (!Object.hasOwn(value, key)) continue;
        const inner = (value as Record<string, unknown>)[key];
        // Match the existing stable serializer's omitted undefined properties
        // and legacy __proto__ setter behavior, without changing budget thresholds.
        if (inner === undefined || key === "__proto__") continue;
        bytes +=
          (fields++ > 0 ? 1 : 0) + jsonStringUtf8Bytes(key) + 1 + serializedValueBytes(inner);
      }
      return bytes;
    }
    default:
      return jsonStringUtf8Bytes(String(value));
  }
}

/** Count nested physical vectors without constructing their payload objects.
 * Map normalization needs one index per distinct stringified key, matching
 * the public last-key-wins object semantics without retaining decoded values.
 */
export function serializedVectorBytes(vector: Vector, index: number, scope?: MemoryScope): number {
  const workspace = new StringKeyWorkspace(scope);
  try {
    return vectorBytes(vector, index, workspace);
  } finally {
    workspace.dispose();
  }
}

function vectorBytes(vector: Vector, index: number, workspace: StringKeyWorkspace): number {
  if (index < 0 || index >= vectorLength(vector))
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Vector index is out of bounds", {
      index,
      length: vectorLength(vector),
    });
  if ("valid" in vector && vector.valid?.[index] === 0) return 4;
  switch (vector.type) {
    case "union": {
      const child = vector.children[vector.tags[index] ?? -1];
      const offset = vector.offsets[index];
      if (child === undefined || offset === undefined)
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Invalid union row");
      return vectorBytes(child, offset, workspace);
    }
    case "null":
      return 4;
    case "bool":
      return vector.values[index] === 1 ? 4 : 5;
    case "u8":
    case "i32":
    case "u32":
    case "f32":
    case "f64":
    case "i64":
    case "u64":
      return serializedValueBytes(vector.values[index] ?? 0);
    case "utf8":
      return jsonStringUtf8Bytes(vector.values[index] ?? "");
    case "binary":
      return 2 + (vector.values[index]?.byteLength ?? 0) * 2;
    case "dict":
      return vectorBytes(vector.dictionary, vector.indices[index] ?? 0, workspace);
    case "list": {
      const start = vector.offsets[index] ?? 0;
      const end = vector.offsets[index + 1] ?? start;
      let bytes = 2 + Math.max(0, end - start - 1);
      for (let child = start; child < end; child++)
        bytes += vectorBytes(vector.child, child, workspace);
      return bytes;
    }
    case "struct": {
      if (semanticObject((name) => Object.hasOwn(vector.fields, name)))
        return serializedValueBytes(vectorValue(vector, index));
      let bytes = 2;
      let count = 0;
      for (const name in vector.fields) {
        if (!Object.hasOwn(vector.fields, name) || name === "__proto__") continue;
        const field = vector.fields[name];
        if (field === undefined) continue;
        bytes +=
          (count++ > 0 ? 1 : 0) +
          jsonStringUtf8Bytes(name) +
          1 +
          vectorBytes(field, index, workspace);
      }
      return bytes;
    }
    case "map": {
      const start = vector.offsets[index] ?? 0;
      const end = vector.offsets[index + 1] ?? start;
      return workspace.withKeys(
        start,
        end,
        (child) => stringKeyAt(vector.keys, child),
        (entries) => {
          if (semanticObject((name) => entries.has(name)))
            return serializedValueBytes(vectorValue(vector, index));
          let bytes = 2;
          let fields = 0;
          entries.forEach((key, child) => {
            if (key !== "__proto__")
              bytes +=
                (fields++ > 0 ? 1 : 0) +
                jsonStringUtf8Bytes(key) +
                1 +
                vectorBytes(vector.values, child, workspace);
          });
          return bytes;
        },
      );
    }
    default:
      return serializedValueBytes(vectorValue(vector, index));
  }
}

// These field combinations may be recognized by jsonSafeValue as logical
// timestamp/interval objects. Preserve that normalization rather than treating
// them as ordinary struct/map records; key presence alone cannot prove the type.
function semanticObject(has: (name: string) => boolean): boolean {
  return (
    (has("epochNanoseconds") && has("unit") && has("isAdjustedToUTC")) ||
    (has("kind") && has("months") && has("days") && has("nanoseconds"))
  );
}

/** Count serialized rows directly from columns without creating row objects. */
export function serializedBatchBytes(batch: Batch, scope?: MemoryScope): number {
  const columns = Object.entries(batch.columns).filter(([name]) => name !== "__proto__");
  const rowOverhead =
    2 +
    Math.max(0, columns.length - 1) +
    columns.reduce((sum, [name]) => sum + jsonStringUtf8Bytes(name) + 1, 0);
  let bytes = 2 + Math.max(0, batch.rowCount - 1) + batch.rowCount * rowOverhead;
  const workspace = new StringKeyWorkspace(scope);
  try {
    for (let row = 0; row < batch.rowCount; row++)
      for (const [, vector] of columns) bytes += vectorBytes(vector, row, workspace);
    return bytes;
  } finally {
    workspace.dispose();
  }
}

// Hash comparisons revisit keys. Read string/dictionary backing directly while
// retaining the same bounds and null-mask checks as vectorValue.
function stringKeyAt(vector: Vector, index: number): string | null {
  while (vector.type === "dict") {
    if (index < 0 || index >= vector.indices.length) return String(vectorValue(vector, index));
    if (vector.valid?.[index] === 0) return null;
    index = vector.indices[index] ?? 0;
    vector = vector.dictionary;
  }
  if (vector.type === "utf8") {
    if (index < 0 || index >= vector.values.length) return String(vectorValue(vector, index));
    return vector.valid?.[index] === 0 ? null : (vector.values[index] ?? "");
  }
  const key = vectorValue(vector, index);
  return key === null ? null : String(key);
}
