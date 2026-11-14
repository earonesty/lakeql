import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Batch, type Vector, vectorLength } from "./batch.js";
import { LakeqlError } from "./errors.js";
import { sliceInt96Storage } from "./int96-storage.js";
import type { MemoryScope } from "./memory-scope.js";

/** Slice logical rows while preserving typed payload backing and nested children.
 * Legacy JS array vectors copy their reference array; ownership of backing is
 * the caller's responsibility. Nested offsets remain relative to retained children.
 */
export function sliceVector(
  vector: Vector,
  start: number,
  end: number,
  scope?: MemoryScope,
): Vector {
  if (
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(end) ||
    start < 0 ||
    end < start ||
    end > vectorLength(vector)
  ) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Vector slice is out of bounds", { start, end });
  }
  if (start === 0 && end === vectorLength(vector)) return vector;
  if (scope)
    return scope.allocate(sliceVectorBytes(vector, end - start), "vector slice metadata", () =>
      sliceVector(vector, start, end),
    ).value;
  const valid =
    "valid" in vector && vector.valid !== undefined ? vector.valid.subarray(start, end) : undefined;
  switch (vector.type) {
    case "union":
      return {
        type: "union",
        tags: vector.tags.subarray(start, end),
        offsets: vector.offsets.subarray(start, end),
        children: vector.children,
      };
    case "binary-view":
    case "utf8-view": {
      return optionalVectorValidity(
        { ...vector, rowOffset: (vector.rowOffset ?? 0) + start, length: end - start },
        valid,
      );
    }
    case "null":
      return { type: "null", length: end - start };
    case "f32":
      return optionalVectorValidity(
        { type: "f32", values: vector.values.subarray(start, end) },
        valid,
      );
    case "f64":
      return optionalVectorValidity(
        { type: "f64", values: vector.values.subarray(start, end) },
        valid,
      );
    case "i32":
      return optionalVectorValidity(
        { type: "i32", values: vector.values.subarray(start, end) },
        valid,
      );
    case "u32":
      return optionalVectorValidity(
        { type: "u32", values: vector.values.subarray(start, end) },
        valid,
      );
    case "u8":
      return optionalVectorValidity(
        { type: "u8", values: vector.values.subarray(start, end) },
        valid,
      );
    case "i64":
      return optionalVectorValidity(
        { type: "i64", values: vector.values.subarray(start, end) },
        valid,
      );
    case "u64":
      return optionalVectorValidity(
        { type: "u64", values: vector.values.subarray(start, end) },
        valid,
      );
    case "timestamp96":
      return optionalVectorValidity(
        {
          type: "timestamp96",
          ...sliceInt96Storage(vector, start, end),
          unit: vector.unit,
          isAdjustedToUTC: vector.isAdjustedToUTC,
        },
        valid,
      );
    case "timestamp":
      return optionalVectorValidity(
        {
          type: "timestamp",
          values: vector.values.subarray(start, end),
          unit: vector.unit,
          isAdjustedToUTC: vector.isAdjustedToUTC,
        },
        valid,
      );
    case "bool":
      return optionalVectorValidity(
        { type: "bool", values: vector.values.subarray(start, end) },
        valid,
      );
    case "decimal":
      return optionalVectorValidity(
        { type: "decimal", values: vector.values.slice(start, end) },
        valid,
      );
    case "utf8":
      return optionalVectorValidity(
        { type: "utf8", values: vector.values.slice(start, end) },
        valid,
      );
    case "binary":
      return optionalVectorValidity(
        { type: "binary", values: vector.values.slice(start, end) },
        valid,
      );
    case "dict":
      return optionalVectorValidity(
        {
          type: "dict",
          indices: vector.indices.subarray(start, end),
          dictionary: vector.dictionary,
        },
        valid,
      );
    case "list":
      return optionalVectorValidity(
        { type: "list", offsets: vector.offsets.subarray(start, end + 1), child: vector.child },
        valid,
      );
    case "map":
      return optionalVectorValidity(
        {
          type: "map",
          offsets: vector.offsets.subarray(start, end + 1),
          keys: vector.keys,
          values: vector.values,
        },
        valid,
      );
    case "struct": {
      const fields: Record<string, Vector> = {};
      for (const name in vector.fields)
        if (Object.hasOwn(vector.fields, name))
          fields[name] = sliceVector(vector.fields[name] as Vector, start, end);
      return optionalVectorValidity({ type: "struct", fields, length: end - start }, valid);
    }
  }
}

function optionalVectorValidity<T extends Vector>(vector: T, valid: Uint8Array | undefined): T {
  if (valid !== undefined) (vector as T & { valid: Uint8Array }).valid = valid;
  return vector;
}

/** Slice a contiguous row interval without constructing gather indices. */
export function sliceBatch(batch: Batch, start: number, end: number, scope?: MemoryScope): Batch {
  if (
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(end) ||
    start < 0 ||
    end < start ||
    end > batch.rowCount
  ) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Batch slice is out of bounds", { start, end });
  }
  if (start === 0 && end === batch.rowCount) return batch;
  if (scope) {
    let bytes = c.object + 2 * c.property + c.object;
    for (const name in batch.columns)
      if (Object.hasOwn(batch.columns, name))
        bytes += c.property + sliceVectorBytes(batch.columns[name] as Vector, end - start);
    return scope.allocate(bytes, "batch slice metadata", () => sliceBatch(batch, start, end)).value;
  }
  const columns: Record<string, Vector> = {};
  for (const name in batch.columns)
    if (Object.hasOwn(batch.columns, name))
      columns[name] = sliceVector(batch.columns[name] as Vector, start, end);
  return { rowCount: end - start, columns };
}

/** Forecast only new records, typed views, and copied reference arrays. Payload
 * and nested list/map/dictionary children remain borrowed from the input owner.
 * Counting all input properties conservatively covers optional shape metadata.
 */
function sliceVectorBytes(vector: Vector, length: number): number {
  if (length === vectorLength(vector)) return 0;
  let bytes: number = c.object;
  for (const key in vector) if (Object.hasOwn(vector, key)) bytes += c.property;
  if ("valid" in vector && vector.valid !== undefined) bytes += c.view;
  switch (vector.type) {
    case "null":
      return bytes;
    case "binary-view":
    case "utf8-view":
      return bytes + (vector.rowOffset === undefined ? c.property : 0);
    case "decimal":
    case "utf8":
    case "binary":
      return bytes + c.array + length * c.arraySlot;
    case "union":
      return bytes + 2 * c.view;
    case "timestamp96":
      return bytes + 2 * c.view + c.object + 2 * c.property;
    case "struct":
      bytes += c.object;
      for (const name in vector.fields)
        if (Object.hasOwn(vector.fields, name))
          bytes += c.property + sliceVectorBytes(vector.fields[name] as Vector, length);
      return bytes;
    default:
      return bytes + c.view;
  }
}
