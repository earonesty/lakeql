import { type Batch, type Vector, vectorLength } from "./batch.js";
import { LakeqlError } from "./errors.js";
import { sliceInt96Storage } from "./int96-storage.js";

/** Slice logical rows while preserving typed payload backing and nested children.
 * Legacy JS array vectors copy their reference array; ownership of backing is
 * the caller's responsibility. Nested offsets remain relative to retained children.
 */
export function sliceVector(vector: Vector, start: number, end: number): Vector {
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
      const { valid: _valid, ...storage } = vector;
      return optionalVectorValidity(
        { ...storage, rowOffset: (vector.rowOffset ?? 0) + start, length: end - start },
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
      for (const [name, field] of Object.entries(vector.fields))
        fields[name] = sliceVector(field, start, end);
      return optionalVectorValidity({ type: "struct", fields, length: end - start }, valid);
    }
  }
}

function optionalVectorValidity<T extends Vector>(vector: T, valid: Uint8Array | undefined): T {
  return valid === undefined ? vector : { ...vector, valid };
}

/** Slice a contiguous row interval without constructing gather indices. */
export function sliceBatch(batch: Batch, start: number, end: number): Batch {
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
  const columns: Record<string, Vector> = {};
  for (const [name, vector] of Object.entries(batch.columns))
    columns[name] = sliceVector(vector, start, end);
  return { rowCount: end - start, columns };
}
