import type { PackedByteVector } from "./batch.js";
import { LakeqlError } from "./errors.js";

/** Locate a packed row without allocating a per-row range descriptor. */
export function packedByteStart(vector: PackedByteVector, index: number): number {
  const row = (vector.rowOffset ?? 0) + index;
  return vector.ranges === undefined
    ? row * (vector.fixedLength ?? 0)
    : (vector.ranges[row * 2] ?? 0);
}

/** Validate before admission/copy; callers resolve nulls before reading payload. */
export function packedByteLength(vector: PackedByteVector, index: number): number {
  const row = (vector.rowOffset ?? 0) + index;
  const length =
    vector.ranges === undefined ? (vector.fixedLength ?? 0) : (vector.ranges[row * 2 + 1] ?? 0);
  const start = vector.ranges === undefined ? row * length : (vector.ranges[row * 2] ?? 0);
  if (
    !Number.isSafeInteger(row) ||
    row < 0 ||
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(length) ||
    start < 0 ||
    length < 0 ||
    start + length > vector.data.byteLength
  ) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Packed range exceeds payload");
  }
  return length;
}
