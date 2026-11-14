import { LakeqlError } from "./errors.js";
import type { MemoryScope } from "./memory-scope.js";
import { admitVectorBuffer } from "./vector-buffer.js";

export interface Int96Storage {
  readonly low: BigUint64Array;
  readonly high: Int32Array;
}

const MIN = -(1n << 95n);
const MAX = (1n << 95n) - 1n;

/** Fixed-width signed storage: one 12-byte-per-value allocation, two typed views.
 * Low/high are structure-of-arrays views to keep both properly aligned at any length.
 */
export function allocateInt96Storage(length: number, scope?: MemoryScope): Int96Storage {
  if (!Number.isSafeInteger(length) || length < 0 || !Number.isSafeInteger(length * 12))
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid signed 96-bit storage length", {
      length,
    });
  const backing = admitVectorBuffer(scope, length * 12, () => new ArrayBuffer(length * 12));
  return {
    low: new BigUint64Array(backing, 0, length),
    high: new Int32Array(backing, length * 8, length),
  };
}

export function writeInt96(storage: Int96Storage, index: number, value: bigint): void {
  validateIndex(storage, index);
  if (value < MIN || value > MAX)
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Value exceeds signed 96-bit storage capacity", {
      storageBits: 96,
      index,
    });
  storage.low[index] = BigInt.asUintN(64, value);
  storage.high[index] = Number(value >> 64n);
}

export function readInt96(storage: Int96Storage, index: number): bigint {
  validateIndex(storage, index);
  return (BigInt(storage.high[index] ?? 0) << 64n) | (storage.low[index] ?? 0n);
}

function validateIndex(storage: Int96Storage, index: number): void {
  if (
    storage.low.length !== storage.high.length ||
    !Number.isSafeInteger(index) ||
    index < 0 ||
    index >= storage.low.length
  )
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid signed 96-bit storage index", {
      index,
    });
}

/** Borrow a contiguous interval; callers retain the original backing owner. */
export function sliceInt96Storage(storage: Int96Storage, start: number, end: number): Int96Storage {
  if (
    storage.low.length !== storage.high.length ||
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(end) ||
    start < 0 ||
    end < start ||
    end > storage.low.length
  )
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid signed 96-bit storage range", {
      start,
      end,
    });
  return { low: storage.low.subarray(start, end), high: storage.high.subarray(start, end) };
}

/** Validate selection before allocating; copying words creates no bigint values. */
export function gatherInt96Storage(
  storage: Int96Storage,
  indices: ArrayLike<number>,
  scope?: MemoryScope,
): Int96Storage {
  if (!Number.isSafeInteger(indices.length) || indices.length < 0)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid signed 96-bit selection length");
  for (let i = 0; i < indices.length; i++) validateIndex(storage, indices[i] ?? NaN);
  const output = allocateInt96Storage(indices.length, scope);
  for (let i = 0; i < indices.length; i++) {
    const index = indices[i] ?? NaN;
    validateIndex(storage, index);
    output.low[i] = storage.low[index] ?? 0n;
    output.high[i] = storage.high[index] ?? 0;
  }
  return output;
}

/** One destination allocation, no row-sized intermediate selection or value array. */
export function concatInt96Storage(
  inputs: readonly Int96Storage[],
  scope?: MemoryScope,
): Int96Storage {
  let length = 0;
  for (const input of inputs) {
    if (input.low.length !== input.high.length)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Mismatched signed 96-bit storage planes");
    length += input.low.length;
    if (!Number.isSafeInteger(length))
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Invalid signed 96-bit concatenation length",
      );
  }
  const output = allocateInt96Storage(length, scope);
  let offset = 0;
  for (const input of inputs) {
    output.low.set(input.low, offset);
    output.high.set(input.high, offset);
    offset += input.low.length;
  }
  return output;
}
