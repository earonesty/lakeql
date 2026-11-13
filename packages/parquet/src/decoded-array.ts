import type { DecodedArray } from "hyparquet";
import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";

export function allocateDecodedArray<T>(length: number, scope?: MemoryScope): T[] {
  if (!Number.isSafeInteger(length) || length < 0)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid decoded array length");
  return scope === undefined
    ? new Array<T>(length)
    : scope.allocate(
        c.array + length * c.arraySlot,
        "decoded vector array",
        () => new Array<T>(length),
      ).value;
}

/** Strings already belong to the decoded page; null slots use the empty literal. */
export function decodedStringValue(value: unknown): string {
  if (value === undefined || value === null) return "";
  if (typeof value !== "string")
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Mixed decoded string values");
  return value;
}

/** Preserve existing byte views and admit newly constructed view headers. */
export function decodedBinaryValue(value: unknown, scope?: MemoryScope): Uint8Array {
  if (value instanceof Uint8Array) return value;
  const create = () => {
    if (value instanceof ArrayBuffer) return new Uint8Array(value);
    if (ArrayBuffer.isView(value))
      return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    if (value === undefined || value === null) return new Uint8Array();
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Mixed decoded binary values");
  };
  return scope === undefined
    ? create()
    : scope.allocate(c.view, "decoded binary view", create).value;
}

/** Compact only when null placeholders actually exist; preserve existing storage
 * for already-dense arrays and typed inputs. Payload values remain borrowed.
 */
export function compactDecodedValues(values: DecodedArray, scope?: MemoryScope): DecodedArray {
  if (!Array.isArray(values)) return values;
  let present = 0;
  for (const value of values) if (value !== null && value !== undefined) present++;
  if (present === values.length) return values;
  const output = allocateDecodedArray<unknown>(present, scope);
  let index = 0;
  for (const value of values) if (value !== null && value !== undefined) output[index++] = value;
  return output as DecodedArray;
}
