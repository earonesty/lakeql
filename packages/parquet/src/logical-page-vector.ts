import type { DecodedArray } from "hyparquet/src/types.js";
import { LakeqlError, type MemoryScope, type Vector, vectorFromSource } from "lakeql-core";

/** Replayable expansion of flat logical values without a row-sized staging array.
 * Child construction reads grouped indices in order and rewinds on each pass.
 */
export function logicalPageVector(
  values: DecodedArray,
  levels: ArrayLike<number> | undefined,
  count: number,
  dictionary: DecodedArray | undefined,
  scope?: MemoryScope,
): Vector {
  if (
    !Number.isSafeInteger(count) ||
    count < 0 ||
    (levels !== undefined && levels.length !== count)
  )
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid logical page extent");
  let present = count;
  if (levels !== undefined) {
    present = 0;
    for (let index = 0; index < count; index++) {
      if (levels[index] !== 0 && levels[index] !== 1)
        throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid logical page presence");
      if (levels[index] === 1) present++;
    }
  }
  if (present !== values.length)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Logical page value count mismatch");
  let row = 0;
  let valueIndex = 0;
  return vectorFromSource(
    {
      length: count,
      get(index) {
        if (index < row) {
          row = 0;
          valueIndex = 0;
        }
        while (row < index) {
          if (levels === undefined || levels[row] !== 0) valueIndex++;
          row++;
        }
        if (levels !== undefined && levels[index] === 0) return null;
        const value = values[valueIndex];
        if (dictionary === undefined) return value;
        if (
          typeof value !== "number" ||
          !Number.isSafeInteger(value) ||
          value < 0 ||
          value >= dictionary.length
        )
          throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid logical dictionary index");
        return dictionary[value];
      },
    },
    scope,
  );
}
