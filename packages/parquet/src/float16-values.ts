import { parseFloat16 } from "hyparquet/src/convert.js";
import type { DecodedArray } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";

/** Admit one logical output array without an intermediate Array.from copy. */
export function convertFloat16Values(values: DecodedArray, scope: MemoryScope): DecodedArray {
  // Validate physical carriers before admitting or constructing logical output.
  for (const value of values) {
    if (
      value !== undefined &&
      value !== null &&
      (!(value instanceof Uint8Array) || value.byteLength !== 2)
    )
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid FLOAT16 physical value");
  }
  return scope.allocate(
    c.array + values.length * (c.arraySlot + c.number),
    "converted FLOAT16 values",
    () => {
      const output = new Array<number | undefined>(values.length);
      for (let index = 0; index < values.length; index++)
        output[index] = parseFloat16(values[index] as Uint8Array | undefined);
      return output;
    },
  ).value;
}
