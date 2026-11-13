import type { DecodedArray } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";
import { parseAdmittedJson } from "./json-allocation.js";

const decoder = new TextDecoder();

/** Admit text before decoding and each parsed tree before construction.
 * Failed pages release already parsed trees; successful pages retain only output.
 */
export function convertJsonValues(values: DecodedArray, scope: MemoryScope): DecodedArray {
  const construction = scope.childScope();
  try {
    const output = construction.allocate(
      c.array + values.length * c.arraySlot,
      "JSON logical output array",
      () => new Array<unknown>(values.length),
    ).value;
    for (let index = 0; index < values.length; index++) {
      const value = values[index];
      if (!(value instanceof Uint8Array))
        throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid JSON physical value");
      const scratch = construction.childScope();
      try {
        const text = scratch.allocate(
          c.string + value.byteLength * c.stringCodeUnit,
          "JSON decoded text",
          () => decoder.decode(value),
        ).value;
        output[index] = parseAdmittedJson(text, construction);
      } finally {
        scratch.dispose();
      }
    }
    scope.adopt(construction.retainAllocations(output));
    return output as DecodedArray;
  } finally {
    construction.dispose();
  }
}
