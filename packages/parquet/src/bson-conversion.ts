import type { DecodedArray } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES as c, LakeqlError, MemoryBudget, MemoryScope } from "lakeql-core";
import { decodeBsonValue } from "./bson-values.js";

export function bsonFromBytes(value: unknown, scope?: MemoryScope): Map<string, unknown> {
  if (!(value instanceof Uint8Array))
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid BSON physical value");
  if (scope !== undefined) return decodeBsonValue(value, scope);
  // Public borrowed readers transfer returned JS values to their caller. Their
  // traversal still uses the same admitted, nonrecursive decoder contract.
  const temporary = new MemoryScope(new MemoryBudget(Number.MAX_SAFE_INTEGER));
  try {
    return decodeBsonValue(value, temporary);
  } finally {
    temporary.dispose();
  }
}

export function convertBsonValues(values: DecodedArray, parent?: MemoryScope): DecodedArray {
  if (parent === undefined)
    return Array.from(values, (value) => bsonFromBytes(value)) as DecodedArray;
  const scope = parent.childScope();
  try {
    const output = scope.allocate(
      c.array + values.length * c.arraySlot,
      "BSON logical output array",
      () => new Array<unknown>(values.length),
    ).value;
    for (let index = 0; index < values.length; index++)
      output[index] = bsonFromBytes(values[index], scope);
    const owner = scope.retainAllocations(output);
    try {
      parent.adopt(owner);
    } catch (error) {
      owner.release();
      throw error;
    }
    return output as DecodedArray;
  } finally {
    scope.dispose();
  }
}
