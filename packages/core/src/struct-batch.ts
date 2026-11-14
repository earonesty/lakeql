import { type Batch, type Vector, vectorLength } from "./batch.js";
import { LakeqlError } from "./errors.js";
import { MATERIALIZATION_CHARGES as c } from "./materialization-plan.js";
import type { MemoryScope } from "./memory-scope.js";

/** Borrow the columns of an all-present struct as a batch. Parent nullability
 * cannot be discarded: callers must propagate it into fields before conversion.
 */
export function batchFromStructVector(vector: Vector, scope: MemoryScope): Batch {
  const invalid = () =>
    new LakeqlError("LAKEQL_TYPE_ERROR", "Batch conversion requires an all-present struct");
  if (vector.type !== "struct" || !Number.isSafeInteger(vector.length) || vector.length < 0)
    throw invalid();
  if (vector.valid) {
    if (vector.valid.length !== vector.length) throw invalid();
    for (const valid of vector.valid) if (valid !== 1) throw invalid();
  }
  for (const name in vector.fields) {
    if (!Object.hasOwn(vector.fields, name)) continue;
    const field = vector.fields[name];
    if (!field || vectorLength(field) !== vector.length) throw invalid();
  }
  return scope.allocate(
    c.object + 2 * c.property,
    "struct batch descriptor",
    (): Batch => ({ rowCount: vector.length, columns: vector.fields }),
  ).value;
}
