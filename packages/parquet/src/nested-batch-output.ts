import {
  batchFromStructVector,
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type MemoryScope,
  type OwnedMemory,
  type Vector,
} from "lakeql-core";
import type { ParquetColumnBatch } from "./types.js";

/** Bridge expiring internal vector leases to caller-released public batch owners.
 * No row values are materialized. The source closes on early consumer return.
 */
export async function* readNestedBatchOutputs(
  batches: AsyncIterable<OwnedMemory<Vector>>,
  scope: MemoryScope,
  rowOffset: number,
  residualPredicateSatisfied: boolean,
): AsyncGenerator<OwnedMemory<ParquetColumnBatch>> {
  if (!Number.isSafeInteger(rowOffset) || rowOffset < 0)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid nested batch row offset");
  for await (const input of batches) {
    const output = scope.childScope();
    try {
      const pin = input.retainIn(output.budget);
      try {
        output.adopt(pin);
      } catch (error) {
        pin.release();
        throw error;
      }
      const batch = batchFromStructVector(input.value, output);
      if (!Number.isSafeInteger(rowOffset + batch.rowCount))
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Nested batch row offset overflow");
      const value = output.allocate(
        c.object + 3 * c.property,
        "nested public batch",
        (): ParquetColumnBatch => ({ rowOffset, batch, residualPredicateSatisfied }),
      ).value;
      const owner = output.retainAllocations(value);
      output.dispose();
      rowOffset += batch.rowCount;
      // Public owned batches transfer their lease to the caller, unlike internal streams.
      yield owner;
    } finally {
      output.dispose();
    }
  }
}
