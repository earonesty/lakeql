import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Batch } from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import { sliceBatch } from "./vector-slice.js";

type PositionedBatch = { rowOffset: number; batch: Batch };
type Input = PositionedBatch & { owner?: OwnedMemory<PositionedBatch> };

/** Lend bounded positioned batches without copying payload. Borrowed input must
 * remain valid until iteration ends. Owned input is retained independently for
 * each slice, so downstream owners can survive advancement or early closure.
 */
export function* boundedVectorBatches(
  input: Input,
  batchSize: number,
  budget: MemoryBudget,
  signal?: AbortSignal,
): Generator<Input, void, unknown> {
  if (!Number.isSafeInteger(batchSize) || batchSize <= 0)
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "batchSize must be a positive safe integer");
  const check = () => {
    if (signal?.aborted) throw new LakeqlError("LAKEQL_ABORTED", "Vector batching aborted");
  };
  check();
  if (input.batch.rowCount <= batchSize) {
    yield input;
    check();
    return;
  }
  for (let start = 0; start < input.batch.rowCount; start += batchSize) {
    check();
    const scope = new MemoryScope(budget, signal ? [signal] : []);
    let owner: OwnedMemory<PositionedBatch> | undefined;
    try {
      if (input.owner) {
        const retained = input.owner.retain();
        try {
          scope.adopt(retained);
        } catch (error) {
          retained.release();
          throw error;
        }
      }
      const batch = sliceBatch(
        input.batch,
        start,
        Math.min(input.batch.rowCount, start + batchSize),
        scope,
      );
      const positioned = scope.allocate(
        c.object + 2 * c.property,
        "positioned vector slice",
        () => ({ rowOffset: input.rowOffset + start, batch }),
      ).value;
      if (input.owner) {
        owner = scope.retainAllocations(positioned);
        yield { ...positioned, owner };
      } else yield positioned;
      check();
    } finally {
      owner?.release();
      scope.dispose();
    }
  }
}
