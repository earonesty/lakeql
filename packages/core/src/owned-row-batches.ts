import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import type { Row } from "./types.js";

/** Consume an owned collection and lend bounded row batches until advancement or
 * iterator closure. Payload remains charged through every yield; only reference
 * arrays are copied. A single complete batch reuses the admitted input array.
 */
export function* borrowOwnedRowBatches(
  input: OwnedMemory<Row[]>,
  batchSize: number,
  signal?: AbortSignal,
): Generator<Row[], void, unknown> {
  const scope = new MemoryScope(input.budget, signal ? [signal] : []);
  const check = () => {
    if (signal?.aborted) throw new LakeqlError("LAKEQL_ABORTED", "Owned row batching aborted");
  };
  try {
    check();
    if (!Number.isSafeInteger(batchSize) || batchSize <= 0)
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "batchSize must be a positive safe integer");
    scope.adopt(input);
    const rows = input.value;
    for (let start = 0; start < rows.length; start += batchSize) {
      check();
      const batch = scope.childScope();
      try {
        const end = Math.min(rows.length, start + batchSize);
        yield start === 0 && end === rows.length
          ? rows
          : batch.allocate(c.array + (end - start) * c.arraySlot, "borrowed output batch", () =>
              rows.slice(start, end),
            ).value;
        check();
      } finally {
        batch.dispose();
      }
    }
  } finally {
    scope.dispose();
    // Covers admission/validation failure before the input could be adopted.
    input.release();
  }
}
