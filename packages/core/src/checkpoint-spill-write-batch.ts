import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import { MemoryScope } from "./memory-scope.js";
import type { SpillAdapter } from "./runtime.js";

/** Publish a complete checkpoint write set when the adapter offers atomic batches. */
export async function withCheckpointSpillBatch<T>(
  spill: SpillAdapter | undefined,
  scope: MemoryScope,
  operation: (target: SpillAdapter | undefined) => Promise<T>,
): Promise<T> {
  if (spill?.beginWriteBatch === undefined) return operation(spill);
  // Keep rollback bookkeeping charged even if the query scope is cancelled.
  const lifetime = new MemoryScope(scope.budget);
  const check = () => {
    if (scope.disposed) throw new LakeqlError("LAKEQL_ABORTED", "Checkpoint batch scope disposed");
  };
  try {
    check();
    const batch = lifetime.allocate(c.object + 3 * c.property, "checkpoint spill transaction", () =>
      spill.beginWriteBatch?.(),
    ).value;
    if (batch === undefined)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Spill batch capability changed");
    try {
      const readOwned = spill.readOwned?.bind(spill);
      const target = lifetime.allocate(
        c.object + 6 * c.property,
        "checkpoint spill transaction adapter",
        (): SpillAdapter => ({
          write: (id, data) => batch.write(id, data),
          read: (ref) => spill.read(ref),
          ...(readOwned === undefined ? {} : { readOwned }),
          delete: (ref) => spill.delete(ref),
          usage: () => spill.usage(),
        }),
      ).value;
      const result = await operation(target);
      check();
      await batch.commit();
      check();
      return result;
    } catch (error) {
      try {
        await batch.rollback();
      } catch (cleanup) {
        throw new AggregateError([error, cleanup], "Checkpoint batch and rollback failed");
      }
      throw error;
    }
  } finally {
    lifetime.dispose();
  }
}
