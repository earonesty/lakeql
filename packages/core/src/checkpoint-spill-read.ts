import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { MemoryScope } from "./memory-scope.js";
import type { SpillAdapter } from "./runtime.js";

export async function readCheckpointSpill(
  spill: Pick<SpillAdapter, "read" | "readOwned">,
  id: string,
  scope: MemoryScope,
): Promise<Uint8Array> {
  if (spill.readOwned === undefined) {
    // Legacy adapters allocate externally. Charge the returned buffer while it
    // is retained; only readOwned can admit the adapter's allocation beforehand.
    const bytes = await spill.read(id);
    return scope.allocate(c.view + bytes.byteLength, "legacy checkpoint read buffer", () => bytes)
      .value;
  }
  const owner = await spill.readOwned(id, scope.budget);
  try {
    // Cancellation can dispose the parent while the adapter is completing I/O.
    // Keep the independent owner until settlement, then reject before decoding.
    return scope.adopt(owner).value;
  } catch (error) {
    owner.release();
    throw error;
  }
}
