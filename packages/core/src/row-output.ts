import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { MemoryScope } from "./memory-scope.js";
import { detachOwnedRow } from "./owned-row-detachment.js";
import type { Row } from "./types.js";

/** Shared projection handoff for borrowed and retained row producers. */
export function appendProjectedOutput(
  output: Row[],
  projected: Row,
  projection: MemoryScope,
  target: MemoryScope,
  owned: boolean,
  retainedSource?: MemoryScope,
): void {
  let value = projected;
  if (owned) {
    const owner =
      retainedSource?.tryRetainAllocation(projected) ??
      detachOwnedRow(projected, target.budget, projection);
    try {
      value = target.adopt(owner).value;
    } catch (error) {
      owner.release();
      throw error;
    }
  } else target.adopt(projection.retainAllocations(undefined));
  target.allocate(c.arraySlot, "row output slot", () => output.push(value));
}
