import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";
import { detachOwnedRow } from "./owned-row-detachment.js";
import type { Row } from "./types.js";

/** One retained value. Admit old/new overlap before replacement; failed copies
 * preserve the prior owner. Mutable scanner data is detached before next yield.
 */
export class OwnedReplacementValue {
  private owner: OwnedMemory<Row> | undefined;
  constructor(private readonly scope: MemoryScope) {
    scope.signal.addEventListener(
      "abort",
      () => {
        this.owner?.release();
        this.owner = undefined;
      },
      { once: true },
    );
  }

  replace<T>(value: T): T {
    if (this.scope.disposed) throw new LakeqlError("LAKEQL_ABORTED", "Value owner disposed");
    const scratch = this.scope.reserve(c.object + c.property, "replacement input wrapper");
    try {
      const next = detachOwnedRow({ value }, this.scope.budget);
      const previous = this.owner;
      this.owner = next;
      previous?.release();
      return next.value.value as T;
    } finally {
      scratch.release();
    }
  }
}
