import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { MemoryReservation } from "./memory-budget.js";
import type { MemoryScope } from "./memory-scope.js";

/** Admit traversal capacity once per depth. Sibling containers reuse admission;
 * keeping the high-water charge until disposal also covers retained stack slots.
 */
export class JsonTraversalBudget {
  private readonly reservations: MemoryReservation[] = [];
  private base: MemoryReservation | undefined;

  constructor(
    private readonly scope: MemoryScope | undefined,
    private readonly frameBytes: number,
    private readonly baseBytes: number,
  ) {}

  admit(depth: number): void {
    if (this.scope === undefined || depth <= this.reservations.length) return;
    this.base ??= this.scope.reserve(this.baseBytes + c.array, "JSON traversal capacity");
    while (this.reservations.length < depth) {
      this.reservations.push(
        this.scope.reserve(this.frameBytes + c.arraySlot, "JSON traversal depth"),
      );
    }
  }

  dispose(): void {
    for (const reservation of this.reservations) reservation.release();
    this.reservations.length = 0;
    this.base?.release();
    this.base = undefined;
  }
}
