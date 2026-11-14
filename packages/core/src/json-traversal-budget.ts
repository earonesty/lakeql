import type { MemoryReservation } from "./memory-budget.js";
import { reserveMemoryCapacity } from "./memory-capacity.js";
import type { MemoryScope } from "./memory-scope.js";

/** Admit traversal capacity once per depth. Sibling containers reuse admission;
 * keeping the high-water charge until disposal also covers retained stack slots.
 */
export class JsonTraversalBudget {
  private reservation: MemoryReservation | undefined;
  private depth = 0;

  constructor(
    private readonly scope: MemoryScope | undefined,
    private readonly frameBytes: number,
    private readonly baseBytes: number,
  ) {}

  admit(depth: number): void {
    if (this.scope === undefined || depth <= this.depth) return;
    this.reservation = reserveMemoryCapacity(
      this.scope,
      this.reservation,
      this.baseBytes + depth * this.frameBytes,
      "JSON traversal capacity",
    );
    this.depth = depth;
  }

  dispose(): void {
    this.reservation?.release();
    this.reservation = undefined;
    this.depth = 0;
  }
}
