import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { scalarVectorValue, type Vector } from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { Scalar } from "./expr.js";
import type { MemoryReservation } from "./memory-budget.js";
import type { MemoryScope } from "./memory-scope.js";
import type { VectorAllocator } from "./vector-buffer.js";

export const SCALAR_READ_WORKSPACE_BYTES = c.object + 7 * c.property;

/** Reusable storage admission for two synchronous borrowed scalar reads. Each
 * scalar leaf admits one payload; dictionary/union reads delegate to that leaf.
 * Grow by a delta while the first scalar is live, merging exclusive admission
 * without releasing capacity or spending an admitted stage allowance twice. */
export class ScalarReadWorkspace implements VectorAllocator {
  private backing: MemoryReservation | undefined;
  private capacity = 0;
  private used = 0;
  private active = false;
  private closed = false;

  private constructor(
    private readonly scope: MemoryScope,
    private readonly metadata: MemoryReservation,
  ) {}

  static create(scope: MemoryScope): ScalarReadWorkspace {
    const metadata = scope.reserve(SCALAR_READ_WORKSPACE_BYTES, "scalar comparison metadata");
    try {
      return new ScalarReadWorkspace(scope, metadata);
    } catch (error) {
      metadata.release();
      throw error;
    }
  }

  consume(
    vector: Vector,
    left: number,
    right: number,
    compare: (left: Scalar, right: Scalar) => number,
  ): number {
    this.requireOpen();
    if (this.active)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Scalar reads cannot reenter");
    this.active = true;
    this.used = 0;
    try {
      const first = scalarVectorValue(vector, left, this);
      // One immutable decoded text can serve both operands when their physical
      // dictionary entry and outer null state match. Still call the comparator:
      // equality does not bypass its validation or tie-breaking contract.
      const sameText =
        vector.type === "dict" &&
        vector.dictionary.type === "utf8-view" &&
        right >= 0 &&
        right < vector.indices.length &&
        vector.indices[left] === vector.indices[right] &&
        (vector.valid?.[left] === 0) === (vector.valid?.[right] === 0);
      return compare(first, sameText ? first : scalarVectorValue(vector, right, this));
    } catch (error) {
      this.release();
      throw error;
    } finally {
      this.active = false;
      this.used = 0;
      if (this.closed || this.scope.disposed) {
        this.release();
        this.metadata.release();
      }
    }
  }

  allocate<T>(bytes: number, _purpose: string, allocate: () => T): { value: T } {
    this.requireOpen();
    if (!this.active)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Scalar read outside comparison");
    const needed = this.used + bytes;
    if (!Number.isSafeInteger(bytes) || bytes < 0 || !Number.isSafeInteger(needed))
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid scalar read extent");
    if (needed > this.capacity) {
      const extra = this.scope.reserve(needed - this.capacity, "scalar comparison growth");
      try {
        this.backing = this.backing === undefined ? extra : this.backing.merge(extra);
      } catch (error) {
        extra.release();
        throw error;
      }
      this.capacity = needed;
    }
    this.used = needed;
    const value = allocate();
    this.requireOpen();
    return { value };
  }

  dispose(): void {
    this.closed = true;
    if (!this.active) {
      this.release();
      this.metadata.release();
    }
  }

  private requireOpen(): void {
    if (this.closed || this.scope.disposed) {
      if (!this.active) {
        this.release();
        this.metadata.release();
      }
      throw new LakeqlError("LAKEQL_ABORTED", "Scalar read workspace has been disposed");
    }
  }

  private release(): void {
    this.backing?.release();
    this.backing = undefined;
    this.capacity = 0;
  }
}
