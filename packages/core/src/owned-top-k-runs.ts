import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Batch } from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";

/** Adjacent stable top-k runs. The reducer borrows inputs and returns an independent
 * owner containing at most k rows, with older input winning ties. After append,
 * run sizes decrease by more than a factor of two: retained rows are below 2k.
 * Merges preserve encounter order, including unequal and partially filled batches.
 * Failed reductions close the accumulator; caller-owned inputs remain valid.
 */
export class OwnedTopKRuns {
  private readonly scope: MemoryScope;
  private readonly runs: OwnedMemory<Batch>[];
  private closed = false;

  constructor(
    parent: MemoryScope,
    private readonly limit: number,
    private readonly reduce: (
      older: OwnedMemory<Batch>,
      newer: OwnedMemory<Batch>,
    ) => OwnedMemory<Batch>,
    private readonly eager = false,
  ) {
    if (!Number.isSafeInteger(limit) || limit < 0)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid top-k run limit");
    this.scope = parent.childScope();
    try {
      // One incoming run plus the geometric stack. Capacity is independent of
      // input length; even MAX_SAFE_INTEGER needs fewer than 55 slots.
      const slots = 2 + Math.ceil(Math.log2(Math.max(1, limit)));
      this.runs = this.scope.allocate(
        c.object + 6 * c.property + c.array + slots * c.arraySlot,
        "top-k run directory",
        () => [] as OwnedMemory<Batch>[],
      ).value;
    } catch (error) {
      this.scope.dispose();
      throw error;
    }
  }

  append(input: OwnedMemory<Batch>): void {
    this.requireOpen();
    let carry: OwnedMemory<Batch> | undefined;
    try {
      this.validate(input);
      if (input.value.rowCount === 0) return;
      carry = this.own(input.retainIn(this.scope.budget));
      while (this.runs.length > 0) {
        const previous = this.runs[this.runs.length - 1] as OwnedMemory<Batch>;
        if (!this.eager && previous.value.rowCount > 2 * carry.value.rowCount) break;
        const merged = this.own(this.reduce(previous, carry));
        try {
          this.validate(merged);
          this.requireOpen();
        } catch (error) {
          merged.release();
          throw error;
        }
        this.runs.pop();
        previous.release();
        carry.release();
        carry = merged;
      }
      this.requireOpen();
      this.runs.push(carry);
      carry = undefined;
    } catch (error) {
      this.dispose();
      throw error;
    } finally {
      carry?.release();
    }
  }

  /** Eager mode exposes the exact prefix winners for legacy per-prefix checks. */
  get prefix(): Batch | undefined {
    this.requireOpen();
    if (!this.eager)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Prefix requires eager top-k runs");
    return this.runs[0]?.value;
  }

  finish(): OwnedMemory<Batch> | undefined {
    this.requireOpen();
    let output = this.runs.pop();
    try {
      while (output !== undefined && this.runs.length > 0) {
        const previous = this.runs[this.runs.length - 1] as OwnedMemory<Batch>;
        const next = this.own(this.reduce(previous, output));
        try {
          this.validate(next);
          this.requireOpen();
        } catch (error) {
          next.release();
          throw error;
        }
        this.runs.pop();
        previous.release();
        output.release();
        output = next;
      }
      const result = output?.retain();
      output?.release();
      output = undefined;
      return result;
    } finally {
      output?.release();
      this.dispose();
    }
  }

  dispose(): void {
    if (this.closed) return;
    this.closed = true;
    for (const run of this.runs) run.release();
    this.runs.length = 0;
    this.scope.dispose();
  }

  private own(input: OwnedMemory<Batch>): OwnedMemory<Batch> {
    try {
      return this.scope.adopt(input);
    } catch (error) {
      input.release();
      throw error;
    }
  }

  private validate(input: OwnedMemory<Batch>): void {
    const rows = input.value.rowCount;
    if (!Number.isSafeInteger(rows) || rows < 0 || rows > this.limit)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid top-k run extent");
  }

  private requireOpen(): void {
    if (this.closed || this.scope.disposed)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Top-k runs are disposed");
  }
}
