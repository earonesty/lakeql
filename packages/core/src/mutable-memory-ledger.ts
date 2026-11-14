import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { isLakeqlError, LakeqlError } from "./errors.js";
import type { MemoryReservation } from "./memory-budget.js";
import type { MemoryScope } from "./memory-scope.js";

// Reserve bookkeeping as well as payload. One ledger entry covers many appends.
const ledgerBytes = c.object + 32 * c.property + c.arraySlot;
const maxChunk = 64 * 1024;

/** Exclusive mutable ledger storage accounting. No mutable backing is retained out
 * of this lifetime; outputs must be independently admitted before scope closure.
 */
export class MutableMemoryLedger {
  private readonly chunks: MemoryReservation[] = [];
  private spare = 0;
  private payload = 0;
  private closed = false;
  private mutating = false;
  private readonly detach: () => void;

  constructor(private readonly scope: MemoryScope) {
    this.detach = scope.budget.registerReclaimer(() => this.trim());
    scope.signal.addEventListener("abort", () => this.dispose(), { once: true });
  }

  append(bytes: number, purpose: string, mutate: () => void): void {
    if (this.closed || this.scope.disposed)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Mutable ledger memory is disposed");
    if (this.mutating)
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Reentrant append mutation is not supported",
      );
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid append extent", { bytes });
    let added: MemoryReservation | undefined;
    let capacity = 0;
    if (bytes > this.spare) {
      capacity = Math.max(bytes - this.spare, Math.min(maxChunk, Math.max(1024, this.payload)));
      try {
        added = this.scope.reserve(capacity + ledgerBytes, purpose);
      } catch (error) {
        if (!isLakeqlError(error) || error.code !== "LAKEQL_BUDGET_EXCEEDED") throw error;
        capacity = bytes - this.spare;
        added = this.scope.reserve(capacity + ledgerBytes, purpose);
      }
      // Reclaimers can trim this allocator during reservation. Re-evaluate its
      // available payload before mutation, rather than relying on stale capacity.
      if (capacity + this.spare < bytes) {
        added.release();
        capacity = bytes - this.spare;
        added = this.scope.reserve(capacity + ledgerBytes, purpose);
      }
    }
    const previousSpare = this.spare;
    const previousPayload = this.payload;
    if (added) {
      this.chunks.push(added);
      this.payload += capacity;
      this.spare += capacity;
    }
    // Protect pending payload from synchronous reclaimers during mutation.
    this.spare -= bytes;
    this.mutating = true;
    try {
      mutate();
      if (this.closed || this.scope.disposed)
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "Mutable ledger memory closed during mutation",
        );
    } catch (error) {
      if (!this.closed) {
        if (added) {
          this.chunks.pop()?.release();
          this.payload = previousPayload;
          this.spare = previousSpare;
        } else this.spare += bytes;
      }
      throw error;
    } finally {
      this.mutating = false;
    }
  }

  /** Release logical storage after replacement. Spare remains available for
   * subsequent updates and is reclaimable without retaining obsolete payloads.
   */
  release(bytes: number): void {
    if (this.closed || this.scope.disposed || this.mutating)
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Cannot release storage in this ledger state",
      );
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > this.payload - this.spare)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid released extent", { bytes });
    this.spare += bytes;
  }

  get usedBytes(): number {
    return this.payload - this.spare;
  }

  /** Return optional headroom without releasing any committed payload. Logical
   * payload is fungible across exclusive chunks; no output retains these ledgers.
   */
  trim(): void {
    if (this.closed) return;
    while (this.spare > 0) {
      const last = this.chunks[this.chunks.length - 1];
      if (!last) throw new Error("Missing mutable ledger capacity");
      const capacity = last.bytes - ledgerBytes;
      const bytes = Math.min(capacity, this.spare);
      if (bytes === capacity) {
        this.chunks.pop();
        last.release();
      } else {
        last.shrink(last.bytes - bytes);
      }
      this.payload -= bytes;
      this.spare -= bytes;
    }
  }

  dispose(): void {
    if (this.closed) return;
    this.closed = true;
    this.detach();
    for (const chunk of this.chunks) chunk.release();
    this.chunks.length = 0;
    this.spare = this.payload = 0;
  }
}
