import { isLakeqlError, LakeqlError } from "./errors.js";

const reservationState = Symbol("reservation state");

/** Allocation admission, separate from serialized operator-size limits. */
export class MemoryBudget {
  private readonly chain: readonly MemoryBudget[];
  private used = 0;
  private peak = 0;
  private readonly reclaimers = new Set<(bytesNeeded: number) => void>();
  private reclaiming = false;

  constructor(
    readonly maxBytes: number,
    private readonly parent?: MemoryBudget,
  ) {
    validateBytes(maxBytes);
    this.chain = parent === undefined ? [this] : [this, ...parent.chain];
  }

  get usedBytes(): number {
    return this.used;
  }

  get peakBytes(): number {
    return this.peak;
  }

  get availableBytes(): number {
    return Math.min(this.maxBytes - this.used, this.parent?.availableBytes ?? Infinity);
  }

  /** A child charges its parent as well; sibling jobs compete for the same pool. */
  child(maxBytes = this.maxBytes): MemoryBudget {
    return new MemoryBudget(maxBytes, this);
  }

  /** Register synchronous eviction of reusable memory. Returns an idempotent detach. */
  registerReclaimer(reclaim: (bytesNeeded: number) => void): () => void {
    this.reclaimers.add(reclaim);
    // Reusable child allocations also occupy every ancestor pool. A sibling
    // request must be able to reclaim them even when this child has headroom.
    const detachParent = this.parent?.registerReclaimer(reclaim);
    return () => {
      this.reclaimers.delete(reclaim);
      detachParent?.();
    };
  }

  /** Reserve before allocating. Reclaims caches before rejecting admission. */
  reserve(bytes: number, purpose: string): MemoryReservation {
    validateBytes(bytes);
    return this.pin({ bytes, purpose, root: this.root(), holders: new Map() });
  }

  /** Optional optimizations may decline admission without weakening mandatory limits. */
  tryReserve(bytes: number, purpose: string): MemoryReservation | undefined {
    try {
      return this.reserve(bytes, purpose);
    } catch (error) {
      if (isLakeqlError(error) && error.code === "LAKEQL_BUDGET_EXCEEDED") return undefined;
      throw error;
    }
  }

  /** Shared physical-allocation pool; child limits still apply to each holder. */
  get allocationPool(): MemoryBudget {
    return this.root();
  }

  sharesPoolWith(other: MemoryBudget): boolean {
    return this.root() === other.root();
  }

  private root(): MemoryBudget {
    return this.chain[this.chain.length - 1] ?? this;
  }

  private pin(allocation: Allocation): MemoryReservation {
    if (allocation.root !== this.root()) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Allocation ownership requires a shared root budget",
      );
    }
    // Budget ancestry is immutable; share it across all allocation handles.
    const chain = this.chain;
    // Reject impossible local capacity before any cache eviction.
    for (const budget of chain) {
      if (!allocation.holders.has(budget) && allocation.bytes > budget.maxBytes)
        budget.exceeded(allocation.bytes, allocation.purpose);
    }
    for (const budget of chain) {
      if (!allocation.holders.has(budget)) budget.reclaimLocal(allocation.bytes);
    }
    for (const budget of chain) {
      if (!allocation.holders.has(budget) && allocation.bytes > budget.maxBytes - budget.used)
        budget.exceeded(allocation.bytes, allocation.purpose);
    }
    for (const budget of chain) {
      const holders = allocation.holders.get(budget) ?? 0;
      if (holders === 0) {
        budget.used += allocation.bytes;
        budget.peak = Math.max(budget.peak, budget.used);
      }
      allocation.holders.set(budget, holders + 1);
    }
    return this.handle(allocation);
  }

  /** Construct ownership for charges already installed in every ancestor. */
  private handle(allocation: Allocation): MemoryReservation {
    const chain = this.chain;
    let released = false;
    const requireLive = () => {
      if (released)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Cannot retain released memory");
    };
    const result: MemoryReservation & { [reservationState](consume?: boolean): Allocation } = {
      [reservationState]: (consume = false) => {
        requireLive();
        if (consume) released = true;
        return allocation;
      },
      merge: (other) => {
        requireLive();
        const state = (
          other as MemoryReservation & { [reservationState]?: (consume?: boolean) => Allocation }
        )[reservationState];
        if (state === undefined || other.budget !== this)
          throw new LakeqlError(
            "LAKEQL_VALIDATION_ERROR",
            "Merging requires reservations from one budget",
          );
        const second = state();
        if (second === allocation)
          throw new LakeqlError(
            "LAKEQL_VALIDATION_ERROR",
            "Cannot merge a reservation with itself",
          );
        for (const ledger of [allocation, second]) {
          if (
            ledger.holders.size !== chain.length ||
            chain.some((budget) => ledger.holders.get(budget) !== 1)
          )
            throw new LakeqlError(
              "LAKEQL_VALIDATION_ERROR",
              "Merging requires exclusive reservation ownership",
            );
        }
        const total = allocation.bytes + second.bytes;
        validateBytes(total);
        // Transfer installed charges. No pool release, readmission, or reclamation.
        state(true);
        released = true;
        allocation.bytes = total;
        return this.handle(allocation);
      },
      bytes: allocation.bytes,
      budget: this,
      retain: () => {
        requireLive();
        return this.pin(allocation);
      },
      retainIn: (budget) => {
        requireLive();
        // This budget already holds the allocation: no admission or reclaimer
        // can run, so an additional temporary source guard is unnecessary.
        if (budget === this) return this.pin(allocation);
        // Reclaimers may release the source handle during target admission.
        // Keep the physical allocation charged until admission has settled.
        const guard = this.pin(allocation);
        try {
          return budget.pin(allocation);
        } finally {
          guard.release();
        }
      },
      partition: (parts) => {
        requireLive();
        let total = 0;
        for (const bytes of parts) {
          validateBytes(bytes);
          total += bytes;
          validateBytes(total);
        }
        if (total !== allocation.bytes) {
          throw new LakeqlError(
            "LAKEQL_VALIDATION_ERROR",
            "Reservation partitions must cover the complete reservation",
          );
        }
        if (
          allocation.holders.size !== chain.length ||
          chain.some((budget) => allocation.holders.get(budget) !== 1)
        ) {
          throw new LakeqlError(
            "LAKEQL_VALIDATION_ERROR",
            "Partitioning requires exclusive reservation ownership",
          );
        }
        // Transfer existing charges, without releasing/re-admitting capacity or
        // invoking cache reclamation. Shared physical allocations cannot split:
        // another holder would otherwise retain an obsolete extent.
        const result = parts.map((bytes, index) => {
          // Exclusive transfer can reuse the source ledger for the final extent.
          // Earlier extents receive independent ledgers before it is resized.
          if (index === parts.length - 1) {
            allocation.bytes = bytes;
            return this.handle(allocation);
          }
          return this.handle({
            bytes,
            purpose: allocation.purpose,
            root: allocation.root,
            holders: new Map(allocation.holders),
          });
        });
        released = true;
        return result;
      },
      release: () => {
        if (released) return;
        released = true;
        for (const budget of chain) {
          const remaining = (allocation.holders.get(budget) ?? 1) - 1;
          if (remaining === 0) {
            allocation.holders.delete(budget);
            budget.used -= allocation.bytes;
          } else allocation.holders.set(budget, remaining);
        }
      },
    };
    return result;
  }

  private reclaimLocal(bytes: number): void {
    if (!this.reclaiming && bytes > this.maxBytes - this.used) {
      this.reclaiming = true;
      try {
        for (const reclaim of this.reclaimers) {
          const needed = bytes - (this.maxBytes - this.used);
          if (needed <= 0) break;
          reclaim(needed);
        }
      } finally {
        this.reclaiming = false;
      }
    }
  }

  private exceeded(bytes: number, purpose: string): never {
    throw new LakeqlError("LAKEQL_BUDGET_EXCEEDED", "Allocation exceeds memory budget", {
      metric: "allocated memory bytes",
      limit: this.maxBytes,
      actual: this.used + bytes,
      purpose,
    });
  }
}

export interface MemoryReservation {
  readonly bytes: number;
  readonly budget: MemoryBudget;
  /** Pin the same allocation without charging its backing storage twice. */
  retain(): MemoryReservation;
  /** Charge another budget in the same tree while counting common ancestors once. */
  retainIn(budget: MemoryBudget): MemoryReservation;
  /** Consume an exclusive reservation into independently releasable extents.
   * Parts must sum exactly to bytes. This transfers admission, not allocation data.
   * On failure the original reservation remains live and unchanged.
   */
  partition(bytes: readonly number[]): MemoryReservation[];
  /** Combine two exclusive extents from the same budget without readmission.
   * On success both original handles are consumed; failure preserves them. */
  merge(other: MemoryReservation): MemoryReservation;
  /** Idempotent; the allocation remains charged while another holder retains it. */
  release(): void;
}

interface Allocation {
  bytes: number;
  purpose: string;
  root: MemoryBudget;
  holders: Map<MemoryBudget, number>;
}

function validateBytes(bytes: number): void {
  if (!Number.isSafeInteger(bytes) || bytes < 0) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Memory bytes must be a non-negative safe integer",
      {
        bytes,
      },
    );
  }
}
