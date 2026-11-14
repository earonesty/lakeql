import { isLakeqlError, LakeqlError } from "./errors.js";

const reservationState = Symbol("reservation state");

/** Synchronous temporary allocation admission. Callers release reservations before
 * returning; owning scopes and query budgets implement the same contract. */
export interface MemoryReservationSource {
  reserve(bytes: number, purpose: string): MemoryReservation;
}

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
    return new MemoryBudget.Reservation(this, allocation);
  }

  /** Share method implementations instead of allocating closures per handle. */
  private static readonly Reservation = class implements MemoryReservation {
    #released = false;
    readonly #allocation: Allocation;
    bytes: number;

    constructor(
      readonly budget: MemoryBudget,
      allocation: Allocation,
    ) {
      this.#allocation = allocation;
      this.bytes = allocation.bytes;
    }

    private requireLive(): void {
      if (this.#released)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Cannot retain released memory");
    }

    [reservationState](consume = false): Allocation {
      this.requireLive();
      if (consume) this.#released = true;
      return this.#allocation;
    }
    merge(other: MemoryReservation): MemoryReservation {
      this.requireLive();
      const state = (
        other as MemoryReservation & { [reservationState]?: (consume?: boolean) => Allocation }
      )[reservationState];
      if (state === undefined || other.budget !== this.budget)
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "Merging requires reservations from one budget",
        );
      const second = state.call(other);
      if (second === this.#allocation)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Cannot merge a reservation with itself");
      for (const ledger of [this.#allocation, second]) {
        if (
          ledger.holders.size !== this.budget.chain.length ||
          this.budget.chain.some((budget) => ledger.holders.get(budget) !== 1)
        )
          throw new LakeqlError(
            "LAKEQL_VALIDATION_ERROR",
            "Merging requires exclusive reservation ownership",
          );
      }
      const total = this.#allocation.bytes + second.bytes;
      validateBytes(total);
      // Transfer installed charges. No pool release, readmission, or reclamation.
      state.call(other, true);
      this.#released = true;
      this.#allocation.bytes = total;
      return this.budget.handle(this.#allocation);
    }
    shrink(bytes: number): void {
      this.requireLive();
      validateBytes(bytes);
      if (bytes > this.#allocation.bytes)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Cannot grow a reservation by shrinking");
      if (
        this.#allocation.holders.size !== this.budget.chain.length ||
        this.budget.chain.some((budget) => this.#allocation.holders.get(budget) !== 1)
      )
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "Shrinking requires exclusive reservation ownership",
        );
      const releasedBytes = this.#allocation.bytes - bytes;
      this.#allocation.bytes = bytes;
      this.bytes = bytes;
      for (const budget of this.budget.chain) budget.used -= releasedBytes;
    }
    retain(): MemoryReservation {
      this.requireLive();
      return this.budget.pin(this.#allocation);
    }
    retainIn(budget: MemoryBudget): MemoryReservation {
      this.requireLive();
      // This budget already holds the allocation: no admission or reclaimer
      // can run, so an additional temporary source guard is unnecessary.
      if (budget === this.budget) return this.budget.pin(this.#allocation);
      // Reclaimers may release the source handle during target admission.
      // Keep the physical allocation charged until admission has settled.
      const guard = this.budget.pin(this.#allocation);
      try {
        return budget.pin(this.#allocation);
      } finally {
        guard.release();
      }
    }
    partition(parts: readonly number[]): MemoryReservation[] {
      this.requireLive();
      let total = 0;
      for (const bytes of parts) {
        validateBytes(bytes);
        total += bytes;
        validateBytes(total);
      }
      if (total !== this.#allocation.bytes) {
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "Reservation partitions must cover the complete reservation",
        );
      }
      if (
        this.#allocation.holders.size !== this.budget.chain.length ||
        this.budget.chain.some((budget) => this.#allocation.holders.get(budget) !== 1)
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
          this.#allocation.bytes = bytes;
          return this.budget.handle(this.#allocation);
        }
        return this.budget.handle({
          bytes,
          purpose: this.#allocation.purpose,
          root: this.#allocation.root,
          holders: new Map(this.#allocation.holders),
        });
      });
      this.#released = true;
      return result;
    }
    release(): void {
      if (this.#released) return;
      this.#released = true;
      for (const budget of this.budget.chain) {
        const remaining = (this.#allocation.holders.get(budget) ?? 1) - 1;
        if (remaining === 0) {
          this.#allocation.holders.delete(budget);
          budget.used -= this.#allocation.bytes;
        } else this.#allocation.holders.set(budget, remaining);
      }
    }
  };

  static {
    // Preserve detachable callback semantics while allocating bound functions
    // only for methods actually used by a handle. Implementations remain shared.
    const prototype = MemoryBudget.Reservation.prototype;
    for (const name of ["retain", "retainIn", "partition", "merge", "shrink", "release"] as const) {
      const method = prototype[name];
      Object.defineProperty(prototype, name, {
        configurable: true,
        get(this: InstanceType<typeof MemoryBudget.Reservation>) {
          const bound = method.bind(this);
          if (Object.isExtensible(this))
            Object.defineProperty(this, name, {
              value: bound,
              configurable: true,
              writable: true,
              enumerable: true,
            });
          return bound;
        },
        set(this: InstanceType<typeof MemoryBudget.Reservation>, value: unknown) {
          Object.defineProperty(this, name, {
            value,
            configurable: true,
            writable: true,
            enumerable: true,
          });
        },
      });
    }
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
  /** Reduce an exclusively owned extent after dropping the released storage.
   * Keeps this handle live; cannot grow, reclaim, or change shared ownership. */
  shrink(bytes: number): void;
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
