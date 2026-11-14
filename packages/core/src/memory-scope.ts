import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import type { MemoryBudget, MemoryReservation } from "./memory-budget.js";

/** One exclusive reservation ledger with independently scoped lifetime holders. */
class StageAdmission {
  private holders = 1;
  constructor(private remaining: MemoryReservation) {}

  get bytes(): number {
    return this.remaining.bytes;
  }

  retain(): StageAdmission {
    this.holders++;
    return this;
  }

  release(): void {
    if (--this.holders === 0) this.remaining.release();
  }

  reserve(bytes: number, purpose: string): MemoryReservation {
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid allocation extent", { bytes });
    if (bytes > this.remaining.bytes)
      throw new LakeqlError("LAKEQL_BUDGET_EXCEEDED", "Allocation exceeds admitted stage extent", {
        purpose,
        requested: bytes,
        remaining: this.remaining.bytes,
      });
    const [allocation, remainder] = this.remaining.partition([bytes, this.remaining.bytes - bytes]);
    if (allocation === undefined || remainder === undefined)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing stage admission extent");
    this.remaining = remainder;
    return allocation;
  }
}

/** Explicit ownership of a reserved allocation, independent of JavaScript GC. */
export interface OwnedMemory<T> {
  /** Access is valid only while this handle is live. Retain before releasing. */
  readonly value: T;
  readonly bytes: number;
  readonly budget: MemoryBudget;
  /** Extend ownership beyond this scope, for example into a cache or operator. */
  retain(): OwnedMemory<T>;
  /** Retain in another budget sharing the same root allocation pool. */
  retainIn(budget: MemoryBudget): OwnedMemory<T>;
  release(): void;
}

// Internal provenance is deliberately unavailable on third-party ownership handles.
// Unknown handles must remain pinned in full when selecting backing.
const allocationProvenance = new WeakMap<
  OwnedMemory<unknown>,
  readonly OwnedMemory<unknown>[] | null
>();

interface ScopedAllocation extends OwnedMemory<unknown> {
  previous: ScopedAllocation;
  next: ScopedAllocation;
}

/** A query or execution stage releases its remaining ownership on every exit path. */
export class MemoryScope {
  private readonly owned = new Map<() => void, OwnedMemory<unknown>>();
  private readonly allocationsByValue = new Map<unknown, ScopedAllocation>();
  private snapshot: CompositeBacking | undefined;
  private admission: StageAdmission | undefined;
  private allocationSource: MemoryScope | undefined;
  private closed = false;
  private controller: AbortController | undefined;

  private detachSignals: (() => void) | undefined;
  private readonly children = new Set<MemoryScope>();
  private parentScope: MemoryScope | undefined;

  constructor(
    readonly budget: MemoryBudget,
    signals: readonly AbortSignal[] = [],
  ) {
    const unique = [...new Set(signals)];
    const close = () => this.dispose();
    this.detachSignals = () => {
      for (const signal of unique) signal.removeEventListener("abort", close);
    };
    for (const signal of unique) signal.addEventListener("abort", close, { once: true });
    if (unique.some((signal) => signal.aborted)) this.dispose();
  }

  /** Aborts when the scope closes; allocations remain charged until their work settles. */
  get signal(): AbortSignal {
    if (this.controller === undefined) {
      this.controller = new AbortController();
      if (this.closed) this.controller.abort();
    }
    return this.controller.signal;
  }

  get disposed(): boolean {
    return this.closed;
  }

  /** Transfer an independently retained handle into a stage using the same budget. */
  adopt<T>(owner: OwnedMemory<T>): OwnedMemory<T> {
    this.requireOpen();
    if (owner.budget !== this.budget) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Ownership transfer requires the same memory budget",
      );
    }
    return this.trackOwner(owner);
  }

  /** Pin existing backing using this stage's input capacity instead of charging
   * both reserved input space and the pin. The returned independent owner must
   * be released by the caller, including when asynchronous work is canceled.
   */
  pinExisting<T>(owner: OwnedMemory<T>): OwnedMemory<T> {
    this.requireOpen();
    if (owner.budget.allocationPool !== this.budget.allocationPool)
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Input pin requires the same allocation pool",
      );
    if (owner.budget === this.budget) return owner.retain();
    if (this.hasAdmission && owner.bytes <= this.availableAllocationBytes) {
      // Replacing this reservation cannot increase any ancestor's charge: the
      // existing owner already pins the backing, and the released capacity was
      // charged throughout this budget's ancestry. No await separates the swap.
      this.reserve(owner.bytes, "existing stage input").release();
    }
    const pinned = owner.retainIn(this.budget);
    try {
      this.requireOpen();
      return pinned;
    } catch (error) {
      pinned.release();
      throw error;
    }
  }

  /** Keep an output's existing backing allocations alive without charging them again.
   * The caller must separately reserve any newly allocated output storage.
   * bytes is a conservative sum when distinct handles alias the same allocation.
   */
  retainBacking<T>(value: T, backing: readonly OwnedMemory<unknown>[]): OwnedMemory<T> {
    this.requireOpen();
    const owners = [...new Set(backing)];
    for (const owner of owners) {
      if (owner.budget !== this.budget) {
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "Backing ownership requires the same memory budget",
        );
      }
    }
    return this.trackOwner(compositeMemory(value, owners, this.budget));
  }

  /** Retain current completed allocations independently of this scope.
   * Conservatively preserves all possible aliases when a decoder cannot identify
   * which inputs back its output. Pending and future allocations are excluded.
   */
  retainAllocations<T>(value: T): OwnedMemory<T> {
    this.requireOpen();
    this.snapshot ??= captureCompositeBacking([...this.owned.values()], this.budget);
    this.snapshot.references++;
    return compositeHandle(value, this.snapshot);
  }

  /** Guard completed allocations in this scope and its attached descendants.
   * Detached cache/transport scopes have independent lifetimes and are excluded.
   * Pending allocations already carry their own settlement guards.
   */
  retainTreeAllocations<T>(value: T): OwnedMemory<T> {
    this.requireOpen();
    const owners: OwnedMemory<unknown>[] = [];
    try {
      owners.push(this.retainAllocations(undefined));
      for (const child of this.children) owners.push(child.retainTreeAllocations(undefined));
      return compositeMemory(value, owners, this.budget);
    } finally {
      for (const owner of owners) owner.release();
    }
  }

  /** Retain exact known backing when present; borrowed values have no owner here. */
  tryRetainAllocation<T>(value: T): OwnedMemory<T> | undefined {
    this.requireOpen();
    return this.exactAllocation(value)?.retain() as OwnedMemory<T> | undefined;
  }

  /** Retain one exact allocated value. Views/aliases require explicit backing ownership. */
  retainAllocation<T>(value: T): OwnedMemory<T> {
    const retained = this.tryRetainAllocation(value);
    if (retained !== undefined) return retained;
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Value is not an allocation owned by this scope",
    );
  }

  /** End this scope's ownership of an exact value; independent retained handles survive. */
  releaseAllocation(value: unknown): void {
    if (this.tryReleaseAllocation(value)) return;
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Value is not an allocation owned by this scope",
    );
  }

  /** Release exact owned scratch if present. Borrowed values and views are untouched. */
  tryReleaseAllocation(value: unknown): boolean {
    this.requireOpen();
    const allocation = this.exactAllocation(value);
    if (allocation === undefined) return false;
    allocation.release();
    return true;
  }

  allocate<T>(bytes: number, purpose: string, allocate: () => T): OwnedMemory<T> {
    this.requireOpen();
    return this.allocateWithReservation(this.reserve(bytes, purpose), allocate);
  }

  /** Admit all new backing for this stage. Allocations cannot exceed this extent.
   * Existing source owners remain separately charged; unused capacity ends on disposal.
   */
  admit(bytes: number, purpose: string): void {
    this.requireOpen();
    if (this.allocationSource !== undefined)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Child scope consumes parent admission");
    if (this.admission !== undefined)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Scope already has stage admission");
    this.admission = new StageAdmission(this.reserveFresh(bytes, purpose));
  }

  /** Transfer exclusive stage capacity without inheriting its source's lifetime.
   * Validation failure preserves caller ownership. On success the caller's
   * reservation is consumed and this scope owns the remaining extent.
   */
  admitReserved(admission: MemoryReservation): void {
    this.requireOpen();
    if (this.admission !== undefined || this.allocationSource !== undefined)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Scope already has stage admission");
    this.admission = new StageAdmission(this.consumeAdmission(admission));
  }

  get hasAdmission(): boolean {
    return this.admission !== undefined || (this.allocationSource?.hasAdmission ?? false);
  }

  /** Share remaining allocation authority without inheriting cancellation.
   * All holders draw from one extent; its unused capacity stays charged until
   * the last holder closes. This is for independently shared work, not a child
   * whose lifetime should end when its parent closes.
   */
  detachedScope(signals: readonly AbortSignal[] = []): MemoryScope {
    this.requireOpen();
    const scope = new MemoryScope(this.budget, signals);
    scope.requireOpen();
    const admission = this.inheritedAdmission();
    if (admission !== undefined) scope.admission = admission.retain();
    return scope;
  }

  private inheritedAdmission(): StageAdmission | undefined {
    return this.admission ?? this.allocationSource?.inheritedAdmission();
  }

  /** Capacity available to this stage, including already admitted but unconsumed bytes. */
  get availableAllocationBytes(): number {
    return this.closed
      ? 0
      : (this.admission?.bytes ??
          this.allocationSource?.availableAllocationBytes ??
          this.budget.availableBytes);
  }

  /** Isolate ownership/cancellation while consuming the parent's allocation extent.
   * Siblings share remaining admission; releasing child allocations does not
   * replenish it. Disposing a child never disposes its parent or siblings.
   */
  childScope(signals: readonly AbortSignal[] = []): MemoryScope {
    this.requireOpen();
    const child = new MemoryScope(this.budget, signals);
    child.allocationSource = this;
    if (!child.disposed) {
      this.children.add(child);
      child.parentScope = this;
    }
    return child;
  }

  /** Transfer stage capacity to a child that closes with this scope.
   * The caller must dispose the child after transferring any surviving output.
   */
  childStage(bytes: number, purpose: string): MemoryScope {
    const admission = this.reserve(bytes, purpose);
    const child = new MemoryScope(this.budget);
    try {
      child.admitReserved(admission);
      this.requireOpen();
      this.children.add(child);
      child.parentScope = this;
      return child;
    } catch (error) {
      admission.release();
      child.dispose();
      throw error;
    }
  }

  /** Transfer capacity to an allocator, drawing from stage admission when present.
   * The recipient must consume or release the returned reservation.
   */
  reserve(bytes: number, purpose: string): MemoryReservation {
    this.requireOpen();
    if (this.admission === undefined) {
      if (this.allocationSource === undefined) return this.reserveFresh(bytes, purpose);
      const reservation = this.allocationSource.reserve(bytes, purpose);
      try {
        this.requireOpen();
        return reservation;
      } catch (error) {
        reservation.release();
        throw error;
      }
    }
    return this.admission.reserve(bytes, purpose);
  }

  private reserveFresh(bytes: number, purpose: string): MemoryReservation {
    const reservation = this.budget.reserve(bytes, purpose);
    try {
      // Cache eviction can synchronously close or reconfigure this scope.
      this.requireOpen();
      if (this.admission !== undefined)
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "Scope admission changed during reclamation",
        );
      return reservation;
    } catch (error) {
      reservation.release();
      throw error;
    }
  }

  /** Consume exclusive pre-admitted capacity before invoking the allocator.
   * Validation failures preserve caller ownership; after transfer, failures release it.
   */
  allocateReserved<T>(admission: MemoryReservation, allocate: () => T): OwnedMemory<T> {
    return this.allocateWithReservation(this.consumeAdmission(admission), allocate);
  }

  private allocateWithReservation<T>(
    reservation: MemoryReservation,
    allocate: () => T,
  ): OwnedMemory<T> {
    let value: T;
    try {
      value = allocate();
      this.requireOpen();
    } catch (error) {
      reservation.release();
      throw error;
    }
    return this.track(value, reservation);
  }

  /** Reserve before starting I/O, and keep the reservation until I/O actually settles. */
  async allocateAsync<T>(
    bytes: number,
    purpose: string,
    allocate: () => Promise<T>,
  ): Promise<OwnedMemory<T>> {
    this.requireOpen();
    return this.allocateWithReservationAsync(this.reserve(bytes, purpose), allocate);
  }

  /** Transferred capacity stays charged until pending work settles, even after disposal. */
  async allocateReservedAsync<T>(
    admission: MemoryReservation,
    allocate: () => Promise<T>,
  ): Promise<OwnedMemory<T>> {
    return this.allocateWithReservationAsync(this.consumeAdmission(admission), allocate);
  }

  private async allocateWithReservationAsync<T>(
    reservation: MemoryReservation,
    allocate: () => Promise<T>,
  ): Promise<OwnedMemory<T>> {
    try {
      const value = await allocate();
      this.requireOpen();
      return this.track(value, reservation);
    } catch (error) {
      reservation.release();
      throw error;
    }
  }

  private consumeAdmission(admission: MemoryReservation): MemoryReservation {
    this.requireOpen();
    if (admission.budget !== this.budget) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Pre-admitted allocation requires the same memory budget",
      );
    }
    const [reservation] = admission.partition([admission.bytes]);
    if (reservation === undefined) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing admission partition");
    }
    return reservation;
  }

  /** Pending I/O remains charged until it settles; disposal cannot free live I/O buffers. */
  dispose(): void {
    if (this.closed) return;
    this.closed = true;
    this.parentScope?.children.delete(this);
    this.parentScope = undefined;
    for (const child of this.children) child.dispose();
    this.detachSignals?.();
    this.detachSignals = undefined;
    this.controller?.abort();
    this.admission?.release();
    this.admission = undefined;
    this.invalidateSnapshot();
    for (const release of this.owned.keys()) release();
  }

  private track<T>(value: T, reservation: MemoryReservation): OwnedMemory<T> {
    const owner = ownedMemory(value, reservation, this.budget);
    return this.trackOwner(owner);
  }

  private trackOwner<T>(owner: OwnedMemory<T>): OwnedMemory<T> {
    let released = false;
    let key: unknown = owner.value;
    const release = () => {
      if (released) return;
      released = true;
      this.owned.delete(release);
      this.invalidateSnapshot();
      if (allocation.next === allocation) this.allocationsByValue.delete(key);
      else {
        allocation.previous.next = allocation.next;
        allocation.next.previous = allocation.previous;
        if (this.allocationsByValue.get(key) === allocation)
          this.allocationsByValue.set(key, allocation.next);
      }
      // Do not let a released handle retain other live owners through the list.
      allocation.previous = allocation.next = allocation;
      key = undefined;
      owner.release();
    };
    // The public scope handle also serves as its index node: no second object
    // per allocation just to link duplicate values.
    const allocation = {
      get value() {
        if (released) throw releasedOwnership();
        return owner.value;
      },
      bytes: owner.bytes,
      budget: owner.budget,
      retain() {
        if (released) throw releasedOwnership();
        return owner.retain();
      },
      retainIn(budget: MemoryBudget) {
        if (released) throw releasedOwnership();
        return owner.retainIn(budget);
      },
      release,
    } as ScopedAllocation & OwnedMemory<T>;
    Object.defineProperties(allocation, {
      previous: { value: allocation, writable: true },
      next: { value: allocation, writable: true },
    });
    const first = this.allocationsByValue.get(key);
    if (first === undefined) {
      allocation.previous = allocation.next = allocation;
      this.allocationsByValue.set(key, allocation);
    } else {
      allocation.previous = first.previous;
      allocation.next = first;
      first.previous.next = allocation;
      first.previous = allocation;
    }
    this.invalidateSnapshot();
    this.owned.set(release, owner);
    return allocation;
  }

  private invalidateSnapshot(): void {
    const snapshot = this.snapshot;
    this.snapshot = undefined;
    if (snapshot !== undefined) releaseCompositeBacking(snapshot);
  }

  private exactAllocation(value: unknown): ScopedAllocation | undefined {
    // Preserve the previous strict-equality lookup contract for NaN scalars.
    return typeof value === "number" && Number.isNaN(value)
      ? undefined
      : this.allocationsByValue.get(value);
  }

  private requireOpen(): void {
    if (this.closed) throw new LakeqlError("LAKEQL_ABORTED", "Memory scope has been disposed");
  }
}

class AtomicMemory<T> implements OwnedMemory<T> {
  #released = false;
  #payload: T;
  #reservation: MemoryReservation;
  declare readonly value: T;
  readonly bytes: number;

  constructor(
    value: T,
    reservation: MemoryReservation,
    readonly budget: MemoryBudget,
  ) {
    this.#payload = value;
    this.#reservation = reservation;
    this.bytes = reservation.bytes;
    // Keep spread wrappers and detached callbacks compatible with public handles.
    this.retain = this.retain.bind(this);
    this.retainIn = this.retainIn.bind(this);
    this.release = this.release.bind(this);
    Object.defineProperty(this, "value", {
      enumerable: true,
      configurable: true,
      get: atomicValue,
    });
    allocationProvenance.set(this, null);
  }

  readValue(): T {
    if (this.#released) throw releasedOwnership();
    return this.#payload;
  }

  retain(): OwnedMemory<T> {
    if (this.#released) throw releasedOwnership();
    return new AtomicMemory(this.#payload, this.#reservation.retain(), this.budget);
  }

  retainIn(target: MemoryBudget): OwnedMemory<T> {
    if (this.#released) throw releasedOwnership();
    return new AtomicMemory(this.#payload, this.#reservation.retainIn(target), target);
  }

  release(): void {
    if (this.#released) return;
    this.#released = true;
    allocationProvenance.delete(this);
    this.#payload = undefined as T;
    this.#reservation.release();
  }
}

function atomicValue(this: AtomicMemory<unknown>): unknown {
  return this.readValue();
}

function ownedMemory<T>(
  value: T,
  reservation: MemoryReservation,
  budget: MemoryBudget,
): OwnedMemory<T> {
  return new AtomicMemory(value, reservation, budget);
}

function releasedOwnership(): LakeqlError {
  return new LakeqlError(
    "LAKEQL_VALIDATION_ERROR",
    "Cannot access or retain released memory ownership",
  );
}

function compositeMemory<T>(
  value: T,
  backing: readonly OwnedMemory<unknown>[],
  budget: MemoryBudget,
): OwnedMemory<T> {
  return compositeHandle(value, captureCompositeBacking(backing, budget));
}

function captureCompositeBacking(
  backing: readonly OwnedMemory<unknown>[],
  budget: MemoryBudget,
): CompositeBacking {
  const retained: OwnedMemory<unknown>[] = [];
  let bytes = 0;
  try {
    for (const owner of backing) {
      bytes += owner.bytes;
      if (!Number.isSafeInteger(bytes)) {
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "Backing byte count exceeds safe integer range",
        );
      }
      retained.push(owner.budget === budget ? owner.retain() : owner.retainIn(budget));
    }
  } catch (error) {
    for (const owner of retained) owner.release();
    throw error;
  }
  return { retained, bytes, budget, references: 1, nextRelease: undefined };
}

interface CompositeBacking {
  nextRelease: CompositeBacking | undefined;
  retained: readonly OwnedMemory<unknown>[];
  bytes: number;
  budget: MemoryBudget;
  references: number;
}

/** Same-budget handles share one backing pin set; other budgets need admission. */
function compositeHandle<T>(value: T, backing: CompositeBacking): OwnedMemory<T> {
  let released = false;
  const retain = () => {
    if (released) throw releasedOwnership();
    backing.references++;
    return compositeHandle(value, backing);
  };
  const owner: OwnedMemory<T> = {
    get value() {
      if (released) throw releasedOwnership();
      return value;
    },
    bytes: backing.bytes,
    budget: backing.budget,
    retain,
    retainIn(target) {
      if (released) throw releasedOwnership();
      if (target === backing.budget) return retain();
      // Keep the complete backing set live if target reclamation disposes the
      // original producer while admitting individual target allocations.
      const guard = retain();
      try {
        return compositeMemory(value, backing.retained, target);
      } finally {
        guard.release();
      }
    },
    release() {
      if (released) return;
      released = true;
      allocationProvenance.delete(owner);
      value = undefined as T;
      releaseCompositeBacking(backing);
    },
  };
  allocationProvenance.set(owner, backing.retained);
  return owner;
}

let pendingBackingRelease: CompositeBacking | undefined;
let drainingBackingRelease = false;

function releaseCompositeBacking(backing: CompositeBacking): void {
  if (--backing.references !== 0) return;
  // Intrusive release queue uses the existing backing records, not a growing
  // JavaScript call stack or a newly allocated array of pending owners.
  backing.nextRelease = pendingBackingRelease;
  pendingBackingRelease = backing;
  if (drainingBackingRelease) return;
  drainingBackingRelease = true;
  let failed = false;
  let failure: unknown;
  try {
    while (pendingBackingRelease !== undefined) {
      const current: CompositeBacking = pendingBackingRelease;
      pendingBackingRelease = current.nextRelease;
      current.nextRelease = undefined;
      const owners = current.retained;
      current.retained = [];
      for (const owner of owners) {
        try {
          owner.release();
        } catch (error) {
          if (!failed) {
            failed = true;
            failure = error;
          }
        }
      }
    }
  } finally {
    drainingBackingRelease = false;
  }
  if (failed) throw failure;
}

/** Transfer selected atomic allocations from trusted internal ownership.
 * The caller must identify every reachable allocation, including borrowed aliases.
 * Unknown external owners are conservatively retained whole. Selection failures
 * leave the target unchanged; sources stay guarded throughout target admission.
 */
export function retainSelectedAllocations(
  input: OwnedMemory<unknown>,
  target: MemoryScope,
  keep: (allocatedValue: unknown) => boolean,
): void {
  const selected = target.childScope();
  try {
    visitOwnedAllocations(input, selected, (owner, opaque) => {
      if (opaque || keep(owner.value)) selected.adopt(owner.retainIn(target.budget));
    });
    target.adopt(selected.retainAllocations(undefined));
  } finally {
    selected.dispose();
  }
}

/** Index trusted immutable scalar backing once for a detachment stage. Unknown
 * external ownership has no atomic provenance and is left to the copying path.
 * The returned child scope must stay live during lookup, then be disposed.
 */
export function indexImmutableBacking(
  input: OwnedMemory<unknown>,
  parent: MemoryScope,
): MemoryScope {
  const index = parent.childScope();
  try {
    index.allocate(c.object, "immutable backing index", () => undefined);
    visitOwnedAllocations(input, index, (owner, opaque) => {
      if (opaque || (typeof owner.value !== "string" && typeof owner.value !== "bigint")) return;
      const existing = index.tryRetainAllocation(owner.value);
      if (existing !== undefined) {
        existing.release();
        return;
      }
      index.allocate(c.property, "immutable backing index entry", () => undefined);
      index.adopt(owner.retainIn(parent.budget));
    });
    return index;
  } catch (error) {
    index.dispose();
    throw error;
  }
}

function visitOwnedAllocations(
  input: OwnedMemory<unknown>,
  parent: MemoryScope,
  visit: (owner: OwnedMemory<unknown>, opaque: boolean) => void,
): void {
  const guard = input.retain();
  let traversal: MemoryScope | undefined;
  try {
    const frames = parent.childScope();
    traversal = frames;
    interface Frame {
      owner: OwnedMemory<unknown>;
      index: number;
      parent: OwnedMemory<Frame> | undefined;
    }
    const push = (owner: OwnedMemory<unknown>, parent: OwnedMemory<Frame> | undefined) =>
      frames.allocate(c.object + 3 * c.property, "ownership traversal frame", () => ({
        owner,
        index: 0,
        parent,
      }));
    let frame: OwnedMemory<Frame> | undefined = push(guard, undefined);
    while (frame !== undefined) {
      const state: Frame = frame.value;
      const provenance = allocationProvenance.get(state.owner);
      if (provenance !== undefined && provenance !== null && state.index < provenance.length) {
        const child = provenance[state.index++];
        if (child !== undefined) {
          const childProvenance = allocationProvenance.get(child);
          if (childProvenance === undefined || childProvenance === null) {
            visit(child, childProvenance === undefined);
          } else if (childProvenance.length > 0) frame = push(child, frame);
        }
        continue;
      }
      if (provenance === undefined || provenance === null)
        visit(state.owner, provenance === undefined);
      const parent: OwnedMemory<Frame> | undefined = state.parent;
      frame.release();
      frame = parent;
    }
  } finally {
    traversal?.dispose();
    guard.release();
  }
}
