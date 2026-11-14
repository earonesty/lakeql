import { LakeqlError } from "./errors.js";
import { MATERIALIZATION_CHARGES } from "./materialization-plan.js";
import type { MemoryBudget, MemoryReservation } from "./memory-budget.js";
import { reserveMemoryCapacity } from "./memory-capacity.js";
import { MemoryScope, type OwnedMemory, retainContainerPayload } from "./memory-scope.js";
import type { Row } from "./types.js";

/** Explicitly owned collection. A single chunk transfers directly; additional
 * chunks append into one private, incrementally admitted reference directory.
 * Payload ownership transfers before input directories are released. */
export class OwnedRowCollector {
  private readonly scope: MemoryScope;
  private pending: OwnedMemory<Row[]> | undefined;
  private pendingCharge: MemoryReservation | undefined;
  private rows: Row[] | undefined;
  private directory: MemoryReservation | undefined;
  private count = 0;
  private closed = false;
  private appending = false;

  constructor(readonly budget: MemoryBudget) {
    this.scope = new MemoryScope(budget);
  }

  get rowCount(): number {
    return this.count;
  }

  /** Retains independently: the caller may release its input handle on return. */
  append(input: OwnedMemory<Row[]>): void {
    this.requireOpen();
    if (this.appending)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Collection append is already active");
    this.appending = true;
    try {
      this.appendInput(input);
    } finally {
      this.appending = false;
      if (this.closed) this.scope.dispose();
    }
  }

  private appendInput(input: OwnedMemory<Row[]>): void {
    const incoming = input.value.length;
    const count = this.count + incoming;
    if (!Number.isSafeInteger(count) || count > 0xffffffff)
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Materialized collection exceeds array capacity",
      );
    if (incoming === 0) return;
    const cost = MATERIALIZATION_CHARGES;
    if (this.count === 0) {
      const retained = retainForCollection(input, this.scope);
      try {
        this.pendingCharge = this.scope.reserve(
          cost.object + 2 * cost.property,
          "output collector pending chunk",
        );
        this.requireOpen();
        this.pending = retained;
        this.count = count;
      } catch (error) {
        try {
          retained.release();
        } catch {
          /* Preserve the admission failure. */
        }
        throw error;
      }
      return;
    }
    // Active copy ownership survives reentrant collector disposal until this
    // synchronous operation settles, like a pending read's transport ownership.
    const temporary = new MemoryScope(this.budget);
    const hadDirectory = this.rows !== undefined;
    let activeDirectory: MemoryReservation | undefined;
    let committed = false;
    let failed = false;
    let failure: unknown;
    try {
      const retained = retainForCollection(input, temporary);
      const prior =
        this.pending === undefined ? undefined : retainForCollection(this.pending, temporary);
      this.directory = reserveMemoryCapacity(
        this.scope,
        this.directory,
        cost.array + count * cost.arraySlot,
        "collected output directory",
      );
      activeDirectory = this.directory.retain();
      this.requireOpen();
      const output = this.rows ?? new Array<Row>(count);
      let offset = this.count;
      if (prior !== undefined) {
        const first = prior.value;
        if (first.length !== this.count)
          throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Collected input extent changed");
        for (let i = 0; i < first.length; i++) output[i] = first[i] as Row;
      }
      const next = retained.value;
      if (next.length !== incoming)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Collected input extent changed");
      for (let i = 0; i < incoming; i++) output[offset++] = next[i] as Row;
      if (prior !== undefined) retainContainerPayload(prior, temporary);
      retainContainerPayload(retained, temporary);
      prior?.release();
      retained.release();
      this.requireOpen();
      const payload = temporary.retainAllocations(undefined);
      try {
        this.scope.adopt(payload);
      } catch (error) {
        payload.release();
        throw error;
      }
      this.rows = output;
      this.count = count;
      const pending = this.pending;
      const charge = this.pendingCharge;
      this.pending = undefined;
      this.pendingCharge = undefined;
      committed = true;
      charge?.release();
      pending?.release();
    } catch (error) {
      failed = true;
      failure = error;
      try {
        if (committed) this.dispose();
        else {
          if (this.rows !== undefined) this.rows.length = this.count;
          // Truncating a private array need not release its backing capacity.
          // Keep grown capacity charged for reuse; only discard a failed initial
          // promotion, whose new directory never became collector state.
          if (!hadDirectory) {
            this.directory?.release();
            this.directory = undefined;
          }
        }
      } catch {
        try {
          this.dispose();
        } catch {
          /* Preserve the original failure. */
        }
      }
    }
    try {
      temporary.dispose();
    } catch (error) {
      if (!failed) {
        failed = true;
        failure = error;
        try {
          this.dispose();
        } catch {
          /* Preserve the cleanup failure. */
        }
      }
    }
    try {
      activeDirectory?.release();
    } catch (error) {
      if (!failed) {
        failed = true;
        failure = error;
        try {
          this.dispose();
        } catch {
          /* Preserve the release failure. */
        }
      }
    }
    if (failed) throw failure;
  }

  /** Finishing consumes the collector, including on allocation failure. */
  finish(): OwnedMemory<Row[]> {
    this.requireOpen();
    if (this.appending)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Cannot finish during collection append");
    try {
      if (this.pending !== undefined) return this.pending.retainIn(this.budget);
      if (this.rows === undefined)
        return this.scope
          .allocateContainer(
            MATERIALIZATION_CHARGES.array,
            "empty collected output",
            () => [] as Row[],
          )
          .retain();
      const rows = this.rows;
      this.scope.allocateContainerReserved(this.directory as MemoryReservation, () => rows);
      this.directory = undefined;
      return this.scope.retainAllocations(rows);
    } finally {
      this.dispose();
    }
  }

  dispose(): void {
    if (this.closed) return;
    this.closed = true;
    const directory = this.directory;
    const pendingCharge = this.pendingCharge;
    this.directory = this.pendingCharge = undefined;
    this.pending = undefined;
    this.rows = undefined;
    try {
      directory?.release();
    } finally {
      try {
        pendingCharge?.release();
      } finally {
        // An active append still references accumulated payload. Defer its
        // release until append settles, without rescanning all previous owners.
        if (!this.appending) this.scope.dispose();
      }
    }
  }

  private requireOpen(): void {
    if (this.closed)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Output collector has been disposed");
  }
}

function retainForCollection(input: OwnedMemory<Row[]>, target: MemoryScope): OwnedMemory<Row[]> {
  const retained = input.retainIn(target.budget);
  try {
    return target.adopt(retained);
  } catch (error) {
    retained.release();
    throw error;
  }
}

/** Consumes each yielded handle, including the current handle when append fails.
 * Async iterator closing propagates cancellation/failure to the producer.
 * A source factory receives a terminal failure callback: it releases collected
 * rows synchronously and throws the supplied error before producer cleanup awaits.
 */
export async function collectOwnedRows(
  source:
    | AsyncIterable<OwnedMemory<Row[]>>
    | ((fail: (error: unknown) => never) => AsyncIterable<OwnedMemory<Row[]>>),
  budget: MemoryBudget,
  signal?: AbortSignal,
): Promise<OwnedMemory<Row[]>> {
  const collector = new OwnedRowCollector(budget);
  const check = () => {
    if (signal?.aborted) throw new LakeqlError("LAKEQL_ABORTED", "Owned output collection aborted");
  };
  const abort = () => collector.dispose();
  signal?.addEventListener("abort", abort, { once: true });
  try {
    check();
    const iterable =
      typeof source === "function"
        ? source((error) => {
            collector.dispose();
            throw error;
          })
        : source;
    for await (const rows of iterable) {
      try {
        check();
        collector.append(rows);
      } catch (error) {
        // Async iterator closing can await remote cleanup. Failed collection no
        // longer needs earlier output: release it before awaiting producer return.
        collector.dispose();
        throw error;
      } finally {
        rows.release();
      }
    }
    check();
    return collector.finish();
  } finally {
    signal?.removeEventListener("abort", abort);
    collector.dispose();
  }
}
