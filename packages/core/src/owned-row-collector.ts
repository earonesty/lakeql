import { LakeqlError } from "./errors.js";
import { MATERIALIZATION_CHARGES } from "./materialization-plan.js";
import type { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import type { Row } from "./types.js";

interface RowChunk {
  readonly rows: OwnedMemory<Row[]>;
  next?: RowChunk | undefined;
}

/** Explicitly owned collection. Payload owners stay live across scan iteration;
 * bookkeeping and the final reference array have separately admitted extents.
 */
export class OwnedRowCollector {
  private readonly scope: MemoryScope;
  private first: RowChunk | undefined;
  private last: RowChunk | undefined;
  private count = 0;
  private closed = false;

  constructor(readonly budget: MemoryBudget) {
    this.scope = new MemoryScope(budget);
  }

  get rowCount(): number {
    return this.count;
  }

  /** Retains independently: the caller may release its input handle on return. */
  append(input: OwnedMemory<Row[]>): void {
    this.requireOpen();
    const count = this.count + input.value.length;
    if (!Number.isSafeInteger(count) || count > 0xffffffff)
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Materialized collection exceeds array capacity",
      );
    if (input.value.length === 0) return;
    const retained = this.scope.adopt(input.retainIn(this.budget));
    try {
      const charges = MATERIALIZATION_CHARGES;
      const node = this.scope.allocate(
        charges.object + charges.property * 2,
        "output collector chunk",
        () => ({ rows: retained }) as RowChunk,
      ).value;
      if (this.last === undefined) this.first = node;
      else this.last.next = node;
      this.last = node;
      this.count = count;
    } catch (error) {
      retained.release();
      throw error;
    }
  }

  /** Builds one exact-length array, preserving row identity and payload owners.
   * Finishing consumes the collector, including on admission/allocation failure.
   */
  finish(): OwnedMemory<Row[]> {
    this.requireOpen();
    try {
      if (this.first !== undefined && this.first.next === undefined)
        return this.first.rows.retainIn(this.budget);
      const cost = MATERIALIZATION_CHARGES;
      const rows = this.scope.allocate(
        cost.array + this.count * cost.arraySlot,
        "collected output array",
        () => {
          const output = new Array<Row>(this.count);
          let offset = 0;
          for (let chunk = this.first; chunk !== undefined; chunk = chunk.next)
            for (const row of chunk.rows.value) output[offset++] = row;
          return output;
        },
      );
      // Nodes are scratch. Payload owners remain in the scope independently.
      let chunk = this.first;
      this.first = this.last = undefined;
      while (chunk !== undefined) {
        const next = chunk.next;
        chunk.next = undefined;
        this.scope.releaseAllocation(chunk);
        chunk = next;
      }
      return this.scope.retainAllocations(rows.value);
    } finally {
      this.dispose();
    }
  }

  dispose(): void {
    if (this.closed) return;
    this.closed = true;
    this.first = this.last = undefined;
    this.scope.dispose();
  }

  private requireOpen(): void {
    if (this.closed)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Output collector has been disposed");
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
