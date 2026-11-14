import { LakeqlError, type MemoryBudget, MemoryScope, type OwnedMemory } from "lakeql-core";
import type { StoreAsyncBuffer } from "./types.js";

/** A decoder stage owns its range/decompression backing until explicit handoff.
 * Decoded objects and arrays need separate admission by the decoder itself.
 */
export class ParquetReadScope {
  readonly allocations: MemoryScope;
  readonly file: StoreAsyncBuffer;

  constructor(
    source: StoreAsyncBuffer,
    budget: MemoryBudget | MemoryScope,
    signals: readonly AbortSignal[] = [],
  ) {
    const read = source.sliceOwned?.bind(source);
    if (read === undefined) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Parquet allocation tracking requires owned range reads",
      );
    }
    this.allocations =
      budget instanceof MemoryScope ? budget.childScope(signals) : new MemoryScope(budget, signals);
    const scope = this.allocations;
    const prefetch = source.prefetchOwned?.bind(source);
    this.file = {
      byteLength: source.byteLength,
      ...(source.etag === undefined ? {} : { etag: source.etag }),
      sliceOwned: (start, end, target) => read(start, end, target),
      ...(source.ownedReadAllocationBytes === undefined
        ? {}
        : {
            ownedReadAllocationBytes: source.ownedReadAllocationBytes.bind(source),
          }),
      async slice(start, end = source.byteLength) {
        return (await read(start, end, scope)).value;
      },
      // Never silently fall back to borrowed prefetch, which bypasses admission.
      ...(prefetch === undefined
        ? {}
        : {
            prefetch: (ranges) => prefetch(ranges, scope),
          }),
    };
  }

  /** Call after decode settles. The returned owner survives disposal of this stage.
   * It covers existing input/decompression allocations, not newly decoded storage.
   */
  retainInputBacking<T>(value: T): OwnedMemory<T> {
    return this.allocations.retainAllocations(value);
  }

  dispose(): void {
    this.allocations.dispose();
  }
}
