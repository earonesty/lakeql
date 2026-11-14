import {
  MATERIALIZATION_CHARGES as c,
  closeAsyncIterators,
  LakeqlError,
  type MemoryScope,
  mapConcurrent,
  type OwnedMemory,
} from "lakeql-core";
import type { NestedColumnRow } from "./nested-column-rows.js";
import { validateParquetReadConcurrency } from "./read-concurrency.js";

export interface AlignedNestedRow {
  row: number;
  rowCount?: number;
  columns: readonly NestedColumnRow[];
}

/** Synchronize sealed row streams, not raw page boundaries. Retained output pins all
 * participating rows. Advance callbacks are bounded and drained before cleanup on error.
 */
export async function* readAlignedNestedRows(
  sources: readonly AsyncIterable<OwnedMemory<NestedColumnRow>>[],
  scope: MemoryScope,
  concurrency = 4,
): AsyncGenerator<OwnedMemory<AlignedNestedRow>> {
  validateParquetReadConcurrency({ maxConcurrentReads: concurrency });
  if (sources.length === 0) return;
  const lifetime = scope.childScope();
  let iterators: AsyncIterator<OwnedMemory<NestedColumnRow>>[] | undefined;
  let failed = false;
  const cleanup = async () => {
    try {
      if (iterators) await closeAsyncIterators(iterators);
    } catch (error) {
      if (!failed) throw error;
    } finally {
      lifetime.dispose();
    }
  };
  try {
    iterators = lifetime.allocate(
      c.array + sources.length * c.arraySlot,
      "nested row iterator directory",
      () => [] as AsyncIterator<OwnedMemory<NestedColumnRow>>[],
    ).value;
    for (const source of sources) iterators.push(source[Symbol.asyncIterator]());
    let row = 0;
    while (true) {
      if (scope.disposed)
        throw new LakeqlError("LAKEQL_ABORTED", "Nested alignment scope disposed");
      const round = lifetime.childScope();
      const scratch = round.childScope();
      try {
        const resultBytes = c.array + sources.length * (c.arraySlot + c.object + 2 * c.property);
        const workerDirectories =
          2 * (c.array + Math.min(concurrency, sources.length) * c.arraySlot);
        const results = (
          await scratch.allocateAsync(
            resultBytes + workerDirectories,
            "nested concurrent advance results",
            () => mapConcurrent(iterators ?? [], concurrency, (iterator) => iterator.next()),
          )
        ).value;
        const done = results.reduce((count, result) => count + Number(result.done === true), 0);
        if (done === sources.length) return;
        if (done !== 0)
          throw new LakeqlError(
            "LAKEQL_PARQUET_READ_ERROR",
            "Nested columns have different row counts",
          );
        const first = results[0];
        const rowCount = first && !first.done ? (first.value.value.rowCount ?? 1) : 0;
        if (
          !Number.isSafeInteger(rowCount) ||
          rowCount < 1 ||
          !Number.isSafeInteger(row + rowCount)
        )
          throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid nested row block count");
        const columns = round.allocate(
          c.array + sources.length * c.arraySlot,
          "aligned nested row columns",
          () => new Array<NestedColumnRow>(sources.length),
        ).value;
        for (let i = 0; i < results.length; i++) {
          const result = results[i];
          if (
            !result ||
            result.done ||
            result.value.value.row !== row ||
            (result.value.value.rowCount ?? 1) !== rowCount
          )
            throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Nested column row order mismatch");
          const pin = result.value.retainIn(round.budget);
          try {
            columns[i] = round.adopt(pin).value;
          } catch (error) {
            pin.release();
            throw error;
          }
        }
        const value = round.allocate(
          c.object + (rowCount > 1 ? 3 : 2) * c.property,
          "aligned nested row",
          () => ({
            row,
            ...(rowCount > 1 ? { rowCount } : {}),
            columns,
          }),
        ).value;
        scratch.dispose();
        const owner = round.retainAllocations(value);
        round.dispose();
        try {
          yield owner;
        } finally {
          owner.release();
        }
        row += rowCount;
      } finally {
        scratch.dispose();
        round.dispose();
      }
    }
  } catch (error) {
    failed = true;
    throw error;
  } finally {
    await cleanup();
  }
}
