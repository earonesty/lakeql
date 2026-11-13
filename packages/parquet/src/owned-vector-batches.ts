import { type MemoryBudget, MemoryScope, type OwnedMemory, throwIfAborted } from "lakeql-core";
import { parquetBatchSize } from "./batch-size.js";
import { readOwnedParquetColumnBatches } from "./column-batches.js";
import { iterateIndexedGroups } from "./indexed-row-group-plan.js";
import { canReadNestedRowGroupBatches } from "./nested-read-capability.js";
import { readNestedRowGroupBatches } from "./nested-row-group-read.js";
import { validateParquetReadConcurrency } from "./read-concurrency.js";
import { ParquetReadScope } from "./read-scope.js";
import type {
  ParquetMetadata,
  ParquetRowGroupSource,
  ReadParquetBatchOptions,
  StoreAsyncBuffer,
} from "./types.js";
import {
  canReadParquetVectorBatches,
  iterateDirectVectorRanges,
  type ParquetVectorBatch,
  readParquetVectorBatchesFromFile,
} from "./vector-batches.js";

/** Allocation owners for decoded batches, independent of iterator lifetime.
 * The receiver must release each yielded owner, including after early exit.
 * Direct decoding retires consumed pages within each row-group stage; fallback
 * decoding owns one requested batch.
 * Retained output may outlive either decode stage.
 * This accounts admitted backing, not uninstrumented decoder/JS allocations.
 */
export async function* readOwnedParquetVectorBatches(
  source: StoreAsyncBuffer,
  input: ParquetMetadata | ParquetRowGroupSource,
  budget: MemoryBudget,
  options: Omit<ReadParquetBatchOptions, "allocationScope" | "decompressionScope">,
  signals: readonly AbortSignal[] = [],
): AsyncIterable<OwnedMemory<ParquetVectorBatch>> {
  const indexed = "readRowGroup" in input ? input : undefined;
  const metadata: ParquetMetadata =
    indexed === undefined ? (input as ParquetMetadata) : { ...indexed.metadata, row_groups: [] };
  function* rowGroups() {
    if (indexed === undefined) {
      yield* metadata.row_groups;
      return;
    }
    yield* iterateIndexedGroups(indexed, signals);
  }
  for (const signal of signals) throwIfAborted(signal);
  parquetBatchSize(options.batchSize);
  validateParquetReadConcurrency(options);
  const windowLimit = options.maxConcurrentReads ?? 4;
  let rowGroupStart = 0;
  const requestedStart = options.rowStart ?? 0;
  const requestedEnd = options.rowEnd ?? Number(metadata.num_rows);
  // Keep early-stop reads page-driven; prefetching complete chunks defeats the
  // decoder's page windows even when the speculative buffers fit the budget.
  const groups =
    options.canStopEarly === true ||
    source.prefetchOwned === undefined ||
    options.columns === undefined
      ? undefined
      : iterateDirectVectorRanges(
          metadata,
          options.columns,
          requestedStart,
          requestedEnd,
          options.where,
          rowGroups(),
        );
  const pending: {
    rowGroupIndex: number;
    ranges: { start: number; end: number }[];
    prefetched: boolean;
  }[] = [];
  const fillWindow = () => {
    while (groups !== undefined && pending.length < windowLimit) {
      const next = groups.next();
      if (next.done) break;
      pending.push({ ...next.value, prefetched: false });
    }
  };
  let nextRowGroupIndex = 0;
  try {
    for (const rowGroup of rowGroups()) {
      const rowGroupIndex = nextRowGroupIndex++;
      for (const signal of signals) throwIfAborted(signal);
      const count = Number(rowGroup.num_rows);
      const rowGroupEnd = rowGroupStart + count;
      const start = Math.max(requestedStart, rowGroupStart);
      const end = Math.min(requestedEnd, rowGroupEnd);
      if (start < end) {
        fillWindow();
        if (pending[0]?.rowGroupIndex === rowGroupIndex) {
          if (!pending[0].prefetched && source.prefetchOwned !== undefined) {
            // Keep half of current shared headroom available for demand/decode.
            // The owned cache performs final admission using actual backing sizes.
            const byteLimit = Math.floor(budget.availableBytes / 2);
            const ranges: { start: number; end: number }[] = [];
            let bytes = 0;
            for (const group of pending) {
              const groupBytes = group.ranges.reduce(
                (total, range) => total + range.end - range.start,
                0,
              );
              if (groupBytes > byteLimit - bytes) break;
              ranges.push(...group.ranges);
              bytes += groupBytes;
              group.prefetched = true;
            }
            if (ranges.length > 0) {
              const prefetch = new MemoryScope(budget, signals);
              try {
                await source.prefetchOwned(ranges, prefetch);
              } finally {
                prefetch.dispose();
              }
            }
          }
          pending.shift();
        }

        const stage = new ParquetReadScope(source, budget, signals);
        try {
          const groupMetadata = {
            ...metadata,
            num_rows: rowGroup.num_rows,
            row_groups: [rowGroup],
          };
          const groupOptions = {
            ...options,
            rowStart: start - rowGroupStart,
            rowEnd: end - rowGroupStart,
            ...(options.decodedColumnCacheKey === undefined
              ? {}
              : {
                  decodedColumnCacheKey: `owned-row-group:${rowGroupStart}:${options.decodedColumnCacheKey}`,
                }),
            allocationScope: stage.allocations,
            pageAllocationLifetime: "iteration" as const,
            adaptiveReadWindows: true,
            maxConcurrentReads: 1,
            maxConcurrentPageReads:
              options.maxConcurrentPageReads ?? options.maxConcurrentReads ?? 4,
          };
          if (canReadParquetVectorBatches(groupMetadata, groupOptions, { nested: false })) {
            for await (const output of readParquetVectorBatchesFromFile(
              stage.file,
              groupMetadata,
              groupOptions,
            )) {
              yield stage.retainInputBacking({
                ...output,
                rowOffset: rowGroupStart + output.rowOffset,
              });
            }
          } else {
            const outputs = canReadNestedRowGroupBatches(
              groupMetadata,
              groupOptions,
              stage.allocations,
            )
              ? readNestedRowGroupBatches(
                  stage.file,
                  groupMetadata,
                  stage.allocations,
                  groupOptions,
                )
              : readOwnedParquetColumnBatches(source, groupMetadata, budget, groupOptions, signals);
            for await (const output of outputs) {
              const pin = stage.allocations.adopt(output);
              try {
                yield stage.retainInputBacking({
                  ...output.value,
                  rowOffset: rowGroupStart + output.value.rowOffset,
                });
              } finally {
                pin.release();
              }
            }
          }
        } finally {
          stage.dispose();
        }
      }
      rowGroupStart = rowGroupEnd;
      if (rowGroupStart >= requestedEnd) break;
    }
  } finally {
    groups?.return(undefined);
  }
}
