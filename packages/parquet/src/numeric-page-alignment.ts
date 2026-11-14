import type { ColumnDecoder } from "hyparquet/src/types.js";
import {
  batchFromVectors,
  closeAsyncIterators,
  isLakeqlError,
  LakeqlError,
  type MemoryScope,
  type MemoryStageScheduler,
  type OwnedMemory,
  sliceVector,
  type Vector,
  vectorLength,
} from "lakeql-core";
import { parquetBatchSize } from "./batch-size.js";
import { cachedPageWindow } from "./cached-page-window.js";
import { type DecodedColumnCache, decodedColumnPageCacheKey } from "./decoded-column-cache.js";
import { type NumericPageGroupEntry, planNumericPageGroup } from "./numeric-page-group.js";
import { preparePageWindow } from "./page-window.js";
import { validateParquetReadConcurrency } from "./read-concurrency.js";
import type { StoreAsyncBuffer } from "./types.js";
import type { ParquetVectorBatch } from "./vector-batches.js";

export interface PreparedNumericColumnPage extends NumericPageGroupEntry {
  rowOffset: number;
  rowCount: number;
}

/** Align prepared numeric columns while admitting replacements as one group.
 * Sources own header inspection; this iterator owns decoded pages. Yielded owners
 * belong to the consumer and may outlive the iterator, counting against its pool.
 */
export async function* readAlignedNumericPageGroups(
  file: StoreAsyncBuffer,
  sources: readonly { column: string; pages: AsyncIterable<PreparedNumericColumnPage> }[],
  scope: MemoryScope,
  options: {
    batchSize?: number;
    maxConcurrentReads?: number;
    scheduler?: MemoryStageScheduler;
    decode?: (work: () => Vector) => Vector;
    onCacheLookup?: (hit: boolean) => void;
    rowStart?: number;
    rowEnd?: number;
    decodedCache?: { cache: DecodedColumnCache; path: string; rowGroupStart: number };
  } = {},
): AsyncIterable<OwnedMemory<ParquetVectorBatch>> {
  validateParquetReadConcurrency(options);
  const batchSize = parquetBatchSize(options.batchSize);
  const requestedEnd = options.rowEnd ?? Number.MAX_SAFE_INTEGER;
  if (
    (options.rowStart !== undefined &&
      (!Number.isSafeInteger(options.rowStart) || options.rowStart < 0)) ||
    !Number.isSafeInteger(requestedEnd) ||
    requestedEnd < (options.rowStart ?? 0)
  )
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid aligned row range");
  const names = new Set(sources.map((source) => source.column));
  if (names.size !== sources.length)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Duplicate aligned column");
  const cursors = sources.map((source) => ({
    column: source.column,
    iterator: source.pages[Symbol.asyncIterator](),
    current: undefined as
      | { page: PreparedNumericColumnPage; owner: OwnedMemory<Vector> }
      | undefined,
    nextRow: undefined as number | undefined,
  }));
  try {
    if (cursors.length === 0 || requestedEnd === (options.rowStart ?? 0)) return;
    let nextOutputRow: number | undefined = options.rowStart;
    for (;;) {
      if (scope.disposed)
        throw new LakeqlError("LAKEQL_ABORTED", "Numeric alignment scope disposed");
      const replacements: { index: number; page: PreparedNumericColumnPage }[] = [];
      let ended = 0;
      for (const [index, cursor] of cursors.entries()) {
        if (cursor.current !== undefined) continue;
        const next = await cursor.iterator.next();
        if (next.done) {
          ended++;
          continue;
        }
        const page = next.value;
        if (
          !Number.isSafeInteger(page.rowOffset) ||
          page.rowOffset < 0 ||
          !Number.isSafeInteger(page.rowCount) ||
          page.rowCount <= 0 ||
          !Number.isSafeInteger(page.rowOffset + page.rowCount) ||
          (cursor.nextRow !== undefined && page.rowOffset !== cursor.nextRow)
        )
          throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Noncontiguous numeric page rows");
        replacements.push({ index, page });
      }
      if (ended > 0) {
        if (ended !== cursors.length)
          throw new LakeqlError(
            "LAKEQL_PARQUET_READ_ERROR",
            "Aligned columns have unequal row extents",
          );
        if (options.rowEnd !== undefined && nextOutputRow !== requestedEnd)
          throw new LakeqlError(
            "LAKEQL_PARQUET_READ_ERROR",
            "Aligned columns ended before the requested row range",
          );
        return;
      }
      if (replacements.length > 0) {
        const owners: OwnedMemory<Vector>[] = [];
        const missing: { index: number; page: PreparedNumericColumnPage; key?: string }[] = [];
        try {
          if (scope.disposed)
            throw new LakeqlError("LAKEQL_ABORTED", "Numeric alignment scope disposed");
          for (const [index, replacement] of replacements.entries()) {
            const cached = options.decodedCache;
            const column = cursors[replacement.index]?.column;
            if (column === undefined) throw new Error("Missing numeric cache column");
            const key =
              cached === undefined
                ? undefined
                : decodedColumnPageCacheKey({
                    path: cached.path,
                    byteLength: file.byteLength,
                    ...(file.etag === undefined ? {} : { etag: file.etag }),
                    column,
                    rowGroupStart: cached.rowGroupStart,
                    pageRowStart: cached.rowGroupStart + replacement.page.rowOffset,
                    pageRowEnd:
                      cached.rowGroupStart + replacement.page.rowOffset + replacement.page.rowCount,
                    pageOffset: replacement.page.page.bodyOffset,
                    compressedPageSize: replacement.page.page.header.compressed_page_size,
                  });
            let owner: OwnedMemory<Vector> | undefined;
            if (cached !== undefined && key !== undefined) {
              try {
                owner = cached.cache.acquireVectorOwned(key, scope.budget);
              } catch (error) {
                if (!isLakeqlError(error) || error.code !== "LAKEQL_BUDGET_EXCEEDED") throw error;
              }
            }
            if (key !== undefined) options.onCacheLookup?.(owner !== undefined);
            if (owner === undefined)
              missing.push({
                index,
                page: replacement.page,
                ...(key === undefined ? {} : { key }),
              });
            else owners[index] = owner;
          }
          if (missing.length > 0) {
            const plan = planNumericPageGroup(
              missing.map((entry) => entry.page),
              file,
            );
            if (plan === undefined)
              throw new LakeqlError(
                "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
                "Missing grouped numeric page capability",
              );
            let decoded: OwnedMemory<Vector>[] = [];
            // Queue root contention, but measure when the conservative bound
            // cannot fit this job even if other jobs finish.
            const measured =
              !scope.hasAdmission && plan.bytes > scope.budget.maxBytes - scope.budget.usedBytes
                ? await plan.measureOwned(file, scope)
                : undefined;
            const executionPlan = measured ?? plan;
            const decodeInto = (target: MemoryScope) =>
              executionPlan.executeOwned(file, target, options.maxConcurrentReads, options.decode);
            try {
              if (options.scheduler === undefined || scope.hasAdmission) {
                decoded = await decodeInto(scope);
              } else {
                const completion = await options.scheduler.run({
                  budget: scope.budget,
                  bytes: executionPlan.bytes,
                  purpose: "aligned numeric replacements",
                  signal: scope.signal,
                  async execute(stage) {
                    // Keep per-page ownership: a composite scheduled output would
                    // retain exhausted peers until the last aligned page retires.
                    decoded = await decodeInto(stage);
                  },
                });
                completion.release();
              }
            } catch (error) {
              // Cancellation can win after execution creates independent owners
              // but before the scheduler publishes its completion.
              for (const owner of decoded) owner.release();
              throw error;
            } finally {
              measured?.dispose();
            }
            // Transfer every result before cache insertion so all are released if insertion fails.
            for (const [i, entry] of missing.entries()) {
              const owner = decoded[i];
              if (owner === undefined) throw new Error("Missing numeric decoded owner");
              owners[entry.index] = owner;
            }
          }
          for (const [i, replacement] of replacements.entries()) {
            const owner = owners[i];
            if (owner === undefined || vectorLength(owner.value) !== replacement.page.rowCount)
              throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Decoded page row extent changed");
          }
          for (const entry of missing) {
            const owner = owners[entry.index];
            if (entry.key !== undefined && owner !== undefined)
              options.decodedCache?.cache.setVectorOwned(entry.key, owner);
          }
        } catch (error) {
          for (const owner of owners) owner?.release();
          throw error;
        }
        for (const [i, replacement] of replacements.entries()) {
          const cursor = cursors[replacement.index];
          const owner = owners[i];
          if (cursor === undefined || owner === undefined) {
            for (const owner of owners) owner.release();
            throw new Error("Missing aligned numeric owner");
          }
          cursor.current = { page: replacement.page, owner };
          cursor.nextRow = replacement.page.rowOffset + replacement.page.rowCount;
        }
      }
      const rowOffset = Math.max(
        nextOutputRow ?? 0,
        ...cursors.map((cursor) => cursor.current?.page.rowOffset ?? 0),
      );
      const rowEnd = Math.min(requestedEnd, ...cursors.map((cursor) => cursor.nextRow ?? 0));
      if (
        nextOutputRow === undefined
          ? cursors.some((cursor) => cursor.current?.page.rowOffset !== rowOffset)
          : nextOutputRow !== rowOffset
      )
        throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Aligned numeric rows have a gap");
      if (rowEnd <= rowOffset)
        throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Numeric page ranges do not overlap");
      for (let start = rowOffset; start < rowEnd; start += batchSize) {
        const end = Math.min(start + batchSize, rowEnd);
        const vectors: Record<string, Vector> = {};
        const backing: OwnedMemory<Vector>[] = [];
        for (const cursor of cursors) {
          const current = cursor.current;
          if (current === undefined) throw new Error("Missing aligned page");
          vectors[cursor.column] = sliceVector(
            current.owner.value,
            start - current.page.rowOffset,
            end - current.page.rowOffset,
          );
          backing.push(current.owner);
        }
        const scoped = scope.retainBacking(
          { rowOffset: start, batch: batchFromVectors(vectors) },
          backing,
        );
        const output = scoped.retain();
        scoped.release();
        yield output;
      }
      if (rowEnd === requestedEnd) return;
      nextOutputRow = rowEnd;
      for (const cursor of cursors) {
        if (cursor.nextRow === rowEnd) {
          cursor.current?.owner.release();
          cursor.current = undefined;
        }
      }
    }
  } finally {
    for (const cursor of cursors) cursor.current?.owner.release();
    await closeAsyncIterators(cursors.map((cursor) => cursor.iterator));
  }
}

/** Header-only V1/V2 page traversal for a flat numeric column. Skipped page bodies are
 * never loaded; the alignment iterator decides when admitted body reads begin.
 */
export async function* prepareNumericColumnPages(
  file: StoreAsyncBuffer,
  decoder: ColumnDecoder,
  extent: { start: number; end: number },
  scope: MemoryScope,
  rows: { start: number; end: number },
  cache?: { cache: DecodedColumnCache; path: string },
): AsyncIterable<PreparedNumericColumnPage> {
  if (
    !Number.isSafeInteger(rows.start) ||
    rows.start < 0 ||
    !Number.isSafeInteger(rows.end) ||
    rows.end < rows.start
  )
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid numeric source row range");
  if (rows.start === rows.end) return;
  let offset = 0,
    rowOffset = 0;
  while (extent.start + offset < extent.end && rowOffset < rows.end) {
    const page = cache
      ? await cachedPageWindow(file, extent, offset, scope, cache)
      : await preparePageWindow(file, extent.start, extent.end, offset, scope);
    if (page === undefined) break;
    offset = page.bodyEnd - extent.start;
    const rowCount =
      page.header.type === "DATA_PAGE_V2"
        ? page.header.data_page_header_v2?.num_rows
        : page.header.type === "DATA_PAGE"
          ? page.header.data_page_header?.num_values
          : undefined;
    if (rowCount === undefined)
      throw new LakeqlError(
        "LAKEQL_UNSUPPORTED_PARQUET_FEATURE",
        "Numeric header source requires a data page",
      );
    if (
      !Number.isSafeInteger(rowCount) ||
      rowCount < 0 ||
      !Number.isSafeInteger(rowOffset + rowCount)
    )
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid numeric source row count");
    if (rowCount > 0 && rowOffset + rowCount > rows.start)
      yield { page, decoder, rowOffset, rowCount };
    rowOffset += rowCount;
  }
  if (rowOffset < rows.end)
    throw new LakeqlError(
      "LAKEQL_PARQUET_READ_ERROR",
      "Numeric column ended before the requested row range",
    );
}
