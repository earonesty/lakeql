import { detachOwnedRow, type MemoryScope, type ObjectStore } from "lakeql-core";
import { readCachedParquetMetadata, readOwnedParquetMetadata } from "./metadata-cache.js";
import { cachedRangeBuffer } from "./range-cache.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import {
  aggregateScanOptions,
  enforceAggregateTaskBudget,
  type ScanParquetTaskOptions,
} from "./task.js";
import type { ParquetMetadata, StoreAsyncBuffer } from "./types.js";

/** Task execution shares ordinary scan read controls, budgets and range caching. */
export async function taskSource(
  store: ObjectStore,
  path: string,
  options: ScanParquetTaskOptions,
): Promise<StoreAsyncBuffer> {
  const scan = aggregateScanOptions(options);
  enforceAggregateTaskBudget(scan);
  const file = await asyncBufferFromStore(store, path, scan);
  enforceAggregateTaskBudget(scan);
  return options.scanRangeCache === undefined
    ? file
    : cachedRangeBuffer(file, options.scanRangeCache, path);
}

/** Retain admitted parsed metadata for the enclosing scan, including cache pins. */
export async function taskMetadataOwned(
  path: string,
  file: StoreAsyncBuffer,
  scope: MemoryScope,
  options: ScanParquetTaskOptions,
): Promise<{ metadata: ParquetMetadata; cached: boolean }> {
  const scan = aggregateScanOptions(options);
  enforceAggregateTaskBudget(scan);
  if (file.sliceOwned === undefined) {
    // Legacy reads/parser allocation are external. Admit an independent retained
    // metadata copy, without claiming pre-allocation control of that source.
    const { metadata, cached } = await readCachedParquetMetadata(path, file, options.metadataCache);
    enforceAggregateTaskBudget(scan);
    const owner = detachOwnedRow({ metadata }, scope.budget);
    try {
      return { metadata: scope.adopt(owner).value.metadata as ParquetMetadata, cached };
    } catch (error) {
      owner.release();
      throw error;
    }
  }
  const { owner, cached } = await readOwnedParquetMetadata(
    path,
    file,
    scope.budget,
    options.metadataCache,
    options.budget?.signal === undefined ? [] : [options.budget.signal],
  );
  try {
    enforceAggregateTaskBudget(scan);
    return { metadata: scope.adopt(owner).value, cached };
  } catch (error) {
    owner.release();
    throw error;
  }
}
