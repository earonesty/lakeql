import type { ObjectStore } from "lakeql-core";
import { cachedRangeBuffer } from "./range-cache.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import { aggregateScanOptions, type ScanParquetTaskOptions } from "./task.js";
import type { StoreAsyncBuffer } from "./types.js";

/** Task execution shares ordinary scan read controls, budgets and range caching. */
export async function taskSource(
  store: ObjectStore,
  path: string,
  options: ScanParquetTaskOptions,
): Promise<StoreAsyncBuffer> {
  const file = await asyncBufferFromStore(store, path, aggregateScanOptions(options));
  return options.scanRangeCache === undefined
    ? file
    : cachedRangeBuffer(file, options.scanRangeCache, path);
}
