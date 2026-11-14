import {
  LakeqlError,
  type MemoryScope,
  type ObjectInfo,
  type ObjectStore,
  type OwnedMemory,
  type ScanOptions,
  throwIfAborted,
  withObjectStoreReadControls,
} from "lakeql-core";
import type { StoreAsyncBuffer } from "./types.js";

/**
 * Bridge an ObjectStore path to hyparquet's AsyncBuffer (length + ranged slice).
 */
export async function asyncBufferFromStore(
  store: ObjectStore,
  path: string,
  options: ScanOptions | undefined = undefined,
) {
  const controlledStore =
    options === undefined ? store : withObjectStoreReadControls(store, options.budget);
  throwIfAborted(options?.budget.signal);
  const headStartedAt = options?.now();
  const head = await controlledStore.head(path);
  if (options !== undefined && headStartedAt !== undefined) {
    options.stats.objectStoreWaitMs =
      (options.stats.objectStoreWaitMs ?? 0) + (options.now() - headStartedAt);
  }
  if (!head) {
    throw new LakeqlError("LAKEQL_OBJECT_NOT_FOUND", `No object at ${path}`, { path });
  }
  return storeAsyncBuffer(
    controlledStore,
    path,
    head.size,
    head.etag,
    options,
    ownedStore(store, options),
  );
}

export function asyncBufferFromObjectInfo(
  store: ObjectStore,
  object: ObjectInfo,
  options: ScanOptions | undefined = undefined,
): StoreAsyncBuffer {
  const controlledStore =
    options === undefined ? store : withObjectStoreReadControls(store, options.budget);
  return storeAsyncBuffer(
    controlledStore,
    object.path,
    object.size,
    object.etag,
    options,
    ownedStore(store, options),
  );
}

function ownedStore(store: ObjectStore, options: ScanOptions | undefined): ObjectStore {
  // Owned I/O follows the supplied stage lifetime, which may have several query
  // waiters. The initiating query must not own cancellation of that shared read.
  return options?.budget.maxConcurrentReads === undefined
    ? store
    : withObjectStoreReadControls(store, { maxConcurrentReads: options.budget.maxConcurrentReads });
}

function storeAsyncBuffer(
  store: ObjectStore,
  path: string,
  byteLength: number,
  etag: string | undefined,
  options: ScanOptions | undefined,
  ownedSource: ObjectStore,
): StoreAsyncBuffer {
  if (!Number.isSafeInteger(byteLength) || byteLength < 0)
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid Parquet object size", {
      path,
      byteLength,
    });
  function validateRange(start: number, end: number): void {
    if (
      !Number.isSafeInteger(start) ||
      !Number.isSafeInteger(end) ||
      start < 0 ||
      end < start ||
      end > byteLength
    )
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid Parquet byte range", {
        path,
        start,
        end,
        byteLength,
      });
  }
  function beginRead(length: number, signal = options?.budget.signal): number | undefined {
    if (options) {
      throwIfAborted(signal);
      options.stats.rangeRequests += 1;
      options.stats.bytesRequested += length;
      options.stats.physicalBytesRequested = (options.stats.physicalBytesRequested ?? 0) + length;
      enforcePhysicalReadBudget(options);
    }
    return options?.now();
  }
  function endRead(started: number | undefined, signal = options?.budget.signal): void {
    if (options !== undefined && started !== undefined) {
      options.stats.objectStoreWaitMs =
        (options.stats.objectStoreWaitMs ?? 0) + (options.now() - started);
    }
    throwIfAborted(signal);
  }
  const ownedRead = ownedSource.getRangeOwned?.bind(ownedSource);
  const buffer: StoreAsyncBuffer = {
    byteLength,
    ...(ownedRead === undefined
      ? {}
      : {
          // The object-store result and a possible contiguous ArrayBuffer copy.
          ownedReadAllocationBytes: (start: number, end: number) => {
            validateRange(start, end);
            const forecast = ownedSource.getRangeOwnedAllocation?.(path, {
              offset: start,
              length: end - start,
            });
            if (forecast === undefined) return 2 * (end - start);
            if (!Number.isSafeInteger(forecast.bytes) || forecast.bytes < 0)
              throw new LakeqlError(
                "LAKEQL_VALIDATION_ERROR",
                "Invalid owned store allocation forecast",
              );
            return forecast.bytes + (forecast.output === "exact-buffer" ? 0 : end - start);
          },
          async sliceOwned(
            start: number,
            end: number,
            scope: MemoryScope,
          ): Promise<OwnedMemory<ArrayBuffer>> {
            validateRange(start, end);
            const started = beginRead(end - start, scope.signal);
            const owner = await ownedRead(path, { offset: start, length: end - start }, scope);
            try {
              endRead(started, scope.signal);
              const bytes = owner.value;
              if (
                bytes.buffer instanceof ArrayBuffer &&
                bytes.byteOffset === 0 &&
                bytes.byteLength === bytes.buffer.byteLength
              ) {
                return arrayBufferOwner(owner, bytes.buffer);
              }
              const output = scope.allocate(bytes.byteLength, "Parquet contiguous range", () =>
                bytesToArrayBuffer(bytes),
              );
              owner.release();
              return output;
            } catch (error) {
              owner.release();
              throw error;
            }
          },
        }),
    slice: async (start: number, end = byteLength): Promise<ArrayBuffer> => {
      validateRange(start, end);
      const length = end - start;
      const started = beginRead(length);
      const bytes = await store.getRange(path, { offset: start, length });
      endRead(started);
      return bytesToArrayBuffer(bytes);
    },
  };
  if (etag !== undefined) buffer.etag = etag;
  return buffer;
}

function enforcePhysicalReadBudget(options: ScanOptions): void {
  const { budget, stats } = options;
  if (budget.maxRangeRequests !== undefined && stats.rangeRequests > budget.maxRangeRequests) {
    throwReadBudget("range requests", budget.maxRangeRequests, stats.rangeRequests, stats);
  }
  if (budget.maxBytes !== undefined && stats.bytesRequested > budget.maxBytes) {
    throwReadBudget("bytes", budget.maxBytes, stats.bytesRequested, stats);
  }
}

function throwReadBudget(
  metric: string,
  limit: number,
  actual: number,
  stats: ScanOptions["stats"],
): never {
  throw new LakeqlError(
    "LAKEQL_BUDGET_EXCEEDED",
    `Query exceeded ${metric} budget (${actual} > ${limit}). Add a partition filter, date filter, h3 filter, or limit.`,
    {
      metric,
      limit,
      actual,
      measurements: { ...stats, columnsRead: [...stats.columnsRead] },
    },
  );
}

function bytesToArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  if (bytes.buffer instanceof ArrayBuffer) {
    if (bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength) {
      return bytes.buffer;
    }
    return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  }
  const out = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(out).set(bytes);
  return out;
}

function arrayBufferOwner(
  owner: OwnedMemory<Uint8Array>,
  value: ArrayBuffer,
): OwnedMemory<ArrayBuffer> {
  return {
    value,
    bytes: owner.bytes,
    budget: owner.budget,
    retain: () => arrayBufferOwner(owner.retain(), value),
    retainIn: (budget) => arrayBufferOwner(owner.retainIn(budget), value),
    release: () => owner.release(),
  };
}
