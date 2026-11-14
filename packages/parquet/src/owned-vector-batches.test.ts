import { parquetWriteBuffer } from "hyparquet-writer";
import {
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  memoryStore,
  SharedMemoryCache,
} from "lakeql-core";
import { expect, it, vi } from "vitest";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { asyncBufferFromStore } from "./store-buffer.js";

async function fixture(groups: number, rowsPerGroup = 4) {
  const values = Array.from({ length: groups * rowsPerGroup }, (_, index) => index % rowsPerGroup);
  const bytes = parquetWriteBuffer({
    codec: "UNCOMPRESSED",
    rowGroupSize: Array.from({ length: groups }, () => rowsPerGroup),
    columnData: [{ name: "id", type: "INT32", encoding: "PLAIN", data: values }],
  });
  const store = memoryStore();
  await store.put("owned.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "owned.parquet");
  const metadata = await readParquetMetadataFromFile(source);
  return { source, metadata, values };
}

it.each([
  true,
  false,
])("releases completed groups without peak growth (direct=%s)", async (direct) => {
  const peaks: number[] = [];
  for (const groups of [1, 20]) {
    const { source, metadata, values } = await fixture(groups);
    const budget = new MemoryBudget(65536);
    const rows = [];
    for await (const owner of readOwnedParquetVectorBatches(source, metadata, budget, {
      ...(direct ? { columns: ["id"] } : {}),
      batchSize: 2,
    })) {
      expect(owner.value.rowOffset).toBe(rows.length);
      rows.push(...materializeBatchRows(owner.value.batch));
      owner.release();
    }
    expect(rows).toEqual(values.map((id) => ({ id })));
    expect(budget.usedBytes).toBe(0);
    peaks.push(budget.peakBytes);
  }
  expect(peaks[1]).toBe(peaks[0]);
});

it("keeps yielded backing charged after early iterator return until receiver release", async () => {
  const { source, metadata } = await fixture(3);
  const budget = new MemoryBudget(65536);
  const iterator = readOwnedParquetVectorBatches(source, metadata, budget, {
    columns: ["id"],
    rowStart: 5,
    rowEnd: 10,
    batchSize: 1,
  })[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done) throw new Error("missing output");
  expect(first.value.value.rowOffset).toBe(5);
  const retained = budget.usedBytes;
  expect(retained).toBeGreaterThan(0);
  await iterator.return?.();
  expect(budget.usedBytes).toBe(retained);
  expect(materializeBatchRows(first.value.value.batch)).toEqual([{ id: 1 }]);
  first.value.release();
  expect(budget.usedBytes).toBe(0);
});

it.each([
  true,
  false,
])("cleans up failed admission and cancellation (direct=%s)", async (direct) => {
  const { source, metadata } = await fixture(3);
  const tight = new MemoryBudget(1);
  await expect(async () => {
    for await (const owner of readOwnedParquetVectorBatches(source, metadata, tight, {
      ...(direct ? { columns: ["id"] } : {}),
    }))
      owner.release();
  }).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(tight.usedBytes).toBe(0);
  const controller = new AbortController();
  const budget = new MemoryBudget(65536);
  const iterator = readOwnedParquetVectorBatches(
    source,
    metadata,
    budget,
    {
      ...(direct ? { columns: ["id"] } : {}),
      batchSize: 1,
    },
    [controller.signal],
  )[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done) throw new Error("missing output");
  controller.abort();
  await expect(iterator.next()).rejects.toThrow();
  expect(budget.usedBytes).toBeGreaterThan(0);
  first.value.release();
  expect(budget.usedBytes).toBe(0);
});

it("runs multiple groups at the measured exact cap and rejects one byte less cleanly", async () => {
  const { source, metadata } = await fixture(5);
  async function run(budget: MemoryBudget) {
    let rows = 0;
    for await (const owner of readOwnedParquetVectorBatches(source, metadata, budget, {
      columns: ["id"],
    })) {
      rows += owner.value.batch.rowCount;
      owner.release();
    }
    return rows;
  }
  const measured = new MemoryBudget(65536);
  expect(await run(measured)).toBe(20);
  const exact = new MemoryBudget(measured.peakBytes);
  expect(await run(exact)).toBe(20);
  expect(exact.usedBytes).toBe(0);
  const tight = new MemoryBudget(measured.peakBytes - 1);
  await expect(run(tight)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(tight.usedBytes).toBe(0);
});

it("prefetches bounded row-group windows and closes each prefetch scope", async () => {
  const { source, metadata } = await fixture(5);
  const windows: number[] = [];
  const scopes: import("lakeql-core").MemoryScope[] = [];
  const file = {
    ...source,
    async prefetchOwned(
      ranges: readonly { start: number; end: number }[],
      scope: import("lakeql-core").MemoryScope,
    ) {
      windows.push(ranges.length);
      scopes.push(scope);
    },
  };
  const budget = new MemoryBudget(65536);
  for await (const owner of readOwnedParquetVectorBatches(file, metadata, budget, {
    columns: ["id"],
    maxConcurrentReads: 2,
  }))
    owner.release();
  expect(windows).toEqual([2, 2, 1]);
  expect(scopes.every((scope) => scope.disposed)).toBe(true);
  expect(budget.usedBytes).toBe(0);
});

it("skips speculative windows when other jobs consume shared headroom", async () => {
  const { source, metadata } = await fixture(3);
  const root = new MemoryBudget(65536);
  const probe = root.child();
  for await (const owner of readOwnedParquetVectorBatches(source, metadata, probe, {
    columns: ["id"],
  }))
    owner.release();
  const otherJob = root.reserve(root.maxBytes - probe.peakBytes, "other job");
  let prefetches = 0;
  const file = {
    ...source,
    async prefetchOwned() {
      prefetches += 1;
    },
  };
  const child = root.child();
  for await (const owner of readOwnedParquetVectorBatches(file, metadata, child, {
    columns: ["id"],
  }))
    owner.release();
  expect(prefetches).toBe(0);
  expect(child.usedBytes).toBe(0);
  otherJob.release();
  expect(root.usedBytes).toBe(0);
});

it.each([
  0,
  -1,
  1.5,
  Infinity,
  NaN,
])("rejects an unbounded/invalid prefetch window %s", async (maxConcurrentReads) => {
  const { source, metadata } = await fixture(1);
  let reads = 0;
  const file = {
    ...source,
    async prefetchOwned() {
      reads += 1;
    },
  };
  await expect(async () => {
    for await (const owner of readOwnedParquetVectorBatches(
      file,
      metadata,
      new MemoryBudget(65536),
      {
        columns: ["id"],
        maxConcurrentReads,
      },
    ))
      owner.release();
  }).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
  expect(reads).toBe(0);
});

it("does not advance prefetch windows after an early consumer exit", async () => {
  const { source, metadata } = await fixture(8);
  let windows = 0;
  const file = {
    ...source,
    async prefetchOwned() {
      windows += 1;
    },
  };
  const budget = new MemoryBudget(65536);
  for await (const owner of readOwnedParquetVectorBatches(file, metadata, budget, {
    columns: ["id"],
    maxConcurrentReads: 2,
    batchSize: 1,
  })) {
    owner.release();
    break;
  }
  expect(windows).toBe(1);
  expect(budget.usedBytes).toBe(0);
});

it("plans only the lookahead window before the first output", async () => {
  const { source, metadata } = await fixture(8);
  const third = metadata.row_groups[2];
  if (!third) throw new Error("missing third group");
  Object.defineProperty(third, "columns", {
    get() {
      throw new Error("planned beyond window");
    },
  });
  const file = { ...source, async prefetchOwned() {} };
  const budget = new MemoryBudget(65536);
  for await (const owner of readOwnedParquetVectorBatches(file, metadata, budget, {
    columns: ["id"],
    maxConcurrentReads: 2,
  })) {
    expect(owner.value.rowOffset).toBe(0);
    owner.release();
    break;
  }
  expect(budget.usedBytes).toBe(0);
});

it("propagates prefetch transport failure and releases its allocations", async () => {
  const { source, metadata } = await fixture(3);
  const budget = new MemoryBudget(65536);
  const failure = new Error("remote range failed");
  const file = {
    ...source,
    async prefetchOwned(
      _ranges: readonly { start: number; end: number }[],
      scope: import("lakeql-core").MemoryScope,
    ) {
      scope.allocate(128, "prefetch response", () => new Uint8Array(128));
      throw failure;
    },
  };
  await expect(async () => {
    for await (const owner of readOwnedParquetVectorBatches(file, metadata, budget, {
      columns: ["id"],
    }))
      owner.release();
  }).rejects.toBe(failure);
  expect(budget.peakBytes).toBe(128);
  expect(budget.usedBytes).toBe(0);
});

it("does not initiate speculative work for an already cancelled job", async () => {
  const { source, metadata } = await fixture(3);
  const controller = new AbortController();
  controller.abort();
  let calls = 0;
  const file = {
    ...source,
    async prefetchOwned() {
      calls += 1;
    },
  };
  const budget = new MemoryBudget(65536);
  await expect(async () => {
    for await (const owner of readOwnedParquetVectorBatches(
      file,
      metadata,
      budget,
      { columns: ["id"] },
      [controller.signal],
    ))
      owner.release();
  }).rejects.toThrow();
  expect(calls).toBe(0);
  expect(budget.peakBytes).toBe(0);
});

it("reuses owned decoded pages across jobs and preserves pins after cache eviction", async () => {
  const { source, metadata, values } = await fixture(3);
  const root = new MemoryBudget(65536);
  const shared = new SharedMemoryCache({ maxBytes: 32768, memoryBudget: root });
  const cache = new DecodedColumnCache(shared, { maxBytes: 32768, policy: "latency" });
  const writes = vi.spyOn(cache, "setVectorOwned");
  const options = {
    columns: ["id"],
    batchSize: 1,
    decodedColumnCache: cache,
    decodedColumnCacheKey: "fixture",
  };
  const cold = root.child();
  const rows = [];
  for await (const owner of readOwnedParquetVectorBatches(source, metadata, cold, options)) {
    rows.push(...materializeBatchRows(owner.value.batch));
    owner.release();
  }
  expect(rows).toEqual(values.map((id) => ({ id })));
  expect(writes).toHaveBeenCalledTimes(3);
  expect(cold.usedBytes).toBe(0);
  expect(root.usedBytes).toBeGreaterThan(0);
  writes.mockClear();
  const warm = root.child();
  const iterator = readOwnedParquetVectorBatches(source, metadata, warm, options)[
    Symbol.asyncIterator
  ]();
  const first = await iterator.next();
  if (first.done) throw new Error("missing cached output");
  await iterator.return?.();
  expect(writes).not.toHaveBeenCalled();
  expect(warm.usedBytes).toBeGreaterThan(0);
  shared.deleteWhere(() => true);
  expect(root.usedBytes).toBeGreaterThan(0);
  expect(materializeBatchRows(first.value.value.batch)).toEqual([{ id: 0 }]);
  first.value.release();
  expect(warm.usedBytes).toBe(0);
  expect(root.usedBytes).toBe(0);
});

it("re-decodes a real page when its cached conservative backing exceeds the job limit", async () => {
  const { source, metadata, values } = await fixture(1);
  const root = new MemoryBudget(65536);
  const shared = new SharedMemoryCache({ maxBytes: 32768, memoryBudget: root });
  const cache = new DecodedColumnCache(shared, { maxBytes: 32768, policy: "latency" });
  const writes = vi.spyOn(cache, "setVectorOwned");
  const options = {
    columns: ["id"],
    decodedColumnCache: cache,
    decodedColumnCacheKey: "large-backing",
  };
  const coldSource = {
    ...source,
    ownedReadAllocationBytes(start: number, end: number) {
      return 2048 + (source.ownedReadAllocationBytes?.(start, end) ?? 2 * (end - start));
    },
    async sliceOwned(start: number, end: number, scope: import("lakeql-core").MemoryScope) {
      // Model an input stage retaining unrelated transport/codec workspace.
      scope.allocate(2048, "stage workspace", () => new Uint8Array(2048));
      if (!source.sliceOwned) throw new Error("missing owned read");
      return source.sliceOwned(start, end, scope);
    },
  };
  for await (const owner of readOwnedParquetVectorBatches(
    coldSource,
    metadata,
    root.child(),
    options,
  ))
    owner.release();
  expect(writes).toHaveBeenCalledTimes(1);
  expect(root.usedBytes).toBeGreaterThan(2048);
  writes.mockClear();
  const smallJob = root.child(1024);
  const rows = [];
  for await (const owner of readOwnedParquetVectorBatches(source, metadata, smallJob, options)) {
    rows.push(...materializeBatchRows(owner.value.batch));
    owner.release();
  }
  expect(rows).toEqual(values.map((id) => ({ id })));
  expect(writes).toHaveBeenCalledTimes(1);
  expect(smallJob.peakBytes).toBeLessThanOrEqual(1024);
  expect(smallJob.usedBytes).toBe(0);
  shared.deleteWhere(() => true);
  expect(root.usedBytes).toBe(0);
});

it("preserves absolute row offsets and requested bounds in the general owned fallback", async () => {
  const { source, metadata, values } = await fixture(4);
  const budget = new MemoryBudget(65536);
  const rows = [];
  for await (const owner of readOwnedParquetVectorBatches(source, metadata, budget, {
    rowStart: 5,
    rowEnd: 11,
    batchSize: 2,
  })) {
    expect(owner.value.rowOffset).toBe(5 + rows.length);
    expect(owner.value.batch.rowCount).toBeLessThanOrEqual(2);
    rows.push(...materializeBatchRows(owner.value.batch));
    owner.release();
  }
  expect(rows).toEqual(values.slice(5, 11).map((id) => ({ id })));
  expect(budget.usedBytes).toBe(0);
});

it("keeps cancelled in-flight prefetch charged until allocation work settles", async () => {
  const { source, metadata } = await fixture(2);
  const budget = new MemoryBudget(65536);
  const controller = new AbortController();
  let finish!: () => void;
  let entered!: () => void;
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const started = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const file = {
    ...source,
    async prefetchOwned(
      _ranges: readonly { start: number; end: number }[],
      scope: import("lakeql-core").MemoryScope,
    ) {
      await scope.allocateAsync(128, "pending transport buffer", async () => {
        entered();
        await pending;
        return new Uint8Array(128);
      });
    },
  };
  const iterator = readOwnedParquetVectorBatches(file, metadata, budget, { columns: ["id"] }, [
    controller.signal,
  ])[Symbol.asyncIterator]();
  const result = iterator.next();
  const rejected = expect(result).rejects.toThrow();
  await started;
  controller.abort();
  expect(budget.usedBytes).toBe(128);
  finish();
  await rejected;
  expect(budget.usedBytes).toBe(0);
  expect((await iterator.next()).done).toBe(true);
});

it("does not transfer decoded-cache charges between independent allocation pools", async () => {
  const { source, metadata, values } = await fixture(2);
  const cachePool = new MemoryBudget(65536);
  const otherPool = new MemoryBudget(65536);
  const shared = new SharedMemoryCache({ memoryBudget: cachePool, maxBytes: 32768 });
  const cache = new DecodedColumnCache(shared, { maxBytes: 32768, policy: "latency" });
  const options = {
    columns: ["id"],
    decodedColumnCache: cache,
    decodedColumnCacheKey: "independent-pools",
  };
  for await (const owner of readOwnedParquetVectorBatches(
    source,
    metadata,
    cachePool.child(),
    options,
  ))
    owner.release();
  const cachedBytes = cachePool.usedBytes;
  expect(cachedBytes).toBeGreaterThan(0);
  const rows = [];
  for await (const owner of readOwnedParquetVectorBatches(
    source,
    metadata,
    otherPool.child(),
    options,
  )) {
    rows.push(...materializeBatchRows(owner.value.batch));
    owner.release();
  }
  expect(rows).toEqual(values.map((id) => ({ id })));
  expect(otherPool.usedBytes).toBe(0);
  expect(cachePool.usedBytes).toBe(cachedBytes);
  shared.deleteWhere(() => true);
  expect(cachePool.usedBytes).toBe(0);
});

it.each([
  64, 128,
])("enforces a nearly full %i MiB pool across retained decoded jobs", async (mib) => {
  const { source, metadata, values } = await fixture(1);
  const options = { columns: ["id"], batchSize: 4 };
  const probe = new MemoryBudget(65536);
  for await (const owner of readOwnedParquetVectorBatches(source, metadata, probe, options))
    owner.release();
  const root = new MemoryBudget(mib * 1024 * 1024);
  const cache = new SharedMemoryCache({ memoryBudget: root, maxBytes: root.maxBytes });
  const producer = new MemoryScope(root.child());
  const backgroundBytes = root.maxBytes - probe.peakBytes;
  const background = producer.allocate(backgroundBytes, "live cache payload", () => {
    const bytes = new Uint8Array(backgroundBytes);
    bytes.fill(7);
    return bytes;
  });
  expect(cache.setOwned("shared-data", background)).toBe(true);
  const activeCacheReader = cache.acquireOwned<Uint8Array>("shared-data");
  if (!activeCacheReader) throw new Error("missing cache pin");
  producer.dispose();
  const firstBudget = root.child();
  const firstIterator = readOwnedParquetVectorBatches(source, metadata, firstBudget, options)[
    Symbol.asyncIterator
  ]();
  const first = await firstIterator.next();
  if (first.done) throw new Error("missing first output");
  await firstIterator.return?.();
  expect(root.usedBytes).toBe(root.maxBytes);
  expect(root.peakBytes).toBe(root.maxBytes);
  const secondBudget = root.child();
  await expect(async () => {
    for await (const owner of readOwnedParquetVectorBatches(
      source,
      metadata,
      secondBudget,
      options,
    ))
      owner.release();
  }).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(secondBudget.usedBytes).toBe(0);
  expect(materializeBatchRows(first.value.value.batch)).toEqual(values.map((id) => ({ id })));
  expect(activeCacheReader.value[0]).toBe(7);
  expect(activeCacheReader.value[backgroundBytes - 1]).toBe(7);
  first.value.release();
  expect(firstBudget.usedBytes).toBe(0);
  const rows = [];
  for await (const owner of readOwnedParquetVectorBatches(
    source,
    metadata,
    secondBudget,
    options,
  )) {
    rows.push(...materializeBatchRows(owner.value.batch));
    owner.release();
  }
  expect(rows).toEqual(values.map((id) => ({ id })));
  expect(secondBudget.usedBytes).toBe(0);
  activeCacheReader.release();
  cache.deleteWhere(() => true);
  expect(root.usedBytes).toBe(0);
});

it("bounds fallback lifetime by one batch within the same physical row group", async () => {
  const { source, metadata, values } = await fixture(1, 128);
  async function run(budget: MemoryBudget, rowEnd: number) {
    const rows = [];
    for await (const owner of readOwnedParquetVectorBatches(source, metadata, budget, {
      batchSize: 1,
      rowEnd,
    })) {
      rows.push(...materializeBatchRows(owner.value.batch));
      owner.release();
    }
    expect(budget.usedBytes).toBe(0);
    return rows;
  }
  const single = new MemoryBudget(65536);
  expect(await run(single, 1)).toEqual([{ id: 0 }]);
  const all = new MemoryBudget(single.peakBytes);
  expect(await run(all, values.length)).toEqual(values.map((id) => ({ id })));
  expect(all.peakBytes).toBe(single.peakBytes);
  const tight = new MemoryBudget(single.peakBytes - 1);
  await expect(run(tight, values.length)).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  expect(tight.usedBytes).toBe(0);
});

it("retains an earlier fallback batch independently of advancing and closing the reader", async () => {
  const { source, metadata } = await fixture(1);
  const budget = new MemoryBudget(65536);
  const iterator = readOwnedParquetVectorBatches(source, metadata, budget, {
    batchSize: 1,
  })[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done) throw new Error("missing first batch");
  const firstBytes = first.value.bytes;
  const second = await iterator.next();
  if (second.done) throw new Error("missing second batch");
  expect(materializeBatchRows(second.value.value.batch)).toEqual([{ id: 1 }]);
  second.value.release();
  await iterator.return?.();
  expect(budget.usedBytes).toBe(firstBytes);
  expect(materializeBatchRows(first.value.value.batch)).toEqual([{ id: 0 }]);
  first.value.release();
  expect(budget.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("releases direct page decode backing within one row group (window=%s)", async (canStopEarly) => {
  const { readParquetVectorBatchesFromFile } = await import("./vector-batches.js");
  const values = Array.from({ length: 256 }, (_, index) => index * 3);
  const bytes = parquetWriteBuffer({
    codec: "UNCOMPRESSED",
    pageSize: 64,
    rowGroupSize: [values.length],
    columnData: [{ name: "id", type: "INT32", encoding: "PLAIN", data: values }],
  });
  const store = memoryStore();
  await store.put("pages.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "pages.parquet");
  const metadata = await readParquetMetadataFromFile(source);
  async function run(maxBytes: number) {
    const budget = new MemoryBudget(maxBytes);
    const scope = new MemoryScope(budget);
    const rows = [];
    let firstPeak = 0;
    try {
      for await (const output of readParquetVectorBatchesFromFile(source, metadata, {
        columns: ["id"],
        canStopEarly,
        batchSize: 8,
        allocationScope: scope,
        pageAllocationLifetime: "iteration",
      })) {
        if (firstPeak === 0) firstPeak = budget.peakBytes;
        rows.push(...materializeBatchRows(output.batch));
      }
      expect(rows).toEqual(values.map((id) => ({ id })));
      expect(budget.usedBytes).toBe(0);
      expect(budget.peakBytes).toBe(firstPeak);
      return firstPeak;
    } finally {
      scope.dispose();
      expect(budget.usedBytes).toBe(0);
    }
  }
  const peak = await run(65536);
  expect(peak).toBeGreaterThan(0);
  expect(peak).toBeLessThan(values.length * 4);
  await run(peak);
  await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });

  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  const iterator = readParquetVectorBatchesFromFile(source, metadata, {
    columns: ["id"],
    canStopEarly,
    batchSize: 8,
    allocationScope: scope,
    pageAllocationLifetime: "iteration",
  })[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done) throw new Error("missing first page");
  const retained = scope.retainAllocations(first.value.batch);
  try {
    while (!(await iterator.next()).done) {
      /* Advance through every subsequent page. */
    }
    scope.dispose();
    expect(budget.usedBytes).toBeGreaterThan(0);
    expect(materializeBatchRows(retained.value)).toEqual(values.slice(0, 8).map((id) => ({ id })));
  } finally {
    await iterator.return?.();
    scope.dispose();
    retained.release();
  }
  expect(budget.usedBytes).toBe(0);
});

it("does not prefetch entire column chunks for an early-stop owned scan", async () => {
  const { source, metadata } = await fixture(5, 1000);
  const prefetchOwned = vi.fn(async () => {});
  const budget = new MemoryBudget(65536);
  let rows = 0;
  for await (const owner of readOwnedParquetVectorBatches(
    { ...source, prefetchOwned },
    metadata,
    budget,
    {
      columns: ["id"],
      batchSize: 1,
      canStopEarly: true,
      maxConcurrentReads: 4,
    },
  )) {
    expect(materializeBatchRows(owner.value.batch)).toEqual([{ id: 0 }]);
    rows += owner.value.batch.rowCount;
    owner.release();
    break;
  }
  expect(rows).toBe(1);
  expect(prefetchOwned).not.toHaveBeenCalled();
  expect(budget.usedBytes).toBe(0);
});

it("keeps borrowed early-stop reads demand-driven", async () => {
  const { readParquetVectorBatchesFromFile } = await import("./vector-batches.js");
  const { source, metadata } = await fixture(5, 1000);
  const prefetch = vi.fn(async () => {});
  let rows = 0;
  for await (const output of readParquetVectorBatchesFromFile({ ...source, prefetch }, metadata, {
    columns: ["id"],
    batchSize: 1,
    canStopEarly: true,
    maxConcurrentReads: 4,
  })) {
    expect(materializeBatchRows(output.batch)).toEqual([{ id: 0 }]);
    rows += output.batch.rowCount;
    break;
  }
  expect(rows).toBe(1);
  expect(prefetch).not.toHaveBeenCalled();
});

it.each([
  0,
  -1,
  1.5,
  Number.NaN,
  Number.POSITIVE_INFINITY,
])("validates borrowed concurrency even when early-stop disables prefetch (%s)", async (maxConcurrentReads) => {
  const { readParquetVectorBatchesFromFile } = await import("./vector-batches.js");
  const { source, metadata } = await fixture(1);
  const iterator = readParquetVectorBatchesFromFile(source, metadata, {
    columns: ["id"],
    canStopEarly: true,
    maxConcurrentReads,
  })[Symbol.asyncIterator]();
  await expect(iterator.next()).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
});

it.each([
  "PLAIN",
  "DELTA_BINARY_PACKED",
] as const)("owns dense decimal conversion from real %s pages", async (encoding) => {
  const data = [-12.34, null, 0, 56.78, -12.34];
  const bytes = parquetWriteBuffer({
    codec: "SNAPPY",
    schema: [
      { name: "schema", num_children: 1 },
      {
        name: "amount",
        type: "INT64",
        converted_type: "DECIMAL",
        precision: 15,
        scale: 2,
        repetition_type: "OPTIONAL",
      },
    ],
    columnData: [{ name: "amount", data, encoding }],
  });
  const store = memoryStore();
  await store.put("decimal.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "decimal.parquet");
  const metadata = await readParquetMetadataFromFile(source);
  const budget = new MemoryBudget(65536);
  const rows = [];
  for await (const owner of readOwnedParquetVectorBatches(source, metadata, budget, {
    columns: ["amount"],
    batchSize: 2,
  })) {
    const vector = owner.value.batch.columns.amount;
    expect(vector?.type === "f64" || vector?.type === "dict").toBe(true);
    rows.push(...materializeBatchRows(owner.value.batch));
    owner.release();
  }
  expect(rows).toEqual(data.map((amount) => ({ amount })));
  expect(budget.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("admits complete final vectors during ordinary scans (window=%s)", async (canStopEarly) => {
  const data = Array.from({ length: 40 }, (_, i) => (i % 3 === 0 ? null : i));
  const bytes = parquetWriteBuffer({
    codec: "SNAPPY",
    pageSize: 64,
    columnData: [{ name: "id", type: "INT32", encoding: "PLAIN", data }],
  });
  const store = memoryStore();
  await store.put("admitted-vectors.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "admitted-vectors.parquet");
  const metadata = await readParquetMetadataFromFile(source);
  const stage = vi.spyOn(MemoryScope.prototype, "childStage");
  const budget = new MemoryBudget(65536);
  try {
    const rows = [];
    for await (const owner of readOwnedParquetVectorBatches(source, metadata, budget, {
      columns: ["id"],
      batchSize: 3,
      canStopEarly,
    })) {
      rows.push(...materializeBatchRows(owner.value.batch));
      owner.release();
    }
    expect(rows).toEqual(data.map((id) => ({ id })));
    expect(
      stage.mock.calls.some(
        ([bytes, purpose]) => bytes > 0 && purpose === "complete Parquet numeric vector",
      ),
    ).toBe(true);
    expect(budget.usedBytes).toBe(0);
  } finally {
    stage.mockRestore();
  }
});

it.each([
  false,
  true,
])("aligns unequal numeric page boundaries under a measured cap (window=%s)", async (canStopEarly) => {
  const rows = Array.from({ length: 257 }, (_, i) => ({
    id: i,
    amount: i % 3 === 0 ? null : i + 0.25,
  }));
  const bytes = parquetWriteBuffer({
    codec: "SNAPPY",
    pageSize: 64,
    columnData: [
      { name: "id", type: "INT32", encoding: "PLAIN", data: rows.map((row) => row.id) },
      { name: "amount", type: "DOUBLE", encoding: "PLAIN", data: rows.map((row) => row.amount) },
    ],
  });
  const store = memoryStore();
  await store.put("unequal-pages.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "unequal-pages.parquet");
  const metadata = await readParquetMetadataFromFile(source);
  const { deserializeTCompactProtocol } = await import("hyparquet/src/thrift.js");
  const boundaries = metadata.row_groups[0]?.columns.map((column) => {
    const chunk = column.meta_data;
    if (!chunk) throw new Error("Missing column metadata");
    const reader = { view: new DataView(bytes), offset: Number(chunk.data_page_offset) };
    let row = 0;
    const ends = [];
    while (row < rows.length) {
      const raw = deserializeTCompactProtocol(reader) as {
        field_3: number;
        field_8: { field_3: number };
      };
      row += raw.field_8.field_3;
      ends.push(row);
      reader.offset += raw.field_3;
    }
    return ends;
  });
  expect(boundaries?.[0]).not.toEqual(boundaries?.[1]);
  async function run(cap: number, batchSize: number, budget = new MemoryBudget(cap)) {
    const result = [];
    for await (const owner of readOwnedParquetVectorBatches(source, metadata, budget, {
      columns: ["id", "amount"],
      batchSize,
      canStopEarly,
    })) {
      expect(owner.value.rowOffset).toBe(result.length);
      expect(owner.value.batch.rowCount).toBeLessThanOrEqual(batchSize);
      result.push(...materializeBatchRows(owner.value.batch));
      owner.release();
    }
    expect(result).toEqual(rows);
    expect(budget.usedBytes).toBe(0);
    return budget.peakBytes;
  }
  for (const batchSize of [1, 13]) {
    const peak = await run(1 << 20, batchSize);
    expect(await run(peak, batchSize)).toBeLessThanOrEqual(peak);
    const shared = new MemoryBudget(2 * peak);
    await Promise.all([run(peak, batchSize, shared.child()), run(peak, batchSize, shared.child())]);
    expect(shared.peakBytes).toBeLessThanOrEqual(2 * peak);
    expect(shared.usedBytes).toBe(0);
  }
  const budget = new MemoryBudget(1 << 20);
  const iterator = readOwnedParquetVectorBatches(source, metadata, budget, {
    columns: ["id", "amount"],
    batchSize: 13,
    canStopEarly,
  })[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done) throw new Error("Missing first aligned batch");
  await iterator.return();
  expect(budget.usedBytes).toBeGreaterThan(0);
  expect(materializeBatchRows(first.value.value.batch)).toEqual(
    rows.slice(0, first.value.value.batch.rowCount),
  );
  first.value.release();
  expect(budget.usedBytes).toBe(0);
});

it.each([
  "failure",
  "cancel",
] as const)("preserves retained aligned output during replacement %s", async (mode) => {
  const rows = Array.from({ length: 100 }, (_, i) => ({ a: i, b: i + 100 }));
  const bytes = parquetWriteBuffer({
    codec: "SNAPPY",
    pageSize: 64,
    columnData: [
      { name: "a", type: "INT32", encoding: "PLAIN", data: rows.map((row) => row.a) },
      { name: "b", type: "INT32", encoding: "PLAIN", data: rows.map((row) => row.b) },
    ],
  });
  const store = memoryStore();
  await store.put("replacement.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "replacement.parquet");
  const metadata = await readParquetMetadataFromFile(source);
  const budget = new MemoryBudget(65536);
  const controller = new AbortController();
  let arm = false;
  let entered!: () => void;
  let finish!: () => void;
  const started = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const failure = new Error("replacement read failed");
  const file = {
    ...source,
    async sliceOwned(start: number, end: number, scope: MemoryScope) {
      if (arm) {
        arm = false;
        await scope.allocateAsync(64, "pending replacement backing", async () => {
          entered();
          await pending;
          if (mode === "failure") throw failure;
          return new Uint8Array(64);
        });
      }
      if (!source.sliceOwned) throw new Error("Missing owned read");
      return source.sliceOwned(start, end, scope);
    },
  };
  const iterator = readOwnedParquetVectorBatches(
    file,
    metadata,
    budget,
    {
      columns: ["a", "b"],
      batchSize: 13,
      canStopEarly: true,
    },
    [controller.signal],
  )[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done) throw new Error("Missing aligned output");
  const expected = rows.slice(0, first.value.value.batch.rowCount);
  arm = true;
  const drain = (async () => {
    for (;;) {
      const next = await iterator.next();
      if (next.done) return;
      next.value.release();
    }
  })();
  const rejected =
    mode === "failure" ? expect(drain).rejects.toBe(failure) : expect(drain).rejects.toThrow();
  await started;
  if (mode === "cancel") controller.abort();
  expect(budget.usedBytes).toBeGreaterThan(64);
  expect(materializeBatchRows(first.value.value.batch)).toEqual(expected);
  finish();
  await rejected;
  expect(materializeBatchRows(first.value.value.batch)).toEqual(expected);
  expect(budget.usedBytes).toBeGreaterThan(0);
  first.value.release();
  expect(budget.usedBytes).toBe(0);
});

it("skips out-of-range page body reads before decode", async () => {
  const values = Array.from({ length: 1024 }, (_, i) => i);
  const bytes = parquetWriteBuffer({
    codec: "SNAPPY",
    pageSize: 128,
    columnData: [{ name: "id", type: "INT32", encoding: "PLAIN", data: values }],
  });
  const store = memoryStore();
  await store.put("skip-page-bodies.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "skip-page-bodies.parquet");
  const metadata = await readParquetMetadataFromFile(source);
  const column = metadata.row_groups[0]?.columns[0]?.meta_data;
  if (!column) throw new Error("Missing metadata");
  const { deserializeTCompactProtocol } = await import("hyparquet/src/thrift.js");
  const reader = { view: new DataView(bytes), offset: Number(column.data_page_offset) };
  const excluded: [number, number][] = [];
  let row = 0;
  while (row < 500) {
    const header = deserializeTCompactProtocol(reader) as {
      field_3: number;
      field_8: { field_3: number };
    };
    row += header.field_8.field_3;
    if (row <= 500) excluded.push([reader.offset, reader.offset + header.field_3]);
    reader.offset += header.field_3;
  }
  expect(excluded.length).toBeGreaterThan(1);
  const reads: [number, number][] = [];
  const traced = {
    ...source,
    async sliceOwned(start: number, end: number, scope: MemoryScope) {
      reads.push([start, end]);
      if (!source.sliceOwned) throw new Error("Missing owned read");
      return source.sliceOwned(start, end, scope);
    },
  };
  const budget = new MemoryBudget(65536);
  const result = [];
  for await (const owner of readOwnedParquetVectorBatches(traced, metadata, budget, {
    columns: ["id"],
    rowStart: 500,
    rowEnd: 520,
    batchSize: 7,
    canStopEarly: true,
  })) {
    result.push(...materializeBatchRows(owner.value.batch));
    owner.release();
  }
  expect(result).toEqual(values.slice(500, 520).map((id) => ({ id })));
  for (const range of excluded) expect(reads).not.toContainEqual(range);
  expect(budget.usedBytes).toBe(0);
});

it.each([
  "incomplete",
  "invalid-type",
])("releases failed header probes with appropriate retry (%s)", async (kind) => {
  const { source, metadata } = await fixture(1, 1024);
  const budget = new MemoryBudget(65536);
  const before: number[] = [];
  const sizes: number[] = [];
  const corrupt = {
    ...source,
    async sliceOwned(start: number, end: number, scope: MemoryScope) {
      before.push(budget.usedBytes);
      sizes.push(end - start);
      return scope.allocate(end - start, "invalid header probe", () => {
        const bytes = new Uint8Array(end - start);
        if (kind === "invalid-type") bytes.fill(255);
        else bytes.set([0x98, 0x88, 0x27]); // Unknown binary field needs 5,000 bytes.
        return bytes.buffer;
      });
    },
  };
  await expect(async () => {
    for await (const owner of readOwnedParquetVectorBatches(corrupt, metadata, budget, {
      columns: ["id"],
      canStopEarly: true,
    }))
      owner.release();
  }).rejects.toThrow();
  if (kind === "incomplete") expect(sizes.length).toBeGreaterThan(1);
  else expect(sizes.length).toBe(1);
  expect(before.every((bytes) => bytes === 0)).toBe(true);
  expect(budget.peakBytes).toBe(Math.max(...sizes));
  expect(budget.usedBytes).toBe(0);
});

it("grows a genuinely incomplete header and then decodes its page", async () => {
  const { source, metadata } = await fixture(1, 1024);
  const chunk = metadata.row_groups[0]?.columns[0]?.meta_data;
  if (!chunk) throw new Error("Missing column metadata");
  const startOffset = Number(chunk.data_page_offset);
  // Valid V2 header plus an unknown 300-byte binary field. The data body is
  // one present INT32 value, with RLE definition levels preceding its bytes.
  const header = Uint8Array.from([
    0x15,
    6,
    0x15,
    12,
    0x15,
    12,
    0x5c,
    0x15,
    2,
    0x15,
    0,
    0x15,
    2,
    0x15,
    0,
    0x15,
    4,
    0x15,
    0,
    0x12,
    0,
    0x18,
    0xac,
    2,
    ...Array(300).fill(0),
    0,
  ]);
  const page = new Uint8Array(header.length + 6);
  page.set(header);
  page.set([2, 1, 42, 0, 0, 0], header.length);
  const sizes: number[] = [];
  const budget = new MemoryBudget(65536);
  const file = {
    ...source,
    async sliceOwned(start: number, end: number, scope: MemoryScope) {
      sizes.push(end - start);
      return scope.allocate(end - start, "long header fixture range", () => {
        const bytes = new Uint8Array(end - start);
        const relative = start - startOffset;
        bytes.set(page.subarray(relative, Math.min(page.length, relative + bytes.length)));
        return bytes.buffer;
      });
    },
  };
  const rows = [];
  for await (const owner of readOwnedParquetVectorBatches(file, metadata, budget, {
    columns: ["id"],
    rowEnd: 1,
    canStopEarly: true,
  })) {
    rows.push(...materializeBatchRows(owner.value.batch));
    owner.release();
  }
  expect(rows).toEqual([{ id: 42 }]);
  expect(sizes).toEqual([256, 1024, 6]);
  expect(budget.peakBytes).toBe(1024);
  expect(budget.usedBytes).toBe(0);
});

it.each([
  0,
  -1,
  1.5,
  NaN,
  Infinity,
  Number.MAX_SAFE_INTEGER + 1,
])("rejects invalid demand-page concurrency before routing or I/O (%s)", async (maxConcurrentPageReads) => {
  const { readParquetVectorBatchesFromFile } = await import("./vector-batches.js");
  const { source, metadata } = await fixture(1);
  let requests = 0;
  const unexpected = async (): Promise<never> => {
    requests++;
    throw new Error("Unexpected I/O before validation");
  };
  const file = { ...source, slice: unexpected, sliceOwned: unexpected, prefetchOwned: unexpected };
  for (const canStopEarly of [false, true]) {
    const options = { columns: ["id"], canStopEarly, maxConcurrentPageReads };
    const budget = new MemoryBudget(65536);
    const owned = readOwnedParquetVectorBatches(file, metadata, budget, options)[
      Symbol.asyncIterator
    ]();
    await expect(owned.next()).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    const borrowed = readParquetVectorBatchesFromFile(file, metadata, options)[
      Symbol.asyncIterator
    ]();
    await expect(borrowed.next()).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
    expect(budget.usedBytes).toBe(0);
  }
  expect(requests).toBe(0);
});

it("keeps schema lookup context off shared caller metadata", async () => {
  const { source, metadata, values } = await fixture(3);
  const { canReadParquetVectorBatches, iterateDirectVectorRanges } = await import(
    "./vector-batches.js"
  );
  const originalKeys = Reflect.ownKeys(metadata);
  Object.freeze(metadata);
  expect(canReadParquetVectorBatches(metadata, { columns: ["id"] })).toBe(true);
  expect([
    ...iterateDirectVectorRanges(metadata, ["id"], 0, values.length, undefined),
  ]).toHaveLength(3);
  for (let run = 0; run < 2; run++) {
    const budget = new MemoryBudget(65536);
    const rows: unknown[] = [];
    for await (const owned of readOwnedParquetVectorBatches(source, metadata, budget, {
      columns: ["id"],
      batchSize: 2,
    })) {
      rows.push(...materializeBatchRows(owned.value.batch));
      owned.release();
    }
    expect(rows).toEqual(values.map((id) => ({ id })));
    expect(budget.usedBytes).toBe(0);
  }
  expect(Reflect.ownKeys(metadata)).toEqual(originalKeys);
});

it("keeps emulated-range demand reads page-bounded when whole chunks exceed headroom", async () => {
  const values = Array.from({ length: 4096 }, (_, index) => index * 3);
  const store = memoryStore();
  await store.put(
    "emulated.parquet",
    new Uint8Array(
      parquetWriteBuffer({
        codec: "UNCOMPRESSED",
        pageSize: 64,
        rowGroupSize: [values.length],
        columnData: [{ name: "id", type: "INT32", encoding: "PLAIN", data: values }],
      }),
    ),
  );
  const source = await asyncBufferFromStore(store, "emulated.parquet");
  const metadata = await readParquetMetadataFromFile(source);
  const ownedRead = source.sliceOwned?.bind(source);
  if (!ownedRead) throw new Error("Missing owned source");
  const largest: number[] = [];
  for (const maxBytes of [4096, 65536]) {
    const budget = new MemoryBudget(maxBytes);
    let maxRange = 0;
    let count = 0;
    for await (const owner of readOwnedParquetVectorBatches(
      {
        ...source,
        sliceOwned(start, end, scope) {
          maxRange = Math.max(maxRange, end - start);
          return ownedRead(start, end, scope);
        },
      },
      metadata,
      budget,
      { columns: ["id"], batchSize: 8, canStopEarly: true, rangeReadMode: "emulated" },
    )) {
      const rows = materializeBatchRows(owner.value.batch);
      expect(rows).toEqual(values.slice(count, count + rows.length).map((id) => ({ id })));
      count += rows.length;
      owner.release();
    }
    expect(count).toBe(values.length);
    expect(budget.usedBytes).toBe(0);
    expect(budget.peakBytes).toBeLessThanOrEqual(maxBytes);
    largest.push(maxRange);
  }
  expect(largest[0]).toBeLessThan(4096);
  expect(largest[1]).toBeGreaterThan(values.length * 4);
});
