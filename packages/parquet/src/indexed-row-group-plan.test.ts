import { parquetMetadata } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { gte, MemoryBudget, MemoryScope, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { readParquetFooterEnvelope } from "./incremental-metadata.js";
import { openIndexedParquetEnvelope } from "./indexed-footer.js";
import { planOwnedIndexedRowGroups } from "./indexed-row-group-plan.js";
import { planRowGroupsFromMetadata } from "./row-group-plan.js";
import { asyncBufferFromStore } from "./store-buffer.js";

async function fixture() {
  const bytes = parquetWriteBuffer({
    rowGroupSize: Array(20).fill(3),
    columnData: [{ name: "id", type: "INT32", data: Array.from({ length: 60 }, (_, i) => i) }],
  });
  const store = memoryStore();
  await store.put("groups.parquet", new Uint8Array(bytes));
  const file = await asyncBufferFromStore(store, "groups.parquet");
  const footerBytes = new DataView(bytes).getUint32(bytes.byteLength - 8, true) + 8;
  const model = new MemoryScope(new MemoryBudget(Number.MAX_SAFE_INTEGER));
  const envelope = readParquetFooterEnvelope(bytes, undefined, model);
  const envelopeBytes = model.budget.usedBytes;
  const indexed = openIndexedParquetEnvelope(envelope, model);
  const sourceCapacity = bytes.byteLength + model.budget.usedBytes;
  indexed.readRowGroup(0, model);
  const capacity = bytes.byteLength + model.budget.peakBytes;
  const groupBytes = bytes.byteLength + model.budget.usedBytes - sourceCapacity;
  model.dispose();
  return { bytes, file, capacity, sourceCapacity, envelopeBytes, groupBytes, footerBytes };
}

it("plans actual file ranges at exact raw-metadata capacity and releases backing", async () => {
  const { bytes, file, capacity } = await fixture();
  const oracle = parquetMetadata(bytes);
  const read = file.sliceOwned?.bind(file);
  if (!read) throw new Error("Fixture requires owned reads");
  let requests = 0;
  file.sliceOwned = (...args) => {
    requests++;
    return read(...args);
  };
  for (const where of [undefined, gte("id", 28), gte("id", 100)]) {
    const budget = new MemoryBudget(capacity);
    requests = 0;
    expect(await planOwnedIndexedRowGroups(file, budget, where)).toEqual(
      planRowGroupsFromMetadata(oracle, where),
    );
    expect(requests).toBe(1);
    expect(budget.usedBytes).toBe(0);
  }
  const ample = new MemoryBudget(capacity * 2);
  requests = 0;
  expect(await planOwnedIndexedRowGroups(file, ample)).toEqual(planRowGroupsFromMetadata(oracle));
  expect(requests).toBe(1);
  expect(ample.usedBytes).toBe(0);
  const tight = new MemoryBudget(capacity - 1);
  await expect(planOwnedIndexedRowGroups(file, tight)).rejects.toThrow(/budget/);
  expect(tight.usedBytes).toBe(0);
});

it("releases acquisition on continuation failure and performs no reads after prior abort", async () => {
  const { file, footerBytes, envelopeBytes } = await fixture();
  const capacity = footerBytes + envelopeBytes + 160;
  const read = file.sliceOwned?.bind(file);
  if (!read) throw new Error("Fixture requires owned reads");
  let requests = 0;
  file.sliceOwned = (...args) => {
    if (++requests === 2) return Promise.reject(new Error("continuation failed"));
    return read(...args);
  };
  const budget = new MemoryBudget(capacity);
  await expect(planOwnedIndexedRowGroups(file, budget)).rejects.toThrow("continuation failed");
  expect(budget.usedBytes).toBe(0);
  const controller = new AbortController();
  controller.abort();
  requests = 0;
  await expect(
    planOwnedIndexedRowGroups(file, budget, undefined, [controller.signal]),
  ).rejects.toThrow();
  expect(requests).toBe(0);
  expect(budget.usedBytes).toBe(0);
});

it("keeps concurrent planners within child and shared pool limits", async () => {
  const { bytes, file, capacity } = await fixture();
  const read = file.sliceOwned?.bind(file);
  if (!read) throw new Error("Fixture requires owned reads");
  const pool = new MemoryBudget(capacity * 2);
  const jobs = [pool.child(capacity), pool.child(capacity)];
  let arrivals = 0;
  let unblock = () => {};
  const barrier = new Promise<void>((resolve) => {
    unblock = resolve;
  });
  file.sliceOwned = async (...args) => {
    const owner = await read(...args);
    // Hold both initial reads simultaneously to exercise real overlapping charges.
    if (arrivals < 2) {
      if (++arrivals === 2) unblock();
      await barrier;
    }
    return owner;
  };
  const plans = await Promise.all(jobs.map((job) => planOwnedIndexedRowGroups(file, job)));
  for (const plan of plans) expect(plan).toEqual(planRowGroupsFromMetadata(parquetMetadata(bytes)));
  expect(arrivals).toBe(2);
  expect(pool.peakBytes).toBeLessThanOrEqual(capacity * 2);
  expect(pool.peakBytes).toBeGreaterThan(capacity);
  expect(pool.usedBytes).toBe(0);
  for (const job of jobs) {
    expect(job.peakBytes).toBe(capacity);
    expect(job.usedBytes).toBe(0);
  }
});

it("keeps acquired source backing alive until disposal and follows parent scope closure", async () => {
  const { MemoryScope } = await import("lakeql-core");
  const { acquireIndexedParquetSource } = await import("./indexed-source.js");
  const { bytes, file, capacity, sourceCapacity } = await fixture();
  const oracle = parquetMetadata(bytes);
  for (const close of ["source", "parent", "abort"]) {
    const budget = new MemoryBudget(capacity);
    const parent = new MemoryScope(budget);
    const controller = new AbortController();
    const source = await acquireIndexedParquetSource(file, parent, [controller.signal]);
    expect(source.rowGroupCount).toBe(20);
    expect(source.readRowGroup(19)).toEqual(oracle.row_groups[19]);
    expect(budget.usedBytes).toBe(sourceCapacity);
    if (close === "abort") controller.abort();
    else if (close === "parent") parent.dispose();
    else source.dispose();
    expect(budget.usedBytes).toBe(0);
    expect(() => source.readRowGroup(0)).toThrow(/closed/);
    expect(() => source.metadata).toThrow(/closed/);
    expect(() => source.rowGroupCount).toThrow(/closed/);
    source.dispose();
    parent.dispose();
    expect(budget.usedBytes).toBe(0);
  }
});

it("reuses shared encoded footer cache across jobs and survives eviction while source is live", async () => {
  const { SharedMemoryCache } = await import("lakeql-core");
  const { cachedRangeBuffer } = await import("./range-cache.js");
  const { acquireIndexedParquetSource } = await import("./indexed-source.js");
  const { bytes, file, capacity, sourceCapacity } = await fixture();
  const read = file.sliceOwned?.bind(file);
  if (!read) throw new Error("Fixture requires owned reads");
  let requests = 0;
  file.sliceOwned = (...args) => {
    requests++;
    return read(...args);
  };
  const pool = new MemoryBudget(capacity * 4);
  const sharedCache = new SharedMemoryCache({ maxBytes: bytes.byteLength, memoryBudget: pool });
  const cached = () =>
    cachedRangeBuffer(
      file,
      {
        maxBytes: bytes.byteLength,
        coalesceBytes: 0,
        sharedCache,
      },
      "indexed-source-fixture",
    );
  const firstBudget = pool.child(capacity * 2);
  const first = await acquireIndexedParquetSource(cached(), firstBudget);
  expect(requests).toBe(1);
  first.dispose();
  expect(firstBudget.usedBytes).toBe(0);
  expect(pool.usedBytes).toBe(bytes.byteLength);
  const secondBudget = pool.child(capacity * 2);
  const second = await acquireIndexedParquetSource(cached(), secondBudget);
  expect(requests).toBe(1);
  sharedCache.deleteWhere(() => true);
  expect(pool.usedBytes).toBe(sourceCapacity);
  expect(second.readRowGroup(19)).toEqual(parquetMetadata(bytes).row_groups[19]);
  second.dispose();
  expect(secondBudget.usedBytes).toBe(0);
  expect(pool.usedBytes).toBe(0);
});

it("sizes footer probes for copying adapters instead of exhausting output headroom", async () => {
  const { bytes, file, capacity } = await fixture();
  const read = file.sliceOwned?.bind(file);
  if (!read) throw new Error("Fixture requires owned reads");
  const lengths: number[] = [];
  file.ownedReadAllocationBytes = (start, end) => (end - start) * 2;
  file.sliceOwned = async (start, end, scope) => {
    lengths.push(end - start);
    const input = await read(start, end, scope);
    try {
      return scope.allocate(end - start, "copying adapter output", () => input.value.slice(0));
    } finally {
      input.release();
    }
  };
  const budget = new MemoryBudget(capacity * 2);
  expect(await planOwnedIndexedRowGroups(file, budget)).toEqual(
    planRowGroupsFromMetadata(parquetMetadata(bytes)),
  );
  expect(lengths[0]).toBeLessThanOrEqual(capacity);
  expect(budget.peakBytes).toBeLessThanOrEqual(capacity * 2);
  expect(budget.usedBytes).toBe(0);
});

it("releases indexed planning backing when a streaming consumer exits or throws", async () => {
  const { iterateOwnedIndexedRowGroups } = await import("./indexed-row-group-plan.js");
  const { bytes, file, capacity, sourceCapacity, groupBytes } = await fixture();
  for (const fail of [false, true]) {
    const budget = new MemoryBudget(capacity);
    const consume = async () => {
      for await (const group of iterateOwnedIndexedRowGroups(file, budget)) {
        expect(group).toEqual(planRowGroupsFromMetadata(parquetMetadata(bytes)).rowGroups[0]);
        expect(budget.usedBytes).toBe(sourceCapacity + groupBytes);
        if (fail) throw new Error("consumer failure");
        break;
      }
    };
    if (fail) await expect(consume()).rejects.toThrow("consumer failure");
    else await consume();
    expect(budget.usedBytes).toBe(0);
  }
});

it("decodes indexed row groups with bounded prefetch and global row offsets", async () => {
  const { materializeBatchRows, SharedMemoryCache } = await import("lakeql-core");
  const { acquireIndexedParquetSource } = await import("./indexed-source.js");
  const { readOwnedParquetVectorBatches } = await import("./owned-vector-batches.js");
  const { cachedRangeBuffer } = await import("./range-cache.js");
  const { bytes, file } = await fixture();
  const budget = new MemoryBudget(bytes.byteLength * 8);
  const cache = new SharedMemoryCache({ maxBytes: bytes.byteLength * 2, memoryBudget: budget });
  const cached = cachedRangeBuffer(
    file,
    { maxBytes: bytes.byteLength * 2, sharedCache: cache, coalesceBytes: 0 },
    "indexed-vectors",
  );
  const indexed = await acquireIndexedParquetSource(cached, budget);
  const options = { columns: ["id"], batchSize: 2, rowStart: 4, rowEnd: 13, maxConcurrentReads: 2 };
  const collect = async (metadata: Parameters<typeof readOwnedParquetVectorBatches>[1]) => {
    const result = [];
    for await (const owner of readOwnedParquetVectorBatches(cached, metadata, budget, options)) {
      try {
        result.push({
          offset: owner.value.rowOffset,
          rows: materializeBatchRows(owner.value.batch),
        });
      } finally {
        owner.release();
      }
    }
    return result;
  };
  try {
    const visited: number[] = [];
    const result = await collect({
      metadata: indexed.metadata,
      rowGroupCount: indexed.rowGroupCount,
      readRowGroup(index) {
        visited.push(index);
        return indexed.readRowGroup(index);
      },
    });
    expect(Math.max(...visited)).toBe(4);
    expect(result).toEqual(await collect(parquetMetadata(bytes)));
    expect(result.flatMap((batch) => batch.rows)).toEqual(
      Array.from({ length: 9 }, (_, i) => ({ id: i + 4 })),
    );
    expect(result[0]?.offset).toBe(4);
  } finally {
    indexed.dispose();
    cache.deleteWhere(() => true);
  }
  expect(budget.usedBytes).toBe(0);
});

it.each([
  "complete",
  "break",
  "throw",
])("releases owned group metadata across demand and prefetch (%s)", async (exit) => {
  const { MemoryScope, materializeBatchRows } = await import("lakeql-core");
  const { readOwnedParquetVectorBatches } = await import("./owned-vector-batches.js");
  const { bytes, file } = await fixture();
  const full = parquetMetadata(bytes);
  const budget = new MemoryBudget(1024 * 1024);
  const scope = new MemoryScope(budget);
  let live = 0;
  let peak = 0;
  let acquired = 0;
  const source = {
    metadata: full,
    rowGroupCount: full.row_groups.length,
    readRowGroup() {
      throw new Error("Owned source must be used");
    },
    acquireRowGroup(index: number) {
      const owner = scope.allocate(1, "test metadata lifetime token", () => {
        const group = full.row_groups[index];
        if (group === undefined) throw new Error("Missing fixture row group");
        return group;
      });
      acquired++;
      peak = Math.max(peak, ++live);
      let released = false;
      return {
        ...owner,
        release() {
          if (!released) {
            released = true;
            live--;
          }
          owner.release();
        },
      };
    },
  };
  // A prefetch-capable file exercises the second, independently suspended
  // metadata iterator without conflating metadata tokens with network caching.
  file.prefetchOwned = async () => {};
  const rows: unknown[] = [];
  const consume = async () => {
    for await (const output of readOwnedParquetVectorBatches(file, source, budget, {
      columns: ["id"],
      batchSize: 2,
      maxConcurrentReads: 2,
    })) {
      try {
        rows.push(...materializeBatchRows(output.value.batch));
        if (exit === "throw") throw new Error("consumer failure");
        if (exit === "break") break;
      } finally {
        output.release();
      }
    }
  };
  try {
    if (exit === "throw") await expect(consume()).rejects.toThrow("consumer failure");
    else await consume();
    expect(acquired).toBeGreaterThan(1);
    expect(peak).toBe(2);
    expect(live).toBe(0);
    expect(budget.usedBytes).toBe(0);
    if (exit === "complete") expect(rows).toEqual(Array.from({ length: 60 }, (_, id) => ({ id })));
  } finally {
    scope.dispose();
  }
});

it("pins acquired group footer backing independently of source disposal", async () => {
  const { acquireIndexedParquetSource } = await import("./indexed-source.js");
  const { bytes, file, groupBytes } = await fixture();
  const budget = new MemoryBudget(65536);
  const source = await acquireIndexedParquetSource(file, budget);
  const expected = parquetMetadata(bytes).row_groups[0];
  const first = source.acquireRowGroup(0);
  const second = source.acquireRowGroup(1);
  const before = budget.usedBytes;
  expect(first.bytes).toBeGreaterThan(0);
  expect(() => source.acquireRowGroup(-1)).toThrow();
  expect(budget.usedBytes).toBe(before);
  source.dispose();
  expect(first.value).toEqual(expected);
  expect(budget.usedBytes).toBe(first.bytes + groupBytes);
  expect(budget.usedBytes).toBeLessThan(before);
  expect(() => source.acquireRowGroup(0)).toThrow(/closed/);
  first.release();
  first.release();
  expect(budget.usedBytes).toBe(second.bytes);
  second.release();
  expect(budget.usedBytes).toBe(0);
});
