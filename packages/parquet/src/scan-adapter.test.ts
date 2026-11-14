import {
  and,
  eq,
  gatherOwnedBatch,
  gt,
  Lake,
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  memoryCache,
  memoryStore,
  type QueryStats,
  SharedMemoryCache,
  type Vector,
} from "lakeql-core";
import { describe, expect, it } from "vitest";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { readParquetFooterEnvelope } from "./incremental-metadata.js";
import { writeParquet } from "./index.js";
import { openIndexedParquetEnvelope } from "./indexed-footer.js";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { ParquetScanAdapter } from "./scan-adapter.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import { countingObjectStore } from "./test-helpers.js";

describe("ParquetScanAdapter", () => {
  it("scans rows and vector batches with shared metadata, decoded, and range cache state", async () => {
    const backing = memoryStore();
    await writeParquet(backing, "data/adapter.parquet", {
      rowGroupSize: [3],
      columnData: [
        { name: "id", data: [1, 2, 3, 4, 5, 6], type: "INT32" },
        { name: "amount", data: [5, 10, 15, 20, 25, 30], type: "DOUBLE" },
        { name: "region", data: ["west", "west", "east", "west", "east", "west"], type: "STRING" },
      ],
    });
    const counted = countingObjectStore(backing);
    const metadataCache = memoryCache();
    const sharedCache = new SharedMemoryCache({ maxBytes: 8 * 1024 * 1024 });
    const decodedColumnCache = new DecodedColumnCache(sharedCache, {
      maxBytes: 8 * 1024 * 1024,
      policy: "latency",
    });
    const adapter = new ParquetScanAdapter(counted, {
      batchSize: 2,
      metadataCache,
      decodedColumnCache,
      scanRangeCache: {
        maxBytes: 8 * 1024 * 1024,
        sharedCache,
        cacheOptions: { policy: "io" },
      },
    });

    const rows = [];
    const rowStats = queryStats();
    for await (const batch of adapter.scan("data/adapter.parquet", {
      batchSize: 2,
      columns: ["id", "amount"],
      where: gt("amount", 18),
      stats: rowStats,
      budget: {},
      now: () => 0,
      startedAt: 0,
    })) {
      rows.push(...batch);
    }
    expect(rows).toEqual([
      { id: 4, amount: 20 },
      { id: 5, amount: 25 },
      { id: 6, amount: 30 },
    ]);
    expect(rowStats.rowGroupsSkipped).toBe(1);
    expect(rowStats.rowGroupsRead).toBe(1);
    expect(rowStats.columnsRead).toContain("id");

    const vectorRows = [];
    const vectorStats = queryStats();
    for await (const { rowOffset, batch } of adapter.scanVectorBatches("data/adapter.parquet", {
      batchSize: 4,
      columns: ["id", "region"],
      rowStart: 1,
      rowEnd: 5,
      where: and(gt("id", 1), eq("region", "west")),
      stats: vectorStats,
      budget: {},
      now: () => 0,
      startedAt: 0,
    })) {
      vectorRows.push({ rowOffset, rowCount: batch.rowCount, columns: Object.keys(batch.columns) });
    }
    expect(vectorRows).toEqual([
      { rowOffset: 1, rowCount: 2, columns: ["id", "region"] },
      { rowOffset: 3, rowCount: 2, columns: ["id", "region"] },
    ]);
    expect(vectorStats.cacheHits).toBeGreaterThanOrEqual(1);

    const columnRows = [];
    for await (const batch of adapter.scanColumns("data/adapter.parquet", {
      batchSize: 3,
      columns: ["amount"],
      rowStart: 0,
      rowEnd: 3,
      stats: queryStats(),
      budget: {},
      now: () => 0,
      startedAt: 0,
    })) {
      columnRows.push(batch.rowCount);
    }
    expect(columnRows).toEqual([3]);

    const plan = await adapter.planTask("data/adapter.parquet", {
      partitionValues: {},
      where: gt("amount", 18),
    });
    expect(plan).toEqual({ rowGroupCount: 2, rowGroupRanges: [{ start: 1, end: 2 }] });
    expect(counted.counters.get + counted.counters.getRange).toBeGreaterThan(0);
  });
});

function queryStats(): QueryStats {
  return {
    queryId: "scan-adapter-test",
    elapsedMs: 0,
    manifestsRead: 0,
    manifestsSkipped: 0,
    filesPlanned: 0,
    filesRead: 0,
    filesSkipped: 0,
    rowGroupsRead: 0,
    rowGroupsSkipped: 0,
    columnsRead: [],
    bytesRequested: 0,
    rangeRequests: 0,
    rowsDecoded: 0,
    rowsMatched: 0,
    rowsReturned: 0,
    cacheHits: 0,
    cacheMisses: 0,
  };
}

it.each([
  false,
  true,
])("hands off packed scanner ownership with explicit projection=%s", async (direct) => {
  const store = memoryStore();
  await writeParquet(store, "owned-adapter.parquet", {
    codec: "UNCOMPRESSED",
    rowGroupSize: [2, 2],
    columnData: [
      { name: "id", type: "INT32", encoding: "PLAIN", data: [1, 2, 3, 4] },
      { name: "name", type: "STRING", encoding: "PLAIN", data: ["é", null, "", "猫"] },
    ],
  });
  const adapter = new ParquetScanAdapter(store, {});
  if (adapter.scanOwnedVectorBatches === undefined)
    throw new Error("Owned scan capability missing");
  const memoryBudget = new MemoryBudget(65536);
  const options = {
    memoryBudget,
    batchSize: 1,
    budget: {},
    stats: queryStats(),
    now: () => 0,
    startedAt: 0,
    ...(direct ? { columns: ["id", "name"] } : {}),
  };
  const iterator = adapter
    .scanOwnedVectorBatches("owned-adapter.parquet", options)
    [Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done) throw new Error("missing output");
  expect(packedLeafType(first.value.value.batch.columns.name)).toBe("utf8-view");
  expect(Object.keys(first.value.value.batch.columns)).toEqual(["id", "name"]);
  await iterator.return?.();
  expect(memoryBudget.usedBytes).toBeGreaterThan(0);
  expect(materializeBatchRows(first.value.value.batch)).toEqual([{ id: 1, name: "é" }]);
  first.value.release();
  expect(memoryBudget.usedBytes).toBe(0);
  const rows = [];
  for await (const owner of adapter.scanOwnedVectorBatches("owned-adapter.parquet", options)) {
    rows.push(...materializeBatchRows(owner.value.batch));
    owner.release();
  }
  expect(rows).toEqual([
    { id: 1, name: "é" },
    { id: 2, name: null },
    { id: 3, name: "" },
    { id: 4, name: "猫" },
  ]);
  expect(memoryBudget.usedBytes).toBe(0);
});

it.each([
  { columns: undefined },
  { columns: ["raw"] },
])("preserves raw binary storage through Parquet readers (%j)", async ({ columns }) => {
  const store = memoryStore();
  const values = [Uint8Array.of(0, 255), null, new Uint8Array(), Uint8Array.of(128)];
  await writeParquet(store, "binary-fallback.parquet", {
    codec: "UNCOMPRESSED",
    schema: [
      { name: "root", num_children: 1 },
      { name: "raw", type: "BYTE_ARRAY", repetition_type: "OPTIONAL" },
    ],
    columnData: [{ name: "raw", encoding: "PLAIN", data: values }],
  });
  const adapter = new ParquetScanAdapter(store, {});
  if (adapter.scanOwnedVectorBatches === undefined)
    throw new Error("Owned scan capability missing");
  const budget = new MemoryBudget(65536);
  const rows = [];
  for await (const owner of adapter.scanOwnedVectorBatches("binary-fallback.parquet", {
    memoryBudget: budget,
    columns,
    batchSize: 4,
    budget: {},
    stats: queryStats(),
    now: () => 0,
    startedAt: 0,
  })) {
    if (columns === undefined)
      expect(packedLeafType(owner.value.batch.columns.raw)).toBe("binary-view");
    rows.push(...materializeBatchRows(owner.value.batch));
    owner.release();
  }
  expect(rows).toEqual(values.map((raw) => ({ raw })));
  expect(budget.usedBytes).toBe(0);
  const objectRows = [];
  for await (const batch of adapter.scan("binary-fallback.parquet", {
    columns,
    batchSize: 4,
    budget: {},
    stats: queryStats(),
    now: () => 0,
    startedAt: 0,
  }))
    objectRows.push(...batch);
  expect(objectRows).toEqual(values.map((raw) => ({ raw })));
});

it.each([
  false,
  true,
])("runs normal aggregate queries with bounded owned Parquet input (%s)", async (grouped) => {
  const store = memoryStore();
  await writeParquet(store, "aggregate-owned.parquet", {
    codec: "UNCOMPRESSED",
    rowGroupSize: [2, 2, 2],
    columnData: [{ name: "id", type: "INT32", data: [1, 2, 1, 2, 1, 2] }],
  });
  const run = async (limit: number) => {
    const pool = new MemoryBudget(limit);
    const adapter = new ParquetScanAdapter(store, {});
    if (adapter.scanOwnedVectorBatches === undefined)
      throw new Error("Owned scan capability missing");
    const lake = new Lake({ store, scanner: adapter, memoryPool: pool });
    try {
      const rows = await lake
        .path("aggregate-owned.parquet")
        .batchSize(1)
        .groupBy(grouped ? ["id"] : [])
        .aggregate({ total: { op: "sum", column: "id" } });
      expect(rows).toEqual(
        grouped
          ? [
              { id: 1, total: 3 },
              { id: 2, total: 6 },
            ]
          : [{ total: 9 }],
      );
      expect(pool.peakBytes).toBeGreaterThan(0);
      return pool.peakBytes;
    } finally {
      expect(pool.usedBytes).toBe(0);
    }
  };
  let upper = await run(65536);
  let lower = 0;
  // Arena capacity can be reclaimed under pressure, so the unconstrained peak
  // is not the minimum viable cap. Locate and verify the actual budget boundary.
  while (lower < upper) {
    const candidate = Math.floor((lower + upper) / 2);
    try {
      expect(await run(candidate)).toBeLessThanOrEqual(candidate);
      upper = candidate;
    } catch (error) {
      expect(error).toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
      lower = candidate + 1;
    }
  }
  expect(await run(upper)).toBeLessThanOrEqual(upper);
  await expect(run(upper - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});

it("does not advertise owned scans for a borrowed-only store", () => {
  const { getRangeOwned: _owned, ...store } = memoryStore();
  expect(new ParquetScanAdapter(store).scanOwnedVectorBatches).toBeUndefined();
});

it.each([
  false,
  true,
])("keeps gathered Parquet byte backing admitted after scanner shutdown (direct=%s)", async (direct) => {
  const store = memoryStore();
  await writeParquet(store, "gather-owned.parquet", {
    codec: "UNCOMPRESSED",
    schema: [
      { name: "root", num_children: 2 },
      { name: "raw", type: "BYTE_ARRAY", repetition_type: "OPTIONAL" },
      { name: "id", type: "INT32", repetition_type: "REQUIRED" },
    ],
    columnData: [
      {
        name: "id",
        encoding: "RLE_DICTIONARY",
        data: Array.from({ length: 9 }, (_, i) => 5 + (i % 3)),
      },
      {
        name: "raw",
        encoding: "RLE_DICTIONARY",
        data: Array.from({ length: 9 }, (_, index) =>
          index % 3 === 0 ? Uint8Array.of(255, 0) : index % 3 === 1 ? null : new Uint8Array(),
        ),
      },
    ],
  });
  const metadata = await readParquetMetadataFromFile(
    await asyncBufferFromStore(store, "gather-owned.parquet"),
  );
  expect(metadata.row_groups[0]?.columns[0]?.meta_data?.dictionary_page_offset).toBeDefined();
  const adapter = new ParquetScanAdapter(store);
  if (adapter.scanOwnedVectorBatches === undefined) throw new Error("Missing owned scanner");
  const root = new MemoryBudget(65536);
  const producer = root.child();
  const consumer = root.child();
  const iterator = adapter
    .scanOwnedVectorBatches("gather-owned.parquet", {
      ...(direct ? { columns: ["raw", "id"] } : {}),
      memoryBudget: producer,
      batchSize: 3,
      budget: {},
      stats: queryStats(),
      now: () => 0,
      startedAt: 0,
    })
    [Symbol.asyncIterator]();
  const next = await iterator.next();
  if (next.done) throw new Error("Missing batch");
  const bridge = new MemoryScope(producer);
  const input = bridge.retainBacking(next.value.value.batch, [next.value]);
  const output = gatherOwnedBatch(input, [2, 0, 1, 0], consumer);
  bridge.dispose();
  next.value.release();
  await iterator.return?.();
  expect(output.value.columns.raw?.type).toBe("binary-view");
  expect(producer.usedBytes).toBe(0);
  expect(consumer.usedBytes).toBeGreaterThan(16);
  expect(root.usedBytes).toBe(consumer.usedBytes);
  expect(materializeBatchRows(output.value)).toEqual([
    { raw: new Uint8Array(), id: 7 },
    { raw: Uint8Array.of(255, 0), id: 5 },
    { raw: null, id: 6 },
    { raw: Uint8Array.of(255, 0), id: 5 },
  ]);
  output.release();
  expect(root.usedBytes).toBe(0);
});

it("runs filtered top-k over real Parquet groups at the measured allocation cap", async () => {
  const store = memoryStore();
  await writeParquet(store, "top-k-owned.parquet", {
    codec: "UNCOMPRESSED",
    rowGroupSize: [2, 2, 2],
    columnData: [{ name: "id", type: "INT32", data: [7, 1, 5, 3, 9, 2] }],
  });
  async function run(maxBytes: number) {
    const pool = new MemoryBudget(maxBytes);
    const lake = new Lake({ store, scanner: new ParquetScanAdapter(store), memoryPool: pool });
    try {
      const rows = await lake
        .path("top-k-owned.parquet")
        .batchSize(1)
        .where(gt("id", 2))
        .orderBy([{ column: "id" }])
        .offset(1)
        .limit(2)
        .toArray();
      expect(rows).toEqual([{ id: 5 }, { id: 7 }]);
      return pool.peakBytes;
    } finally {
      expect(pool.usedBytes).toBe(0);
    }
  }
  const peak = await run(65536);
  expect(await run(peak)).toBe(peak);
  await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});

it("keeps legacy borrowed metadata caches on the compatible scan capability", () => {
  const cache = memoryCache<import("./types.js").ParquetMetadata>();
  expect(
    new ParquetScanAdapter(memoryStore(), { metadataCache: cache }).scanOwnedVectorBatches,
  ).toBeUndefined();
});

it("preserves all output columns when filter, order, or projections have no explicit select", async () => {
  const store = memoryStore();
  await writeParquet(store, "implicit-all.parquet", {
    codec: "UNCOMPRESSED",
    columnData: [
      { name: "id", type: "INT32", data: [2, 1, 3] },
      { name: "label", type: "STRING", data: ["two", "one", "three"] },
    ],
  });
  const lake = new Lake({ store, scanner: new ParquetScanAdapter(store) });
  expect(
    await lake
      .path("implicit-all.parquet")
      .where(gt("id", 1))
      .orderBy([{ column: "id" }])
      .limit(1)
      .toArray(),
  ).toEqual([{ id: 2, label: "two" }]);
  expect(
    await lake
      .path("implicit-all.parquet")
      .where(gt("id", 1))
      .project({ copy: { kind: "column", name: "id" } })
      .limit(1)
      .toArray(),
  ).toEqual([{ id: 2, label: "two", copy: 2 }]);
});

it("plans budgeted tasks from indexed groups with exact backing admission", async () => {
  const store = memoryStore();
  const path = "indexed-tasks.parquet";
  await writeParquet(store, path, {
    rowGroupSize: [2, 2, 2],
    columnData: [{ name: "id", type: "INT32", data: [0, 1, 9, 10, 2, 3] }],
  });
  const bytes = await store.get(path);
  if (!bytes) throw new Error("Missing fixture");
  const capacity = indexedPlanningCapacity(bytes);
  const adapter = new ParquetScanAdapter(store);
  const oracle = await adapter.planTask(path, { partitionValues: {}, where: gt("id", 5) });
  const stats = queryStats();
  const budget = new MemoryBudget(capacity);
  expect(
    await adapter.planTask(path, {
      partitionValues: {},
      where: gt("id", 5),
      memoryBudget: budget,
      stats,
    }),
  ).toEqual(oracle);
  expect(stats.rowGroupsTotal).toBe(3);
  expect(stats.rowGroupsPlanned).toBe(1);
  expect(budget.usedBytes).toBe(0);
  expect(budget.peakBytes).toBeLessThanOrEqual(capacity);
  const tight = new MemoryBudget(capacity - 1);
  await expect(
    adapter.planTask(path, { partitionValues: {}, memoryBudget: tight }),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(tight.usedBytes).toBe(0);
});

it("defaults lake task planning to indexed sources with reusable encoded ranges", async () => {
  const { createParquetLake } = await import("./index.js");
  const store = memoryStore();
  const path = "default-indexed-tasks.parquet";
  await writeParquet(store, path, {
    rowGroupSize: [2, 2],
    columnData: [{ name: "id", type: "INT32", data: [0, 1, 9, 10] }],
  });
  const bytes = await store.get(path);
  if (!bytes) throw new Error("Missing fixture");
  const read = store.getRangeOwned?.bind(store);
  if (!read) throw new Error("Owned fixture required");
  let requests = 0;
  store.getRangeOwned = (...args) => {
    requests++;
    return read(...args);
  };
  const run = async (capacity: number) => {
    requests = 0;
    const pool = new MemoryBudget(capacity);
    const sharedCache = new SharedMemoryCache({ maxBytes: bytes.byteLength, memoryBudget: pool });
    const lake = createParquetLake({
      store,
      memoryPool: pool,
      scanRangeCache: { maxBytes: bytes.byteLength, coalesceBytes: 0, sharedCache },
      queryId: () => "indexed-default",
    });
    const query = lake.path(path).where(gt("id", 5));
    const first = await query.planOwnedTasks();
    try {
      expect(first.value[0]?.rowGroupRanges).toEqual([{ start: 1, end: 2 }]);
      expect(first.bytes).toBeGreaterThan(0);
      expect(requests).toBe(1);
      const second = await query.planOwnedTasks();
      try {
        expect(second.value).toEqual(first.value);
        expect(requests).toBe(1);
      } finally {
        second.release();
      }
      sharedCache.deleteWhere(() => true);
      expect(pool.usedBytes).toBe(first.bytes);
      return pool.peakBytes;
    } finally {
      first.release();
      sharedCache.deleteWhere(() => true);
      expect(pool.usedBytes).toBe(0);
    }
  };
  // Include query task metadata and overlapping queued plans, not only the
  // adapter's indexed-footer workspace used by the lower-level tests above.
  const peak = await run(65536);
  expect(await run(peak)).toBe(peak);
});

it("produces missing-column batches from indexed schema-evolution sources", async () => {
  const store = memoryStore();
  await writeParquet(store, "old.parquet", {
    rowGroupSize: [2, 2],
    columnData: [{ name: "id", type: "INT32", data: [1, 2, 3, 4] }],
  });
  await writeParquet(store, "new.parquet", {
    columnData: [{ name: "added", type: "INT32", data: [9] }],
  });
  const adapter = new ParquetScanAdapter(store);
  const objects = [];
  for await (const object of store.list("")) objects.push(object);
  const planned = await adapter.planObjects(objects, {});
  const object = planned.find((entry) => entry.path === "old.parquet");
  if (!object || !adapter.scanOwnedVectorBatches) throw new Error("Missing planned owned fixture");
  const budget = new MemoryBudget(65536);
  const rows = [];
  for await (const owner of adapter.scanOwnedVectorBatches(object.path, {
    object,
    memoryBudget: budget,
    columns: ["added"],
    rowStart: 1,
    rowEnd: 3,
    batchSize: 1,
    budget: {},
    stats: queryStats(),
    now: () => 0,
    startedAt: 0,
  })) {
    try {
      rows.push({ offset: owner.value.rowOffset, rows: materializeBatchRows(owner.value.batch) });
    } finally {
      owner.release();
    }
  }
  expect(rows).toEqual([
    { offset: 1, rows: [{ added: null }] },
    { offset: 2, rows: [{ added: null }] },
  ]);
  expect(budget.usedBytes).toBe(0);
});

it.each([
  "auto",
  "indexed",
  "cached",
] as const)("preserves warm scan results and cache statistics (%s)", async (scanMetadataMode) => {
  const { createParquetLake } = await import("./index.js");
  const store = memoryStore();
  await writeParquet(store, "scan-policy.parquet", {
    rowGroupSize: [2, 2],
    columnData: [{ name: "id", type: "INT32", data: [0, 1, 2, 3] }],
  });
  const pool = new MemoryBudget(65536);
  const sharedCache = new SharedMemoryCache({ maxBytes: 32768, memoryBudget: pool });
  const lake = createParquetLake({
    store,
    memoryPool: pool,
    scanMetadataMode,
    scanRangeCache: { maxBytes: 32768, coalesceBytes: 0, sharedCache },
  });
  for (const warm of [false, true]) {
    const result = lake.path("scan-policy.parquet").select(["id"]).run();
    expect(await result.toArray()).toEqual([0, 1, 2, 3].map((id) => ({ id })));
    expect(result.stats.cacheHits).toBe(warm ? 1 : 0);
    expect(result.stats.cacheMisses).toBe(warm ? 0 : 1);
    if (warm) expect(result.stats.rangeRequests).toBe(0);
  }
  sharedCache.deleteWhere(() => true);
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "rows",
  "vectors",
  "owned",
] as const)("reuses matching planned object info without a second head (%s)", async (mode) => {
  const store = memoryStore();
  const path = "planned-info.parquet";
  await writeParquet(store, path, { columnData: [{ name: "id", type: "INT32", data: [1, 2] }] });
  const head = await store.head(path);
  if (!head) throw new Error("Missing fixture head");
  const originalHead = store.head.bind(store);
  let heads = 0;
  store.head = async (...args) => {
    heads++;
    return originalHead(...args);
  };
  const adapter = new ParquetScanAdapter(store);
  for (const matching of [true, false]) {
    const budget = new MemoryBudget(65536);
    const options = {
      object: { ...head, path: matching ? path : "different.parquet" },
      memoryBudget: budget,
      columns: ["id"],
      batchSize: 1,
      budget: {},
      stats: queryStats(),
      now: () => 0,
      startedAt: 0,
    };
    const rows = [];
    if (mode === "rows") for await (const batch of adapter.scan(path, options)) rows.push(...batch);
    else if (mode === "vectors")
      for await (const batch of adapter.scanVectorBatches(path, options))
        rows.push(...materializeBatchRows(batch.batch));
    else {
      if (!adapter.scanOwnedVectorBatches) throw new Error("Missing owned scanner");
      for await (const owner of adapter.scanOwnedVectorBatches(path, options)) {
        try {
          rows.push(...materializeBatchRows(owner.value.batch));
        } finally {
          owner.release();
        }
      }
    }
    expect(rows).toEqual([{ id: 1 }, { id: 2 }]);
    expect(heads).toBe(matching ? 0 : 1);
    expect(budget.usedBytes).toBe(0);
  }
});

it.each([
  "indexed",
  "cached",
] as const)("bounds %s multi-file schema discovery and releases backing on success and conflicts", async (mode) => {
  const store = memoryStore();
  for (const path of ["a.parquet", "b.parquet"]) {
    await writeParquet(store, path, {
      rowGroupSize: Array(20).fill(1),
      columnData: [{ name: "id", type: "INT32", data: Array.from({ length: 20 }, (_, i) => i) }],
    });
  }
  const objects = [];
  for await (const object of store.list("")) objects.push(object);
  const adapter = new ParquetScanAdapter(store, { taskMetadataMode: mode });
  const expected = await adapter.planObjects(objects, {});
  const budget = new MemoryBudget(mode === "indexed" ? 65536 : 1024 * 1024);
  expect(await adapter.planObjects(objects, { memoryBudget: budget })).toEqual(expected);
  expect(budget.peakBytes).toBeGreaterThan(0);
  expect(budget.usedBytes).toBe(0);
  const tiny = new MemoryBudget(7);
  await expect(adapter.planObjects(objects, { memoryBudget: tiny })).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  expect(tiny.usedBytes).toBe(0);
  await writeParquet(store, "b.parquet", {
    columnData: [{ name: "id", type: "STRING", data: ["conflict"] }],
  });
  const changed = [];
  for await (const object of store.list("")) changed.push(object);
  await expect(adapter.planObjects(changed, { memoryBudget: budget })).rejects.toMatchObject({
    code: "LAKEQL_SCHEMA_CONFLICT",
  });
  expect(budget.usedBytes).toBe(0);
});

function packedLeafType(vector: Vector | undefined): string | undefined {
  while (vector?.type === "dict") vector = vector.dictionary;
  return vector?.type;
}

/** Full initial read, envelope/directory, and the largest live raw row group. */
function indexedPlanningCapacity(bytes: Uint8Array): number {
  const buffer = bytes.slice().buffer;
  const scope = new MemoryScope(new MemoryBudget(Number.MAX_SAFE_INTEGER));
  try {
    const envelope = readParquetFooterEnvelope(buffer, undefined, scope);
    const indexed = openIndexedParquetEnvelope(envelope, scope);
    let groupBytes = 0;
    for (let index = 0; index < indexed.rowGroupCount; index++) {
      const groupScope = new MemoryScope(new MemoryBudget(Number.MAX_SAFE_INTEGER));
      try {
        indexed.readRowGroup(index, groupScope);
        groupBytes = Math.max(groupBytes, groupScope.budget.peakBytes);
      } finally {
        groupScope.dispose();
      }
    }
    return bytes.byteLength + scope.budget.usedBytes + groupBytes;
  } finally {
    scope.dispose();
  }
}

it("preserves prototype-like Parquet column names through owned projection", async () => {
  const { createParquetLake } = await import("./index.js");
  const store = memoryStore();
  await writeParquet(store, "data-keys.parquet", {
    columnData: [
      { name: "__proto__", type: "INT32", data: [7, 8] },
      { name: "constructor", type: "STRING", data: ["a", "b"] },
    ],
  });
  const budget = new MemoryBudget(65536);
  const lake = createParquetLake({ store, memoryPool: budget, scanRangeCache: { maxBytes: 0 } });
  for (const filtered of [false, true]) {
    const base = lake.path("data-keys.parquet").batchSize(1);
    const query = filtered
      ? base.select(["__proto__", "constructor"]).where(gt("__proto__", 7))
      : base;
    const owner = await query.run().toOwnedArray();
    try {
      expect(
        owner.value.map((row) => Object.getOwnPropertyDescriptor(row, "__proto__")?.value),
      ).toEqual(filtered ? [8] : [7, 8]);
      expect(owner.value.map((row) => row.constructor)).toEqual(filtered ? ["b"] : ["a", "b"]);
      for (const row of owner.value) expect(Object.getPrototypeOf(row)).toBe(Object.prototype);
    } finally {
      owner.release();
    }
    expect(budget.usedBytes).toBe(0);
  }
});
