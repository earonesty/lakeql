/// <reference types="@cloudflare/vitest-pool-workers/types" />
import { env } from "cloudflare:test";
import {
  eq,
  gatherOwnedBatch,
  Lake,
  lt,
  MemoryBudget,
  MemoryScope,
  materializeBatchRows,
  memoryCache,
  memoryStore,
  SharedMemoryCache,
} from "lakeql-core";
import { describe, expect, it, vi } from "vitest";
import { type R2BucketLike, R2ObjectStore } from "../../r2/src/index.js";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { createParquetLake, type ParquetMetadata, writeParquet } from "./index.js";
import {
  readOwnedParquetMetadata,
  readParquetMetadataFromFile,
  sharedParquetMetadataCache,
} from "./metadata-cache.js";
import { readOwnedParquetVectorBatches } from "./owned-vector-batches.js";
import { cachedRangeBuffer } from "./range-cache.js";
import { ParquetScanAdapter } from "./scan-adapter.js";
import { asyncBufferFromStore } from "./store-buffer.js";

describe("parquet workerd runtime", () => {
  it("writes and scans Parquet through the real adapter in the Workers runtime", async () => {
    expect((globalThis as Record<string, unknown>).WebSocketPair).toBeTypeOf("function");

    const store = memoryStore();
    await writeParquet(store, "events.parquet", {
      rowGroupSize: [2],
      columnData: [
        { name: "id", data: [1, 2, 3], type: "INT32" },
        { name: "region", data: ["west", "east", "west"], type: "STRING" },
        { name: "amount", data: [10, 20, 30], type: "INT32" },
      ],
    });

    const metadataCache = memoryCache<ParquetMetadata>();
    const lake = createParquetLake({
      store,
      metadataCache,
      queryId: () => "q_parquet_workerd",
    });
    const result = lake
      .path("events.parquet")
      .select(["id", "amount"])
      .where(eq("region", "west"))
      .run();

    await expect(result.toArray()).resolves.toEqual([
      { id: 1, amount: 10 },
      { id: 3, amount: 30 },
    ]);
    expect(result.stats).toMatchObject({
      queryId: "q_parquet_workerd",
      filesPlanned: 1,
      filesRead: 1,
      rowsDecoded: 3,
      rowsMatched: 2,
      rowsReturned: 2,
    });
    expect(result.stats.rangeRequests).toBeGreaterThan(0);
    expect(result.stats.cacheMisses).toBe(1);

    const cached = lake.path("events.parquet").where(lt("amount", 0)).run();
    await expect(cached.count()).resolves.toBe(0);
    expect(cached.stats.cacheHits).toBe(1);
    expect(cached.stats.cacheMisses).toBe(0);
    expect(cached.stats.rangeRequests).toBe(0);
  });
});

it.each(
  [64, 128].flatMap((mib) => [
    { mib, codec: "UNCOMPRESSED" as const, encoding: "PLAIN" as const },
    { mib, codec: "SNAPPY" as const, encoding: "PLAIN" as const },
    { mib, codec: "SNAPPY" as const, encoding: "RLE_DICTIONARY" as const },
  ]),
)("decodes native R2 concurrent owned batches ($mib MiB, $codec, $encoding)", async ({
  mib,
  codec,
  encoding,
}) => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const store = new R2ObjectStore(bucket);
  const path = `owned-decode-${mib}-${codec}-${encoding}.parquet`;
  const names = ["é", null, "", "猫", "tail", "é"];
  await writeParquet(store, path, {
    codec,
    rowGroupSize: [2, 2, 2],
    columnData: [
      { name: "id", type: "INT32", encoding, data: [0, 1, 2, 3, 4, 5] },
      { name: "name", type: "STRING", encoding: "PLAIN", data: names },
    ],
  });
  const source = await asyncBufferFromStore(store, path);
  const metadata = await readParquetMetadataFromFile(source);
  const root = new MemoryBudget(mib * 1024 * 1024);
  const shared = new SharedMemoryCache({ memoryBudget: root, maxBytes: 16384 });
  const file = cachedRangeBuffer(
    source,
    { maxBytes: 16384, coalesceBytes: 128, sharedCache: shared },
    path,
  );
  const decoded = new DecodedColumnCache(shared, { maxBytes: 16384, policy: "latency" });
  const options = {
    columns: ["id", "name"],
    batchSize: 1,
    maxConcurrentReads: 2,
    decodedColumnCache: decoded,
    decodedColumnCacheKey: path,
  };
  const run = async () => {
    const budget = root.child();
    const rows = [];
    for await (const owner of readOwnedParquetVectorBatches(file, metadata, budget, options)) {
      try {
        expect(owner.value.batch.rowCount).toBe(1);
        rows.push(...materializeBatchRows(owner.value.batch));
      } finally {
        owner.release();
      }
    }
    expect(budget.usedBytes).toBe(0);
    return rows;
  };
  const expected = names.map((name, id) => ({ id, name }));
  expect(await Promise.all([run(), run()])).toEqual([expected, expected]);
  expect(await run()).toEqual(expected);
  const child = root.child();
  const iterator = readOwnedParquetVectorBatches(file, metadata, child, options)[
    Symbol.asyncIterator
  ]();
  const first = await iterator.next();
  if (first.done) throw new Error("missing owned output");
  await iterator.return?.();
  shared.deleteWhere(() => true);
  expect(child.usedBytes).toBeGreaterThan(0);
  expect(materializeBatchRows(first.value.value.batch)).toEqual(expected.slice(0, 1));
  first.value.release();
  expect(child.usedBytes).toBe(0);
  expect(root.usedBytes).toBe(0);
  expect(root.peakBytes).toBeLessThanOrEqual(root.maxBytes);
});

it("decodes nested R2 fallback vectors at their measured admitted boundary", async () => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const store = new R2ObjectStore(bucket);
  const path = "owned-nested.parquet";
  await writeParquet(store, path, {
    codec: "UNCOMPRESSED",
    rowGroupSize: [2, 2],
    schema: [
      { name: "root", num_children: 2 },
      { name: "tags", converted_type: "LIST", repetition_type: "OPTIONAL", num_children: 1 },
      { name: "list", repetition_type: "REPEATED", num_children: 1 },
      { name: "element", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "OPTIONAL" },
      { name: "attrs", converted_type: "MAP", repetition_type: "OPTIONAL", num_children: 1 },
      { name: "key_value", repetition_type: "REPEATED", num_children: 2 },
      { name: "key", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "REQUIRED" },
      { name: "value", type: "INT32", repetition_type: "OPTIONAL" },
    ],
    columnData: [
      { name: "tags", data: [["é", null, "猫"], [], null, [""]] },
      {
        name: "attrs",
        data: [
          new Map<string, number | null>([
            ["a", 1],
            ["b", null],
          ]),
          new Map(),
          null,
          new Map([["c", 3]]),
        ],
      },
    ],
  });
  const file = await asyncBufferFromStore(store, path);
  const metadata = await readParquetMetadataFromFile(file);
  async function run(budget: MemoryBudget) {
    const rows = [];
    for await (const owner of readOwnedParquetVectorBatches(file, metadata, budget, {
      batchSize: 1,
    })) {
      try {
        rows.push(...materializeBatchRows(owner.value.batch));
      } finally {
        owner.release();
      }
    }
    return rows;
  }
  const expected = [
    { tags: ["é", null, "猫"], attrs: { a: 1, b: null } },
    { tags: [], attrs: {} },
    { tags: null, attrs: null },
    { tags: [""], attrs: { c: 3 } },
  ];
  const measured = new MemoryBudget(65536);
  expect(await run(measured)).toEqual(expected);
  expect(measured.usedBytes).toBe(0);
  const exact = new MemoryBudget(measured.peakBytes);
  expect(await run(exact)).toEqual(expected);
  expect(exact.usedBytes).toBe(0);
  const tight = new MemoryBudget(measured.peakBytes - 1);
  await expect(run(tight)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(tight.usedBytes).toBe(0);
});

it("enforces normal aggregate input admission through native R2 streams", async () => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const store = new R2ObjectStore(bucket);
  const path = "aggregate-owned-r2.parquet";
  await writeParquet(store, path, {
    codec: "UNCOMPRESSED",
    rowGroupSize: [2, 2, 2],
    columnData: [{ name: "id", type: "INT32", data: [1, 2, 1, 2, 1, 2] }],
  });
  const run = async (limit: number) => {
    const pool = new MemoryBudget(limit);
    const lake = new Lake({ store, scanner: new ParquetScanAdapter(store), memoryPool: pool });
    try {
      expect(
        await lake
          .path(path)
          .batchSize(1)
          .groupBy(["id"])
          .aggregate({ total: { op: "sum", column: "id" } }),
      ).toEqual([
        { id: 1, total: 3 },
        { id: 2, total: 6 },
      ]);
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

it.each([
  false,
  true,
])("compacts gathered Parquet bytes from native R2 after scanner shutdown (direct=%s)", async (direct) => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const store = new R2ObjectStore(bucket);
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
      stats: {
        queryId: "owned-gather",
        manifestsRead: 0,
        manifestsSkipped: 0,
        rowGroupsRead: 0,
        rowGroupsSkipped: 0,
        columnsRead: [],
        filesPlanned: 0,
        filesRead: 0,
        filesSkipped: 0,
        bytesRequested: 0,
        rangeRequests: 0,
        rowsDecoded: 0,
        rowsMatched: 0,
        rowsReturned: 0,
        cacheHits: 0,
        cacheMisses: 0,
        elapsedMs: 0,
      },
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

it("round-trips dictionary fallback pages through native R2", async () => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const store = new R2ObjectStore(bucket);
  const values = [Uint8Array.of(255, 0), null, new Uint8Array()];
  await writeParquet(store, "dictionary-fallback-r2.parquet", {
    codec: "UNCOMPRESSED",
    schema: [
      { name: "root", num_children: 1 },
      { name: "raw", type: "BYTE_ARRAY", repetition_type: "OPTIONAL" },
    ],
    columnData: [{ name: "raw", encoding: "RLE_DICTIONARY", data: values }],
  });
  const metadata = await readParquetMetadataFromFile(
    await asyncBufferFromStore(store, "dictionary-fallback-r2.parquet"),
  );
  const column = metadata.row_groups[0]?.columns[0]?.meta_data;
  expect(column?.dictionary_page_offset).toBeUndefined();
  expect(column?.encodings).not.toContain("RLE_DICTIONARY");
  expect(
    await createParquetLake({ store }).path("dictionary-fallback-r2.parquet").toArray(),
  ).toEqual(values.map((raw) => ({ raw })));
});

it("bounds filtered top-k and retained candidates over native R2 streams", async () => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const store = new R2ObjectStore(bucket);
  const path = "top-k-owned-r2.parquet";
  await writeParquet(store, path, {
    codec: "UNCOMPRESSED",
    rowGroupSize: [2, 2, 2],
    columnData: [{ name: "id", type: "INT32", data: [7, 1, 5, 3, 9, 2] }],
  });
  async function run(maxBytes: number) {
    const pool = new MemoryBudget(maxBytes);
    const lake = new Lake({ store, scanner: new ParquetScanAdapter(store), memoryPool: pool });
    try {
      expect(
        await lake
          .path(path)
          .batchSize(1)
          .where(lt("id", 9))
          .orderBy([{ column: "id" }])
          .offset(1)
          .limit(2)
          .toArray(),
      ).toEqual([{ id: 2 }, { id: 3 }]);
      return pool.peakBytes;
    } finally {
      expect(pool.usedBytes).toBe(0);
    }
  }
  let peak = await run(65536);
  // Adaptive reads can lower their peak when given a tighter budget. Find the
  // stable measured peak before asserting its exact admission boundary.
  for (;;) {
    const measured = await run(peak);
    expect(measured).toBeLessThanOrEqual(peak);
    if (measured === peak) break;
    peak = measured;
  }
  await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});

it("admits projected filtered output and closes native R2 scans on early limit", async () => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const store = new R2ObjectStore(bucket);
  const path = "projected-owned-r2.parquet";
  await writeParquet(store, path, {
    codec: "UNCOMPRESSED",
    rowGroupSize: [2, 2, 2],
    columnData: [{ name: "id", type: "INT32", data: [7, 1, 5, 3, 9, 2] }],
  });
  async function run(maxBytes: number) {
    const pool = new MemoryBudget(maxBytes);
    const lake = new Lake({ store, scanner: new ParquetScanAdapter(store), memoryPool: pool });
    try {
      expect(
        await lake
          .path(path)
          .batchSize(2)
          .where(lt("id", 7))
          .project({ copy: { kind: "column", name: "id" } })
          .limit(2)
          .toArray(),
      ).toEqual([
        { id: 1, copy: 1 },
        { id: 5, copy: 5 },
      ]);
      return pool.peakBytes;
    } finally {
      expect(pool.usedBytes).toBe(0);
    }
  }
  let peak = await run(65536);
  // Adaptive reads can lower their peak when given a tighter budget. Find the
  // stable measured peak before asserting its exact admission boundary.
  for (;;) {
    const measured = await run(peak);
    expect(measured).toBeLessThanOrEqual(peak);
    if (measured === peak) break;
    peak = measured;
  }
  await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});

it("owns full GeoParquet metadata across R2 cache eviction in workerd", async () => {
  const { parquetWriteBuffer } = await import("hyparquet-writer");
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const store = new R2ObjectStore(bucket);
  const bytes = parquetWriteBuffer({
    rowGroupSize: [2, 2],
    columnData: [
      {
        name: "geometry",
        type: "BYTE_ARRAY",
        data: Array.from({ length: 4 }, () => new Uint8Array([1, 2, 3])),
      },
    ],
    kvMetadata: [
      {
        key: "geo",
        value: JSON.stringify({
          columns: {
            geometry: { encoding: "WKB", crs: { id: { authority: "EPSG", code: 4326 } } },
          },
        }),
      },
    ],
  });
  await store.put("owned-full-geo.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "owned-full-geo.parquet");
  let reads = 0;
  const file = {
    ...source,
    async sliceOwned(...args: Parameters<NonNullable<typeof source.sliceOwned>>) {
      reads++;
      if (!source.sliceOwned) throw new Error("Missing owned read");
      return source.sliceOwned(...args);
    },
  };
  const pool = new MemoryBudget(64 * 1024 * 1024);
  const cache = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 1024 * 1024 });
  const adapter = sharedParquetMetadataCache(cache);
  const first = await readOwnedParquetMetadata(
    "owned-full-geo.parquet",
    file,
    pool.child(),
    adapter,
  );
  expect(first.cached).toBe(false);
  expect(first.owner.bytes).toBeGreaterThan(bytes.byteLength);
  first.owner.release();
  const coldReads = reads;
  const owners = await Promise.all(
    Array.from({ length: 4 }, () =>
      readOwnedParquetMetadata("owned-full-geo.parquet", file, pool.child(), adapter),
    ),
  );
  try {
    expect(reads).toBe(coldReads);
    cache.deleteWhere(() => true);
    expect(pool.usedBytes).toBeGreaterThan(0);
    for (const result of owners) {
      expect(result.cached).toBe(true);
      expect(result.owner.value.num_rows).toBe(4n);
      expect(result.owner.value.row_groups).toHaveLength(2);
      expect(result.owner.value.schema[1]?.logical_type).toEqual({
        type: "GEOMETRY",
        crs: "EPSG:4326",
      });
    }
  } finally {
    for (const result of owners) result.owner.release();
    cache.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
  expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
});

it.each([
  "DELTA_BYTE_ARRAY",
  "DELTA_LENGTH_BYTE_ARRAY",
] as const)("owns concurrent delta output over R2, encoding=%s", async (encoding) => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const store = new R2ObjectStore(bucket);
  const path = `owned-${encoding}.parquet`;
  const values = Array.from({ length: 128 }, (_, i) =>
    i % 7 === 0 ? null : `prefix-${"é😀".repeat(100)}-${i}`,
  );
  await writeParquet(store, path, {
    rowGroupSize: [64, 64],
    columnData: [{ name: "value", type: "STRING", encoding, data: values }],
  });
  const pool = new MemoryBudget(64 * 1024 * 1024);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const owners = await Promise.all(
    Array.from({ length: 4 }, () => lake.path(path).batchSize(3).run().toOwnedArray()),
  );
  try {
    for (const owner of owners) expect(owner.value).toEqual(values.map((value) => ({ value })));
    expect(pool.usedBytes).toBeGreaterThan(0);
    expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
  } finally {
    for (const owner of owners) owner.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it("preserves recursive JSON unions across concurrent R2 scans and shared-cache reuse", async () => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const store = new R2ObjectStore(bucket);
  const path = "owned-recursive-json.parquet";
  const records = Array.from({ length: 48 }, (_, id) => ({
    id,
    payload:
      id % 4 === 0
        ? { nested: [id, "雪😀", null] }
        : id % 4 === 1
          ? ["list", id, false]
          : id % 4 === 2
            ? "scalar"
            : null,
  }));
  await writeParquet(store, path, {
    rowGroupSize: [12, 12, 12, 12],
    schema: [
      { name: "root", num_children: 2 },
      { name: "id", type: "INT32", repetition_type: "REQUIRED" },
      { name: "payload", type: "BYTE_ARRAY", converted_type: "JSON", repetition_type: "OPTIONAL" },
    ],
    columnData: [
      { name: "id", data: records.map((row) => row.id) },
      {
        name: "payload",
        data: records.map((row) => (row.payload === null ? undefined : row.payload)),
      },
    ],
  });
  const pool = new MemoryBudget(64 * 1024 * 1024);
  const cache = new SharedMemoryCache({ maxBytes: 256 * 1024, memoryBudget: pool });
  const lake = createParquetLake({
    store,
    memoryPool: pool,
    budget: { maxAllocatedMemoryBytes: 1024 * 1024 },
    scanRangeCache: { maxBytes: 256 * 1024, sharedCache: cache },
  });
  const reads = vi.spyOn(store, "getRangeOwned");
  try {
    for (let round = 0; round < 2; round++) {
      reads.mockClear();
      const owners = await Promise.all(
        Array.from({ length: 4 }, () => lake.path(path).batchSize(3).run().toOwnedArray()),
      );
      try {
        for (const owner of owners) expect(owner.value).toEqual(records);
        expect(pool.usedBytes).toBeGreaterThan(0);
        if (round === 0) expect(reads.mock.calls.length).toBeGreaterThan(0);
        else expect(reads).not.toHaveBeenCalled();
      } finally {
        for (const owner of owners) owner.release();
      }
    }
    const filtered = await lake
      .path(path)
      .where(lt("id", 17))
      .select(["payload"])
      .batchSize(3)
      .run()
      .toOwnedArray();
    try {
      expect(filtered.value).toEqual(records.slice(0, 17).map(({ payload }) => ({ payload })));
    } finally {
      filtered.release();
    }
  } finally {
    reads.mockRestore();
    cache.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
  expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
});

it.each([
  false,
  true,
])("joins selected owned aggregates through bounded native R2 windows, CASE=%s", async (withCase) => {
  const bucket = (env as unknown as { MEMORY_TEST_BUCKET: R2BucketLike }).MEMORY_TEST_BUCKET;
  const store = new R2ObjectStore(bucket);
  const path = "deferred-aggregate-owned-r2.parquet";
  await writeParquet(store, path, {
    codec: "UNCOMPRESSED",
    rowGroupSize: Array.from({ length: 10 }, () => 10),
    columnData: [
      { name: "id", type: "INT32", data: Array.from({ length: 100 }, (_, id) => id) },
      { name: "value", type: "INT32", data: Array.from({ length: 100 }, (_, id) => id * 10) },
    ],
  });
  const pool = new MemoryBudget(1024 * 1024);
  const adapter = new ParquetScanAdapter(store);
  if (!adapter.scanOwnedVectorBatches) throw Error("Missing owned scanner");
  const scan = vi.spyOn(adapter, "scanOwnedVectorBatches");
  try {
    const lake = new Lake({ store, scanner: adapter, memoryPool: pool });
    const rows = await lake
      .path(path)
      .batchSize(10)
      .where(lt("id", 2))
      .groupBy(["id"])
      .aggregate({
        n: { op: "count" },
        total: withCase
          ? {
              op: "sum",
              expr: {
                kind: "case",
                whens: [
                  {
                    when: eq("id", 1),
                    value: { kind: "column", name: "value" },
                  },
                ],
                else: { kind: "literal", value: 0 },
              },
            }
          : { op: "sum", column: "value" },
      });
    expect(rows).toEqual([
      { id: 0, n: 1, total: 0 },
      { id: 1, n: 1, total: 10 },
    ]);
    expect(
      scan.mock.calls.some(
        ([, options]) =>
          options.columns?.length === 1 &&
          options.columns[0] === "value" &&
          options.rowStart === 0 &&
          options.rowEnd === 2,
      ),
    ).toBe(true);
    expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
  } finally {
    scan.mockRestore();
    expect(pool.usedBytes).toBe(0);
  }
});
