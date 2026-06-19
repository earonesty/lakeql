import {
  gt,
  materializeBatchRows,
  memoryStore,
  type QueryStats,
  SharedMemoryCache,
} from "lakeql-core";
import { describe, expect, it } from "vitest";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { readParquetMetadata, writeParquet } from "./index.js";
import type { StoreAsyncBuffer } from "./types.js";
import { canReadParquetVectorBatches, readParquetVectorBatchesFromFile } from "./vector-batches.js";

describe("direct Parquet vector batches", () => {
  it("reads the known emitted DATA_PAGE_V2 vector shapes including dictionary strings", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/vector-physical-shapes.parquet", {
      rowGroupSize: [8],
      pageSize: 1024,
      schema: [
        { name: "root", num_children: 6 },
        { name: "f64", type: "DOUBLE", repetition_type: "REQUIRED" },
        { name: "i64", type: "INT64", repetition_type: "OPTIONAL" },
        {
          name: "loaded_at",
          type: "INT64",
          converted_type: "TIMESTAMP_MICROS",
          repetition_type: "OPTIONAL",
        },
        { name: "flag", type: "BOOLEAN", repetition_type: "OPTIONAL" },
        { name: "label", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "REQUIRED" },
        {
          name: "plain_name",
          type: "BYTE_ARRAY",
          converted_type: "UTF8",
          repetition_type: "OPTIONAL",
        },
      ],
      columnData: [
        { name: "f64", data: [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5] },
        { name: "i64", data: [1n, 2n, null, 4n, 5n, 6n, null, 8n] },
        {
          name: "loaded_at",
          data: [
            1_700_000_000_000_001n,
            null,
            1_700_000_000_000_003n,
            1_700_000_000_000_004n,
            1_700_000_000_000_005n,
            null,
            1_700_000_000_000_007n,
            1_700_000_000_000_008n,
          ],
        },
        { name: "flag", data: [true, false, true, null, false, true, false, true] },
        {
          name: "label",
          data: ["a", "b", "a", "b", "a", "b", "a", "b"],
          encoding: "RLE_DICTIONARY",
        },
        {
          name: "plain_name",
          data: ["aa", null, "cc", "dd", "ee", "ff", null, "hh"],
          encoding: "PLAIN",
        },
      ],
    });
    const file = await fileBuffer(store, "data/vector-physical-shapes.parquet");
    const metadata = await readParquetMetadata(store, "data/vector-physical-shapes.parquet");
    const columnMetadata = Object.fromEntries(
      metadata.row_groups[0]?.columns.map((column) => [
        column.meta_data?.path_in_schema.join("."),
        column.meta_data,
      ]) ?? [],
    );

    for (const name of ["f64", "i64", "loaded_at", "flag", "plain_name"]) {
      expect(columnMetadata[name]?.encoding_stats).toEqual([
        { page_type: "DATA_PAGE_V2", encoding: "PLAIN", count: 1 },
      ]);
    }
    expect(columnMetadata.label?.encoding_stats).toEqual([
      { page_type: "DICTIONARY_PAGE", encoding: "PLAIN", count: 1 },
      { page_type: "DATA_PAGE_V2", encoding: "RLE_DICTIONARY", count: 1 },
    ]);
    expect(columnMetadata.label?.dictionary_page_offset).toEqual(expect.any(BigInt));

    const batches = [];
    for await (const batch of readParquetVectorBatchesFromFile(file, metadata, {
      columns: ["f64", "i64", "loaded_at", "flag", "label", "plain_name"],
      rowStart: 1,
      rowEnd: 7,
      batchSize: 3,
      stats: queryStats(),
    })) {
      batches.push(batch);
    }
    expect(batches).toHaveLength(1);
    const batch = batches[0]?.batch;
    if (batch === undefined) throw new Error("missing vector batch");
    expect(batch.columns.f64).toMatchObject({ type: "f64" });
    expect(batch.columns.i64).toMatchObject({
      type: "i64",
      valid: new Uint8Array([1, 0, 1, 1, 1, 0]),
    });
    expect(batch.columns.loaded_at).toMatchObject({
      type: "timestamp",
      unit: "micros",
      isAdjustedToUTC: true,
      valid: new Uint8Array([0, 1, 1, 1, 0, 1]),
    });
    expect(batch.columns.flag).toMatchObject({
      type: "bool",
      valid: new Uint8Array([1, 1, 0, 1, 1, 1]),
    });
    expect(batch.columns.label).toMatchObject({
      type: "dict",
      dictionary: { type: "utf8", values: ["a", "b"] },
    });
    expect(batch.columns.plain_name).toMatchObject({
      type: "utf8",
      valid: new Uint8Array([0, 1, 1, 1, 1, 0]),
    });
    expect(materializeBatchRows(batch)).toEqual([
      { f64: 2.5, i64: 2n, loaded_at: null, flag: false, label: "b", plain_name: null },
      {
        f64: 3.5,
        i64: null,
        loaded_at: expect.objectContaining({
          epochNanoseconds: 1_700_000_000_000_003_000n,
          unit: "micros",
        }),
        flag: true,
        label: "a",
        plain_name: "cc",
      },
      {
        f64: 4.5,
        i64: 4n,
        loaded_at: expect.objectContaining({
          epochNanoseconds: 1_700_000_000_000_004_000n,
          unit: "micros",
        }),
        flag: null,
        label: "b",
        plain_name: "dd",
      },
      {
        f64: 5.5,
        i64: 5n,
        loaded_at: expect.objectContaining({
          epochNanoseconds: 1_700_000_000_000_005_000n,
          unit: "micros",
        }),
        flag: false,
        label: "a",
        plain_name: "ee",
      },
      { f64: 6.5, i64: 6n, loaded_at: null, flag: true, label: "b", plain_name: "ff" },
      {
        f64: 7.5,
        i64: null,
        loaded_at: expect.objectContaining({
          epochNanoseconds: 1_700_000_000_000_007_000n,
          unit: "micros",
        }),
        flag: false,
        label: "a",
        plain_name: null,
      },
    ]);
  });

  it("reads supported scalar leaf types with row windows and decoded cache reuse", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/vector-batches.parquet", {
      rowGroupSize: [4],
      columnData: [
        {
          name: "flag",
          data: [true, false, null, true, false, true, null, false],
          type: "BOOLEAN",
        },
        { name: "i32", data: [1, 2, null, 4, 5, 6, 7, 8], type: "INT32" },
        { name: "i64", data: [1n, 2n, 3n, null, 5n, 6n, 7n, 8n], type: "INT64" },
        { name: "f32", data: [1.5, 2.5, 3.5, 4.5, null, 6.5, 7.5, 8.5], type: "FLOAT" },
        { name: "f64", data: [10, 20, 30, 40, 50, null, 70, 80], type: "DOUBLE" },
        { name: "name", data: ["a", "b", null, "d", "e", "f", "g", null], type: "STRING" },
      ],
    });
    const file = await fileBuffer(store, "data/vector-batches.parquet");
    const metadata = await readParquetMetadata(store, "data/vector-batches.parquet");
    const shared = new SharedMemoryCache({ maxBytes: 256 * 1024 * 1024 });
    const decodedColumnCache = new DecodedColumnCache(shared, {
      maxBytes: 256 * 1024 * 1024,
      policy: "latency",
    });
    const options = {
      columns: ["flag", "i32", "i64", "f32", "f64", "name"],
      rowStart: 1,
      rowEnd: 7,
      batchSize: 2,
      decodedColumnCache,
      decodedColumnCacheKey: "data/vector-batches.parquet",
      stats: queryStats(),
    };

    expect(canReadParquetVectorBatches(metadata, options)).toBe(true);
    const first = await collectVectorRows(file, metadata, options);
    const firstStats = options.stats;
    options.stats = queryStats();
    const second = await collectVectorRows(file, metadata, options);

    expect(first).toEqual([
      { flag: false, i32: 2, i64: 2n, f32: 2.5, f64: 20, name: "b" },
      { flag: null, i32: null, i64: 3n, f32: 3.5, f64: 30, name: null },
      { flag: true, i32: 4, i64: null, f32: 4.5, f64: 40, name: "d" },
      { flag: false, i32: 5, i64: 5n, f32: null, f64: 50, name: "e" },
      { flag: true, i32: 6, i64: 6n, f32: 6.5, f64: null, name: "f" },
      { flag: null, i32: 7, i64: 7n, f32: 7.5, f64: 70, name: "g" },
    ]);
    expect(second).toEqual(first);
    expect(firstStats.cacheMisses).toBeGreaterThan(0);
    expect(options.stats.cacheHits).toBeGreaterThan(0);
    expect(firstStats.rowGroupsRead).toBe(2);
    expect(firstStats.columnsRead).toEqual(["f32", "f64", "flag", "i32", "i64", "name"]);
  });

  it("returns no direct batches for empty or unsupported vector requests", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/vector-capability.parquet", {
      columnData: [
        { name: "id", data: [1, 2], type: "INT32" },
        { name: "name", data: ["a", "b"], type: "STRING" },
      ],
    });
    const file = await fileBuffer(store, "data/vector-capability.parquet");
    const metadata = await readParquetMetadata(store, "data/vector-capability.parquet");
    const unsupportedDateMetadata = structuredClone(metadata);
    const idLeaf = unsupportedDateMetadata.schema.find((entry) => entry.name === "id");
    if (idLeaf !== undefined) idLeaf.converted_type = "DATE";

    expect(canReadParquetVectorBatches(metadata, { columns: undefined })).toBe(false);
    expect(canReadParquetVectorBatches(metadata, { columns: [] })).toBe(false);
    expect(canReadParquetVectorBatches(metadata, { columns: ["missing"] })).toBe(false);
    expect(canReadParquetVectorBatches(unsupportedDateMetadata, { columns: ["id"] })).toBe(false);
    expect(canReadParquetVectorBatches(metadata, { columns: ["id", "name"] })).toBe(true);

    const rows = await collectVectorRows(file, metadata, {
      columns: ["id", "name"],
      rowStart: 0,
      rowEnd: 2,
      batchSize: 10,
      stats: queryStats(),
    });
    expect(rows).toEqual([
      { id: 1, name: "a" },
      { id: 2, name: "b" },
    ]);

    const none = [];
    for await (const batch of readParquetVectorBatchesFromFile(file, metadata, {
      columns: undefined,
      stats: queryStats(),
    })) {
      none.push(batch);
    }
    expect(none).toEqual([]);
  });

  it("checks direct vector capability across logical and malformed metadata shapes", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/vector-logical-capability.parquet", {
      schema: [
        { name: "root", num_children: 5 },
        { name: "id", type: "INT32", repetition_type: "OPTIONAL" },
        {
          name: "payload",
          type: "BYTE_ARRAY",
          converted_type: "JSON",
          repetition_type: "OPTIONAL",
        },
        {
          name: "event_time",
          type: "INT64",
          logical_type: { type: "TIMESTAMP", unit: "MICROS", isAdjustedToUTC: true },
          repetition_type: "OPTIONAL",
        },
        { name: "raw", type: "BYTE_ARRAY", repetition_type: "OPTIONAL" },
        { name: "name", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "OPTIONAL" },
      ],
      columnData: [
        { name: "id", data: [1] },
        { name: "payload", data: ['{"ok":true}'] },
        { name: "event_time", data: [1_700_000_000_000_001n] },
        { name: "raw", data: [new Uint8Array([1, 2, 3])] },
        { name: "name", data: ["alpha"] },
      ],
    });
    const metadata = await readParquetMetadata(store, "data/vector-logical-capability.parquet");

    expect(
      canReadParquetVectorBatches(metadata, { columns: ["payload", "event_time", "raw"] }),
    ).toBe(true);

    const unsignedLogical = structuredClone(metadata);
    const idLeaf = unsignedLogical.schema.find((entry) => entry.name === "id");
    if (idLeaf !== undefined) {
      idLeaf.logical_type = { type: "INTEGER", bitWidth: 64, isSigned: false };
    }
    expect(canReadParquetVectorBatches(unsignedLogical, { columns: ["id"] })).toBe(false);

    for (const logicalType of ["DATE", "GEOMETRY", "GEOGRAPHY"] as const) {
      const unsupported = structuredClone(metadata);
      const rawLeaf = unsupported.schema.find((entry) => entry.name === "raw");
      if (rawLeaf !== undefined) rawLeaf.logical_type = { type: logicalType };
      expect(canReadParquetVectorBatches(unsupported, { columns: ["raw"] })).toBe(false);
    }

    const bson = structuredClone(metadata);
    const rawLeaf = bson.schema.find((entry) => entry.name === "raw");
    if (rawLeaf !== undefined) rawLeaf.converted_type = "BSON";
    expect(canReadParquetVectorBatches(bson, { columns: ["raw"] })).toBe(false);

    const missingMetadata = structuredClone(metadata);
    const firstColumn = missingMetadata.row_groups[0]?.columns[0];
    if (firstColumn !== undefined) delete firstColumn.meta_data;
    expect(canReadParquetVectorBatches(missingMetadata, { columns: ["id"] })).toBe(false);
  });

  it("returns no direct batches for skipped windows and runtime metadata fallback", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/vector-runtime-fallback.parquet", {
      rowGroupSize: [2, 2],
      columnData: [
        { name: "id", data: [1, 2, 3, 4], type: "INT32" },
        { name: "name", data: ["a", "b", "c", "d"], type: "STRING" },
      ],
    });
    const file = await fileBuffer(store, "data/vector-runtime-fallback.parquet");
    const metadata = await readParquetMetadata(store, "data/vector-runtime-fallback.parquet");

    const beforeWindowStats = queryStats();
    expect(
      await collectVectorRows(file, metadata, {
        columns: ["id"],
        rowStart: 4,
        rowEnd: 4,
        batchSize: 2,
        stats: beforeWindowStats,
      }),
    ).toEqual([]);
    expect(beforeWindowStats.rowGroupsRead).toBe(0);
    expect(beforeWindowStats.rowGroupsSkipped).toBe(2);

    const afterWindowStats = queryStats();
    expect(
      await collectVectorRows(file, metadata, {
        columns: ["id"],
        rowStart: 10,
        rowEnd: 12,
        batchSize: 2,
        stats: afterWindowStats,
      }),
    ).toEqual([]);
    expect(afterWindowStats.rowGroupsRead).toBe(0);
    expect(afterWindowStats.rowGroupsSkipped).toBe(2);

    const unsupported = structuredClone(metadata);
    const idLeaf = unsupported.schema.find((entry) => entry.name === "id");
    if (idLeaf !== undefined) idLeaf.converted_type = "DATE";
    expect(
      await collectVectorRows(file, unsupported, {
        columns: ["id"],
        batchSize: 2,
        stats: queryStats(),
      }),
    ).toEqual([]);

    const missingColumn = structuredClone(metadata);
    missingColumn.row_groups[0]?.columns.pop();
    expect(
      await collectVectorRows(file, missingColumn, {
        columns: ["name"],
        batchSize: 2,
        stats: queryStats(),
      }),
    ).toEqual([]);
  });

  it("reads vectors without stats or decoded cache keys", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/vector-no-stats.parquet", {
      columnData: [
        { name: "id", data: [1, 2, 3], type: "INT32" },
        { name: "name", data: ["a", null, "c"], type: "STRING" },
      ],
    });
    const file = await fileBuffer(store, "data/vector-no-stats.parquet");
    const metadata = await readParquetMetadata(store, "data/vector-no-stats.parquet");
    const cache = new DecodedColumnCache(new SharedMemoryCache({ maxBytes: 1024 * 1024 }), {
      maxBytes: 1024 * 1024,
      policy: "balanced",
    });

    expect(
      await collectVectorRows(file, metadata, {
        columns: ["id", "name"],
        rowStart: 1,
        rowEnd: 3,
        batchSize: 1,
        decodedColumnCache: cache,
      }),
    ).toEqual([
      { id: 2, name: null },
      { id: 3, name: "c" },
    ]);
  });

  it("reads timestamp and raw byte vectors across pruned row groups", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/vector-pruned.parquet", {
      rowGroupSize: [2, 2, 2],
      schema: [
        { name: "root", num_children: 4 },
        { name: "id", type: "INT32", repetition_type: "OPTIONAL" },
        {
          name: "loaded_at",
          type: "INT64",
          converted_type: "TIMESTAMP_MICROS",
          repetition_type: "OPTIONAL",
        },
        { name: "raw", type: "BYTE_ARRAY", repetition_type: "OPTIONAL" },
        { name: "label", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "OPTIONAL" },
      ],
      columnData: [
        { name: "id", data: [1, 2, 3, 4, 5, 6] },
        {
          name: "loaded_at",
          data: [
            1_700_000_000_000_001n,
            null,
            1_700_000_000_000_003n,
            1_700_000_000_000_004n,
            null,
            1_700_000_000_000_006n,
          ],
        },
        {
          name: "raw",
          data: [
            new Uint8Array([1]),
            new Uint8Array([2]),
            new Uint8Array([3]),
            null,
            new Uint8Array([5]),
            new Uint8Array([6]),
          ],
        },
        { name: "label", data: ["a", "a", "b", "b", "c", "c"] },
      ],
    });
    const file = await fileBuffer(store, "data/vector-pruned.parquet");
    const metadata = await readParquetMetadata(store, "data/vector-pruned.parquet");
    const options = {
      columns: ["id", "loaded_at", "raw", "label"],
      rowStart: 1,
      rowEnd: 6,
      batchSize: 2,
      where: gt("id", 3),
      stats: queryStats(),
    };

    expect(canReadParquetVectorBatches(metadata, options)).toBe(true);
    const batches = [];
    for await (const batch of readParquetVectorBatchesFromFile(file, metadata, options)) {
      batches.push(batch);
    }
    expect(batches.map((batch) => [batch.rowOffset, batch.batch.rowCount])).toEqual([
      [2, 2],
      [4, 2],
    ]);
    expect(batches.flatMap(({ batch }) => materializeBatchRows(batch))).toEqual([
      {
        id: 3,
        loaded_at: expect.objectContaining({
          epochNanoseconds: 1_700_000_000_000_003_000n,
          unit: "micros",
          isAdjustedToUTC: true,
        }),
        raw: "\x03",
        label: "b",
      },
      {
        id: 4,
        loaded_at: expect.objectContaining({
          epochNanoseconds: 1_700_000_000_000_004_000n,
          unit: "micros",
          isAdjustedToUTC: true,
        }),
        raw: null,
        label: "b",
      },
      { id: 5, loaded_at: null, raw: "\x05", label: "c" },
      {
        id: 6,
        loaded_at: expect.objectContaining({
          epochNanoseconds: 1_700_000_000_000_006_000n,
          unit: "micros",
          isAdjustedToUTC: true,
        }),
        raw: "\x06",
        label: "c",
      },
    ]);
    expect(options.stats.rowGroupsRead).toBe(2);
    expect(options.stats.rowGroupsSkipped).toBe(1);
    expect(options.stats.rowsDecoded).toBe(4);
    expect(options.stats.columnsRead).toEqual(["id", "label", "loaded_at", "raw"]);

    const uuidMetadata = structuredClone(metadata);
    const rawLeaf = uuidMetadata.schema.find((entry) => entry.name === "raw");
    if (rawLeaf !== undefined) rawLeaf.logical_type = { type: "UUID" };
    expect(canReadParquetVectorBatches(uuidMetadata, { columns: ["raw"] })).toBe(false);

    const unsignedMetadata = structuredClone(metadata);
    const idLeaf = unsignedMetadata.schema.find((entry) => entry.name === "id");
    if (idLeaf !== undefined) idLeaf.converted_type = "UINT_64";
    expect(canReadParquetVectorBatches(unsignedMetadata, { columns: ["id"] })).toBe(false);
  });
});

async function collectVectorRows(
  file: StoreAsyncBuffer,
  metadata: Awaited<ReturnType<typeof readParquetMetadata>>,
  options: Parameters<typeof readParquetVectorBatchesFromFile>[2],
): Promise<Record<string, unknown>[]> {
  const rows = [];
  for await (const { batch } of readParquetVectorBatchesFromFile(file, metadata, options)) {
    rows.push(...materializeBatchRows(batch));
  }
  return rows;
}

async function fileBuffer(
  store: ReturnType<typeof memoryStore>,
  path: string,
): Promise<StoreAsyncBuffer> {
  const head = await store.head(path);
  if (head === null) throw new Error(`missing ${path}`);
  return {
    byteLength: head.size,
    ...(head.etag === undefined ? {} : { etag: head.etag }),
    async slice(start, end) {
      return (await store.getRange(path, { offset: start, length: (end ?? head.size) - start }))
        .buffer;
    },
  };
}

function queryStats(): QueryStats {
  return {
    queryId: "vector-batches-test",
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
