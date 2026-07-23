import {
  col,
  fn,
  gt,
  lit,
  materializeBatchRows,
  memoryCache,
  memoryStore,
  type QueryStats,
  SharedMemoryCache,
} from "lakeql-core";
import { describe, expect, it } from "vitest";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { createParquetLake, readParquetMetadata, writeParquet } from "./index.js";
import type { StoreAsyncBuffer } from "./types.js";
import { canReadParquetVectorBatches, readParquetVectorBatchesFromFile } from "./vector-batches.js";

describe("direct Parquet vector batches", () => {
  it("reads supported nested Parquet columns as aligned vector batches", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/vector-nested.parquet", {
      rowGroupSize: [4],
      pageSize: 1024,
      schema: [
        { name: "root", num_children: 3 },
        { name: "id", type: "INT32", repetition_type: "REQUIRED" },
        { name: "tags", converted_type: "LIST", repetition_type: "OPTIONAL", num_children: 1 },
        { name: "list", repetition_type: "REPEATED", num_children: 1 },
        {
          name: "element",
          type: "BYTE_ARRAY",
          converted_type: "UTF8",
          repetition_type: "OPTIONAL",
        },
        { name: "attrs", converted_type: "MAP", repetition_type: "OPTIONAL", num_children: 1 },
        { name: "key_value", repetition_type: "REPEATED", num_children: 2 },
        { name: "key", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "REQUIRED" },
        { name: "value", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "OPTIONAL" },
      ],
      columnData: [
        { name: "id", data: [1, 2, 3, 4] },
        { name: "tags", data: [["coffee", "wifi"], null, [], ["park"]] },
        {
          name: "attrs",
          data: [
            new Map<string, string | null>([
              ["category", "cafe"],
              ["chain", null],
            ]),
            null,
            new Map<string, string>(),
            new Map<string, string>([["category", "park"]]),
          ],
        },
      ],
    });
    const file = await fileBuffer(store, "data/vector-nested.parquet");
    const metadata = await readParquetMetadata(store, "data/vector-nested.parquet");
    const decodedColumnCache = new DecodedColumnCache(
      new SharedMemoryCache({ maxBytes: 1024 * 1024 }),
      {
        maxBytes: 1024 * 1024,
        policy: "latency",
      },
    );
    const stats = queryStats();

    expect(canReadParquetVectorBatches(metadata, { columns: ["id", "tags", "attrs"] })).toBe(true);

    const batches = [];
    for await (const batch of readParquetVectorBatchesFromFile(file, metadata, {
      columns: ["id", "tags", "attrs"],
      rowStart: 1,
      rowEnd: 4,
      batchSize: 2,
      stats,
      decodedColumnCache,
      decodedColumnCacheKey: "data/vector-nested.parquet",
    })) {
      batches.push(batch);
    }

    expect(batches.map((batch) => [batch.rowOffset, batch.batch.rowCount])).toEqual([
      [1, 2],
      [3, 1],
    ]);
    const rows = batches.flatMap((batch) => materializeBatchRows(batch.batch));
    expect(rows).toEqual([
      { id: 2, tags: null, attrs: null },
      { id: 3, tags: [], attrs: {} },
      { id: 4, tags: ["park"], attrs: { category: "park" } },
    ]);
    expect(batches[0]?.batch.columns.id?.type).toBe("i32");
    expect(batches[0]?.batch.columns.tags?.type).toBe("list");
    expect(batches[0]?.batch.columns.attrs?.type).toBe("map");
    expect(stats.rowsDecoded).toBe(3);
    expect(stats.cacheMisses).toBeGreaterThan(0);

    const warmStats = queryStats();
    for await (const _ of readParquetVectorBatchesFromFile(file, metadata, {
      columns: ["id", "tags", "attrs"],
      rowStart: 1,
      rowEnd: 4,
      batchSize: 2,
      stats: warmStats,
      decodedColumnCache,
      decodedColumnCacheKey: "data/vector-nested.parquet",
    })) {
      // drain the warm nested vector scan
    }
    expect(warmStats.cacheHits).toBeGreaterThan(0);
  });

  it("slices nested vector sources when scalar pages split the row window", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/vector-nested-sliced.parquet", {
      rowGroupSize: [6],
      pageSize: 64,
      schema: [
        { name: "root", num_children: 3 },
        { name: "id", type: "INT32", repetition_type: "REQUIRED" },
        { name: "tags", converted_type: "LIST", repetition_type: "OPTIONAL", num_children: 1 },
        { name: "list", repetition_type: "REPEATED", num_children: 1 },
        {
          name: "element",
          type: "BYTE_ARRAY",
          converted_type: "UTF8",
          repetition_type: "OPTIONAL",
        },
        { name: "attrs", converted_type: "MAP", repetition_type: "OPTIONAL", num_children: 1 },
        { name: "key_value", repetition_type: "REPEATED", num_children: 2 },
        { name: "key", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "REQUIRED" },
        { name: "value", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "OPTIONAL" },
      ],
      columnData: [
        { name: "id", data: [1, 2, 3, 4, 5, 6] },
        {
          name: "tags",
          data: [["a"], ["b", "c"], null, [], ["e"], ["f", null]],
        },
        {
          name: "attrs",
          data: [{ kind: "one" }, { kind: "two" }, null, {}, { kind: "five" }, { kind: null }],
        },
      ],
    });
    const file = await fileBuffer(store, "data/vector-nested-sliced.parquet");
    const metadata = await readParquetMetadata(store, "data/vector-nested-sliced.parquet");

    const batches = [];
    for await (const batch of readParquetVectorBatchesFromFile(file, metadata, {
      columns: ["id", "tags", "attrs"],
      rowStart: 1,
      rowEnd: 6,
      batchSize: 5,
      stats: queryStats(),
    })) {
      batches.push(batch);
    }

    expect(batches.flatMap(({ batch }) => materializeBatchRows(batch))).toEqual([
      { id: 2, tags: ["b", "c"], attrs: { kind: "two" } },
      { id: 3, tags: null, attrs: null },
      { id: 4, tags: [], attrs: {} },
      { id: 5, tags: ["e"], attrs: { kind: "five" } },
      { id: 6, tags: ["f", null], attrs: { kind: null } },
    ]);
    expect(batches.some(({ batch }) => batch.columns.tags?.type === "list")).toBe(true);
    expect(batches.some(({ batch }) => batch.columns.attrs?.type === "map")).toBe(true);
  });

  it("reads dictionary and all-null physical pages through direct vectors", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/vector-null-dictionary.parquet", {
      rowGroupSize: [7],
      pageSize: 1024,
      schema: [
        { name: "root", num_children: 5 },
        { name: "id", type: "INT32", repetition_type: "REQUIRED" },
        { name: "label", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "REQUIRED" },
        {
          name: "plain_label",
          type: "BYTE_ARRAY",
          converted_type: "UTF8",
          repetition_type: "OPTIONAL",
        },
        { name: "maybe_bool", type: "BOOLEAN", repetition_type: "OPTIONAL" },
        {
          name: "empty_text",
          type: "BYTE_ARRAY",
          converted_type: "UTF8",
          repetition_type: "OPTIONAL",
        },
      ],
      columnData: [
        { name: "id", data: [1, 2, 3, 4, 5, 6, 7] },
        {
          name: "label",
          data: ["a", "a", "b", "a", "b", "c", "b"],
          encoding: "RLE_DICTIONARY",
        },
        { name: "plain_label", data: ["p1", null, "p3", "p4", null, "p6", "p7"] },
        { name: "maybe_bool", data: [null, null, null, null, null, null, null] },
        {
          name: "empty_text",
          data: [null, null, null, null, null, null, null],
          encoding: "PLAIN",
        },
      ],
    });
    const file = await fileBuffer(store, "data/vector-null-dictionary.parquet");
    const metadata = await readParquetMetadata(store, "data/vector-null-dictionary.parquet");
    const decodedColumnCache = new DecodedColumnCache(
      new SharedMemoryCache({ maxBytes: 1024 * 1024 }),
      { maxBytes: 1024 * 1024, policy: "latency" },
    );
    const coldStats = queryStats();

    const coldRows = await collectVectorRows(file, metadata, {
      columns: ["id", "label", "plain_label", "maybe_bool", "empty_text"],
      rowStart: 1,
      rowEnd: 6,
      batchSize: 2,
      stats: coldStats,
      decodedColumnCache,
      decodedColumnCacheKey: "data/vector-null-dictionary.parquet",
    });
    const warmStats = queryStats();
    const warmRows = await collectVectorRows(file, metadata, {
      columns: ["id", "label", "plain_label", "maybe_bool", "empty_text"],
      rowStart: 1,
      rowEnd: 6,
      batchSize: 2,
      stats: warmStats,
      decodedColumnCache,
      decodedColumnCacheKey: "data/vector-null-dictionary.parquet",
    });

    expect(coldRows).toEqual([
      { id: 2, label: "a", plain_label: null, maybe_bool: null, empty_text: null },
      { id: 3, label: "b", plain_label: "p3", maybe_bool: null, empty_text: null },
      { id: 4, label: "a", plain_label: "p4", maybe_bool: null, empty_text: null },
      { id: 5, label: "b", plain_label: null, maybe_bool: null, empty_text: null },
      { id: 6, label: "c", plain_label: "p6", maybe_bool: null, empty_text: null },
    ]);
    expect(warmRows).toEqual(coldRows);
    expect(coldStats.cacheMisses).toBeGreaterThan(0);
    expect(warmStats.cacheHits).toBeGreaterThan(0);

    const batches = [];
    for await (const batch of readParquetVectorBatchesFromFile(file, metadata, {
      columns: ["label", "plain_label", "maybe_bool", "empty_text"],
      rowStart: 1,
      rowEnd: 6,
      batchSize: 2,
      stats: queryStats(),
    })) {
      batches.push(batch.batch);
    }
    expect(batches[0]?.columns.label).toMatchObject({
      type: "dict",
    });
    expect(batches[0]?.columns.plain_label).toMatchObject({
      type: "utf8",
      valid: new Uint8Array([0, 1, 1, 0, 1]),
    });
    expect(batches[0]?.columns.maybe_bool).toMatchObject({
      type: "null",
      length: 5,
    });
    expect(batches[0]?.columns.empty_text).toMatchObject({
      type: "null",
      length: 5,
    });

    const malformedDictionaryMetadata = structuredClone(metadata);
    const label = malformedDictionaryMetadata.row_groups[0]?.columns.find(
      (column) => column.meta_data?.path_in_schema.join(".") === "label",
    )?.meta_data;
    if (label !== undefined) delete label.dictionary_page_offset;
    expect(canReadParquetVectorBatches(malformedDictionaryMetadata, { columns: ["label"] })).toBe(
      false,
    );
  });

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

  it("reads only needed page windows for partial row ranges", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/vector-page-window.parquet", {
      rowGroupSize: [256],
      pageSize: 512,
      columnData: [
        {
          name: "payload",
          data: Array.from({ length: 256 }, (_, index) => `payload-${index}-${"x".repeat(96)}`),
          type: "STRING",
        },
      ],
    });
    const file = await fileBuffer(store, "data/vector-page-window.parquet");
    const metadata = await readParquetMetadata(store, "data/vector-page-window.parquet");
    const compressedChunkBytes = Number(
      metadata.row_groups[0]?.columns.find(
        (column) => column.meta_data?.path_in_schema.join(".") === "payload",
      )?.meta_data?.total_compressed_size ?? 0,
    );
    const ranges: number[] = [];
    const countedFile: StoreAsyncBuffer = {
      ...file,
      async slice(start, end) {
        ranges.push((end ?? file.byteLength) - start);
        return await file.slice(start, end);
      },
    };

    const rows = await collectVectorRows(countedFile, metadata, {
      columns: ["payload"],
      rowStart: 0,
      rowEnd: 2,
      batchSize: 2,
      stats: queryStats(),
    });

    expect(rows).toEqual([
      {
        payload:
          "payload-0-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
      },
      {
        payload:
          "payload-1-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
      },
    ]);
    expect(ranges.reduce((sum, bytes) => sum + bytes, 0)).toBeLessThan(compressedChunkBytes);
  });

  it("streams pages for full row ranges when the consumer can stop early", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/vector-early-stop.parquet", {
      rowGroupSize: [256],
      pageSize: 512,
      columnData: [
        {
          name: "payload",
          data: Array.from({ length: 256 }, (_, index) => `payload-${index}-${"x".repeat(96)}`),
          type: "STRING",
        },
      ],
    });
    const file = await fileBuffer(store, "data/vector-early-stop.parquet");
    const metadata = await readParquetMetadata(store, "data/vector-early-stop.parquet");
    const compressedChunkBytes = Number(
      metadata.row_groups[0]?.columns.find(
        (column) => column.meta_data?.path_in_schema.join(".") === "payload",
      )?.meta_data?.total_compressed_size ?? 0,
    );
    const ranges: number[] = [];
    const countedFile: StoreAsyncBuffer = {
      ...file,
      async slice(start, end) {
        ranges.push((end ?? file.byteLength) - start);
        return await file.slice(start, end);
      },
    };
    const batches = [];

    for await (const batch of readParquetVectorBatchesFromFile(countedFile, metadata, {
      columns: ["payload"],
      batchSize: 2,
      canStopEarly: true,
      stats: queryStats(),
    })) {
      batches.push(batch);
      break;
    }

    expect(
      materializeBatchRows(batches[0]?.batch ?? { rowCount: 0, columns: {} }).slice(0, 2),
    ).toEqual([
      {
        payload:
          "payload-0-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
      },
      {
        payload:
          "payload-1-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
      },
    ]);
    expect(ranges.reduce((sum, bytes) => sum + bytes, 0)).toBeLessThan(compressedChunkBytes);
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

    for (const logicalType of ["GEOMETRY", "GEOGRAPHY"] as const) {
      const supported = structuredClone(metadata);
      const rawLeaf = supported.schema.find((entry) => entry.name === "raw");
      if (rawLeaf !== undefined) rawLeaf.logical_type = { type: logicalType };
      expect(canReadParquetVectorBatches(supported, { columns: ["raw"] })).toBe(true);
    }

    for (const logicalType of ["DATE"] as const) {
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

  it("preserves nullable accelerator-width numeric pages", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/vector-narrow-numeric.parquet", {
      rowGroupSize: [5],
      pageSize: 1024,
      schema: [
        { name: "root", num_children: 5 },
        { name: "f32", type: "FLOAT", repetition_type: "OPTIONAL" },
        { name: "i32", type: "INT32", repetition_type: "OPTIONAL" },
        {
          name: "u32",
          type: "INT32",
          converted_type: "UINT_32",
          repetition_type: "OPTIONAL",
        },
        { name: "required_f32", type: "FLOAT", repetition_type: "REQUIRED" },
        {
          name: "required_u32",
          type: "INT32",
          converted_type: "UINT_32",
          repetition_type: "REQUIRED",
        },
      ],
      columnData: [
        { name: "f32", data: [1.25, null, 3.5, 4.75, 5] },
        { name: "i32", data: [-2, -1, null, 1, 2] },
        { name: "u32", data: [0, null, 2_147_483_648, 4_000_000_000, 4_294_967_295] },
        { name: "required_f32", data: [10.25, 20.5, 30.75, 40, 50.125] },
        { name: "required_u32", data: [10, 20, 3_000_000_000, 40, 4_294_967_295] },
      ],
    });
    const file = await fileBuffer(store, "data/vector-narrow-numeric.parquet");
    const metadata = await readParquetMetadata(store, "data/vector-narrow-numeric.parquet");

    const batches = [];
    for await (const batch of readParquetVectorBatchesFromFile(file, metadata, {
      columns: ["f32", "i32", "u32", "required_f32", "required_u32"],
      rowStart: 1,
      rowEnd: 5,
      batchSize: 2,
      stats: queryStats(),
    })) {
      batches.push(batch.batch);
    }

    expect(batches).toHaveLength(1);
    const batch = batches[0];
    if (batch === undefined) throw new Error("missing narrow numeric vector batch");
    expect(batch.columns.f32).toMatchObject({
      type: "f32",
      valid: new Uint8Array([0, 1, 1, 1]),
    });
    expect(batch.columns.i32).toMatchObject({
      type: "i32",
      valid: new Uint8Array([1, 0, 1, 1]),
    });
    expect(batch.columns.u32).toMatchObject({
      type: "u32",
      valid: new Uint8Array([0, 1, 1, 1]),
    });
    expect(batch.columns.required_f32).toMatchObject({ type: "f32" });
    expect(batch.columns.required_u32).toMatchObject({ type: "u32" });
    expect(materializeBatchRows(batch)).toEqual([
      { f32: null, i32: -1, u32: null, required_f32: 20.5, required_u32: 20 },
      {
        f32: 3.5,
        i32: null,
        u32: 2_147_483_648,
        required_f32: 30.75,
        required_u32: 3_000_000_000,
      },
      { f32: 4.75, i32: 1, u32: 4_000_000_000, required_f32: 40, required_u32: 40 },
      {
        f32: 5,
        i32: 2,
        u32: 4_294_967_295,
        required_f32: 50.125,
        required_u32: 4_294_967_295,
      },
    ]);
  });

  it("queries GeoParquet WKB geometry columns through vector spatial predicates", async () => {
    const store = memoryStore();
    const path = "data/vector-geoparquet-wkb.parquet";
    await writeParquet(store, path, {
      rowGroupSize: [4],
      pageSize: 1024,
      schema: [
        { name: "root", num_children: 2 },
        { name: "id", type: "INT32", repetition_type: "REQUIRED" },
        {
          name: "geometry",
          type: "BYTE_ARRAY",
          repetition_type: "OPTIONAL",
        },
      ],
      columnData: [
        { name: "id", data: [1, 2, 3, 4] },
        {
          name: "geometry",
          data: [wkbPoint(1, 1), wkbPoint(5, 5), null, wkbPoint(1.5, 1.5)],
        },
      ],
    });

    const file = await fileBuffer(store, path);
    const metadata = await readParquetMetadata(store, path);
    const geometryLeaf = metadata.schema.find((entry) => entry.name === "geometry");
    if (geometryLeaf !== undefined) geometryLeaf.logical_type = { type: "GEOMETRY" };
    const batches = [];
    for await (const batch of readParquetVectorBatchesFromFile(file, metadata, {
      columns: ["id", "geometry"],
      rowStart: 0,
      rowEnd: 4,
      batchSize: 4,
      stats: queryStats(),
    })) {
      batches.push(batch.batch);
    }
    expect(batches[0]?.columns.geometry?.type).toBe("binary");

    const head = await store.head(path);
    if (head === null) throw new Error(`missing ${path}`);
    const metadataCache = memoryCache<typeof metadata>();
    await metadataCache.set(`parquet-metadata:${path}:${head.size}:${head.etag ?? "no-etag"}`, {
      value: metadata,
    });
    const lake = createParquetLake({ store, metadataCache });
    const polygon = wkbPolygon([
      [0, 0],
      [2, 0],
      [2, 2],
      [0, 2],
      [0, 0],
    ]);
    await expect(
      lake
        .path(path)
        .where(fn("st_within", col("geometry"), lit(polygon)))
        .select(["id"])
        .toArray(),
    ).resolves.toEqual([{ id: 1 }, { id: 4 }]);
    await expect(
      lake
        .path(path)
        .where(fn("st_contains", lit(polygon), col("geometry")))
        .select(["id"])
        .toArray(),
    ).resolves.toEqual([{ id: 1 }, { id: 4 }]);
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

function wkbPoint(x: number, y: number): Uint8Array {
  const bytes = new Uint8Array(1 + 4 + 8 + 8);
  const view = new DataView(bytes.buffer);
  view.setUint8(0, 1);
  view.setUint32(1, 1, true);
  view.setFloat64(5, x, true);
  view.setFloat64(13, y, true);
  return bytes;
}

function wkbPolygon(points: readonly (readonly [number, number])[]): Uint8Array {
  const bytes = new Uint8Array(1 + 4 + 4 + 4 + points.length * 16);
  const view = new DataView(bytes.buffer);
  view.setUint8(0, 1);
  view.setUint32(1, 3, true);
  view.setUint32(5, 1, true);
  view.setUint32(9, points.length, true);
  let offset = 13;
  for (const [x, y] of points) {
    view.setFloat64(offset, x, true);
    view.setFloat64(offset + 8, y, true);
    offset += 16;
  }
  return bytes;
}
