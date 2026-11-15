import { MemoryBudget, materializeBatchRows, memoryStore, SharedMemoryCache } from "lakeql-core";
import { expect, it, vi } from "vitest";
import {
  DecodedColumnCache,
  decodedColumnCacheKey,
  decodedColumnPageCacheKey,
  decodedDictionaryPageCacheKey,
} from "./decoded-column-cache.js";
import { readParquetColumnBatches, writeParquet } from "./index.js";

it("separates column-list boundaries containing the historical delimiter", () => {
  const common = { path: "fixture", byteLength: 123, rowStart: 0, rowEnd: 1 };
  expect(decodedColumnCacheKey({ ...common, columns: ["a\u001fb", "c"] })).not.toBe(
    decodedColumnCacheKey({ ...common, columns: ["a", "b\u001fc"] }),
  );
  expect(decodedColumnCacheKey({ ...common, columns: ["a", "b"] })).not.toBe(
    decodedColumnCacheKey({ ...common, columns: ["a\u001fb"] }),
  );
});

it("separates file identity fields in page and dictionary keys", () => {
  const common = {
    column: "value",
    rowGroupStart: 0,
    pageRowStart: 0,
    pageRowEnd: 1,
    pageOffset: 4,
    compressedPageSize: 10,
    values: 1,
  };
  for (const key of [decodedColumnPageCacheKey, decodedDictionaryPageCacheKey]) {
    expect(key({ ...common, path: "x\u001f7", byteLength: 8, etag: "tag" })).not.toBe(
      key({ ...common, path: "x", byteLength: 7, etag: "8\u001ftag" }),
    );
  }
});

it("returns correct cold and warm Parquet projections with delimiter-bearing column names", async () => {
  const store = memoryStore();
  await writeParquet(store, "fixture.parquet", {
    rowGroupSize: [2],
    columnData: [
      { name: "a\u001fb", type: "INT32", data: [11, 12] },
      { name: "c", type: "INT32", data: [13, 14] },
      { name: "a", type: "INT32", data: [21, 22] },
      { name: "b\u001fc", type: "INT32", data: [23, 24] },
    ],
  });
  const pool = new MemoryBudget(1024 * 1024);
  const shared = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 1024 * 1024 });
  const cache = new DecodedColumnCache(shared, { maxBytes: 1024 * 1024, policy: "latency" });
  const lookups = vi.spyOn(cache, "get");
  const read = async (columns: string[]) => {
    const rows = [];
    for await (const value of readParquetColumnBatches(store, "fixture.parquet", {
      columns,
      batchSize: 2,
      decodedColumnCache: cache,
      decodedColumnCacheKey: "fixture.parquet",
    }))
      rows.push(...materializeBatchRows(value.batch));
    return rows;
  };
  try {
    for (let repeat = 0; repeat < 2; repeat++) {
      expect(await read(["a\u001fb", "c"])).toEqual([
        { "a\u001fb": 11, c: 13 },
        { "a\u001fb": 12, c: 14 },
      ]);
      expect(await read(["a", "b\u001fc"])).toEqual([
        { a: 21, "b\u001fc": 23 },
        { a: 22, "b\u001fc": 24 },
      ]);
    }
    expect(
      lookups.mock.results.map((result) => result.type === "return" && result.value !== undefined),
    ).toEqual([false, false, true, true]);
  } finally {
    lookups.mockRestore();
    shared.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
});

it("distinguishes empty, missing, escaped and Unicode identity fields", () => {
  const common = { path: "file", byteLength: 10, rowStart: 0, rowEnd: 1 };
  const columns = [
    [],
    [""],
    ["\u001f"],
    ["\\u001f"],
    ["\ud800"],
    ["\\ud800"],
    ["猫"],
    ["a", "b"],
    ["a\u001fb"],
  ];
  const keys = columns.map((value) => decodedColumnCacheKey({ ...common, columns: value }));
  expect(new Set(keys).size).toBe(columns.length);
  expect(decodedColumnCacheKey({ ...common, columns: [], etag: "" })).not.toBe(
    decodedColumnCacheKey({ ...common, columns: [] }),
  );
  expect(decodedColumnCacheKey({ ...common, columns: ["a", "b"] })).not.toBe(
    decodedColumnCacheKey({ ...common, columns: ["b", "a"] }),
  );
});
