import type { ColumnMetaData } from "hyparquet";
import { expect, it } from "vitest";
import { dictionaryPageOffset } from "./dictionary-offset.js";

it("ignores unused zero dictionary offsets but preserves real zero-based dictionaries", () => {
  const column = { dictionary_page_offset: 0n, encodings: ["PLAIN", "RLE"] } as ColumnMetaData;
  expect(dictionaryPageOffset(column)).toBeUndefined();
  for (const encoding of ["PLAIN_DICTIONARY", "RLE_DICTIONARY"] as const)
    expect(dictionaryPageOffset({ ...column, encodings: ["PLAIN", encoding] })).toBe(0n);
  expect(dictionaryPageOffset({ ...column, dictionary_page_offset: 4n })).toBe(4n);
});

it("reads every value in the real unused-zero-offset fixture with owned cleanup", async () => {
  const { readFileSync } = await import("node:fs");
  const { MemoryBudget, memoryStore } = await import("lakeql-core");
  const { createParquetLake } = await import("./index.js");
  const store = memoryStore();
  await store.put(
    "zero.parquet",
    readFileSync(
      new URL(
        "../../../fixtures/external/parquet-testing/data/dict-page-offset-zero.parquet",
        import.meta.url,
      ),
    ),
  );
  const pool = new MemoryBudget(65536);
  const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
  const output = await lake.path("zero.parquet").batchSize(7).run().toOwnedArray();
  try {
    expect(output.value).toEqual(Array.from({ length: 39 }, () => ({ l_partkey: 1552 })));
  } finally {
    output.release();
  }
  expect(pool.usedBytes).toBe(0);
});
