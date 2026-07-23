import { gte, memoryCache, memoryStore } from "lakeql-core";
import { describe, expect, it } from "vitest";
import {
  createParquetLake,
  encodedParquetMetadataCache,
  readParquetMetadata,
  writeParquet,
} from "./index.js";
import { countingObjectStore } from "./test-helpers.js";

describe("encoded Parquet metadata cache", () => {
  it("preserves parsed metadata across lake instances through a byte cache", async () => {
    const store = countingObjectStore(memoryStore());
    await writeParquet(store, "data/cache.parquet", {
      rowGroupSize: 2,
      columnData: [
        { name: "serial", type: "INT64", data: [1n, 2n, 3n, 4n] },
        { name: "label", type: "BYTE_ARRAY", data: ["a", "b", "c", "d"] },
      ],
    });
    const byteCache = memoryCache<Uint8Array>();
    const metadataCache = encodedParquetMetadataCache(byteCache);

    const metadata = await readParquetMetadata(store, "data/cache.parquet");
    await metadataCache.set("roundtrip", { value: metadata });
    const roundtrip = await metadataCache.get("roundtrip");
    expect(roundtrip?.value.num_rows).toBe(metadata.num_rows);
    expect(typeof roundtrip?.value.num_rows).toBe("bigint");
    expect(roundtrip?.value.row_groups).toHaveLength(2);

    store.resetCounters();
    await createParquetLake({ store, metadataCache })
      .path("data/cache.parquet")
      .where(gte("serial", 3n))
      .planTasks();
    expect(store.counters.getRange).toBeGreaterThan(0);

    store.resetCounters();
    const tasks = await createParquetLake({ store, metadataCache })
      .path("data/cache.parquet")
      .where(gte("serial", 3n))
      .planTasks();
    expect(tasks[0]?.rowGroupRanges).toEqual([{ start: 1, end: 2 }]);
    expect(store.counters.getRange).toBe(0);
  });
});
