import { isIn, memoryStore, type ObjectStore } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { createParquetLake, writeParquet } from "./index.js";

describe("scattered point lookup physical reads", () => {
  it("bounds concurrent reads by configuration and reads selected rather than total row groups", async () => {
    const rowGroups = 1_300;
    const selectedSerials = Array.from({ length: 32 }, (_, index) => index * 40 + 7);
    const backing = memoryStore();
    await writeParquet(backing, "data/scattered.parquet", {
      rowGroupSize: 1,
      columnData: [
        {
          name: "serial",
          type: "INT32",
          data: Array.from({ length: rowGroups }, (_, index) => index),
        },
        {
          name: "mark_text",
          type: "BYTE_ARRAY",
          data: Array.from({ length: rowGroups }, (_, index) => `mark-${index}`),
        },
        {
          name: "sourced_at",
          type: "INT64",
          data: Array.from({ length: rowGroups }, (_, index) => BigInt(index)),
        },
      ],
    });
    const store = delayedRangeStore(backing);
    const lake = createParquetLake({
      store,
      budget: { maxConcurrentReads: 4 },
      scanRangeCache: { maxBytes: 16 * 1024 * 1024, coalesceBytes: 0 },
    });
    const result = lake
      .path("data/scattered.parquet")
      .select(["serial", "mark_text", "sourced_at"])
      .where(isIn("serial", selectedSerials))
      .run();

    const rows = await result.toArray();

    expect(rows.map((row) => row.serial)).toEqual(selectedSerials);
    expect(result.stats.rowGroupsRead).toBe(selectedSerials.length);
    expect(result.stats.rowGroupsSkipped).toBe(rowGroups - selectedSerials.length);
    expect(result.stats.rowsDecoded).toBe(selectedSerials.length);
    expect(result.stats.rangeRequests).toBeLessThanOrEqual(selectedSerials.length * 3 + 2);
    expect(store.peakActiveRanges).toBeGreaterThan(1);
    expect(store.peakActiveRanges).toBeLessThanOrEqual(4);
  });

  it("does not introduce range-read concurrency when it is not configured", async () => {
    const backing = memoryStore();
    await writeParquet(backing, "data/serial.parquet", {
      rowGroupSize: 1,
      columnData: [
        { name: "serial", type: "INT32", data: [1, 2, 3, 4] },
        { name: "mark_text", type: "BYTE_ARRAY", data: ["a", "b", "c", "d"] },
      ],
    });
    const store = delayedRangeStore(backing);
    const lake = createParquetLake({
      store,
      scanRangeCache: { maxBytes: 1024 * 1024, coalesceBytes: 0 },
    });

    await lake
      .path("data/serial.parquet")
      .select(["serial", "mark_text"])
      .where(isIn("serial", [1, 4]))
      .toArray();

    expect(store.peakActiveRanges).toBe(1);
  });
});

function delayedRangeStore(
  inner: ObjectStore,
): ObjectStore & { readonly peakActiveRanges: number } {
  let activeRanges = 0;
  let peakActiveRanges = 0;
  return {
    get peakActiveRanges() {
      return peakActiveRanges;
    },
    get(path) {
      return inner.get(path);
    },
    async getRange(path, range) {
      activeRanges += 1;
      peakActiveRanges = Math.max(peakActiveRanges, activeRanges);
      try {
        await new Promise<void>((resolve) => setTimeout(resolve, 1));
        return await inner.getRange(path, range);
      } finally {
        activeRanges -= 1;
      }
    },
    put(path, body, options) {
      return inner.put(path, body, options);
    },
    delete(path) {
      return inner.delete(path);
    },
    list(prefix, options) {
      return inner.list(prefix, options);
    },
    head(path) {
      return inner.head(path);
    },
  };
}
