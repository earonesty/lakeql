import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, memoryCache, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { ParquetScanAdapter } from "./scan-adapter.js";
import type { ParquetMetadata } from "./types.js";

it("admits cached task metadata even after an unowned metadata-cache hit", async () => {
  const store = memoryStore();
  await store.put(
    "tasks.parquet",
    new Uint8Array(
      parquetWriteBuffer({
        rowGroupSize: [2, 2, 2],
        columnData: [{ name: "id", type: "INT32", data: [0, 1, 2, 3, 4, 5] }],
      }),
    ),
  );
  const adapter = new ParquetScanAdapter(store, {
    taskMetadataMode: "cached",
    metadataCache: memoryCache<ParquetMetadata>(),
    scanRangeCache: { maxBytes: 0 },
  });
  const expected = { rowGroupCount: 3, rowGroupRanges: [{ start: 0, end: 3 }] };
  expect(await adapter.planTask("tasks.parquet", { partitionValues: {} })).toEqual(expected);
  const tight = new MemoryBudget(64);
  await expect(
    adapter.planTask("tasks.parquet", { partitionValues: {}, memoryBudget: tight }),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(tight.usedBytes).toBe(0);
  const pool = new MemoryBudget(1024 * 1024);
  expect(
    await adapter.planTask("tasks.parquet", { partitionValues: {}, memoryBudget: pool }),
  ).toEqual(expected);
  expect(pool.peakBytes).toBeGreaterThan(64);
  expect(pool.usedBytes).toBe(0);
});
