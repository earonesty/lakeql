import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";

it.each([
  false,
  true,
])("owns borrowed column top-k across reused batches (owned=%s)", async (owned) => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const run = async (capacity: number) => {
    const pool = new MemoryBudget(capacity);
    let closed = false;
    const bytes = Uint8Array.of(1, 2);
    const lake = new Lake({
      store,
      memoryPool: pool,
      scanner: {
        scan() {
          throw Error("Unexpected row fallback");
        },
        async *scanColumns() {
          try {
            yield batchFromColumns({ score: [1], payload: [bytes] });
            bytes.fill(9);
            yield batchFromColumns({ score: [2], payload: [bytes] });
            bytes.fill(99);
          } finally {
            closed = true;
          }
        },
      },
    });
    try {
      const query = lake
        .path("table")
        .orderBy([{ column: "score" }])
        .limit(1);
      const output = owned ? await query.run().toOwnedArray() : undefined;
      try {
        const rows = output?.value ?? (await query.toArray());
        expect(rows).toEqual([{ score: 1, payload: Uint8Array.of(1, 2) }]);
        expect(closed).toBe(true);
        if (output) expect(pool.usedBytes).toBeGreaterThan(0);
      } finally {
        output?.release();
      }
      return pool.peakBytes;
    } finally {
      expect(pool.usedBytes).toBe(0);
    }
  };
  const peak = await run(65536);
  expect(await run(peak)).toBe(peak);
  await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});
