import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { col, gt } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";

it.each(
  [false, true].flatMap((streamed) =>
    [false, true].flatMap((filtered) =>
      [false, true].map((limited) => ({ streamed, filtered, limited })),
    ),
  ),
)("owns borrowed projected output across reused batches (%j)", async ({
  streamed,
  filtered,
  limited,
}) => {
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
        async *scanVectorBatches() {
          try {
            yield { rowOffset: 0, batch: batchFromColumns({ score: [1], payload: [bytes] }) };
            bytes.fill(9);
            yield { rowOffset: 1, batch: batchFromColumns({ score: [2], payload: [bytes] }) };
            bytes.fill(99);
          } finally {
            closed = true;
          }
        },
      },
    });
    try {
      let query = lake.path("table").select(["*"]);
      if (filtered) query = query.where(gt(col("score"), 0));
      if (limited) query = query.offset(1).limit(1);
      const streamedRows = [];
      if (streamed) for await (const batch of query.run().batches()) streamedRows.push(...batch);
      try {
        const rows = streamed ? streamedRows : await query.toArray();
        const expected = [
          { score: 1, payload: Uint8Array.of(1, 2) },
          { score: 2, payload: Uint8Array.of(9, 9) },
        ];
        expect(rows).toEqual(limited ? expected.slice(1) : expected);
        expect(closed).toBe(true);
      } finally {
        expect(pool.usedBytes).toBe(0);
      }
      return pool.peakBytes;
    } finally {
      expect(closed).toBe(true);
      expect(pool.usedBytes).toBe(0);
    }
  };
  const peak = await run(65536);
  expect(await run(peak)).toBe(peak);
  await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});
