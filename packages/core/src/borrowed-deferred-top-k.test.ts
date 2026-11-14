import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake, type ScanAdapter } from "./query.js";

it.each([
  false,
  true,
])("admits borrowed deferred winners and detaches reused binary output at the exact peak (owned=%s)", async (owned) => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const run = async (capacity: number) => {
    const pool = new MemoryBudget(capacity);
    const reads: string[][] = [];
    const bytes = Uint8Array.of(7, 8);
    const scanner: ScanAdapter = {
      scan() {
        throw Error("Unexpected row fallback");
      },
      async *scanVectorBatches(_path, options) {
        reads.push(options.columns ?? []);
        if (options.columns?.includes("payload")) {
          yield { rowOffset: 1, batch: batchFromColumns({ payload: [bytes] }) };
          bytes.fill(99);
        } else {
          yield { rowOffset: 0, batch: batchFromColumns({ score: [2, 9, 4] }) };
        }
      },
    };
    try {
      const query = new Lake({ store, scanner, memoryPool: pool })
        .path("table")
        .select(["score", "payload"])
        .orderBy([{ column: "score", direction: "desc" }])
        .limit(1);
      const output = owned ? await query.run().toOwnedArray() : undefined;
      const rows = output?.value ?? (await query.toArray());
      if (output) {
        expect(pool.usedBytes).toBeGreaterThan(0);
        output.release();
      }
      expect(reads).toEqual([["score"], ["payload"]]);
      expect(rows).toEqual([{ score: 9, payload: Uint8Array.of(7, 8) }]);
      expect(bytes).toEqual(Uint8Array.of(99, 99));
      return pool.peakBytes;
    } finally {
      expect(pool.usedBytes).toBe(0);
    }
  };
  const peak = await run(65536);
  expect(peak).toBeGreaterThan(0);
  expect(await run(peak)).toBe(peak);
  await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});
