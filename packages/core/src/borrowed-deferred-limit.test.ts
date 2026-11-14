import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { gt } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";

it.each([
  "failure",
  "cancel",
])("releases borrowed deferred limit inputs on late read %s", async (outcome) => {
  const pool = new MemoryBudget(1024 * 1024);
  const controller = new AbortController();
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  let closed = 0;
  const lake = new Lake({
    store,
    memoryPool: pool,
    budget: { signal: controller.signal },
    scanner: {
      scan() {
        throw Error("Unexpected row fallback");
      },
      async *scanVectorBatches(_path, options) {
        try {
          if (options.columns?.includes("value")) {
            await Promise.resolve();
            if (outcome === "failure") throw Error("late read failure");
            controller.abort();
            yield { rowOffset: 8, batch: batchFromColumns({ value: [80] }) };
          } else
            yield {
              rowOffset: 0,
              batch: batchFromColumns({ id: Array.from({ length: 10 }, (_, i) => i) }),
            };
        } finally {
          closed++;
        }
      },
    },
  });
  const pending = lake
    .path("table")
    .where(gt("id", 7))
    .select(["id", "value"])
    .limit(1)
    .run()
    .toOwnedArray();
  if (outcome === "failure") await expect(pending).rejects.toThrow("late read failure");
  else await expect(pending).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(closed).toBe(2);
  expect(pool.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("bounds deferred offset scanning and detaches reused payload (owned=%s)", async (owned) => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  async function run(count: number, capacity: number) {
    const pool = new MemoryBudget(capacity);
    let earlyClosed = 0,
      lateClosed = 0;
    const payload = Uint8Array.of(7, 8);
    const windows: number[][] = [];
    const lake = new Lake({
      store,
      memoryPool: pool,
      budget: { maxAllocatedMemoryBytes: capacity, maxMemoryBytes: 1 },
      scanner: {
        scan() {
          throw Error("Unexpected row fallback");
        },
        async *scanVectorBatches(_path, options) {
          if (options.columns?.includes("payload")) {
            try {
              windows.push([options.rowStart ?? -1, options.rowEnd ?? -1]);
              yield { rowOffset: count - 1, batch: batchFromColumns({ payload: [payload] }) };
            } finally {
              payload.fill(99);
              lateClosed++;
            }
          } else {
            try {
              for (let start = 0; start < count; start += 10) {
                yield {
                  rowOffset: start,
                  batch: batchFromColumns({ id: Array.from({ length: 10 }, (_, i) => start + i) }),
                };
              }
            } finally {
              earlyClosed++;
            }
          }
        },
      },
    });
    try {
      const query = lake
        .path("table")
        .where(gt("id", -1))
        .select(["id", "payload"])
        .offset(count - 1)
        .limit(1)
        .batchSize(10);
      const output = owned ? await query.run().toOwnedArray() : undefined;
      try {
        expect(output?.value ?? (await query.toArray())).toEqual([
          { id: count - 1, payload: Uint8Array.of(7, 8) },
        ]);
        expect(windows).toEqual([[count - 1, count]]);
        if (output) expect(pool.usedBytes).toBeGreaterThan(0);
      } finally {
        output?.release();
      }
      return pool.peakBytes;
    } finally {
      expect(earlyClosed).toBe(1);
      expect(lateClosed).toBeLessThanOrEqual(1);
      expect(pool.usedBytes).toBe(0);
    }
  }
  const peak = await run(10, 65536);
  expect(await run(1000, peak)).toBe(peak);
  await expect(run(1000, peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});
