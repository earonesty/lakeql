import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { gt } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";

it.each([
  "failure",
  "cancel",
])("releases borrowed deferred aggregate inputs on late read %s", async (outcome) => {
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
            yield { rowOffset: 8, batch: batchFromColumns({ value: [80, 90] }) };
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
    .groupBy([])
    .aggregate({ total: { op: "sum", column: "value" } });
  if (outcome === "failure") await expect(pending).rejects.toThrow("late read failure");
  else await expect(pending).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(closed).toBe(2);
  expect(pool.usedBytes).toBe(0);
});
