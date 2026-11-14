import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";

it("keeps prototype-like physical group columns when projections do not own them", async () => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(65536);
  const requests: (string[] | undefined)[] = [];
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan(_path, options) {
        requests.push(options.columns);
        const row = { constructor: 7 };
        yield [
          Object.fromEntries(
            (options.columns ?? Object.keys(row)).map((name) => [
              name,
              row[name as keyof typeof row],
            ]),
          ),
        ];
      },
    },
  });
  const result = await lake
    .path("table")
    .project({})
    .run()
    .aggregateWithState(["constructor"], { n: { op: "count" } });
  expect(requests).toEqual([["constructor"]]);
  expect(result.rows).toEqual([{ constructor: 7, n: 1 }]);
  expect(pool.usedBytes).toBe(0);
});
