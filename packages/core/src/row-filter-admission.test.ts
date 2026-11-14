import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Expr } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake, type QueryResult } from "./query.js";

const routes = ["batches", "aggregate", "ordered", "topK", "sort"] as const;

async function consume(query: QueryResult, route: (typeof routes)[number]) {
  if (route === "aggregate")
    return (await query.aggregateWithState([], { count: { op: "count" } })).rows;
  if (route === "topK") return (await query.topKWithState()).rows;
  if (route === "sort") return (await query.sortWithState()).rows;
  return query.toArray();
}

it.each(routes)("admits row filter expansion and closes %s scans on failure", async (route) => {
  for (const expand of [false, true]) {
    const pool = new MemoryBudget(65536);
    const sibling = pool.reserve(1024, "unrelated job");
    const store = memoryStore();
    await store.put("table", new Uint8Array(1));
    let closed = false;
    const lake = new Lake({
      store,
      memoryPool: pool,
      scanner: {
        async *scan() {
          try {
            yield [{ key: "ß" }, { key: "a" }];
          } finally {
            closed = true;
          }
        },
      },
    });
    const expr: Expr = {
      kind: "compare",
      op: "eq",
      left: {
        kind: "call",
        fn: "replace",
        args: [
          { kind: "column", name: "key" },
          { kind: "literal", value: "ß" },
          { kind: "literal", value: expand ? "a".repeat(100000) : "accepted" },
        ],
      },
      right: { kind: "literal", value: "accepted" },
    };
    let path = lake.path("table").where(expr);
    if (route === "ordered" || route === "topK" || route === "sort")
      path = path.orderBy([{ column: "key" }]);
    if (route === "topK") path = path.limit(1);
    const result = consume(path.run(), route);
    if (expand) await expect(result).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    else expect(await result).toEqual(route === "aggregate" ? [{ count: 1 }] : [{ key: "ß" }]);
    expect(closed).toBe(true);
    expect(pool.usedBytes).toBe(sibling.bytes);
    sibling.release();
    expect(pool.usedBytes).toBe(0);
  }
});

it("releases filter scratch before yielding and closes the scanner on early return", async () => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  let closed = false;
  let planningBytes = 0;
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        planningBytes = pool.usedBytes;
        try {
          for (let i = 0; i < 100; i++) yield [{ key: "ß" }];
        } finally {
          closed = true;
        }
      },
    },
  });
  const rows = lake
    .path("table")
    .where({
      kind: "compare",
      op: "eq",
      left: { kind: "call", fn: "upper", args: [{ kind: "column", name: "key" }] },
      right: { kind: "literal", value: "SS" },
    })
    .run()
    .rows()
    [Symbol.asyncIterator]();
  expect((await rows.next()).value).toEqual({ key: "ß" });
  expect(pool.peakBytes).toBeGreaterThan(0);
  // Filter scratch has released; the suspended output directory remains live.
  expect(planningBytes).toBeGreaterThan(0);
  expect(pool.usedBytes).toBe(planningBytes + c.array + c.arraySlot);
  await rows.return?.();
  expect(closed).toBe(true);
  expect(pool.usedBytes).toBe(0);
});
