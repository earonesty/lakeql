import { expect, it } from "vitest";
import type { Expr } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";

for (const ordered of [false, true]) {
  it.each([
    "projection",
    "qualify",
  ])(`admits window %s expansion (ordered=${ordered})`, async (route) => {
    for (const expand of [false, true]) {
      const pool = new MemoryBudget(65536);
      const sibling = pool.reserve(1024, "sibling job");
      const store = memoryStore();
      await store.put("table", new Uint8Array(1));
      const lake = new Lake({
        store,
        memoryPool: pool,
        scanner: {
          async *scan() {
            yield [
              { id: 0, text: "ß" },
              { id: 1, text: "a" },
            ];
          },
        },
      });
      const expression: Expr = {
        kind: "call",
        fn: "replace",
        args: [
          { kind: "column", name: "text" },
          { kind: "literal", value: "ß" },
          { kind: "literal", value: expand ? "x".repeat(100000) : "accepted" },
        ],
      };
      let query = lake
        .path("table")
        .window("rn", {
          fn: "row_number",
          args: [],
          over: { partitionBy: [], orderBy: [{ expr: { kind: "column", name: "id" } }] },
        })
        .batchSize(1);
      if (ordered) query = query.orderBy([{ column: "id" }]);
      if (route === "projection") query = query.project({ result: expression });
      else
        query = query.qualify({
          kind: "compare",
          op: "eq",
          left: expression,
          right: { kind: "literal", value: "accepted" },
        });
      const result = query.toArray();
      if (expand) await expect(result).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
      else
        expect(await result).toEqual(
          route === "projection"
            ? [
                { id: 0, text: "ß", rn: 1, result: "accepted" },
                { id: 1, text: "a", rn: 2, result: "a" },
              ]
            : [{ id: 0, text: "ß", rn: 1 }],
        );
      expect(pool.usedBytes).toBe(sibling.bytes);
      sibling.release();
    }
  });

  it(`releases window projection batches on advance and return (ordered=${ordered})`, async () => {
    const pool = new MemoryBudget(65536);
    const store = memoryStore();
    await store.put("table", new Uint8Array(1));
    const lake = new Lake({
      store,
      memoryPool: pool,
      scanner: {
        async *scan() {
          yield Array.from({ length: 8 }, (_, id) => ({ id, text: "ß" }));
        },
      },
    });
    let query = lake
      .path("table")
      .window("rn", {
        fn: "row_number",
        args: [],
        over: { partitionBy: [], orderBy: [{ expr: { kind: "column", name: "id" } }] },
      })
      .batchSize(1)
      .project({ text: { kind: "call", fn: "upper", args: [{ kind: "column", name: "text" }] } });
    if (ordered) query = query.orderBy([{ column: "id" }]);
    const iterator = query.run().batches()[Symbol.asyncIterator]();
    expect((await iterator.next()).value).toEqual([{ id: 0, rn: 1, text: "SS" }]);
    const live = pool.usedBytes;
    expect(live).toBeGreaterThan(0);
    expect((await iterator.next()).value).toEqual([{ id: 1, rn: 2, text: "SS" }]);
    expect(pool.usedBytes).toBe(live);
    await iterator.return?.();
    expect(pool.usedBytes).toBe(0);
  });
}
