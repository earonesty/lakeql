import { expect, it } from "vitest";
import { matches } from "./evaluator.js";
import type { Expr } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";

const having: Expr = {
  kind: "compare",
  op: "eq",
  left: { kind: "call", fn: "upper", args: [{ kind: "column", name: "key" }] },
  right: { kind: "literal", value: "SS" },
};

it("releases predicate temporary ownership after true, false, and budget failure", () => {
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  for (let i = 0; i < 1000; i++) {
    expect(matches(having, { key: "ß" }, scope)).toBe(true);
    expect(matches(having, { key: "a" }, scope)).toBe(false);
    expect(pool.usedBytes).toBe(0);
  }
  const peak = pool.peakBytes;
  for (const cap of [peak, peak - 1]) {
    const bounded = new MemoryScope(new MemoryBudget(cap));
    if (cap === peak) expect(matches(having, { key: "ß" }, bounded)).toBe(true);
    else expect(() => matches(having, { key: "ß" }, bounded)).toThrow(/budget/i);
    expect(bounded.budget.usedBytes).toBe(0);
    bounded.dispose();
  }
  scope.dispose();
});

it("budgets HAVING output expansion alongside live aggregate results", async () => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const run = (pool: MemoryBudget, expand: boolean) =>
    new Lake({
      store,
      memoryPool: pool,
      scanner: {
        async *scan() {
          yield [{ key: "ß" }, { key: "a" }];
        },
      },
    })
      .path("table")
      .run()
      .aggregateWithState(
        ["key"],
        { count: { op: "count" } },
        {
          having: expand
            ? {
                kind: "compare",
                op: "eq",
                left: {
                  kind: "call",
                  fn: "replace",
                  args: [
                    { kind: "column", name: "key" },
                    { kind: "literal", value: "ß" },
                    { kind: "literal", value: "a".repeat(100000) },
                  ],
                },
                right: { kind: "literal", value: "a" },
              }
            : having,
          orderBy: [{ column: "count", direction: "desc" }],
          limit: 1,
        },
      );
  const pool = new MemoryBudget(65536);
  expect((await run(pool, false)).rows).toEqual([{ key: "ß", count: 1 }]);
  expect(pool.usedBytes).toBe(0);
  await expect(run(pool, true)).rejects.toThrow(/budget/i);
  expect(pool.usedBytes).toBe(0);
});
