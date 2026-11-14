import { expect, it } from "vitest";
import { evaluate } from "./evaluator.js";
import type { Expr } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";

const expression: Expr = {
  kind: "call",
  fn: "replace",
  args: [
    {
      kind: "call",
      fn: "cast",
      args: [
        { kind: "column", name: "value" },
        { kind: "literal", value: "string" },
      ],
    },
    { kind: "literal", value: "1" },
    { kind: "literal", value: "$`é😀$'" },
  ],
};

it("admits nested row casts and replacement expansion at the exact boundary", () => {
  const row = { value: 111n };
  const expected = String(row.value).replaceAll("1", "$`é😀$'");
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(evaluate(expression, row, scope)).toBe(expected);
  const bytes = scope.budget.usedBytes;
  scope.dispose();
  for (const cap of [bytes, bytes - 1]) {
    const bounded = new MemoryScope(new MemoryBudget(cap));
    try {
      if (cap === bytes) expect(evaluate(expression, row, bounded)).toBe(expected);
      else expect(() => evaluate(expression, row, bounded)).toThrow(/budget/i);
    } finally {
      bounded.dispose();
    }
    expect(bounded.budget.usedBytes).toBe(0);
  }
});

it("releases aggregate expression temporaries between rows and on rejection", async () => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  for (const huge of [false, true]) {
    const pool = new MemoryBudget(32768);
    let closed = false;
    const lake = new Lake({
      store,
      memoryPool: pool,
      scanner: {
        async *scan() {
          try {
            for (let i = 0; i < 1000; i++) yield [{ value: huge ? "1".repeat(1000) : 111n }];
          } finally {
            closed = true;
          }
        },
      },
    });
    const result = lake
      .path("table")
      .run()
      .aggregateWithState([], {
        count: { op: "count", expr: expression },
      });
    if (huge) await expect(result).rejects.toThrow(/budget/i);
    else expect((await result).rows).toEqual([{ count: 1000 }]);
    expect(closed).toBe(true);
    expect(pool.usedBytes).toBe(0);
  }
});
