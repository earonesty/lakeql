import { expect, it } from "vitest";
import { batchExprValues } from "./batch.js";
import { evaluate } from "./evaluator.js";
import type { Expr } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it.each([
  "lower",
  "upper",
  "trim",
  "substr",
] as const)("%s preserves row/vector Unicode semantics and rejects before exceeding the cap", (fn) => {
  for (const value of ["", " ßİﬃ😀\ud800 ", "a".repeat(10000)]) {
    const expr: Expr = {
      kind: "call",
      fn,
      args: [
        { kind: "literal", value },
        ...(fn === "substr"
          ? [
              { kind: "literal" as const, value: -6 },
              { kind: "literal" as const, value: 4 },
            ]
          : []),
      ],
    };
    const expected =
      fn === "lower"
        ? value.toLowerCase()
        : fn === "upper"
          ? value.toUpperCase()
          : fn === "trim"
            ? value.trim()
            : value.slice(-6, -2);
    const vector = batchExprValues({ rowCount: 1, columns: {} }, expr);
    for (const read of [
      (scope: MemoryScope) => evaluate(expr, {}, scope),
      (scope: MemoryScope) => vector.valueAt(0, scope),
    ]) {
      const scope = new MemoryScope(new MemoryBudget(1000000));
      expect(read(scope)).toBe(expected);
      const extent = scope.budget.usedBytes;
      scope.dispose();
      for (const limit of [extent, extent - 1]) {
        const bounded = new MemoryScope(new MemoryBudget(limit));
        try {
          if (limit === extent) expect(read(bounded)).toBe(expected);
          else expect(() => read(bounded)).toThrow(/budget/i);
        } finally {
          bounded.dispose();
        }
        expect(bounded.budget.usedBytes).toBe(0);
      }
    }
  }
});
