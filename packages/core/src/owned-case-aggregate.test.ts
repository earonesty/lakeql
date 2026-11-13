import { expect, it } from "vitest";
import type { Batch } from "./batch.js";
import type { Expr } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { OwnedAggregateState } from "./owned-aggregate-state.js";
import { OwnedGroupDirectory } from "./owned-group-directory.js";

const expression: Expr = {
  kind: "case",
  whens: [
    {
      when: {
        kind: "in",
        target: { kind: "call", fn: "upper", args: [{ kind: "column", name: "text" }] },
        values: [{ kind: "literal", value: "SS" }],
        negated: false,
      },
      value: { kind: "literal", value: 1 },
    },
  ],
  else: { kind: "literal", value: 2 },
};
const batch: Batch = {
  rowCount: 3,
  columns: {
    key: { type: "u8", values: new Uint8Array([0, 1, 0]) },
    text: { type: "utf8-view", data: new TextEncoder().encode("ßéß"), fixedLength: 2, length: 3 },
  },
};
it.each([false, true])("admits CASE plans and scratch in owned aggregate grouped=%s", (grouped) => {
  const run = (cap: number) => {
    const scope = new MemoryScope(new MemoryBudget(cap));
    try {
      const spec = { total: { op: "sum" as const, expr: expression } };
      const state = grouped
        ? OwnedGroupDirectory.allocate(scope, ["key"], spec)
        : new OwnedAggregateState(scope, spec);
      state.updateBatch(batch);
      const retained = scope.budget.usedBytes;
      state.updateBatch(batch, new Uint8Array([1, 0, 1]));
      expect(scope.budget.usedBytes).toBe(retained);
      const result = state.finish();
      try {
        expect(result.value).toEqual(
          grouped
            ? [
                { key: 0, total: 4 },
                { key: 1, total: 2 },
              ]
            : { total: 6 },
        );
      } finally {
        result.release();
      }
      return scope.budget.peakBytes;
    } finally {
      scope.dispose();
      expect(scope.budget.usedBytes).toBe(0);
    }
  };
  const peak = run(65536);
  expect(run(peak)).toBe(peak);
  // Arena slack can be reclaimed under pressure, so the roomy peak is not
  // the minimum executable budget. Find and verify the actual boundary.
  let low = 0;
  let high = peak;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    try {
      expect(run(middle)).toBeLessThanOrEqual(middle);
      high = middle;
    } catch (error) {
      expect(error).toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
      low = middle + 1;
    }
  }
  expect(run(low)).toBeLessThanOrEqual(low);
  expect(() => run(low - 1)).toThrow(/budget/i);
});

it("rejects unsupported CASE branches before mutating aggregate state", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const state = new OwnedAggregateState(scope, {
    n: { op: "count" },
    total: {
      op: "sum",
      expr: {
        kind: "case",
        whens: [
          {
            when: { kind: "literal", value: false },
            value: { kind: "call", fn: "regexp_replace", args: [] },
          },
        ],
        else: { kind: "literal", value: 1 },
      },
    },
  });
  expect(() => state.updateBatch(batch)).toThrow(/allocation contract/);
  const result = state.finish();
  expect(result.value).toEqual({ n: 0, total: 0 });
  result.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
