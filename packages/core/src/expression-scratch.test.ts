import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Batch, batchExprValues, batchFromColumns, type Vector } from "./batch.js";
import type { Expr } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

const column: Expr = { kind: "column", name: "text" };
const batch: Batch = {
  rowCount: 1,
  columns: {
    text: { type: "utf8-view", data: new TextEncoder().encode("ß"), fixedLength: 2, length: 1 },
  },
};

it("charges nested string calls and decoded operands in the per-value scope", () => {
  const expression: Expr = {
    kind: "call",
    fn: "lower",
    args: [{ kind: "call", fn: "upper", args: [column] }],
  };
  const values = batchExprValues(batch, expression);
  const bytes = c.view + c.string + 4 + c.string + 6 + c.string + 12;
  const scope = new MemoryScope(new MemoryBudget(bytes));
  expect(values.valueAt(0, scope)).toBe("ss");
  expect(scope.budget.usedBytes).toBe(bytes);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const rejected = new MemoryScope(new MemoryBudget(bytes - 1));
  expect(() => values.valueAt(0, rejected)).toThrow(/budget/i);
  rejected.dispose();
  expect(rejected.budget.usedBytes).toBe(0);
});

it("keeps CASE masks separate from selected-branch value scratch", () => {
  const pool = new MemoryBudget(65536);
  const compiled = new MemoryScope(pool);
  const values = batchExprValues(
    batch,
    {
      kind: "case",
      whens: [{ when: { kind: "literal", value: true }, value: column }],
      else: { kind: "call", fn: "upper", args: [column] },
    },
    compiled,
  );
  const masks = pool.usedBytes;
  expect(masks).toBeGreaterThan(0);
  for (let i = 0; i < 128; i++) {
    const scratch = new MemoryScope(pool);
    expect(values.valueAt(0, scratch)).toBe("ß");
    expect(pool.usedBytes).toBe(masks + c.view + c.string + 4);
    scratch.dispose();
    expect(pool.usedBytes).toBe(masks);
  }
  compiled.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("propagates per-value admission through coalesce, nullif and numeric operands", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const coalesce = batchExprValues(batch, {
    kind: "call",
    fn: "coalesce",
    args: [{ kind: "literal", value: null }, column],
  });
  expect(coalesce.valueAt(0, scope)).toBe("ß");
  const afterCoalesce = scope.budget.usedBytes;
  expect(afterCoalesce).toBe(c.view + c.string + 4);
  const nullif = batchExprValues(batch, {
    kind: "call",
    fn: "nullif",
    args: [column, { kind: "literal", value: "ß" }],
  });
  expect(nullif.valueAt(0, scope)).toBeNull();
  expect(scope.budget.usedBytes).toBe(afterCoalesce);
  const arithmetic = batchExprValues(batch, {
    kind: "arithmetic",
    op: "add",
    left: column,
    right: { kind: "literal", value: 1 },
  });
  expect(() => arithmetic.valueAt(0, scope)).toThrow(/numeric/);
  expect(scope.budget.usedBytes).toBe(2 * afterCoalesce);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("bounds eager COALESCE scratch independently of unused argument count", () => {
  const upper: Expr = { kind: "call", fn: "upper", args: [column] };
  const run = (count: number, cap: number) => {
    const scope = new MemoryScope(new MemoryBudget(cap));
    try {
      const values = batchExprValues(batch, {
        kind: "call",
        fn: "coalesce",
        args: Array(count).fill(upper),
      });
      expect(values.valueAt(0, scope)).toBe("SS");
      return { used: scope.budget.usedBytes, peak: scope.budget.peakBytes };
    } finally {
      scope.dispose();
      expect(scope.budget.usedBytes).toBe(0);
    }
  };
  const baseline = run(2, 65536);
  expect(run(100, baseline.peak)).toEqual(baseline);
  expect(baseline.peak).toBe(baseline.used * 2);
  expect(() => run(100, baseline.peak - 1)).toThrow(/budget/i);
});

it("still evaluates rejected COALESCE arguments and cleans their failed scratch", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const values = batchExprValues(batch, {
    kind: "call",
    fn: "coalesce",
    args: [
      { kind: "literal", value: "chosen" },
      { kind: "arithmetic", op: "add", left: column, right: { kind: "literal", value: 1 } },
    ],
  });
  expect(() => values.valueAt(0, scope)).toThrow(/numeric/);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it.each(["ß", "different"])("retains only the returned NULLIF payload for %s", (right) => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const values = batchExprValues(batch, {
    kind: "call",
    fn: "nullif",
    args: [column, { kind: "literal", value: right }],
  });
  expect(values.valueAt(0, scope)).toBe(right === "ß" ? null : "ß");
  expect(scope.budget.usedBytes).toBe(right === "ß" ? 0 : c.view + c.string + 4);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("releases both NULLIF operand scopes on a comparison error", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const values = batchExprValues(batch, {
    kind: "call",
    fn: "nullif",
    args: [column, { kind: "literal", value: 1 }],
  });
  expect(() => values.valueAt(0, scope)).toThrow(/different types/);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("advertises allocation-free reads only for borrowed scalar shapes", () => {
  const numeric = batchFromColumns({ value: [1, null, 2], text: ["a", "b", "c"] });
  expect(batchExprValues(numeric, { kind: "column", name: "value" }).allocationFree).toBe(true);
  expect(batchExprValues(numeric, { kind: "column", name: "text" }).allocationFree).toBe(true);
  expect(batchExprValues(batch, column).allocationFree).toBe(false);
  const scope = new MemoryScope(new MemoryBudget(0));
  const value = batchExprValues(numeric, {
    kind: "call",
    fn: "coalesce",
    args: [
      { kind: "column", name: "value" },
      { kind: "literal", value: 9 },
    ],
  });
  expect([0, 1, 2].map((index) => value.valueAt(index, scope))).toEqual([1, 9, 2]);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it.each([
  false,
  true,
])("propagates nested dictionary/union allocation capability, packed=%s", (packed) => {
  const leaf: Vector = packed
    ? { type: "utf8-view", data: new TextEncoder().encode("ß"), fixedLength: 2, length: 1 }
    : { type: "utf8", values: ["ß"] };
  const vector: Vector = {
    type: "dict",
    indices: new Uint32Array([1, 0, 1]),
    dictionary: {
      type: "union",
      tags: new Uint8Array([0, 1]),
      offsets: new Uint32Array([0, 0]),
      children: [{ type: "null", length: 1 }, leaf],
    },
  };
  const input: Batch = { rowCount: 3, columns: { text: vector } };
  expect(batchExprValues(input, column).allocationFree).toBe(!packed);
  const expression: Expr = {
    kind: "call",
    fn: "coalesce",
    args: [column, { kind: "literal", value: "fallback" }],
  };
  const values = batchExprValues(input, expression);
  const scope = new MemoryScope(new MemoryBudget(packed ? 65536 : 0));
  expect([0, 1, 2].map((index) => values.valueAt(index, scope))).toEqual(["ß", "fallback", "ß"]);
  expect(scope.budget.usedBytes).toBe(packed ? 2 * (c.view + c.string + 4) : 0);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  if (packed) {
    const tight = new MemoryScope(new MemoryBudget(0));
    expect(() => values.valueAt(0, tight)).toThrow(/budget/i);
    expect(tight.budget.usedBytes).toBe(0);
    tight.dispose();
  }
});
