import { expect, it } from "vitest";
import { batchFromColumns, materializeBatchRows, vectorFromSource } from "./batch.js";
import { col, gt, lit, mul } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { vectorProjectBatch } from "./vector-project.js";

it("admits projected typed outputs and releases CASE workspace before returning", () => {
  const batch = batchFromColumns({ id: [1, 2, 3], n: [10, null, 30] });
  function run(maxBytes: number) {
    const budget = new MemoryBudget(maxBytes);
    const scope = new MemoryScope(budget);
    try {
      const projected = vectorProjectBatch(
        batch,
        ["id"],
        {
          doubled: mul(col("n"), 2),
          category: { kind: "case", whens: [{ when: gt("n", 20), value: lit(1) }], else: lit(0) },
        },
        scope,
      );
      expect(projected.columns.id).toBe(batch.columns.id);
      expect(materializeBatchRows(projected)).toEqual([
        { id: 1, doubled: 20, category: 0 },
        { id: 2, doubled: null, category: 0 },
        { id: 3, doubled: 60, category: 1 },
      ]);
      expect(budget.usedBytes).toBe(51); // Two Float64 outputs and one validity mask.
      return budget.peakBytes;
    } finally {
      scope.dispose();
      expect(budget.usedBytes).toBe(0);
    }
  }
  const peak = run(65536);
  expect(peak).toBe(54); // CASE condition overlaps construction, then releases.
  expect(run(peak)).toBe(peak);
  expect(() => run(peak - 1)).toThrow();
});

it.each([
  -1,
  1.5,
  Infinity,
  NaN,
  Number.MAX_SAFE_INTEGER + 1,
])("rejects invalid indexed source length %s before reading values", (length) => {
  expect(() =>
    vectorFromSource({
      length,
      get() {
        throw new Error("must not read");
      },
    }),
  ).toThrow(/length/);
});

it("constructs string output during inference without re-evaluating expressions", () => {
  const values = [null, "a\ud800", "😀", null];
  const visits = new Uint8Array(values.length);
  const vector = vectorFromSource({
    length: values.length,
    get(index) {
      visits[index]++;
      return values[index];
    },
  });
  expect(Array.from(visits)).toEqual([1, 1, 1, 1]);
  expect(materializeBatchRows({ rowCount: values.length, columns: { s: vector } })).toEqual(
    values.map((s) => ({ s })),
  );
});
