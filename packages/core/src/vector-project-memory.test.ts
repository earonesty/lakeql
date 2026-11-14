import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
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
      const descriptors =
        2 * c.object +
        2 * c.property +
        ["id", "doubled", "category"].reduce(
          (bytes, name) => bytes + c.property + c.string + name.length * c.stringCodeUnit,
          0,
        );
      expect(budget.usedBytes).toBe(descriptors + 51); // Typed outputs plus admitted descriptors.
      return budget.peakBytes;
    } finally {
      scope.dispose();
      expect(budget.usedBytes).toBe(0);
    }
  }
  const peak = run(65536);
  expect(peak).toBe(728); // CASE condition overlaps construction, then releases.
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

it("charges duplicate projected names once and releases failed output independently", () => {
  const batch = batchFromColumns({ id: [1] });
  const descriptors = 2 * c.object + 2 * c.property + c.property + c.string + 2 * c.stringCodeUnit;
  const pool = new MemoryBudget(descriptors + 16);
  const parent = new MemoryScope(pool);
  parent.allocate(16, "sibling", () => undefined);
  const output = vectorProjectBatch(batch, ["id", "*", "id"], { id: col("id") }, parent);
  expect(Object.keys(output.columns)).toEqual(["id"]);
  expect(output.columns.id).toBe(batch.columns.id);
  expect(pool.usedBytes).toBe(descriptors + 16);
  parent.dispose();
  expect(pool.usedBytes).toBe(0);

  for (const mode of ["missing-selection", "missing-expression", "budget"]) {
    const budget = new MemoryBudget(mode === "budget" ? 32 : 65536);
    const scope = new MemoryScope(budget);
    scope.allocate(16, "sibling", () => undefined);
    expect(() =>
      vectorProjectBatch(
        batch,
        mode === "missing-selection" ? ["id", "missing"] : ["id"],
        mode === "missing-expression" ? { alias: col("missing") } : {},
        scope,
      ),
    ).toThrow();
    expect(budget.usedBytes).toBe(16);
    scope.dispose();
    expect(budget.usedBytes).toBe(0);
  }
});
