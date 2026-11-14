import { expect, it } from "vitest";
import { type Batch, materializeBatchRows, type Vector, vectorValue } from "./batch.js";
import { planConcat } from "./concat-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { planUnionConcat } from "./union-concat.js";
import { concatBatches, concatOwnedBatches } from "./vector-sort.js";

it.each([1, 2, 256, 257, 1025])("concatenates %i child inputs with bounded byte tags", (count) => {
  const vectors: Vector[] = Array.from({ length: count }, (_, i) => ({
    type: "f64",
    values: Float64Array.of(i),
  }));
  const plan = planUnionConcat(vectors);
  const scope = new MemoryScope(new MemoryBudget(plan.bytes));
  const result = plan.execute(scope);
  for (let row = 0; row < count; row++) expect(vectorValue(result, row)).toBe(row);
  expect(scope.budget.usedBytes).toBe(plan.bytes);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  if (plan.bytes > 0) {
    const tight = new MemoryScope(new MemoryBudget(plan.bytes - 1));
    expect(() => plan.execute(tight)).toThrow(/budget/);
    expect(tight.budget.peakBytes).toBe(0);
    tight.dispose();
  } else expect(result).toBe(vectors[0]);
});

it("plans union/null/dictionary concat and pins input ownership after producer release", () => {
  const budget = new MemoryBudget(65536);
  const producer = new MemoryScope(budget);
  const numbers = producer.allocate(16, "input", () => Float64Array.of(7, 9)).value;
  const union: Vector = {
    type: "union",
    tags: Uint8Array.of(0, 1),
    offsets: Uint32Array.of(0, 1),
    children: [
      { type: "utf8", values: ["text"] },
      { type: "f64", values: numbers },
    ],
  };
  const batches: Batch[] = [
    { rowCount: 2, columns: { v: union } },
    { rowCount: 1, columns: { v: { type: "null", length: 1 } } },
    {
      rowCount: 2,
      columns: { v: { type: "dict", indices: Uint32Array.of(1, 0), dictionary: union } },
    },
  ];
  const owners = batches.map((batch) => producer.retainAllocations(batch));
  const plan = planConcat(batches);
  expect(plan).toBeDefined();
  const output = concatOwnedBatches(owners, budget);
  for (const owner of owners) owner.release();
  producer.dispose();
  expect(materializeBatchRows(output.value)).toEqual([
    { v: "text" },
    { v: 9 },
    { v: null },
    { v: 9 },
    { v: "text" },
  ]);
  expect(budget.usedBytes).toBe((plan?.bytes ?? 0) + 16);
  output.release();
  expect(budget.usedBytes).toBe(0);
  const direct = new MemoryScope(new MemoryBudget(plan?.bytes ?? 0));
  expect(materializeBatchRows(concatBatches(batches, direct, plan))).toHaveLength(5);
  direct.dispose();
});

it("handles empty children and rejects invalid row extents before allocation", () => {
  const plan = planUnionConcat([
    { type: "null", length: 0 },
    { type: "null", length: 2 },
    { type: "null", length: 0 },
  ]);
  const scope = new MemoryScope(new MemoryBudget(plan.bytes));
  const vector = plan.execute(scope);
  expect(vectorValue(vector, 0)).toBe(null);
  expect(vectorValue(vector, 1)).toBe(null);
  scope.dispose();
  expect(() => planUnionConcat([])).toThrow();
  expect(() => planUnionConcat([{ type: "null", length: 0x100000000 }])).toThrow(/extent/);
});

it("fills lower tree levels instead of allocating sparse intermediate nodes", () => {
  const vectors: Vector[] = Array.from({ length: 513 }, (_, row) => ({
    type: "f64",
    values: Float64Array.of(row),
  }));
  const plan = planUnionConcat(vectors);
  const scope = new MemoryScope(new MemoryBudget(plan.bytes));
  const result = plan.execute(scope);
  if (result.type !== "union") throw Error("Expected union");
  expect(result.children).toHaveLength(3);
  const left = result.children[0];
  const middle = result.children[1];
  expect(left?.type === "union" ? left.children.length : -1).toBe(256);
  expect(middle?.type === "union" ? middle.children.length : -1).toBe(256);
  expect(result.children[2]).toBe(vectors[512]);
  expect(plan.bytes).toBeLessThan(16384);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
