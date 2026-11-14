import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { batchFromColumns, materializeBatchRows } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { gatherBatch } from "./vector-sort.js";

it("shares one admitted selection across nested columns without flattening children", () => {
  const source = batchFromColumns({
    lists: [[1, 2], null, []],
    structs: [{ x: 1 }, null, { x: 3 }],
    maps: [new Map([["a", 1]]), null, new Map()],
  });
  const budget = new MemoryBudget(c.view + 16);
  const scope = new MemoryScope(budget);
  const result = gatherBatch(source, [2, 1, 0, 0], scope);
  expect(budget.usedBytes).toBe(c.view + 16);
  const columns = Object.values(result.columns);
  for (const [index, vector] of columns.entries()) {
    expect(vector.type).toBe("dict");
    if (vector.type !== "dict") throw new Error("Expected dictionary");
    expect(vector.dictionary).toBe(Object.values(source.columns)[index]);
    expect(vector.indices).toBe(columns[0]?.type === "dict" ? columns[0].indices : undefined);
  }
  const rows = materializeBatchRows(source);
  expect(materializeBatchRows(result)).toEqual([rows[2], rows[1], rows[0], rows[0]]);
  expect(materializeBatchRows(gatherBatch(result, [3, 1]))).toEqual([rows[0], rows[1]]);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
  const under = new MemoryScope(new MemoryBudget(c.view + 15));
  expect(() => gatherBatch(source, [2, 1, 0, 0], under)).toThrow();
  expect(under.budget.peakBytes).toBe(0);
  under.dispose();
});

it("admits numeric payload and validity before allocating gather output", () => {
  const source = batchFromColumns({ value: [1, null, 3] });
  for (const limit of [2 * c.view + 18, 2 * c.view + 17]) {
    const scope = new MemoryScope(new MemoryBudget(limit));
    try {
      if (limit === 2 * c.view + 18) {
        expect(materializeBatchRows(gatherBatch(source, [2, 1], scope))).toEqual([
          { value: 3 },
          { value: null },
        ]);
        expect(scope.budget.usedBytes).toBe(2 * c.view + 18);
      } else {
        expect(() => gatherBatch(source, [2, 1], scope)).toThrow();
        expect(scope.budget.peakBytes).toBe(0);
      }
    } finally {
      scope.dispose();
    }
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it("gathers a large nested payload using only row indices", () => {
  const source = batchFromColumns({ values: [Array.from({ length: 200000 }, (_, i) => i)] });
  const scope = new MemoryScope(new MemoryBudget(c.view + 4));
  const result = gatherBatch(source, [0], scope);
  expect(result.columns.values).toMatchObject({ type: "dict", dictionary: source.columns.values });
  expect(scope.budget.peakBytes).toBe(c.view + 4);
  scope.dispose();
});

it.each([
  -1,
  0.5,
  3,
  NaN,
  Infinity,
  Number.MAX_SAFE_INTEGER + 1,
])("rejects invalid gather index %s before allocating", (index) => {
  const source = batchFromColumns({ id: [1, 2, 3], nested: [[1], [], null] });
  const scope = new MemoryScope(new MemoryBudget(100));
  expect(() => gatherBatch(source, [0, index], scope)).toThrow(/Gather index/);
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});

it("does not charge discarded validity when selected values are all present", () => {
  const source = batchFromColumns({ id: [1, null, 3] });
  const scope = new MemoryScope(new MemoryBudget(c.view + 16));
  const result = gatherBatch(source, [2, 0], scope);
  expect(materializeBatchRows(result)).toEqual([{ id: 3 }, { id: 1 }]);
  expect(result.columns.id).not.toHaveProperty("valid");
  expect(scope.budget.peakBytes).toBe(c.view + 16);
  expect(() => gatherBatch(source, [], scope)).toThrow(/budget/); // Empty output still needs a view.
  scope.dispose();
});

it("checks the physical dictionary index capacity without allocating a giant source", () => {
  const source = {
    rowCount: 0x100000001,
    columns: { value: { type: "struct" as const, fields: {}, length: 0x100000001 } },
  };
  const scope = new MemoryScope(new MemoryBudget(c.view + 4));
  expect(() => gatherBatch(source, [0x100000000], scope)).toThrow(/Uint32 capacity/);
  expect(scope.budget.usedBytes).toBe(0);
  expect(materializeBatchRows(gatherBatch(source, [0xffffffff], scope))).toEqual([{ value: {} }]);
  expect(scope.budget.usedBytes).toBe(c.view + 4);
  scope.dispose();
});

it("admits empty output views and releases a failed gather without disturbing prior output", () => {
  const source = batchFromColumns({ id: [1, 2] });
  const scope = new MemoryScope(new MemoryBudget(c.view));
  const empty = gatherBatch(source, [], scope);
  expect(empty.rowCount).toBe(0);
  expect(scope.budget.usedBytes).toBe(c.view);
  expect(() => gatherBatch(source, [0], scope)).toThrow(/budget/);
  expect(scope.budget.usedBytes).toBe(c.view);
  const owner = scope.retainAllocations(empty);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(c.view);
  owner.release();
  expect(scope.budget.usedBytes).toBe(0);
});
