import { expect, it } from "vitest";
import { batchFromColumns, materializeBatchRows } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { concatBatches, concatOwnedBatches } from "./vector-sort.js";

it.each([36, 35])("admits nullable bigint concat at exact limit %i", (limit) => {
  const left = batchFromColumns({ value: [1n, null] });
  const right = batchFromColumns({ value: [9223372036854775807n, -9223372036854775808n] });
  const scope = new MemoryScope(new MemoryBudget(limit));
  try {
    if (limit === 35) {
      expect(() => concatBatches([left, right], scope)).toThrow();
      expect(scope.budget.peakBytes).toBe(4);
    } else {
      const result = concatBatches([left, right], scope);
      expect(materializeBatchRows(result)).toEqual([
        { value: 1n },
        { value: null },
        { value: 9223372036854775807n },
        { value: -9223372036854775808n },
      ]);
      expect(scope.budget.usedBytes).toBe(36);
    }
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it.each([
  64, 63,
])("charges source/output concat overlap and releases detached sources (%i)", (limit) => {
  const root = new MemoryBudget(limit);
  const stage = new MemoryScope(root);
  const left = stage.retainAllocations(batchFromColumns({ value: [1, 2] }, stage));
  const rightScope = new MemoryScope(root);
  const right = rightScope.retainAllocations(batchFromColumns({ value: [3, 4] }, rightScope));
  stage.dispose();
  rightScope.dispose();
  if (limit === 63) {
    expect(() => concatOwnedBatches([left, right])).toThrow();
    expect(root.usedBytes).toBe(32);
  } else {
    const output = concatOwnedBatches([left, right]);
    left.release();
    right.release();
    expect(root.usedBytes).toBe(32);
    expect(materializeBatchRows(output.value)).toEqual([1, 2, 3, 4].map((value) => ({ value })));
    output.release();
  }
  left.release();
  right.release();
  expect(root.usedBytes).toBe(0);
});

it("retains shared dictionaries after both input owners are released", () => {
  const root = new MemoryBudget(100);
  const scope = new MemoryScope(root);
  const values = scope.allocate(16, "dictionary values", () => Float64Array.of(10, 20)).value;
  const dictionary = { type: "f64" as const, values };
  const firstIndices = scope.allocate(4, "first indices", () => Uint32Array.of(1)).value;
  const left = scope.retainAllocations({
    rowCount: 1,
    columns: {
      value: { type: "dict" as const, indices: firstIndices, dictionary },
    },
  });
  const secondIndices = scope.allocate(4, "second indices", () => Uint32Array.of(0)).value;
  const right = scope.retainAllocations({
    rowCount: 1,
    columns: {
      value: { type: "dict" as const, indices: secondIndices, dictionary },
    },
  });
  scope.dispose();
  const output = concatOwnedBatches([left, right]);
  left.release();
  right.release();
  expect(root.usedBytes).toBe(32);
  expect(materializeBatchRows(output.value)).toEqual([{ value: 20 }, { value: 10 }]);
  output.release();
  expect(root.usedBytes).toBe(0);
});

it("cleans up earlier pins when a later concat input belongs to another pool", () => {
  const roots = [new MemoryBudget(100), new MemoryBudget(100)];
  const inputs = roots.map((root) => {
    const scope = new MemoryScope(root);
    const owner = scope.retainAllocations(batchFromColumns({ value: [1] }, scope));
    scope.dispose();
    return owner;
  });
  expect(() => concatOwnedBatches(inputs)).toThrow();
  expect(roots.map((root) => root.usedBytes)).toEqual([8, 8]);
  for (const input of inputs) input.release();
  expect(roots.map((root) => root.usedBytes)).toEqual([0, 0]);
  expect(() => concatOwnedBatches([])).toThrow();
  const empty = concatOwnedBatches([], roots[0]);
  expect(empty.value).toEqual({ rowCount: 0, columns: {} });
  empty.release();
});

it("shares concatenated dictionary indices only for identical complete source sequences", () => {
  const first = Uint32Array.of(0);
  const second = Uint32Array.of(1);
  const different = Uint32Array.of(0);
  const numbers = { type: "f64" as const, values: Float64Array.of(10, 20) };
  const text = { type: "utf8" as const, values: ["a", "b"] };
  const left = {
    rowCount: 1,
    columns: {
      a: { type: "dict" as const, indices: first, dictionary: numbers },
      b: { type: "dict" as const, indices: first, dictionary: text },
      c: { type: "dict" as const, indices: first, dictionary: numbers },
    },
  };
  const right = {
    rowCount: 1,
    columns: {
      a: { type: "dict" as const, indices: second, dictionary: numbers },
      b: { type: "dict" as const, indices: second, dictionary: text },
      c: { type: "dict" as const, indices: different, dictionary: numbers },
    },
  };
  const budget = new MemoryBudget(16);
  const scope = new MemoryScope(budget);
  const output = concatBatches([left, right], scope);
  const { a, b, c } = output.columns;
  if (a?.type !== "dict" || b?.type !== "dict" || c?.type !== "dict")
    throw new Error("Expected dictionaries");
  expect(a.indices).toBe(b.indices);
  expect(c.indices).not.toBe(a.indices);
  expect(budget.usedBytes).toBe(16);
  expect(materializeBatchRows(output)).toEqual([
    { a: 10, b: "a", c: 10 },
    { a: 20, b: "b", c: 10 },
  ]);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});

it.each([36, 35])("concatenates different nested bigint dictionaries within %i bytes", (limit) => {
  const left = {
    rowCount: 2,
    columns: {
      value: {
        type: "dict" as const,
        indices: Uint32Array.of(0, 1),
        dictionary: {
          type: "dict" as const,
          indices: Uint32Array.of(1, 0),
          dictionary: {
            type: "i64" as const,
            values: BigInt64Array.of(9007199254740993n, 5n),
            valid: Uint8Array.of(1, 0),
          },
        },
      },
    },
  };
  const right = {
    rowCount: 2,
    columns: {
      value: {
        type: "dict" as const,
        indices: Uint32Array.of(0, 1),
        valid: Uint8Array.of(0, 1),
        dictionary: { type: "i64" as const, values: BigInt64Array.of(7n, -9007199254740993n) },
      },
    },
  };
  const scope = new MemoryScope(new MemoryBudget(limit));
  try {
    if (limit === 35) expect(() => concatBatches([left, right], scope)).toThrow("budget");
    else {
      const output = concatBatches([left, right], scope);
      expect(output.columns.value?.type).toBe("i64");
      expect(materializeBatchRows(output)).toEqual([
        { value: null },
        { value: 9007199254740993n },
        { value: null },
        { value: -9007199254740993n },
      ]);
      expect(scope.budget.usedBytes).toBe(36);
    }
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it("detaches newly packed dictionary unions from producer ownership across sibling jobs", () => {
  const root = new MemoryBudget(10000);
  const producer = root.child();
  const consumer = root.child();
  const inputs = ["a", "b"].map((value) => {
    const scope = new MemoryScope(producer);
    const data = scope.allocate(1, "dictionary payload", () =>
      new TextEncoder().encode(value),
    ).value;
    const indices = scope.allocate(400, "source indices", () => new Uint32Array(100)).value;
    const owner = scope.retainAllocations({
      rowCount: 100,
      columns: {
        value: {
          type: "dict" as const,
          indices,
          dictionary: { type: "utf8-view" as const, data, fixedLength: 1, length: 1 },
        },
      },
    });
    scope.dispose();
    return owner;
  });
  const output = concatOwnedBatches(inputs, consumer);
  expect(output.value.columns.value?.type).toBe("dict");
  expect(output.bytes).toBe(818); // 800 indices + 16 dictionary ranges + 2 payload.
  for (const input of inputs) input.release();
  expect(producer.usedBytes).toBe(0);
  expect(consumer.usedBytes).toBe(818);
  expect(root.usedBytes).toBe(818);
  expect(materializeBatchRows(output.value)).toEqual(
    Array.from({ length: 200 }, (_, i) => ({ value: i < 100 ? "a" : "b" })),
  );
  output.release();
  expect(root.usedBytes).toBe(0);
});

it("concatenates packed and dictionary-backed text batches from adaptive gathers", () => {
  const packed = {
    type: "utf8-view" as const,
    data: new TextEncoder().encode("ab"),
    fixedLength: 1,
    length: 2,
  };
  const dictionary = { type: "dict" as const, indices: Uint32Array.of(1, 0), dictionary: packed };
  const scope = new MemoryScope(new MemoryBudget(24));
  const output = concatBatches(
    [
      { rowCount: 2, columns: { value: packed } },
      { rowCount: 2, columns: { value: dictionary } },
    ],
    scope,
  );
  expect(materializeBatchRows(output)).toEqual([
    { value: "a" },
    { value: "b" },
    { value: "b" },
    { value: "a" },
  ]);
  expect(scope.budget.usedBytes).toBe(16);
  expect(scope.budget.peakBytes).toBe(24);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
