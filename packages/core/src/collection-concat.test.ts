import { expect, it } from "vitest";
import { type Batch, materializeBatchRows, type Vector } from "./batch.js";
import { concatAllocationBytes } from "./concat-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { concatOwnedBatches } from "./vector-sort.js";

it.each([74, 73])("preserves empty maps in lists and rebases child windows at cap %s", (cap) => {
  const vector: Vector = {
    type: "list",
    offsets: Int32Array.of(0, 0, 2),
    child: {
      type: "map",
      offsets: Int32Array.of(1, 1, 2),
      keys: { type: "utf8-view", data: Uint8Array.of(97, 98), fixedLength: 1, length: 2 },
      values: { type: "i64", values: BigInt64Array.of(99n, 2n) },
    },
  };
  const batch: Batch = { rowCount: 2, columns: { x: vector } };
  expect(concatAllocationBytes([batch, batch])).toBe(74);
  const budget = new MemoryBudget(cap);
  const scope = new MemoryScope(budget);
  const input = scope.retainAllocations(batch);
  try {
    if (cap === 73) {
      expect(() => concatOwnedBatches([input, input])).toThrow(/budget/);
      expect(budget.peakBytes).toBe(0);
    } else {
      const output = concatOwnedBatches([input, input]);
      try {
        expect(materializeBatchRows(output.value)).toEqual([
          { x: [] },
          { x: [{}, { b: 2n }] },
          { x: [] },
          { x: [{}, { b: 2n }] },
        ]);
        expect(budget.usedBytes).toBe(74);
        const list = output.value.columns.x;
        expect(list?.type).toBe("list");
        if (list?.type === "list") expect(list.child.type).toBe("map");
      } finally {
        output.release();
      }
    }
  } finally {
    input.release();
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it("normalizes null collection batches without losing empty collections", () => {
  const batches: Batch[] = [
    { rowCount: 2, columns: { x: { type: "null", length: 2 } } },
    {
      rowCount: 2,
      columns: {
        x: {
          type: "list",
          offsets: Int32Array.of(0, 0, 1),
          child: { type: "i32", values: Int32Array.of(7) },
        },
      },
    },
  ];
  const scope = new MemoryScope(new MemoryBudget(1024));
  const inputs = batches.map((batch) => scope.retainAllocations(batch));
  try {
    const output = concatOwnedBatches(inputs);
    try {
      expect(materializeBatchRows(output.value)).toEqual([
        { x: null },
        { x: null },
        { x: [] },
        { x: [7] },
      ]);
    } finally {
      output.release();
    }
  } finally {
    for (const input of inputs) input.release();
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it.each([
  { offsets: [2, 1] },
  { offsets: [0, 3] },
  { offsets: [] },
])("rejects malformed collection offsets $offsets before output allocation", ({ offsets }) => {
  const scope = new MemoryScope(new MemoryBudget(1024));
  const batch: Batch = {
    rowCount: Math.max(0, offsets.length - 1),
    columns: {
      x: {
        type: "list",
        offsets: Int32Array.from(offsets),
        child: { type: "i32", values: Int32Array.of(1, 2) },
      },
    },
  };
  const input = scope.retainAllocations(batch);
  try {
    expect(() => concatOwnedBatches([input])).toThrow(/offset/);
    expect(scope.budget.peakBytes).toBe(0);
  } finally {
    input.release();
    scope.dispose();
  }
});

it("keeps direct scalar backing charged when null normalization creates a dictionary", () => {
  const budget = new MemoryBudget(128);
  const scope = new MemoryScope(budget);
  const values = scope.allocate(8, "source", () => Float64Array.of(42)).value;
  const batches: Batch[] = [
    { rowCount: 1, columns: { x: { type: "f64", values } } },
    { rowCount: 1, columns: { x: { type: "null", length: 1 } } },
  ];
  const inputs = batches.map((batch) => scope.retainAllocations(batch));
  scope.dispose();
  try {
    const output = concatOwnedBatches(inputs);
    try {
      for (const input of inputs) input.release();
      expect(materializeBatchRows(output.value)).toEqual([{ x: 42 }, { x: null }]);
      expect(budget.usedBytes).toBe(18);
    } finally {
      output.release();
    }
  } finally {
    for (const input of inputs) input.release();
  }
  expect(budget.usedBytes).toBe(0);
});

it.each(
  [false, true].flatMap((nullFirst) =>
    [25, 24].flatMap((cap) => [0, 1, 2].map((depth) => ({ nullFirst, cap, depth }))),
  ),
)("preserves dictionary-backed maps with nulls ($nullFirst, cap $cap, depth $depth)", ({
  nullFirst,
  cap,
  depth,
}) => {
  let dictionary: Vector = {
    type: "map",
    offsets: Int32Array.of(0, 0, 1),
    keys: { type: "utf8", values: ["k"] },
    values: { type: "i32", values: Int32Array.of(7) },
  };
  for (let i = 0; i < depth; i++)
    dictionary = { type: "dict", indices: Uint32Array.of(0, 1), dictionary };
  const concrete: Batch = {
    rowCount: 3,
    columns: { x: { type: "dict", indices: Uint32Array.of(0, 1, 0), dictionary } },
  };
  const empty: Batch = { rowCount: 1, columns: { x: { type: "null", length: 1 } } };
  const batches = nullFirst ? [empty, concrete] : [concrete, empty];
  const expected = batches.flatMap(materializeBatchRows);
  const scope = new MemoryScope(new MemoryBudget(cap));
  const inputs = batches.map((batch) => scope.retainAllocations(batch));
  try {
    if (cap === 24) {
      expect(() => concatOwnedBatches(inputs)).toThrow(/budget/);
      expect(scope.budget.usedBytes).toBe(0);
      return;
    }
    const output = concatOwnedBatches(inputs);
    try {
      expect(materializeBatchRows(output.value)).toEqual(expected);
      expect(scope.budget.peakBytes).toBe(25);
      expect(scope.budget.usedBytes).toBe(20);
      const result = output.value.columns.x;
      if (result?.type !== "dict") throw new Error("Expected shared dictionary");
      expect(result.dictionary).toBe(dictionary);
    } finally {
      output.release();
    }
  } finally {
    for (const input of inputs) input.release();
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});
