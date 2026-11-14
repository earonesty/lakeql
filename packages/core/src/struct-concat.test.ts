import { expect, it } from "vitest";
import { type Batch, materializeBatchRows, type Vector } from "./batch.js";
import { concatAllocationBytes } from "./concat-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { concatOwnedBatches } from "./vector-sort.js";

it.each([40, 39])("concatenates recursive struct fields within cap %s", (cap) => {
  const batches: Batch[] = [0, 1].map((index) => ({
    rowCount: 1,
    columns: {
      record: {
        type: "struct",
        length: 1,
        valid: Uint8Array.of(index === 0 ? 1 : 0),
        fields: Object.fromEntries([
          ["id", { type: "i64", values: BigInt64Array.of(9007199254740993n + BigInt(index)) }],
          ["__proto__", { type: "bool", values: Uint8Array.of(1) }],
          [
            "inner",
            {
              type: "struct",
              length: 1,
              valid: Uint8Array.of(1),
              fields: {
                data: { type: "binary-view", data: Uint8Array.of(255), fixedLength: 1, length: 1 },
              },
            },
          ],
        ]) as Record<string, Vector>,
      },
    },
  }));
  expect(concatAllocationBytes(batches)).toBe(40);
  const expected = batches.flatMap(materializeBatchRows);
  const budget = new MemoryBudget(cap);
  const scope = new MemoryScope(budget);
  const inputs = batches.map((batch) => scope.retainAllocations(batch));
  try {
    if (cap === 39) {
      expect(() => concatOwnedBatches(inputs)).toThrow(/budget/);
      expect(budget.peakBytes).toBe(0);
    } else {
      const output = concatOwnedBatches(inputs);
      try {
        expect(budget.usedBytes).toBe(40);
        expect(materializeBatchRows(output.value)).toEqual(expected);
        const vector = output.value.columns.record;
        if (vector?.type !== "struct") throw new Error("expected struct");
        expect(vector.fields.id?.type).toBe("i64");
        expect(Object.hasOwn(vector.fields, "__proto__")).toBe(true);
        expect(vector.fields.inner?.type).toBe("struct");
      } finally {
        output.release();
      }
    }
  } finally {
    for (const input of inputs) input.release();
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it.each([32, 31])("preserves nested dictionary ownership at cap %s", (cap) => {
  const budget = new MemoryBudget(cap);
  const scope = new MemoryScope(budget);
  const dictionary: Vector = {
    type: "i64",
    values: scope.allocate(16, "dictionary", () => BigInt64Array.of(9007199254740993n, 2n)).value,
  };
  const batches: Batch[] = [0, 1].map((index) => ({
    rowCount: 1,
    columns: {
      record: {
        type: "struct",
        length: 1,
        fields: {
          x: {
            type: "dict",
            indices: scope.allocate(4, "indices", () => Uint32Array.of(index)).value,
            dictionary,
          },
        },
      },
    },
  }));
  const inputs = batches.map((batch) => scope.retainAllocations(batch));
  scope.dispose();
  try {
    if (cap === 31) {
      expect(() => concatOwnedBatches(inputs)).toThrow(/budget/);
      expect(budget.peakBytes).toBe(24);
    } else {
      const output = concatOwnedBatches(inputs);
      try {
        for (const input of inputs) input.release();
        expect(budget.usedBytes).toBe(32);
        expect(materializeBatchRows(output.value)).toEqual([
          { record: { x: 9007199254740993n } },
          { record: { x: 2n } },
        ]);
      } finally {
        output.release();
      }
    }
  } finally {
    for (const input of inputs) input.release();
  }
  expect(budget.usedBytes).toBe(0);
});

it("preserves normalization for different struct field sets", () => {
  const batches: Batch[] = ["a", "b"].map((name) => ({
    rowCount: 1,
    columns: {
      record: {
        type: "struct",
        length: 1,
        fields: { [name]: { type: "i32", values: Int32Array.of(1) } },
      },
    },
  }));
  expect(concatAllocationBytes(batches)).toBeUndefined();
  const scope = new MemoryScope(new MemoryBudget(1024));
  const inputs = batches.map((batch) => scope.retainAllocations(batch));
  try {
    const output = concatOwnedBatches(inputs);
    try {
      expect(materializeBatchRows(output.value)).toEqual([
        { record: { a: 1, b: null } },
        { record: { a: null, b: 1 } },
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
