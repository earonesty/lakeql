import { expect, it } from "vitest";
import { type Batch, materializeBatchRows, type Vector } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { expandNestedDictionary } from "./nested-dictionary.js";
import { concatOwnedBatches } from "./vector-sort.js";

it.each(
  (["map", "list", "struct"] as const).flatMap((shape) =>
    [false, true].map((nullLeaf) => ({ shape, nullLeaf })),
  ),
)("concatenates distinct nested $shape dictionaries (null leaf $nullLeaf)", ({
  shape,
  nullLeaf,
}) => {
  const batches: Batch[] = [0, 1].map((part) => {
    const map: Vector = {
      type: "map",
      offsets: Int32Array.of(0, 0, 1),
      keys: { type: "utf8", values: [`k${part}`] },
      values: { type: "i64", values: BigInt64Array.of(9007199254740993n + BigInt(part)) },
    };
    const leaf: Vector =
      shape === "map"
        ? map
        : shape === "list"
          ? { type: "list", offsets: Int32Array.of(0, 1, 2), child: map }
          : { type: "struct", length: 2, fields: { map } };
    const dictionary: Vector = {
      type: "dict",
      indices: Uint32Array.of(0, 1, 0, 1),
      dictionary: leaf,
      valid: Uint8Array.of(1, 1, 0, 1),
    };
    return {
      rowCount: 4,
      columns: { x: { type: "dict", indices: Uint32Array.of(3, 0, 2, 1), dictionary } },
    };
  });
  if (nullLeaf)
    batches.splice(1, 0, {
      rowCount: 2,
      columns: {
        x: { type: "dict", indices: Uint32Array.of(0, 0), dictionary: { type: "null", length: 1 } },
      },
    });
  const expected = batches.flatMap(materializeBatchRows);
  function run(cap: number): number {
    const scope = new MemoryScope(new MemoryBudget(cap));
    const inputs = batches.map((batch) => scope.retainAllocations(batch));
    try {
      const output = concatOwnedBatches(inputs);
      try {
        expect(materializeBatchRows(output.value)).toEqual(expected);
        expect(output.value.columns.x?.type).toBe(shape);
        return scope.budget.peakBytes;
      } finally {
        output.release();
      }
    } finally {
      for (const input of inputs) input.release();
      scope.dispose();
      expect(scope.budget.usedBytes).toBe(0);
    }
  }
  const peak = run(65536);
  expect(run(peak)).toBe(peak);
  expect(() => run(peak - 1)).toThrow(/budget|admitted/);
});

it("expands only selected collection entries, not unused dictionary payload", () => {
  const child: Vector = {
    type: "i32",
    values: new Proxy(new Int32Array(100000), {
      get(target, key) {
        if (typeof key === "string" && /^\d+$/.test(key))
          throw new Error("unused payload inspected");
        return Reflect.get(target, key, target);
      },
    }),
  };
  const leaf: Vector = { type: "list", offsets: Int32Array.of(0, 100000, 100000), child };
  const scope = new MemoryScope(new MemoryBudget(8));
  const input: Extract<Vector, { type: "dict" }> = {
    type: "dict",
    indices: Uint32Array.of(1),
    dictionary: leaf,
  };
  const expanded = expandNestedDictionary(input, scope);
  expect(expanded.vector.type).toBe("list");
  expect(scope.budget.usedBytes).toBe(8);
  expanded.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("rejects invalid nested indices before allocating selection storage", () => {
  const scope = new MemoryScope(new MemoryBudget(1024));
  const input: Extract<Vector, { type: "dict" }> = {
    type: "dict",
    indices: Uint32Array.of(2),
    dictionary: { type: "list", offsets: Int32Array.of(0, 0), child: { type: "null", length: 0 } },
  };
  expect(() => expandNestedDictionary(input, scope)).toThrow(/bounds/);
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});

it("uses parent admission for selection metadata without double charging", () => {
  const budget = new MemoryBudget(12);
  const scope = new MemoryScope(budget);
  scope.admit(12, "parent plan");
  const input: Extract<Vector, { type: "dict" }> = {
    type: "dict",
    indices: Uint32Array.of(0),
    dictionary: {
      type: "list",
      offsets: Int32Array.of(0, 1),
      child: { type: "i32", values: Int32Array.of(7) },
    },
  };
  const expanded = expandNestedDictionary(input, scope);
  expect(budget.peakBytes).toBe(12);
  expect(scope.availableAllocationBytes).toBe(0);
  expanded.release();
  expect(budget.usedBytes).toBe(0);
  expect(scope.availableAllocationBytes).toBe(0);
  scope.dispose();
});

it("cleans up if cache reclamation cancels the parent during admission", () => {
  const budget = new MemoryBudget(8);
  const scope = new MemoryScope(budget);
  const cache = budget.reserve(8, "cache");
  const detach = budget.registerReclaimer(() => {
    cache.release();
    scope.dispose();
  });
  const input: Extract<Vector, { type: "dict" }> = {
    type: "dict",
    indices: Uint32Array.of(0),
    dictionary: { type: "list", offsets: Int32Array.of(0, 0), child: { type: "null", length: 0 } },
  };
  try {
    expect(() => expandNestedDictionary(input, scope)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_ABORTED" }),
    );
    expect(scope.disposed).toBe(true);
  } finally {
    detach();
    scope.dispose();
    cache.release();
  }
  expect(budget.usedBytes).toBe(0);
  expect(budget.peakBytes).toBe(8);
});

it("detaches nested selected backing across producer cleanup and consumer handoff", () => {
  const root = new MemoryBudget(48);
  const producer = root.child();
  const consumer = root.child();
  const source = new MemoryScope(producer);
  const dictionary: Vector = {
    type: "f64",
    values: source.allocate(16, "payload", () => Float64Array.of(10, 20)).value,
  };
  const field: Vector = {
    type: "dict",
    indices: source.allocate(8, "field indices", () => Uint32Array.of(0, 1)).value,
    dictionary,
  };
  const batches: Batch[] = [0, 1].map((index) => ({
    rowCount: 1,
    columns: {
      x: {
        type: "dict",
        indices: source.allocate(4, "outer index", () => Uint32Array.of(index)).value,
        dictionary: { type: "struct", fields: { value: field }, length: 2 },
      },
    },
  }));
  const inputs = batches.map((batch) => source.retainAllocations(batch));
  source.dispose();
  try {
    const output = concatOwnedBatches(inputs, consumer);
    try {
      for (const input of inputs) input.release();
      expect(producer.usedBytes).toBe(0);
      expect(consumer.usedBytes).toBe(16);
      expect(root.usedBytes).toBe(16);
      expect(root.peakBytes).toBe(48);
      dictionary.values[0] = 99;
      expect(materializeBatchRows(output.value)).toEqual([
        { x: { value: 10 } },
        { x: { value: 20 } },
      ]);
    } finally {
      output.release();
    }
  } finally {
    for (const input of inputs) input.release();
  }
  expect(root.usedBytes).toBe(0);
});
