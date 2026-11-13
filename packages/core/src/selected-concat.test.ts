import { expect, it } from "vitest";
import { type Batch, materializeBatchRows, type Vector } from "./batch.js";
import { planConcat } from "./concat-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { concatOwnedBatches } from "./vector-sort.js";

it.each([
  "struct",
  "list",
  "map",
  "nested",
] as const)("atomically constructs selected %s output without expansion scratch", (shape) => {
  const batches: Batch[] = [1, 2].map((part) => {
    const values: Vector = {
      type: "i64",
      values: BigInt64Array.of(BigInt(part), 9007199254740993n, 3n),
      valid: Uint8Array.of(1, 0, 1),
    };
    const list: Vector = { type: "list", offsets: Int32Array.of(0, 1, 3), child: values };
    const leaf: Vector =
      shape === "struct"
        ? { type: "struct", length: 2, fields: { x: values } }
        : shape === "list"
          ? list
          : shape === "map"
            ? {
                type: "map",
                offsets: Int32Array.of(0, 1, 3),
                keys: { type: "i32", values: Int32Array.of(1, 2, 3) },
                values,
              }
            : { type: "struct", length: 2, fields: { x: list } };
    return {
      rowCount: 4,
      columns: {
        prefix: { type: "u8", values: new Uint8Array(4) },
        x: {
          type: "dict",
          indices: Uint32Array.of(1, 0, 99, 1),
          valid: Uint8Array.of(1, 1, 0, 1),
          dictionary: leaf,
        },
      },
    };
  });
  batches.push({
    rowCount: 1,
    columns: {
      prefix: { type: "u8", values: Uint8Array.of(0) },
      x: { type: "dict", indices: Uint32Array.of(0), dictionary: { type: "null", length: 1 } },
    },
  });
  const expected = batches.flatMap(materializeBatchRows);
  const plan = planConcat(batches);
  if (plan === undefined || plan.columns.get("x")?.selected === undefined)
    throw new Error("Missing recursive selection plan");
  for (const cap of [plan.bytes - 1, plan.bytes]) {
    const root = new MemoryBudget(cap);
    const scope = new MemoryScope(root);
    const inputs = batches.map((batch) => scope.retainAllocations(batch));
    try {
      if (cap < plan.bytes) {
        expect(() => concatOwnedBatches(inputs)).toThrow(/budget/);
        expect(root.peakBytes).toBe(0);
      } else {
        const output = concatOwnedBatches(inputs);
        expect(materializeBatchRows(output.value)).toEqual(expected);
        expect(root.peakBytes).toBe(plan.bytes);
        expect(root.usedBytes).toBe(plan.bytes);
        output.release();
      }
    } finally {
      for (const input of inputs) input.release();
      scope.dispose();
    }
    expect(root.usedBytes).toBe(0);
  }
});

it.each([
  "first",
  "last",
] as const)("admits direct null normalization in %s position without scratch", (position) => {
  const leaf: Batch = {
    rowCount: 2,
    columns: {
      scalar: { type: "f64", values: Float64Array.of(1, 2) },
      text: { type: "utf8", values: ["猫", ""] },
      nested: {
        type: "list",
        offsets: Int32Array.of(0, 1, 2),
        child: { type: "i32", values: Int32Array.of(3, 4) },
      },
    },
  };
  const nulls: Batch = {
    rowCount: 3,
    columns: Object.fromEntries(
      Object.keys(leaf.columns).map((name) => [name, { type: "null", length: 3 }]),
    ),
  };
  const batches = position === "first" ? [nulls, leaf] : [leaf, nulls];
  const plan = planConcat(batches);
  if (plan === undefined) throw new Error("Missing null normalization plan");
  expect(plan.columns.size).toBe(3);
  for (const cap of [plan.bytes - 1, plan.bytes]) {
    const budget = new MemoryBudget(cap);
    const scope = new MemoryScope(budget);
    const inputs = batches.map((batch) => scope.retainAllocations(batch));
    try {
      if (cap < plan.bytes) {
        expect(() => concatOwnedBatches(inputs)).toThrow(/budget/);
        expect(budget.peakBytes).toBe(0);
      } else {
        const output = concatOwnedBatches(inputs);
        expect(materializeBatchRows(output.value)).toEqual(batches.flatMap(materializeBatchRows));
        expect(budget.usedBytes).toBe(plan.bytes);
        output.release();
      }
    } finally {
      for (const input of inputs) input.release();
      scope.dispose();
    }
    expect(budget.usedBytes).toBe(0);
  }
});

it("plans mixed timestamp units with exact admission and no row objects", () => {
  const batches: Batch[] = [
    {
      rowCount: 2,
      columns: {
        at: {
          type: "timestamp",
          unit: "millis",
          isAdjustedToUTC: true,
          values: BigInt64Array.of(-2n, 0n),
          valid: Uint8Array.of(1, 0),
        },
      },
    },
    {
      rowCount: 1,
      columns: {
        at: {
          type: "timestamp",
          unit: "nanos",
          isAdjustedToUTC: true,
          values: BigInt64Array.of(7n),
        },
      },
    },
  ];
  expect(planConcat(batches)?.bytes).toBe(27);
  for (const cap of [26, 27]) {
    const scope = new MemoryScope(new MemoryBudget(cap));
    const inputs = batches.map((batch) => scope.retainAllocations(batch));
    try {
      if (cap === 26) {
        expect(() => concatOwnedBatches(inputs)).toThrow(/budget/);
        expect(scope.budget.peakBytes).toBe(0);
      } else {
        const output = concatOwnedBatches(inputs);
        expect(output.value.columns.at).toMatchObject({
          type: "timestamp",
          unit: "nanos",
          values: BigInt64Array.of(-2_000_000n, 0n, 7n),
          valid: Uint8Array.of(1, 0, 1),
        });
        expect(scope.budget.usedBytes).toBe(27);
        output.release();
      }
    } finally {
      for (const input of inputs) input.release();
      scope.dispose();
    }
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it("plans wider timestamp promotion and rejects incompatible semantics", () => {
  const first: Batch = {
    rowCount: 1,
    columns: {
      at: {
        type: "timestamp",
        unit: "millis",
        isAdjustedToUTC: true,
        values: BigInt64Array.of((1n << 63n) - 1n),
      },
    },
  };
  for (const isAdjustedToUTC of [true, false]) {
    const second: Batch = {
      rowCount: 1,
      columns: {
        at: { type: "timestamp", unit: "nanos", isAdjustedToUTC, values: BigInt64Array.of(1n) },
      },
    };
    if (isAdjustedToUTC) expect(planConcat([first, second])?.bytes).toBe(24);
    else
      expect(() => planConcat([first, second])).toThrowError(
        expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }),
      );
  }
});
