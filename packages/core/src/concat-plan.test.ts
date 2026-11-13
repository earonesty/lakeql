import { expect, it } from "vitest";
import { type Batch, materializeBatchRows, type Vector } from "./batch.js";
import { concatAllocationBytes, planConcat } from "./concat-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { concatBatches, concatOwnedBatches } from "./vector-sort.js";

const leaves: Vector[] = [
  { type: "null", length: 2 },
  { type: "bool", values: Uint8Array.of(1, 0) },
  { type: "u8", values: Uint8Array.of(1, 255) },
  { type: "i32", values: Int32Array.of(-1, 2) },
  { type: "u32", values: Uint32Array.of(1, 0xffffffff) },
  { type: "f32", values: Float32Array.of(1.5, 2.25) },
  { type: "f64", values: Float64Array.of(1.5, Number.NaN) },
  { type: "i64", values: BigInt64Array.of(-1n, 9007199254740993n) },
  { type: "u64", values: BigUint64Array.of(1n, 18446744073709551615n) },
  { type: "timestamp", values: BigInt64Array.of(1n, 2n), unit: "ns", isAdjustedToUTC: true },
];

it.each(
  leaves.map((vector) => [vector.type, vector] as const),
)("measures exact fixed-width output for %s", (_type, vector) => {
  for (const nullable of [false, true]) {
    const input: Batch = {
      rowCount: 2,
      columns: {
        x: vector.type === "null" || !nullable ? vector : { ...vector, valid: Uint8Array.of(1, 0) },
      },
    };
    const bytes = concatAllocationBytes([input, input]);
    if (bytes === undefined) throw new Error("missing shape capability");
    const scope = new MemoryScope(new MemoryBudget(bytes));
    try {
      const output = concatBatches([input, input], scope);
      expect(scope.budget.usedBytes).toBe(bytes);
      expect(materializeBatchRows(output)).toEqual([
        ...materializeBatchRows(input),
        ...materializeBatchRows(input),
      ]);
    } finally {
      scope.dispose();
    }
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it.each([88, 87])("admits all fixed-width columns atomically at cap %s", (cap) => {
  const batch: Batch = {
    rowCount: 2,
    columns: {
      a: { type: "f32", values: Float32Array.of(1, 2) },
      b: { type: "u8", values: Uint8Array.of(3, 4) },
      c: {
        type: "i64",
        values: BigInt64Array.of(9007199254740993n, 1n),
        valid: Uint8Array.of(1, 0),
      },
      d: { type: "timestamp", values: BigInt64Array.of(1n, 2n), unit: "ns", isAdjustedToUTC: true },
    },
  };
  const budget = new MemoryBudget(cap);
  const scope = new MemoryScope(budget);
  // Borrowed fixture inputs isolate new-output admission; overlap is exercised
  // separately by vector-concat-memory.test.ts with charged input owners.
  const input = scope.retainAllocations(batch);
  try {
    expect(concatAllocationBytes([batch, batch])).toBe(88);
    if (cap === 87) {
      expect(() => concatOwnedBatches([input, input])).toThrow(/budget/);
      expect(budget.peakBytes).toBe(0);
    } else {
      const output = concatOwnedBatches([input, input]);
      expect(budget.usedBytes).toBe(88);
      expect(materializeBatchRows(output.value)).toEqual([
        ...materializeBatchRows(batch),
        ...materializeBatchRows(batch),
      ]);
      output.release();
    }
  } finally {
    input.release();
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it("plans native text and lossless timestamp conversions with exact extents", () => {
  const text: Batch = { rowCount: 1, columns: { x: { type: "utf8", values: ["x"] } } };
  expect(concatAllocationBytes([text])).toBe(9);
  const first: Batch = {
    rowCount: 1,
    columns: {
      x: { type: "timestamp", values: BigInt64Array.of(1n), unit: "ns", isAdjustedToUTC: true },
    },
  };
  const second: Batch = {
    rowCount: 1,
    columns: {
      x: { type: "timestamp", values: BigInt64Array.of(1n), unit: "ms", isAdjustedToUTC: true },
    },
  };
  expect(concatAllocationBytes([first, second])).toBe(16);
  expect(concatAllocationBytes([])).toBe(0);
});

it("rejects unsafe accumulated extents before allocation", () => {
  const batch: Batch = {
    rowCount: Number.MAX_SAFE_INTEGER,
    columns: { x: { type: "null", length: Number.MAX_SAFE_INTEGER } },
  };
  expect(() => concatAllocationBytes([batch, batch])).toThrow(/safe integer/);
});

it.each([
  26, 25,
])("plans dictionary index reuse and replacement in column order at cap %s", (cap) => {
  const first = Uint32Array.of(0);
  const second = Uint32Array.of(1);
  const different = Uint32Array.of(0);
  const numbers: Vector = { type: "i32", values: Int32Array.of(10, 20) };
  const strings: Vector = { type: "utf8", values: ["x", "y"] };
  const make = (indices: Uint32Array, other: Uint32Array, valid: number): Batch => ({
    rowCount: 1,
    columns: {
      a: { type: "dict", indices, dictionary: numbers },
      b: { type: "dict", indices, dictionary: strings, valid: Uint8Array.of(valid) },
      c: { type: "dict", indices: other, dictionary: numbers },
      d: { type: "dict", indices, dictionary: numbers },
    },
  });
  const left = make(first, first, 0);
  const right = make(second, different, 1);
  const budget = new MemoryBudget(cap);
  const scope = new MemoryScope(budget);
  const inputs = [scope.retainAllocations(left), scope.retainAllocations(right)];
  try {
    // A and B share one output; C replaces the cache candidate; D copies again.
    expect(concatAllocationBytes([left, right])).toBe(26);
    if (cap === 25) {
      expect(() => concatOwnedBatches(inputs)).toThrow(/budget/);
      expect(budget.peakBytes).toBe(0);
    } else {
      const output = concatOwnedBatches(inputs);
      try {
        expect(budget.usedBytes).toBe(26);
        expect(materializeBatchRows(output.value)).toEqual([
          { a: 10, b: null, c: 10, d: 10 },
          { a: 20, b: "y", c: 10, d: 20 },
        ]);
        const { a, b, d } = output.value.columns;
        if (a?.type !== "dict" || b?.type !== "dict" || d?.type !== "dict")
          throw new Error("missing dictionaries");
        expect(a.indices).toBe(b.indices);
        expect(a.indices).not.toBe(d.indices);
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

it("plans selected packed expansion for distinct small dictionaries", () => {
  const batches: Batch[] = [1, 2].map((value) => ({
    rowCount: 1,
    columns: {
      x: {
        type: "dict",
        indices: Uint32Array.of(0),
        dictionary: { type: "utf8", values: [String(value)] },
      },
    },
  }));
  expect(concatAllocationBytes(batches)).toBe(18);
});

it.each(
  leaves.filter((leaf) => leaf.type !== "null").map((leaf) => [leaf.type, leaf] as const),
)("atomically plans distinct %s dictionaries with nested nulls", (_type, leaf) => {
  for (const nullable of [false, true]) {
    const batches: Batch[] = [0, 1].map((index) => ({
      rowCount: 3,
      columns: {
        prefix: { type: "u8", values: Uint8Array.of(1, 2, 3) },
        x: {
          type: "dict",
          indices: Uint32Array.of(1, 0, 1),
          dictionary: {
            type: "dict",
            indices: Uint32Array.of(index, 1 - index),
            dictionary: { ...leaf },
            ...(nullable ? { valid: Uint8Array.of(1, 0) } : {}),
          },
        },
      },
    }));
    if (!("values" in leaf) || !ArrayBuffer.isView(leaf.values)) throw new Error("typed leaf");
    const bytes = 6 + 6 * (leaf.values.BYTES_PER_ELEMENT + (nullable ? 1 : 0));
    expect(concatAllocationBytes(batches)).toBe(bytes);
    for (const cap of [bytes - 1, bytes]) {
      const budget = new MemoryBudget(cap);
      const scope = new MemoryScope(budget);
      const inputs = batches.map((batch) => scope.retainAllocations(batch));
      try {
        if (cap < bytes) {
          expect(() => concatOwnedBatches(inputs)).toThrow(/budget/);
          // Even the preceding prefix column must not allocate on rejection.
          expect(budget.peakBytes).toBe(0);
        } else {
          const output = concatOwnedBatches(inputs);
          try {
            expect(budget.usedBytes).toBe(bytes);
            expect(materializeBatchRows(output.value)).toEqual(
              batches.flatMap(materializeBatchRows),
            );
          } finally {
            output.release();
          }
        }
      } finally {
        for (const input of inputs) input.release();
        scope.dispose();
      }
      expect(budget.usedBytes).toBe(0);
    }
  }
});

it("counts an outer validity mask once for distinct scalar dictionaries", () => {
  const batches: Batch[] = [1, 2].map((value) => ({
    rowCount: 1,
    columns: {
      x: {
        type: "dict",
        indices: Uint32Array.of(0),
        valid: Uint8Array.of(1),
        dictionary: { type: "i32", values: Int32Array.of(value), valid: Uint8Array.of(0) },
      },
    },
  }));
  expect(concatAllocationBytes(batches)).toBe(10);
  const scope = new MemoryScope(new MemoryBudget(10));
  const inputs = batches.map((batch) => scope.retainAllocations(batch));
  try {
    const output = concatOwnedBatches(inputs);
    expect(materializeBatchRows(output.value)).toEqual([{ x: null }, { x: null }]);
    expect(scope.budget.usedBytes).toBe(10);
    output.release();
  } finally {
    for (const input of inputs) input.release();
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it("plans non-nullable distinct dictionaries without inspecting selected rows", () => {
  const batches: Batch[] = [1, 2].map((value) => ({
    rowCount: 128,
    columns: {
      x: {
        type: "dict",
        indices: new Proxy(new Uint32Array(128), {
          get(target, key) {
            if (typeof key === "string" && /^\d+$/.test(key))
              throw new Error("Planner inspected selected rows");
            return Reflect.get(target, key, target);
          },
        }),
        dictionary: { type: "i32", values: Int32Array.of(value) },
      },
    },
  }));
  expect(concatAllocationBytes(batches)).toBe(1024);
});

it.each([
  36, 35,
])("admits distinct dictionary output alongside charged inputs and a sibling at %s", (cap) => {
  const root = new MemoryBudget(cap);
  const query = root.child(24);
  const sibling = root.child(12).reserve(12, "other job retained output");
  const scope = new MemoryScope(query);
  const batches: Batch[] = [10, 20].map((value) => ({
    rowCount: 1,
    columns: {
      x: {
        type: "dict",
        indices: scope.allocate(4, "input index", () => Uint32Array.of(0)).value,
        dictionary: {
          type: "i32",
          values: scope.allocate(4, "input payload", () => Int32Array.of(value)).value,
        },
      },
    },
  }));
  const inputs = batches.map((batch) => scope.retainAllocations(batch));
  scope.dispose();
  try {
    if (cap === 35) {
      expect(() => concatOwnedBatches(inputs)).toThrow(/budget/);
      expect(root.peakBytes).toBe(28);
      expect(query.usedBytes).toBe(16);
    } else {
      const output = concatOwnedBatches(inputs);
      try {
        for (const input of inputs) input.release();
        expect(query.usedBytes).toBe(8);
        expect(root.usedBytes).toBe(20);
        expect(root.peakBytes).toBe(36);
        expect(materializeBatchRows(output.value)).toEqual([{ x: 10 }, { x: 20 }]);
      } finally {
        output.release();
      }
    }
  } finally {
    for (const input of inputs) input.release();
    sibling.release();
  }
  expect(root.usedBytes).toBe(0);
});

it.each([64, 63])("charges shared-dictionary inputs alongside mixed output at cap %s", (cap) => {
  const budget = new MemoryBudget(cap);
  const scope = new MemoryScope(budget);
  const dictionary: Vector = {
    type: "f64",
    values: scope.allocate(16, "dictionary", () => Float64Array.of(10, 20)).value,
  };
  const batches: Batch[] = [0, 1].map((index) => ({
    rowCount: 1,
    columns: {
      scalar: {
        type: "f64",
        values: scope.allocate(8, "scalar", () => Float64Array.of(index)).value,
      },
      encoded: {
        type: "dict",
        indices: scope.allocate(4, "indices", () => Uint32Array.of(index)).value,
        dictionary,
      },
    },
  }));
  const inputs = batches.map((batch) => scope.retainAllocations(batch));
  scope.dispose();
  expect(budget.usedBytes).toBe(40);
  try {
    expect(concatAllocationBytes(batches)).toBe(24);
    if (cap === 63) {
      expect(() => concatOwnedBatches(inputs)).toThrow(/budget/);
      expect(budget.peakBytes).toBe(40);
    } else {
      const output = concatOwnedBatches(inputs);
      try {
        for (const input of inputs) input.release();
        expect(budget.usedBytes).toBe(64);
        expect(materializeBatchRows(output.value)).toEqual([
          { scalar: 0, encoded: 10 },
          { scalar: 1, encoded: 20 },
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

it("plans shared dictionary shapes with linear column lookups without inspecting values", () => {
  const width = 16;
  const count = 512;
  let lookups = 0;
  const dictionary: Vector = {
    type: "utf8",
    get values(): string[] {
      throw new Error("planner inspected dictionary values");
    },
  };
  const batches: Batch[] = Array.from({ length: count }, () => {
    const indices = Uint32Array.of(0);
    const vector: Vector = { type: "dict", indices, dictionary };
    const columns = Object.fromEntries(Array.from({ length: width }, (_, i) => [`c${i}`, vector]));
    return {
      rowCount: 1,
      columns: new Proxy(columns, {
        get(target, key, receiver) {
          lookups++;
          return Reflect.get(target, key, receiver);
        },
      }),
    };
  });
  expect(concatAllocationBytes(batches)).toBe(count * 4);
  expect(lookups).toBeLessThanOrEqual(3 * count * width);
});

it("admits complete packed unions and releases construction scratch", () => {
  for (const outerMask of [false, true]) {
    const batches: Batch[] = ["a", "b"].map((value) => ({
      rowCount: 100,
      columns: {
        prefix: { type: "u8", values: new Uint8Array(100) },
        x: {
          type: "dict",
          indices: Uint32Array.from({ length: 100 }, (_, i) => i % 2),
          dictionary: { type: "utf8", values: [value, "unused"], valid: Uint8Array.of(1, 0) },
          ...(outerMask ? { valid: new Uint8Array(100).fill(1) } : {}),
        },
      },
    }));
    const bytes = 1254;
    expect(concatAllocationBytes(batches)).toBe(bytes);
    for (const cap of [bytes - 1, bytes]) {
      const budget = new MemoryBudget(cap);
      const scope = new MemoryScope(budget);
      const inputs = batches.map((batch) => scope.retainAllocations(batch));
      try {
        if (cap < bytes) {
          expect(() => concatOwnedBatches(inputs)).toThrow(/budget/);
          expect(budget.peakBytes).toBe(0);
        } else {
          const result = concatOwnedBatches(inputs);
          expect(result.value.columns.x?.type).toBe("dict");
          expect(materializeBatchRows(result.value)).toEqual(batches.flatMap(materializeBatchRows));
          expect(budget.peakBytes).toBe(bytes);
          expect(budget.usedBytes).toBe(bytes - 16);
          result.release();
        }
      } finally {
        for (const input of inputs) input.release();
        scope.dispose();
      }
      expect(budget.usedBytes).toBe(0);
    }
  }
});

it.each([
  false,
  true,
])("reuses packed plans through nested struct=%s without rescanning selected keys", (nested) => {
  let reads = 0;
  const batches: Batch[] = ["a", "b"].map((value) => {
    const values = [value];
    Object.defineProperty(values, 0, {
      get() {
        reads++;
        return value;
      },
    });
    const dictionary: Vector = {
      type: "dict",
      indices: new Uint32Array(1000),
      dictionary: { type: "utf8", values },
    };
    return {
      rowCount: 1000,
      columns: {
        x: nested ? { type: "struct", length: 1000, fields: { key: dictionary } } : dictionary,
      },
    };
  });
  const plan = planConcat(batches);
  if (plan === undefined) throw new Error("Missing physical plan");
  expect(reads).toBeGreaterThanOrEqual(2000);
  reads = 0;
  const scope = new MemoryScope(new MemoryBudget(plan.bytes));
  scope.admit(plan.bytes, "prepared concat");
  const result = concatBatches(batches, scope, plan);
  // Only the two union dictionary entries are inspected during construction.
  expect(reads).toBeLessThanOrEqual(10);
  expect(result.rowCount).toBe(2000);
  expect(scope.availableAllocationBytes).toBe(0);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("reuses direct packed sizing without an extra range-length pass", () => {
  let reads = 0;
  const vectors: Vector[] = [1, 2].map((byte) => ({
    type: "binary-view",
    data: new Uint8Array(1000).fill(byte),
    length: 1000,
    get fixedLength() {
      reads++;
      return 1;
    },
  }));
  const batches = vectors.map((x) => ({ rowCount: 1000, columns: { x } }));
  const plan = planConcat(batches);
  if (plan === undefined) throw new Error("Missing direct packed plan");
  expect(reads).toBe(2000);
  expect(plan.bytes).toBe(18000);
  reads = 0;
  const scope = new MemoryScope(new MemoryBudget(plan.bytes));
  scope.admit(plan.bytes, "direct packed concat");
  const result = concatBatches(batches, scope, plan);
  expect(reads).toBe(4000); // start and length while copying, with no sizing pass
  expect(result.columns.x?.type).toBe("binary-view");
  expect(scope.availableAllocationBytes).toBe(0);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("plans same-unit timestamp backing without reading individual epochs", () => {
  const values = new Proxy(new BigInt64Array(1024), {
    get(target, key) {
      if (typeof key === "string" && /^\d+$/u.test(key))
        throw new Error("Planning inspected a timestamp row");
      return Reflect.get(target, key, target);
    },
  });
  const batch: Batch = {
    rowCount: values.length,
    columns: { at: { type: "timestamp", values, unit: "nanos", isAdjustedToUTC: true } },
  };
  expect(concatAllocationBytes([batch, batch])).toBe(16384);
});
