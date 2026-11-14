import { expect, it } from "vitest";
import { batchFromColumns, materializeBatchRows, vectorFromValues, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it("admits nullable numeric vector backing at the exact byte boundary", () => {
  const scope = new MemoryScope(new MemoryBudget(18));
  const batch = batchFromColumns({ value: [1, null] }, scope);
  expect(materializeBatchRows(batch)).toEqual([{ value: 1 }, { value: null }]);
  expect(scope.budget.usedBytes).toBe(18);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(17));
  expect(() => batchFromColumns({ value: [1, null] }, tight)).toThrow(/budget/);
  expect(tight.budget.peakBytes).toBe(2);
  tight.dispose();
  expect(tight.budget.usedBytes).toBe(0);
});

it("admits nested offsets, child payload and validity in the same scope", () => {
  const scope = new MemoryScope(new MemoryBudget(37 + 256));
  const vector = vectorFromValues([[1, null], null, []], scope);
  expect([0, 1, 2].map((index) => vectorValue(vector, index))).toEqual([[1, null], null, []]);
  expect(scope.budget.usedBytes).toBe(37 + 256);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(36 + 256));
  expect(() => vectorFromValues([[1, null], null, []], tight)).toThrow(/budget/);
  tight.dispose();
  expect(tight.budget.usedBytes).toBe(0);
});

it("admits bigint, boolean and timestamp typed buffers through generic construction", () => {
  const scope = new MemoryScope(new MemoryBudget(34));
  const batch = batchFromColumns(
    { integer: [1n, 2n], boolean: [true, false], date: [new Date(0), new Date(1)] },
    scope,
  );
  expect(batch.rowCount).toBe(2);
  expect(scope.budget.usedBytes).toBe(34);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("constructs a large list without spreading children into function arguments", () => {
  const length = 200000;
  const children = Array.from({ length }, (_, index) => index);
  const scope = new MemoryScope(new MemoryBudget(256 + 8 + length * 8));
  const vector = vectorFromValues([children], scope);
  if (vector.type !== "list" || vector.child.type !== "f64") throw new Error("unexpected shape");
  expect(vector.offsets).toEqual(Int32Array.of(0, length));
  expect(vector.child.values[0]).toBe(0);
  expect(vector.child.values[length - 1]).toBe(length - 1);
  expect(scope.budget.peakBytes).toBe(256 + 8 + length * 8);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("rejects list offsets outside their physical Int32 capacity before flattening", () => {
  const tooLarge = new Array(0x80000000);
  const scope = new MemoryScope(new MemoryBudget(256 + 8));
  expect(() => vectorFromValues([tooLarge], scope)).toThrow(/Int32 offset capacity/);
  expect(scope.budget.peakBytes).toBe(256 + 8);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("restarts segmented child traversal across empty lists, struct fields and unsigned inference", () => {
  const values = [
    [],
    null,
    [{ id: 1n, tags: ["é"] }],
    [],
    [
      { id: 0xffffffffffffffffn, tags: [] },
      { id: 3n, tags: ["猫", ""] },
    ],
    [],
  ];
  const scope = new MemoryScope(new MemoryBudget(4096));
  const vector = vectorFromValues(values, scope);
  expect(values.map((_, index) => vectorValue(vector, index))).toEqual(values);
  if (vector.type !== "list" || vector.child.type !== "struct")
    throw new Error("unexpected nested shape");
  expect(vector.child.fields.id?.type).toBe("u64");
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("constructs map children through restartable cursors with exact typed admission", () => {
  const values = [
    new Map([["a", 1n]]),
    new Map(),
    null,
    new Map<string, bigint | null>([
      ["b", 0xffffffffffffffffn],
      ["c", null],
    ]),
    new Map(),
  ];
  const scope = new MemoryScope(new MemoryBudget(168 + 320));
  const vector = vectorFromValues(values, scope);
  expect(values.map((_, index) => vectorValue(vector, index))).toEqual([
    { a: 1n },
    {},
    null,
    { b: 0xffffffffffffffffn, c: null },
    {},
  ]);
  if (vector.type !== "map") throw new Error("expected map");
  expect(vector.values.type).toBe("u64");
  expect(scope.budget.usedBytes).toBe(168 + 320);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(167 + 320));
  expect(() => vectorFromValues(values, tight)).toThrow(/budget/);
  tight.dispose();
  expect(tight.budget.usedBytes).toBe(0);
});

it("restarts map traversal for nested struct and list fields", () => {
  const vector = vectorFromValues([
    new Map([["a", { id: 1, tags: ["é"] }]]),
    new Map(),
    new Map([
      ["b", { id: 2, tags: [] }],
      ["c", { id: 3, tags: ["猫", ""] }],
    ]),
  ]);
  expect([0, 1, 2].map((index) => vectorValue(vector, index))).toEqual([
    { a: { id: 1, tags: ["é"] } },
    {},
    { b: { id: 2, tags: [] }, c: { id: 3, tags: ["猫", ""] } },
  ]);
});

it("admits scoped binary vectors as packed ranges and bytes with isolated payload", () => {
  const input = Uint8Array.of(0, 255, 128, 7);
  const values = [input.subarray(1, 3), null, new Uint8Array(), Uint8Array.of(9)];
  const scope = new MemoryScope(new MemoryBudget(39));
  const vector = vectorFromValues(values, scope);
  expect(vector.type).toBe("binary-view");
  expect(values.map((_, index) => vectorValue(vector, index))).toEqual(values);
  expect(scope.budget.usedBytes).toBe(39);
  input.fill(1);
  expect(vectorValue(vector, 0)).toEqual(Uint8Array.of(255, 128));
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(38));
  expect(() => vectorFromValues(values, tight)).toThrow(/budget/);
  expect(tight.budget.peakBytes).toBe(36);
  tight.dispose();
  expect(tight.budget.usedBytes).toBe(0);
});

it("preserves timestamp storage boundaries and promotes values that would wrap", async () => {
  const { timestampFromEpoch, timestampEpochForUnit } = await import("./timestamp.js");
  for (const unit of ["millis", "micros", "nanos"] as const) {
    const min = -(1n << 63n),
      max = (1n << 63n) - 1n;
    const scope = new MemoryScope(new MemoryBudget(16));
    const vector = vectorFromValues(
      [timestampFromEpoch(min, unit), timestampFromEpoch(max, unit)],
      scope,
    );
    for (const [index, expected] of [min, max].entries()) {
      const value = vectorValue(vector, index);
      expect(timestampEpochForUnit(value as ReturnType<typeof timestampFromEpoch>, unit)).toBe(
        expected,
      );
    }
    expect(scope.budget.usedBytes).toBe(16);
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
    for (const value of [min - 1n, max + 1n]) {
      const promoted = new MemoryScope(new MemoryBudget(12));
      const vector = vectorFromValues([timestampFromEpoch(value, unit)], promoted);
      expect(vector.type).toBe("timestamp96");
      expect(vectorValue(vector, 0)).toEqual(timestampFromEpoch(value, unit));
      expect(promoted.budget.usedBytes).toBe(12);
      promoted.dispose();
      expect(promoted.budget.usedBytes).toBe(0);
      const tight = new MemoryScope(new MemoryBudget(11));
      expect(() => vectorFromValues([timestampFromEpoch(value, unit)], tight)).toThrow(/budget/);
      expect(tight.budget.peakBytes).toBe(0);
      tight.dispose();
    }
  }
});

it("selects lossless timestamp precision independently of row order within exact capacity", async () => {
  const { TimestampValue, timestampFromEpoch } = await import("./timestamp.js");
  const values = [
    timestampFromEpoch(2n, "millis"),
    timestampFromEpoch(-1001n, "micros"),
    new TimestampValue(7n, "millis"),
  ];
  for (const input of [values, [...values].reverse()]) {
    const scope = new MemoryScope(new MemoryBudget(24));
    const vector = vectorFromValues(input, scope);
    expect(vector).toMatchObject({ type: "timestamp", unit: "nanos" });
    for (const [index, expected] of input.entries())
      expect(vectorValue(vector, index)).toMatchObject({
        epochNanoseconds: expected.epochNanoseconds,
      });
    expect(scope.budget.usedBytes).toBe(24);
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
    const tight = new MemoryScope(new MemoryBudget(23));
    expect(() => vectorFromValues(input, tight)).toThrow(/budget/);
    expect(tight.budget.peakBytes).toBe(0);
    tight.dispose();
  }
});

it("rejects incompatible timestamp semantics and invalid dates before backing allocation", async () => {
  const { timestampFromEpoch } = await import("./timestamp.js");
  for (const input of [
    [timestampFromEpoch(1n, "millis", true), timestampFromEpoch(2n, "millis", false)],
    [new Date(NaN)],
  ]) {
    const scope = new MemoryScope(new MemoryBudget(16));
    expect(() => vectorFromValues(input, scope)).toThrowError(
      expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }),
    );
    expect(scope.budget.peakBytes).toBe(0);
    scope.dispose();
  }
});
