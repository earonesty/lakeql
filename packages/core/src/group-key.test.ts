import { expect, it } from "vitest";
import type { Scalar } from "./expr.js";
import { groupKey, groupKeyAllocationBytes } from "./group-key.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { TimestampValue } from "./timestamp.js";

it.each([
  { values: [], expected: "" },
  { values: [null], expected: "null:" },
  { values: [""], expected: "string:" },
  { values: ["é😀"], expected: "string:é😀" },
  { values: [-0], expected: "number:0" },
  { values: [NaN], expected: "number:NaN" },
  { values: [Infinity], expected: "number:Infinity" },
  { values: [true], expected: "boolean:true" },
  { values: [-123n], expected: "bigint:-123" },
  { values: [new TimestampValue(-123n, "nanos", true)], expected: "timestamp:-123" },
  { values: ["a", "b"], expected: "8:string:a8:string:b" },
  { values: [null, 1], expected: "5:null:8:number:1" },
])("admits the existing encoding for $expected", ({ values, expected }) => {
  const bytes = groupKeyAllocationBytes(values);
  const scope = new MemoryScope(new MemoryBudget(bytes));
  expect(groupKey(values, scope)).toBe(expected);
  expect(groupKey(values)).toBe(expected);
  expect(scope.budget.usedBytes).toBe(bytes);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  if (bytes > 0) {
    const rejected = new MemoryScope(new MemoryBudget(bytes + 6));
    rejected.allocate(7, "caller", () => new Uint8Array(7));
    expect(() => groupKey(values, rejected)).toThrow(/budget/i);
    expect(rejected.budget.usedBytes).toBe(7);
    rejected.dispose();
  }
});

it("distinguishes types and composite delimiter boundaries", () => {
  const tuples: Scalar[][] = [
    [1],
    [1n],
    ["1"],
    ["a", "12:string:b"],
    ["a12:string:", "b"],
    ["a:b", "c"],
    ["a", "b:c"],
  ];
  const keys = new Set(tuples.map((values) => groupKey(values)));
  expect(keys.size).toBe(tuples.length);
  expect(groupKey([new TimestampValue(1n, "nanos", true)])).toBe(
    groupKey([new TimestampValue(1n, "micros", false)]),
  );
});

it("admits wide integer keys and preserves sparse/nullish tuple semantics", () => {
  const values: Scalar[] = [
    -(10n ** 1000n),
    new TimestampValue(10n ** 1000n, "nanos", false),
    "é😀:",
  ];
  const scope = new MemoryScope(new MemoryBudget(groupKeyAllocationBytes(values)));
  expect(groupKey(values, scope)).toBe(groupKey(values));
  scope.dispose();
  const sparse = new Array<Scalar>(2);
  const empty = new MemoryScope(new MemoryBudget(groupKeyAllocationBytes(sparse)));
  expect(groupKey(sparse, empty)).toBe("5:null:5:null:");
  empty.dispose();
});

it("rejects binary and interval keys before reserving output", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  for (const value of [
    new Uint8Array(2),
    { kind: "interval" as const, months: 1, days: 2, nanoseconds: "3" },
  ]) {
    expect(() => groupKey([value], scope)).toThrow(/group key/);
    expect(scope.budget.usedBytes).toBe(0);
  }
  scope.dispose();
});
