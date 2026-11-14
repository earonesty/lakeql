import { expect, it } from "vitest";
import { type Vector, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { visitRoutedRows } from "./routed-rows.js";

it("resolves repeated reordered mixed rows and nulls without decoding payloads", () => {
  const text: Vector = { type: "utf8", values: ["a", "b"] };
  const number: Vector = { type: "f64", values: Float64Array.of(7, 9), valid: Uint8Array.of(1, 0) };
  const union: Vector = {
    type: "union",
    tags: Uint8Array.of(0, 1, 0, 1),
    offsets: Uint32Array.of(0, 0, 1, 1),
    children: [text, number],
  };
  const source: Vector = {
    type: "dict",
    indices: Uint32Array.of(2, 0, 2, 3, 1, 0),
    dictionary: union,
    valid: Uint8Array.of(1, 0, 1, 1, 1, 1),
  };
  const scope = new MemoryScope(new MemoryBudget(65536));
  const rows: unknown[] = [];
  visitRoutedRows(source, scope, (leaf, index, row) => {
    expect(row).toBe(rows.length);
    expect(leaf === undefined || leaf === text || leaf === number).toBe(true);
    rows.push(leaf === undefined ? null : vectorValue(leaf, index));
  });
  expect(rows).toEqual(["b", null, "b", null, 7, "a"]);
  expect(scope.budget.usedBytes).toBe(0);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  let calls = 0;
  expect(() =>
    visitRoutedRows(source, tight, () => {
      calls++;
    }),
  ).toThrow(/budget/);
  expect(calls).toBe(0);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it("resolves 12000 routing levels iteratively", () => {
  const leaf: Vector = { type: "f64", values: Float64Array.of(42) };
  let source = leaf;
  for (let i = 0; i < 12000; i++)
    source = { type: "dict", indices: Uint32Array.of(0), dictionary: source };
  const scope = new MemoryScope(new MemoryBudget(2 * 1024 * 1024));
  let calls = 0;
  visitRoutedRows(source, scope, (resolved, index) => {
    calls++;
    expect(resolved).toBe(leaf);
    expect(index).toBe(0);
  });
  expect(calls).toBe(1);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("rejects cyclic and out-of-range routing and releases workspace on visitor errors", () => {
  const cyclic: Extract<Vector, { type: "dict" }> = {
    type: "dict",
    indices: Uint32Array.of(0),
    dictionary: { type: "null", length: 1 },
  };
  cyclic.dictionary = cyclic;
  const malformed: Vector = {
    type: "dict",
    indices: Uint32Array.of(2),
    dictionary: { type: "null", length: 1 },
  };
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(() => visitRoutedRows(cyclic, scope, () => {})).toThrow(/Cyclic/);
  expect(() => visitRoutedRows(malformed, scope, () => {})).toThrow(/Invalid/);
  expect(() =>
    visitRoutedRows({ type: "null", length: 1 }, scope, () => {
      throw Error("visitor");
    }),
  ).toThrow(/visitor/);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("does not access packed payload storage while resolving physical rows", () => {
  const packed: Vector = {
    type: "utf8-view",
    length: 1,
    fixedLength: 4,
    get data(): Uint8Array {
      throw Error("payload accessed");
    },
  };
  const source: Vector = { type: "dict", indices: Uint32Array.of(0, 0), dictionary: packed };
  const scope = new MemoryScope(new MemoryBudget(65536));
  const rows: number[] = [];
  visitRoutedRows(source, scope, (leaf, index, row) => {
    expect(leaf).toBe(packed);
    expect(index).toBe(0);
    rows.push(row);
  });
  expect(rows).toEqual([0, 1]);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
