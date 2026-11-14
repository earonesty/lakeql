import { expect, it } from "vitest";
import { stableStringify } from "./manifest.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import {
  encodeOwnedCheckpoint,
  equalStableData,
  stringifyOwnedStableData,
} from "./owned-checkpoint-encoding.js";

it("preserves stable checkpoint bytes across keys, numeric edges and escaped text", () => {
  const value = {
    version: 1,
    rows: [{ z: "雪\ud800\n", "10": 3, "2": 4, a: Infinity, n: NaN, missing: undefined }],
    orderBy: [{ column: "z", direction: "asc" }],
  };
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  const output = encodeOwnedCheckpoint(value, scope);
  expect(output.value).toEqual(new TextEncoder().encode(stableStringify(value)));
  scope.dispose();
  expect(budget.usedBytes).toBe(output.bytes);
  output.release();
  expect(budget.usedBytes).toBe(0);
});

it("admits normalization/text/output overlap at exact capacity and cleans up below it", () => {
  const value = { runs: [{ rows: [{ text: "x".repeat(1000), id: 1 }] }], version: 1 };
  const measured = new MemoryBudget(65536);
  const scope = new MemoryScope(measured);
  encodeOwnedCheckpoint(value, scope).release();
  const required = measured.peakBytes;
  scope.dispose();
  for (const cap of [required - 1, required]) {
    const budget = new MemoryBudget(cap);
    const target = new MemoryScope(budget);
    if (cap < required) expect(() => encodeOwnedCheckpoint(value, target)).toThrow(/budget/i);
    else encodeOwnedCheckpoint(value, target).release();
    target.dispose();
    expect(budget.usedBytes).toBe(0);
  }
});

it("rejects cycles and unsupported objects without retaining traversal charges", () => {
  const value: Record<string, unknown> = {};
  value.self = value;
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  expect(() => encodeOwnedCheckpoint(value, scope)).toThrow(/Cyclic/);
  expect(() => encodeOwnedCheckpoint(new Date(), scope)).toThrow(/JSON-shaped/);
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
});

it("preserves stable data-key semantics for nested special values", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const value = {
    nested: [
      9007199254740993n,
      new Date("2020-01-01T00:00:00Z"),
      new Uint8Array([0, 255]),
      Infinity,
      undefined,
    ],
    text: '雪\\"',
  };
  const key = stringifyOwnedStableData(value, scope);
  expect(key.value).toBe(stableStringify(value));
  key.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("compares stable data with admitted overlap and releases mismatch/error scratch", () => {
  const left = { z: "雪".repeat(100), a: [3n, Infinity, undefined] };
  const right = { a: ["3", "Infinity", "undefined"], z: left.z };
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  expect(equalStableData(left, right, scope)).toBe(true);
  const required = pool.peakBytes;
  expect(pool.usedBytes).toBe(0);
  expect(equalStableData(left, { ...right, z: "different" }, scope)).toBe(false);
  expect(pool.usedBytes).toBe(0);
  const cyclic: Record<string, unknown> = {};
  cyclic.self = cyclic;
  expect(() => equalStableData(left, cyclic, scope)).toThrow(/Cyclic/);
  expect(pool.usedBytes).toBe(0);
  scope.dispose();
  for (const cap of [required - 1, required]) {
    const target = new MemoryScope(new MemoryBudget(cap));
    if (cap < required) expect(() => equalStableData(left, right, target)).toThrow(/budget/i);
    else expect(equalStableData(left, right, target)).toBe(true);
    expect(target.budget.usedBytes).toBe(0);
    target.dispose();
  }
});
