import { expect, it, vi } from "vitest";
import { stableStringify } from "./manifest.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import {
  encodeOwnedCheckpoint,
  equalStableData,
  internStableDataKey,
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

it("interns equal stable keys without retaining duplicate string storage", () => {
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  try {
    const first = internStableDataKey({ b: ["猫", 2], a: "same" }, scope);
    const retained = budget.usedBytes;
    expect(retained).toBeGreaterThan(0);
    for (let index = 0; index < 100; index++) {
      expect(internStableDataKey({ a: "same", b: ["猫", 2] }, scope)).toBe(first);
      expect(budget.usedBytes).toBe(retained);
    }
    expect(internStableDataKey({ a: "different" }, scope)).not.toBe(first);
    expect(budget.usedBytes).toBeGreaterThan(retained);
  } finally {
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it("admits interned key construction at exact peak and rolls back one byte below", () => {
  const value = { text: "猫\u0000".repeat(100), n: 123 };
  const run = (budget: MemoryBudget) => {
    const scope = new MemoryScope(budget);
    try {
      expect(internStableDataKey(value, scope)).toBe(stableStringify(value));
    } finally {
      scope.dispose();
    }
  };
  const measured = new MemoryBudget(65536);
  run(measured);
  run(new MemoryBudget(measured.peakBytes));
  const small = new MemoryBudget(measured.peakBytes - 1);
  expect(() => run(small)).toThrow(expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }));
  expect(small.usedBytes).toBe(0);
});

it("transfers an interned key from pre-admitted stage capacity without charging twice", () => {
  const input = { key: ["猫", 123] };
  // Stage capacity is consumed cumulatively, unlike a live-peak budget.
  const budget = new MemoryBudget(65536);
  const reserve = vi.spyOn(budget, "reserve");
  const parent = new MemoryScope(budget);
  const stage = parent.childStage(65536, "key stage");
  try {
    expect(internStableDataKey(input, stage)).toBe(stableStringify(input));
    expect(reserve).toHaveBeenCalledTimes(1);
  } finally {
    parent.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});
