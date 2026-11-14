import { expect, it, vi } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Vector } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { ScalarReadWorkspace } from "./scalar-read-workspace.js";

function packed(lengths: number[]): Vector {
  const data = new Uint8Array(lengths.reduce((sum, length) => sum + length, 0)).fill(97);
  const ranges = new Uint32Array(lengths.length * 2);
  let offset = 0;
  lengths.forEach((length, index) => {
    ranges[index * 2] = offset;
    ranges[index * 2 + 1] = length;
    offset += length;
  });
  return { type: "utf8-view", data, ranges, length: lengths.length };
}

const bytes = (length: number) => c.view + c.string + length * c.stringCodeUnit;

it("reuses admission and grows only by the live pair's additional extent", () => {
  const vector = packed([1, 2, 200, 400]);
  const scope = new MemoryScope(new MemoryBudget(bytes(1) + bytes(400)));
  const reader = new ScalarReadWorkspace(scope);
  for (const [left, right, capacity] of [
    [0, 1, bytes(1) + bytes(2)],
    [2, 1, bytes(200) + bytes(2)],
    [0, 3, bytes(1) + bytes(400)],
    [1, 0, bytes(1) + bytes(400)],
  ]) {
    expect(reader.consume(vector, left, right, (a, b) => String(a).length - String(b).length)).toBe(
      ([1, 2, 200, 400][left] ?? 0) - ([1, 2, 200, 400][right] ?? 0),
    );
    expect(scope.budget.usedBytes).toBe(capacity);
  }
  expect(scope.budget.peakBytes).toBe(scope.budget.maxBytes);
  reader.dispose();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("rejects a one-byte-short second scalar before decoding and releases the first", () => {
  const vector = packed([1, 200]);
  const scope = new MemoryScope(new MemoryBudget(bytes(1) + bytes(200) - 1));
  const reader = new ScalarReadWorkspace(scope);
  const decode = vi.spyOn(TextDecoder.prototype, "decode");
  try {
    expect(() => reader.consume(vector, 0, 1, () => 0)).toThrow(/budget/i);
    expect(decode).toHaveBeenCalledTimes(1);
    expect(scope.budget.usedBytes).toBe(0);
  } finally {
    decode.mockRestore();
    reader.dispose();
    scope.dispose();
  }
});

it("releases reservations on consumer failure and rejects closed scope reuse", () => {
  const vector = packed([1, 2]);
  const scope = new MemoryScope(new MemoryBudget(4096));
  const reader = new ScalarReadWorkspace(scope);
  expect(() =>
    reader.consume(vector, 0, 1, () => {
      throw new Error("consumer failure");
    }),
  ).toThrow("consumer failure");
  expect(scope.budget.usedBytes).toBe(0);
  reader.consume(vector, 0, 1, () => 0);
  scope.dispose();
  expect(() => reader.consume(vector, 0, 1, () => 0)).toThrow(/disposed/);
  expect(scope.budget.usedBytes).toBe(0);
  reader.dispose();
});

it("keeps active values admitted until a disposing consumer returns", () => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  const reader = new ScalarReadWorkspace(scope);
  reader.consume(packed([1, 2]), 0, 1, () => {
    reader.dispose();
    expect(scope.budget.usedBytes).toBe(bytes(1) + bytes(2));
    return 0;
  });
  expect(scope.budget.usedBytes).toBe(0);
  expect(() => reader.consume(packed([1, 2]), 0, 1, () => 0)).toThrow(/disposed/);
  scope.dispose();
});

it("cleans up when reclamation closes the scope during pair growth", () => {
  const pool = new MemoryBudget(bytes(1) + bytes(2));
  const peer = pool.reserve(64, "reclaimable peer");
  const scope = new MemoryScope(pool);
  const reader = new ScalarReadWorkspace(scope);
  const detach = pool.registerReclaimer(() => {
    peer.release();
    scope.dispose();
  });
  expect(() => reader.consume(packed([1, 2]), 0, 1, () => 0)).toThrow(/disposed/);
  expect(pool.usedBytes).toBe(0);
  detach();
  reader.dispose();
});

it("does not create per-comparison scopes or repeatedly reserve stable scalar extents", () => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  const reader = new ScalarReadWorkspace(scope);
  const vector: Vector = {
    type: "dict",
    indices: Uint32Array.of(0, 1),
    dictionary: packed([1, 2]),
  };
  const reserve = vi.spyOn(scope, "reserve");
  const child = vi.spyOn(scope, "childScope");
  for (let i = 0; i < 10000; i++) expect(reader.consume(vector, 0, 1, () => -1)).toBe(-1);
  expect(reserve).toHaveBeenCalledTimes(2);
  expect(child).not.toHaveBeenCalled();
  reader.dispose();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("handles union leaves and reversed extents within one admitted stage", () => {
  const vector: Vector = {
    type: "union",
    tags: Uint8Array.of(0, 1),
    offsets: Uint32Array.of(0, 0),
    children: [packed([1]), packed([200])],
  };
  const pool = new MemoryBudget(bytes(1) + bytes(200));
  const parent = new MemoryScope(pool);
  const stage = parent.childStage(pool.maxBytes, "comparison stage");
  const reader = new ScalarReadWorkspace(stage);
  expect(reader.consume(vector, 0, 1, (a, b) => String(a).length - String(b).length)).toBe(-199);
  expect(reader.consume(vector, 1, 0, (a, b) => String(a).length - String(b).length)).toBe(199);
  reader.dispose();
  stage.dispose();
  parent.dispose();
  expect(pool.peakBytes).toBe(pool.maxBytes);
  expect(pool.usedBytes).toBe(0);
});
