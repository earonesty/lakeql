import { expect, it, vi } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Vector } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import {
  SCALAR_READ_WORKSPACE_BYTES as metadataBytes,
  ScalarReadWorkspace,
} from "./scalar-read-workspace.js";

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
  const scope = new MemoryScope(new MemoryBudget(metadataBytes + bytes(1) + bytes(400)));
  const reader = ScalarReadWorkspace.create(scope);
  for (const [left, right, capacity] of [
    [0, 1, bytes(1) + bytes(2)],
    [2, 1, bytes(200) + bytes(2)],
    [0, 3, bytes(1) + bytes(400)],
    [1, 0, bytes(1) + bytes(400)],
  ]) {
    expect(reader.consume(vector, left, right, (a, b) => String(a).length - String(b).length)).toBe(
      ([1, 2, 200, 400][left] ?? 0) - ([1, 2, 200, 400][right] ?? 0),
    );
    expect(scope.budget.usedBytes).toBe(metadataBytes + (capacity as number));
  }
  expect(scope.budget.peakBytes).toBe(scope.budget.maxBytes);
  reader.dispose();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("rejects a one-byte-short second scalar before decoding and releases the first", () => {
  const vector = packed([1, 200]);
  const scope = new MemoryScope(new MemoryBudget(metadataBytes + bytes(1) + bytes(200) - 1));
  const reader = ScalarReadWorkspace.create(scope);
  const decode = vi.spyOn(TextDecoder.prototype, "decode");
  try {
    expect(() => reader.consume(vector, 0, 1, () => 0)).toThrow(/budget/i);
    expect(decode).toHaveBeenCalledTimes(1);
    expect(scope.budget.usedBytes).toBe(metadataBytes);
  } finally {
    decode.mockRestore();
    reader.dispose();
    scope.dispose();
  }
});

it("releases reservations on consumer failure and rejects closed scope reuse", () => {
  const vector = packed([1, 2]);
  const scope = new MemoryScope(new MemoryBudget(4096));
  const reader = ScalarReadWorkspace.create(scope);
  expect(() =>
    reader.consume(vector, 0, 1, () => {
      throw new Error("consumer failure");
    }),
  ).toThrow("consumer failure");
  expect(scope.budget.usedBytes).toBe(metadataBytes);
  reader.consume(vector, 0, 1, () => 0);
  scope.dispose();
  expect(() => reader.consume(vector, 0, 1, () => 0)).toThrow(/disposed/);
  expect(scope.budget.usedBytes).toBe(0);
  reader.dispose();
});

it("keeps active values admitted until a disposing consumer returns", () => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  const reader = ScalarReadWorkspace.create(scope);
  reader.consume(packed([1, 2]), 0, 1, () => {
    reader.dispose();
    expect(scope.budget.usedBytes).toBe(metadataBytes + bytes(1) + bytes(2));
    return 0;
  });
  expect(scope.budget.usedBytes).toBe(0);
  expect(() => reader.consume(packed([1, 2]), 0, 1, () => 0)).toThrow(/disposed/);
  scope.dispose();
});

it("cleans up when reclamation closes the scope during pair growth", () => {
  const pool = new MemoryBudget(metadataBytes + bytes(1) + bytes(2));
  const peer = pool.reserve(64, "reclaimable peer");
  const scope = new MemoryScope(pool);
  const reader = ScalarReadWorkspace.create(scope);
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
  const reader = ScalarReadWorkspace.create(scope);
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
  const pool = new MemoryBudget(metadataBytes + bytes(1) + bytes(200));
  const parent = new MemoryScope(pool);
  const stage = parent.childStage(pool.maxBytes, "comparison stage");
  const reader = ScalarReadWorkspace.create(stage);
  expect(reader.consume(vector, 0, 1, (a, b) => String(a).length - String(b).length)).toBe(-199);
  expect(reader.consume(vector, 1, 0, (a, b) => String(a).length - String(b).length)).toBe(199);
  reader.dispose();
  stage.dispose();
  parent.dispose();
  expect(pool.peakBytes).toBe(pool.maxBytes);
  expect(pool.usedBytes).toBe(0);
});

it("keeps a borrowed pair charged when its parent scope closes inside comparison", () => {
  const pool = new MemoryBudget(metadataBytes + bytes(1) + bytes(2));
  const parent = new MemoryScope(pool);
  const stage = parent.childStage(pool.maxBytes, "comparison stage");
  const reader = ScalarReadWorkspace.create(stage);
  expect(
    reader.consume(packed([1, 2]), 0, 1, (left, right) => {
      parent.dispose();
      expect(pool.usedBytes).toBe(pool.maxBytes);
      expect(left).toBe("a");
      expect(right).toBe("aa");
      expect(() => pool.reserve(1, "sibling cannot reuse live pair storage")).toThrow(/budget/i);
      return -1;
    }),
  ).toBe(-1);
  expect(pool.usedBytes).toBe(0);
  expect(() => reader.consume(packed([1, 2]), 0, 1, () => 0)).toThrow(/disposed/);
  reader.dispose();
});

it("holds scalar admission through a decoder callback that closes its scope", () => {
  const pool = new MemoryBudget(metadataBytes + bytes(1));
  const scope = new MemoryScope(pool);
  const reader = ScalarReadWorkspace.create(scope);
  const original = TextDecoder.prototype.decode;
  let calls = 0;
  const decode = vi.spyOn(TextDecoder.prototype, "decode").mockImplementation(function (...args) {
    calls++;
    scope.dispose();
    expect(pool.usedBytes).toBe(pool.maxBytes);
    expect(() => pool.reserve(1, "sibling during active decode")).toThrow(/budget/i);
    return original.apply(this, args);
  });
  try {
    expect(() => reader.consume(packed([1, 2]), 0, 1, () => 0)).toThrow(/disposed/);
    expect(calls).toBe(1);
    expect(pool.usedBytes).toBe(0);
  } finally {
    decode.mockRestore();
    reader.dispose();
    scope.dispose();
  }
});

it("decodes a shared immutable dictionary operand once while still calling the comparator", () => {
  const vector: Vector = { type: "dict", indices: Uint32Array.of(0, 0), dictionary: packed([200]) };
  const pool = new MemoryBudget(metadataBytes + bytes(200));
  const scope = new MemoryScope(pool);
  const reader = ScalarReadWorkspace.create(scope);
  const decode = vi.spyOn(TextDecoder.prototype, "decode");
  const compare = vi.fn((left, right) => {
    expect(left).toBe(right);
    return 7;
  });
  try {
    expect(reader.consume(vector, 0, 1, compare)).toBe(7);
    expect(compare).toHaveBeenCalledTimes(1);
    expect(decode).toHaveBeenCalledTimes(1);
    expect(pool.peakBytes).toBe(metadataBytes + bytes(200));
  } finally {
    decode.mockRestore();
    reader.dispose();
    scope.dispose();
  }
  expect(pool.usedBytes).toBe(0);
});

it("preserves different outer null states and rejects invalid right indices", () => {
  const vector: Vector = {
    type: "dict",
    indices: Uint32Array.of(0, 0),
    valid: Uint8Array.of(0, 1),
    dictionary: packed([2]),
  };
  const scope = new MemoryScope(new MemoryBudget(4096));
  const reader = ScalarReadWorkspace.create(scope);
  try {
    expect(
      reader.consume(vector, 0, 1, (left, right) => {
        expect(left).toBe(null);
        expect(right).toBe("aa");
        return -1;
      }),
    ).toBe(-1);
    expect(
      reader.consume(vector, 1, 0, (left, right) => {
        expect(left).toBe("aa");
        expect(right).toBe(null);
        return 1;
      }),
    ).toBe(1);
    expect(() => reader.consume(vector, 0, 2, () => 0)).toThrow(/bounds/);
    expect(scope.budget.usedBytes).toBe(metadataBytes);
  } finally {
    reader.dispose();
    scope.dispose();
  }
});

it("admits fixed workspace metadata at the exact boundary before scalar reads", () => {
  const short = new MemoryScope(new MemoryBudget(metadataBytes - 1));
  expect(() => ScalarReadWorkspace.create(short)).toThrow(/budget/i);
  expect(short.budget.usedBytes).toBe(0);
  short.dispose();
  const exact = new MemoryScope(new MemoryBudget(metadataBytes));
  const reader = ScalarReadWorkspace.create(exact);
  expect(exact.budget.usedBytes).toBe(metadataBytes);
  expect(
    reader.consume(
      { type: "i32", values: Int32Array.of(1, 2) },
      0,
      1,
      (left, right) => Number(left) - Number(right),
    ),
  ).toBe(-1);
  expect(exact.budget.peakBytes).toBe(metadataBytes);
  reader.dispose();
  reader.dispose();
  exact.dispose();
  expect(exact.budget.usedBytes).toBe(0);
});

it("releases metadata admission if reclamation closes the scope during creation", () => {
  const pool = new MemoryBudget(metadataBytes);
  const scope = new MemoryScope(pool);
  const peer = pool.reserve(metadataBytes, "reclaimable peer");
  const detach = pool.registerReclaimer(() => {
    peer.release();
    scope.dispose();
  });
  try {
    expect(() => ScalarReadWorkspace.create(scope)).toThrow(/disposed/);
    expect(pool.usedBytes).toBe(0);
  } finally {
    detach();
    peer.release();
    scope.dispose();
  }
});
