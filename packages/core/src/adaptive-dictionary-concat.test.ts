import { expect, it, vi } from "vitest";
import { materializeBatchRows } from "./batch.js";
import { concatAllocationBytes } from "./concat-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { concatAdaptivePackedDictionaries, concatPackedVectors } from "./packed-gather.js";

it("chooses a compact union including nulls and charges construction scratch", () => {
  const vectors = ["a", "b"].map((value) => ({
    type: "dict" as const,
    indices: Uint32Array.from({ length: 100 }, (_, i) => i % 2),
    dictionary: { type: "utf8" as const, values: [value, "unused"], valid: Uint8Array.of(1, 0) },
  }));
  const scope = new MemoryScope(new MemoryBudget(1054));
  const reserve = vi.spyOn(scope.budget, "reserve");
  const output = concatAdaptivePackedDictionaries(vectors, undefined, scope);
  expect(reserve).toHaveBeenCalledTimes(1);
  expect(reserve).toHaveBeenCalledWith(1054, "dictionary union working set");
  if (output?.type !== "dict") throw new Error("Expected union");
  expect(output.dictionary.type).toBe("utf8-view");
  expect(scope.budget.usedBytes).toBe(1038);
  expect(scope.budget.peakBytes).toBe(1054);
  expect(materializeBatchRows({ rowCount: 200, columns: { value: output } })).toEqual(
    Array.from({ length: 200 }, (_, i) => ({ value: i % 2 ? null : i < 100 ? "a" : "b" })),
  );
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("declines unused large dictionaries when packing selected values is smaller", () => {
  const vectors = ["a", "b"].map((value) => ({
    type: "dict" as const,
    indices: Uint32Array.of(0),
    dictionary: {
      type: "utf8" as const,
      values: [value, "large".repeat(1000)],
    },
  }));
  const scope = new MemoryScope(new MemoryBudget(18));
  const output = concatAdaptivePackedDictionaries(vectors, undefined, scope);
  expect(output?.type).toBe("utf8-view");
  expect(scope.budget.usedBytes).toBe(18);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("declines large dictionary cardinality without inspecting unused values", () => {
  const values = new Array<string>(10000);
  values[0] = "selected";
  Object.defineProperty(values, 1, {
    get() {
      throw new Error("Unused dictionary entry was inspected");
    },
  });
  const vectors = [0, 1].map(() => ({
    type: "dict" as const,
    indices: Uint32Array.of(0),
    dictionary: { type: "utf8" as const, values },
  }));
  const scope = new MemoryScope(new MemoryBudget(32));
  const output = concatAdaptivePackedDictionaries(vectors, undefined, scope);
  if (output === undefined) throw new Error("Missing selected output");
  expect(output.type).toBe("utf8-view");
  expect(materializeBatchRows({ rowCount: 2, columns: { value: output } })).toEqual([
    { value: "selected" },
    { value: "selected" },
  ]);
  expect(scope.budget.usedBytes).toBe(32);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("releases atomic union admission when its parent scope closes during reservation", () => {
  const budget = new MemoryBudget(10000);
  const scope = new MemoryScope(budget);
  const vectors = ["a", "b"].map((value) => ({
    type: "dict" as const,
    indices: new Uint32Array(100),
    dictionary: { type: "utf8" as const, values: [value] },
  }));
  const reserve = budget.reserve.bind(budget);
  const spy = vi.spyOn(budget, "reserve").mockImplementation((bytes, purpose) => {
    const admission = reserve(bytes, purpose);
    scope.dispose();
    return admission;
  });
  try {
    expect(() => concatAdaptivePackedDictionaries(vectors, undefined, scope)).toThrow();
    expect(budget.usedBytes).toBe(0);
    expect(scope.disposed).toBe(true);
  } finally {
    spy.mockRestore();
    scope.dispose();
  }
});

it("constructs a union from parent admission even when global headroom is zero", () => {
  const scope = new MemoryScope(new MemoryBudget(826));
  scope.admit(826, "parent output plan");
  const reserve = vi.spyOn(scope.budget, "reserve");
  const vectors = ["a", "b"].map((value) => ({
    type: "dict" as const,
    indices: new Uint32Array(100),
    dictionary: { type: "utf8" as const, values: [value] },
  }));
  const output = concatAdaptivePackedDictionaries(vectors, undefined, scope);
  expect(output?.type).toBe("dict");
  expect(reserve).not.toHaveBeenCalled();
  expect(scope.budget.usedBytes).toBe(818);
  expect(scope.budget.peakBytes).toBe(826);
  expect(scope.availableAllocationBytes).toBe(0);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("keeps a compact union when repeated expansion exceeds Uint32 payload offsets", () => {
  const size = 1024 * 1024;
  const vectors = [3, 7].map((byte) => ({
    type: "dict" as const,
    indices: new Uint32Array(4096),
    dictionary: {
      type: "binary-view" as const,
      data: new Uint8Array(size).fill(byte),
      fixedLength: size,
      length: 1,
    },
  }));
  const rejected = new MemoryScope(new MemoryBudget(64));
  expect(() => concatPackedVectors(vectors, undefined, rejected)).toThrow(/Uint32/);
  expect(rejected.budget.peakBytes).toBe(0);
  rejected.dispose();
  const extent = size * 2 + 8192 * 4 + 2 * 8 + 2 * 4;
  expect(concatAllocationBytes(vectors.map((x) => ({ rowCount: 4096, columns: { x } })))).toBe(
    extent,
  );
  const scope = new MemoryScope(new MemoryBudget(extent));
  scope.admit(extent, "compact repeated payload");
  const output = concatAdaptivePackedDictionaries(vectors, undefined, scope);
  if (output?.type !== "dict" || output.dictionary.type !== "binary-view")
    throw new Error("Expected compact binary union");
  expect(output.dictionary.data.byteLength).toBe(size * 2);
  expect(output.dictionary.data[0]).toBe(3);
  expect(output.dictionary.data[size]).toBe(7);
  expect(output.indices[4095]).toBe(0);
  expect(output.indices[4096]).toBe(1);
  expect(scope.budget.peakBytes).toBe(extent);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
