import { MemoryBudget, MemoryScope, memoryStore, type ObjectStore } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { asyncBufferFromObjectInfo } from "./store-buffer.js";

function ownedStore(partial: boolean): ObjectStore {
  const base = memoryStore();
  return {
    get: (path) => base.get(path),
    getRange: (path, range) => base.getRange(path, range),
    put: (path, body) => base.put(path, body),
    delete: (path) => base.delete(path),
    list: (prefix) => base.list(prefix),
    head: (path) => base.head(path),
    async getRangeOwned(_path, _range, scope) {
      return scope.allocate(8, "adapter backing", () => {
        const bytes = Uint8Array.of(0, 1, 2, 3, 4, 5, 6, 7);
        return partial ? bytes.subarray(2, 6) : bytes;
      });
    },
  };
}

const object = { path: "fixture", size: 8 };

describe("owned Parquet store-buffer bridge", () => {
  it("reuses a complete backing buffer without a second charge", async () => {
    const scope = new MemoryScope(new MemoryBudget(8));
    const file = asyncBufferFromObjectInfo(ownedStore(false), object);
    const owner = await file.sliceOwned?.(0, 8, scope);
    if (owner === undefined) throw new Error("Missing owned output");
    expect(new Uint8Array(owner.value)).toEqual(Uint8Array.of(0, 1, 2, 3, 4, 5, 6, 7));
    expect(scope.budget.peakBytes).toBe(8);
    const retained = owner?.retain();
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(8);
    retained?.release();
    expect(scope.budget.usedBytes).toBe(0);
  });

  it("charges source and contiguous output together when a view needs copying", async () => {
    const scope = new MemoryScope(new MemoryBudget(12));
    const file = asyncBufferFromObjectInfo(ownedStore(true), object);
    const owner = await file.sliceOwned?.(2, 6, scope);
    if (owner === undefined) throw new Error("Missing owned output");
    expect(new Uint8Array(owner.value)).toEqual(Uint8Array.of(2, 3, 4, 5));
    expect(owner?.bytes).toBe(4);
    expect(scope.budget.peakBytes).toBe(12);
    expect(scope.budget.usedBytes).toBe(4);
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  });

  it("releases input ownership if the contiguous copy cannot fit", async () => {
    const scope = new MemoryScope(new MemoryBudget(11));
    const file = asyncBufferFromObjectInfo(ownedStore(true), object);
    await expect(file.sliceOwned?.(2, 6, scope)).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
    });
    expect(scope.budget.peakBytes).toBe(8);
    expect(scope.budget.usedBytes).toBe(0);
  });

  it("does not claim allocation control for an adapter lacking the capability", () => {
    const { getRangeOwned: _owned, ...unowned } = ownedStore(false);
    expect(asyncBufferFromObjectInfo(unowned, object).sliceOwned).toBeUndefined();
  });
});

it("rejects invalid ranges before forecasts, reads or allocation", async () => {
  let calls = 0;
  const store: ObjectStore = {
    ...ownedStore(false),
    getRange: async () => {
      calls++;
      throw Error("unexpected read");
    },
    getRangeOwned: async () => {
      calls++;
      throw Error("unexpected owned read");
    },
    getRangeOwnedAllocation: () => {
      calls++;
      throw Error("unexpected forecast");
    },
  };
  const file = asyncBufferFromObjectInfo(store, object);
  const scope = new MemoryScope(new MemoryBudget(0));
  for (const [start, end] of [
    [-1, 1],
    [0, 9],
    [3, 2],
    [NaN, 1],
    [0, Infinity],
    [0.5, 1],
    [0, Number.MAX_SAFE_INTEGER + 1],
  ]) {
    await expect(file.slice(start, end)).rejects.toMatchObject({
      code: "LAKEQL_PARQUET_READ_ERROR",
    });
    await expect(file.sliceOwned?.(start, end, scope)).rejects.toMatchObject({
      code: "LAKEQL_PARQUET_READ_ERROR",
    });
    expect(() => file.ownedReadAllocationBytes?.(start, end)).toThrow(/Invalid Parquet byte range/);
  }
  expect(calls).toBe(0);
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});

it.each([
  -1,
  NaN,
  Infinity,
  1.5,
  Number.MAX_SAFE_INTEGER + 1,
])("rejects invalid object size %s", (size) => {
  expect(() => asyncBufferFromObjectInfo(ownedStore(false), { ...object, size })).toThrow(
    /Invalid Parquet object size/,
  );
});
