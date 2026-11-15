import {
  MATERIALIZATION_CHARGES as c,
  MemoryBudget,
  MemoryScope,
  SharedMemoryCache,
} from "lakeql-core";
import { expect, it, vi } from "vitest";
import {
  DecodedColumnCache,
  decodedCacheNumberCharacters,
  decodedColumnCacheKey,
  decodedColumnPageCacheKey,
  decodedDictionaryPageCacheKey,
  planDecodedColumnCacheKey,
  planDecodedColumnPageCacheKey,
  planDecodedDictionaryPageCacheKey,
  prefixDecodedCacheKey,
} from "./decoded-column-cache.js";

const parts = {
  path: "file\u0000\ud800",
  byteLength: 10,
  etag: '"\\tag',
  columns: ["a\n", "猫"],
  rowStart: 0,
  rowEnd: 2,
  column: "a\n",
  rowGroupStart: 0,
  pageRowStart: 0,
  pageRowEnd: 2,
  pageOffset: 4,
  compressedPageSize: 10,
  values: 2,
};

it("defers all decoded identity serialization and preserves existing formats", () => {
  for (const [planKey, rawKey] of [
    [planDecodedColumnCacheKey, decodedColumnCacheKey],
    [planDecodedColumnPageCacheKey, decodedColumnPageCacheKey],
    [planDecodedDictionaryPageCacheKey, decodedDictionaryPageCacheKey],
  ] as const) {
    const expected = rawKey(parts);
    const stringify = vi.spyOn(JSON, "stringify");
    try {
      const plan = planKey(parts);
      expect(stringify).not.toHaveBeenCalled();
      expect(plan.build()).toBe(expected);
      expect(expected.length).toBeLessThanOrEqual(plan.characters);
    } finally {
      stringify.mockRestore();
    }
  }
});

it("admits serialized identities and qualified-key workspace at the exact query boundary", () => {
  const plan = planDecodedColumnCacheKey(parts);
  const bytes =
    plan.workspaceBytes +
    c.string +
    ("decoded-column:".length + plan.characters) * c.stringCodeUnit;
  for (const shortage of [0, 1]) {
    const pool = new MemoryBudget(65536);
    const scope = new MemoryScope(pool.child(bytes - shortage));
    const cache = new DecodedColumnCache(
      new SharedMemoryCache({ memoryBudget: scope.budget, maxBytes: 65536 }),
      {},
    );
    const build = vi.spyOn(plan, "build");
    try {
      expect(cache.get(plan, scope)).toBeUndefined();
      expect(build).toHaveBeenCalledTimes(shortage ? 0 : 1);
      expect(scope.budget.peakBytes).toBe(shortage ? 0 : bytes);
      expect(scope.budget.usedBytes).toBe(0);
    } finally {
      build.mockRestore();
      scope.dispose();
    }
    expect(pool.usedBytes).toBe(0);
  }
});

it("does not consume a nonrefundable decode extent for optional key construction", () => {
  for (const spare of [0, 8192]) {
    const pool = new MemoryBudget(32768 + spare);
    const root = new MemoryScope(pool);
    const stage = root.childStage(32768, "mandatory decode");
    const plan = planDecodedColumnCacheKey(parts);
    const build = vi.spyOn(plan, "build");
    const cache = new DecodedColumnCache(
      new SharedMemoryCache({ memoryBudget: pool, maxBytes: 65536 }),
      {},
    );
    try {
      cache.get(plan, stage);
      expect(build).toHaveBeenCalledTimes(spare ? 1 : 0);
      expect(stage.availableAllocationBytes).toBe(32768);
      stage.allocate(32768, "mandatory output", () => undefined);
    } finally {
      build.mockRestore();
      root.dispose();
    }
    expect(pool.usedBytes).toBe(0);
  }
});

it("keeps measured ASCII identities usable in a one-KiB cache", () => {
  const pool = new MemoryBudget(4096);
  const shared = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 1024 });
  const cache = new DecodedColumnCache(shared, { maxBytes: 1024 });
  const input = { path: "a".repeat(40), byteLength: 10, columns: ["id"], rowStart: 0, rowEnd: 0 };
  const plan = planDecodedColumnCacheKey(input);
  const batch = { rowCount: 0, columns: {} };
  expect(plan.characters).toBe(decodedColumnCacheKey(input).length);
  try {
    cache.set(plan, batch);
    expect(cache.get(plan)).toBe(batch);
    expect(pool.usedBytes).toBeGreaterThan(0);
  } finally {
    shared.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
});

it("composes row-group identity plans without constructing their parent strings", () => {
  const parent = prefixDecodedCacheKey("owned-row-group:12:", parts.path);
  const build = vi.spyOn(parent, "build");
  try {
    const plan = planDecodedColumnCacheKey({ ...parts, path: parent });
    expect(build).not.toHaveBeenCalled();
    expect(plan.build()).toBe(
      decodedColumnCacheKey({ ...parts, path: `owned-row-group:12:${parts.path}` }),
    );
    expect(build).toHaveBeenCalledTimes(1);
    expect(plan.build().length).toBeLessThanOrEqual(plan.characters);
    expect(plan.workspaceBytes).toBeGreaterThan(parent.workspaceBytes);
  } finally {
    build.mockRestore();
  }
});

it("bounds decimal workspace at numeric representation boundaries", () => {
  for (const value of [
    undefined,
    NaN,
    Infinity,
    -Infinity,
    -0,
    0,
    9,
    10,
    99,
    100,
    Number.MAX_SAFE_INTEGER,
    Number.MIN_SAFE_INTEGER,
    Number.MAX_VALUE,
    Number.MIN_VALUE,
    1e-7,
    -1e21,
  ]) {
    const serialized = JSON.stringify(value ?? null) ?? "null";
    expect(decodedCacheNumberCharacters(value)).toBeGreaterThanOrEqual(serialized.length);
  }
});
