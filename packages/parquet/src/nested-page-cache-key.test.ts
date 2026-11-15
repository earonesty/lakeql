import {
  MATERIALIZATION_CHARGES as c,
  MemoryBudget,
  MemoryScope,
  SharedMemoryCache,
} from "lakeql-core";
import { expect, it, vi } from "vitest";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { type NestedPageKeyParts, planNestedPageCacheKey } from "./nested-page-cache-key.js";

const parts: NestedPageKeyParts = {
  shape: "compact",
  interpretation: "julian-nanos",
  parentKey: "parent\u0000\ud800",
  byteLength: Number.MAX_SAFE_INTEGER,
  etag: '"tag\\\n',
  path: ["list", "\u0000\udfff猫"],
  start: 0,
  end: Number.MAX_SAFE_INTEGER,
};
const expected = () =>
  JSON.stringify([
    "nested-page-v3",
    parts.shape,
    parts.interpretation,
    parts.parentKey,
    parts.byteLength,
    parts.etag,
    parts.path,
    parts.start,
    parts.end,
    parts.previousDefinition,
  ]);

it("preserves nested identity and admits before construction at an exact workspace boundary", () => {
  const plan = planNestedPageCacheKey(parts);
  const wanted = expected();
  const bytes =
    plan.workspaceBytes + c.string + ("decoded-value:".length + plan.characters) * c.stringCodeUnit;
  for (const shortage of [0, 1]) {
    const pool = new MemoryBudget(bytes - shortage);
    const shared = new SharedMemoryCache({ memoryBudget: pool });
    const cache = new DecodedColumnCache(shared, {});
    const lookup = vi.spyOn(shared, "get");
    const reclaim = vi.fn();
    const detach = pool.registerReclaimer(reclaim);
    const stringify = vi.spyOn(JSON, "stringify");
    try {
      expect(cache.getValue(plan)).toBeUndefined();
      expect(stringify).toHaveBeenCalledTimes(shortage ? 0 : 1);
      if (!shortage) expect(lookup).toHaveBeenCalledWith(`decoded-value:${wanted}`);
      expect(reclaim).not.toHaveBeenCalled();
      expect(pool.peakBytes).toBe(shortage ? 0 : bytes);
    } finally {
      stringify.mockRestore();
      lookup.mockRestore();
      detach();
    }
    expect(pool.usedBytes).toBe(0);
  }
});

it("releases nested key scratch before subsequent decoding and payload retention", () => {
  const root = new MemoryScope(new MemoryBudget(1024 * 1024));
  const cache = new DecodedColumnCache(new SharedMemoryCache({ memoryBudget: root.budget }), {});
  const key = planNestedPageCacheKey({ ...parts, path: new Array(1000).fill("\u0000") });
  cache.getValue(key, root);
  expect(root.budget.peakBytes).toBeGreaterThan(64);
  expect(root.budget.usedBytes).toBe(0);
  root.allocate(64, "page descriptor", () => undefined);
  const page = root.retainAllocations({ values: [1] });
  root.dispose();
  expect(root.budget.usedBytes).toBe(64);
  page.release();
  expect(root.budget.usedBytes).toBe(0);
});

it("preserves mandatory stage admission while using only spare cache-pool workspace", () => {
  for (const spare of [0, 32768]) {
    const root = new MemoryScope(new MemoryBudget(32768 + spare));
    const stage = root.childStage(32768, "mandatory decode extent");
    const cache = new DecodedColumnCache(new SharedMemoryCache({ memoryBudget: root.budget }), {});
    const key = planNestedPageCacheKey(parts);
    const stringify = vi.spyOn(JSON, "stringify");
    try {
      cache.getValue(key, stage);
      expect(stringify).toHaveBeenCalledTimes(spare ? 1 : 0);
      expect(stage.availableAllocationBytes).toBe(32768);
      stage.allocate(32768, "mandatory decode", () => undefined);
    } finally {
      stringify.mockRestore();
      root.dispose();
    }
    expect(root.budget.usedBytes).toBe(0);
  }
});
