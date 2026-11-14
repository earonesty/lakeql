import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { type NestedPageKeyParts, nestedPageCacheKey } from "./nested-page-cache-key.js";

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

it("preserves key identity and admits before construction at an exact workspace boundary", () => {
  const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
  expect(nestedPageCacheKey(scope, parts)).toBe(expected());
  const bytes = scope.budget.peakBytes;
  scope.dispose();
  for (const shortage of [0, 1]) {
    const pool = new MemoryBudget(bytes - shortage);
    const target = new MemoryScope(pool);
    let reclaimed = 0;
    const detach = pool.registerReclaimer(() => {
      reclaimed++;
    });
    const stringify = vi.spyOn(JSON, "stringify");
    try {
      const key = nestedPageCacheKey(target, parts);
      expect(stringify).toHaveBeenCalledTimes(shortage === 0 ? 1 : 0);
      expect(key !== undefined).toBe(shortage === 0);
      expect(reclaimed).toBe(0);
      expect(pool.usedBytes).toBe(shortage === 0 ? bytes : 0);
    } finally {
      stringify.mockRestore();
      detach();
      target.dispose();
    }
    expect(pool.usedBytes).toBe(0);
  }
});

it("keeps key scratch out of retained page payload", () => {
  const root = new MemoryScope(new MemoryBudget(1024 * 1024));
  const scratch = root.childScope();
  const key = nestedPageCacheKey(scratch, { ...parts, path: new Array(1000).fill("\u0000") });
  expect(key).toBeDefined();
  root.allocate(64, "page descriptor", () => undefined);
  const page = root.retainAllocations({ values: [1] });
  root.dispose();
  expect(root.budget.usedBytes).toBe(64);
  page.release();
  expect(root.budget.usedBytes).toBe(0);
});

it("preserves nonrefundable stage admission for mandatory page decoding", () => {
  const root = new MemoryScope(new MemoryBudget(65536));
  const stage = root.childStage(32768, "mandatory decode extent");
  const scratch = stage.childScope();
  const stringify = vi.spyOn(JSON, "stringify");
  try {
    expect(nestedPageCacheKey(scratch, parts)).toBeUndefined();
    expect(stringify).not.toHaveBeenCalled();
    expect(stage.availableAllocationBytes).toBe(32768);
    stage.allocate(32768, "mandatory decode", () => undefined);
  } finally {
    stringify.mockRestore();
    root.dispose();
  }
  expect(root.budget.usedBytes).toBe(0);
});
