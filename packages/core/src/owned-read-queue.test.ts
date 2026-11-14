import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { withObjectStoreReadControls } from "./store.js";

it.each([
  "range",
  "head",
] as const)("cancels a queued owned %s without waiting for an unrelated active read", async (kind) => {
  let finish!: () => void;
  let started!: () => void;
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const activeStarted = new Promise<void>((resolve) => {
    started = resolve;
  });
  let reads = 0;
  const store = withObjectStoreReadControls(
    {
      ...memoryStore(),
      async headOwned(_path, scope) {
        reads++;
        started();
        await gate;
        return scope.allocate(8, "test metadata", () => ({ size: 8 })).retain();
      },
      async getRangeOwned(_path, range, scope) {
        reads++;
        started();
        await gate;
        return scope.allocate(range.length, "test range", () => new Uint8Array(range.length));
      },
    },
    { maxConcurrentReads: 1 },
  );
  const pool = new MemoryBudget(16);
  const activeScope = new MemoryScope(pool);
  const queuedScope = new MemoryScope(pool);
  const { getRangeOwned, headOwned } = store;
  if (getRangeOwned === undefined || headOwned === undefined)
    throw new Error("missing owned reader");
  const read = (path: string, scope: MemoryScope) =>
    kind === "head" ? headOwned(path, scope) : getRangeOwned(path, { offset: 0, length: 8 }, scope);
  const active = read("active", activeScope);
  await activeStarted;
  const queued = read("queued", queuedScope);
  const rejected = expect(queued).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  queuedScope.dispose();
  try {
    // The active gate remains closed until after the queued rejection settles.
    await rejected;
    expect(reads).toBe(1);
    expect(pool.peakBytes).toBe(0);
  } finally {
    finish();
    const owner = await active;
    expect(owner?.value).toBeDefined();
    expect(pool.usedBytes).toBe(8);
    owner?.release();
    activeScope.dispose();
  }
  expect(pool.usedBytes).toBe(0);
});
