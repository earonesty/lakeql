import { expect, it } from "vitest";
import { col } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import { memorySpillAdapter } from "./runtime.js";
import {
  serializeWindowOperatorState,
  windowOperatorState,
  windowOperatorStateFromRuns,
} from "./window-state.js";

it("preserves checkpoint bytes with exact admission and rejects one byte below", () => {
  const rows = [{ text: "猫\u0000\ud800", value: 12n, bytes: Uint8Array.of(0, 255), n: null }];
  const expected = serializeWindowOperatorState(windowOperatorState({}, rows));
  const measure = new MemoryBudget(65536);
  const run = (budget: MemoryBudget) => {
    const scope = new MemoryScope(budget);
    try {
      const state = windowOperatorState({}, rows, scope);
      const bytes = serializeWindowOperatorState(state, scope);
      expect(bytes).toEqual(expected);
      expect(budget.usedBytes).toBeGreaterThanOrEqual(bytes.byteLength);
    } finally {
      scope.dispose();
    }
  };
  run(measure);
  run(new MemoryBudget(measure.peakBytes));
  const short = new MemoryBudget(measure.peakBytes - 1);
  expect(() => run(short)).toThrow(expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }));
  expect(short.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("keeps encoded run bytes admitted until a write settles (failure=%s)", async (failure) => {
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  const backing = memorySpillAdapter();
  let start!: () => void;
  let finish!: () => void;
  const ready = new Promise<void>((resolve) => {
    start = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  let encodedLength = 0;
  const result = windowOperatorStateFromRuns(
    {},
    [[{ value: "猫".repeat(1000) }]],
    {
      ...backing,
      async write(id, bytes) {
        encodedLength = bytes.byteLength;
        start();
        await pending;
        if (failure) throw Error("write failed");
        return backing.write(id, bytes);
      },
    },
    "checkpoint",
    scope,
  );
  await ready;
  expect(budget.usedBytes).toBeGreaterThan(encodedLength + 2000);
  finish();
  if (failure) await expect(result).rejects.toThrow("write failed");
  else {
    const state = await result;
    expect(state.runs).toHaveLength(1);
    expect(budget.usedBytes).toBeLessThan(encodedLength);
  }
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});

it.each([
  1, 2,
])("rejects cancellation during checkpoint write %s after that write settles", async (cancelWrite) => {
  const controller = new AbortController();
  const budget = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: budget,
    budget: { signal: controller.signal },
    scanner: {
      async *scan() {
        yield [{ id: 1, text: "猫".repeat(100) }];
      },
    },
  });
  let writes = 0;
  const backing = memorySpillAdapter();
  const result = lake
    .path("table")
    .window("rn", {
      fn: "row_number",
      args: [],
      over: { partitionBy: [], orderBy: [{ expr: col("id") }] },
    })
    .windowWithState({
      spill: {
        ...backing,
        async write(id, bytes) {
          writes++;
          if (writes === cancelWrite) {
            controller.abort();
            expect(budget.usedBytes).toBeGreaterThan(bytes.byteLength);
          }
          return backing.write(id, bytes);
        },
      },
    });
  await expect(result).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(writes).toBe(cancelWrite);
  expect(budget.usedBytes).toBe(0);
});

it("rejects an oversized run before invoking the spill writer", async () => {
  const budget = new MemoryBudget(4096);
  const scope = new MemoryScope(budget);
  let writes = 0;
  const backing = memorySpillAdapter();
  try {
    await expect(
      windowOperatorStateFromRuns(
        {},
        [[{ value: "x".repeat(10000) }]],
        {
          ...backing,
          async write(id, bytes) {
            writes++;
            return backing.write(id, bytes);
          },
        },
        "oversized",
        scope,
      ),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    expect(writes).toBe(0);
  } finally {
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});
