import { expect, it } from "vitest";
import { readCheckpointSpill } from "./checkpoint-spill-read.js";
import { col } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import { memorySpillAdapter } from "./runtime.js";
import {
  serializeWindowOperatorState,
  windowOperatorState,
  windowRowsFromState,
} from "./window-state.js";

it.each([
  false,
  true,
])("admits restored checkpoint rows at the exact peak (encoded=%s)", async (encoded) => {
  const snapshot = windowOperatorState({}, [{ value: "猫\u0000", id: 1 }]);
  const input = encoded ? serializeWindowOperatorState(snapshot) : snapshot;
  const run = async (budget: MemoryBudget) => {
    const scope = new MemoryScope(budget);
    try {
      const rows = await windowRowsFromState({}, { operatorState: input }, scope);
      expect(rows).toEqual([{ value: "猫\u0000", id: 1 }]);
      expect(budget.usedBytes).toBeGreaterThan(0);
      return budget.peakBytes;
    } finally {
      scope.dispose();
    }
  };
  const peak = await run(new MemoryBudget(65536));
  await run(new MemoryBudget(peak));
  const insufficient = new MemoryBudget(peak - 1);
  await expect(run(insufficient)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(insufficient.usedBytes).toBe(0);
});

it("charges legacy spill buffers after externally allocated reads", async () => {
  const backing = memorySpillAdapter();
  await backing.write("state", new Uint8Array(1000));
  const budget = new MemoryBudget(4096);
  const scope = new MemoryScope(budget);
  try {
    await readCheckpointSpill({ read: (ref) => backing.read(ref) }, "state", scope);
    expect(budget.usedBytes).toBeGreaterThanOrEqual(1000);
  } finally {
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it("uses owned reads for both checkpoint manifest and runs on resume", async () => {
  const budget = new MemoryBudget(128 * 1024);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: budget,
    scanner: {
      async *scan() {
        yield [{ id: 1 }];
      },
    },
  });
  const query = lake.path("table").window("rn", {
    fn: "row_number",
    args: [],
    over: { partitionBy: [], orderBy: [{ expr: col("id") }] },
  });
  const spill = memorySpillAdapter();
  const first = await query.windowWithState({ spill, spillId: "first" });
  let reads = 0;
  const readOwned = spill.readOwned.bind(spill);
  spill.read = async () => {
    throw Error("unowned read used");
  };
  spill.readOwned = async (ref, pool) => {
    reads++;
    return readOwned(ref, pool);
  };
  const resumed = await query.windowWithState({
    spill,
    spillId: "resumed",
    operatorState: { spillRef: first.operatorSpill?.id ?? "missing" },
  });
  expect(resumed.rows).toEqual([
    { id: 1, rn: 1 },
    { id: 1, rn: 2 },
  ]);
  expect(reads).toBe(2);
  expect(budget.usedBytes).toBe(0);
});

it.each([
  "{",
  '{"version":1,"windows":{},"rows":[{"value":[]}]}',
])("releases malformed restoration: %s", async (input) => {
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  try {
    await expect(
      windowRowsFromState({}, { operatorState: new TextEncoder().encode(input) }, scope),
    ).rejects.toThrow();
  } finally {
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it("holds an owned read through cancellation and rejects before parsing on settlement", async () => {
  const controller = new AbortController();
  const budget = new MemoryBudget(128 * 1024);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: budget,
    budget: { signal: controller.signal },
    scanner: {
      async *scan() {
        yield [{ id: 1 }];
      },
    },
  });
  const query = lake.path("table").window("rn", {
    fn: "row_number",
    args: [],
    over: { partitionBy: [], orderBy: [{ expr: col("id") }] },
  });
  const spill = memorySpillAdapter();
  await query.windowWithState({ spill, spillId: "state" });
  const read = spill.readOwned.bind(spill);
  let start!: () => void;
  let finish!: () => void;
  const ready = new Promise<void>((resolve) => {
    start = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  spill.readOwned = async (ref, pool) => {
    const owner = await read(ref, pool);
    start();
    await pending;
    return owner;
  };
  const result = query.windowWithState({ spill, operatorState: { spillRef: "state" } });
  await ready;
  controller.abort();
  expect(budget.usedBytes).toBeGreaterThan(0);
  finish();
  await expect(result).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(budget.usedBytes).toBe(0);
});
