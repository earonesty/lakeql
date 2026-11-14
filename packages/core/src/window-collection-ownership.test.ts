import { expect, it } from "vitest";
import { col, lit } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import { memorySpillAdapter } from "./runtime.js";

async function query(budget: MemoryBudget, count: number) {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: budget,
    scanner: {
      async *scan() {
        yield Array.from({ length: count }, (_, id) => ({ id, text: `x${id}` }));
      },
    },
  });
  return lake
    .path("table")
    .window("rn", {
      fn: "row_number",
      args: [],
      over: { partitionBy: [], orderBy: [{ expr: col("id") }] },
    })
    .project({
      expanded: {
        kind: "call",
        fn: "replace",
        args: [col("text"), lit("x"), lit("a".repeat(10000))],
      },
    })
    .select(["id"])
    .batchSize(1)
    .run();
}

it("retains collected window output across jobs until its owner releases", async () => {
  const budget = new MemoryBudget(128 * 1024);
  const owner = await (await query(budget, 3)).toOwnedArray();
  expect(owner.value.map((row) => row.expanded)).toEqual(
    [0, 1, 2].map((id) => "a".repeat(10000) + id),
  );
  expect(budget.usedBytes).toBeGreaterThanOrEqual(60000);
  const sibling = budget.reserve(1000, "sibling");
  owner.release();
  expect(budget.usedBytes).toBe(1000);
  sibling.release();
});

it.each([
  "plain",
  "owned",
  "checkpoint",
])("rejects cumulative projected output before exceeding the cap (%s)", async (route) => {
  const budget = new MemoryBudget(65536);
  const q = await query(budget, 4);
  await expect(
    route === "plain" ? q.toArray() : route === "owned" ? q.toOwnedArray() : q.windowWithState(),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(budget.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("holds projected checkpoint output until writes settle (failure=%s)", async (failure) => {
  const budget = new MemoryBudget(128 * 1024);
  const q = await query(budget, 3);
  let start!: () => void;
  let finish!: () => void;
  const ready = new Promise<void>((resolve) => {
    start = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const backing = memorySpillAdapter();
  const result = q.windowWithState({
    spill: {
      ...backing,
      async write(id, bytes) {
        start();
        await pending;
        if (failure) throw Error("write failed");
        return backing.write(id, bytes);
      },
    },
  });
  await ready;
  expect(budget.usedBytes).toBeGreaterThanOrEqual(60000);
  finish();
  if (failure) await expect(result).rejects.toThrow("write failed");
  else expect((await result).rows.map((row) => row.id)).toEqual([0, 1, 2]);
  expect(budget.usedBytes).toBe(0);
});
