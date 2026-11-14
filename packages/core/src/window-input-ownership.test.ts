import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import { memorySpillAdapter } from "./runtime.js";

it.each([
  "stream",
  "state",
  "spill",
])("detaches reusable scanner rows through window %s", async (route) => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const reused = { id: 0, text: "" };
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        for (let id = 0; id < 4; id++) {
          reused.id = id;
          reused.text = `text ${id}`;
          yield [reused];
        }
        reused.text = "overwritten";
      },
    },
  });
  const query = lake
    .path("table")
    .window("rn", {
      fn: "row_number",
      args: [],
      over: { partitionBy: [], orderBy: [{ expr: { kind: "column", name: "id" } }] },
    })
    .batchSize(1);
  const result =
    route === "stream"
      ? await query.toArray()
      : (await query.windowWithState(route === "spill" ? { spill: memorySpillAdapter() } : {}))
          .rows;
  expect(result).toEqual(
    Array.from({ length: 4 }, (_, id) => ({ id, text: `text ${id}`, rn: id + 1 })),
  );
  expect(pool.usedBytes).toBe(0);
});

it("keeps detached mutable window input live through streaming output", async () => {
  const pool = new MemoryBudget(65536);
  const sibling = pool.reserve(1024, "sibling");
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const reused = { id: 0, bytes: new Uint8Array(1) };
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        for (let id = 0; id < 4; id++) {
          reused.id = id;
          reused.bytes[0] = id;
          yield [reused];
        }
        reused.bytes.fill(99);
      },
    },
  });
  const stream = lake
    .path("table")
    .window("rn", {
      fn: "row_number",
      args: [],
      over: { partitionBy: [], orderBy: [{ expr: { kind: "column", name: "id" } }] },
    })
    .batchSize(1)
    .run()
    .batches()
    [Symbol.asyncIterator]();
  expect((await stream.next()).value).toEqual([{ id: 0, bytes: new Uint8Array([0]), rn: 1 }]);
  expect(pool.usedBytes).toBeGreaterThan(sibling.bytes);
  expect((await stream.next()).value).toEqual([{ id: 1, bytes: new Uint8Array([1]), rn: 2 }]);
  await stream.return?.();
  expect(pool.usedBytes).toBe(sibling.bytes);
  sibling.release();
});

it.each([
  false,
  true,
])("rejects window input growth before completion (spill=%s)", async (spill) => {
  const pool = new MemoryBudget(16384);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  let scanned = 0;
  let closed = false;
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        try {
          for (; scanned < 100; scanned++) yield [{ id: scanned, text: "x".repeat(2000) }];
        } finally {
          closed = true;
        }
      },
    },
  });
  const query = lake
    .path("table")
    .window("rn", {
      fn: "row_number",
      args: [],
      over: { partitionBy: [], orderBy: [{ expr: { kind: "column", name: "id" } }] },
    })
    .batchSize(1);
  await expect(
    spill ? query.windowWithState({ spill: memorySpillAdapter() }) : query.toArray(),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(scanned).toBeLessThan(100);
  expect(closed).toBe(true);
  expect(pool.usedBytes).toBe(0);
});

it("holds window input reservations while asynchronous checkpoint writes are pending", async () => {
  const pool = new MemoryBudget(65536);
  const sibling = pool.reserve(1024, "sibling");
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        yield [{ id: 0, text: "x".repeat(2000) }];
      },
    },
  });
  let started!: () => void;
  let finish!: () => void;
  const ready = new Promise<void>((resolve) => {
    started = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const backing = memorySpillAdapter();
  const result = lake
    .path("table")
    .window("rn", {
      fn: "row_number",
      args: [],
      over: { partitionBy: [], orderBy: [{ expr: { kind: "column", name: "id" } }] },
    })
    .windowWithState({
      spill: {
        ...backing,
        async write(id, bytes) {
          started();
          await pending;
          return backing.write(id, bytes);
        },
      },
    });
  await ready;
  expect(pool.usedBytes).toBeGreaterThanOrEqual(sibling.bytes + 4000);
  finish();
  expect((await result).rows).toEqual([{ id: 0, text: "x".repeat(2000), rn: 1 }]);
  expect(pool.usedBytes).toBe(sibling.bytes);
  sibling.release();
});
