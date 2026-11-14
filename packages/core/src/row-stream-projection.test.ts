import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { detachOwnedRow } from "./owned-row-detachment.js";
import { Lake } from "./query.js";

it("transfers known immutable payload ownership while copying mutable source data", () => {
  const budget = new MemoryBudget(65536);
  const source = new MemoryScope(budget);
  const text = source.allocate(10000, "known projected text", () => "x".repeat(4000)).value;
  const bytes = new Uint8Array([1, 2]);
  const owner = detachOwnedRow({ text, bytes }, budget, source);
  source.dispose();
  bytes.fill(0);
  expect(owner.value).toEqual({ text, bytes: new Uint8Array([1, 2]) });
  expect(budget.usedBytes).toBeLessThan(12000);
  expect(budget.usedBytes).toBeGreaterThanOrEqual(10000);
  owner.release();
  expect(budget.usedBytes).toBe(0);
});

it("bounds projection batches independently of oversized scanner batches", async () => {
  const pool = new MemoryBudget(16384);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  let closed = false;
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        try {
          yield Array.from({ length: 1000 }, (_, id) => ({ id, text: "ß" }));
        } finally {
          closed = true;
        }
      },
    },
  });
  const query = () =>
    lake
      .path("table")
      .batchSize(1)
      .project({ text: { kind: "call", fn: "upper", args: [{ kind: "column", name: "text" }] } })
      .run();
  let count = 0;
  for await (const batch of query().batches()) {
    expect(batch).toEqual([{ id: count++, text: "SS" }]);
    expect(pool.usedBytes).toBeGreaterThan(0);
  }
  expect(count).toBe(1000);
  expect(closed).toBe(true);
  expect(pool.usedBytes).toBe(0);
  // Collection retains every output; the same cap must reject rather than
  // silently release prior batches while the collector still references them.
  await expect(query().toArray()).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(pool.usedBytes).toBe(0);
});

it("owns selected fallback results across scanner reuse and owner release", async () => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const row = { id: 0, bytes: new Uint8Array(1), ignored: "x".repeat(100000) };
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        for (let id = 0; id < 3; id++) {
          row.id = id;
          row.bytes[0] = id;
          yield [row];
        }
        row.bytes.fill(99);
      },
    },
  });
  const owner = await lake.path("table").select(["id", "bytes"]).batchSize(1).run().toOwnedArray();
  expect(owner.value).toEqual([0, 1, 2].map((id) => ({ id, bytes: new Uint8Array([id]) })));
  expect(pool.usedBytes).toBeGreaterThan(0);
  owner.release();
  expect(pool.usedBytes).toBe(0);
});

it("releases failed accumulated projection before awaiting scanner cleanup", async () => {
  const pool = new MemoryBudget(65536);
  const sibling = pool.reserve(1024, "sibling");
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  let planningBytes = 0;
  let cleanupStarted!: () => void;
  let finishCleanup!: () => void;
  const ready = new Promise<void>((resolve) => {
    cleanupStarted = resolve;
  });
  const cleanup = new Promise<void>((resolve) => {
    finishCleanup = resolve;
  });
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        planningBytes = pool.usedBytes - sibling.bytes;
        try {
          yield [{ text: "small" }];
          yield [{ text: "ß" }];
        } finally {
          cleanupStarted();
          await cleanup;
        }
      },
    },
  });
  const result = lake
    .path("table")
    .batchSize(1)
    .project({
      text: {
        kind: "call",
        fn: "replace",
        args: [
          { kind: "column", name: "text" },
          { kind: "literal", value: "ß" },
          { kind: "literal", value: "x".repeat(100000) },
        ],
      },
    })
    .run()
    .toOwnedArray();
  const rejected = expect(result).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  await ready;
  expect(planningBytes).toBeGreaterThan(0);
  expect(pool.usedBytes).toBe(sibling.bytes + planningBytes);
  finishCleanup();
  await rejected;
  expect(pool.usedBytes).toBe(sibling.bytes);
  sibling.release();
});

it.each([
  "scan",
  "sort",
  "topK",
])("admits collected %s projections at the measured exact peak", async (route) => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  async function run(cap: number, succeeds: boolean) {
    const pool = new MemoryBudget(cap);
    const lake = new Lake({
      store,
      memoryPool: pool,
      scanner: {
        async *scan() {
          yield [{ text: "ß" }, { text: "a" }];
        },
      },
    });
    let query = lake
      .path("table")
      .batchSize(1)
      .project({ text: { kind: "call", fn: "upper", args: [{ kind: "column", name: "text" }] } });
    if (route !== "scan") query = query.orderBy([{ column: "text", direction: "desc" }]);
    if (route === "topK") query = query.limit(2);
    const result = query.run().toOwnedArray();
    if (succeeds) {
      const owner = await result;
      expect(owner.value).toEqual([{ text: "SS" }, { text: "A" }]);
      expect(pool.usedBytes).toBeGreaterThan(0);
      owner.release();
    } else await expect(result).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    expect(pool.usedBytes).toBe(0);
    return pool.peakBytes;
  }
  const peak = await run(65536, true);
  expect(await run(peak, true)).toBe(peak);
  await run(peak - 1, false);
});

it.each([
  false,
  true,
])("releases ordered batch projection on early return (topK=%s)", async (topK) => {
  const pool = new MemoryBudget(65536);
  const sibling = pool.reserve(1024, "sibling");
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        yield [
          { id: 2, text: "ß" },
          { id: 1, text: "ß" },
          { id: 0, text: "ß" },
        ];
      },
    },
  });
  let query = lake
    .path("table")
    .orderBy([{ column: "id" }])
    .batchSize(1)
    .project({
      text: { kind: "call", fn: "upper", args: [{ kind: "column", name: "text" }] },
    });
  if (topK) query = query.limit(2);
  const iterator = query.run().batches()[Symbol.asyncIterator]();
  expect((await iterator.next()).value).toEqual([{ id: 0, text: "SS" }]);
  const live = pool.usedBytes;
  expect(live).toBeGreaterThan(sibling.bytes);
  expect((await iterator.next()).value).toEqual([{ id: 1, text: "SS" }]);
  expect(pool.usedBytes).toBe(live);
  await iterator.return?.();
  expect(pool.usedBytes).toBe(sibling.bytes);
  sibling.release();
  expect(pool.usedBytes).toBe(0);
});
