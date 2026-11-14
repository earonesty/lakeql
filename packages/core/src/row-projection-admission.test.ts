import { expect, it } from "vitest";
import type { Expr } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import { memorySpillAdapter } from "./runtime.js";

const expression = (text: string): Expr => ({
  kind: "call",
  fn: "replace",
  args: [
    { kind: "column", name: "key" },
    { kind: "literal", value: "ß" },
    { kind: "literal", value: text },
  ],
});

it.each([
  "topKWithState",
  "sortWithState",
] as const)("%s admits projections through spill and rejects oversized expansion", async (method) => {
  for (const huge of [false, true]) {
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
            { key: "ß", id: 2 },
            { key: "a", id: 1 },
          ];
        },
      },
    });
    const spill = memorySpillAdapter();
    let writes = 0;
    spill.write = async (id, bytes) => {
      writes++;
      await Promise.resolve();
      expect(pool.usedBytes).toBeGreaterThan(sibling.bytes + bytes.length);
      return { id, byteSize: bytes.length };
    };
    const result = lake
      .path("table")
      .orderBy([{ column: "id" }])
      .offset(1)
      .limit(1)
      .project({ projected: expression(huge ? "x".repeat(100000) : "accepted") })
      .run()
      [method]({ spill });
    if (huge) {
      await expect(result).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
      expect(writes).toBe(0);
    } else {
      expect((await result).rows).toEqual([{ key: "ß", id: 2, projected: "accepted" }]);
      expect(writes).toBeGreaterThan(0);
    }
    expect(pool.usedBytes).toBe(sibling.bytes);
    sibling.release();
  }
});

it("releases aggregate projection scratch each row and retains copied aggregate values", async () => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  for (const count of [1, 1000]) {
    const pool = new MemoryBudget(65536);
    const lake = new Lake({
      store,
      memoryPool: pool,
      scanner: {
        async *scan() {
          for (let i = 0; i < count; i++) yield [{ key: "ß" }];
        },
      },
    });
    const result = await lake
      .path("table")
      .project({ projected: expression("accepted") })
      .run()
      .aggregateWithState([], { min: { op: "min", column: "projected" }, count: { op: "count" } });
    expect(result.rows).toEqual([{ min: "accepted", count }]);
    expect(pool.usedBytes).toBe(0);
    await expect(
      lake
        .path("table")
        .project({ projected: expression("x".repeat(100000)) })
        .run()
        .aggregateWithState([], { count: { op: "count" } }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    expect(pool.usedBytes).toBe(0);
  }
});

it("preserves special source fields without mutating the source or output prototype", async () => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const source = Object.freeze({ id: 1, ["__proto__"]: "source" });
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        yield [source];
      },
    },
  });
  const result = await lake
    .path("table")
    .orderBy([{ column: "id" }])
    .limit(1)
    .project({ projected: { kind: "literal", value: "safe" } })
    .run()
    .topKWithState();
  expect(Object.getPrototypeOf(result.rows[0])).toBe(Object.prototype);
  expect(Object.hasOwn(result.rows[0] ?? {}, "__proto__")).toBe(true);
  expect(result.rows[0]?.__proto__).toBe("source");
  expect(result.rows[0]?.projected).toBe("safe");
  expect(source).toEqual({ id: 1, ["__proto__"]: "source" });
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "topKWithState",
  "sortWithState",
] as const)("%s pins projected output until an aborted spill settles", async (method) => {
  const pool = new MemoryBudget(65536);
  const controller = new AbortController();
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    budget: { signal: controller.signal },
    scanner: {
      async *scan() {
        yield [{ key: "ß" }];
      },
    },
  });
  let started!: () => void;
  let settle!: () => void;
  const ready = new Promise<void>((resolve) => {
    started = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    settle = resolve;
  });
  const spill = memorySpillAdapter();
  spill.write = async (id, bytes) => {
    started();
    await pending;
    return { id, byteSize: bytes.length };
  };
  const result = lake
    .path("table")
    .orderBy([{ column: "key" }])
    .limit(1)
    .project({ projected: expression("x".repeat(2000)) })
    .run()
    [method]({ spill });
  const rejected = expect(result).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  await ready;
  controller.abort();
  expect(pool.usedBytes).toBeGreaterThanOrEqual(4000);
  settle();
  await rejected;
  expect(pool.usedBytes).toBe(0);
});
