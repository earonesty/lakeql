import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { batchFromColumns } from "./batch.js";
import { gt } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { Lake, type ScanOptions } from "./query.js";

async function fixture(pool: MemoryBudget, cap: number) {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const seen: ScanOptions[] = [];
  const lake = new Lake({
    store,
    memoryPool: pool,
    budget: { maxAllocatedMemoryBytes: cap },
    scanner: {
      async *scan(_path, options) {
        seen.push(options);
        if (!options.memoryBudget) throw new Error("missing query budget");
        const scope = new MemoryScope(
          options.memoryBudget,
          options.budget.signal ? [options.budget.signal] : [],
        );
        try {
          // Scratch backing does not escape the adapter; yielded rows contain copied scalars.
          const scratch = scope.allocate(4, "fixture scratch", () => Uint8Array.of(1, 2, 3, 4));
          yield [{ value: scratch.value[0] }];
          yield [{ value: scratch.value[1] }];
        } finally {
          scope.dispose();
        }
      },
    },
  });
  return { lake, seen };
}

it("shares physical headroom across query children and cleans up an early consumer exit", async () => {
  const perQuery = 2048;
  const pool = new MemoryBudget(2 * perQuery);
  const { lake, seen } = await fixture(pool, perQuery);
  const first = lake.path("table").run();
  const second = lake.path("table").run();
  expect(first.memoryBudget).not.toBe(second.memoryBudget);
  const a = first.batches()[Symbol.asyncIterator]();
  const b = second.batches()[Symbol.asyncIterator]();
  try {
    await a.next();
    await b.next();
    const live = first.memoryBudget.usedBytes;
    expect(live).toBeGreaterThan(4 + c.array + c.arraySlot);
    expect(pool.usedBytes).toBe(2 * live);
    expect(second.memoryBudget.usedBytes).toBe(live);
    expect(seen.map((options) => options.memoryBudget)).toEqual([
      first.memoryBudget,
      second.memoryBudget,
    ]);
    const pressure = pool.reserve(pool.maxBytes - pool.usedBytes, "concurrent pressure");
    try {
      await expect(
        lake.path("table").run().batches()[Symbol.asyncIterator]().next(),
      ).rejects.toMatchObject({
        code: "LAKEQL_BUDGET_EXCEEDED",
      });
      expect(pool.usedBytes).toBe(pool.maxBytes);
    } finally {
      pressure.release();
    }
  } finally {
    await a.return?.();
    await b.return?.();
  }
  expect(pool.usedBytes).toBe(0);
  const rows = [];
  for await (const row of lake.path("table").run().rows()) rows.push(row);
  expect(rows).toEqual([{ value: 1 }, { value: 2 }]);
  expect(pool.usedBytes).toBe(0);
});

it("enforces query child limits even when the shared pool has spare capacity", async () => {
  const pool = new MemoryBudget(128);
  const { lake } = await fixture(pool, 3);
  await expect(lake.path("table").toArray()).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  expect(pool.peakBytes).toBe(0);
});

it("uses a bounded default pool and inherits stricter serialized limits unless overridden", () => {
  const base = {
    store: memoryStore(),
    scanner: {
      async *scan() {
        yield [];
      },
    },
  };
  expect(new Lake(base).memoryPool.maxBytes).toBe(64 * 1024 * 1024);
  const lake = new Lake({ ...base, budget: { maxMemoryBytes: 1024 } });
  expect(lake.path("table").run().memoryBudget.maxBytes).toBe(1024);
  const shared = new MemoryBudget(4096);
  const configured = new Lake({
    ...base,
    memoryPool: shared,
    budget: { maxAllocatedMemoryBytes: 2048 },
  });
  expect(configured.path("table").run().memoryBudget.maxBytes).toBe(2048);
  expect(configured.memoryPool).toBe(shared);
});

it("rejects owned collection aborted as its producer completes and releases accumulated output", async () => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(4096);
  const controller = new AbortController();
  let closed = false;
  const lake = new Lake({
    store,
    memoryPool: pool,
    budget: { signal: controller.signal },
    scanner: {
      async *scan() {
        yield [];
      },
      async *scanOwnedVectorBatches(_path, options) {
        const scope = new MemoryScope(options.memoryBudget);
        try {
          const values = scope.allocate(8, "fixture values", () => Float64Array.of(42));
          yield scope.retainAllocations({
            rowOffset: 0,
            batch: {
              rowCount: 1,
              columns: { value: { type: "f64" as const, values: values.value } },
            },
          });
          controller.abort();
        } finally {
          closed = true;
          scope.dispose();
        }
      },
    },
  });
  await expect(lake.path("table").run().toOwnedArray()).rejects.toMatchObject({
    code: "LAKEQL_ABORTED",
  });
  expect(closed).toBe(true);
  expect(pool.usedBytes).toBe(0);
});

it("releases owned output on materialization failure before scanner shutdown finishes", async () => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(4096);
  let enter!: () => void;
  let resume!: () => void;
  let produced = 0;
  let planningBytes = 0;
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  const waiting = new Promise<void>((resolve) => {
    resume = resolve;
  });
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      scan() {
        throw new Error("Unexpected row fallback");
      },
      async *scanOwnedVectorBatches(_path, options) {
        planningBytes = pool.usedBytes;
        try {
          for (const text of ["first", "x".repeat(4096)]) {
            const scope = new MemoryScope(options.memoryBudget);
            const values = scope.allocate(8, "fixture references", () => [text]);
            const owner = scope.retainAllocations({
              rowOffset: produced++,
              batch: {
                rowCount: 1,
                columns: { value: { type: "utf8" as const, values: values.value } },
              },
            });
            scope.dispose();
            yield owner;
          }
        } finally {
          enter();
          await waiting;
        }
      },
    },
  });
  const result = lake.path("table").run().toOwnedArray();
  const rejected = expect(result).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
    details: { purpose: "materialized output" },
  });
  await entered;
  try {
    expect(produced).toBe(2);
    expect(planningBytes).toBeGreaterThan(0);
    expect(pool.usedBytes).toBe(planningBytes);
    const sibling = pool.reserve(pool.maxBytes - planningBytes, "sibling during scanner shutdown");
    sibling.release();
  } finally {
    resume();
  }
  await rejected;
  expect(pool.usedBytes).toBe(0);
});

it("releases earlier output before a failed deferred-column read finishes closing", async () => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(8192);
  let enter!: () => void;
  let resume!: () => void;
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  const waiting = new Promise<void>((resolve) => {
    resume = resolve;
  });
  const reads: number[] = [];
  let planningBytes = 0;
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      scan() {
        throw new Error("Unexpected row fallback");
      },
      async *scanOwnedVectorBatches(_path, options) {
        const late = options.columns?.includes("value") === true;
        if (!late) planningBytes = pool.usedBytes;
        const start = options.rowStart ?? 0;
        const end = options.rowEnd ?? 2;
        try {
          for (let index = start; index < end; index++) {
            const scope = new MemoryScope(options.memoryBudget);
            const batch = late
              ? {
                  rowCount: 1,
                  columns: {
                    value: {
                      type: "utf8" as const,
                      values: ["x".repeat(index === 0 ? 1000 : 10000)],
                    },
                  },
                }
              : batchFromColumns({ id: [index] }, scope);
            const owner = scope.retainAllocations({ rowOffset: index, batch });
            scope.dispose();
            if (late) reads.push(index);
            yield owner;
          }
        } finally {
          if (late && start === 1) {
            enter();
            await waiting;
          }
        }
      },
    },
  });
  const result = lake
    .path("table")
    .select(["value"])
    .where(gt("id", -1))
    .limit(2)
    .batchSize(1)
    .run()
    .toOwnedArray();
  const rejected = expect(result).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
    details: { purpose: "materialized output" },
  });
  await entered;
  try {
    expect(reads).toEqual([0, 1]);
    // Current predicate/gather backing can remain until outer cleanup, but the
    // first output's 2,000-byte text payload must already be released.
    expect(planningBytes).toBeGreaterThan(0);
    expect(pool.usedBytes - planningBytes).toBeLessThan(1024);
  } finally {
    resume();
  }
  await rejected;
  expect(pool.usedBytes).toBe(0);
});
