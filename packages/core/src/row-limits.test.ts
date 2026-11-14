import { expect, it, vi } from "vitest";
import { batchFromColumns, materializeBatchRows } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import { vectorTopKBatch, vectorTopKOwnedBatch } from "./vector-sort.js";

const invalid = [NaN, Infinity, -1, 0.5, Number.MAX_SAFE_INTEGER + 1];
const bounds = [
  ...invalid.map((limit) => ({ limit, offset: 0 })),
  ...invalid.map((offset) => ({ limit: 1, offset })),
  { limit: Number.MAX_SAFE_INTEGER, offset: 1 },
  { limit: 1, offset: Number.MAX_SAFE_INTEGER },
];
it.each(bounds)("rejects row bounds %j before query I/O or vector allocation", async (options) => {
  const store = memoryStore();
  const head = vi.spyOn(store, "head");
  const scan = vi.fn(async function* () {
    yield [{ key: 1 }];
  });
  const pool = new MemoryBudget(65536);
  const lake = new Lake({ store, scanner: { scan }, memoryPool: pool });
  const typed = expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" });
  expect(() => lake.path("file").limit(options.limit).offset(options.offset).run()).toThrow(typed);
  await expect(
    lake
      .path("file")
      .run()
      .aggregateWithState([], { n: { op: "count" } }, options),
  ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
  expect(head).not.toHaveBeenCalled();
  expect(scan).not.toHaveBeenCalled();
  expect(pool.usedBytes).toBe(0);
  const scope = new MemoryScope(pool);
  const batch = batchFromColumns({ key: [3, 1, 2] }, scope);
  const input = scope.retainAllocations(batch);
  try {
    const live = pool.usedBytes;
    const peak = pool.peakBytes;
    expect(() => vectorTopKBatch(batch, [{ column: "key" }], options)).toThrow(typed);
    expect(() => vectorTopKOwnedBatch(input, [{ column: "key" }], options)).toThrow(typed);
    expect(pool.usedBytes).toBe(live);
    expect(pool.peakBytes).toBe(peak);
  } finally {
    input.release();
    scope.dispose();
  }
  expect(pool.usedBytes).toBe(0);
});

it("accepts exact safe bounds without allocating requested-limit-sized vectors", () => {
  const batch = batchFromColumns({ key: [3, 1, 2] });
  for (const options of [
    { limit: Number.MAX_SAFE_INTEGER, offset: 0 },
    { limit: 0, offset: Number.MAX_SAFE_INTEGER },
  ]) {
    const pool = new MemoryBudget(65536);
    const scope = new MemoryScope(pool);
    const ownedBatch = batchFromColumns({ key: [3, 1, 2] }, scope);
    const input = scope.retainAllocations(ownedBatch);
    try {
      const result = vectorTopKOwnedBatch(input, [{ column: "key" }], options);
      try {
        const expected = options.limit === 0 ? [] : [{ key: 1 }, { key: 2 }, { key: 3 }];
        expect(materializeBatchRows(result.value)).toEqual(expected);
        expect(materializeBatchRows(vectorTopKBatch(batch, [{ column: "key" }], options))).toEqual(
          expected,
        );
      } finally {
        result.release();
      }
    } finally {
      input.release();
      scope.dispose();
    }
    expect(pool.usedBytes).toBe(0);
  }
});

it.each([...invalid, 0])("rejects invalid batch/group capacity %s before I/O", async (capacity) => {
  const store = memoryStore();
  const head = vi.spyOn(store, "head");
  const scan = vi.fn(async function* () {
    yield [];
  });
  const lake = new Lake({ store, scanner: { scan } });
  expect(() => lake.path("file").batchSize(capacity).run()).toThrow(
    expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }),
  );
  await expect(
    lake
      .path("file")
      .run()
      .aggregateWithState([], { n: { op: "count" } }, { maxGroups: capacity }),
  ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
  expect(head).not.toHaveBeenCalled();
  expect(scan).not.toHaveBeenCalled();
});

it("keeps vector top-k's limit mandatory for JavaScript callers", () => {
  const batch = batchFromColumns({ key: [1] });
  const scope = new MemoryScope(new MemoryBudget(65536));
  const input = scope.retainAllocations(batch);
  const missing = {} as { limit: number };
  try {
    expect(() => vectorTopKBatch(batch, [{ column: "key" }], missing)).toThrow(/limit/);
    expect(() => vectorTopKOwnedBatch(input, [{ column: "key" }], missing)).toThrow(/limit/);
  } finally {
    input.release();
    scope.dispose();
  }
});
