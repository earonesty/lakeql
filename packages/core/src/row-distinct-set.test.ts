import { expect, it } from "vitest";
import { jsonSafeValue } from "./evaluator.js";
import { stableStringify } from "./manifest.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import { RowDistinctSet } from "./row-distinct-set.js";

it("preserves legacy DISTINCT normalization and serialized sizes", () => {
  const pool = new MemoryBudget(1024 * 1024);
  const scope = new MemoryScope(pool);
  const expected = new Set<string>();
  const set = new RowDistinctSet(scope, (rows, bytes) => {
    expect(rows).toBe(expected.size);
    expect(bytes).toBe(new TextEncoder().encode(JSON.stringify([...expected])).byteLength);
  });
  const rows = [
    { value: 3n },
    { value: 3 },
    { value: 9007199254740993n },
    { value: "9007199254740993" },
    { value: new Uint8Array([0, 255]) },
    { value: "00ff" },
    { value: [undefined, null, { text: '雪\n"' }] },
    { value: new Date(0) },
    { value: {} },
    { value: undefined },
    {},
  ];
  for (const row of [...rows, ...rows]) {
    const key = stableStringify(jsonSafeValue(row));
    const fresh = !expected.has(key);
    expected.add(key);
    expect(set.add(row)).toBe(fresh);
  }
  const live = pool.usedBytes;
  for (let i = 0; i < 1000; i++) expect(set.add({ value: 3n })).toBe(false);
  expect(pool.usedBytes).toBe(live);
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("rejects oversized key formatting, cycles and accessors without releasing siblings", () => {
  const pool = new MemoryBudget(8192);
  const sibling = pool.reserve(1024, "sibling");
  for (const kind of ["large", "cycle", "accessor", "scalarAccessor"]) {
    const scope = new MemoryScope(pool);
    const set = new RowDistinctSet(scope, () => {});
    const row: Record<string, unknown> = {};
    if (kind === "large") row.value = new Uint8Array(65536);
    if (kind === "cycle") row.value = row;
    if (kind === "accessor" || kind === "scalarAccessor")
      Object.defineProperty(row, kind === "scalarAccessor" ? "kind" : "value", {
        enumerable: true,
        get() {
          throw new Error("must not run");
        },
      });
    expect(() => set.add(row)).toThrowError(
      expect.objectContaining({
        code: kind === "large" ? "LAKEQL_BUDGET_EXCEEDED" : "LAKEQL_TYPE_ERROR",
      }),
    );
    scope.dispose();
    expect(pool.usedBytes).toBe(sibling.bytes);
  }
  sibling.release();
});

it.each([
  false,
  true,
])("retains owned DISTINCT output and releases state on return (ordered=%s)", async (ordered) => {
  const pool = new MemoryBudget(65536);
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        yield [{ value: 2 }, { value: 1 }, { value: 2 }, { value: 3 }];
      },
    },
  });
  const query = () => {
    let result = lake.path("table").distinct().batchSize(1);
    if (ordered) result = result.orderBy([{ column: "value" }]);
    return result.run();
  };
  const owner = await query().toOwnedArray();
  expect(owner.value).toEqual((ordered ? [1, 2, 3] : [2, 1, 3]).map((value) => ({ value })));
  expect(pool.usedBytes).toBeGreaterThan(0);
  owner.release();
  expect(pool.usedBytes).toBe(0);
  const stream = query().batches()[Symbol.asyncIterator]();
  await stream.next();
  expect(pool.usedBytes).toBeGreaterThan(0);
  await stream.return?.();
  expect(pool.usedBytes).toBe(0);
});

it("scales retained DISTINCT keys with cardinality rather than input count", () => {
  function measure(count: number, duplicate: boolean) {
    const pool = new MemoryBudget(2 * 1024 * 1024);
    const scope = new MemoryScope(pool);
    const set = new RowDistinctSet(scope, () => {});
    for (let index = 0; index < count; index++)
      set.add({ value: duplicate ? "same" : String(index).padStart(6, "0") });
    const result = { live: pool.usedBytes, peak: pool.peakBytes };
    scope.dispose();
    expect(pool.usedBytes).toBe(0);
    return result;
  }
  // Include one duplicate probe in the baseline: its scratch overlaps the retained key.
  expect(measure(1000, true)).toEqual(measure(2, true));
  const small = measure(100, false);
  const large = measure(1000, false);
  expect(large.live).toBeGreaterThan(small.live);
  expect(large.peak).toBeLessThan(small.peak * 12);
});
