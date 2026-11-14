import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { cloneObjectInfos } from "./object-info.js";
import { Lake } from "./query.js";
import { memoryCache } from "./runtime.js";

it.each([
  "native",
  "emulated",
  undefined,
] as const)("preserves %s range transport metadata and detaches mutable dates", (rangeReadMode) => {
  const lastModified = new Date("2026-01-01T00:00:00Z");
  const object = {
    path: "猫.parquet",
    size: 7,
    etag: "etag",
    lastModified,
    ...(rangeReadMode === undefined ? {} : { rangeReadMode }),
  };
  const copied = cloneObjectInfos([object]);
  expect(copied).toEqual([object]);
  expect(copied[0]).not.toBe(object);
  lastModified.setTime(0);
  object.path = "changed";
  expect(copied[0]?.path).toBe("猫.parquet");
  expect(copied[0]?.lastModified?.toISOString()).toBe("2026-01-01T00:00:00.000Z");
});

it("admits copied metadata at exact capacity and rolls back partial failure", () => {
  const objects = [
    { path: "one", size: 1 },
    {
      path: "猫".repeat(100),
      size: 2,
      lastModified: new Date(0),
      etag: "e",
      rangeReadMode: "emulated" as const,
    },
  ];
  const run = (capacity: number) => {
    const budget = new MemoryBudget(capacity);
    const scope = new MemoryScope(budget);
    scope.allocate(8, "sibling", () => undefined);
    try {
      expect(cloneObjectInfos(objects, scope)).toEqual(objects);
      return budget.peakBytes;
    } catch (error) {
      expect(budget.usedBytes).toBe(8);
      throw error;
    } finally {
      scope.dispose();
      expect(budget.usedBytes).toBe(0);
    }
  };
  const peak = run(65536);
  expect(run(peak)).toBe(peak);
  expect(() => run(peak - 1)).toThrowError(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
});

it.each([
  "native",
  "emulated",
] as const)("preserves %s transport hints on cold and cached object discovery", async (rangeReadMode) => {
  for (const source of ["data/one.parquet", "data/*.parquet"]) {
    const store = memoryStore();
    await store.put("data/one.parquet", new Uint8Array(1));
    const head = store.head.bind(store);
    const list = store.list.bind(store);
    let discoveries = 0;
    store.head = async (path) => {
      discoveries++;
      const info = await head(path);
      return info === undefined ? undefined : { ...info, rangeReadMode };
    };
    store.list = async function* (prefix, options) {
      discoveries++;
      for await (const info of list(prefix, options)) yield { ...info, rangeReadMode };
    };
    const seen: string[] = [];
    const lake = new Lake({
      store,
      planningCache: memoryCache(),
      scanner: {
        async planObjects(objects) {
          expect(objects[0]?.rangeReadMode).toBe(rangeReadMode);
          seen.push(objects[0]?.rangeReadMode ?? "missing");
          return objects;
        },
        scan() {
          throw Error("unexpected scan");
        },
      },
    });
    const query = lake.path(source);
    await query.planTasks();
    await query.planTasks();
    expect(seen).toEqual([rangeReadMode, rangeReadMode]);
    expect(discoveries).toBe(1);
  }
});
