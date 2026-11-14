import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { flushSortRun, mergeSortRuns, normalizeOrderBy } from "./query-sort.js";

it("admits merge scratch and output overlap exactly, before reading sort keys", () => {
  const runs = [[{ key: 1 }, { key: 3 }], [], [{ key: 2 }]];
  const order = [{ column: "key" }];
  const measured = new MemoryBudget(65536);
  const scope = new MemoryScope(measured);
  expect(mergeSortRuns(runs, order, scope)).toEqual([{ key: 1 }, { key: 2 }, { key: 3 }]);
  const required = measured.peakBytes;
  scope.dispose();
  for (const cap of [required - 1, required]) {
    const budget = new MemoryBudget(cap);
    const target = new MemoryScope(budget);
    if (cap < required) {
      let reads = 0;
      const source = runs.map((run) =>
        run.map((row) => ({
          get key() {
            reads++;
            return row.key;
          },
        })),
      );
      expect(() => mergeSortRuns(source, order, target)).toThrow(/budget/i);
      expect(reads).toBe(0);
      expect(budget.usedBytes).toBe(0);
    } else expect(mergeSortRuns(runs, order, target).map((row) => row.key)).toEqual([1, 2, 3]);
    target.dispose();
    expect(budget.usedBytes).toBe(0);
  }
});

it("denies run output before mutation and releases scratch after comparator failure", () => {
  const budget = new MemoryBudget(1);
  const scope = new MemoryScope(budget);
  const current = [{ key: 2 }, { key: 1 }];
  expect(() => flushSortRun([], current, [{ column: "key" }], scope)).toThrow(/budget/i);
  expect(current.map((row) => row.key)).toEqual([2, 1]);
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
  const failureBudget = new MemoryBudget(65536);
  const failureScope = new MemoryScope(failureBudget);
  expect(() => mergeSortRuns([[{}], [{}]], [{ column: "missing" }], failureScope)).toThrow(
    /Unknown column/,
  );
  expect(failureBudget.usedBytes).toBe(0);
  failureScope.dispose();
});

it("admits normalized ordering metadata at exact capacity", () => {
  const terms = [{ column: "雪".repeat(100) }, { column: "second", direction: "desc" as const }];
  const measured = new MemoryBudget(65536);
  const scope = new MemoryScope(measured);
  expect(normalizeOrderBy(terms, scope)).toEqual(normalizeOrderBy(terms));
  const required = measured.peakBytes;
  scope.dispose();
  for (const cap of [required - 1, required]) {
    const pool = new MemoryBudget(cap + 17);
    const sibling = pool.reserve(17, "sibling");
    const target = new MemoryScope(pool.child(cap));
    if (cap < required) expect(() => normalizeOrderBy(terms, target)).toThrow(/budget/i);
    else expect(normalizeOrderBy(terms, target)).toEqual(normalizeOrderBy(terms));
    target.dispose();
    expect(pool.usedBytes).toBe(17);
    sibling.release();
  }
});
