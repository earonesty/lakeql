import {
  add,
  and,
  between,
  col,
  eq,
  fn,
  gt,
  isIn,
  isNull,
  like,
  lit,
  memoryCache,
  not,
  or,
  type QueryStats,
  type TaskInput,
} from "lakeql-core";
import { describe, expect, it } from "vitest";
import {
  aggregateScanOptions,
  aggregateTaskReadColumns,
  appendRowGroupRange,
  cloneTaskWithRanges,
  enforceAggregateTaskBudget,
  groupAggregateTasks,
  taskReadOptions,
  taskRowWindows,
  validateTaskWorkUnitOptions,
} from "./task.js";

describe("Parquet task planning contracts", () => {
  it("builds task read options from vector scan inputs without empty-column noise", () => {
    expect(taskReadOptions(2, 8, undefined, undefined, {})).toEqual({
      rowStart: 2,
      rowEnd: 8,
    });
    expect(taskReadOptions(2, 8, [], undefined, { batchSize: 3 })).toEqual({
      rowStart: 2,
      rowEnd: 8,
      batchSize: 3,
    });

    const stats = queryStats();
    const predicate = gt("amount", 10);
    expect(taskReadOptions(2, 8, ["amount"], predicate, { batchSize: 3, stats })).toEqual({
      rowStart: 2,
      rowEnd: 8,
      batchSize: 3,
      columns: ["amount"],
      where: predicate,
      stats,
    });
  });

  it("derives row windows from finite row-group ranges and skips invalid spans", () => {
    expect(
      taskRowWindows(
        { row_groups: [{ num_rows: 10 }, { num_rows: 5 }, { num_rows: 7 }] },
        {
          path: "data.parquet",
          rowGroupRanges: [
            { start: -1, end: 1 },
            { start: 0, end: 2 },
            { start: 2, end: 9 },
            { start: 3, end: 4 },
            { start: 2, end: 2 },
          ],
          partitionValues: {},
        },
      ),
    ).toEqual([
      { rowStart: 0, rowEnd: 15 },
      { rowStart: 15, rowEnd: 22 },
    ]);
  });

  it("validates task work-unit budgets as explicit positive integers", () => {
    expect(() => validateTaskWorkUnitOptions({})).toThrowError(
      expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }),
    );
    for (const maxRowGroupsPerTask of [0, 1.5]) {
      expect(() => validateTaskWorkUnitOptions({ maxRowGroupsPerTask })).toThrowError(
        expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }),
      );
    }
    for (const maxRowsPerTask of [0, 1.5]) {
      expect(() => validateTaskWorkUnitOptions({ maxRowsPerTask })).toThrowError(
        expect.objectContaining({ code: "LAKEQL_TYPE_ERROR" }),
      );
    }
    expect(() =>
      validateTaskWorkUnitOptions({ maxRowGroupsPerTask: 1, maxRowsPerTask: 10 }),
    ).not.toThrow();
  });

  it("clones and merges row-group ranges without aliasing caller state", () => {
    const ranges: TaskInput["rowGroupRanges"] = [];
    appendRowGroupRange(ranges, 1);
    appendRowGroupRange(ranges, 2);
    appendRowGroupRange(ranges, 4);
    expect(ranges).toEqual([
      { start: 1, end: 3 },
      { start: 4, end: 5 },
    ]);

    const task: TaskInput = {
      path: "data.parquet",
      size: 10,
      etag: "abc",
      rowGroupCount: 5,
      rowGroupRanges: ranges,
      projectedColumns: ["amount"],
      residualPredicate: eq("tenant", "a"),
      partitionValues: { tenant: "a" },
    };
    const clone = cloneTaskWithRanges(task, [{ start: 0, end: 1 }]);
    clone.partitionValues.tenant = "b";
    clone.projectedColumns?.push("extra");
    clone.rowGroupRanges[0].end = 2;

    expect(task.partitionValues).toEqual({ tenant: "a" });
    expect(task.projectedColumns).toEqual(["amount"]);
    expect(task.rowGroupRanges).toEqual(ranges);

    const grouped = groupAggregateTasks([
      { ...task, rowGroupRanges: [{ start: 4, end: 6 }] },
      { ...task, rowGroupRanges: [{ start: 1, end: 3 }] },
      { ...task, rowGroupRanges: [{ start: 3, end: 5 }] },
      { ...task, etag: "other", rowGroupRanges: [{ start: 9, end: 10 }] },
    ]);
    expect(grouped).toEqual([
      { ...task, rowGroupRanges: [{ start: 1, end: 6 }] },
      { ...task, etag: "other", rowGroupRanges: [{ start: 9, end: 10 }] },
    ]);
  });

  it("collects physical aggregate columns across expression families and partitions", () => {
    const task: TaskInput = {
      path: "data.parquet",
      rowGroupRanges: [{ start: 0, end: 1 }],
      partitionValues: { tenant: "acme", p_region: "west" },
      residualPredicate: and(
        or(eq("tenant", "acme"), isIn("status", ["ok", "late"])),
        not(between("amount", 10, add(col("limit"), 2))),
        like("label", "A%"),
      ),
    };
    const spec = {
      rows: { op: "count" },
      total: { op: "sum", column: "amount" },
      fallback: { op: "max", expr: fn("coalesce", col("fallback"), lit(0)) },
      bucketed: {
        op: "count",
        expr: {
          kind: "case",
          whens: [{ when: isNull("closed_at"), value: col("opened_at") }],
          else: col("closed_at"),
        },
      },
    } as const;

    expect(aggregateTaskReadColumns(task, spec, ["p_region", "group_key"])).toEqual([
      "amount",
      "closed_at",
      "fallback",
      "group_key",
      "label",
      "limit",
      "opened_at",
      "status",
    ]);

    expect(
      aggregateTaskReadColumns(
        { path: "data.parquet", rowGroupRanges: [], partitionValues: { tenant: "acme" } },
        { rows: { op: "count" } },
        ["tenant"],
      ),
    ).toBeUndefined();
  });

  it("creates aggregate scan options only when stats or budgets are active", () => {
    expect(aggregateScanOptions({})).toBeUndefined();

    const stats = queryStats();
    const withStats = aggregateScanOptions({ stats });
    expect(withStats?.stats).toBe(stats);
    expect(withStats?.batchSize).toBe(4096);
    expect(withStats?.budget).toEqual({});

    const withBudget = aggregateScanOptions({ batchSize: 128, budget: { maxBytes: 10 } });
    expect(withBudget?.batchSize).toBe(128);
    expect(withBudget?.stats).toMatchObject({ queryId: "aggregate-parquet-task" });
  });

  it("enforces every aggregate task budget counter with actionable error details", () => {
    expect(() => enforceAggregateTaskBudget(undefined)).not.toThrow();

    const counters: Array<[keyof QueryStats, string, number]> = [
      ["bytesRequested", "bytes", 11],
      ["rangeRequests", "range requests", 3],
      ["rowsDecoded", "rows decoded", 5],
    ];
    for (const [field, metric, actual] of counters) {
      const stats = queryStats();
      stats[field] = actual;
      expect(() =>
        enforceAggregateTaskBudget({
          batchSize: 1,
          stats,
          budget:
            field === "bytesRequested"
              ? { maxBytes: actual - 1 }
              : field === "rangeRequests"
                ? { maxRangeRequests: actual - 1 }
                : { maxRowsDecoded: actual - 1 },
          now: () => 0,
          startedAt: 0,
        }),
      ).toThrowError(
        expect.objectContaining({
          code: "LAKEQL_BUDGET_EXCEEDED",
          details: { metric, limit: actual - 1, actual },
        }),
      );
    }

    expect(() =>
      enforceAggregateTaskBudget({
        batchSize: 1,
        stats: queryStats(),
        budget: { maxElapsedMs: 9 },
        now: () => 10,
        startedAt: 0,
      }),
    ).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: { metric: "elapsed milliseconds", limit: 9, actual: 10 },
      }),
    );
  });

  it("accepts metadata cache handles in work-unit validation shapes", () => {
    expect(() =>
      validateTaskWorkUnitOptions({ maxRowsPerTask: 1, metadataCache: memoryCache() }),
    ).not.toThrow();
  });
});

function queryStats(): QueryStats {
  return {
    queryId: "task-test",
    elapsedMs: 0,
    manifestsRead: 0,
    manifestsSkipped: 0,
    filesPlanned: 0,
    filesRead: 0,
    filesSkipped: 0,
    rowGroupsRead: 0,
    rowGroupsSkipped: 0,
    columnsRead: [],
    bytesRequested: 0,
    rangeRequests: 0,
    rowsDecoded: 0,
    rowsMatched: 0,
    rowsReturned: 0,
    cacheHits: 0,
    cacheMisses: 0,
  };
}

it("streams row windows with constant live storage and lazy metadata traversal", async () => {
  const { iterateTaskRowWindows } = await import("./task.js");
  const { MemoryBudget, MemoryScope } = await import("lakeql-core");
  let reads = 0;
  const metadata = {
    row_groups: Array.from({ length: 1000 }, () => ({
      get num_rows() {
        reads++;
        return 2;
      },
    })),
  };
  const task = {
    path: "data",
    partitionValues: {},
    rowGroupRanges: Array.from({ length: 1000 }, (_, start) => ({ start, end: start + 1 })),
  };
  const budget = new MemoryBudget(1024);
  const scope = new MemoryScope(budget);
  const iterator = iterateTaskRowWindows(metadata, task, scope);
  expect(reads).toBe(0);
  expect(iterator.next().value).toEqual({ rowStart: 0, rowEnd: 2 });
  expect(reads).toBe(1);
  const held = budget.usedBytes;
  expect(held).toBeGreaterThan(0);
  let count = 1;
  for (const window of iterator) {
    expect(window).toEqual({ rowStart: count * 2, rowEnd: (count + 1) * 2 });
    expect(budget.usedBytes).toBe(held);
    count++;
  }
  expect(count).toBe(1000);
  expect(reads).toBe(1000);
  expect(budget.usedBytes).toBe(0);
  const early = iterateTaskRowWindows(metadata, task, scope);
  early.next();
  early.return?.();
  expect(budget.usedBytes).toBe(0);
  expect([
    ...iterateTaskRowWindows(
      metadata,
      {
        ...task,
        rowGroupRanges: [
          { start: 3, end: 5 },
          { start: 1, end: 4 },
          { start: 0, end: 1 },
        ],
      },
      scope,
    ),
  ]).toEqual([
    { rowStart: 6, rowEnd: 10 },
    { rowStart: 2, rowEnd: 8 },
    { rowStart: 0, rowEnd: 2 },
  ]);
  scope.dispose();
});

it.each([
  1024,
  64 * 1024,
])("adapts backward row-window traversal within %i bytes", async (capacity) => {
  const { iterateTaskRowWindows } = await import("./task.js");
  const { MemoryBudget, MemoryScope } = await import("lakeql-core");
  let reads = 0;
  const metadata = {
    row_groups: Array.from({ length: 1000 }, () => ({
      get num_rows() {
        reads++;
        return 2;
      },
    })),
  };
  const task = {
    path: "data",
    partitionValues: {},
    rowGroupRanges: Array.from({ length: 20 }, (_, i) => ({ start: 999 - i, end: 1000 - i })),
  };
  const pool = new MemoryBudget(capacity);
  const scope = new MemoryScope(pool);
  const windows = [...iterateTaskRowWindows(metadata, task, scope)];
  expect(windows).toEqual(
    task.rowGroupRanges.map(({ start, end }) => ({ rowStart: start * 2, rowEnd: end * 2 })),
  );
  expect(reads).toBe(capacity === 1024 ? 19810 : 2000);
  expect(pool.peakBytes).toBeLessThanOrEqual(capacity);
  expect(pool.usedBytes).toBe(0);
  const iterator = iterateTaskRowWindows(metadata, task, scope);
  iterator.next();
  iterator.next();
  expect(pool.usedBytes).toBeGreaterThan(0);
  iterator.return?.();
  expect(pool.usedBytes).toBe(0);
  scope.dispose();
});

it("rechecks shared headroom between row windows and releases an admitted index on failure", async () => {
  const { iterateTaskRowWindows } = await import("./task.js");
  const { MemoryBudget, MemoryScope } = await import("lakeql-core");
  const pool = new MemoryBudget(64 * 1024);
  const scope = new MemoryScope(pool);
  let reads = 0;
  let fail = false;
  const metadata = {
    row_groups: Array.from({ length: 1000 }, () => ({
      get num_rows() {
        if (fail) throw new Error("metadata unavailable");
        reads++;
        return 1;
      },
    })),
  };
  const task = {
    path: "data",
    partitionValues: {},
    rowGroupRanges: [
      { start: 999, end: 1000 },
      { start: 998, end: 999 },
      { start: 997, end: 998 },
    ],
  };
  const iterator = iterateTaskRowWindows(metadata, task, scope);
  iterator.next();
  const competing = pool.reserve(pool.availableBytes - 1024, "competing job");
  expect(iterator.next().value).toEqual({ rowStart: 998, rowEnd: 999 });
  expect(reads).toBe(1999);
  competing.release();
  expect(iterator.next().value).toEqual({ rowStart: 997, rowEnd: 998 });
  expect(reads).toBe(2999);
  expect(pool.usedBytes).toBeGreaterThan(8000);
  iterator.return?.();
  expect(pool.usedBytes).toBe(0);
  const failing = iterateTaskRowWindows(metadata, task, scope);
  failing.next();
  fail = true;
  expect(() => failing.next()).toThrow("metadata unavailable");
  expect(pool.usedBytes).toBe(0);
  const reuse = pool.reserve(pool.maxBytes, "next job");
  reuse.release();
  scope.dispose();
});

it("combines fragmented task ranges once without mutating input ranges", () => {
  const tasks: TaskInput[] = Array.from({ length: 1000 }, (_, index) => ({
    path: "data",
    partitionValues: {},
    rowGroupRanges: [Object.freeze({ start: (999 - index) * 2, end: (999 - index) * 2 + 1 })],
  }));
  const grouped = groupAggregateTasks(tasks);
  expect(grouped).toHaveLength(1);
  expect(grouped[0]?.rowGroupRanges).toEqual(
    Array.from({ length: 1000 }, (_, index) => ({ start: index * 2, end: index * 2 + 1 })),
  );
  expect(tasks[0]?.rowGroupRanges).toEqual([{ start: 1998, end: 1999 }]);
  expect(grouped[0]?.rowGroupRanges[0]).not.toBe(tasks[999]?.rowGroupRanges[0]);
  const overlapping = groupAggregateTasks([
    ...tasks,
    { path: "data", partitionValues: {}, rowGroupRanges: [{ start: 0, end: 2000 }] },
  ]);
  expect(overlapping[0]?.rowGroupRanges).toEqual([{ start: 0, end: 2000 }]);
});
