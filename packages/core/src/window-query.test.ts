import { describe, expect, it } from "vitest";
import { col } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { Lake } from "./query.js";
import type { WindowExpr } from "./window.js";
import {
  compatibleWindowSortGroups,
  windowReadColumns,
  windowSortGroupCount,
  windowTaskPlanForWindows,
} from "./window-query.js";

describe("window query planning", () => {
  it("uses the longest prefix-compatible sort spec as the physical sort", () => {
    const short = rowNumber([col("account")], [col("event_day")]);
    const long = rowNumber([col("account")], [col("event_day"), col("event_id")]);

    const groups = compatibleWindowSortGroups([
      { alias: "short", expr: short },
      { alias: "long", expr: long },
    ]);

    expect(groups).toHaveLength(1);
    expect(groups[0]?.sortExpr).toBe(long);
    expect(windowSortGroupCount({ short, long })).toBe(1);
  });

  it("explains unavailable fan-out for mixed and incompatible partition specs", () => {
    const global = rowNumber([], [col("event_id")]);
    const byAccount = rowNumber([col("account")], [col("event_id")]);
    const byRegion = rowNumber([col("region")], [col("event_id")]);

    expect(windowTaskPlanForWindows({ global, byAccount }, 8)).toEqual({
      topology: "window-partition-fanout",
      available: false,
      bucketCount: 1,
      reason: "window query mixes global and partitioned windows",
    });
    expect(windowTaskPlanForWindows({ byAccount, byRegion }, 8)).toEqual({
      topology: "window-partition-fanout",
      available: false,
      bucketCount: 1,
      reason: "window partition specs are not identical",
    });
  });
});

function rowNumber(partitionBy: WindowExpr["over"]["partitionBy"], orderExprs: WindowExpr["args"]) {
  return {
    fn: "row_number",
    args: [],
    over: {
      partitionBy,
      orderBy: orderExprs.map((expr) => ({ expr })),
    },
  } satisfies WindowExpr;
}

it("prunes window source columns only with an explicit bounded projection", () => {
  const windows = { rank: rowNumber([], [col("key")]) };
  for (const select of [undefined, [], ["*"]]) {
    expect(
      windowReadColumns({ windows, ...(select === undefined ? {} : { select }) }),
    ).toBeUndefined();
  }
  expect(windowReadColumns({ windows, select: ["payload", "rank"] })).toEqual(["key", "payload"]);
  expect(windowReadColumns({ windows, projections: { renamed: col("payload") } })).toBeUndefined();
});

it("admits deduplicated window columns and excludes only own aliases", () => {
  const windows = Object.assign(Object.create({ inherited: rowNumber([], [col("ignored")]) }), {
    rank: rowNumber([col("constructor")], [col("key")]),
  }) as Record<string, WindowExpr>;
  const run = (capacity: number) => {
    const pool = new MemoryBudget(capacity);
    const scope = new MemoryScope(pool);
    try {
      expect(
        windowReadColumns({ select: ["rank", "key", "key", "inherited", "列"], windows }, scope),
      ).toEqual(["constructor", "inherited", "key", "列"]);
      return pool.peakBytes;
    } finally {
      scope.dispose();
      expect(pool.usedBytes).toBe(0);
    }
  };
  const peak = run(65536);
  expect(run(peak)).toBe(peak);
  expect(() => run(peak - 1)).toThrow();
});

it("preserves prototype-named physical columns in window scans", async () => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(65536);
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan(_path, options) {
        expect(options.columns).toEqual(["constructor"]);
        yield [{ constructor: 2 }, { constructor: 1 }];
      },
    },
  });
  const rows = await lake
    .path("table")
    .select(["constructor", "rank"])
    .window("rank", rowNumber([], [col("constructor")]))
    .toArray();
  expect(rows).toEqual([
    { constructor: 2, rank: 2 },
    { constructor: 1, rank: 1 },
  ]);
  expect(pool.usedBytes).toBe(0);
});
