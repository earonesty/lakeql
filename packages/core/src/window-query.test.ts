import { describe, expect, it } from "vitest";
import { col } from "./expr.js";
import type { WindowExpr } from "./window.js";
import {
  compatibleWindowSortGroups,
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
