import { describe, expect, it } from "vitest";
import { col } from "./expr.js";
import type { WindowExpr } from "./window.js";
import {
  evaluateWindowWorkUnitPartials,
  partitionWindowWorkUnitRows,
  type WindowWorkUnitPartial,
  type WindowWorkUnitRow,
  windowPartitionBucket,
} from "./window-work-unit.js";

const windows: Record<string, WindowExpr> = {
  rn: {
    fn: "row_number",
    args: [],
    over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
  },
  running: {
    fn: { aggregate: "sum" },
    args: [col("amount")],
    over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
  },
};

describe("window work units", () => {
  it("repartitions rows by window partition key and restores source order after evaluation", async () => {
    const firstRows: WindowWorkUnitRow[] = [
      { ordinal: { task: 0, row: 0 }, row: { id: 2, region: "east", amount: 20 } },
      { ordinal: { task: 0, row: 1 }, row: { id: 1, region: "west", amount: 10 } },
    ];
    const secondRows: WindowWorkUnitRow[] = [
      { ordinal: { task: 1, row: 0 }, row: { id: 4, region: "east", amount: 40 } },
      { ordinal: { task: 1, row: 1 }, row: { id: 3, region: "west", amount: 30 } },
    ];
    const partials = [
      partitionWindowWorkUnitRows(firstRows, windows, 3),
      partitionWindowWorkUnitRows(secondRows, windows, 3),
    ];

    expect(
      partials.flatMap((partial) => partial.buckets).every((bucket) => bucket.rows.length > 0),
    ).toBe(true);
    expect(windowPartitionBucket({ id: 1, region: "west", amount: 10 }, windows, 3)).toBe(
      windowPartitionBucket({ id: 3, region: "west", amount: 30 }, windows, 3),
    );

    await expect(
      evaluateWindowWorkUnitPartials(partials, windows, { bucketCount: 3 }),
    ).resolves.toEqual([
      { id: 2, region: "east", amount: 20, rn: 1, running: 20 },
      { id: 1, region: "west", amount: 10, rn: 1, running: 10 },
      { id: 4, region: "east", amount: 40, rn: 2, running: 60 },
      { id: 3, region: "west", amount: 30, rn: 2, running: 40 },
    ]);
  });

  it("rejects invalid bucket shapes with typed errors", async () => {
    expect(() => windowPartitionBucket({ region: "west" }, windows, 0)).toThrow(
      "bucket count must be a positive integer",
    );

    const mismatched: WindowWorkUnitPartial[] = [
      { bucketCount: 2, buckets: [] },
      { bucketCount: 3, buckets: [] },
    ];
    await expect(
      evaluateWindowWorkUnitPartials(mismatched, windows, { bucketCount: 2 }),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
  });
});
