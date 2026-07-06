import { describe, expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { col } from "./expr.js";
import { applyWindowsToVectorBatches } from "./window-vector.js";

describe("applyWindowsToVectorBatches", () => {
  it("evaluates partitioned windows across selected vector batches", async () => {
    const rows = await applyWindowsToVectorBatches(
      [
        {
          batch: batchFromColumns({
            event_id: [1, 2, 3],
            account: ["a", "a", "b"],
            amount: [10, 20, 5],
            keep: [true, false, true],
          }),
          selection: new Uint8Array([1, 0, 1]),
        },
        {
          batch: batchFromColumns({
            event_id: [4, 5, 6],
            account: ["a", "b", "b"],
            amount: [40, 15, 25],
            keep: [true, true, true],
          }),
          selection: new Uint8Array([1, 1, 1]),
        },
      ],
      {
        account_rank: {
          fn: "row_number",
          args: [],
          over: { partitionBy: [col("account")], orderBy: [{ expr: col("event_id") }] },
        },
        running_amount: {
          fn: { aggregate: "sum" },
          args: [col("amount")],
          over: { partitionBy: [col("account")], orderBy: [{ expr: col("event_id") }] },
        },
      },
    );

    expect(
      rows.map((row) => ({
        event_id: row.event_id,
        account: row.account,
        account_rank: row.account_rank,
        running_amount: row.running_amount,
      })),
    ).toEqual([
      { event_id: 1, account: "a", account_rank: 1, running_amount: 10 },
      { event_id: 3, account: "b", account_rank: 1, running_amount: 5 },
      { event_id: 4, account: "a", account_rank: 2, running_amount: 50 },
      { event_id: 5, account: "b", account_rank: 2, running_amount: 20 },
      { event_id: 6, account: "b", account_rank: 3, running_amount: 45 },
    ]);
  });

  it("enforces operator callbacks for buffered rows, partitions, and memory", async () => {
    const bufferedRows: number[] = [];
    const partitionRows: number[] = [];
    const memoryValues: unknown[] = [];
    const rows = await applyWindowsToVectorBatches(
      [
        {
          batch: batchFromColumns({
            event_id: [1, 2, 3],
            account: ["a", "a", "a"],
            amount: [10, 20, 30],
          }),
        },
      ],
      {
        running_amount: {
          fn: { aggregate: "sum" },
          args: [col("amount")],
          over: { partitionBy: [col("account")], orderBy: [{ expr: col("event_id") }] },
        },
      },
      {
        enforceBufferedRows: (rows) => bufferedRows.push(rows),
        enforceWindowPartitionRows: (rows) => partitionRows.push(rows),
        estimateMemoryBytes: (value) => {
          memoryValues.push(value);
          return 128;
        },
        enforceMemoryBytes: (bytes) => memoryValues.push(bytes),
      },
    );

    expect(rows.map((row) => row.running_amount)).toEqual([10, 30, 60]);
    expect(bufferedRows).toEqual([3]);
    expect(partitionRows).toEqual([3]);
    expect(memoryValues).toHaveLength(2);
    expect(memoryValues).toContain(128);
  });
});
