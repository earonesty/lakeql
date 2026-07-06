import { describe, expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { col } from "./expr.js";
import type { QueryStats, Row } from "./types.js";
import {
  collectWindowRuns,
  executeWindowBatches,
  type WindowRunnerOptions,
} from "./window-runner.js";

describe("window runner", () => {
  it("falls back to row run collection when vector predicates are unsupported", async () => {
    const rowsByPath: Record<string, Row[]> = {
      table: [
        { id: 1, account: "a", amount: 10 },
        { id: 2, account: "a", amount: 20 },
        { id: 3, account: "a", amount: 30 },
      ],
    };
    const options = runnerOptions(rowsByPath, { maxBufferedRows: 2 });
    const runs: Row[][] = [];

    await expect(collectWindowRuns(options, runs, 0)).resolves.toBe("row-materialized");

    expect(runs.map((run) => run.map((row) => row.id))).toEqual([[1, 2], [3]]);
    expect(options.stats.rowsDecoded).toBe(3);
    expect(options.stats.rowsMatched).toBe(3);
  });

  it("executes planned window task rows through post-window processing", async () => {
    const options = runnerOptions({}, {});
    options.config.scanner.executeWindowTasks = async () => [
      { id: 1, account: "a", running: 10 },
      { id: 2, account: "a", running: 30 },
    ];
    options.config.select = ["running"];
    options.tasksFromObjects = async () => [
      {
        path: "table",
        size: 3,
        rowGroupRanges: [{ start: 0, end: 1 }],
        partitionValues: {},
        window: {
          topology: "window-partition-fanout",
          available: true,
          bucketCount: 2,
          partitionBy: [col("account")],
        },
      },
    ];

    const batches: Row[][] = [];
    for await (const batch of executeWindowBatches(options)) batches.push(batch);

    expect(batches).toEqual([[{ running: 10 }, { running: 30 }]]);
    expect(options.stats.filesPlanned).toBe(1);
    expect(options.stats.filesRead).toBe(1);
    expect(options.stats.rowsReturned).toBe(2);
  });
});

function runnerOptions(
  rowsByPath: Record<string, Row[]>,
  budget: WindowRunnerOptions["config"]["budget"],
) {
  const stats = queryStats();
  const scanner: WindowRunnerOptions["config"]["scanner"] = {
    async *scan(path, options) {
      const rows = rowsByPath[path] ?? [];
      yield rows.map((row) => {
        if (options.columns === undefined) return row;
        return Object.fromEntries(
          options.columns.filter((column) => column in row).map((column) => [column, row[column]]),
        );
      });
    },
    async *scanVectorBatches(path) {
      const rows = rowsByPath[path] ?? [];
      yield {
        rowOffset: 0,
        batch: batchFromColumns({
          id: rows.map((row) => row.id),
          account: rows.map((row) => row.account),
          amount: rows.map((row) => row.amount),
        }),
      };
    },
  };
  return {
    config: {
      from: "table",
      scanner,
      budget,
      now: () => 0,
      windows: {
        running: {
          fn: { aggregate: "sum" },
          args: [col("amount")],
          over: { partitionBy: [col("account")], orderBy: [{ expr: col("id") }] },
        },
      },
    },
    stats,
    planObjects: async () => ({
      planned: [{ path: "table", size: 3 }],
      skipped: 0,
    }),
    tasksFromObjects: async () => [],
    vectorBatchScanner: (target) => target.scanVectorBatches?.bind(target),
    columnarBatchSize: (batchSize) => batchSize ?? 4096,
    parseHivePartitions: () => ({}),
    enforceBudget: () => {},
    enforceBufferedRows: () => {},
    enforceWindowPartitionRows: () => {},
    enforceMemoryBytes: () => {},
    estimateMemoryBytes: () => 1,
    batchExecution: {
      project: (row, select) =>
        select === undefined
          ? row
          : Object.fromEntries(
              select.filter((column) => column in row).map((column) => [column, row[column]]),
            ),
      compareRows: () => 0,
      addDistinctRow: () => true,
      vectorExprSupported: () => false,
    },
  } satisfies WindowRunnerOptions;
}

function queryStats(): QueryStats {
  return {
    queryId: "test",
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
