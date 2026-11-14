import { describe, expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { col } from "./expr.js";
import type { QueryStats, Row } from "./types.js";
import {
  collectWindowMatches,
  collectWindowRuns,
  executeWindowBatches,
  type WindowRunnerOptions,
} from "./window-runner.js";

describe("window runner", () => {
  it("accounts window runs incrementally and resets after each flush", async () => {
    const rows = [
      { id: 1, account: "é😀", amount: 10 },
      { id: 2, account: "a", amount: 20 },
      { id: 3, account: "b", amount: 30 },
    ];
    const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).byteLength;
    const options: WindowRunnerOptions = runnerOptions(
      { table: rows },
      { maxBufferedRows: 2, maxMemoryBytes: 1000 },
    );
    let itemMeasurements = 0;
    options.estimateBufferItemBytes = (row) => {
      itemMeasurements += 1;
      return bytes([row]) - 2;
    };
    options.estimateMemoryBytes = () => {
      throw new Error("Unexpected whole-buffer measurement");
    };
    const measured: number[] = [];
    options.enforceMemoryBytes = (_budget, size) => {
      measured.push(size);
    };
    const runs: Row[][] = [];
    await collectWindowRuns(options, runs, 0);
    expect(runs).toEqual([rows.slice(0, 2), rows.slice(2)]);
    expect(itemMeasurements).toBe(3);
    expect(measured).toEqual([
      bytes(rows.slice(0, 1)),
      bytes(rows.slice(0, 2)),
      bytes(rows.slice(2)),
    ]);
  });

  it("measures restored window rows once and skips optional accounting without a budget", async () => {
    const rows = Array.from({ length: 1000 }, (_, id) => ({ id, account: "a", amount: id }));
    const options: WindowRunnerOptions = runnerOptions(
      { table: rows },
      { maxMemoryBytes: 1_000_000 },
    );
    let measurements = 0;
    options.estimateBufferItemBytes = (row) => {
      measurements += 1;
      return new TextEncoder().encode(JSON.stringify([row])).byteLength - 2;
    };
    options.estimateMemoryBytes = () => {
      throw new Error("whole-buffer measurement");
    };
    const restored = [{ id: -1, account: "a", amount: 0 }];
    await collectWindowMatches(options, restored, 0);
    expect(measurements).toBe(1001);
    expect(restored).toHaveLength(1001);
    const unbudgeted: WindowRunnerOptions = runnerOptions({ table: rows }, {});
    unbudgeted.estimateMemoryBytes = options.estimateMemoryBytes;
    unbudgeted.estimateBufferItemBytes = () => {
      throw new Error("item measurement");
    };
    const matched: Row[] = [];
    await collectWindowMatches(unbudgeted, matched, 0);
    expect(matched).toEqual(rows);
  });

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
      createDistinct: () => ({ add: () => true }),
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
