import {
  col,
  eq,
  gt,
  jsonWorkUnitBoundary,
  like,
  materializeBatchRows,
  memoryStore,
  type TaskInput,
} from "lakeql-core";
import { describe, expect, it } from "vitest";
import {
  aggregateParquetGroupTask,
  aggregateParquetGroupTasks,
  aggregateParquetGroupTasksBatch,
  aggregateParquetTask,
  aggregateParquetTasks,
  createParquetLake,
  scanParquetTaskBatches,
  scanParquetTaskColumnBatches,
  windowParquetTasks,
  writeParquet,
} from "./index.js";
import { testQueryStats } from "./test-helpers.js";

describe("Parquet task execution", () => {
  it("scans row and column task batches across projection, partition, and residual paths", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/task-scan.parquet", {
      rowGroupSize: [3],
      columnData: [
        { name: "id", data: [1, 2, 3, 4, 5, 6], type: "INT32" },
        { name: "amount", data: [5, 10, 15, 20, 25, 30], type: "DOUBLE" },
        { name: "label", data: ["a", "b", "c", "d", "e", "f"], type: "STRING" },
      ],
    });

    const baseTask: TaskInput = {
      path: "data/task-scan.parquet",
      rowGroupRanges: [{ start: 0, end: 2 }],
      projectedColumns: ["id", "amount"],
      residualPredicate: gt("amount", 18),
      partitionValues: {},
    };
    const rowBatches = [];
    for await (const batch of scanParquetTaskBatches(store, baseTask, { batchSize: 2 })) {
      rowBatches.push(...batch);
    }
    expect(rowBatches).toEqual([
      { id: 4, amount: 20 },
      { id: 5, amount: 25 },
      { id: 6, amount: 30 },
    ]);

    const vectorStats = testQueryStats();
    const vectorBatches = [];
    for await (const batch of scanParquetTaskColumnBatches(store, baseTask, {
      batchSize: 3,
      stats: vectorStats,
    })) {
      vectorBatches.push(...materializeBatchRows(batch.batch));
    }
    expect(vectorBatches).toEqual([
      { id: 4, amount: 20 },
      { id: 5, amount: 25 },
      { id: 6, amount: 30 },
    ]);
    expect(vectorStats.rowsMatched).toBe(3);

    const partitionTask: TaskInput = {
      path: "data/task-scan.parquet",
      rowGroupRanges: [{ start: 0, end: 1 }],
      projectedColumns: ["country", "id"],
      residualPredicate: eq("country", "US"),
      partitionValues: { country: "US" },
    };
    const partitionRows = [];
    for await (const batch of scanParquetTaskColumnBatches(store, partitionTask, {
      batchSize: 2,
      stats: testQueryStats(),
    })) {
      partitionRows.push(...materializeBatchRows(batch.batch));
    }
    expect(partitionRows).toEqual([
      { country: "US", id: 1 },
      { country: "US", id: 2 },
      { country: "US", id: 3 },
    ]);

    const passthroughRows = [];
    for await (const batch of scanParquetTaskColumnBatches(
      store,
      {
        path: "data/task-scan.parquet",
        rowGroupRanges: [{ start: 0, end: 1 }],
        projectedColumns: ["id"],
        partitionValues: {},
      },
      { batchSize: 10, stats: testQueryStats() },
    )) {
      passthroughRows.push(...materializeBatchRows(batch.batch));
    }
    expect(passthroughRows).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
  });

  it("runs window functions over repartitioned Parquet work units", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/task-window.parquet", {
      rowGroupSize: [2],
      columnData: [
        { name: "id", data: [1, 2, 3, 4, 5, 6], type: "INT32" },
        { name: "region", data: ["west", "east", "west", "east", "west", "east"], type: "STRING" },
        { name: "amount", data: [10, 20, 30, 40, 50, 60], type: "DOUBLE" },
      ],
    });
    const tasks: TaskInput[] = [
      {
        path: "data/task-window.parquet",
        rowGroupRanges: [{ start: 0, end: 1 }],
        projectedColumns: ["id", "region", "amount"],
        partitionValues: {},
      },
      {
        path: "data/task-window.parquet",
        rowGroupRanges: [{ start: 1, end: 2 }],
        projectedColumns: ["id", "region", "amount"],
        partitionValues: {},
      },
      {
        path: "data/task-window.parquet",
        rowGroupRanges: [{ start: 2, end: 3 }],
        projectedColumns: ["id", "region", "amount"],
        partitionValues: {},
      },
    ];
    const windows = {
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
    const plannedTasks = tasks.map((task) => ({
      ...task,
      window: {
        topology: "window-partition-fanout" as const,
        available: true as const,
        bucketCount: 3,
        partitionBy: [col("region")],
      },
    }));
    let partials = 0;
    const fanOut = await windowParquetTasks(store, plannedTasks, windows, {
      maxConcurrentTasks: 2,
      partialBoundary(partial) {
        partials += 1;
        expect(partial.bucketCount).toBe(3);
        return jsonWorkUnitBoundary(partial);
      },
    });
    expect(partials).toBe(3);

    const lake = createParquetLake({ store });
    const materialized = await lake
      .path("data/task-window.parquet")
      .window("rn", windows.rn)
      .window("running", windows.running)
      .toArray();
    expect(fanOut).toEqual(materialized);
    expect(fanOut).toEqual([
      { id: 1, region: "west", amount: 10, rn: 1, running: 10 },
      { id: 2, region: "east", amount: 20, rn: 1, running: 20 },
      { id: 3, region: "west", amount: 30, rn: 2, running: 40 },
      { id: 4, region: "east", amount: 40, rn: 2, running: 60 },
      { id: 5, region: "west", amount: 50, rn: 3, running: 90 },
      { id: 6, region: "east", amount: 60, rn: 3, running: 120 },
    ]);
  });

  it("uses window work-unit fan-out for normal partitioned window queries", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/query-window.parquet", {
      rowGroupSize: [2],
      columnData: [
        { name: "id", data: [1, 2, 3, 4], type: "INT32" },
        { name: "region", data: ["west", "east", "west", "east"], type: "STRING" },
        { name: "amount", data: [10, 20, 30, 40], type: "DOUBLE" },
      ],
    });
    const lake = createParquetLake({ store });
    const query = lake
      .path("data/query-window.parquet")
      .window("rn", {
        fn: "row_number",
        args: [],
        over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
      })
      .window("running", {
        fn: { aggregate: "sum" },
        args: [col("amount")],
        over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
      })
      .qualify(gt("rn", 1))
      .select(["id", "region", "running"])
      .orderBy([{ column: "id" }]);

    await expect(query.explain()).resolves.toMatchObject({
      json: { windowPlan: { execution: "parquet-work-unit-fanout" } },
    });
    const result = query.run();
    await expect(result.toArray()).resolves.toEqual([
      { id: 3, region: "west", running: 40 },
      { id: 4, region: "east", running: 60 },
    ]);
    expect(result.stats).toMatchObject({
      rowsDecoded: 4,
      rowsMatched: 4,
      rowsReturned: 2,
    });
  });

  it("aggregates task batches with vector residuals, partition groups, and result options", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/task-aggregate.parquet", {
      rowGroupSize: [3],
      columnData: [
        { name: "id", data: [1, 2, 3, 4, 5, 6], type: "INT32" },
        { name: "amount", data: [5, 10, 15, 20, 25, 30], type: "DOUBLE" },
        { name: "region", data: ["west", "west", "east", "west", "east", "west"], type: "STRING" },
      ],
    });
    const tasks: TaskInput[] = [
      {
        path: "data/task-aggregate.parquet",
        rowGroupRanges: [{ start: 0, end: 2 }],
        projectedColumns: ["amount", "region"],
        residualPredicate: gt("amount", 10),
        partitionValues: {},
      },
    ];

    await expect(
      aggregateParquetTasks(
        store,
        tasks,
        { rows: { op: "count" }, total: { op: "sum", column: "amount" } },
        { stats: testQueryStats() },
      ),
    ).resolves.toEqual({ rows: 4, total: 90 });

    await expect(
      aggregateParquetTask(
        store,
        [
          {
            ...tasks[0],
            residualPredicate: like("region", "w%"),
          },
        ][0],
        { rows: { op: "count" } },
      ),
    ).rejects.toMatchObject({ code: "LAKEQL_UNSUPPORTED_PUSHDOWN" });

    const partitionTask: TaskInput = {
      path: "data/task-aggregate.parquet",
      rowGroupRanges: [{ start: 0, end: 2 }],
      projectedColumns: ["amount"],
      partitionValues: { country: "US" },
    };
    const grouped = await aggregateParquetGroupTask(
      store,
      partitionTask,
      ["country"],
      { rows: { op: "count" }, total: { op: "sum", column: "amount" } },
      { stats: testQueryStats() },
    );
    expect(grouped.groups).toHaveLength(1);
    await expect(
      aggregateParquetGroupTasks(store, [partitionTask], ["country"], {
        rows: { op: "count" },
        total: { op: "sum", column: "amount" },
      }),
    ).resolves.toEqual([{ country: "US", rows: 6, total: 105 }]);

    const optionTasks: TaskInput[] = [
      {
        path: "data/task-aggregate.parquet",
        rowGroupRanges: [{ start: 0, end: 2 }],
        projectedColumns: ["amount", "region"],
        partitionValues: {},
      },
    ];
    await expect(
      materializeBatchRows(
        await aggregateParquetGroupTasksBatch(
          store,
          optionTasks,
          ["region"],
          { rows: { op: "count" }, total: { op: "sum", column: "amount" } },
          { orderBy: [{ column: "total", direction: "desc" }], offset: 1 },
        ),
      ),
    ).toEqual([{ region: "east", rows: 2, total: 40 }]);

    await expect(
      materializeBatchRows(
        await aggregateParquetGroupTasksBatch(
          store,
          optionTasks,
          ["region"],
          { rows: { op: "count" }, total: { op: "sum", column: "amount" } },
          { offset: 1, limit: 1 },
        ),
      ),
    ).toEqual([{ region: "east", rows: 2, total: 40 }]);

    await expect(
      aggregateParquetGroupTasksBatch(
        store,
        optionTasks,
        ["region"],
        { rows: { op: "count" } },
        { offset: -1 },
      ),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
    await expect(
      aggregateParquetGroupTasksBatch(
        store,
        optionTasks,
        ["region"],
        { rows: { op: "count" } },
        { limit: -1 },
      ),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
  });
});
