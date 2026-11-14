import {
  col,
  eq,
  gt,
  jsonWorkUnitBoundary,
  like,
  MemoryBudget,
  materializeBatchRows,
  memoryStore,
  type TaskInput,
} from "lakeql-core";
import { describe, expect, it } from "vitest";
import {
  aggregateParquetGroupTask,
  aggregateParquetGroupTaskOwned,
  aggregateParquetGroupTasks,
  aggregateParquetGroupTasksBatch,
  aggregateParquetGroupTasksOwned,
  aggregateParquetTask,
  aggregateParquetTasks,
  aggregateParquetTasksOwned,
  createParquetLake,
  scanParquetTaskBatches,
  scanParquetTaskColumnBatches,
  windowParquetTask,
  windowParquetTasks,
  windowParquetTasksOwned,
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

  it("uses planned bucket counts and budget checks in direct window tasks", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/task-window-direct.parquet", {
      rowGroupSize: [2],
      columnData: [
        { name: "id", data: [1, 2, 3, 4], type: "INT32" },
        { name: "region", data: ["west", "east", "west", "east"], type: "STRING" },
      ],
    });
    const task: TaskInput = {
      path: "data/task-window-direct.parquet",
      rowGroupRanges: [{ start: 0, end: 2 }],
      projectedColumns: ["id", "region"],
      partitionValues: {},
      window: {
        topology: "window-partition-fanout",
        available: true,
        bucketCount: 4,
        partitionBy: [col("region")],
      },
    };
    const windows = {
      rn: {
        fn: "row_number" as const,
        args: [],
        over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
      },
    };
    const partial = await windowParquetTask(store, task, windows, 0);
    expect(partial.bucketCount).toBe(4);

    await expect(
      windowParquetTask(store, task, windows, 0, { budget: { maxBufferedRows: 1 } }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    await expect(
      windowParquetTask(store, task, windows, 0, {
        budget: { maxElapsedMs: 1 },
        now: () => 10,
        startedAt: 0,
      }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
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

it("shares the caller allocation budget with window indexes after task fan-in", async () => {
  const store = memoryStore();
  const keys = Array.from({ length: 800 }, (_, index) => index);
  await writeParquet(store, "adaptive-window.parquet", {
    rowGroupSize: [400, 400],
    columnData: [
      { name: "key", data: keys },
      { name: "before", data: keys.map((key) => (key % 2 ? key : 0)) },
    ],
  });
  const tasks: TaskInput[] = [0, 1].map((index) => ({
    path: "adaptive-window.parquet",
    rowGroupRanges: [{ start: index, end: index + 1 }],
    projectedColumns: ["key", "before"],
    partitionValues: {},
  }));
  const windows = {
    total: {
      fn: { aggregate: "sum" },
      args: [col("key")],
      over: {
        partitionBy: [],
        orderBy: [{ expr: col("key") }],
        frame: {
          mode: "groups",
          start: { kind: "preceding", offset: col("before") },
          end: { kind: "current-row" },
        },
      },
    },
  } satisfies Record<string, import("lakeql-core").WindowExpr>;
  for (const cap of [4 * 1024 * 1024, 1024 * 1024, 3204, 3203]) {
    const pool = new MemoryBudget(cap);
    const result = windowParquetTasks(store, tasks, windows, {
      memoryBudget: pool,
      maxConcurrentTasks: 2,
    });
    if (cap < 4 * 1024 * 1024)
      await expect(result).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    else {
      const output = await result;
      expect(output.map((row) => row.total)).toEqual(
        keys.map((key) => (key % 2 ? (key * (key + 1)) / 2 : key)),
      );
      expect(pool.peakBytes).toBeGreaterThan(3204);
    }
    expect(pool.peakBytes).toBeLessThanOrEqual(cap);
    expect(pool.usedBytes).toBe(0);
  }
});

it.each([
  false,
  true,
])("retains task storage across a delayed transport boundary (failure=%s)", async (fail) => {
  const store = memoryStore();
  await writeParquet(store, "owned-window.parquet", {
    rowGroupSize: [2],
    columnData: [{ name: "key", data: [1, 2] }],
  });
  const tasks: TaskInput[] = [
    {
      path: "owned-window.parquet",
      rowGroupRanges: [{ start: 0, end: 1 }],
      projectedColumns: ["key"],
      partitionValues: {},
    },
  ];
  const windows = {
    rn: { fn: "row_number", args: [], over: { partitionBy: [], orderBy: [{ expr: col("key") }] } },
  } satisfies Record<string, import("lakeql-core").WindowExpr>;
  const pool = new MemoryBudget(1024 * 1024);
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let entered!: () => void;
  const ready = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const pending = windowParquetTasksOwned(store, tasks, windows, {
    memoryBudget: pool,
    async partialBoundary(partial) {
      entered();
      await gate;
      if (fail) throw new Error("transport failed");
      return jsonWorkUnitBoundary(partial);
    },
  });
  const outcome = pending.then(
    (value) => ({ value }),
    (error: unknown) => ({ error }),
  );
  await ready;
  expect(pool.usedBytes).toBeGreaterThan(0);
  expect(() => pool.reserve(pool.availableBytes + 1, "competing job")).toThrow(/budget/i);
  release();
  const result = await outcome;
  if ("error" in result) {
    expect(fail).toBe(true);
    expect(result.error).toEqual(new Error("transport failed"));
  } else {
    expect(fail).toBe(false);
    expect(result.value.value.map((row) => row.rn)).toEqual([1, 2]);
    expect(pool.usedBytes).toBeGreaterThan(0);
    result.value.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it.each([64, 128])("shares a %s MiB pool across concurrent owned window jobs", async (mib) => {
  const store = memoryStore();
  const count = 256;
  await writeParquet(store, "concurrent-owned-window.parquet", {
    rowGroupSize: [64, 64, 64, 64],
    columnData: [
      { name: "key", data: Array.from({ length: count }, (_, index) => index) },
      {
        name: "payload",
        data: Array.from({ length: count }, (_, index) => `${index}:${"x".repeat(4096)}`),
      },
    ],
  });
  const tasks: TaskInput[] = [
    {
      path: "concurrent-owned-window.parquet",
      rowGroupRanges: [{ start: 0, end: 4 }],
      projectedColumns: ["key", "payload"],
      partitionValues: {},
    },
  ];
  const windows = {
    rn: { fn: "row_number", args: [], over: { partitionBy: [], orderBy: [{ expr: col("key") }] } },
  } satisfies Record<string, import("lakeql-core").WindowExpr>;
  const pool = new MemoryBudget(mib * 1024 * 1024);
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let arrived!: () => void;
  const ready = new Promise<void>((resolve) => {
    arrived = resolve;
  });
  let waiting = 0;
  const jobs = Array.from({ length: 4 }, () =>
    windowParquetTasksOwned(store, tasks, windows, {
      memoryBudget: pool,
      batchSize: 32,
      async partialBoundary(partial) {
        if (++waiting === 4) arrived();
        await gate;
        return partial;
      },
    }),
  );
  // A failing job must unblock this barrier so test failures cannot hang peers.
  const all = Promise.all(jobs);
  const failure = all.catch((error: unknown) => {
    release();
    throw error;
  });
  await Promise.race([ready, failure]);
  expect(pool.usedBytes).toBeGreaterThan(4 * count * 4096);
  expect(() => pool.reserve(pool.availableBytes + 1, "competing allocation")).toThrow(/budget/i);
  release();
  const outputs = await failure;
  try {
    for (const output of outputs) {
      expect(output.value).toHaveLength(count);
      expect(output.value[255]?.rn).toBe(256);
      expect(output.value[255]?.payload).toBe(`255:${"x".repeat(4096)}`);
    }
    const held = pool.usedBytes;
    expect(held).toBeGreaterThan(0);
    outputs[0]?.release();
    expect(pool.usedBytes).toBeLessThan(held);
    expect(pool.usedBytes).toBeGreaterThan(0);
    expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
  } finally {
    for (const output of outputs) output.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it("uses immutable backing reuse when window task headroom is tight", async () => {
  const store = memoryStore();
  const payloads = Array.from({ length: 64 }, (_, id) => `${id}:${"x".repeat(16384)}`);
  await writeParquet(store, "tight-window.parquet", {
    rowGroupSize: Array(8).fill(8),
    columnData: [
      { name: "id", data: payloads.map((_, id) => id) },
      { name: "payload", data: payloads },
    ],
  });
  const pool = new MemoryBudget(4 * 1024 * 1024);
  const result = await windowParquetTasksOwned(
    store,
    [
      {
        path: "tight-window.parquet",
        rowGroupRanges: [{ start: 0, end: 8 }],
        projectedColumns: ["id", "payload"],
        partitionValues: {},
      },
    ],
    {
      rn: { fn: "row_number", args: [], over: { partitionBy: [], orderBy: [{ expr: col("id") }] } },
    },
    { memoryBudget: pool, batchSize: 8 },
  );
  try {
    expect(result.value.map((row) => row.payload)).toEqual(payloads);
    expect(result.value[63]?.rn).toBe(64);
    expect(pool.usedBytes).toBeGreaterThan(2 * 1024 * 1024);
    expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
  } finally {
    result.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("owns aggregate partials across delayed transport (failure=%s)", async (fail) => {
  const store = memoryStore();
  await writeParquet(store, "owned-aggregate.parquet", {
    rowGroupSize: [2, 2],
    columnData: [{ name: "value", type: "DOUBLE", data: [1, null, 3, 7] }],
  });
  const tasks: TaskInput[] = [0, 1].map((index) => ({
    path: "owned-aggregate.parquet",
    rowGroupRanges: [{ start: index, end: index + 1 }],
    partitionValues: {},
  }));
  const pool = new MemoryBudget(1024 * 1024);
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let entered!: () => void;
  const ready = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const pending = aggregateParquetTasksOwned(
    store,
    tasks,
    {
      middle: { op: "median", column: "value" },
      total: { op: "sum", column: "value" },
      n: { op: "count", column: "value" },
    },
    {
      memoryBudget: pool,
      maxConcurrentTasks: 2,
      preserveTaskBoundaries: true,
      async partialBoundary(partial, _task, index) {
        if (index === 0) {
          entered();
          await gate;
          if (fail) throw new Error("aggregate transport failed");
        }
        return jsonWorkUnitBoundary(partial);
      },
    },
  );
  const outcome = pending.then(
    (value) => ({ value }),
    (error: unknown) => ({ error }),
  );
  await ready;
  expect(pool.usedBytes).toBeGreaterThan(0);
  expect(() => pool.reserve(pool.availableBytes + 1, "competing allocation")).toThrow(/budget/i);
  release();
  const result = await outcome;
  if ("error" in result) {
    expect(fail).toBe(true);
    expect(result.error).toEqual(new Error("aggregate transport failed"));
  } else {
    expect(fail).toBe(false);
    expect(result.value.value).toEqual({ middle: 3, total: 11, n: 3 });
    expect(pool.usedBytes).toBeGreaterThan(0);
    result.value.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "aggregate",
  "window",
] as const)("admits in-place transport mutations for %s tasks", async (kind) => {
  const store = memoryStore();
  await writeParquet(store, "mutated-partial.parquet", {
    rowGroupSize: [1],
    columnData: [{ name: "value", type: "DOUBLE", data: [1] }],
  });
  const tasks: TaskInput[] = [
    {
      path: "mutated-partial.parquet",
      rowGroupRanges: [{ start: 0, end: 1 }],
      partitionValues: {},
    },
  ];
  const pool = new MemoryBudget(65536);
  const pending =
    kind === "aggregate"
      ? aggregateParquetTasksOwned(
          store,
          tasks,
          { middle: { op: "median", column: "value" } },
          {
            memoryBudget: pool,
            partialBoundary(partial) {
              const middle = partial.middle;
              if (middle?.op !== "median") throw new Error("Missing median checkpoint");
              middle.values = Array(10000).fill(1);
              return partial;
            },
          },
        )
      : windowParquetTasksOwned(
          store,
          tasks,
          { rn: { fn: "row_number", args: [], over: { partitionBy: [], orderBy: [] } } },
          {
            memoryBudget: pool,
            partialBoundary(partial) {
              const row = partial.buckets[0]?.rows[0]?.row;
              if (!row) throw new Error("Missing partial row");
              row.payload = "x".repeat(100000);
              return partial;
            },
          },
        );
  await expect(pending).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(pool.usedBytes).toBe(0);
});

it("retains grouped task checkpoints and cleans up group-limit rejection", async () => {
  const store = memoryStore();
  await writeParquet(store, "owned-group-task.parquet", {
    rowGroupSize: [2, 2],
    columnData: [
      { name: "g", type: "STRING", data: ["a", null, "a", "b"] },
      { name: "value", type: "DOUBLE", data: [1, 2, 3, 4] },
    ],
  });
  const task: TaskInput = {
    path: "owned-group-task.parquet",
    rowGroupRanges: [{ start: 0, end: 2 }],
    partitionValues: { region: "west" },
  };
  const spec = { total: { op: "sum" as const, column: "value" }, n: { op: "count" as const } };
  const pool = new MemoryBudget(1024 * 1024);
  const owner = await aggregateParquetGroupTaskOwned(store, task, ["g", "region"], spec, {
    memoryBudget: pool,
    batchSize: 1,
  });
  try {
    expect(
      owner.value.groups.map((group) => ({ keys: group.keyValues, states: group.states })),
    ).toEqual([
      {
        keys: ["a", "west"],
        states: { total: { op: "sum", sum: 4 }, n: { op: "count", count: 2 } },
      },
      {
        keys: [null, "west"],
        states: { total: { op: "sum", sum: 2 }, n: { op: "count", count: 1 } },
      },
      {
        keys: ["b", "west"],
        states: { total: { op: "sum", sum: 4 }, n: { op: "count", count: 1 } },
      },
    ]);
    expect(pool.usedBytes).toBeGreaterThan(0);
  } finally {
    owner.release();
  }
  expect(pool.usedBytes).toBe(0);
  await expect(
    aggregateParquetGroupTaskOwned(store, task, ["g"], spec, { memoryBudget: pool, maxGroups: 2 }),
  ).rejects.toMatchObject({ code: "LAKEQL_GROUP_LIMIT_EXCEEDED" });
  expect(pool.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("owns grouped fan-in through transport and final shaping (failure=%s)", async (fail) => {
  const store = memoryStore();
  await writeParquet(store, "owned-group-fanin.parquet", {
    rowGroupSize: [2, 2],
    columnData: [
      { name: "g", type: "STRING", data: ["a", "b", "a", "b"] },
      { name: "value", type: "DOUBLE", data: [1, 2, 3, 4] },
    ],
  });
  const tasks: TaskInput[] = [0, 1].map((index) => ({
    path: "owned-group-fanin.parquet",
    rowGroupRanges: [{ start: index, end: index + 1 }],
    partitionValues: {},
  }));
  const pool = new MemoryBudget(1024 * 1024);
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let entered!: () => void;
  const ready = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const pending = aggregateParquetGroupTasksOwned(
    store,
    tasks,
    ["g"],
    { total: { op: "sum", column: "value" } },
    {
      memoryBudget: pool,
      budget: { maxOutputRows: 1 },
      orderBy: [{ column: "total", direction: "desc" }],
      offset: 1,
      limit: 1,
      preserveTaskBoundaries: true,
      maxConcurrentTasks: 2,
      async partialBoundary(partial, _task, index) {
        if (index === 0) {
          entered();
          await gate;
          if (fail) throw new Error("group transport failed");
        }
        return jsonWorkUnitBoundary(partial);
      },
    },
  );
  const outcome = pending.then(
    (value) => ({ value }),
    (error: unknown) => ({ error }),
  );
  await ready;
  expect(pool.usedBytes).toBeGreaterThan(0);
  release();
  const result = await outcome;
  if ("error" in result) {
    expect(fail).toBe(true);
    expect(result.error).toEqual(new Error("group transport failed"));
  } else {
    expect(fail).toBe(false);
    try {
      expect(result.value.value).toEqual([{ g: "a", total: 4 }]);
      expect(pool.usedBytes).toBeGreaterThan(0);
    } finally {
      result.value.release();
    }
  }
  expect(pool.usedBytes).toBe(0);
});
