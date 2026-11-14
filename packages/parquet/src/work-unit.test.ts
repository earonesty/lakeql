import { readFileSync } from "node:fs";
import {
  createVectorAggregateStates,
  fanInWorkUnits,
  gte,
  jsonWorkUnitBoundary,
  materializeBatchRows,
  memoryCache,
  memoryStore,
  type TaskInput,
} from "lakeql-core";
import { fixturePath, STATS } from "lakeql-fixtures";
import { describe, expect, it } from "vitest";
import {
  aggregateParquetGroupTasks,
  aggregateParquetGroupTasksBatch,
  aggregateParquetTasks,
  createParquetLake,
  type ParquetMetadata,
  planParquetTaskWorkUnits,
  writeParquet,
} from "./index.js";
import { countingObjectStore } from "./test-helpers.js";
import {
  asTestDeployment,
  deploymentWorkUnitInputs,
  finalizePortableAggregate,
  mergePortableAggregatePartial,
  type PortableScanPartial,
  portableAggregateTransports,
  portableScanTransports,
  sortedTestDeployments,
  type TestDeployment,
} from "./work-unit-test-helpers.js";

describe("Parquet work-unit execution", () => {
  it("splits row-group tasks into bounded JSON-portable work units", async () => {
    const store = memoryStore();
    await store.put(`data/${STATS.file}`, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({ store, queryId: () => "work-unit-split" });
    const [task] = await lake
      .path(`data/${STATS.file}`)
      .select(["id"])
      .where(gte("metric", 100))
      .planTasks();
    expect(task).toBeDefined();

    const workUnits = await planParquetTaskWorkUnits(store, task, { maxRowGroupsPerTask: 1 });
    const transported = jsonWorkUnitBoundary(workUnits);

    expect(transported).toEqual([
      {
        ...task,
        rowGroupRanges: [{ start: 1, end: 2 }],
      },
      {
        ...task,
        rowGroupRanges: [{ start: 2, end: 3 }],
      },
    ]);
    expect(workUnits[0]).not.toBe(task);
    expect(task.rowGroupRanges).toEqual([{ start: 1, end: 3 }]);
  });

  it("fans aggregate partials through browser, Cloudflare Worker, and Supabase Edge boundaries", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/browser.parquet", {
      columnData: [{ name: "metric", data: [1, 2, 3], type: "DOUBLE" }],
    });
    await writeParquet(store, "data/cloudflare.parquet", {
      columnData: [{ name: "metric", data: [4, 5], type: "DOUBLE" }],
    });
    await writeParquet(store, "data/supabase.parquet", {
      columnData: [{ name: "metric", data: [6, 7, 8, 9], type: "DOUBLE" }],
    });
    const tasks = [
      {
        path: "data/browser.parquet",
        rowGroupRanges: [{ start: 0, end: 1 }],
        projectedColumns: ["metric"],
        partitionValues: { deployment: "browser" },
      },
      {
        path: "data/cloudflare.parquet",
        rowGroupRanges: [{ start: 0, end: 1 }],
        projectedColumns: ["metric"],
        partitionValues: { deployment: "cloudflare-worker" },
      },
      {
        path: "data/supabase.parquet",
        rowGroupRanges: [{ start: 0, end: 1 }],
        projectedColumns: ["metric"],
        partitionValues: { deployment: "supabase-edge" },
      },
    ];
    const transported: TestDeployment[] = [];

    const row = await aggregateParquetTasks(
      store,
      jsonWorkUnitBoundary(tasks),
      {
        rows: { op: "count" },
        totalMetric: { op: "sum", column: "metric" },
        maxMetric: { op: "max", column: "metric" },
      },
      {
        maxConcurrentTasks: 3,
        partialBoundary(partial, task) {
          transported.push(asTestDeployment(task.partitionValues.deployment));
          return jsonWorkUnitBoundary(partial);
        },
      },
    );

    expect(row).toEqual({ rows: 9, totalMetric: 45, maxMetric: 9 });
    expect(sortedTestDeployments(transported)).toEqual([
      "browser",
      "cloudflare-worker",
      "supabase-edge",
    ]);
  });

  it("runs aggregate work units through deployment runners before generic fan-in", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/aggregate-runners.parquet", {
      rowGroupSize: [2],
      columnData: [{ name: "metric", data: [1, 2, 3, 4, 5, 6], type: "DOUBLE" }],
    });
    const lake = createParquetLake({ store, queryId: () => "aggregate-runner-work-units" });
    const query = lake.path("data/aggregate-runners.parquet").select(["metric"]);
    const [task] = await query.planTasks();
    expect(task).toBeDefined();
    const workUnits = await planParquetTaskWorkUnits(store, task, { maxRowGroupsPerTask: 1 });
    const spec = {
      rows: { op: "count" },
      totalMetric: { op: "sum", column: "metric" },
      maxMetric: { op: "max", column: "metric" },
    } as const;
    const transports = portableAggregateTransports(store, spec, { batchSize: 2 });
    const inputs = jsonWorkUnitBoundary(deploymentWorkUnitInputs(workUnits)).map(
      (input, index) => ({
        ...input,
        delayMs: index === 0 ? 250 : 0,
      }),
    );
    const completed: number[] = [];
    const reduced: TestDeployment[] = [];

    const merged = await fanInWorkUnits({
      inputs,
      initial: createVectorAggregateStates(spec),
      maxConcurrentTasks: 3,
      async run(input, index) {
        const partial = await transports[input.deployment](input, index);
        completed.push(partial.index);
        return partial;
      },
      boundary(partial) {
        return jsonWorkUnitBoundary(partial);
      },
      reduce(accumulator, partial, input, index) {
        expect(partial.index).toBe(index);
        expect(partial.deployment).toBe(input.deployment);
        expect(partial.rowGroupRanges).toEqual(input.task.rowGroupRanges);
        reduced.push(partial.deployment);
        mergePortableAggregatePartial(accumulator, partial);
      },
    });

    expect(workUnits).toHaveLength(3);
    expect(inputs.map((input) => input.task.rowGroupRanges)).toEqual([
      [{ start: 0, end: 1 }],
      [{ start: 1, end: 2 }],
      [{ start: 2, end: 3 }],
    ]);
    expect(completed[0]).not.toBe(0);
    expect([...completed].sort()).toEqual([0, 1, 2]);
    expect(sortedTestDeployments(reduced)).toEqual([
      "browser",
      "cloudflare-worker",
      "supabase-edge",
    ]);
    expect(finalizePortableAggregate(merged)).toEqual({
      rows: 6,
      totalMetric: 21,
      maxMetric: 6,
    });
  });

  it("can preserve same-file row-group work units across aggregate fan-in boundaries", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/row-groups.parquet", {
      rowGroupSize: [2],
      columnData: [{ name: "metric", data: [1, 2, 3, 4, 5, 6], type: "DOUBLE" }],
    });
    const lake = createParquetLake({ store, queryId: () => "same-file-work-unit-fan-in" });
    const [task] = await lake.path("data/row-groups.parquet").select(["metric"]).planTasks();
    expect(task).toBeDefined();
    const workUnits = await planParquetTaskWorkUnits(store, task, { maxRowGroupsPerTask: 1 });
    const transported: TaskInput["rowGroupRanges"][] = [];

    const row = await aggregateParquetTasks(
      store,
      jsonWorkUnitBoundary(workUnits),
      {
        rows: { op: "count" },
        totalMetric: { op: "sum", column: "metric" },
      },
      {
        maxConcurrentTasks: 2,
        preserveTaskBoundaries: true,
        partialBoundary(partial, task) {
          transported.push(task.rowGroupRanges);
          return jsonWorkUnitBoundary(partial);
        },
      },
    );

    expect(row).toEqual({ rows: 6, totalMetric: 21 });
    expect(workUnits).toHaveLength(3);
    expect(transported).toEqual([
      [{ start: 0, end: 1 }],
      [{ start: 1, end: 2 }],
      [{ start: 2, end: 3 }],
    ]);
  });

  it("fans grouped aggregate partials through deployment-neutral JSON boundaries", async () => {
    const store = memoryStore();
    await writeParquet(store, "data/browser-group.parquet", {
      columnData: [{ name: "metric", data: [1, 2, 3], type: "DOUBLE" }],
    });
    await writeParquet(store, "data/cloudflare-group.parquet", {
      columnData: [{ name: "metric", data: [4, 5], type: "DOUBLE" }],
    });
    await writeParquet(store, "data/supabase-group.parquet", {
      columnData: [{ name: "metric", data: [6, 7, 8, 9], type: "DOUBLE" }],
    });
    const tasks = [
      {
        path: "data/browser-group.parquet",
        rowGroupRanges: [{ start: 0, end: 1 }],
        projectedColumns: ["metric"],
        partitionValues: { deployment: "browser" },
      },
      {
        path: "data/cloudflare-group.parquet",
        rowGroupRanges: [{ start: 0, end: 1 }],
        projectedColumns: ["metric"],
        partitionValues: { deployment: "cloudflare-worker" },
      },
      {
        path: "data/supabase-group.parquet",
        rowGroupRanges: [{ start: 0, end: 1 }],
        projectedColumns: ["metric"],
        partitionValues: { deployment: "supabase-edge" },
      },
    ];
    const transported: TestDeployment[] = [];

    const rows = await aggregateParquetGroupTasks(
      store,
      jsonWorkUnitBoundary(tasks),
      ["deployment"],
      {
        rows: { op: "count" },
        totalMetric: { op: "sum", column: "metric" },
        maxMetric: { op: "max", column: "metric" },
      },
      {
        maxConcurrentTasks: 3,
        partialBoundary(partial, task) {
          transported.push(asTestDeployment(task.partitionValues.deployment));
          return jsonWorkUnitBoundary(partial);
        },
      },
    );

    expect(rows).toEqual([
      { deployment: "browser", rows: 3, totalMetric: 6, maxMetric: 3 },
      { deployment: "cloudflare-worker", rows: 2, totalMetric: 9, maxMetric: 5 },
      { deployment: "supabase-edge", rows: 4, totalMetric: 30, maxMetric: 9 },
    ]);
    expect(sortedTestDeployments(transported)).toEqual([
      "browser",
      "cloudflare-worker",
      "supabase-edge",
    ]);

    const orderedBatch = await aggregateParquetGroupTasksBatch(
      store,
      jsonWorkUnitBoundary(tasks),
      ["deployment"],
      {
        rows: { op: "count" },
        totalMetric: { op: "sum", column: "metric" },
      },
      {
        orderBy: [{ column: "totalMetric", direction: "desc" }],
        limit: 2,
        maxConcurrentTasks: 3,
      },
    );

    expect(orderedBatch.columns.totalMetric?.type).toBe("f64");
    expect(materializeBatchRows(orderedBatch)).toEqual([
      { deployment: "supabase-edge", rows: 4, totalMetric: 30 },
      { deployment: "cloudflare-worker", rows: 2, totalMetric: 9 },
    ]);
  });

  it("fans vector scan work units through deployment-neutral runners", async () => {
    const store = memoryStore();
    await store.put(`data/${STATS.file}`, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({ store, queryId: () => "vector-scan-work-units" });
    const query = lake.path(`data/${STATS.file}`).select(["id", "metric"]);
    const expected = await query.toArray();
    const [task] = await query.planTasks();
    expect(task).toBeDefined();
    const workUnits = await planParquetTaskWorkUnits(store, task, { maxRowGroupsPerTask: 1 });
    const deployments = portableScanTransports(store, { batchSize: 4 });
    const transportedInputs = jsonWorkUnitBoundary(deploymentWorkUnitInputs(workUnits)).map(
      (input, index) => ({ ...input, delayMs: index === 0 ? 100 : 0 }),
    );
    const completed: number[] = [];
    const transportedPartials: TestDeployment[] = [];

    const rows = await fanInWorkUnits({
      inputs: transportedInputs,
      initial: [] as PortableScanPartial["rows"],
      maxConcurrentTasks: 3,
      maxBufferedPartials: 3,
      async run(input, index) {
        const partial = await deployments[input.deployment](input, index);
        completed.push(partial.index);
        return partial;
      },
      boundary(partial) {
        transportedPartials.push(partial.deployment);
        return jsonWorkUnitBoundary(partial);
      },
      reduce(accumulator, partial, input, index) {
        expect(partial.index).toBe(index);
        expect(partial.deployment).toBe(input.deployment);
        expect(partial.rowGroupRanges).toEqual(input.task.rowGroupRanges);
        accumulator.push(...partial.rows);
      },
    });

    expect(workUnits).toHaveLength(3);
    expect(transportedInputs.map((input) => input.task.rowGroupRanges)).toEqual([
      [{ start: 0, end: 1 }],
      [{ start: 1, end: 2 }],
      [{ start: 2, end: 3 }],
    ]);
    expect(completed[0]).not.toBe(0);
    expect([...completed].sort()).toEqual([0, 1, 2]);
    expect(sortedTestDeployments(transportedPartials)).toEqual([
      "browser",
      "cloudflare-worker",
      "supabase-edge",
    ]);
    expect(rows).toEqual(expected);
  });

  it("keeps scan metadata caches runtime-local instead of serializing them into work units", async () => {
    const metadataCache = memoryCache<ParquetMetadata>();
    const taskStore = countingObjectStore(memoryStore());
    await taskStore.put(`data/${STATS.file}`, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({
      store: taskStore,
      metadataCache,
      queryId: () => "scan-work-unit-metadata-cache",
    });
    const query = lake.path(`data/${STATS.file}`).select(["id", "metric"]);
    const [task] = await query.planTasks();
    expect(task).toBeDefined();
    const [input] = jsonWorkUnitBoundary(
      deploymentWorkUnitInputs(
        await planParquetTaskWorkUnits(taskStore, task, { maxRowGroupsPerTask: 1 }),
      ),
    );
    expect(input).toBeDefined();

    const uncachedTransports = portableScanTransports(taskStore, { batchSize: 4 });
    taskStore.resetCounters();
    const uncached = await uncachedTransports.browser(input, 0);
    const uncachedRanges = taskStore.counters.getRange;
    expect(uncachedRanges).toBeGreaterThan(0);

    const cachedTransports = portableScanTransports(taskStore, { batchSize: 4, metadataCache });
    taskStore.resetCounters();
    const cached = await cachedTransports.browser(input, 0);

    expect(cached).toEqual(uncached);
    expect(taskStore.counters.getRange).toBeGreaterThan(0);
    expect(taskStore.counters.getRange).toBeLessThan(uncachedRanges);
    expect(containsRuntimeHandle(input)).toBe(false);
  });

  it("keeps deployment work-unit envelopes JSON-only runtime data", async () => {
    const store = memoryStore();
    await store.put(`data/${STATS.file}`, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({ store, queryId: () => "portable-work-unit-envelope" });
    const [task] = await lake
      .path(`data/${STATS.file}`)
      .select(["id"])
      .where(gte("metric", 100))
      .planTasks();
    expect(task).toBeDefined();

    const workUnits = await planParquetTaskWorkUnits(store, task, { maxRowGroupsPerTask: 1 });
    const envelope = deploymentWorkUnitInputs(workUnits);
    const transported = JSON.parse(JSON.stringify(envelope));

    expect(transported).toEqual(envelope);
    expect(containsRuntimeHandle(transported)).toBe(false);
  });

  it("executes the same Parquet work-unit contract across browser, Worker, and Edge transports", async () => {
    const store = memoryStore();
    await store.put(`data/${STATS.file}`, readFileSync(fixturePath(STATS.file)));
    const lake = createParquetLake({ store, queryId: () => "deployment-transport-work-units" });
    const query = lake.path(`data/${STATS.file}`).select(["id", "metric"]);
    const expected = await query.toArray();
    const [task] = await query.planTasks();
    expect(task).toBeDefined();
    const workUnits = await planParquetTaskWorkUnits(store, task, { maxRowGroupsPerTask: 1 });
    const inputs = deploymentWorkUnitInputs(workUnits);
    const transports = portableScanTransports(store, { batchSize: 4 });
    const transportedInputs = jsonWorkUnitBoundary(inputs);
    const reducedDeployments: TestDeployment[] = [];

    const rows = await fanInWorkUnits({
      inputs: transportedInputs,
      initial: [] as PortableScanPartial["rows"],
      maxConcurrentTasks: 3,
      async run(input, index) {
        return transports[input.deployment](input, index);
      },
      boundary(partial) {
        return jsonWorkUnitBoundary(partial);
      },
      reduce(accumulator, partial, input, index) {
        expect(partial.index).toBe(index);
        expect(partial.deployment).toBe(input.deployment);
        expect(partial.rowGroupRanges).toEqual(input.task.rowGroupRanges);
        reducedDeployments.push(partial.deployment);
        accumulator.push(...partial.rows);
      },
    });

    expect(rows).toEqual(expected);
    expect(sortedTestDeployments(reducedDeployments)).toEqual([
      "browser",
      "cloudflare-worker",
      "supabase-edge",
    ]);
  });
});

function containsRuntimeHandle(value: unknown): boolean {
  if (typeof value === "function" || value instanceof Uint8Array || value instanceof ArrayBuffer) {
    return true;
  }
  if (value === null || typeof value !== "object") return false;
  if (Array.isArray(value)) return value.some(containsRuntimeHandle);
  return Object.values(value).some(containsRuntimeHandle);
}

it("budgets work-unit metadata and releases it after row-limit planning", async () => {
  const { MemoryBudget } = await import("lakeql-core");
  const store = memoryStore();
  await writeParquet(store, "planning.parquet", {
    rowGroupSize: [2, 2],
    columnData: [{ name: "id", type: "INT32", data: [1, 2, 3, 4] }],
  });
  const task: TaskInput = {
    path: "planning.parquet",
    rowGroupRanges: [{ start: 0, end: 2 }],
    partitionValues: {},
  };
  const pool = new MemoryBudget(128 * 1024);
  const units = await planParquetTaskWorkUnits(store, task, {
    maxRowsPerTask: 2,
    memoryBudget: pool,
  });
  expect(units.map((unit) => unit.rowGroupRanges)).toEqual([
    [{ start: 0, end: 1 }],
    [{ start: 1, end: 2 }],
  ]);
  expect(pool.peakBytes).toBeGreaterThan(0);
  expect(pool.usedBytes).toBe(0);
  await expect(
    planParquetTaskWorkUnits(store, task, { maxRowsPerTask: 1, memoryBudget: pool }),
  ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
  expect(pool.usedBytes).toBe(0);
  const tight = new MemoryBudget(1);
  await expect(
    planParquetTaskWorkUnits(store, task, { maxRowsPerTask: 2, memoryBudget: tight }),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(tight.usedBytes).toBe(0);
});
