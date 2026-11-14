import {
  type AggregateSpec,
  type Batch,
  CpuPhysicalBackend,
  MATERIALIZATION_CHARGES as c,
  detachOwnedRow,
  fanInWorkUnits,
  gatherOwnedBatch,
  LakeqlError,
  MemoryBudget,
  MemoryScope,
  materializeOwnedBatchRows,
  type ObjectStore,
  type OwnedMemory,
  type PhysicalGroupedReduce,
  type PhysicalOutputValue,
  type PhysicalReduce,
  type PhysicalReductionContinuation,
  type Row,
  selectedRowCount,
  type TaskInput,
  tryPredicateSelection,
  type Vector,
  type VectorAggregateStateSnapshots,
  type VectorGroupByStateSnapshot,
  type VectorSource,
  vectorFromSource,
  vectorTopKOwnedBatch,
} from "lakeql-core";
import { readParquetColumnBatchesFromFile } from "./column-batches.js";
import { recordRowsMatched } from "./read-metrics.js";
import { rejectUnsupportedParquetSchema } from "./schema.js";
import {
  type AggregateParquetGroupTaskOptions,
  type AggregateParquetGroupTasksOptions,
  type AggregateParquetTaskOptions,
  type AggregateParquetTasksOptions,
  aggregateScanOptions,
  aggregateTaskReadColumns,
  enforceAggregateTaskBudget,
  groupAggregateTasks,
  iterateTaskRowWindows,
  taskReadOptions,
} from "./task.js";
import { batchWithPartitionValues } from "./task-batch.js";
import { taskMetadataOwned, taskSource } from "./task-source.js";

export async function aggregateParquetTask(
  store: ObjectStore,
  task: TaskInput,
  spec: AggregateSpec,
  options: AggregateParquetTaskOptions = {},
): Promise<VectorAggregateStateSnapshots> {
  const owner = await aggregateParquetTaskOwned(store, task, spec, options);
  try {
    return owner.value;
  } finally {
    owner.release();
  }
}

export async function aggregateParquetTaskOwned(
  store: ObjectStore,
  task: TaskInput,
  spec: AggregateSpec,
  options: AggregateParquetTaskOptions = {},
): Promise<OwnedMemory<VectorAggregateStateSnapshots>> {
  return aggregateTaskSnapshotOwned(store, task, { kind: "reduce", aggregates: spec }, options);
}

async function aggregateTaskSnapshotOwned(
  store: ObjectStore,
  task: TaskInput,
  operator: PhysicalReduce,
  options: AggregateParquetTaskOptions,
): Promise<OwnedMemory<VectorAggregateStateSnapshots>>;
async function aggregateTaskSnapshotOwned(
  store: ObjectStore,
  task: TaskInput,
  operator: PhysicalGroupedReduce,
  options: AggregateParquetGroupTaskOptions,
): Promise<OwnedMemory<VectorGroupByStateSnapshot>>;
async function aggregateTaskSnapshotOwned(
  store: ObjectStore,
  task: TaskInput,
  operator: PhysicalReduce | PhysicalGroupedReduce,
  options: AggregateParquetGroupTaskOptions,
): Promise<OwnedMemory<VectorAggregateStateSnapshots | VectorGroupByStateSnapshot>> {
  const spec = operator.aggregates;
  const scope = new MemoryScope(options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER));
  options = { ...options, memoryBudget: scope.budget };
  let reducer: PhysicalReductionContinuation | undefined;
  try {
    const scanOptions = aggregateScanOptions(options);
    const file = await taskSource(store, task.path, options);
    enforceAggregateTaskBudget(scanOptions);
    const metadataScope = scope.childScope();
    const { metadata, cached } = await taskMetadataOwned(task.path, file, metadataScope, options);
    recordMetadataCache(scanOptions, cached);
    rejectUnsupportedParquetSchema(metadata, {
      allowValueCheckedScalarDecimals: true,
      memoryBudget: options.memoryBudget,
    });
    enforceAggregateTaskBudget(scanOptions);
    const physicalColumns = aggregateTaskReadColumns(
      task,
      spec,
      operator.kind === "grouped-reduce" ? operator.keys : undefined,
    );
    reducer = new CpuPhysicalBackend().openReduction(operator, {
      allocationScope: scope,
      ...(options.budget === undefined ? {} : { budget: options.budget }),
      ...(options.now === undefined ? {} : { now: options.now }),
      ...(options.startedAt === undefined ? {} : { queryStartedAt: options.startedAt }),
    });
    for (const { rowStart, rowEnd } of iterateTaskRowWindows(metadata, task, scope)) {
      const readOptions = taskReadOptions(
        rowStart,
        rowEnd,
        physicalColumns,
        task.residualPredicate,
        options,
      );
      if (scanOptions !== undefined) readOptions.stats = scanOptions.stats;
      for await (const batch of readParquetColumnBatchesFromFile(file, metadata, readOptions)) {
        const scratch = scope.childScope();
        try {
          const vectorBatch = batchWithPartitionValues(batch.batch, task.partitionValues, scratch);
          if (batch.residualPredicateSatisfied === true) {
            if (scanOptions !== undefined) {
              recordRowsMatched(scanOptions.stats, vectorBatch.rowCount);
              enforceAggregateTaskBudget(scanOptions);
            }
            await reducer.append(vectorBatch);
            continue;
          }
          const selection = tryPredicateSelection(vectorBatch, task.residualPredicate, scratch);
          if (selection === undefined) {
            throw new LakeqlError(
              "LAKEQL_UNSUPPORTED_PUSHDOWN",
              "aggregateParquetTask requires a vectorizable residual predicate",
              { path: task.path },
            );
          }
          if (scanOptions !== undefined) {
            recordRowsMatched(scanOptions.stats, selectedRowCount(vectorBatch.rowCount, selection));
            enforceAggregateTaskBudget(scanOptions);
          }
          await reducer.append(vectorBatch, selection);
        } finally {
          scratch.dispose();
        }
      }
    }
    const checkpoint = scope.adopt(await reducer.checkpoint());
    if (
      checkpoint.value.kind !== "aggregate-snapshot" &&
      checkpoint.value.kind !== "grouped-aggregate-snapshot"
    )
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Expected aggregate task checkpoint");
    if (
      (operator.kind === "grouped-reduce") !==
      (checkpoint.value.kind === "grouped-aggregate-snapshot")
    )
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Aggregate task checkpoint kind mismatch");
    return scope.retainAllocations(checkpoint.value.snapshot);
  } catch (cause) {
    if (cause instanceof LakeqlError) throw cause;
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", `Failed to read ${task.path}`, {
      path: task.path,
      cause,
    });
  } finally {
    reducer?.dispose();
    scope.dispose();
  }
}

export async function aggregateParquetTasks(
  store: ObjectStore,
  tasks: TaskInput[],
  spec: AggregateSpec,
  options: AggregateParquetTasksOptions = {},
): Promise<Record<string, unknown>> {
  const owner = await aggregateParquetTasksOwned(store, tasks, spec, options);
  try {
    return owner.value;
  } finally {
    owner.release();
  }
}

export async function aggregateParquetTasksOwned(
  store: ObjectStore,
  tasks: TaskInput[],
  spec: AggregateSpec,
  options: AggregateParquetTasksOptions = {},
): Promise<OwnedMemory<Record<string, unknown>>> {
  const rows = await aggregateTasksOwnedRows(
    tasks,
    { kind: "reduce", aggregates: spec },
    options,
    (task, budget) =>
      aggregateParquetTaskOwned(store, task, spec, { ...options, memoryBudget: budget }),
    options.partialBoundary,
  );
  const scope = new MemoryScope(rows.budget);
  try {
    scope.adopt(rows);
    const row = rows.value[0];
    if (row === undefined)
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Aggregate task returned no result");
    return scope.retainAllocations(row);
  } finally {
    scope.dispose();
  }
}

async function aggregateTasksOwnedRows<
  T extends VectorAggregateStateSnapshots | VectorGroupByStateSnapshot,
>(
  tasks: TaskInput[],
  operator: PhysicalReduce | PhysicalGroupedReduce,
  options: Omit<AggregateParquetTasksOptions, "partialBoundary">,
  runTask: (task: TaskInput, budget: MemoryBudget) => Promise<OwnedMemory<T>>,
  boundary?: (partial: T, task: TaskInput, index: number) => T | Promise<T>,
): Promise<OwnedMemory<Row[]>> {
  const scope = new MemoryScope(options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER));
  const inputs = scope.childScope();
  let reducer: PhysicalReductionContinuation | undefined;
  try {
    reducer = new CpuPhysicalBackend().openReduction(operator, {
      allocationScope: scope,
      enforceOutputRows: operator.kind === "reduce",
      ...(options.budget === undefined ? {} : { budget: options.budget }),
      ...(options.now === undefined ? {} : { now: options.now }),
      ...(options.startedAt === undefined ? {} : { queryStartedAt: options.startedAt }),
    });
    const merged = reducer;
    await fanInWorkUnits({
      memoryBudget: scope.budget,
      inputs: aggregateTaskInputs(tasks, options, inputs),
      initial: merged,
      maxConcurrentTasks: options.maxConcurrentTasks ?? 1,
      ...(options.maxBufferedPartials === undefined
        ? {}
        : { maxBufferedPartials: options.maxBufferedPartials }),
      async run(task) {
        const owner = await runTask(task, scope.budget);
        try {
          return inputs.adopt(owner);
        } catch (error) {
          owner.release();
          throw error;
        }
      },
      async boundary(owner, task, index) {
        if (boundary === undefined) return owner;
        const replacement = await boundary(owner.value, task, index);
        const detached = detachOwnedRow({ snapshot: replacement }, scope.budget);
        try {
          const retained = inputs.retainBacking(detached.value.snapshot as T, [detached]);
          owner.release();
          return retained;
        } finally {
          detached.release();
        }
      },
      async reduce(target, partial) {
        const scratch = scope.childScope();
        try {
          if (target.mergeCheckpoint === undefined)
            throw new LakeqlError(
              "LAKEQL_UNSUPPORTED_PUSHDOWN",
              "Backend lacks checkpoint merge capability",
            );
          const checkpoint = scratch.allocate(
            c.object + 2 * c.property,
            "aggregate task merge checkpoint",
            (): PhysicalOutputValue =>
              operator.kind === "grouped-reduce"
                ? {
                    kind: "grouped-aggregate-snapshot",
                    snapshot: partial.value as VectorGroupByStateSnapshot,
                  }
                : {
                    kind: "aggregate-snapshot",
                    snapshot: partial.value as VectorAggregateStateSnapshots,
                  },
          ).value;
          await target.mergeCheckpoint(checkpoint);
        } finally {
          scratch.dispose();
          partial.release();
        }
      },
    });
    return await merged.finish();
  } finally {
    reducer?.dispose();
    scope.dispose();
  }
}

export async function aggregateParquetGroupTask(
  store: ObjectStore,
  task: TaskInput,
  groupColumns: readonly string[],
  spec: AggregateSpec,
  options: AggregateParquetGroupTaskOptions = {},
): Promise<VectorGroupByStateSnapshot> {
  const owner = await aggregateParquetGroupTaskOwned(store, task, groupColumns, spec, options);
  try {
    return owner.value;
  } finally {
    owner.release();
  }
}

export async function aggregateParquetGroupTaskOwned(
  store: ObjectStore,
  task: TaskInput,
  groupColumns: readonly string[],
  spec: AggregateSpec,
  options: AggregateParquetGroupTaskOptions = {},
): Promise<OwnedMemory<VectorGroupByStateSnapshot>> {
  return aggregateTaskSnapshotOwned(
    store,
    task,
    {
      kind: "grouped-reduce",
      keys: [...groupColumns],
      aggregates: spec,
      ...(options.maxGroups === undefined ? {} : { maxGroups: options.maxGroups }),
    },
    options,
  );
}

export async function aggregateParquetGroupTasks(
  store: ObjectStore,
  tasks: TaskInput[],
  groupColumns: readonly string[],
  spec: AggregateSpec,
  options: AggregateParquetGroupTasksOptions = {},
): Promise<Row[]> {
  const owner = await aggregateParquetGroupTasksOwned(store, tasks, groupColumns, spec, options);
  try {
    return owner.value;
  } finally {
    owner.release();
  }
}

export async function aggregateParquetGroupTasksOwned(
  store: ObjectStore,
  tasks: TaskInput[],
  groupColumns: readonly string[],
  spec: AggregateSpec,
  options: AggregateParquetGroupTasksOptions = {},
): Promise<OwnedMemory<Row[]>> {
  const batch = await aggregateParquetGroupTasksBatchOwned(
    store,
    tasks,
    groupColumns,
    spec,
    options,
  );
  try {
    return materializeOwnedBatchRows(batch);
  } finally {
    batch.release();
  }
}

export async function aggregateParquetGroupTasksBatch(
  store: ObjectStore,
  tasks: TaskInput[],
  groupColumns: readonly string[],
  spec: AggregateSpec,
  options: AggregateParquetGroupTasksOptions = {},
): Promise<Batch> {
  const owner = await aggregateParquetGroupTasksBatchOwned(
    store,
    tasks,
    groupColumns,
    spec,
    options,
  );
  try {
    return owner.value;
  } finally {
    owner.release();
  }
}

export async function aggregateParquetGroupTasksBatchOwned(
  store: ObjectStore,
  tasks: TaskInput[],
  groupColumns: readonly string[],
  spec: AggregateSpec,
  options: AggregateParquetGroupTasksOptions = {},
): Promise<OwnedMemory<Batch>> {
  const rows = await aggregateTasksOwnedRows(
    tasks,
    {
      kind: "grouped-reduce",
      keys: [...groupColumns],
      aggregates: spec,
      ...(options.maxGroups === undefined ? {} : { maxGroups: options.maxGroups }),
    },
    options,
    (task, budget) =>
      aggregateParquetGroupTaskOwned(store, task, groupColumns, spec, {
        ...options,
        memoryBudget: budget,
      }),
    options.partialBoundary,
  );
  const scope = new MemoryScope(rows.budget);
  try {
    scope.adopt(rows);
    const columns = scope.allocate(
      c.object,
      "group task output columns",
      () => Object.create(null) as Record<string, Vector>,
    ).value;
    const addColumn = (name: string) => {
      if (Object.hasOwn(columns, name)) return;
      scope.allocate(
        c.property + c.string + name.length * c.stringCodeUnit,
        "group task output column",
        () => undefined,
      );
      columns[name] = vectorFromSource(
        {
          length: rows.value.length,
          get: (index) => (rows.value[index]?.[name] ?? null) as ReturnType<VectorSource["get"]>,
        },
        scope,
      );
    };
    for (const name of groupColumns) addColumn(name);
    for (const name in spec) if (Object.hasOwn(spec, name)) addColumn(name);
    const batch = scope.allocate(c.object + 2 * c.property, "group task output batch", () => ({
      rowCount: rows.value.length,
      columns,
    })).value;
    const input = scope.retainAllocations(batch);
    let output: OwnedMemory<Batch> | undefined;
    try {
      if (options.orderBy !== undefined) {
        output = vectorTopKOwnedBatch(input, options.orderBy, {
          offset: options.offset ?? 0,
          limit: options.limit ?? batch.rowCount,
        });
      } else if (options.limit === undefined && options.offset === undefined) {
        output = input.retain();
      } else {
        const offset = options.offset ?? 0;
        const limit = options.limit;
        if (!Number.isInteger(offset) || offset < 0)
          throw new LakeqlError("LAKEQL_TYPE_ERROR", "offset must be a non-negative integer", {
            offset,
          });
        if (limit !== undefined && (!Number.isInteger(limit) || limit < 0))
          throw new LakeqlError("LAKEQL_TYPE_ERROR", "limit must be a non-negative integer", {
            limit,
          });
        const start = Math.min(offset, batch.rowCount);
        const end = limit === undefined ? batch.rowCount : Math.min(start + limit, batch.rowCount);
        const indices = scope.allocate(
          c.array + (end - start) * c.arraySlot,
          "group task slice indices",
          () => Array.from({ length: end - start }, (_, index) => start + index),
        ).value;
        output = gatherOwnedBatch(input, indices);
      }
      if (
        options.budget?.maxOutputRows !== undefined &&
        output.value.rowCount > options.budget.maxOutputRows
      )
        throw new LakeqlError(
          "LAKEQL_BUDGET_EXCEEDED",
          "Grouped task result exceeds output rows budget",
        );
      const result = output;
      output = undefined;
      return result;
    } finally {
      output?.release();
      input.release();
    }
  } finally {
    scope.dispose();
  }
}

function aggregateTaskInputs(
  tasks: TaskInput[],
  options: { preserveTaskBoundaries?: boolean },
  scope: MemoryScope,
): TaskInput[] {
  return options.preserveTaskBoundaries === true ? tasks : groupAggregateTasks(tasks, scope);
}

function recordMetadataCache(
  scanOptions: ReturnType<typeof aggregateScanOptions>,
  cached: boolean,
): void {
  if (scanOptions === undefined) return;
  if (cached) scanOptions.stats.cacheHits += 1;
  else scanOptions.stats.cacheMisses += 1;
}
