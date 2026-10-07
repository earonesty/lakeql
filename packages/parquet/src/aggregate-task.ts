import {
  type AggregateSpec,
  type Batch,
  createVectorAggregateStates,
  createVectorGroupByState,
  fanInWorkUnits,
  finalizeVectorAggregateStates,
  finalizeVectorGroupByBatch,
  gatherBatch,
  LakeqlError,
  materializeBatchRows,
  mergeVectorAggregateStateSnapshots,
  mergeVectorGroupByStates,
  type ObjectStore,
  type Row,
  restoreVectorGroupByState,
  selectedRowCount,
  snapshotVectorAggregateStates,
  snapshotVectorGroupByState,
  type TaskInput,
  tryPredicateSelection,
  updateVectorAggregateStates,
  updateVectorGroupByState,
  type Vector,
  type VectorAggregateStateSnapshots,
  type VectorGroupByStateSnapshot,
  vectorFromValues,
  vectorOrderByBatch,
  vectorTopKBatch,
} from "lakeql-core";
import { readParquetColumnBatchesFromFile } from "./column-batches.js";
import { readCachedParquetMetadata } from "./metadata-cache.js";
import { recordRowsMatched } from "./read-metrics.js";
import { rejectUnsupportedParquetSchema } from "./schema.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import {
  type AggregateParquetGroupTaskOptions,
  type AggregateParquetGroupTasksOptions,
  type AggregateParquetTaskOptions,
  type AggregateParquetTasksOptions,
  aggregateScanOptions,
  aggregateTaskReadColumns,
  aggregateVectorOptions,
  enforceAggregateTaskBudget,
  groupAggregateTasks,
  taskReadOptions,
  taskRowWindows,
} from "./task.js";

export async function aggregateParquetTask(
  store: ObjectStore,
  task: TaskInput,
  spec: AggregateSpec,
  options: AggregateParquetTaskOptions = {},
): Promise<VectorAggregateStateSnapshots> {
  try {
    const scanOptions = aggregateScanOptions(options);
    const file = await asyncBufferFromStore(store, task.path, scanOptions);
    enforceAggregateTaskBudget(scanOptions);
    const { metadata, cached } = await readCachedParquetMetadata(
      task.path,
      file,
      options.metadataCache,
    );
    recordMetadataCache(scanOptions, cached);
    rejectUnsupportedParquetSchema(metadata, { allowValueCheckedInt64Decimals: true });
    enforceAggregateTaskBudget(scanOptions);
    const physicalColumns = aggregateTaskReadColumns(task, spec);
    const aggregateOptions = aggregateVectorOptions(options);
    const states = createVectorAggregateStates(spec, aggregateOptions);
    for (const { rowStart, rowEnd } of taskRowWindows(metadata, task)) {
      const readOptions = taskReadOptions(
        rowStart,
        rowEnd,
        physicalColumns,
        task.residualPredicate,
        options,
      );
      if (scanOptions !== undefined) readOptions.stats = scanOptions.stats;
      for await (const batch of readParquetColumnBatchesFromFile(file, metadata, readOptions)) {
        if (batch.residualPredicateSatisfied === true) {
          if (scanOptions !== undefined) {
            recordRowsMatched(scanOptions.stats, batch.batch.rowCount);
            enforceAggregateTaskBudget(scanOptions);
          }
          updateVectorAggregateStates(states, spec, batch.batch, undefined, aggregateOptions);
          continue;
        }
        const selection = tryPredicateSelection(batch.batch, task.residualPredicate);
        if (selection === undefined) {
          throw new LakeqlError(
            "LAKEQL_UNSUPPORTED_PUSHDOWN",
            "aggregateParquetTask requires a vectorizable residual predicate",
            { path: task.path },
          );
        }
        if (scanOptions !== undefined) {
          recordRowsMatched(scanOptions.stats, selectedRowCount(batch.batch.rowCount, selection));
          enforceAggregateTaskBudget(scanOptions);
        }
        updateVectorAggregateStates(states, spec, batch.batch, selection, aggregateOptions);
      }
    }
    return snapshotVectorAggregateStates(states);
  } catch (cause) {
    if (cause instanceof LakeqlError) throw cause;
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", `Failed to read ${task.path}`, {
      path: task.path,
      cause,
    });
  }
}

export async function aggregateParquetTasks(
  store: ObjectStore,
  tasks: TaskInput[],
  spec: AggregateSpec,
  options: AggregateParquetTasksOptions = {},
): Promise<Record<string, unknown>> {
  const maxConcurrentTasks = options.maxConcurrentTasks ?? 1;
  const taskInputs = aggregateTaskInputs(tasks, options);
  const aggregateOptions = aggregateVectorOptions(options);
  const merged = createVectorAggregateStates(spec, aggregateOptions);
  await fanInWorkUnits({
    inputs: taskInputs,
    initial: merged,
    maxConcurrentTasks,
    ...(options.maxBufferedPartials !== undefined
      ? { maxBufferedPartials: options.maxBufferedPartials }
      : {}),
    run(task) {
      return aggregateParquetTask(store, task, spec, options);
    },
    ...(options.partialBoundary === undefined ? {} : { boundary: options.partialBoundary }),
    reduce(accumulator, partial) {
      mergeVectorAggregateStateSnapshots(accumulator, partial, aggregateOptions);
    },
  });
  return finalizeVectorAggregateStates(merged);
}

export async function aggregateParquetGroupTask(
  store: ObjectStore,
  task: TaskInput,
  groupColumns: readonly string[],
  spec: AggregateSpec,
  options: AggregateParquetGroupTaskOptions = {},
): Promise<VectorGroupByStateSnapshot> {
  try {
    const scanOptions = aggregateScanOptions(options);
    const file = await asyncBufferFromStore(store, task.path, scanOptions);
    enforceAggregateTaskBudget(scanOptions);
    const { metadata, cached } = await readCachedParquetMetadata(
      task.path,
      file,
      options.metadataCache,
    );
    recordMetadataCache(scanOptions, cached);
    rejectUnsupportedParquetSchema(metadata, { allowValueCheckedInt64Decimals: true });
    enforceAggregateTaskBudget(scanOptions);
    const physicalColumns = aggregateTaskReadColumns(task, spec, groupColumns);
    const aggregateOptions = aggregateVectorOptions(options);
    const groupOptions =
      options.maxGroups === undefined
        ? aggregateOptions
        : { ...aggregateOptions, maxGroups: options.maxGroups };
    const state = createVectorGroupByState(groupColumns, spec);
    for (const { rowStart, rowEnd } of taskRowWindows(metadata, task)) {
      const readOptions = taskReadOptions(
        rowStart,
        rowEnd,
        physicalColumns,
        task.residualPredicate,
        options,
      );
      if (scanOptions !== undefined) readOptions.stats = scanOptions.stats;
      for await (const batch of readParquetColumnBatchesFromFile(file, metadata, readOptions)) {
        const vectorBatch = batchWithPartitionValues(batch.batch, task.partitionValues);
        if (batch.residualPredicateSatisfied === true) {
          if (scanOptions !== undefined) {
            recordRowsMatched(scanOptions.stats, vectorBatch.rowCount);
            enforceAggregateTaskBudget(scanOptions);
          }
          updateVectorGroupByState(state, vectorBatch, undefined, groupOptions);
          continue;
        }
        const selection = tryPredicateSelection(vectorBatch, task.residualPredicate);
        if (selection === undefined) {
          throw new LakeqlError(
            "LAKEQL_UNSUPPORTED_PUSHDOWN",
            "aggregateParquetGroupTask requires a vectorizable residual predicate",
            { path: task.path },
          );
        }
        if (scanOptions !== undefined) {
          recordRowsMatched(scanOptions.stats, selectedRowCount(vectorBatch.rowCount, selection));
          enforceAggregateTaskBudget(scanOptions);
        }
        updateVectorGroupByState(state, vectorBatch, selection, groupOptions);
      }
    }
    return snapshotVectorGroupByState(state);
  } catch (cause) {
    if (cause instanceof LakeqlError) throw cause;
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", `Failed to read ${task.path}`, {
      path: task.path,
      cause,
    });
  }
}

export async function aggregateParquetGroupTasks(
  store: ObjectStore,
  tasks: TaskInput[],
  groupColumns: readonly string[],
  spec: AggregateSpec,
  options: AggregateParquetGroupTasksOptions = {},
): Promise<Row[]> {
  return materializeBatchRows(
    await aggregateParquetGroupTasksBatch(store, tasks, groupColumns, spec, options),
  );
}

export async function aggregateParquetGroupTasksBatch(
  store: ObjectStore,
  tasks: TaskInput[],
  groupColumns: readonly string[],
  spec: AggregateSpec,
  options: AggregateParquetGroupTasksOptions = {},
): Promise<Batch> {
  const maxConcurrentTasks = options.maxConcurrentTasks ?? 1;
  const taskInputs = aggregateTaskInputs(tasks, options);
  const aggregateOptions = aggregateVectorOptions(options);
  const groupOptions =
    options.maxGroups === undefined
      ? aggregateOptions
      : { ...aggregateOptions, maxGroups: options.maxGroups };
  const merged = createVectorGroupByState(groupColumns, spec);
  await fanInWorkUnits({
    inputs: taskInputs,
    initial: merged,
    maxConcurrentTasks,
    ...(options.maxBufferedPartials !== undefined
      ? { maxBufferedPartials: options.maxBufferedPartials }
      : {}),
    run(task) {
      return aggregateParquetGroupTask(store, task, groupColumns, spec, options);
    },
    ...(options.partialBoundary === undefined ? {} : { boundary: options.partialBoundary }),
    reduce(accumulator, partial) {
      mergeVectorGroupByStates(
        accumulator,
        restoreVectorGroupByState(groupColumns, spec, partial, groupOptions),
        groupOptions,
      );
    },
  });
  return applyGroupResultOptions(finalizeVectorGroupByBatch(merged), options);
}

function aggregateTaskInputs(
  tasks: TaskInput[],
  options: { preserveTaskBoundaries?: boolean },
): TaskInput[] {
  return options.preserveTaskBoundaries === true ? tasks : groupAggregateTasks(tasks);
}

function recordMetadataCache(
  scanOptions: ReturnType<typeof aggregateScanOptions>,
  cached: boolean,
): void {
  if (scanOptions === undefined) return;
  if (cached) scanOptions.stats.cacheHits += 1;
  else scanOptions.stats.cacheMisses += 1;
}

function batchWithPartitionValues(batch: Batch, partitionValues: Record<string, string>): Batch {
  const entries = Object.entries(partitionValues).filter(
    ([column]) => batch.columns[column] === undefined,
  );
  if (entries.length === 0) return batch;
  const columns: Record<string, Vector> = { ...batch.columns };
  for (const [column, value] of entries) {
    columns[column] = vectorFromValues(Array.from({ length: batch.rowCount }, () => value));
  }
  return { rowCount: batch.rowCount, columns };
}

function applyGroupResultOptions(batch: Batch, options: AggregateParquetGroupTasksOptions): Batch {
  if (options.orderBy === undefined) {
    if (options.limit === undefined && options.offset === undefined) return batch;
    return sliceBatch(batch, options.offset ?? 0, options.limit);
  }
  if (options.limit !== undefined) {
    return vectorTopKBatch(
      batch,
      options.orderBy,
      options.offset === undefined
        ? { limit: options.limit }
        : { offset: options.offset, limit: options.limit },
    );
  }
  const ordered = vectorOrderByBatch(batch, options.orderBy);
  return options.offset === undefined || options.offset === 0
    ? ordered
    : sliceBatch(ordered, options.offset);
}

function sliceBatch(batch: Batch, offset: number, limit?: number): Batch {
  if (!Number.isInteger(offset) || offset < 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "offset must be a non-negative integer", { offset });
  }
  if (limit !== undefined && (!Number.isInteger(limit) || limit < 0)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "limit must be a non-negative integer", { limit });
  }
  const end = limit === undefined ? batch.rowCount : Math.min(offset + limit, batch.rowCount);
  const indices: number[] = [];
  for (let index = offset; index < end; index += 1) indices.push(index);
  return gatherBatch(batch, indices);
}
