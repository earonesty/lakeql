import {
  type Batch,
  batchFromColumns,
  gatherBatch,
  LakeqlError,
  matches,
  materializeBatchRows,
  materializeSelectedBatchRows,
  type ObjectStore,
  type Row,
  selectedRowCount,
  type TaskInput,
  tryPredicateSelection,
} from "lakeql-core";
import { readParquetColumnBatchesFromFile } from "./column-batches.js";
import { readCachedParquetMetadata } from "./metadata-cache.js";
import { readParquetObjectBatchesFromFile } from "./object-batches.js";
import { recordRowsMatched } from "./read-metrics.js";
import { rejectUnsupportedParquetSchema } from "./schema.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import {
  aggregateScanOptions,
  appendRowGroupRange,
  cloneTaskWithRanges,
  type PlanParquetTaskWorkUnitsOptions,
  type ScanParquetTaskOptions,
  taskReadOptions,
  taskRowWindows,
  validateTaskWorkUnitOptions,
} from "./task.js";
import type { ParquetColumnBatch, ParquetMetadata, StoreAsyncBuffer } from "./types.js";

export async function* scanParquetTaskBatches(
  store: ObjectStore,
  task: TaskInput,
  options: ScanParquetTaskOptions = {},
): AsyncIterable<Row[]> {
  const file = await asyncBufferFromStore(store, task.path);
  const metadata = await taskMetadata(task.path, file, options);
  rejectUnsupportedParquetSchema(metadata);
  const physicalColumns = task.projectedColumns?.filter(
    (column) => !(column in task.partitionValues),
  );
  for (const { rowStart, rowEnd } of taskRowWindows(metadata, task)) {
    for await (const batch of readParquetObjectBatchesFromFile(
      file,
      metadata,
      taskReadOptions(rowStart, rowEnd, physicalColumns, task.residualPredicate, options),
    )) {
      const rows = batch.rows
        .map((row: Row) => ({ ...task.partitionValues, ...row }))
        .filter((row: Row) => matches(task.residualPredicate, row))
        .map((row: Row) => projectTaskRow(row, task.projectedColumns));
      recordRowsMatched(options.stats, rows.length);
      if (rows.length > 0) yield rows;
    }
  }
}

export async function* scanParquetTaskColumnBatches(
  store: ObjectStore,
  task: TaskInput,
  options: ScanParquetTaskOptions = {},
): AsyncIterable<ParquetColumnBatch> {
  const scanOptions = aggregateScanOptions(options);
  const file = await asyncBufferFromStore(store, task.path, scanOptions);
  const metadata = await taskMetadata(task.path, file, options);
  rejectUnsupportedParquetSchema(metadata);
  const physicalColumns = task.projectedColumns?.filter(
    (column) => !(column in task.partitionValues),
  );
  for (const { rowStart, rowEnd } of taskRowWindows(metadata, task)) {
    for await (const batch of readParquetColumnBatchesFromFile(
      file,
      metadata,
      taskReadOptions(rowStart, rowEnd, physicalColumns, task.residualPredicate, options),
    )) {
      if (batch.residualPredicateSatisfied === true) {
        if (
          Object.keys(task.partitionValues).length === 0 &&
          task.projectedColumns?.every((column) => column in batch.batch.columns) !== false
        ) {
          recordRowsMatched(options.stats, batch.batch.rowCount);
          yield batch;
          continue;
        }
        const rows = materializeBatchRows(batch.batch)
          .map((row: Row) => ({ ...task.partitionValues, ...row }))
          .map((row: Row) => projectTaskRow(row, task.projectedColumns));
        if (rows.length > 0) {
          recordRowsMatched(options.stats, rows.length);
          yield { rowOffset: batch.rowOffset, batch: batchFromRows(rows, task.projectedColumns) };
        }
        continue;
      }
      if (
        Object.keys(task.partitionValues).length === 0 &&
        task.residualPredicate === undefined &&
        task.projectedColumns?.every((column) => column in batch.batch.columns) !== false
      ) {
        yield batch;
        continue;
      }
      const selection = tryPredicateSelection(batch.batch, task.residualPredicate);
      if (
        selection !== undefined &&
        Object.keys(task.partitionValues).length === 0 &&
        task.projectedColumns?.every((column) => column in batch.batch.columns) !== false
      ) {
        recordRowsMatched(options.stats, selectedRowCount(batch.batch.rowCount, selection));
        yield {
          rowOffset: batch.rowOffset,
          batch: gatherBatch(batch.batch, selectionIndices(selection)),
        };
        continue;
      }
      const rows =
        selection === undefined
          ? materializeBatchRows(batch.batch)
              .map((row: Row) => ({ ...task.partitionValues, ...row }))
              .filter((row: Row) => matches(task.residualPredicate, row))
              .map((row: Row) => projectTaskRow(row, task.projectedColumns))
          : materializeSelectedBatchRows(batch.batch, selection)
              .map((row: Row) => ({ ...task.partitionValues, ...row }))
              .map((row: Row) => projectTaskRow(row, task.projectedColumns));
      if (rows.length > 0) {
        yield { rowOffset: batch.rowOffset, batch: batchFromRows(rows, task.projectedColumns) };
      }
    }
  }
}

export async function planParquetTaskWorkUnits(
  store: ObjectStore,
  task: TaskInput,
  options: PlanParquetTaskWorkUnitsOptions,
): Promise<TaskInput[]> {
  validateTaskWorkUnitOptions(options);
  if (
    options.maxRowsPerTask === undefined &&
    options.maxRowGroupsPerTask !== undefined &&
    task.rowGroupCount !== undefined &&
    task.rowGroupRanges.every((range) => Number.isFinite(range.start) && Number.isFinite(range.end))
  ) {
    return splitTaskByRowGroupCount(task, options.maxRowGroupsPerTask, task.rowGroupCount);
  }
  const metadata = await readParquetMetadata(store, task.path, options.metadataCache);
  const rowGroupRows = metadata.row_groups.map((rowGroup) => Number(rowGroup.num_rows));
  const taskWithRowGroupCount =
    task.rowGroupCount === undefined ? { ...task, rowGroupCount: rowGroupRows.length } : task;
  const workUnits: TaskInput[] = [];
  let currentRanges: { start: number; end: number }[] = [];
  let currentGroups = 0;
  let currentRows = 0;

  const flush = () => {
    if (currentGroups === 0) return;
    workUnits.push(cloneTaskWithRanges(taskWithRowGroupCount, currentRanges));
    currentRanges = [];
    currentGroups = 0;
    currentRows = 0;
  };

  for (const range of task.rowGroupRanges) {
    const start = Math.max(0, range.start);
    const end = Math.min(range.end, rowGroupRows.length);
    for (let index = start; index < end; index += 1) {
      const rows = rowGroupRows[index] ?? 0;
      if (options.maxRowsPerTask !== undefined && rows > options.maxRowsPerTask) {
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          "maxRowsPerTask is smaller than a planned Parquet row group",
          {
            path: task.path,
            rowGroup: index,
            rowGroupRows: rows,
            maxRowsPerTask: options.maxRowsPerTask,
          },
        );
      }
      const wouldExceedGroups =
        options.maxRowGroupsPerTask !== undefined &&
        currentGroups > 0 &&
        currentGroups + 1 > options.maxRowGroupsPerTask;
      const wouldExceedRows =
        options.maxRowsPerTask !== undefined &&
        currentRows > 0 &&
        currentRows + rows > options.maxRowsPerTask;
      if (wouldExceedGroups || wouldExceedRows) flush();
      appendRowGroupRange(currentRanges, index);
      currentGroups += 1;
      currentRows += rows;
    }
  }
  flush();
  return workUnits;
}

function splitTaskByRowGroupCount(
  task: TaskInput,
  maxRowGroupsPerTask: number,
  rowGroupCount: number,
): TaskInput[] {
  const workUnits: TaskInput[] = [];
  let currentRanges: { start: number; end: number }[] = [];
  let currentGroups = 0;

  const flush = () => {
    if (currentGroups === 0) return;
    workUnits.push(cloneTaskWithRanges(task, currentRanges));
    currentRanges = [];
    currentGroups = 0;
  };

  for (const range of task.rowGroupRanges) {
    const start = Math.max(0, Math.trunc(range.start));
    const end = Math.min(rowGroupCount, Math.max(start, Math.trunc(range.end)));
    for (let index = start; index < end; index += 1) {
      if (currentGroups > 0 && currentGroups + 1 > maxRowGroupsPerTask) flush();
      appendRowGroupRange(currentRanges, index);
      currentGroups += 1;
    }
  }
  flush();
  return workUnits;
}

/** Read Parquet footer metadata (row groups, schema, stats). */
export async function readParquetMetadata(
  store: ObjectStore,
  path: string,
  metadataCache?: ScanParquetTaskOptions["metadataCache"],
): Promise<ParquetMetadata> {
  const file = await asyncBufferFromStore(store, path);
  return taskMetadata(path, file, metadataCache === undefined ? {} : { metadataCache });
}

async function taskMetadata(
  path: string,
  file: StoreAsyncBuffer,
  options: ScanParquetTaskOptions,
): Promise<ParquetMetadata> {
  const { metadata, cached } = await readCachedParquetMetadata(path, file, options.metadataCache);
  if (options.stats !== undefined) {
    if (cached) options.stats.cacheHits += 1;
    else options.stats.cacheMisses += 1;
  }
  return metadata;
}

function selectionIndices(selection: Uint8Array): number[] {
  const indices: number[] = [];
  for (let index = 0; index < selection.length; index += 1) {
    if (selection[index] === 1) indices.push(index);
  }
  return indices;
}

function projectTaskRow(row: Row, columns: string[] | undefined): Row {
  if (columns === undefined) return row;
  const projected: Row = {};
  for (const column of columns) projected[column] = row[column];
  return projected;
}

function batchFromRows(rows: Row[], columns: string[] | undefined): Batch {
  const names = columns ?? [...new Set(rows.flatMap((row) => Object.keys(row)))];
  const columnsByName: Record<string, unknown[]> = Object.fromEntries(
    names.map((name) => [name, []]),
  );
  for (const row of rows) {
    for (const name of names) columnsByName[name]?.push(row[name]);
  }
  return batchFromColumns(columnsByName);
}
