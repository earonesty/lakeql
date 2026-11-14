import {
  MATERIALIZATION_CHARGES as c,
  detachOwnedRow,
  gatherSelectedBatch,
  LakeqlError,
  MemoryBudget,
  MemoryScope,
  matches,
  type ObjectStore,
  planBatchMaterialization,
  planBatchRowMaterialization,
  type Row,
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
  appendRowGroupRange,
  cloneTaskWithRanges,
  iterateTaskRowWindows,
  type PlanParquetTaskWorkUnitsOptions,
  type ScanParquetTaskOptions,
  taskReadOptions,
  validateTaskWorkUnitOptions,
} from "./task.js";
import { batchWithPartitionValues, projectTaskBatch } from "./task-batch.js";
import { taskMetadataOwned, taskSource } from "./task-source.js";
import type { ParquetColumnBatch, ParquetMetadata, StoreAsyncBuffer } from "./types.js";

export async function* scanParquetTaskBatches(
  store: ObjectStore,
  task: TaskInput,
  options: ScanParquetTaskOptions = {},
): AsyncIterable<Row[]> {
  const scope = new MemoryScope(options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER));
  options = { ...options, memoryBudget: scope.budget };
  try {
    const file = await taskSource(store, task.path, options);
    const { metadata, cached } = await taskMetadataOwned(task.path, file, scope, options);
    if (options.stats !== undefined) {
      if (cached) options.stats.cacheHits++;
      else options.stats.cacheMisses++;
    }
    rejectUnsupportedParquetSchema(metadata, {
      allowValueCheckedScalarDecimals: true,
      memoryBudget: options.memoryBudget,
    });
    const physicalColumns = task.projectedColumns?.filter(
      (column) => !(column in task.partitionValues),
    );
    for (const { rowStart, rowEnd } of iterateTaskRowWindows(metadata, task, scope)) {
      for await (const batch of readParquetObjectBatchesFromFile(
        file,
        metadata,
        taskReadOptions(rowStart, rowEnd, physicalColumns, task.residualPredicate, options),
      )) {
        const output = scope.childScope();
        try {
          const rows = transformTaskRows(batch.rows, task, output);
          recordRowsMatched(options.stats, rows.length);
          if (rows.length > 0) yield rows;
        } finally {
          output.dispose();
        }
      }
    }
  } finally {
    scope.dispose();
  }
}

export async function* scanParquetTaskColumnBatches(
  store: ObjectStore,
  task: TaskInput,
  options: ScanParquetTaskOptions = {},
): AsyncIterable<ParquetColumnBatch> {
  const scope = new MemoryScope(options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER));
  options = { ...options, memoryBudget: scope.budget };
  try {
    const file = await taskSource(store, task.path, options);
    const { metadata, cached } = await taskMetadataOwned(task.path, file, scope, options);
    if (options.stats !== undefined) {
      if (cached) options.stats.cacheHits++;
      else options.stats.cacheMisses++;
    }
    rejectUnsupportedParquetSchema(metadata, {
      allowValueCheckedScalarDecimals: true,
      memoryBudget: options.memoryBudget,
    });
    const physicalColumns = task.projectedColumns?.filter(
      (column) => !(column in task.partitionValues),
    );
    for (const { rowStart, rowEnd } of iterateTaskRowWindows(metadata, task, scope)) {
      for await (const batch of readParquetColumnBatchesFromFile(
        file,
        metadata,
        taskReadOptions(rowStart, rowEnd, physicalColumns, task.residualPredicate, options),
      )) {
        const output = scope.childScope();
        try {
          const augmented = batchWithPartitionValues(batch.batch, task.partitionValues, output);
          let selection: Uint8Array | undefined;
          if (batch.residualPredicateSatisfied !== true && task.residualPredicate !== undefined) {
            selection = tryPredicateSelection(augmented, task.residualPredicate, output);
            if (selection === undefined) {
              selection = output.allocate(
                c.view + augmented.rowCount,
                "task residual selection",
                () => new Uint8Array(augmented.rowCount),
              ).value;
              const batchPlan = planBatchMaterialization(augmented, undefined, output);
              // Use the cheaper batch traversal only when it leaves equal headroom
              // for concurrent work; otherwise keep scalar workspace to one row.
              const useBatch = batchPlan.allocationBytes <= output.budget.availableBytes / 2;
              for (
                let index = 0;
                index < augmented.rowCount;
                index += useBatch ? augmented.rowCount : 1
              ) {
                const scratch = output.childScope();
                try {
                  const rows = (
                    useBatch ? batchPlan : planBatchRowMaterialization(augmented, index, scratch)
                  ).execute(scratch);
                  try {
                    for (let offset = 0; offset < rows.value.length; offset += 1) {
                      const row = rows.value[offset];
                      if (row !== undefined && matches(task.residualPredicate, row, scratch))
                        selection[index + offset] = 1;
                    }
                  } finally {
                    rows.release();
                  }
                } finally {
                  scratch.dispose();
                }
              }
            }
          }
          const projected = projectTaskBatch(augmented, task.projectedColumns, output);
          const selected =
            selection === undefined ? projected : gatherSelectedBatch(projected, selection, output);
          recordRowsMatched(options.stats, selected.rowCount);
          if (selected.rowCount > 0) yield { rowOffset: batch.rowOffset, batch: selected };
        } finally {
          output.dispose();
        }
      }
    }
  } finally {
    scope.dispose();
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
  const scope = new MemoryScope(options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER));
  try {
    const file = await taskSource(store, task.path, { ...options, memoryBudget: scope.budget });
    const { metadata } = await taskMetadataOwned(task.path, file, scope, options);
    const taskWithRowGroupCount =
      task.rowGroupCount === undefined
        ? { ...task, rowGroupCount: metadata.row_groups.length }
        : task;
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
      const end = Math.min(range.end, metadata.row_groups.length);
      for (let index = start; index < end; index += 1) {
        const rows = Number(metadata.row_groups[index]?.num_rows ?? 0);
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
  } finally {
    scope.dispose();
  }
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

/** The source batch stays live while rows are transformed and yielded. */
function transformTaskRows(source: Row[], task: TaskInput, output: MemoryScope): Row[] {
  const rows = output.allocate(c.array, "task output rows", () => [] as Row[]).value;
  for (const row of source) {
    const scratch = output.childScope();
    try {
      const merged = scratch.allocate(c.object, "task predicate row", () => ({}) as Row).value;
      for (let inputIndex = 0; inputIndex < 2; inputIndex += 1) {
        const input = inputIndex === 0 ? task.partitionValues : row;
        for (const name in input) {
          if (!Object.hasOwn(input, name)) continue;
          scratch.allocate(
            Object.hasOwn(merged, name) ? 0 : c.property,
            "task predicate property",
            () => {
              Object.defineProperty(merged, name, {
                value: input[name],
                enumerable: true,
                writable: true,
                configurable: true,
              });
            },
          );
        }
      }
      if (!matches(task.residualPredicate, merged, scratch)) continue;
      let selected = merged;
      if (task.projectedColumns !== undefined) {
        selected = scratch.allocate(c.object, "task projection", () => ({}) as Row).value;
        for (const name of task.projectedColumns) {
          scratch.allocate(
            Object.hasOwn(selected, name) ? 0 : c.property,
            "task projection property",
            () => {
              Object.defineProperty(selected, name, {
                value: merged[name],
                enumerable: true,
                writable: true,
                configurable: true,
              });
            },
          );
        }
      }
      const owned = detachOwnedRow(selected, output.budget);
      try {
        output.adopt(owned);
      } catch (error) {
        owned.release();
        throw error;
      }
      output.allocate(c.arraySlot, "task output row slot", () => {
        rows.push(owned.value);
      });
    } finally {
      scratch.dispose();
    }
  }
  return rows;
}
