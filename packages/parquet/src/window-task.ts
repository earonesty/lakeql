import {
  evaluateWindowWorkUnitPartials,
  fanInWorkUnits,
  type ObjectStore,
  partitionWindowWorkUnitRows,
  type QueryBudget,
  type Row,
  type TaskInput,
  type WindowExpr,
  type WindowWorkUnitPartial,
} from "lakeql-core";
import type { ScanParquetTaskOptions } from "./task.js";
import { scanParquetTaskBatches } from "./task-scan.js";

export interface WindowParquetTasksOptions extends ScanParquetTaskOptions {
  bucketCount?: number;
  budget?: QueryBudget;
  maxConcurrentTasks?: number;
  maxBufferedPartials?: number;
  partialBoundary?(
    partial: WindowWorkUnitPartial,
    task: TaskInput,
    index: number,
  ): WindowWorkUnitPartial | Promise<WindowWorkUnitPartial>;
}

export async function windowParquetTask(
  store: ObjectStore,
  task: TaskInput,
  windows: Record<string, WindowExpr>,
  index: number,
  options: WindowParquetTasksOptions = {},
): Promise<WindowWorkUnitPartial> {
  const rows: { ordinal: { task: number; row: number }; row: Row }[] = [];
  let rowIndex = 0;
  for await (const batch of scanParquetTaskBatches(store, task, options)) {
    for (const row of batch) {
      rows.push({ ordinal: { task: index, row: rowIndex }, row });
      rowIndex += 1;
    }
  }
  return partitionWindowWorkUnitRows(rows, windows, options.bucketCount ?? 1);
}

export async function windowParquetTasks(
  store: ObjectStore,
  tasks: TaskInput[],
  windows: Record<string, WindowExpr>,
  options: WindowParquetTasksOptions = {},
): Promise<Row[]> {
  const bucketCount = options.bucketCount ?? plannedWindowBucketCount(tasks);
  const partials = await fanInWorkUnits({
    inputs: tasks,
    initial: [] as WindowWorkUnitPartial[],
    maxConcurrentTasks: options.maxConcurrentTasks ?? 1,
    ...(options.maxBufferedPartials !== undefined
      ? { maxBufferedPartials: options.maxBufferedPartials }
      : {}),
    run(task, index) {
      return windowParquetTask(store, task, windows, index, { ...options, bucketCount });
    },
    ...(options.partialBoundary === undefined ? {} : { boundary: options.partialBoundary }),
    reduce(accumulator, partial) {
      accumulator.push(partial);
    },
  });
  return evaluateWindowWorkUnitPartials(partials, windows, {
    bucketCount,
  });
}

function plannedWindowBucketCount(tasks: readonly TaskInput[]): number {
  const planned = tasks.find((task) => task.window?.available === true)?.window;
  return planned?.available === true ? planned.bucketCount : 1;
}
