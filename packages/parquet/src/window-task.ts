import {
  MATERIALIZATION_CHARGES as c,
  detachOwnedRow,
  evaluateWindowWorkUnitPartials,
  fanInWorkUnits,
  LakeqlError,
  MemoryBudget,
  MemoryScope,
  type ObjectStore,
  type OwnedMemory,
  partitionWindowWorkUnitRows,
  type QueryBudget,
  type Row,
  type TaskInput,
  type WindowExpr,
  type WindowWorkUnitPartial,
  type WindowWorkUnitRow,
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
  const owned = await windowParquetTaskOwned(store, task, windows, index, options);
  try {
    return owned.value;
  } finally {
    owned.release();
  }
}

export async function windowParquetTaskOwned(
  store: ObjectStore,
  task: TaskInput,
  windows: Record<string, WindowExpr>,
  index: number,
  options: WindowParquetTasksOptions = {},
): Promise<OwnedMemory<WindowWorkUnitPartial>> {
  const scope = new MemoryScope(options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER));
  const buffer = scope.childScope();
  try {
    const rows = buffer.allocate(
      c.array,
      "window task scan rows",
      () => [] as WindowWorkUnitRow[],
    ).value;
    let rowIndex = 0;
    for await (const batch of scanParquetTaskBatches(store, task, {
      ...options,
      memoryBudget: scope.budget,
    })) {
      for (const row of batch) {
        enforceWindowTaskBudget(options, rowIndex + 1);
        const detached = detachOwnedRow(row, scope.budget, buffer);
        try {
          buffer.adopt(detached);
        } catch (error) {
          detached.release();
          throw error;
        }
        rows.push(
          buffer.allocate(
            c.arraySlot + 2 * c.object + 4 * c.property,
            "window task scan ordinal",
            () => ({ ordinal: { task: index, row: rowIndex }, row: detached.value }),
          ).value,
        );
        rowIndex += 1;
      }
    }
    enforceWindowTaskBudget(options, rows.length);
    const partial = partitionWindowWorkUnitRows(
      rows,
      windows,
      options.bucketCount ?? task.window?.bucketCount ?? 1,
      { memoryBudget: scope.budget, resultScope: scope },
    );
    return scope.retainAllocations(partial);
  } finally {
    scope.dispose();
  }
}

export async function windowParquetTasks(
  store: ObjectStore,
  tasks: TaskInput[],
  windows: Record<string, WindowExpr>,
  options: WindowParquetTasksOptions = {},
): Promise<Row[]> {
  const owned = await windowParquetTasksOwned(store, tasks, windows, options);
  try {
    return owned.value;
  } finally {
    owned.release();
  }
}

export async function windowParquetTasksOwned(
  store: ObjectStore,
  tasks: TaskInput[],
  windows: Record<string, WindowExpr>,
  options: WindowParquetTasksOptions = {},
): Promise<OwnedMemory<Row[]>> {
  const scope = new MemoryScope(options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER));
  const inputs = scope.childScope();
  try {
    const bucketCount = options.bucketCount ?? plannedWindowBucketCount(tasks);
    const partials = await fanInWorkUnits({
      memoryBudget: scope.budget,
      inputs: tasks,
      initial: inputs.allocate(
        c.array,
        "window task partial directory",
        () => [] as WindowWorkUnitPartial[],
      ).value,
      maxConcurrentTasks: options.maxConcurrentTasks ?? 1,
      ...(options.maxBufferedPartials !== undefined
        ? { maxBufferedPartials: options.maxBufferedPartials }
        : {}),
      async run(task, index) {
        const owned = await windowParquetTaskOwned(store, task, windows, index, {
          ...options,
          memoryBudget: scope.budget,
          bucketCount,
        });
        try {
          return inputs.adopt(owned).value;
        } catch (error) {
          owned.release();
          throw error;
        }
      },
      async boundary(partial, task, index) {
        const replacement = (await options.partialBoundary?.(partial, task, index)) ?? partial;
        enforceWindowTaskBudget(options, 0);
        if (replacement === partial) return partial;
        // Custom transport allocation is external. Admit the retained copy before
        // storing it; callers must separately bound allocation in their callback.
        const detached = detachOwnedRow({ partial: replacement }, scope.budget);
        try {
          return inputs.adopt(detached).value.partial as WindowWorkUnitPartial;
        } catch (error) {
          detached.release();
          throw error;
        }
      },
      reduce(accumulator, partial) {
        inputs.allocate(c.arraySlot, "window task partial slot", () => undefined);
        accumulator.push(partial);
      },
    });
    enforceWindowTaskBudget(options, 0);
    const rows = await evaluateWindowWorkUnitPartials(partials, windows, {
      memoryBudget: scope.budget,
      resultScope: scope,
      bucketCount,
    });
    return scope.retainAllocations(rows);
  } finally {
    scope.dispose();
  }
}

function plannedWindowBucketCount(tasks: readonly TaskInput[]): number {
  const planned = tasks.find((task) => task.window?.available === true)?.window;
  return planned?.available === true ? planned.bucketCount : 1;
}

function enforceWindowTaskBudget(options: WindowParquetTasksOptions, rows: number): void {
  options.budget?.signal?.throwIfAborted?.();
  if (options.budget?.maxBufferedRows !== undefined && rows > options.budget.maxBufferedRows) {
    throw new LakeqlError(
      "LAKEQL_BUDGET_EXCEEDED",
      `Query exceeded maxBufferedRows budget ${options.budget.maxBufferedRows}`,
    );
  }
  if (
    options.budget?.maxElapsedMs !== undefined &&
    options.now !== undefined &&
    options.startedAt !== undefined &&
    options.now() - options.startedAt > options.budget.maxElapsedMs
  ) {
    throw new LakeqlError(
      "LAKEQL_BUDGET_EXCEEDED",
      `Query exceeded maxElapsedMs budget ${options.budget.maxElapsedMs}`,
    );
  }
}
