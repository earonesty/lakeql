import {
  materializeBatchRows,
  materializeSelectedBatchRows,
  predicateSelection,
  selectedRowCount,
} from "./batch.js";
import { BufferMemory } from "./buffer-memory.js";
import { matches } from "./evaluator.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import type {
  PathQueryInit,
  QueryBudget,
  ScanAdapter,
  ScanOptions,
  ScanVectorBatch,
  TaskInput,
} from "./query.js";
import type { ObjectInfo } from "./store.js";
import type { QueryStats, Row } from "./types.js";
import {
  postWindowQueryBatches,
  type WindowBatchExecutionOptions,
  windowedQueryBatches,
  windowedRunQueryBatches,
} from "./window-execution.js";
import type { WindowExecutionMode } from "./window-query.js";
import { windowReadColumns } from "./window-query.js";
import {
  serializeWindowOperatorState,
  type WindowOptions,
  type WindowResult,
  windowOperatorState,
  windowOperatorStateFromRuns,
  windowRowsFromState,
} from "./window-state.js";
import { applyWindowsToVectorBatches, type VectorWindowBatch } from "./window-vector.js";

export interface WindowRunnerConfig extends PathQueryInit {
  scanner: ScanAdapter;
  budget: QueryBudget;
  now: () => number;
}

export interface WindowRunnerOptions {
  memoryBudget?: MemoryBudget;
  config: WindowRunnerConfig;
  stats: QueryStats;
  planObjects(): Promise<{ planned: ObjectInfo[]; skipped: number }>;
  tasksFromObjects(objects: ObjectInfo[]): Promise<TaskInput[]>;
  vectorBatchScanner(scanner: ScanAdapter): VectorBatchScanner | undefined;
  columnarBatchSize(batchSize: number | undefined): number;
  parseHivePartitions(path: string): Record<string, string>;
  enforceBudget(budget: QueryBudget, stats: QueryStats, now: () => number, startedAt: number): void;
  enforceBufferedRows(budget: QueryBudget, rows: number): void;
  enforceWindowPartitionRows(budget: QueryBudget, rows: number): void;
  enforceMemoryBytes(budget: QueryBudget, bytes: number): void;
  estimateMemoryBytes(value: unknown): number;
  /** Serialized size of one list item, excluding list brackets and separators. */
  estimateBufferItemBytes?(row: Row): number;
  batchExecution: Omit<
    WindowBatchExecutionOptions,
    | "config"
    | "stats"
    | "startedAt"
    | "matched"
    | "enforceBudget"
    | "enforceBufferedRows"
    | "enforceWindowPartitionRows"
    | "enforceMemoryBytes"
    | "estimateMemoryBytes"
  > & {
    vectorExprSupported(expr: PathQueryInit["where"]): boolean;
  };
}

export type VectorBatchScanner = (
  scanner: ScanAdapter,
  path: string,
  options: ScanOptions,
) => AsyncIterable<ScanVectorBatch>;

export async function* executeWindowBatches(options: WindowRunnerOptions): AsyncIterable<Row[]> {
  const { config, stats } = options;
  const startedAt = config.now();
  const taskRows = await collectWindowTaskMatches(options, startedAt);
  if (taskRows !== undefined) {
    yield* postWindowQueryBatches({
      ...windowBatchOptions(options, startedAt),
      rows: taskRows,
    });
    return;
  }

  const vectorRows = await collectVectorWindowRows(options, startedAt);
  if (vectorRows !== undefined) {
    yield* postWindowQueryBatches({
      ...windowBatchOptions(options, startedAt),
      rows: vectorRows,
    });
    return;
  }

  const matched: Row[] = [];
  await collectWindowMatches(options, matched, startedAt);
  yield* windowedQueryBatches({
    ...windowBatchOptions(options, startedAt),
    matched,
  });
  stats.elapsedMs = config.now() - startedAt;
}

export async function executeWindowWithState(
  options: WindowRunnerOptions,
  windowOptions: WindowOptions,
  spillId: string,
): Promise<WindowResult> {
  const { config } = options;
  const windows = config.windows ?? {};
  const previousRows = await windowRowsFromState(windows, windowOptions);
  const runs =
    windowOptions.spill === undefined
      ? [previousRows]
      : chunkRowsForRuns(previousRows, config.budget);
  const startedAt = config.now();
  let matched: Row[] | undefined;
  if (windowOptions.spill === undefined) {
    matched = runs[0] ?? [];
    options.enforceBufferedRows(config.budget, matched.length);
    options.enforceMemoryBytes(config.budget, options.estimateMemoryBytes(matched));
    await collectWindowMatches(options, matched, startedAt);
  } else {
    await collectWindowRuns(options, runs, startedAt);
  }

  const rows: Row[] = [];
  const batches =
    windowOptions.spill === undefined
      ? windowedQueryBatches({
          ...windowBatchOptions(options, startedAt),
          matched: matched ?? [],
        })
      : windowedRunQueryBatches({
          ...windowBatchOptions(options, startedAt),
          runs,
        });
  for await (const batch of batches) rows.push(...batch);

  const state =
    windowOptions.spill === undefined
      ? windowOperatorState(windows, matched ?? [])
      : await windowOperatorStateFromRuns(windows, runs, windowOptions.spill, spillId);
  const operatorState = serializeWindowOperatorState(state);
  const result: WindowResult = { rows, operatorState };
  if (windowOptions.spill !== undefined) {
    result.operatorSpill = await windowOptions.spill.write(spillId, operatorState);
  }
  return result;
}

export async function collectWindowMatches(
  options: WindowRunnerOptions,
  matched: Row[],
  startedAt: number,
): Promise<WindowExecutionMode> {
  const { config, stats } = options;
  const scope = new MemoryScope(options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER));
  try {
    const buffer = new WindowRowBuffer(options, matched);
    const { planned: paths, skipped: skippedFiles } = await options.planObjects();
    stats.filesSkipped = skippedFiles;
    const columns = windowReadColumns(config);
    for (const object of paths) {
      stats.filesPlanned += 1;
      stats.filesRead += 1;
      stats.bytesRequested += object.size;
      options.enforceBudget(config.budget, stats, config.now, startedAt);
      const scanOptions = rowScanOptions(options, startedAt);
      const partitionValues = config.hive ? options.parseHivePartitions(object.path) : {};
      const physicalColumns = columns?.filter((column) => !(column in partitionValues));
      if (physicalColumns !== undefined && physicalColumns.length > 0) {
        scanOptions.columns = physicalColumns;
      }
      if (config.where !== undefined) scanOptions.where = config.where;
      for await (const rawBatch of config.scanner.scan(object.path, scanOptions)) {
        for (const rawRow of rawBatch) {
          const row = config.hive ? { ...partitionValues, ...rawRow } : rawRow;
          stats.rowsDecoded += 1;
          options.enforceBudget(config.budget, stats, config.now, startedAt);
          if (!matches(config.where, row, scope)) continue;
          stats.rowsMatched += 1;
          buffer.append(row);
        }
      }
    }
    stats.elapsedMs = config.now() - startedAt;
    return "row-materialized";
  } finally {
    scope.dispose();
  }
}

export async function collectWindowRuns(
  options: WindowRunnerOptions,
  runs: Row[][],
  startedAt: number,
): Promise<WindowExecutionMode> {
  const currentRun = new WindowRowBuffer(options);
  const execution = (await collectVectorWindowRunMatches(options, runs, currentRun, startedAt))
    ? "vector-window"
    : await collectRowWindowRunMatches(options, runs, currentRun, startedAt);
  flushWindowRun(runs, currentRun);
  return execution;
}

async function collectWindowTaskMatches(
  options: WindowRunnerOptions,
  startedAt: number,
): Promise<Row[] | undefined> {
  const { config, stats } = options;
  if (config.windows === undefined || config.scanner.executeWindowTasks === undefined) {
    return undefined;
  }
  const { planned: objects, skipped } = await options.planObjects();
  const tasks = await options.tasksFromObjects(objects);
  if (tasks.some((task) => task.window?.available !== true)) return undefined;

  stats.filesSkipped = skipped;
  for (const task of tasks) {
    stats.filesPlanned += 1;
    stats.filesRead += 1;
    if (task.size !== undefined) stats.bytesRequested += task.size;
    options.enforceBudget(config.budget, stats, config.now, startedAt);
  }
  return config.scanner.executeWindowTasks(tasks, config.windows, {
    ...(options.memoryBudget === undefined ? {} : { memoryBudget: options.memoryBudget }),
    budget: config.budget,
    stats,
    now: config.now,
    startedAt,
    batchSize: config.batchSize ?? 4096,
  });
}

async function collectVectorWindowRows(
  options: WindowRunnerOptions,
  startedAt: number,
): Promise<Row[] | undefined> {
  const { config, stats } = options;
  const scanVectorBatches = vectorWindowScanner(options);
  if (scanVectorBatches === undefined) return undefined;

  const { planned: paths, skipped: skippedFiles } = await options.planObjects();
  stats.filesSkipped = skippedFiles;
  const columns = windowReadColumns(config);
  const inputs: VectorWindowBatch[] = [];
  for (const object of paths) {
    stats.filesPlanned += 1;
    stats.filesRead += 1;
    stats.bytesRequested += object.size;
    options.enforceBudget(config.budget, stats, config.now, startedAt);
    const scanOptions = vectorScanOptions(options, startedAt);
    if (columns !== undefined && columns.length > 0) scanOptions.columns = columns;
    if (config.where !== undefined) scanOptions.where = config.where;
    for await (const { batch } of scanVectorBatches(config.scanner, object.path, scanOptions)) {
      const selection = predicateSelection(batch, config.where);
      stats.rowsMatched += selectedRowCount(batch.rowCount, selection);
      inputs.push({ batch, selection });
      options.enforceBufferedRows(config.budget, stats.rowsMatched);
      options.enforceBudget(config.budget, stats, config.now, startedAt);
      stats.elapsedMs = config.now() - startedAt;
    }
  }
  stats.elapsedMs = config.now() - startedAt;
  return applyWindowsToVectorBatches(inputs, config.windows ?? {}, {
    ...(options.memoryBudget === undefined ? {} : { memoryBudget: options.memoryBudget }),
    estimateMemoryBytes: options.estimateMemoryBytes,
    enforceBufferedRows: (rows) => options.enforceBufferedRows(config.budget, rows),
    enforceWindowPartitionRows: (rows) => options.enforceWindowPartitionRows(config.budget, rows),
    enforceMemoryBytes: (bytes) => options.enforceMemoryBytes(config.budget, bytes),
  });
}

async function collectVectorWindowRunMatches(
  options: WindowRunnerOptions,
  runs: Row[][],
  currentRun: WindowRowBuffer,
  startedAt: number,
): Promise<boolean> {
  const { config, stats } = options;
  const scanVectorBatches = vectorWindowScanner(options);
  if (scanVectorBatches === undefined) return false;

  const { planned: paths, skipped: skippedFiles } = await options.planObjects();
  stats.filesSkipped = skippedFiles;
  const columns = windowReadColumns(config);
  for (const object of paths) {
    stats.filesPlanned += 1;
    stats.filesRead += 1;
    stats.bytesRequested += object.size;
    options.enforceBudget(config.budget, stats, config.now, startedAt);
    const scanOptions = vectorScanOptions(options, startedAt);
    if (columns !== undefined && columns.length > 0) scanOptions.columns = columns;
    if (config.where !== undefined) scanOptions.where = config.where;
    for await (const { batch } of scanVectorBatches(config.scanner, object.path, scanOptions)) {
      const selection = predicateSelection(batch, config.where);
      stats.rowsMatched += selectedRowCount(batch.rowCount, selection);
      const rows =
        selection === undefined
          ? materializeBatchRows(batch)
          : materializeSelectedBatchRows(batch, selection);
      for (const row of rows) {
        pushWindowRunRow(options, runs, currentRun, row);
        options.enforceBudget(config.budget, stats, config.now, startedAt);
      }
      stats.elapsedMs = config.now() - startedAt;
    }
  }
  stats.elapsedMs = config.now() - startedAt;
  return true;
}

async function collectRowWindowRunMatches(
  options: WindowRunnerOptions,
  runs: Row[][],
  currentRun: WindowRowBuffer,
  startedAt: number,
): Promise<WindowExecutionMode> {
  const { config, stats } = options;
  const scope = new MemoryScope(options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER));
  try {
    const { planned: paths, skipped: skippedFiles } = await options.planObjects();
    stats.filesSkipped = skippedFiles;
    const columns = windowReadColumns(config);
    for (const object of paths) {
      stats.filesPlanned += 1;
      stats.filesRead += 1;
      stats.bytesRequested += object.size;
      options.enforceBudget(config.budget, stats, config.now, startedAt);
      const scanOptions = rowScanOptions(options, startedAt);
      const partitionValues = config.hive ? options.parseHivePartitions(object.path) : {};
      const physicalColumns = columns?.filter((column) => !(column in partitionValues));
      if (physicalColumns !== undefined && physicalColumns.length > 0) {
        scanOptions.columns = physicalColumns;
      }
      if (config.where !== undefined) scanOptions.where = config.where;
      for await (const rawBatch of config.scanner.scan(object.path, scanOptions)) {
        for (const rawRow of rawBatch) {
          const row = config.hive ? { ...partitionValues, ...rawRow } : rawRow;
          stats.rowsDecoded += 1;
          options.enforceBudget(config.budget, stats, config.now, startedAt);
          if (!matches(config.where, row, scope)) continue;
          stats.rowsMatched += 1;
          pushWindowRunRow(options, runs, currentRun, row);
        }
      }
    }
    stats.elapsedMs = config.now() - startedAt;
    return "row-materialized";
  } finally {
    scope.dispose();
  }
}

function vectorWindowScanner(options: WindowRunnerOptions): VectorBatchScanner | undefined {
  const { config } = options;
  if (config.hive === true || !options.batchExecution.vectorExprSupported(config.where)) {
    return undefined;
  }
  return options.vectorBatchScanner(config.scanner);
}

function windowBatchOptions(
  options: WindowRunnerOptions,
  startedAt: number,
): Omit<WindowBatchExecutionOptions, "matched"> {
  return {
    config: options.config,
    ...(options.memoryBudget === undefined ? {} : { memoryBudget: options.memoryBudget }),
    stats: options.stats,
    startedAt,
    ...options.batchExecution,
    enforceBudget: options.enforceBudget,
    enforceBufferedRows: options.enforceBufferedRows,
    enforceWindowPartitionRows: options.enforceWindowPartitionRows,
    enforceMemoryBytes: options.enforceMemoryBytes,
    estimateMemoryBytes: options.estimateMemoryBytes,
  };
}

function rowScanOptions(options: WindowRunnerOptions, startedAt: number): ScanOptions {
  return {
    batchSize: options.config.batchSize ?? 4096,
    stats: options.stats,
    ...(options.memoryBudget === undefined ? {} : { memoryBudget: options.memoryBudget }),
    budget: options.config.budget,
    now: options.config.now,
    startedAt,
  };
}

function vectorScanOptions(options: WindowRunnerOptions, startedAt: number): ScanOptions {
  return {
    batchSize: options.columnarBatchSize(options.config.batchSize),
    stats: options.stats,
    ...(options.memoryBudget === undefined ? {} : { memoryBudget: options.memoryBudget }),
    budget: options.config.budget,
    now: options.config.now,
    startedAt,
  };
}

function pushWindowRunRow(
  options: WindowRunnerOptions,
  runs: Row[][],
  currentRun: WindowRowBuffer,
  row: Row,
): void {
  currentRun.append(row);
  if (currentRun.length >= windowRunCapacity(options.config.budget)) {
    flushWindowRun(runs, currentRun);
  }
}

function chunkRowsForRuns(rows: Row[], budget: QueryBudget): Row[][] {
  if (rows.length === 0) return [];
  const capacity = windowRunCapacity(budget);
  const runs: Row[][] = [];
  for (let index = 0; index < rows.length; index += capacity) {
    runs.push(rows.slice(index, index + capacity));
  }
  return runs;
}

function flushWindowRun(runs: Row[][], currentRun: WindowRowBuffer): void {
  currentRun.flush(runs);
}

function windowRunCapacity(budget: QueryBudget): number {
  return Math.max(1, budget.maxBufferedRows ?? Number.POSITIVE_INFINITY);
}

/** Buffer-local accounting follows append/flush ownership rather than rescanning each row. */
class WindowRowBuffer {
  private readonly memory: BufferMemory<Row> | undefined;

  constructor(
    private readonly options: WindowRunnerOptions,
    private readonly rows: Row[] = [],
  ) {
    this.memory =
      options.config.budget.maxMemoryBytes !== undefined &&
      options.estimateBufferItemBytes !== undefined
        ? new BufferMemory(options.estimateBufferItemBytes, rows)
        : undefined;
  }

  get length(): number {
    return this.rows.length;
  }

  append(row: Row): void {
    this.rows.push(row);
    this.memory?.add(row);
    const budget = this.options.config.budget;
    this.options.enforceBufferedRows(budget, this.rows.length);
    if (budget.maxMemoryBytes !== undefined) {
      this.options.enforceMemoryBytes(
        budget,
        this.memory?.bytes ?? this.options.estimateMemoryBytes(this.rows),
      );
    }
  }

  flush(runs: Row[][]): void {
    if (this.rows.length === 0) return;
    runs.push(this.rows.splice(0, this.rows.length));
    this.memory?.clear();
  }
}
