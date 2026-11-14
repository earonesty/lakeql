import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { predicateSelection, selectedRowCount } from "./batch.js";
import { BufferMemory } from "./buffer-memory.js";
import { withCheckpointSpillBatch } from "./checkpoint-spill-write-batch.js";
import { LakeqlError } from "./errors.js";
import { matches } from "./evaluator.js";
import { materializeOwnedBatchRows } from "./materialization-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import { OwnedRowCollector } from "./owned-row-collector.js";
import { detachOwnedBatch, detachOwnedRow } from "./owned-row-detachment.js";
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
  const retainedScope = new MemoryScope(
    options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER),
  );
  try {
    const { config, stats } = options;
    const startedAt = config.now();
    const taskRows = await collectWindowTaskMatches(options, startedAt, retainedScope);
    if (taskRows !== undefined) {
      yield* postWindowQueryBatches({
        ...windowBatchOptions(options, startedAt, retainedScope),
        rows: taskRows,
      });
      return;
    }

    const vectorRows = await collectVectorWindowRows(options, startedAt, retainedScope);
    if (vectorRows !== undefined) {
      yield* postWindowQueryBatches({
        ...windowBatchOptions(options, startedAt, retainedScope),
        rows: vectorRows,
      });
      return;
    }

    const matched: Row[] = [];
    await collectWindowMatches(options, matched, startedAt, retainedScope);
    yield* windowedQueryBatches({
      ...windowBatchOptions(options, startedAt, retainedScope),
      matched,
    });
    stats.elapsedMs = config.now() - startedAt;
  } finally {
    retainedScope.dispose();
  }
}

export async function executeWindowWithState(
  options: WindowRunnerOptions,
  windowOptions: WindowOptions,
  spillId: string,
): Promise<WindowResult> {
  const retainedScope = new MemoryScope(
    options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER),
  );
  const collector = new OwnedRowCollector(retainedScope.budget);
  let collected: OwnedMemory<Row[]> | undefined;
  const retainOutput = (output: OwnedMemory<Row[]>) => {
    try {
      collector.append(output);
    } catch (error) {
      collector.dispose();
      throw error;
    }
  };
  try {
    const { config } = options;
    const windows = config.windows ?? {};
    const startedAt = config.now();
    const checkCheckpoint = () => {
      if (config.budget.signal?.aborted)
        throw new LakeqlError("LAKEQL_ABORTED", "Window checkpoint aborted");
      options.enforceBudget(config.budget, options.stats, config.now, startedAt);
    };
    const previousRows = await windowRowsFromState(
      windows,
      windowOptions,
      retainedScope,
      checkCheckpoint,
    );
    const runs =
      windowOptions.spill === undefined
        ? [previousRows]
        : chunkRowsForRuns(previousRows, config.budget, retainedScope);
    let matched: Row[] | undefined;
    if (windowOptions.spill === undefined) {
      matched = runs[0] ?? [];
      options.enforceBufferedRows(config.budget, matched.length);
      options.enforceMemoryBytes(config.budget, options.estimateMemoryBytes(matched));
      await collectWindowMatches(options, matched, startedAt, retainedScope);
    } else {
      for (const run of runs) new WindowRowBuffer(options, retainedScope, run);
      await collectWindowRuns(options, runs, startedAt, retainedScope);
    }

    const windowScope = retainedScope.childScope();
    const batches =
      windowOptions.spill === undefined
        ? windowedQueryBatches({
            ...windowBatchOptions(options, startedAt, windowScope),
            retainOutput,
            matched: matched ?? [],
          })
        : windowedRunQueryBatches({
            ...windowBatchOptions(options, startedAt, windowScope),
            retainOutput,
            runs,
          });
    for await (const _batch of batches) {
      /* Ownership is retained before each yield. */
    }
    collected = collector.finish();
    // The collector owns detached output; backend result records and values no
    // longer need to overlap checkpoint serialization. Input rows remain pinned.
    windowScope.dispose();
    const rows = collected.value;

    return await withCheckpointSpillBatch(windowOptions.spill, retainedScope, async (spill) => {
      checkCheckpoint();
      const state =
        spill === undefined
          ? windowOperatorState(windows, matched ?? [], retainedScope)
          : await windowOperatorStateFromRuns(
              windows,
              runs,
              spill,
              spillId,
              retainedScope,
              checkCheckpoint,
            );
      const operatorState = serializeWindowOperatorState(state, retainedScope);
      const result: WindowResult = { rows, operatorState };
      if (spill !== undefined) {
        result.operatorSpill = await spill.write(spillId, operatorState);
        checkCheckpoint();
      }
      return result;
    });
  } finally {
    collected?.release();
    collector.dispose();
    retainedScope.dispose();
  }
}

export async function collectWindowMatches(
  options: WindowRunnerOptions,
  matched: Row[],
  startedAt: number,
  retainedScope?: MemoryScope,
): Promise<WindowExecutionMode> {
  const { config, stats } = options;
  const scope =
    retainedScope ??
    new MemoryScope(options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER));
  try {
    const buffer = new WindowRowBuffer(options, scope, matched);
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
    if (retainedScope === undefined) scope.dispose();
  }
}

export async function collectWindowRuns(
  options: WindowRunnerOptions,
  runs: Row[][],
  startedAt: number,
  retainedScope?: MemoryScope,
): Promise<WindowExecutionMode> {
  const scope =
    retainedScope ??
    new MemoryScope(options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER));
  try {
    const currentRun = new WindowRowBuffer(options, scope);
    const execution = (await collectVectorWindowRunMatches(
      options,
      runs,
      currentRun,
      startedAt,
      scope,
    ))
      ? "vector-window"
      : await collectRowWindowRunMatches(options, runs, currentRun, startedAt);
    flushWindowRun(runs, currentRun);
    return execution;
  } finally {
    if (retainedScope === undefined) scope.dispose();
  }
}

async function collectWindowTaskMatches(
  options: WindowRunnerOptions,
  startedAt: number,
  retainedScope: MemoryScope,
): Promise<Row[] | undefined> {
  const { config, stats } = options;
  if (
    config.windows === undefined ||
    (config.scanner.executeOwnedWindowTasks === undefined &&
      config.scanner.executeWindowTasks === undefined)
  ) {
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
  const executionOptions = {
    memoryBudget: retainedScope.budget,
    budget: config.budget,
    stats,
    now: config.now,
    startedAt,
    batchSize: config.batchSize ?? 4096,
  };
  if (config.scanner.executeOwnedWindowTasks !== undefined) {
    const owned = await config.scanner.executeOwnedWindowTasks(
      tasks,
      config.windows,
      executionOptions,
    );
    try {
      if (config.budget.signal?.aborted)
        throw new LakeqlError("LAKEQL_ABORTED", "Window task execution aborted");
      options.enforceBudget(config.budget, stats, config.now, startedAt);
      return retainedScope.adopt(owned).value;
    } catch (error) {
      owned.release();
      throw error;
    }
  }
  const rows = await config.scanner.executeWindowTasks?.(tasks, config.windows, executionOptions);
  if (rows === undefined) return undefined;
  if (config.budget.signal?.aborted)
    throw new LakeqlError("LAKEQL_ABORTED", "Window task execution aborted");
  options.enforceBudget(config.budget, stats, config.now, startedAt);
  // Legacy adapter allocation is external; admit independent retained storage.
  const retained = retainedScope.allocate(
    c.array + rows.length * c.arraySlot,
    "legacy window task rows",
    () => [] as Row[],
  ).value;
  for (const row of rows) retained.push(retainWindowRow(row, retainedScope));
  return retained;
}

async function collectVectorWindowRows(
  options: WindowRunnerOptions,
  startedAt: number,
  retainedScope: MemoryScope,
): Promise<Row[] | undefined> {
  const { config, stats } = options;
  const scanVectorBatches = vectorWindowScanner(options);
  if (scanVectorBatches === undefined) return undefined;

  const { planned: paths, skipped: skippedFiles } = await options.planObjects();
  stats.filesSkipped = skippedFiles;
  const columns = windowReadColumns(config);
  const inputs = retainedScope.allocate(
    c.array,
    "window vector inputs",
    () => [] as VectorWindowBatch[],
  ).value;
  for (const object of paths) {
    stats.filesPlanned += 1;
    stats.filesRead += 1;
    stats.bytesRequested += object.size;
    options.enforceBudget(config.budget, stats, config.now, startedAt);
    const scanOptions = {
      ...vectorScanOptions(options, startedAt),
      memoryBudget: retainedScope.budget,
    };
    if (columns !== undefined && columns.length > 0) scanOptions.columns = columns;
    if (config.where !== undefined) scanOptions.where = config.where;
    for await (const input of scanVectorBatches(config.scanner, object.path, scanOptions)) {
      const stage = retainedScope.childScope();
      try {
        const selection = predicateSelection(input.batch, config.where, stage);
        const count = selectedRowCount(input.batch.rowCount, selection);
        if (count === 0) continue;
        const batch = retainWindowBatch(input, stage);
        retainedScope.adopt(stage.retainAllocations(undefined));
        stats.rowsMatched += count;
        retainedScope.allocate(
          c.arraySlot + c.object + 2 * c.property,
          "window vector input",
          () => {
            inputs.push({ batch, selection });
          },
        );
        options.enforceBufferedRows(config.budget, stats.rowsMatched);
        options.enforceBudget(config.budget, stats, config.now, startedAt);
        stats.elapsedMs = config.now() - startedAt;
      } finally {
        stage.dispose();
      }
    }
  }
  stats.elapsedMs = config.now() - startedAt;
  return applyWindowsToVectorBatches(inputs, config.windows ?? {}, {
    memoryBudget: retainedScope.budget,
    resultScope: retainedScope,
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
  retainedScope: MemoryScope,
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
    const scanOptions = {
      ...vectorScanOptions(options, startedAt),
      memoryBudget: retainedScope.budget,
    };
    if (columns !== undefined && columns.length > 0) scanOptions.columns = columns;
    if (config.where !== undefined) scanOptions.where = config.where;
    for await (const input of scanVectorBatches(config.scanner, object.path, scanOptions)) {
      const stage = retainedScope.childScope();
      try {
        const selection = predicateSelection(input.batch, config.where, stage);
        const count = selectedRowCount(input.batch.rowCount, selection);
        stats.rowsMatched += count;
        if (count === 0) continue;
        const batch = retainWindowBatch(input, stage);
        const source = stage.retainAllocations(batch);
        try {
          const rows = materializeOwnedBatchRows(source, selection);
          try {
            for (const row of rows.value) {
              pushWindowRunRow(options, runs, currentRun, row);
              options.enforceBudget(config.budget, stats, config.now, startedAt);
            }
          } finally {
            rows.release();
          }
        } finally {
          source.release();
        }
        stats.elapsedMs = config.now() - startedAt;
      } finally {
        stage.dispose();
      }
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

type WindowVectorScanner = (
  scanner: ScanAdapter,
  path: string,
  options: ScanOptions,
) => AsyncIterable<ScanVectorBatch & { owner?: OwnedMemory<ScanVectorBatch> }>;

function vectorWindowScanner(options: WindowRunnerOptions): WindowVectorScanner | undefined {
  const { config } = options;
  if (config.hive === true || !options.batchExecution.vectorExprSupported(config.where))
    return undefined;
  const borrowed = options.vectorBatchScanner(config.scanner);
  if (borrowed === undefined && config.scanner.scanOwnedVectorBatches === undefined)
    return undefined;
  return (_scanner, path, scanOptions) => windowScanInputs(options, borrowed, path, scanOptions);
}

function windowBatchOptions(
  options: WindowRunnerOptions,
  startedAt: number,
  resultScope: MemoryScope,
): Omit<WindowBatchExecutionOptions, "matched"> {
  return {
    config: options.config,
    ...(options.memoryBudget === undefined ? {} : { memoryBudget: options.memoryBudget }),
    stats: options.stats,
    startedAt,
    ...options.batchExecution,
    resultScope,
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

function chunkRowsForRuns(rows: Row[], budget: QueryBudget, scope: MemoryScope): Row[][] {
  if (rows.length === 0)
    return scope.allocate(c.array, "empty restored window runs", () => [] as Row[][]).value;
  const capacity = Math.min(rows.length, windowRunCapacity(budget));
  const runs = scope.allocate(
    c.array + Math.ceil(rows.length / capacity) * c.arraySlot,
    "restored window runs",
    () => [] as Row[][],
  ).value;
  for (let index = 0; index < rows.length; index += capacity) {
    runs.push(
      scope.allocate(
        c.array + Math.min(capacity, rows.length - index) * c.arraySlot,
        "restored window run",
        () => rows.slice(index, index + capacity),
      ).value,
    );
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
    private readonly scope: MemoryScope,
    private rows: Row[] = [],
  ) {
    const known = scope.tryRetainAllocation(rows);
    if (known === undefined)
      scope.allocate(c.array + rows.length * c.arraySlot, "window input directory", () => rows);
    else known.release();
    for (let index = 0; index < rows.length; index++) {
      const row = rows[index];
      if (row !== undefined) rows[index] = retainWindowRow(row, scope);
    }
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
    const owned = retainWindowRow(row, this.scope);
    this.scope.allocate(c.arraySlot, "window retained row reference", () => this.rows.push(owned));
    this.memory?.add(owned);
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
    this.scope.allocate(c.arraySlot, "window retained run reference", () => runs.push(this.rows));
    this.rows = this.scope.allocate(c.array, "window next run directory", () => [] as Row[]).value;
    this.memory?.clear();
  }
}

function retainWindowRow(row: Row, scope: MemoryScope): Row {
  const known = scope.tryRetainAllocation(row);
  if (known !== undefined) {
    known.release();
    return row;
  }
  const owner = detachOwnedRow(row, scope.budget);
  try {
    return scope.adopt(owner).value;
  } catch (error) {
    owner.release();
    throw error;
  }
}

async function* windowScanInputs(
  options: WindowRunnerOptions,
  borrowed: VectorBatchScanner | undefined,
  path: string,
  scanOptions: ScanOptions,
): AsyncIterable<ScanVectorBatch & { owner?: OwnedMemory<ScanVectorBatch> }> {
  const owned = options.config.scanner.scanOwnedVectorBatches;
  if (owned === undefined) {
    if (borrowed !== undefined) yield* borrowed(options.config.scanner, path, scanOptions);
    return;
  }
  for await (const owner of owned.call(options.config.scanner, path, {
    ...scanOptions,
    memoryBudget:
      scanOptions.memoryBudget ?? options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER),
  })) {
    try {
      yield { ...owner.value, owner };
    } finally {
      owner.release();
    }
  }
}

function adoptWindowInput<T>(scope: MemoryScope, owner: OwnedMemory<T>): void {
  try {
    scope.adopt(owner);
  } catch (error) {
    owner.release();
    throw error;
  }
}

function retainWindowBatch(
  input: ScanVectorBatch & { owner?: OwnedMemory<ScanVectorBatch> },
  scope: MemoryScope,
): import("./batch.js").Batch {
  if (input.owner !== undefined) {
    adoptWindowInput(scope, input.owner.retainIn(scope.budget));
    return input.batch;
  }
  const owner = detachOwnedBatch(input.batch, scope.budget);
  const batch = owner.value;
  adoptWindowInput(scope, owner);
  return batch;
}
