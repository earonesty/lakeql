import {
  type AggregateSpec,
  type CacheAdapter,
  type Expr,
  LakeqlError,
  type OrderByTerm,
  type QueryBudget,
  type QueryStats,
  type ScanOptions,
  type TaskInput,
  type VectorAggregateStateSnapshots,
  type VectorGroupByStateSnapshot,
} from "lakeql-core";
import type { ParquetMetadata } from "./types.js";

export interface ScanParquetTaskOptions {
  batchSize?: number;
  stats?: QueryStats;
  metadataCache?: CacheAdapter<ParquetMetadata>;
  budget?: QueryBudget;
  now?: () => number;
  startedAt?: number;
}

export interface PlanParquetTaskWorkUnitsOptions {
  maxRowGroupsPerTask?: number;
  maxRowsPerTask?: number;
  metadataCache?: CacheAdapter<ParquetMetadata>;
}

export interface AggregateParquetTaskOptions extends ScanParquetTaskOptions {
  budget?: QueryBudget;
}

export interface AggregateParquetTasksOptions extends AggregateParquetTaskOptions {
  maxConcurrentTasks?: number;
  maxBufferedPartials?: number;
  preserveTaskBoundaries?: boolean;
  partialBoundary?(
    partial: VectorAggregateStateSnapshots,
    task: TaskInput,
    index: number,
  ): VectorAggregateStateSnapshots | Promise<VectorAggregateStateSnapshots>;
}

export interface AggregateParquetGroupTaskOptions extends AggregateParquetTaskOptions {
  maxGroups?: number;
}

export interface AggregateParquetGroupTasksOptions extends AggregateParquetGroupTaskOptions {
  maxConcurrentTasks?: number;
  maxBufferedPartials?: number;
  preserveTaskBoundaries?: boolean;
  orderBy?: OrderByTerm[];
  limit?: number;
  offset?: number;
  partialBoundary?(
    partial: VectorGroupByStateSnapshot,
    task: TaskInput,
    index: number,
  ): VectorGroupByStateSnapshot | Promise<VectorGroupByStateSnapshot>;
}

export interface ParquetTaskReadOptions {
  rowStart: number;
  rowEnd: number;
  batchSize?: number;
  columns?: string[];
  where?: Expr;
  stats?: QueryStats;
}

interface RowGroupMetadataLike {
  num_rows: unknown;
}

interface ParquetMetadataLike {
  row_groups: RowGroupMetadataLike[];
}

export function taskReadOptions(
  rowStart: number,
  rowEnd: number,
  physicalColumns: string[] | undefined,
  residualPredicate: Expr | undefined,
  options: ScanParquetTaskOptions,
): ParquetTaskReadOptions {
  const readOptions: ParquetTaskReadOptions = { rowStart, rowEnd };
  if (options.batchSize !== undefined) readOptions.batchSize = options.batchSize;
  if (options.stats !== undefined) readOptions.stats = options.stats;
  if (physicalColumns !== undefined && physicalColumns.length > 0) {
    readOptions.columns = physicalColumns;
  }
  if (residualPredicate !== undefined) readOptions.where = residualPredicate;
  return readOptions;
}

export function aggregateScanOptions(
  options: AggregateParquetTaskOptions,
): ScanOptions | undefined {
  if (options.budget === undefined && options.stats === undefined) return undefined;
  return {
    batchSize: options.batchSize ?? 4096,
    stats: options.stats ?? aggregateTaskStats(),
    budget: options.budget ?? {},
    now: () => Date.now(),
    startedAt: Date.now(),
  };
}

export function aggregateVectorOptions(options: AggregateParquetTaskOptions): {
  budget?: QueryBudget;
} {
  return options.budget === undefined ? {} : { budget: options.budget };
}

export function enforceAggregateTaskBudget(options: ScanOptions | undefined): void {
  if (options === undefined) return;
  const budget = options.budget;
  const stats = options.stats;
  const elapsedMs = options.now() - options.startedAt;
  if (budget.maxBytes !== undefined && stats.bytesRequested > budget.maxBytes) {
    throwAggregateTaskBudget("bytes", budget.maxBytes, stats.bytesRequested);
  }
  if (budget.maxRangeRequests !== undefined && stats.rangeRequests > budget.maxRangeRequests) {
    throwAggregateTaskBudget("range requests", budget.maxRangeRequests, stats.rangeRequests);
  }
  if (budget.maxRowsDecoded !== undefined && stats.rowsDecoded > budget.maxRowsDecoded) {
    throwAggregateTaskBudget("rows decoded", budget.maxRowsDecoded, stats.rowsDecoded);
  }
  if (budget.maxElapsedMs !== undefined && elapsedMs > budget.maxElapsedMs) {
    throwAggregateTaskBudget("elapsed milliseconds", budget.maxElapsedMs, elapsedMs);
  }
}

export function taskRowWindows(
  metadata: ParquetMetadataLike,
  task: TaskInput,
): { rowStart: number; rowEnd: number }[] {
  const windows: { rowStart: number; rowEnd: number }[] = [];
  const starts: number[] = [];
  let rowStart = 0;
  for (const rowGroup of metadata.row_groups) {
    starts.push(rowStart);
    rowStart += Number(rowGroup.num_rows);
  }
  for (const range of task.rowGroupRanges) {
    const startGroup = range.start;
    const endGroup = Math.min(range.end, metadata.row_groups.length);
    if (startGroup < 0 || startGroup >= endGroup || startGroup >= metadata.row_groups.length) {
      continue;
    }
    const start = starts[startGroup] ?? 0;
    const lastGroup = metadata.row_groups[endGroup - 1];
    const end = (starts[endGroup - 1] ?? start) + Number(lastGroup?.num_rows ?? 0);
    windows.push({ rowStart: start, rowEnd: end });
  }
  return windows;
}

export function validateTaskWorkUnitOptions(options: PlanParquetTaskWorkUnitsOptions): void {
  if (options.maxRowGroupsPerTask === undefined && options.maxRowsPerTask === undefined) {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      "Parquet task work units require maxRowGroupsPerTask or maxRowsPerTask",
    );
  }
  if (
    options.maxRowGroupsPerTask !== undefined &&
    (!Number.isInteger(options.maxRowGroupsPerTask) || options.maxRowGroupsPerTask < 1)
  ) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "maxRowGroupsPerTask must be a positive integer", {
      maxRowGroupsPerTask: options.maxRowGroupsPerTask,
    });
  }
  if (
    options.maxRowsPerTask !== undefined &&
    (!Number.isInteger(options.maxRowsPerTask) || options.maxRowsPerTask < 1)
  ) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "maxRowsPerTask must be a positive integer", {
      maxRowsPerTask: options.maxRowsPerTask,
    });
  }
}

export function appendRowGroupRange(
  ranges: { start: number; end: number }[],
  rowGroup: number,
): void {
  const previous = ranges.at(-1);
  if (previous !== undefined && previous.end === rowGroup) {
    previous.end = rowGroup + 1;
  } else {
    ranges.push({ start: rowGroup, end: rowGroup + 1 });
  }
}

export function cloneTaskWithRanges(
  task: TaskInput,
  rowGroupRanges: { start: number; end: number }[],
): TaskInput {
  const clone: TaskInput = {
    path: task.path,
    rowGroupRanges: rowGroupRanges.map((range) => ({ ...range })),
    partitionValues: { ...task.partitionValues },
  };
  if (task.size !== undefined) clone.size = task.size;
  if (task.etag !== undefined) clone.etag = task.etag;
  if (task.rowGroupCount !== undefined) clone.rowGroupCount = task.rowGroupCount;
  if (task.projectedColumns !== undefined) clone.projectedColumns = [...task.projectedColumns];
  if (task.residualPredicate !== undefined) clone.residualPredicate = task.residualPredicate;
  return clone;
}

export function groupAggregateTasks(tasks: TaskInput[]): TaskInput[] {
  const groups = new Map<string, TaskInput>();
  for (const task of tasks) {
    const key = aggregateTaskGroupKey(task);
    const existing = groups.get(key);
    if (existing === undefined) {
      groups.set(key, cloneTaskWithRanges(task, task.rowGroupRanges));
      continue;
    }
    existing.rowGroupRanges = mergeRowGroupRanges([
      ...existing.rowGroupRanges,
      ...task.rowGroupRanges,
    ]);
  }
  return [...groups.values()];
}

export function aggregateTaskReadColumns(
  task: TaskInput,
  spec: AggregateSpec,
  groupColumns: readonly string[] = [],
): string[] | undefined {
  const columns = new Set<string>();
  const partitionColumns = new Set(Object.keys(task.partitionValues));
  collectExprColumns(task.residualPredicate, columns, partitionColumns);
  for (const column of groupColumns) {
    if (!(column in task.partitionValues)) columns.add(column);
  }
  for (const aggregate of Object.values(spec)) {
    if (aggregate.column !== undefined && !(aggregate.column in task.partitionValues)) {
      columns.add(aggregate.column);
    }
    collectExprColumns(aggregate.expr, columns, partitionColumns);
  }
  return columns.size === 0 ? undefined : [...columns].sort();
}

function aggregateTaskGroupKey(task: TaskInput): string {
  return JSON.stringify({
    path: task.path,
    etag: task.etag,
    size: task.size,
    rowGroupCount: task.rowGroupCount,
    partitionValues: sortStringRecord(task.partitionValues),
    projectedColumns: task.projectedColumns,
    residualPredicate: task.residualPredicate,
  });
}

function mergeRowGroupRanges(
  ranges: { start: number; end: number }[],
): { start: number; end: number }[] {
  const sorted = ranges
    .map((range) => ({ ...range }))
    .sort((left, right) => left.start - right.start || left.end - right.end);
  const merged: { start: number; end: number }[] = [];
  for (const range of sorted) {
    const previous = merged.at(-1);
    if (previous !== undefined && previous.end >= range.start) {
      previous.end = Math.max(previous.end, range.end);
    } else {
      merged.push(range);
    }
  }
  return merged;
}

function collectExprColumns(
  expr: Expr | undefined,
  columns: Set<string>,
  partitionColumns: ReadonlySet<string>,
): void {
  if (expr === undefined) return;
  switch (expr.kind) {
    case "literal":
      return;
    case "column":
      if (!partitionColumns.has(expr.name)) columns.add(expr.name);
      return;
    case "compare":
    case "arithmetic":
      collectExprColumns(expr.left, columns, partitionColumns);
      collectExprColumns(expr.right, columns, partitionColumns);
      return;
    case "in":
      collectExprColumns(expr.target, columns, partitionColumns);
      for (const value of expr.values) collectExprColumns(value, columns, partitionColumns);
      return;
    case "between":
      collectExprColumns(expr.target, columns, partitionColumns);
      collectExprColumns(expr.low, columns, partitionColumns);
      collectExprColumns(expr.high, columns, partitionColumns);
      return;
    case "null-check":
    case "like":
      collectExprColumns(expr.target, columns, partitionColumns);
      return;
    case "logical":
      for (const operand of expr.operands) collectExprColumns(operand, columns, partitionColumns);
      return;
    case "not":
      collectExprColumns(expr.operand, columns, partitionColumns);
      return;
    case "call":
      for (const arg of expr.args) collectExprColumns(arg, columns, partitionColumns);
      return;
    case "case":
      for (const branch of expr.whens) {
        collectExprColumns(branch.when, columns, partitionColumns);
        collectExprColumns(branch.value, columns, partitionColumns);
      }
      collectExprColumns(expr.else, columns, partitionColumns);
      return;
  }
}

function sortStringRecord(record: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const key of Object.keys(record).sort()) out[key] = record[key] ?? "";
  return out;
}

function aggregateTaskStats(): QueryStats {
  return {
    queryId: "aggregate-parquet-task",
    elapsedMs: 0,
    manifestsRead: 0,
    manifestsSkipped: 0,
    filesPlanned: 0,
    filesRead: 0,
    filesSkipped: 0,
    rowGroupsRead: 0,
    rowGroupsSkipped: 0,
    columnsRead: [],
    bytesRequested: 0,
    rangeRequests: 0,
    rowsDecoded: 0,
    rowsMatched: 0,
    rowsReturned: 0,
    cacheHits: 0,
    cacheMisses: 0,
  };
}

function throwAggregateTaskBudget(metric: string, limit: number, actual: number): never {
  throw new LakeqlError(
    "LAKEQL_BUDGET_EXCEEDED",
    `Query exceeded ${metric} budget (${actual} > ${limit}). Add a partition filter, date filter, h3 filter, or limit.`,
    { metric, limit, actual },
  );
}
