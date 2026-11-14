import {
  type AggregateSpec,
  type CacheAdapter,
  MATERIALIZATION_CHARGES as c,
  detachOwnedRow,
  type Expr,
  internStableDataKey,
  LakeqlError,
  MemoryBudget,
  MemoryScope,
  type OrderByTerm,
  type QueryBudget,
  type QueryStats,
  type ScanOptions,
  type TaskInput,
  type VectorAggregateStateSnapshots,
  type VectorGroupByStateSnapshot,
} from "lakeql-core";
import type { RangeCacheOptions } from "./range-cache.js";
import type { ParquetMetadata } from "./types.js";

export interface ScanParquetTaskOptions {
  memoryBudget?: MemoryBudget;
  scanRangeCache?: RangeCacheOptions;
  batchSize?: number;
  stats?: QueryStats;
  metadataCache?: CacheAdapter<ParquetMetadata>;
  budget?: QueryBudget;
  now?: () => number;
  startedAt?: number;
}

export interface PlanParquetTaskWorkUnitsOptions {
  memoryBudget?: MemoryBudget;
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
  if (
    options.budget === undefined &&
    options.stats === undefined &&
    options.memoryBudget === undefined
  )
    return undefined;
  return {
    ...(options.memoryBudget === undefined ? {} : { memoryBudget: options.memoryBudget }),
    batchSize: options.batchSize ?? 4096,
    stats: options.stats ?? aggregateTaskStats(),
    budget: options.budget ?? {},
    now: options.now ?? (() => Date.now()),
    startedAt: options.startedAt ?? options.now?.() ?? Date.now(),
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

/** Stream ordered ranges; index repeated backward traversal only when measured
 * work reaches a full metadata pass and available headroom covers the index.
 */
export function* iterateTaskRowWindows(
  metadata: ParquetMetadataLike,
  task: TaskInput,
  scope?: MemoryScope,
): IterableIterator<{ rowStart: number; rowEnd: number }> {
  const workspace = scope?.childScope();
  try {
    let groupIndex = 0;
    let rowOffset = 0;
    let visitedGroups = 0;
    let starts: Float64Array | undefined;
    const advance = (end: number) => {
      while (groupIndex < end) {
        rowOffset += Number(metadata.row_groups[groupIndex]?.num_rows ?? 0);
        groupIndex += 1;
        visitedGroups += 1;
      }
      return rowOffset;
    };
    for (const range of task.rowGroupRanges) {
      const start = range.start;
      const end = Math.min(range.end, metadata.row_groups.length);
      if (!Number.isInteger(start) || !Number.isInteger(end) || start < 0 || start >= end) continue;
      if (starts === undefined && start < groupIndex) {
        const indexBytes =
          c.view + (metadata.row_groups.length + 1) * Float64Array.BYTES_PER_ELEMENT;
        if (
          workspace !== undefined &&
          visitedGroups >= metadata.row_groups.length &&
          indexBytes <= workspace.budget.availableBytes / 2 &&
          indexBytes + c.object + 2 * c.property <= workspace.budget.availableBytes
        ) {
          starts = workspace.allocate(
            indexBytes,
            "task row prefix index",
            () => new Float64Array(metadata.row_groups.length + 1),
          ).value;
          let total = 0;
          for (let index = 0; index < metadata.row_groups.length; index += 1) {
            total += Number(metadata.row_groups[index]?.num_rows ?? 0);
            starts[index + 1] = total;
          }
        }
        groupIndex = 0;
        rowOffset = 0;
      }
      const rowStart = starts === undefined ? advance(start) : (starts[start] ?? 0);
      const rowEnd = starts === undefined ? advance(end) : (starts[end] ?? 0);
      const reservation = scope?.reserve(c.object + 2 * c.property, "task row window");
      try {
        yield { rowStart, rowEnd };
      } finally {
        reservation?.release();
      }
    }
  } finally {
    workspace?.dispose();
  }
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

export function groupAggregateTasks(tasks: TaskInput[], parent?: MemoryScope): TaskInput[] {
  const scope = parent?.childScope() ?? new MemoryScope(new MemoryBudget(Number.MAX_SAFE_INTEGER));
  const keys = scope.childScope();
  try {
    const groups = keys.allocate(
      c.object,
      "task grouping directory",
      () => new Map<string, { task: TaskInput; combined: boolean }>(),
    ).value;
    for (const task of tasks) {
      const key = aggregateTaskGroupKey(task, keys);
      const existing = groups.get(key);
      if (existing === undefined) {
        const detached = detachOwnedRow(task as unknown as Record<string, unknown>, scope.budget);
        try {
          scope.adopt(detached);
        } catch (error) {
          detached.release();
          throw error;
        }
        keys.allocate(c.property + c.object + 2 * c.property, "task grouping entry", () =>
          groups.set(key, { task: detached.value as unknown as TaskInput, combined: false }),
        );
        continue;
      }
      for (const range of task.rowGroupRanges)
        scope.allocate(c.arraySlot + c.object + 2 * c.property, "grouped task range", () =>
          existing.task.rowGroupRanges.push({ ...range }),
        );
      existing.combined = true;
    }
    const output = scope.allocate(
      c.array + groups.size * c.arraySlot,
      "grouped tasks",
      () => [] as TaskInput[],
    ).value;
    for (const { task, combined } of groups.values()) {
      if (combined) {
        const sorting = scope.reserve(
          c.array + task.rowGroupRanges.length * c.arraySlot * 2,
          "task range sort workspace",
        );
        try {
          mergeOwnedRowGroupRanges(task.rowGroupRanges);
        } finally {
          sorting.release();
        }
      }
      output.push(task);
    }
    if (parent !== undefined) {
      const owned = scope.retainAllocations(output);
      try {
        parent.adopt(owned);
      } catch (error) {
        owned.release();
        throw error;
      }
    }
    return output;
  } finally {
    scope.dispose();
  }
}

/** Reuse the requested projection when no virtual partition columns need removal. */
export function taskPhysicalReadColumns(task: TaskInput, scope: MemoryScope): string[] | undefined {
  const requested = task.projectedColumns;
  if (requested === undefined) return undefined;
  let count = 0;
  for (const column of requested) if (!Object.hasOwn(task.partitionValues, column)) count++;
  if (count === requested.length) return requested;
  const physical = scope.allocate(
    c.array + count * c.arraySlot,
    "task physical projection",
    () => [] as string[],
  ).value;
  for (const column of requested)
    if (!Object.hasOwn(task.partitionValues, column)) physical.push(column);
  return physical;
}

export function aggregateTaskReadColumns(
  task: TaskInput,
  spec: AggregateSpec,
  groupColumns: readonly string[] = [],
  scope?: MemoryScope,
): string[] | undefined {
  const scratch = scope?.childScope();
  try {
    const columns =
      scratch === undefined
        ? new Set<string>()
        : scratch.allocate(c.object, "aggregate column directory", () => new Set<string>()).value;
    const add = (name: string) => {
      if (Object.hasOwn(task.partitionValues, name) || columns.has(name)) return;
      if (scratch === undefined) columns.add(name);
      else
        scratch.allocate(
          c.property + c.string + name.length * c.stringCodeUnit,
          "aggregate column entry",
          () => columns.add(name),
        );
    };
    collectExprColumns(task.residualPredicate, add);
    for (const column of groupColumns) add(column);
    for (const name in spec) {
      if (!Object.hasOwn(spec, name)) continue;
      const aggregate = spec[name];
      if (aggregate === undefined) continue;
      if (aggregate.column !== undefined) add(aggregate.column);
      collectExprColumns(aggregate.expr, add);
    }
    if (columns.size === 0) return undefined;
    const output =
      scope === undefined
        ? [...columns]
        : scope.allocate(c.array + columns.size * c.arraySlot, "aggregate read columns", () => [
            ...columns,
          ]).value;
    const sorting = scratch?.reserve(
      c.array + columns.size * c.arraySlot * 2,
      "aggregate column sort workspace",
    );
    try {
      output.sort();
    } finally {
      sorting?.release();
    }
    return output;
  } finally {
    scratch?.dispose();
  }
}

function aggregateTaskGroupKey(task: TaskInput, scope: MemoryScope): string {
  const scratch = scope.childScope();
  try {
    const fields = scratch.allocate(c.object + 7 * c.property, "task grouping key fields", () => ({
      path: task.path,
      etag: task.etag,
      size: task.size,
      rowGroupCount: task.rowGroupCount,
      partitionValues: task.partitionValues,
      projectedColumns: task.projectedColumns,
      residualPredicate: task.residualPredicate,
    })).value;
    return internStableDataKey(fields, scope);
  } finally {
    scratch.dispose();
  }
}

/** Sort and compact caller-independent range storage once per combined task. */
function mergeOwnedRowGroupRanges(ranges: { start: number; end: number }[]): void {
  ranges.sort((left, right) => left.start - right.start || left.end - right.end);
  let count = 0;
  for (let index = 0; index < ranges.length; index++) {
    const range = ranges[index];
    if (range === undefined) continue;
    const previous = count > 0 ? ranges[count - 1] : undefined;
    if (previous !== undefined && previous.end >= range.start) {
      previous.end = Math.max(previous.end, range.end);
    } else {
      ranges[count++] = range;
    }
  }
  ranges.length = count;
}

function collectExprColumns(expr: Expr | undefined, add: (name: string) => void): void {
  if (expr === undefined) return;
  switch (expr.kind) {
    case "literal":
      return;
    case "column":
      add(expr.name);
      return;
    case "compare":
    case "arithmetic":
      collectExprColumns(expr.left, add);
      collectExprColumns(expr.right, add);
      return;
    case "in":
      collectExprColumns(expr.target, add);
      for (const value of expr.values) collectExprColumns(value, add);
      return;
    case "between":
      collectExprColumns(expr.target, add);
      collectExprColumns(expr.low, add);
      collectExprColumns(expr.high, add);
      return;
    case "null-check":
    case "like":
      collectExprColumns(expr.target, add);
      return;
    case "logical":
      for (const operand of expr.operands) collectExprColumns(operand, add);
      return;
    case "not":
      collectExprColumns(expr.operand, add);
      return;
    case "call":
      for (const arg of expr.args) collectExprColumns(arg, add);
      return;
    case "case":
      for (const branch of expr.whens) {
        collectExprColumns(branch.when, add);
        collectExprColumns(branch.value, add);
      }
      collectExprColumns(expr.else, add);
      return;
  }
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
