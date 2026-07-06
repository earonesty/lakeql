import { matches } from "./evaluator.js";
import type { PathQueryInit, QueryBudget } from "./query.js";
import type { QueryStats, Row } from "./types.js";

export interface WindowBatchExecutionOptions {
  config: PathQueryInit & {
    budget: QueryBudget;
    now: () => number;
    metrics?: {
      timing(name: string, value: number, tags?: Record<string, string>): void;
    };
  };
  stats: QueryStats;
  startedAt: number;
  matched: readonly Row[];
  project: (
    row: Row,
    select: string[] | undefined,
    projections: PathQueryInit["projections"],
  ) => Row;
  compareRows: (left: Row, right: Row, orderBy: NonNullable<PathQueryInit["orderBy"]>) => number;
  addDistinctRow: (seen: Set<string>, row: Row, budget: QueryBudget) => boolean;
  enforceBudget: (
    budget: QueryBudget,
    stats: QueryStats,
    now: () => number,
    startedAt: number,
  ) => void;
  enforceBufferedRows: (budget: QueryBudget, rows: number) => void;
  enforceWindowPartitionRows: (budget: QueryBudget, rows: number) => void;
  enforceMemoryBytes: (budget: QueryBudget, bytes: number) => void;
  estimateMemoryBytes: (value: unknown) => number;
}

export async function* windowedQueryBatches(
  options: WindowBatchExecutionOptions,
): AsyncIterable<Row[]> {
  const { config } = options;
  const { applyWindowsToRows } = await import("./window-backend.js");
  const rows = applyWindowsToRows(options.matched, config.windows ?? {}, {
    estimateMemoryBytes: options.estimateMemoryBytes,
    enforceBufferedRows: (rows) => options.enforceBufferedRows(config.budget, rows),
    enforceWindowPartitionRows: (rows) => options.enforceWindowPartitionRows(config.budget, rows),
    enforceMemoryBytes: (bytes) => options.enforceMemoryBytes(config.budget, bytes),
  });
  yield* postWindowQueryBatches({ ...options, rows });
}

export async function* windowedRunQueryBatches(
  options: Omit<WindowBatchExecutionOptions, "matched"> & { runs: readonly (readonly Row[])[] },
): AsyncIterable<Row[]> {
  const { config } = options;
  const { applyWindowsToRuns } = await import("./window-backend.js");
  const runs = applyWindowsToRuns(options.runs, config.windows ?? {}, {
    estimateMemoryBytes: options.estimateMemoryBytes,
    enforceBufferedRows: (rows) => options.enforceBufferedRows(config.budget, rows),
    enforceWindowPartitionRows: (rows) => options.enforceWindowPartitionRows(config.budget, rows),
    enforceMemoryBytes: (bytes) => options.enforceMemoryBytes(config.budget, bytes),
  });
  yield* postWindowQueryBatches({ ...options, rows: runs.flat() });
}

export async function* postWindowQueryBatches(
  options: Omit<WindowBatchExecutionOptions, "matched"> & { rows: readonly Row[] },
): AsyncIterable<Row[]> {
  const { config, stats, startedAt } = options;
  let rows = [...options.rows];
  if (config.qualify !== undefined) rows = rows.filter((row) => matches(config.qualify, row));
  if (config.orderBy !== undefined) {
    rows.sort((left, right) => options.compareRows(left, right, config.orderBy ?? []));
  }

  const distinct = config.distinct === true ? new Set<string>() : undefined;
  const batchSize = config.batchSize ?? 4096;
  let batch: Row[] = [];
  let offsetSkipped = 0;
  let returned = 0;
  for (const row of rows) {
    const projected = options.project(row, config.select, config.projections);
    if (distinct !== undefined && !options.addDistinctRow(distinct, projected, config.budget)) {
      continue;
    }
    if (offsetSkipped < (config.offset ?? 0)) {
      offsetSkipped += 1;
      continue;
    }
    if (config.limit !== undefined && returned >= config.limit) break;
    batch.push(projected);
    returned += 1;
    stats.rowsReturned += 1;
    options.enforceBudget(config.budget, stats, config.now, startedAt);
    if (batch.length >= batchSize) {
      stats.elapsedMs = config.now() - startedAt;
      yield batch;
      batch = [];
    }
  }
  stats.elapsedMs = config.now() - startedAt;
  if (batch.length > 0) yield batch;
  config.metrics?.timing("lakeql.query.elapsed", stats.elapsedMs, { queryId: stats.queryId });
}
