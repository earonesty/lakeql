import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { matches } from "./evaluator.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import type { PathQueryInit, QueryBudget } from "./query.js";
import { appendProjectedOutput } from "./row-output.js";
import type { QueryStats, Row } from "./types.js";

export interface WindowBatchExecutionOptions {
  memoryBudget?: MemoryBudget;
  resultScope?: MemoryScope;
  /** Synchronous receiver retains independently before the producer advances. */
  retainOutput?: (output: OwnedMemory<Row[]>) => void;
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
    scope?: MemoryScope,
  ) => Row;
  compareRows: (left: Row, right: Row, orderBy: NonNullable<PathQueryInit["orderBy"]>) => number;
  createDistinct: (scope: MemoryScope, budget: QueryBudget) => { add(row: Row): boolean };
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
  const resultScope =
    options.resultScope ??
    new MemoryScope(options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER));
  try {
    const { config } = options;
    const { applyWindowsToRows } = await import("./window-backend.js");
    const rows = applyWindowsToRows(options.matched, config.windows ?? {}, {
      memoryBudget: resultScope.budget,
      resultScope,
      estimateMemoryBytes: options.estimateMemoryBytes,
      enforceBufferedRows: (rows) => options.enforceBufferedRows(config.budget, rows),
      enforceWindowPartitionRows: (rows) => options.enforceWindowPartitionRows(config.budget, rows),
      enforceMemoryBytes: (bytes) => options.enforceMemoryBytes(config.budget, bytes),
    });
    yield* postWindowQueryBatches({ ...options, rows });
  } finally {
    if (options.resultScope === undefined) resultScope.dispose();
  }
}

export async function* windowedRunQueryBatches(
  options: Omit<WindowBatchExecutionOptions, "matched"> & { runs: readonly (readonly Row[])[] },
): AsyncIterable<Row[]> {
  const resultScope =
    options.resultScope ??
    new MemoryScope(options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER));
  try {
    const { config } = options;
    const { applyWindowsToRuns } = await import("./window-backend.js");
    const runs = applyWindowsToRuns(options.runs, config.windows ?? {}, {
      memoryBudget: resultScope.budget,
      resultScope,
      estimateMemoryBytes: options.estimateMemoryBytes,
      enforceBufferedRows: (rows) => options.enforceBufferedRows(config.budget, rows),
      enforceWindowPartitionRows: (rows) => options.enforceWindowPartitionRows(config.budget, rows),
      enforceMemoryBytes: (bytes) => options.enforceMemoryBytes(config.budget, bytes),
    });
    yield* postWindowQueryBatches({ ...options, rows: rowsFromRuns(runs) });
  } finally {
    if (options.resultScope === undefined) resultScope.dispose();
  }
}

export async function* postWindowQueryBatches(
  options: Omit<WindowBatchExecutionOptions, "matched"> & { rows: Iterable<Row> },
): AsyncIterable<Row[]> {
  const { config, stats, startedAt } = options;
  const scope = new MemoryScope(options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER));
  try {
    let rows = options.rows;
    // QUALIFY runs before ordering, DISTINCT, offset and limit. Retain only the
    // qualifying references; no directory is needed for an unchanged sequence.
    if (config.qualify !== undefined || config.orderBy !== undefined) {
      const selected = scope.allocate(
        c.array,
        "window output row directory",
        () => [] as Row[],
      ).value;
      for (const row of rows) {
        if (config.qualify !== undefined && !matches(config.qualify, row, scope)) continue;
        scope.allocate(c.arraySlot, "window output row reference", () => selected.push(row));
      }
      if (config.orderBy !== undefined) {
        const scratch = scope.reserve(
          c.array + selected.length * (c.arraySlot + c.object),
          "window output sort workspace",
        );
        try {
          selected.sort((left, right) => options.compareRows(left, right, config.orderBy ?? []));
        } finally {
          scratch.release();
        }
      }
      rows = selected;
    }
    const distinct =
      config.distinct === true ? options.createDistinct(scope, config.budget) : undefined;
    const batchSize = config.batchSize ?? 4096;
    let batchScope = scope.childScope();
    const newBatch = () =>
      batchScope.allocate(c.array, "window output batch", () => [] as Row[]).value;
    let batch = newBatch();
    const retainBatch = () => {
      if (options.retainOutput === undefined) return;
      const owner = batchScope.retainAllocations(batch);
      try {
        options.retainOutput(owner);
      } finally {
        owner.release();
      }
    };
    let offsetSkipped = 0;
    let returned = 0;
    for (const row of rows) {
      const projection = batchScope.childScope();
      try {
        const projected = options.project(row, config.select, config.projections, projection);
        if (distinct !== undefined && !distinct.add(projected)) continue;
        if (offsetSkipped < (config.offset ?? 0)) {
          offsetSkipped += 1;
          continue;
        }
        if (config.limit !== undefined && returned >= config.limit) break;
        appendProjectedOutput(
          batch,
          projected,
          projection,
          batchScope,
          options.retainOutput !== undefined,
        );
        returned += 1;
        stats.rowsReturned += 1;
        options.enforceBudget(config.budget, stats, config.now, startedAt);
      } finally {
        projection.dispose();
      }
      if (batch.length >= batchSize) {
        stats.elapsedMs = config.now() - startedAt;
        retainBatch();
        yield batch;
        batchScope.dispose();
        batchScope = scope.childScope();
        batch = newBatch();
      }
    }
    stats.elapsedMs = config.now() - startedAt;
    if (batch.length > 0) {
      retainBatch();
      yield batch;
    }
    config.metrics?.timing("lakeql.query.elapsed", stats.elapsedMs, { queryId: stats.queryId });
  } finally {
    scope.dispose();
  }
}

function* rowsFromRuns(runs: readonly (readonly Row[])[]): Iterable<Row> {
  for (const run of runs) yield* run;
}
