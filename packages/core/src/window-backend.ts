import { LakeqlError } from "./errors.js";
import { evaluate } from "./evaluator.js";
import type { MemoryBudget } from "./memory-budget.js";
import type { Row } from "./types.js";
import type { WindowExpr } from "./window.js";
import { aggregateWindowValue } from "./window-aggregate.js";
import { frameRowIterator } from "./window-frame.js";
import { WindowPeers } from "./window-peers.js";
import { compatibleWindowSortGroups } from "./window-query.js";
import { segmentTreeAggregateValues } from "./window-segment-tree.js";
import {
  compareWindowRows,
  isAggregateWindow,
  normalizedWindowName,
  partitionRows,
  partitionSpans,
  samePartition,
  samePeer,
  sortWindowRows,
  type WindowRow,
} from "./window-utils.js";

export interface WindowExecutionOptions {
  memoryBudget?: MemoryBudget;
  estimateMemoryBytes?: (value: unknown) => number;
  enforceBufferedRows?: (rows: number) => void;
  enforceWindowPartitionRows?: (rows: number) => void;
  enforceMemoryBytes?: (bytes: number) => void;
}

export function applyWindowsToRows(
  inputRows: readonly Row[],
  windows: Record<string, WindowExpr>,
  options: WindowExecutionOptions = {},
): Row[] {
  const rows = inputRows.map((row) => ({ ...row }));
  options.enforceBufferedRows?.(rows.length);
  if (options.estimateMemoryBytes !== undefined) {
    options.enforceMemoryBytes?.(options.estimateMemoryBytes(rows));
  }
  for (const group of windowSortGroups(windows)) {
    const sorted = partitionRows(rows, group.sortExpr);
    sortWindowRows(sorted, group.sortExpr.over.orderBy);
    for (const { alias, expr } of group.items) {
      const windowRows = windowRowsForExpr(sorted, expr, group.sortExpr);
      const spans = partitionSpans(windowRows);
      const values = evaluateWindowRows(rows.length, windowRows, spans, expr, options);
      for (let index = 0; index < rows.length; index += 1) {
        const row = rows[index];
        if (row !== undefined) row[alias] = values[index] ?? null;
      }
    }
  }
  return rows;
}

export function applyWindowsToRuns(
  inputRuns: readonly (readonly Row[])[],
  windows: Record<string, WindowExpr>,
  options: WindowExecutionOptions = {},
): Row[][] {
  const outputRuns = inputRuns.map((run) => run.map((row) => ({ ...row })));
  for (const group of windowSortGroups(windows)) {
    const sortedRuns = outputRuns.map((run, runIndex) =>
      partitionRows(run, group.sortExpr).map((item, rowIndex) => ({
        ...item,
        runIndex,
        rowIndex,
      })),
    );
    for (const run of sortedRuns) sortWindowRows(run, group.sortExpr.over.orderBy);
    for (const partition of mergeWindowPartitions(sortedRuns, group.sortExpr)) {
      options.enforceWindowPartitionRows?.(partition.length);
      if (options.estimateMemoryBytes !== undefined) {
        options.enforceMemoryBytes?.(
          options.estimateMemoryBytes(partition.map((item) => item.row)),
        );
      }
      for (const { alias, expr } of group.items) {
        const windowRows = windowRowsForExpr(partition, expr, group.sortExpr);
        const values = evaluatePartition(windowRows, expr, options);
        for (let index = 0; index < windowRows.length; index += 1) {
          const item = windowRows[index];
          if (item === undefined) continue;
          const row = outputRuns[item.runIndex]?.[item.rowIndex];
          if (row !== undefined) row[alias] = values[index] ?? null;
        }
      }
    }
  }
  return outputRuns;
}

export function windowSortGroups(windows: Record<string, WindowExpr>) {
  return compatibleWindowSortGroups(
    Object.entries(windows).map(([alias, expr]) => ({ alias, expr })),
  );
}

export function windowRowsForExpr<T extends WindowRow>(
  sorted: readonly T[],
  expr: WindowExpr,
  sortExpr: WindowExpr,
): T[] {
  const orderKeyLength = expr.over.orderBy.length;
  const projected = sorted.map((item) => ({
    ...item,
    orderKey: item.orderKey.slice(0, orderKeyLength),
  })) as T[];
  if (orderKeyLength === sortExpr.over.orderBy.length) return projected;
  stabilizePrefixPeers(projected);
  return projected;
}

interface RunWindowRow extends WindowRow {
  runIndex: number;
  rowIndex: number;
}

function* mergeWindowPartitions(
  runs: readonly RunWindowRow[][],
  sortExpr: WindowExpr,
): Iterable<RunWindowRow[]> {
  const cursors = runs.map(() => 0);
  let current: RunWindowRow[] = [];
  let currentPartition: RunWindowRow | undefined;
  while (true) {
    let bestRun = -1;
    let best: RunWindowRow | undefined;
    for (let runIndex = 0; runIndex < runs.length; runIndex += 1) {
      const candidate = runs[runIndex]?.[cursors[runIndex] ?? 0];
      if (candidate === undefined) continue;
      if (best === undefined || compareWindowRows(candidate, best, sortExpr.over.orderBy) < 0) {
        best = candidate;
        bestRun = runIndex;
      }
    }
    if (best === undefined || bestRun === -1) break;
    cursors[bestRun] = (cursors[bestRun] ?? 0) + 1;
    if (currentPartition !== undefined && !samePartition(best, currentPartition)) {
      yield current;
      current = [];
    }
    currentPartition = best;
    current.push(best);
  }
  if (current.length > 0) yield current;
}

function stabilizePrefixPeers(rows: WindowRow[]): void {
  let start = 0;
  while (start < rows.length) {
    const current = rows[start];
    if (current === undefined) break;

    let end = start + 1;
    while (end < rows.length && samePeer(rows[end] as WindowRow, current)) {
      end += 1;
    }
    if (end - start > 1) {
      rows
        .slice(start, end)
        .sort((left, right) => left.originalIndex - right.originalIndex)
        .forEach((row, index) => {
          rows[start + index] = row;
        });
    }
    start = end;
  }
}

export function evaluateWindowRows(
  rowCount: number,
  sorted: readonly WindowRow[],
  spans: readonly { start: number; end: number }[],
  expr: WindowExpr,
  options: WindowExecutionOptions,
): unknown[] {
  const output = new Array<unknown>(rowCount).fill(null);
  for (const span of spans) {
    const partition = sorted.slice(span.start, span.end);
    options.enforceWindowPartitionRows?.(partition.length);
    const values = evaluatePartition(partition, expr, options);
    for (let index = 0; index < partition.length; index += 1) {
      const item = partition[index];
      if (item !== undefined) output[item.originalIndex] = values[index] ?? null;
    }
  }
  return output;
}

function evaluatePartition(
  partition: readonly WindowRow[],
  expr: WindowExpr,
  options: WindowExecutionOptions,
): unknown[] {
  const peers = new WindowPeers(partition, options.memoryBudget);
  try {
    return evaluatePartitionWithPeers(partition, expr, peers, options.memoryBudget);
  } finally {
    peers.dispose();
  }
}

function evaluatePartitionWithPeers(
  partition: readonly WindowRow[],
  expr: WindowExpr,
  peers: WindowPeers,
  budget?: MemoryBudget,
): unknown[] {
  const fn = normalizedWindowName(expr.fn);
  if (isAggregateWindow(expr.fn)) {
    const segmentValues = segmentTreeAggregateValues(partition, expr, peers, budget);
    if (segmentValues !== undefined) return segmentValues;
    return partition.map((_item, index) =>
      aggregateWindowValue(frameRowIterator(partition, index, expr, peers), expr, budget),
    );
  }
  switch (fn) {
    case "row_number":
      return partition.map((_row, index) => index + 1);
    case "rank":
      return rankValues(partition.length, peers, false);
    case "dense_rank":
      return rankValues(partition.length, peers, true);
    case "percent_rank": {
      if (partition.length <= 1) return partition.map(() => 0);
      const ranks = rankValues(partition.length, peers, false) as number[];
      for (let index = 0; index < ranks.length; index++)
        ranks[index] = ((ranks[index] ?? 1) - 1) / (partition.length - 1);
      return ranks;
    }
    case "cume_dist":
      return cumeDistValues(partition.length, peers);
    case "ntile":
      return ntileValues(partition.length, expr);
    case "lag":
      return offsetValues(partition, expr, -1);
    case "lead":
      return offsetValues(partition, expr, 1);
    case "first_value":
      return partition.map((_item, index) => {
        return frameValueAt(frameRowIterator(partition, index, expr, peers), expr, 1);
      });
    case "last_value":
      return partition.map((_item, index) => {
        return frameValueAt(frameRowIterator(partition, index, expr, peers, true), expr, 1);
      });
    case "nth_value":
      return nthValue(partition, expr, peers);
    default:
      throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", `Unsupported window function ${fn}`, {
        function: fn,
      });
  }
}

/** Ranking consumes the same immutable peer boundaries as frame evaluation.
 * Sequential cursor traversal visits each boundary once, including all-tie input. */
function rankValues(length: number, peers: WindowPeers, dense: boolean): number[] {
  const ranks = new Array<number>(length);
  for (let index = 0; index < length; index++) {
    const peer = peers.peer(index);
    ranks[index] = (dense ? peer.group : peer.start) + 1;
  }
  return ranks;
}

function cumeDistValues(length: number, peers: WindowPeers): number[] {
  const values = new Array<number>(length);
  for (let index = 0; index < length; index++) values[index] = peers.peer(index).end / length;
  return values;
}

function ntileValues(length: number, expr: WindowExpr): number[] {
  const bucketCount = numericArg(expr, 0, 0);
  if (!Number.isInteger(bucketCount) || bucketCount <= 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "ntile requires a positive integer bucket count");
  }
  const base = Math.floor(length / bucketCount);
  const extra = length % bucketCount;
  return Array.from({ length }, (_row, index) =>
    index < (base + 1) * extra
      ? Math.floor(index / (base + 1)) + 1
      : extra + Math.floor((index - (base + 1) * extra) / base) + 1,
  );
}

function offsetValues(rows: readonly WindowRow[], expr: WindowExpr, direction: -1 | 1): unknown[] {
  const offset = expr.args.length >= 2 ? numericArg(expr, 1, 1) : 1;
  if (!Number.isInteger(offset) || offset < 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "lag/lead offset must be a non-negative integer");
  }
  if (expr.ignoreNulls === true) return offsetIgnoringNulls(rows, expr, direction, offset);
  return rows.map((row, index) => {
    const target = rows[index + direction * offset];
    if (target === undefined)
      return expr.args[2] === undefined ? null : evaluate(expr.args[2], row.row);
    return evaluate(valueArg(expr), target.row);
  });
}

/** Maintain the count of non-null values between the current and target row.
 * Both cursors move once through the partition; no offset-sized ring is needed. */
function offsetIgnoringNulls(
  rows: readonly WindowRow[],
  expr: WindowExpr,
  direction: -1 | 1,
  offset: number,
): unknown[] {
  const arg = valueArg(expr);
  if (offset === 0) return rows.map((item) => evaluate(arg, item.row));
  const output = new Array<unknown>(rows.length);
  const start = direction === 1 ? 0 : rows.length - 1;
  let target = start;
  let count = 0;
  let targetValue: unknown = null;
  for (let index = start; index >= 0 && index < rows.length; index += direction) {
    while (count < offset) {
      const next = target + direction;
      if (next < 0 || next >= rows.length) break;
      target = next;
      const item = rows[target];
      if (item === undefined) continue;
      const value = evaluate(arg, item.row);
      if (value !== null) {
        count++;
        targetValue = value;
      }
    }
    const item = rows[index];
    output[index] =
      count === offset
        ? targetValue
        : expr.args[2] === undefined || item === undefined
          ? null
          : evaluate(expr.args[2], item.row);
    const nextIndex = index + direction;
    if (
      nextIndex >= 0 &&
      nextIndex < rows.length &&
      (direction === 1 ? nextIndex <= target : nextIndex >= target)
    ) {
      const departing = rows[nextIndex];
      if (departing !== undefined && evaluate(arg, departing.row) !== null) count--;
    }
  }
  return output;
}

function nthValue(
  partition: readonly WindowRow[],
  expr: WindowExpr,
  peers: WindowPeers,
): unknown[] {
  const nth = numericArg(expr, 1, 1);
  if (!Number.isInteger(nth) || nth <= 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "nth_value requires a positive integer position");
  }
  return partition.map((_item, index) => {
    return frameValueAt(frameRowIterator(partition, index, expr, peers), expr, nth);
  });
}

function valueArg(expr: WindowExpr) {
  const arg = expr.args[0];
  if (arg === undefined) {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      `${normalizedWindowName(expr.fn)} requires a value argument`,
    );
  }
  return arg;
}

function numericArg(expr: WindowExpr, index: number, fallback: number): number {
  const arg = expr.args[index];
  if (arg === undefined) return fallback;
  if (arg.kind !== "literal" || typeof arg.value !== "number") {
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      `${normalizedWindowName(expr.fn)} argument must be numeric literal`,
    );
  }
  return arg.value;
}

function frameValueAt(rows: Iterable<Row>, expr: WindowExpr, ordinal: number): unknown {
  for (const row of rows) {
    const value = evaluate(valueArg(expr), row);
    if (expr.ignoreNulls === true && value === null) continue;
    if (--ordinal === 0) return value;
  }
  return null;
}
