import { LakeqlError } from "./errors.js";
import { evaluate } from "./evaluator.js";
import type { Row } from "./types.js";
import type { WindowExpr } from "./window.js";
import { aggregateWindowValue } from "./window-aggregate.js";
import { frameRows } from "./window-frame.js";
import { compatibleWindowSortGroups } from "./window-query.js";
import { segmentTreeAggregateValues } from "./window-segment-tree.js";
import {
  compareWindowRows,
  isAggregateWindow,
  normalizedWindowName,
  partitionRows,
  partitionSpans,
  samePeer,
  sortWindowRows,
  stableKey,
  type WindowRow,
} from "./window-utils.js";

export interface WindowExecutionOptions {
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
        const values = evaluatePartition(windowRows, expr);
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
  let currentPartitionKey: string | undefined;
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
    const partitionKey = stableKey(best.partitionKey);
    if (currentPartitionKey !== undefined && partitionKey !== currentPartitionKey) {
      yield current;
      current = [];
    }
    currentPartitionKey = partitionKey;
    current.push(best);
  }
  if (current.length > 0) yield current;
}

function stabilizePrefixPeers(rows: WindowRow[]): void {
  let start = 0;
  while (start < rows.length) {
    const current = rows[start];
    if (current === undefined) break;
    const partitionKey = stableKey(current.partitionKey);
    const orderKey = stableKey(current.orderKey);
    let end = start + 1;
    while (
      end < rows.length &&
      stableKey(rows[end]?.partitionKey ?? []) === partitionKey &&
      stableKey(rows[end]?.orderKey ?? []) === orderKey
    ) {
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
    const values = evaluatePartition(partition, expr);
    for (let index = 0; index < partition.length; index += 1) {
      const item = partition[index];
      if (item !== undefined) output[item.originalIndex] = values[index] ?? null;
    }
  }
  return output;
}

function evaluatePartition(partition: readonly WindowRow[], expr: WindowExpr): unknown[] {
  const rows = partition.map((item) => item.row);
  const fn = normalizedWindowName(expr.fn);
  if (isAggregateWindow(expr.fn)) {
    const segmentValues = segmentTreeAggregateValues(partition, expr);
    if (segmentValues !== undefined) return segmentValues;
    return partition.map((_item, index) =>
      aggregateWindowValue(frameRows(partition, index, expr), expr),
    );
  }
  switch (fn) {
    case "row_number":
      return rows.map((_row, index) => index + 1);
    case "rank":
      return rankValues(rows, expr, false);
    case "dense_rank":
      return rankValues(rows, expr, true);
    case "percent_rank": {
      if (rows.length <= 1) return rows.map(() => 0);
      const ranks = rankValues(rows, expr, false) as number[];
      return ranks.map((rank) => (rank - 1) / (rows.length - 1));
    }
    case "cume_dist":
      return cumeDistValues(rows, expr);
    case "ntile":
      return ntileValues(rows, expr);
    case "lag":
      return offsetValues(rows, expr, -1);
    case "lead":
      return offsetValues(rows, expr, 1);
    case "first_value":
      return partition.map((_item, index) => {
        const row = firstFrameValue(frameRows(partition, index, expr), expr);
        return row === undefined ? null : evaluate(valueArg(expr), row);
      });
    case "last_value":
      return partition.map((_item, index) => {
        const row = lastFrameValue(frameRows(partition, index, expr), expr);
        return row === undefined ? null : evaluate(valueArg(expr), row);
      });
    case "nth_value":
      return nthValue(partition, expr);
    default:
      throw new LakeqlError("LAKEQL_SQL_UNSUPPORTED", `Unsupported window function ${fn}`, {
        function: fn,
      });
  }
}

function rankValues(rows: readonly Row[], expr: WindowExpr, dense: boolean): number[] {
  const sorted = partitionRows(rows, expr);
  const ranks: number[] = [];
  let rank = 1;
  let denseRank = 1;
  for (let index = 0; index < sorted.length; index += 1) {
    const previous = sorted[index - 1];
    const current = sorted[index];
    if (
      index > 0 &&
      previous !== undefined &&
      current !== undefined &&
      !samePeer(previous, current)
    ) {
      rank = index + 1;
      denseRank += 1;
    }
    ranks.push(dense ? denseRank : rank);
  }
  return ranks;
}

function cumeDistValues(rows: readonly Row[], expr: WindowExpr): number[] {
  const sorted = partitionRows(rows, expr);
  const out: number[] = [];
  for (let index = 0; index < sorted.length; index += 1) {
    let endPeer = index;
    while (endPeer + 1 < sorted.length) {
      const current = sorted[index];
      const next = sorted[endPeer + 1];
      if (current === undefined || next === undefined || !samePeer(current, next)) break;
      endPeer += 1;
    }
    out.push((endPeer + 1) / rows.length);
  }
  return out;
}

function ntileValues(rows: readonly Row[], expr: WindowExpr): number[] {
  const bucketCount = numericArg(expr, 0, 0);
  if (!Number.isInteger(bucketCount) || bucketCount <= 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "ntile requires a positive integer bucket count");
  }
  return rows.map((_row, index) => Math.floor((index * bucketCount) / rows.length) + 1);
}

function offsetValues(rows: readonly Row[], expr: WindowExpr, direction: -1 | 1): unknown[] {
  const offset = expr.args.length >= 2 ? numericArg(expr, 1, 1) : 1;
  if (!Number.isInteger(offset) || offset < 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "lag/lead offset must be a non-negative integer");
  }
  if (expr.ignoreNulls === true) return offsetIgnoringNulls(rows, expr, direction, offset);
  return rows.map((row, index) => {
    const target = rows[index + direction * offset];
    if (target === undefined)
      return expr.args[2] === undefined ? null : evaluate(expr.args[2], row);
    return evaluate(valueArg(expr), target);
  });
}

function offsetIgnoringNulls(
  rows: readonly Row[],
  expr: WindowExpr,
  direction: -1 | 1,
  offset: number,
): unknown[] {
  const arg = valueArg(expr);
  return rows.map((row, index) => {
    let remaining = offset;
    for (
      let targetIndex = index + direction;
      targetIndex >= 0 && targetIndex < rows.length;
      targetIndex += direction
    ) {
      const target = rows[targetIndex];
      if (target === undefined || evaluate(arg, target) === null) continue;
      remaining -= 1;
      if (remaining === 0) return evaluate(arg, target);
    }
    return expr.args[2] === undefined ? null : evaluate(expr.args[2], row);
  });
}

function nthValue(partition: readonly WindowRow[], expr: WindowExpr): unknown[] {
  const nth = numericArg(expr, 1, 1);
  if (!Number.isInteger(nth) || nth <= 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "nth_value requires a positive integer position");
  }
  return partition.map((_item, index) => {
    const rows = frameValueRows(frameRows(partition, index, expr), expr);
    const target = rows[nth - 1];
    return target === undefined ? null : evaluate(valueArg(expr), target);
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

function firstFrameValue(rows: readonly Row[], expr: WindowExpr): Row | undefined {
  return frameValueRows(rows, expr)[0];
}

function lastFrameValue(rows: readonly Row[], expr: WindowExpr): Row | undefined {
  const values = frameValueRows(rows, expr);
  return values[values.length - 1];
}

function frameValueRows(rows: readonly Row[], expr: WindowExpr): Row[] {
  if (expr.ignoreNulls !== true) return [...rows];
  const arg = valueArg(expr);
  return rows.filter((row) => evaluate(arg, row) !== null);
}
