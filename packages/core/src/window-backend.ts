import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { LakeqlError } from "./errors.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
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
import {
  evaluateWindowValue,
  retainWindowValue,
  withWindowResultScope,
  withWindowValue,
} from "./window-value-ownership.js";

export interface WindowExecutionOptions {
  memoryBudget?: MemoryBudget;
  allocationScope?: MemoryScope;
  resultScope?: MemoryScope;
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
  return withWindowResultScope(options, (resultScope) => {
    options = { ...options, memoryBudget: resultScope.budget, resultScope };
    const rows = inputRows.map((row) => ({ ...row }));
    options.enforceBufferedRows?.(rows.length);
    if (options.estimateMemoryBytes !== undefined) {
      options.enforceMemoryBytes?.(options.estimateMemoryBytes(rows));
    }
    for (const group of windowSortGroups(windows)) {
      const groupScope = new MemoryScope(
        options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER),
      );
      try {
        const sorted = partitionRows(rows, group.sortExpr, groupScope);
        sortWindowRows(sorted, group.sortExpr.over.orderBy, groupScope);
        for (const { alias, expr } of group.items) {
          const expressionScope = groupScope.childScope();
          try {
            const windowRows = windowRowsForExpr(sorted, expr, group.sortExpr, expressionScope);
            const spans = partitionSpans(windowRows, expressionScope);
            const values = evaluateWindowRows(rows.length, windowRows, spans, expr, {
              ...options,
              allocationScope: expressionScope,
            });
            for (let index = 0; index < rows.length; index += 1) {
              const row = rows[index];
              if (row !== undefined) row[alias] = values[index] ?? null;
            }
          } finally {
            expressionScope.dispose();
          }
        }
      } finally {
        groupScope.dispose();
      }
    }
    return rows;
  });
}

export function applyWindowsToRuns(
  inputRuns: readonly (readonly Row[])[],
  windows: Record<string, WindowExpr>,
  options: WindowExecutionOptions = {},
): Row[][] {
  return withWindowResultScope(options, (resultScope) => {
    options = { ...options, memoryBudget: resultScope.budget, resultScope };
    const outputRuns = inputRuns.map((run) => run.map((row) => ({ ...row })));
    for (const group of windowSortGroups(windows)) {
      const groupScope = new MemoryScope(
        options.memoryBudget ?? new MemoryBudget(Number.MAX_SAFE_INTEGER),
      );
      try {
        const sortedRuns = groupScope.allocate(
          c.array + outputRuns.length * c.arraySlot,
          "window sorted runs",
          () =>
            outputRuns.map((run, runIndex) => {
              const indexed = partitionRows(run, group.sortExpr, groupScope) as RunWindowRow[];
              for (let rowIndex = 0; rowIndex < indexed.length; rowIndex++) {
                const item = indexed[rowIndex];
                if (item)
                  groupScope.allocate(2 * c.property, "window run position", () => {
                    item.runIndex = runIndex;
                    item.rowIndex = rowIndex;
                  });
              }
              return indexed;
            }),
        ).value;
        for (const run of sortedRuns) sortWindowRows(run, group.sortExpr.over.orderBy, groupScope);
        for (const partition of mergeWindowPartitions(sortedRuns, group.sortExpr, groupScope)) {
          options.enforceWindowPartitionRows?.(partition.length);
          if (options.estimateMemoryBytes !== undefined) {
            const scratch = groupScope.reserve(
              c.array + partition.length * c.arraySlot,
              "window partition measurement references",
            );
            try {
              options.enforceMemoryBytes?.(
                options.estimateMemoryBytes(partition.map((item) => item.row)),
              );
            } finally {
              scratch.release();
            }
          }
          for (const { alias, expr } of group.items) {
            const expressionScope = groupScope.childScope();
            try {
              const windowRows = windowRowsForExpr(
                partition,
                expr,
                group.sortExpr,
                expressionScope,
              );
              const values = expressionScope.allocate(
                c.array + windowRows.length * c.arraySlot,
                "window partition output",
                () => evaluatePartition(windowRows, expr, options),
              ).value;
              for (let index = 0; index < windowRows.length; index++) {
                const item = windowRows[index];
                if (item === undefined) continue;
                const row = outputRuns[item.runIndex]?.[item.rowIndex];
                if (row !== undefined) row[alias] = values[index] ?? null;
              }
            } finally {
              expressionScope.dispose();
            }
          }
        }
      } finally {
        groupScope.dispose();
      }
    }
    return outputRuns;
  });
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
  scope?: MemoryScope,
): readonly T[] {
  const orderKeyLength = expr.over.orderBy.length;
  if (orderKeyLength === sortExpr.over.orderBy.length) return sorted;
  const build = () =>
    sorted.map((item) => ({ ...item, orderKey: item.orderKey.slice(0, orderKeyLength) })) as T[];
  let bytes = c.array + sorted.length * c.arraySlot;
  if (scope)
    for (const item of sorted) {
      bytes += c.object + c.array + orderKeyLength * c.arraySlot;
      for (const key in item) if (Object.hasOwn(item, key)) bytes += c.property;
    }
  const projected = scope ? scope.allocate(bytes, "window prefix indexes", build).value : build();
  stabilizePrefixPeers(projected, scope);
  return projected;
}

interface RunWindowRow extends WindowRow {
  runIndex: number;
  rowIndex: number;
}

function* mergeWindowPartitions(
  runs: readonly RunWindowRow[][],
  sortExpr: WindowExpr,
  scope: MemoryScope,
): Iterable<RunWindowRow[]> {
  const cursors = scope.allocate(c.array + runs.length * c.arraySlot, "window run cursors", () =>
    runs.map(() => 0),
  ).value;
  let partitionScope = scope.childScope();
  const newPartition = () =>
    partitionScope.allocate(c.array, "window merged partition", () => [] as RunWindowRow[]).value;
  let current = newPartition();
  try {
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
        partitionScope.dispose();
        partitionScope = scope.childScope();
        current = newPartition();
      }
      currentPartition = best;
      partitionScope.allocate(c.arraySlot, "window merged reference", () => current.push(best));
    }
    if (current.length > 0) yield current;
  } finally {
    partitionScope.dispose();
  }
}

function stabilizePrefixPeers(rows: WindowRow[], scope?: MemoryScope): void {
  let start = 0;
  while (start < rows.length) {
    const current = rows[start];
    if (current === undefined) break;

    let end = start + 1;
    while (end < rows.length && samePeer(rows[end] as WindowRow, current)) {
      end += 1;
    }
    if (end - start > 1) {
      const scratch = scope?.reserve(
        c.array + (end - start) * (2 * c.arraySlot + c.object),
        "window prefix stabilization",
      );
      try {
        rows
          .slice(start, end)
          .sort((left, right) => left.originalIndex - right.originalIndex)
          .forEach((row, index) => {
            rows[start + index] = row;
          });
      } finally {
        scratch?.release();
      }
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
  const scope = options.allocationScope;
  const createOutput = () => new Array<unknown>(rowCount).fill(null);
  const output = scope
    ? scope.allocate(c.array + rowCount * c.arraySlot, "window expression output", createOutput)
        .value
    : createOutput();
  for (const span of spans) {
    const temporary = scope?.childScope();
    try {
      const count = span.end - span.start;
      const partition = temporary
        ? temporary.allocate(c.array + count * c.arraySlot, "window partition references", () =>
            sorted.slice(span.start, span.end),
          ).value
        : sorted.slice(span.start, span.end);
      options.enforceWindowPartitionRows?.(partition.length);
      const values = temporary
        ? temporary.allocate(c.array + count * c.arraySlot, "window partition output", () =>
            evaluatePartition(partition, expr, options),
          ).value
        : evaluatePartition(partition, expr, options);
      for (let index = 0; index < partition.length; index++) {
        const item = partition[index];
        if (item !== undefined) output[item.originalIndex] = values[index] ?? null;
      }
    } finally {
      temporary?.dispose();
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
    return evaluatePartitionWithPeers(
      partition,
      expr,
      peers,
      options.memoryBudget,
      options.resultScope,
    );
  } finally {
    peers.dispose();
  }
}

function evaluatePartitionWithPeers(
  partition: readonly WindowRow[],
  expr: WindowExpr,
  peers: WindowPeers,
  budget?: MemoryBudget,
  resultScope?: MemoryScope,
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
      return offsetValues(partition, expr, -1, resultScope);
    case "lead":
      return offsetValues(partition, expr, 1, resultScope);
    case "first_value":
      return partition.map((_item, index) => {
        return frameValueAt(frameRowIterator(partition, index, expr, peers), expr, 1, resultScope);
      });
    case "last_value":
      return partition.map((_item, index) => {
        return frameValueAt(
          frameRowIterator(partition, index, expr, peers, true),
          expr,
          1,
          resultScope,
        );
      });
    case "nth_value":
      return nthValue(partition, expr, peers, resultScope);
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

function offsetValues(
  rows: readonly WindowRow[],
  expr: WindowExpr,
  direction: -1 | 1,
  scope?: MemoryScope,
): unknown[] {
  const offset = expr.args.length >= 2 ? numericArg(expr, 1, 1) : 1;
  if (!Number.isInteger(offset) || offset < 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "lag/lead offset must be a non-negative integer");
  }
  if (expr.ignoreNulls === true) return offsetIgnoringNulls(rows, expr, direction, offset, scope);
  return rows.map((row, index) => {
    const target = rows[index + direction * offset];
    if (target === undefined)
      return expr.args[2] === undefined ? null : evaluateWindowValue(expr.args[2], row.row, scope);
    return evaluateWindowValue(valueArg(expr), target.row, scope);
  });
}

/** Maintain the count of non-null values between the current and target row.
 * Both cursors move once through the partition; no offset-sized ring is needed. */
function offsetIgnoringNulls(
  rows: readonly WindowRow[],
  expr: WindowExpr,
  direction: -1 | 1,
  offset: number,
  scope?: MemoryScope,
): unknown[] {
  const arg = valueArg(expr);
  if (offset === 0) return rows.map((item) => evaluateWindowValue(arg, item.row, scope));
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
      withWindowValue(arg, item.row, scope, (value, temporary) => {
        if (value !== null) {
          count++;
          targetValue = count === offset ? retainWindowValue(value, scope, temporary) : null;
        }
      });
    }
    const item = rows[index];
    output[index] =
      count === offset
        ? targetValue
        : expr.args[2] === undefined || item === undefined
          ? null
          : evaluateWindowValue(expr.args[2], item.row, scope);
    const nextIndex = index + direction;
    if (
      nextIndex >= 0 &&
      nextIndex < rows.length &&
      (direction === 1 ? nextIndex <= target : nextIndex >= target)
    ) {
      const departing = rows[nextIndex];
      if (
        departing !== undefined &&
        withWindowValue(arg, departing.row, scope, (value) => value !== null)
      )
        count--;
    }
  }
  return output;
}

function nthValue(
  partition: readonly WindowRow[],
  expr: WindowExpr,
  peers: WindowPeers,
  scope?: MemoryScope,
): unknown[] {
  const nth = numericArg(expr, 1, 1);
  if (!Number.isInteger(nth) || nth <= 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "nth_value requires a positive integer position");
  }
  return partition.map((_item, index) => {
    return frameValueAt(frameRowIterator(partition, index, expr, peers), expr, nth, scope);
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

function frameValueAt(
  rows: Iterable<Row>,
  expr: WindowExpr,
  ordinal: number,
  scope?: MemoryScope,
): unknown {
  for (const row of rows) {
    let selected = false;
    const value = withWindowValue(valueArg(expr), row, scope, (candidate, temporary) => {
      if (expr.ignoreNulls === true && candidate === null) return null;
      selected = --ordinal === 0;
      return selected ? retainWindowValue(candidate, scope, temporary) : null;
    });
    if (selected) return value;
  }
  return null;
}
