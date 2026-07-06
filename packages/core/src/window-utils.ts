import { LakeqlError } from "./errors.js";
import { evaluate, jsonSafeValue, matches } from "./evaluator.js";
import type { AggregateOp } from "./query.js";
import { compareTimestampValues, isTimestampValue } from "./timestamp.js";
import type { Row } from "./types.js";
import type { WindowExpr, WindowOrderTerm } from "./window.js";

export interface WindowRow {
  row: Row;
  originalIndex: number;
  partitionKey: unknown[];
  orderKey: unknown[];
}

export function normalizedWindowName(fn: WindowExpr["fn"]): string {
  return typeof fn === "string" ? fn : fn.aggregate;
}

export function isAggregateWindow(fn: WindowExpr["fn"]): fn is { aggregate: AggregateOp } {
  return typeof fn === "object" && fn !== null && "aggregate" in fn;
}

export function requireExactWindowAggregate(op: AggregateOp): void {
  if (op === "approx_count_distinct") {
    throw new LakeqlError(
      "LAKEQL_SQL_UNSUPPORTED",
      "approx_count_distinct is not supported as a window aggregate because window results are exact",
      { aggregate: op },
    );
  }
}

export function partitionRows(rows: readonly Row[], expr: WindowExpr): WindowRow[] {
  return rows.map((row, originalIndex) => ({
    row,
    originalIndex,
    partitionKey: expr.over.partitionBy.map((part) => evaluate(part, row)),
    orderKey: expr.over.orderBy.map((term) => evaluate(term.expr, row)),
  }));
}

export function sortWindowRows(rows: WindowRow[], orderBy: readonly WindowOrderTerm[]): void {
  rows.sort((left, right) => compareWindowRows(left, right, orderBy));
}

export function compareWindowRows(
  left: WindowRow,
  right: WindowRow,
  orderBy: readonly WindowOrderTerm[],
): number {
  const partition = compareKeyValues(left.partitionKey, right.partitionKey, defaultOrderTerm());
  if (partition !== 0) return partition;
  for (let index = 0; index < orderBy.length; index += 1) {
    const term = orderBy[index] ?? defaultOrderTerm();
    const comparison = compareWindowValues(left.orderKey[index], right.orderKey[index], term);
    if (comparison !== 0) return comparison;
  }
  return left.originalIndex - right.originalIndex;
}

export function samePartition(left: WindowRow, right: WindowRow): boolean {
  return stableKey(left.partitionKey) === stableKey(right.partitionKey);
}

export function samePeer(left: WindowRow, right: WindowRow): boolean {
  return samePartition(left, right) && stableKey(left.orderKey) === stableKey(right.orderKey);
}

export function partitionSpans(rows: readonly WindowRow[]): { start: number; end: number }[] {
  const spans: { start: number; end: number }[] = [];
  let start = 0;
  while (start < rows.length) {
    let end = start + 1;
    while (end < rows.length && samePartition(rows[start] as WindowRow, rows[end] as WindowRow)) {
      end += 1;
    }
    spans.push({ start, end });
    start = end;
  }
  return spans;
}

export function filterRow(expr: WindowExpr, row: Row): boolean {
  return expr.filter === undefined || matches(expr.filter, row);
}

export function stableKey(values: readonly unknown[]): string {
  return JSON.stringify(values.map(jsonSafeValue));
}

function compareKeyValues(
  left: readonly unknown[],
  right: readonly unknown[],
  term: WindowOrderTerm,
): number {
  const length = Math.max(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    const comparison = compareWindowValues(left[index], right[index], term);
    if (comparison !== 0) return comparison;
  }
  return 0;
}

export function compareWindowValues(left: unknown, right: unknown, term: WindowOrderTerm): number {
  const direction = term.direction ?? "asc";
  const nulls = term.nulls ?? (direction === "asc" ? "last" : "first");
  const leftNull = left === null || left === undefined;
  const rightNull = right === null || right === undefined;
  if (leftNull || rightNull) {
    if (leftNull && rightNull) return 0;
    const nullOrder = nulls === "first" ? -1 : 1;
    return leftNull ? nullOrder : -nullOrder;
  }
  if (isTimestampValue(left) || isTimestampValue(right)) {
    if (!isTimestampValue(left) || !isTimestampValue(right)) {
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        "Window ORDER BY timestamp values must have matching types",
      );
    }
    const multiplier = direction === "desc" ? -1 : 1;
    return compareTimestampValues(left, right) * multiplier;
  }
  if (typeof left !== typeof right) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Window ORDER BY values must have matching types");
  }
  if (
    typeof left !== "string" &&
    typeof left !== "number" &&
    typeof left !== "bigint" &&
    typeof left !== "boolean"
  ) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Window ORDER BY values must be scalar");
  }
  const multiplier = direction === "desc" ? -1 : 1;
  if (left < right) return -1 * multiplier;
  if (left > right) return multiplier;
  return 0;
}

function defaultOrderTerm(): WindowOrderTerm {
  return { expr: { kind: "literal", value: null }, direction: "asc", nulls: "last" };
}
