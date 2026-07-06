import { LakeqlError } from "./errors.js";
import { evaluate, jsonSafeValue, matches } from "./evaluator.js";
import type { AggregateOp } from "./query.js";
import { compareOrderedScalars } from "./scalar-order.js";
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
  return compareOrderedScalars(left, right, term, "Window ORDER BY");
}

function defaultOrderTerm(): WindowOrderTerm {
  return { expr: { kind: "literal", value: null }, direction: "asc", nulls: "last" };
}
