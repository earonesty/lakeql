import { LakeqlError } from "./errors.js";
import type { OrderByTerm } from "./query.js";
import { compareTimestampValues, isTimestampValue, type TimestampValue } from "./timestamp.js";
import type { Row } from "./types.js";

export function normalizeOrderBy(terms: OrderByTerm[]): OrderByTerm[] {
  if (!Array.isArray(terms) || terms.length === 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "orderBy must contain at least one term");
  }
  return terms.map((term) => {
    if (typeof term.column !== "string" || term.column.length === 0) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "orderBy columns must be non-empty strings");
    }
    const direction = term.direction ?? "asc";
    if (direction !== "asc" && direction !== "desc") {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "orderBy direction must be asc or desc", { term });
    }
    const nulls = term.nulls ?? (direction === "asc" ? "last" : "first");
    if (nulls !== "first" && nulls !== "last") {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "orderBy nulls must be first or last", { term });
    }
    return { column: term.column, direction, nulls };
  });
}

export function compareRows(left: Row, right: Row, orderBy: OrderByTerm[]): number {
  for (const term of orderBy) {
    const comparison = compareSortValues(
      valueForColumn(left, term.column),
      valueForColumn(right, term.column),
      term,
    );
    if (comparison !== 0) return comparison;
  }
  return 0;
}

export function addOrderedMatch(
  matched: Row[],
  row: Row,
  orderBy: OrderByTerm[],
  topK: number | undefined,
): void {
  if (topK === undefined) {
    matched.push(row);
    return;
  }
  if (topK === 0) return;
  if (matched.length < topK) {
    matched.push(row);
    return;
  }
  let worstIndex = 0;
  for (let index = 1; index < matched.length; index += 1) {
    const candidate = matched[index];
    const worst = matched[worstIndex];
    if (
      candidate !== undefined &&
      worst !== undefined &&
      compareRows(candidate, worst, orderBy) > 0
    ) {
      worstIndex = index;
    }
  }
  const worst = matched[worstIndex];
  if (worst !== undefined && compareRows(row, worst, orderBy) < 0) matched[worstIndex] = row;
}

export function flushSortRun(runs: Row[][], currentRun: Row[], orderBy: OrderByTerm[]): void {
  if (currentRun.length === 0) return;
  currentRun.sort((left, right) => compareRows(left, right, orderBy));
  runs.push(currentRun.splice(0, currentRun.length));
}

export function mergeSortRuns(runs: Row[][], orderBy: OrderByTerm[]): Row[] {
  const cursors = runs.map(() => 0);
  const out: Row[] = [];
  while (true) {
    let bestRun = -1;
    let bestRow: Row | undefined;
    for (let runIndex = 0; runIndex < runs.length; runIndex += 1) {
      const run = runs[runIndex];
      const cursor = cursors[runIndex] ?? 0;
      const row = run?.[cursor];
      if (row === undefined) continue;
      if (bestRow === undefined || compareRows(row, bestRow, orderBy) < 0) {
        bestRow = row;
        bestRun = runIndex;
      }
    }
    if (bestRow === undefined) return out;
    out.push(bestRow);
    cursors[bestRun] = (cursors[bestRun] ?? 0) + 1;
  }
}

export function validateSortRow(row: Row, orderBy: OrderByTerm[]): void {
  for (const term of orderBy) {
    const value = valueForColumn(row, term.column);
    if (value === null || value === undefined) continue;
    if (!isSortableValue(value)) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "orderBy values must be scalar", {
        column: term.column,
      });
    }
  }
}

export function valueForColumn(row: Row, column: string): unknown {
  if (!(column in row)) {
    throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${column}`, { column });
  }
  return row[column];
}

function compareSortValues(left: unknown, right: unknown, term: OrderByTerm): number {
  const leftNull = left === null || left === undefined;
  const rightNull = right === null || right === undefined;
  if (leftNull || rightNull) {
    if (leftNull && rightNull) return 0;
    const nullOrder = term.nulls === "first" ? -1 : 1;
    return leftNull ? nullOrder : -nullOrder;
  }
  if (!isSortableValue(left) || !isSortableValue(right)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "orderBy values must be scalar", {
      column: term.column,
    });
  }
  if (isTimestampValue(left) || isTimestampValue(right)) {
    if (!isTimestampValue(left) || !isTimestampValue(right)) {
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        "orderBy timestamp values must have matching types",
        { column: term.column },
      );
    }
    const direction = term.direction === "desc" ? -1 : 1;
    return compareTimestampValues(left, right) * direction;
  }
  if (typeof left !== typeof right) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "orderBy values must have matching types", {
      column: term.column,
    });
  }
  const direction = term.direction === "desc" ? -1 : 1;
  if (left < right) return -1 * direction;
  if (left > right) return direction;
  return 0;
}

function isSortableValue(
  value: unknown,
): value is string | number | bigint | boolean | TimestampValue {
  return (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean" ||
    isTimestampValue(value)
  );
}
