import { LakeqlError } from "./errors.js";
import type { OrderByTerm } from "./query.js";
import { compareOrderedScalars, isSortableScalar } from "./scalar-order.js";
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
  const heap: { runIndex: number; row: Row }[] = [];
  const out: Row[] = [];
  for (let runIndex = 0; runIndex < runs.length; runIndex += 1) {
    const row = runs[runIndex]?.[0];
    if (row !== undefined) heapPush(heap, { runIndex, row }, orderBy);
  }
  while (heap.length > 0) {
    const next = heapPop(heap, orderBy);
    if (next === undefined) break;
    out.push(next.row);
    cursors[next.runIndex] = (cursors[next.runIndex] ?? 0) + 1;
    const row = runs[next.runIndex]?.[cursors[next.runIndex] ?? 0];
    if (row !== undefined) heapPush(heap, { runIndex: next.runIndex, row }, orderBy);
  }
  return out;
}

function heapPush(
  heap: { runIndex: number; row: Row }[],
  item: { runIndex: number; row: Row },
  orderBy: OrderByTerm[],
): void {
  heap.push(item);
  for (let index = heap.length - 1; index > 0; ) {
    const parent = Math.floor((index - 1) / 2);
    if (compareRows(heap[index]?.row ?? {}, heap[parent]?.row ?? {}, orderBy) >= 0) break;
    [heap[index], heap[parent]] = [
      heap[parent] as { runIndex: number; row: Row },
      heap[index] as { runIndex: number; row: Row },
    ];
    index = parent;
  }
}

function heapPop(
  heap: { runIndex: number; row: Row }[],
  orderBy: OrderByTerm[],
): { runIndex: number; row: Row } | undefined {
  const root = heap[0];
  const last = heap.pop();
  if (root === undefined || last === undefined) return root;
  if (heap.length > 0) {
    heap[0] = last;
    heapifyDown(heap, 0, orderBy);
  }
  return root;
}

function heapifyDown(
  heap: { runIndex: number; row: Row }[],
  start: number,
  orderBy: OrderByTerm[],
): void {
  for (let index = start; ; ) {
    const left = index * 2 + 1;
    const right = left + 1;
    let smallest = index;
    if (
      heap[left] !== undefined &&
      compareRows(heap[left].row, heap[smallest]?.row ?? {}, orderBy) < 0
    ) {
      smallest = left;
    }
    if (
      heap[right] !== undefined &&
      compareRows(heap[right].row, heap[smallest]?.row ?? {}, orderBy) < 0
    ) {
      smallest = right;
    }
    if (smallest === index) return;
    [heap[index], heap[smallest]] = [
      heap[smallest] as { runIndex: number; row: Row },
      heap[index] as { runIndex: number; row: Row },
    ];
    index = smallest;
  }
}

export function validateSortRow(row: Row, orderBy: OrderByTerm[]): void {
  for (const term of orderBy) {
    const value = valueForColumn(row, term.column);
    if (value === null || value === undefined) continue;
    if (!isSortableScalar(value)) {
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
  return compareOrderedScalars(left, right, term, "orderBy");
}
