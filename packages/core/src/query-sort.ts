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

export function compareRows(left: Row, right: Row, orderBy: readonly OrderByTerm[]): number {
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

/** A single heap implementation for bounded selection and sorted-run merging.
 * The least value according to compare is at the root. */
class PriorityQueue<T> {
  private readonly items: T[] = [];
  constructor(private readonly compare: (a: T, b: T) => number) {}
  get size(): number {
    return this.items.length;
  }
  get first(): T | undefined {
    return this.items[0];
  }
  values(): Iterable<T> {
    return this.items;
  }
  push(value: T): void {
    this.items.push(value);
    let index = this.items.length - 1;
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      const previous = this.items[parent] as T;
      if (this.compare(value, previous) >= 0) break;
      this.items[index] = previous;
      index = parent;
    }
    this.items[index] = value;
  }
  replaceFirst(value: T): void {
    let index = 0;
    while (index * 2 + 1 < this.items.length) {
      let child = index * 2 + 1;
      if (
        child + 1 < this.items.length &&
        this.compare(this.items[child + 1] as T, this.items[child] as T) < 0
      )
        child++;
      const next = this.items[child] as T;
      if (this.compare(value, next) <= 0) break;
      this.items[index] = next;
      index = child;
    }
    this.items[index] = value;
  }
  pop(): T | undefined {
    const first = this.items[0];
    const last = this.items.pop();
    if (this.items.length > 0) this.replaceFirst(last as T);
    return first;
  }
}

/** Stable top-k with O(k) storage and O(log k) accepted-candidate updates.
 * Earlier input wins equal keys. No per-row sort-key cache or input-sized index. */
export class BoundedSelection<T> {
  private ordinal = 0;
  private readonly heap: PriorityQueue<{ value: T; ordinal: number }>;
  constructor(
    readonly limit: number,
    private readonly compare: (a: T, b: T) => number,
    private readonly changed?: (added: T, removed?: T) => void,
  ) {
    if (!Number.isSafeInteger(limit) || limit < 0)
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Top-k limit must be a non-negative safe integer");
    this.heap = new PriorityQueue((a, b) => compare(b.value, a.value) || b.ordinal - a.ordinal);
  }
  get size(): number {
    return this.heap.size;
  }
  *values(): Iterable<T> {
    for (const item of this.heap.values()) yield item.value;
  }
  add(value: T): void {
    if (this.limit === 0) return;
    if (this.ordinal === Number.MAX_SAFE_INTEGER)
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        "Top-k encounter ordinal exceeded safe integer range",
      );
    const ordinal = this.ordinal++;
    const worst = this.heap.first;
    if (this.heap.size < this.limit) {
      this.changed?.(value);
      this.heap.push({ value, ordinal });
    } else if (worst !== undefined && this.compare(value, worst.value) < 0) {
      this.changed?.(value, worst.value);
      this.heap.replaceFirst({ value, ordinal });
    }
  }
  finish(): T[] {
    const result = new Array<T>(this.heap.size);
    for (let index = result.length - 1; index >= 0; index--)
      result[index] = (this.heap.pop() as { value: T }).value;
    return result;
  }
}

export function flushSortRun(runs: Row[][], currentRun: Row[], orderBy: OrderByTerm[]): void {
  if (currentRun.length === 0) return;
  currentRun.sort((left, right) => compareRows(left, right, orderBy));
  runs.push(currentRun.splice(0, currentRun.length));
}

export function mergeSortRuns(runs: Row[][], orderBy: OrderByTerm[]): Row[] {
  const cursors = runs.map(() => 0);
  const heap = new PriorityQueue<{ runIndex: number; row: Row }>(
    (a, b) => compareRows(a.row, b.row, orderBy) || a.runIndex - b.runIndex,
  );
  const out: Row[] = [];
  for (let runIndex = 0; runIndex < runs.length; runIndex++) {
    const row = runs[runIndex]?.[0];
    if (row !== undefined) heap.push({ runIndex, row });
  }
  while (heap.size > 0) {
    const next = heap.pop();
    if (next === undefined) break;
    out.push(next.row);
    cursors[next.runIndex] = (cursors[next.runIndex] ?? 0) + 1;
    const row = runs[next.runIndex]?.[cursors[next.runIndex] ?? 0];
    if (row !== undefined) heap.push({ runIndex: next.runIndex, row });
  }
  return out;
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
