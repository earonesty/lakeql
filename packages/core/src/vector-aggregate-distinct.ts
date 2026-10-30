import {
  jsonItemBytes,
  jsonListBytes,
  jsonListBytesAfterAppend,
  unmeasuredMemoryBytes,
} from "./aggregate-memory.js";
import { LakeqlError } from "./errors.js";
import type { QueryBudget } from "./query.js";
import type { VectorAggregateValue } from "./vector-aggregate.js";

const sortedRunMergeMinValues = 1024;
const sortedRunSampleValues = 1024;
const sortedRunMinSampleDistinctRatio = 0.5;

export type VectorDistinctAggregateState = {
  op: "count_distinct" | "approx_count_distinct";
  values: Set<string>;
  sortedValues?: string[];
  sortedRuns?: string[][];
  memoryBytes: number;
};

export function createDistinctAggregateState(
  op: VectorDistinctAggregateState["op"],
): VectorDistinctAggregateState {
  return { op, values: new Set(), memoryBytes: distinctMemoryBytes(new Set()) };
}

export function distinctKey(value: Exclude<VectorAggregateValue, null>): string {
  return `${typeof value}:${String(value)}`;
}

export function addDistinctValue(
  state: VectorDistinctAggregateState,
  key: string,
  budget?: QueryBudget,
): void {
  const values = mutableDistinctValues(state);
  measureDistinctBytes(state, budget);
  if (values.has(key)) return;
  values.add(key);
  state.memoryBytes =
    budget?.maxMemoryBytes === undefined
      ? unmeasuredMemoryBytes
      : jsonListBytesAfterAppend(state.memoryBytes, values.size - 1, jsonItemBytes(key));
  enforceDistinctBudget(state, budget);
}

export function addDistinctValues(
  state: VectorDistinctAggregateState,
  keys: Iterable<string>,
  budget?: QueryBudget,
): void {
  const values = mutableDistinctValues(state);
  if (budget?.maxMemoryBytes === undefined && budget?.maxBufferedRows === undefined) {
    for (const key of keys) values.add(key);
    state.memoryBytes = 0;
    return;
  }
  for (const key of keys) addDistinctValue(state, key, budget);
}

export function addDistinctStringValues(
  state: VectorDistinctAggregateState,
  values: Iterable<string>,
  budget?: QueryBudget,
): void {
  const distinctValues = mutableDistinctValues(state);
  if (budget?.maxMemoryBytes === undefined && budget?.maxBufferedRows === undefined) {
    for (const value of values) distinctValues.add(`string:${value}`);
    state.memoryBytes = 0;
    return;
  }
  for (const value of values) addDistinctValue(state, `string:${value}`, budget);
}

export function addDistinctSortedStringRun(
  state: VectorDistinctAggregateState,
  values: string[],
  budget?: QueryBudget,
): void {
  if (budget?.maxMemoryBytes !== undefined || budget?.maxBufferedRows !== undefined) {
    addDistinctStringValues(state, values, budget);
    return;
  }
  if (values.length < sortedRunMergeMinValues) {
    addDistinctStringValues(state, new Set(values));
    return;
  }
  if (!hasHighCardinalitySample(values)) {
    addDistinctStringValues(state, new Set(values));
    return;
  }
  const run = prefixedSortedUniqueStrings(values);
  mergeDistinctSortedValues(state, run);
}

export function mergeDistinctSortedValues(
  state: VectorDistinctAggregateState,
  values: readonly string[],
  budget?: QueryBudget,
): void {
  if (budget?.maxMemoryBytes !== undefined || budget?.maxBufferedRows !== undefined) {
    addDistinctValues(state, values, budget);
    return;
  }
  if (values.length === 0) return;
  if (values.length < sortedRunMergeMinValues) {
    addDistinctValues(state, values);
    return;
  }
  const runs = state.sortedRuns ?? [];
  if (state.sortedValues !== undefined) {
    runs.push(state.sortedValues);
    delete state.sortedValues;
  }
  if (state.values.size !== 0) {
    runs.push(Array.from(state.values).sort(compareDistinctKeys));
  }
  runs.push([...values]);
  state.sortedRuns = runs;
  state.values = new Set();
  state.memoryBytes = 0;
}

export function enforceDistinctStateBudget(
  state: VectorDistinctAggregateState,
  budget?: QueryBudget,
): void {
  enforceDistinctBudget(state, budget);
}

export function distinctValueCount(state: VectorDistinctAggregateState): number {
  if (state.sortedRuns !== undefined) return countSortedUniqueRuns(state.sortedRuns);
  return state.sortedValues?.length ?? state.values.size;
}

export function distinctSnapshotValues(state: VectorDistinctAggregateState): string[] {
  if (state.sortedRuns !== undefined) return mergeSortedRuns(state.sortedRuns);
  return state.sortedValues === undefined
    ? [...state.values].sort(compareDistinctKeys)
    : state.sortedValues;
}

export function cloneDistinctAggregateState(
  state: VectorDistinctAggregateState,
): VectorDistinctAggregateState {
  return {
    op: state.op,
    values: new Set(state.values),
    ...(state.sortedValues === undefined ? {} : { sortedValues: [...state.sortedValues] }),
    ...(state.sortedRuns === undefined
      ? {}
      : { sortedRuns: state.sortedRuns.map((run) => [...run]) }),
    memoryBytes: state.memoryBytes,
  };
}

export function distinctMemoryBytes(values: Set<string>, budget?: QueryBudget): number {
  if (budget?.maxMemoryBytes === undefined && values.size !== 0) return unmeasuredMemoryBytes;
  return jsonListBytes(values);
}

function mutableDistinctValues(state: VectorDistinctAggregateState): Set<string> {
  if (state.sortedValues !== undefined || state.sortedRuns !== undefined) {
    state.values = new Set(distinctSnapshotValues(state));
    delete state.sortedValues;
    delete state.sortedRuns;
    state.memoryBytes = unmeasuredMemoryBytes;
  }
  return state.values;
}

function measureDistinctBytes(state: VectorDistinctAggregateState, budget?: QueryBudget): void {
  if (budget?.maxMemoryBytes === undefined || state.memoryBytes !== unmeasuredMemoryBytes) return;
  state.memoryBytes = jsonListBytes(mutableDistinctValues(state));
}

function enforceDistinctBudget(state: VectorDistinctAggregateState, budget?: QueryBudget): void {
  measureDistinctBytes(state, budget);
  const valueCount = distinctValueCount(state);
  if (budget?.maxBufferedRows !== undefined && valueCount > budget.maxBufferedRows) {
    throwBudget("buffered rows", budget.maxBufferedRows, valueCount);
  }
  if (budget?.maxMemoryBytes !== undefined && state.memoryBytes > budget.maxMemoryBytes) {
    throwBudget("operator memory bytes", budget.maxMemoryBytes, state.memoryBytes);
  }
}

function mergeSortedRuns(runs: readonly (readonly string[])[]): string[] {
  if (runs.length === 0) return [];
  if (runs.length === 1) return [...(runs[0] ?? [])];
  const values: string[] = [];
  visitSortedUniqueRuns(runs, (value) => {
    values.push(value);
  });
  return values;
}

function countSortedUniqueRuns(runs: readonly (readonly string[])[]): number {
  if (runs.length === 0) return 0;
  if (runs.length === 1) return runs[0]?.length ?? 0;
  let count = 0;
  visitSortedUniqueRuns(runs, () => {
    count += 1;
  });
  return count;
}

function visitSortedUniqueRuns(
  runs: readonly (readonly string[])[],
  visit: (value: string) => void,
): void {
  const heap = createSortedRunHeap(runs);
  let previous: string | undefined;
  while (heap.length > 0) {
    const entry = heapPop(heap);
    if (entry.value !== previous) {
      visit(entry.value);
      previous = entry.value;
    }
    const run = runs[entry.runIndex];
    const nextIndex = entry.valueIndex + 1;
    const nextValue = run?.[nextIndex];
    if (nextValue !== undefined) {
      heapPush(heap, { value: nextValue, runIndex: entry.runIndex, valueIndex: nextIndex });
    }
  }
}

type SortedRunHeapEntry = {
  value: string;
  runIndex: number;
  valueIndex: number;
};

function createSortedRunHeap(runs: readonly (readonly string[])[]): SortedRunHeapEntry[] {
  const heap: SortedRunHeapEntry[] = [];
  for (let runIndex = 0; runIndex < runs.length; runIndex += 1) {
    const value = runs[runIndex]?.[0];
    if (value !== undefined) heapPush(heap, { value, runIndex, valueIndex: 0 });
  }
  return heap;
}

function heapPush(heap: SortedRunHeapEntry[], entry: SortedRunHeapEntry): void {
  let index = heap.length;
  heap.push(entry);
  while (index > 0) {
    const parentIndex = (index - 1) >> 1;
    const parent = heap[parentIndex];
    if (parent !== undefined && compareHeapEntry(parent, entry) <= 0) break;
    heap[index] = parent as SortedRunHeapEntry;
    index = parentIndex;
  }
  heap[index] = entry;
}

function heapPop(heap: SortedRunHeapEntry[]): SortedRunHeapEntry {
  const first = heap[0];
  if (first === undefined) throw new Error("Cannot pop from an empty sorted run heap");
  const last = heap.pop();
  if (last !== undefined && heap.length > 0) {
    heap[0] = last;
    heapifyDown(heap, 0);
  }
  return first;
}

function heapifyDown(heap: SortedRunHeapEntry[], index: number): void {
  const entry = heap[index];
  if (entry === undefined) return;
  for (;;) {
    const leftIndex = index * 2 + 1;
    const rightIndex = leftIndex + 1;
    const left = heap[leftIndex];
    const right = heap[rightIndex];
    if (left === undefined) break;
    const childIndex =
      right !== undefined && compareHeapEntry(right, left) < 0 ? rightIndex : leftIndex;
    const child = heap[childIndex];
    if (child === undefined || compareHeapEntry(entry, child) <= 0) break;
    heap[index] = child;
    index = childIndex;
  }
  heap[index] = entry;
}

function compareHeapEntry(left: SortedRunHeapEntry, right: SortedRunHeapEntry): number {
  return (
    compareDistinctKeys(left.value, right.value) ||
    left.runIndex - right.runIndex ||
    left.valueIndex - right.valueIndex
  );
}

function compareDistinctKeys(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function prefixedSortedUniqueStrings(values: string[]): string[] {
  values.sort(compareDistinctKeys);
  const distinct: string[] = [];
  let previous: string | undefined;
  for (const value of values) {
    if (value === previous) continue;
    distinct.push(`string:${value}`);
    previous = value;
  }
  return distinct;
}

function hasHighCardinalitySample(values: readonly string[]): boolean {
  const sampleSize = Math.min(values.length, sortedRunSampleValues);
  const sample = new Set<string>();
  for (let index = 0; index < sampleSize; index += 1) {
    const value = values[index];
    if (value !== undefined) sample.add(value);
  }
  return sample.size / sampleSize >= sortedRunMinSampleDistinctRatio;
}

function throwBudget(metric: string, limit: number, actual: number): never {
  throw new LakeqlError(
    "LAKEQL_BUDGET_EXCEEDED",
    `Query exceeded ${metric} budget (${actual} > ${limit}). Add a partition filter, date filter, h3 filter, or limit.`,
    { metric, limit, actual },
  );
}
