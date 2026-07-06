import { continuousQuantile, requiredQuantile } from "./aggregate-quantile.js";
import {
  type Batch,
  batchExprValues,
  type Selection,
  scalarVectorValue,
  selectedRowCount,
  type Vector,
} from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { Scalar } from "./expr.js";
import { isIntervalValue } from "./interval.js";
import type { AggregateExpr, AggregateSpec, QueryBudget } from "./query.js";
import { compareTimestampValues, isTimestampValue, type TimestampValue } from "./timestamp.js";
import {
  addDistinctSortedStringRun,
  addDistinctValue,
  addDistinctValues,
  cloneDistinctAggregateState,
  createDistinctAggregateState,
  distinctKey,
  distinctMemoryBytes,
  distinctValueCount,
  enforceDistinctStateBudget,
  mergeDistinctSortedValues,
  type VectorDistinctAggregateState,
} from "./vector-aggregate-distinct.js";
import {
  snapshotVectorAggregateState,
  vectorAggregateStateFromSnapshot,
} from "./vector-aggregate-snapshot.js";

export type VectorAggregateValue = string | number | boolean | bigint | TimestampValue | null;

export type VectorAggregateState =
  | { op: "count"; count: number }
  | { op: "sum"; sum: number }
  | { op: "avg"; sum: number; count: number }
  | {
      op: "var_samp" | "var_pop" | "stddev_samp" | "stddev_pop";
      count: number;
      mean: number;
      m2: number;
    }
  | { op: "median"; values: (number | string)[]; memoryBytes: number }
  | { op: "quantile"; quantile: number; values: number[]; memoryBytes: number }
  | { op: "min" | "max"; value: VectorAggregateValue }
  | VectorDistinctAggregateState
  | {
      op: "mode";
      values: Map<string, { value: Exclude<VectorAggregateValue, null>; count: number }>;
      memoryBytes: number;
    }
  | { op: "first"; seen: boolean; value: VectorAggregateValue }
  | { op: "last"; seen: boolean; value: VectorAggregateValue }
  | { op: "any"; seen: boolean; value: VectorAggregateValue };

export type VectorAggregateStates = Record<string, VectorAggregateState>;

export type VectorAggregateSnapshotValue =
  | string
  | number
  | boolean
  | null
  | { type: "bigint"; value: string }
  | {
      type: "timestamp";
      epochNanoseconds: string;
      unit: TimestampValue["unit"];
      isAdjustedToUTC: boolean;
    };

export type VectorAggregateStateSnapshot =
  | { op: "count"; count: number }
  | { op: "sum"; sum: number }
  | { op: "avg"; sum: number; count: number }
  | {
      op: "var_samp" | "var_pop" | "stddev_samp" | "stddev_pop";
      count: number;
      mean: number;
      m2: number;
    }
  | { op: "median"; values: (number | string)[] }
  | { op: "quantile"; quantile: number; values: number[] }
  | { op: "min" | "max"; value: VectorAggregateSnapshotValue }
  | { op: "count_distinct" | "approx_count_distinct"; values: string[] }
  | {
      op: "mode";
      values: { key: string; value: VectorAggregateSnapshotValue; count: number }[];
    }
  | { op: "first"; seen: boolean; value: VectorAggregateSnapshotValue }
  | { op: "last"; seen: boolean; value: VectorAggregateSnapshotValue }
  | { op: "any"; seen: boolean; value: VectorAggregateSnapshotValue };

export type VectorAggregateStateSnapshots = Record<string, VectorAggregateStateSnapshot>;

export interface VectorAggregateOptions {
  budget?: QueryBudget;
}

export function createVectorAggregateStates(
  spec: AggregateSpec,
  options: VectorAggregateOptions = {},
): VectorAggregateStates {
  const states: VectorAggregateStates = {};
  for (const [alias, aggregate] of Object.entries(spec)) {
    states[alias] = createVectorAggregateState(aggregate);
    enforceStateBudget(states[alias], options.budget);
  }
  return states;
}

export function updateVectorAggregateStates(
  states: VectorAggregateStates,
  spec: AggregateSpec,
  batch: Batch,
  selection?: Selection,
  options: VectorAggregateOptions = {},
): void {
  for (const [alias, aggregate] of Object.entries(spec)) {
    const state = states[alias];
    if (state === undefined) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", `Missing vector aggregate state ${alias}`, {
        alias,
      });
    }
    updateVectorAggregateState(state, aggregate, batch, selection, options.budget);
  }
}

export function mergeVectorAggregateStates(
  target: VectorAggregateStates,
  source: VectorAggregateStates,
  options: VectorAggregateOptions = {},
): void {
  for (const [alias, sourceState] of Object.entries(source)) {
    const targetState = target[alias];
    if (targetState === undefined) {
      target[alias] = cloneVectorAggregateState(sourceState);
      enforceStateBudget(target[alias], options.budget);
      continue;
    }
    mergeVectorAggregateState(targetState, sourceState, options.budget);
  }
}

export function mergeVectorAggregateStateSnapshots(
  target: VectorAggregateStates,
  snapshots: VectorAggregateStateSnapshots,
  options: VectorAggregateOptions = {},
): void {
  const restored: VectorAggregateStates = {};
  for (const [alias, snapshot] of Object.entries(snapshots)) {
    const targetState = target[alias];
    if (
      targetState !== undefined &&
      (snapshot.op === "count_distinct" || snapshot.op === "approx_count_distinct")
    ) {
      mergeDistinctSnapshot(targetState, snapshot, options.budget);
      continue;
    }
    restored[alias] = vectorAggregateStateFromSnapshot(snapshot);
  }
  mergeVectorAggregateStates(target, restored, options);
}

export function finalizeVectorAggregateStates(
  states: VectorAggregateStates,
): Record<string, unknown> {
  const row: Record<string, unknown> = {};
  for (const [alias, state] of Object.entries(states))
    row[alias] = finalizeVectorAggregateState(state);
  return row;
}

export function snapshotVectorAggregateStates(
  states: VectorAggregateStates,
): VectorAggregateStateSnapshots {
  const snapshots: VectorAggregateStateSnapshots = {};
  for (const [alias, state] of Object.entries(states)) {
    snapshots[alias] = snapshotVectorAggregateState(state);
  }
  return snapshots;
}

export function restoreVectorAggregateStates(
  snapshots: VectorAggregateStateSnapshots,
  options: VectorAggregateOptions = {},
): VectorAggregateStates {
  const states: VectorAggregateStates = {};
  for (const [alias, snapshot] of Object.entries(snapshots)) {
    states[alias] = vectorAggregateStateFromSnapshot(snapshot);
    enforceStateBudget(states[alias], options.budget);
  }
  return states;
}

export function vectorAggregateBatch(
  spec: AggregateSpec,
  batch: Batch,
  selection?: Selection,
  options: VectorAggregateOptions = {},
): Record<string, unknown> {
  const states = createVectorAggregateStates(spec, options);
  updateVectorAggregateStates(states, spec, batch, selection, options);
  return finalizeVectorAggregateStates(states);
}

function createVectorAggregateState(aggregate: AggregateExpr): VectorAggregateState {
  switch (aggregate.op) {
    case "count":
      return { op: "count", count: 0 };
    case "sum":
      return { op: "sum", sum: 0 };
    case "avg":
      return { op: "avg", sum: 0, count: 0 };
    case "var_samp":
    case "var_pop":
    case "stddev_samp":
    case "stddev_pop":
      return { op: aggregate.op, count: 0, mean: 0, m2: 0 };
    case "median":
      return { op: "median", values: [], memoryBytes: 0 };
    case "quantile":
      return { op: "quantile", quantile: requiredQuantile(aggregate), values: [], memoryBytes: 0 };
    case "min":
    case "max":
      return { op: aggregate.op, value: null };
    case "count_distinct":
    case "approx_count_distinct":
      return createDistinctAggregateState(aggregate.op);
    case "mode":
      return createModeAggregateState();
    case "first":
    case "last":
    case "any":
      return { op: aggregate.op, seen: false, value: null };
  }
}

function updateVectorAggregateState(
  state: VectorAggregateState,
  aggregate: AggregateExpr,
  batch: Batch,
  selection?: Selection,
  budget?: QueryBudget,
): void {
  if (
    state.op === "count" &&
    aggregate.op === "count" &&
    aggregate.column === undefined &&
    aggregate.expr === undefined
  ) {
    state.count += selectedRowCount(batch.rowCount, selection);
    return;
  }
  const values = aggregate.expr === undefined ? undefined : batchExprValues(batch, aggregate.expr);
  const vector =
    aggregate.expr !== undefined || aggregate.column === undefined
      ? undefined
      : batch.columns[aggregate.column];
  if (aggregate.column !== undefined && vector === undefined) {
    throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${aggregate.column}`, {
      column: aggregate.column,
    });
  }
  if (
    values === undefined &&
    vector !== undefined &&
    updateDirectUtf8Distinct(state, aggregate, vector, batch.rowCount, selection, budget)
  ) {
    return;
  }

  for (let index = 0; index < batch.rowCount; index += 1) {
    if (selection !== undefined && selection[index] !== 1) continue;
    const value =
      values !== undefined
        ? values.valueAt(index)
        : vector === undefined
          ? true
          : scalarVectorValue(vector, index);
    updateStateValue(state, aggregateScalarValue(value), budget);
  }
}

function aggregateScalarValue(value: Scalar): VectorAggregateValue {
  if (value instanceof Uint8Array) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Binary values are not aggregate values");
  }
  if (isIntervalValue(value)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Interval values are not aggregate values");
  }
  return value;
}

function updateDirectUtf8Distinct(
  state: VectorAggregateState,
  aggregate: AggregateExpr,
  vector: Vector,
  rowCount: number,
  selection?: Selection,
  budget?: QueryBudget,
): boolean {
  if (
    vector.type !== "utf8" ||
    (state.op !== "count_distinct" && state.op !== "approx_count_distinct") ||
    aggregate.op !== state.op
  ) {
    return false;
  }
  const values = vector.values;
  const valid = vector.valid;
  if (budget?.maxMemoryBytes === undefined && budget?.maxBufferedRows === undefined) {
    updateDirectUtf8DistinctFromBatch(state, values, rowCount, selection, valid);
    return true;
  }
  if (selection === undefined && valid === undefined) {
    for (let index = 0; index < rowCount; index += 1) {
      addDistinctValue(state, `string:${values[index] ?? ""}`, budget);
    }
    return true;
  }
  for (let index = 0; index < rowCount; index += 1) {
    if (selection !== undefined && selection[index] !== 1) continue;
    if (valid !== undefined && valid[index] !== 1) continue;
    addDistinctValue(state, `string:${values[index] ?? ""}`, budget);
  }
  return true;
}

function updateDirectUtf8DistinctFromBatch(
  state: Extract<VectorAggregateState, { op: "count_distinct" | "approx_count_distinct" }>,
  values: string[],
  rowCount: number,
  selection?: Selection,
  valid?: Uint8Array,
): void {
  const batchValues: string[] = [];
  for (let index = 0; index < rowCount; index += 1) {
    if (selection !== undefined && selection[index] !== 1) continue;
    if (valid !== undefined && valid[index] !== 1) continue;
    batchValues.push(values[index] ?? "");
  }
  addDistinctSortedStringRun(state, batchValues);
}

function updateStateValue(
  state: VectorAggregateState,
  value: VectorAggregateValue,
  budget?: QueryBudget,
): void {
  switch (state.op) {
    case "count":
      if (value !== null) state.count += 1;
      return;
    case "sum":
      if (value === null) return;
      if (typeof value !== "number") throwAggregateType("sum");
      state.sum += value;
      return;
    case "avg":
      if (value === null) return;
      if (typeof value !== "number") throwAggregateType("avg");
      state.sum += value;
      state.count += 1;
      return;
    case "var_samp":
    case "var_pop":
    case "stddev_samp":
    case "stddev_pop":
      updateVariance(state, value);
      return;
    case "median":
      updateMedian(state, value, budget);
      return;
    case "quantile":
      updateQuantile(state, value, budget);
      return;
    case "min":
    case "max":
      updateMinMax(state, value);
      return;
    case "count_distinct":
    case "approx_count_distinct":
      if (value !== null) addDistinctValue(state, distinctKey(value), budget);
      return;
    case "mode":
      if (value !== null) addModeValue(state, value, budget);
      return;
    case "first":
    case "any":
      if (!state.seen) {
        state.value = value;
        state.seen = true;
      }
      return;
    case "last":
      state.value = value;
      state.seen = true;
      return;
  }
}

export function updateVectorAggregateStateValue(
  state: VectorAggregateState,
  value: VectorAggregateValue,
  options: VectorAggregateOptions = {},
): void {
  updateStateValue(state, value, options.budget);
}

function updateMinMax(
  state: Extract<VectorAggregateState, { op: "min" | "max" }>,
  value: VectorAggregateValue,
): void {
  if (value === null) return;
  if (
    typeof value !== "string" &&
    typeof value !== "number" &&
    typeof value !== "boolean" &&
    typeof value !== "bigint" &&
    !isTimestampValue(value)
  ) {
    throwAggregateType(state.op);
  }
  if (state.value === null) {
    state.value = value;
    return;
  }
  if (typeof state.value !== typeof value) throwAggregateType(state.op);
  if (isTimestampValue(state.value) || isTimestampValue(value)) {
    if (!isTimestampValue(state.value) || !isTimestampValue(value)) throwAggregateType(state.op);
    const comparison = compareTimestampValues(value, state.value);
    if (state.op === "min" ? comparison < 0 : comparison > 0) state.value = value;
    return;
  }
  if (state.op === "min" ? value < state.value : value > state.value) state.value = value;
}

function updateVariance(
  state: Extract<
    VectorAggregateState,
    { op: "var_samp" | "var_pop" | "stddev_samp" | "stddev_pop" }
  >,
  value: VectorAggregateValue,
): void {
  if (value === null) return;
  if (typeof value !== "number") throwAggregateType(state.op);
  state.count += 1;
  const delta = value - state.mean;
  state.mean += delta / state.count;
  state.m2 += delta * (value - state.mean);
}

function mergeVariance(
  target: Extract<
    VectorAggregateState,
    { op: "var_samp" | "var_pop" | "stddev_samp" | "stddev_pop" }
  >,
  source: Extract<
    VectorAggregateState,
    { op: "var_samp" | "var_pop" | "stddev_samp" | "stddev_pop" }
  >,
): void {
  if (source.count === 0) return;
  if (target.count === 0) {
    target.count = source.count;
    target.mean = source.mean;
    target.m2 = source.m2;
    return;
  }
  const count = target.count + source.count;
  const delta = source.mean - target.mean;
  target.mean = (target.mean * target.count + source.mean * source.count) / count;
  target.m2 += source.m2 + (delta * delta * (target.count * source.count)) / count;
  target.count = count;
}

function mergeVectorAggregateState(
  target: VectorAggregateState,
  source: VectorAggregateState,
  budget?: QueryBudget,
): void {
  if (target.op !== source.op) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Cannot merge different aggregate states", {
      target: target.op,
      source: source.op,
    });
  }
  switch (target.op) {
    case "count": {
      const next = sameState(source, "count");
      target.count += next.count;
      return;
    }
    case "sum": {
      const next = sameState(source, "sum");
      target.sum += next.sum;
      return;
    }
    case "avg": {
      const next = sameState(source, "avg");
      target.sum += next.sum;
      target.count += next.count;
      return;
    }
    case "var_samp":
    case "var_pop":
    case "stddev_samp":
    case "stddev_pop":
      mergeVariance(target, sameState(source, target.op));
      return;
    case "median": {
      const next = sameState(source, "median");
      for (const value of next.values) addMedianValue(target, value, budget);
      return;
    }
    case "quantile": {
      const next = sameState(source, "quantile");
      for (const value of next.values) addQuantileValue(target, value, budget);
      return;
    }
    case "min":
    case "max":
      updateMinMax(target, sameState(source, target.op).value);
      return;
    case "count_distinct":
    case "approx_count_distinct": {
      const next = sameState(source, target.op);
      addDistinctValues(target, next.values, budget);
      return;
    }
    case "mode": {
      const next = sameState(source, "mode");
      for (const [key, entry] of next.values)
        addModeEntry(target, key, entry.value, entry.count, budget);
      return;
    }
    case "first": {
      const next = sameState(source, "first");
      if (!target.seen && next.seen) {
        target.value = next.value;
        target.seen = true;
      }
      return;
    }
    case "any": {
      const next = sameState(source, "any");
      if (!target.seen && next.seen) {
        target.value = next.value;
        target.seen = true;
      }
      return;
    }
    case "last": {
      const next = sameState(source, "last");
      if (next.seen) {
        target.value = next.value;
        target.seen = true;
      }
      return;
    }
  }
}

function mergeDistinctSnapshot(
  target: VectorAggregateState,
  snapshot: Extract<
    VectorAggregateStateSnapshot,
    { op: "count_distinct" | "approx_count_distinct" }
  >,
  budget?: QueryBudget,
): void {
  if (target.op !== snapshot.op) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `Cannot merge ${snapshot.op} into ${target.op}`, {
      target: target.op,
      source: snapshot.op,
    });
  }
  mergeDistinctSortedValues(target, snapshot.values, budget);
}

function sameState<Op extends VectorAggregateState["op"]>(
  state: VectorAggregateState,
  op: Op,
): Extract<VectorAggregateState, { op: Op }> {
  if (state.op !== op) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Aggregate state operation mismatch", {
      expected: op,
      actual: state.op,
    });
  }
  return state as Extract<VectorAggregateState, { op: Op }>;
}

function finalizeVectorAggregateState(state: VectorAggregateState): unknown {
  switch (state.op) {
    case "count":
      return state.count;
    case "sum":
      return state.sum;
    case "avg":
      return state.count === 0 ? null : state.sum / state.count;
    case "var_samp":
      return state.count < 2 ? null : state.m2 / (state.count - 1);
    case "stddev_samp":
      return state.count < 2 ? null : Math.sqrt(state.m2 / (state.count - 1));
    case "var_pop":
      return state.count === 0 ? null : state.m2 / state.count;
    case "stddev_pop":
      return state.count === 0 ? null : Math.sqrt(state.m2 / state.count);
    case "median":
      return finalizeMedian(state);
    case "quantile":
      return finalizeQuantile(state);
    case "min":
    case "max":
      return state.value;
    case "count_distinct":
    case "approx_count_distinct":
      return distinctValueCount(state);
    case "mode":
      return finalizeMode(state);
    case "first":
    case "last":
    case "any":
      return state.seen ? state.value : null;
  }
}

function cloneVectorAggregateState(state: VectorAggregateState): VectorAggregateState {
  switch (state.op) {
    case "count":
      return { ...state };
    case "sum":
      return { ...state };
    case "avg":
    case "var_samp":
    case "var_pop":
    case "stddev_samp":
    case "stddev_pop":
      return { ...state };
    case "median":
      return { op: "median", values: [...state.values], memoryBytes: state.memoryBytes };
    case "quantile":
      return {
        op: "quantile",
        quantile: state.quantile,
        values: [...state.values],
        memoryBytes: state.memoryBytes,
      };
    case "min":
    case "max":
      return { ...state };
    case "count_distinct":
    case "approx_count_distinct":
      return cloneDistinctAggregateState(state);
    case "mode":
      return {
        op: "mode",
        values: new Map([...state.values].map(([key, entry]) => [key, { ...entry }])),
        memoryBytes: state.memoryBytes,
      };
    case "first":
    case "last":
    case "any":
      return { ...state };
  }
}

function enforceStateBudget(state: VectorAggregateState, budget?: QueryBudget): void {
  if (state.op === "count_distinct" || state.op === "approx_count_distinct") {
    enforceDistinctStateBudget(state, budget);
  } else if (state.op === "mode") {
    enforceModeBudget(state, budget);
  } else if (state.op === "median") {
    enforceMedianBudget(state, budget);
  } else if (state.op === "quantile") {
    enforceQuantileBudget(state, budget);
  }
}

function updateMedian(
  state: Extract<VectorAggregateState, { op: "median" }>,
  value: VectorAggregateValue,
  budget?: QueryBudget,
): void {
  if (value === null) return;
  if (typeof value !== "number" && typeof value !== "string") throwAggregateType("median");
  addMedianValue(state, value, budget);
}

function addMedianValue(
  state: Extract<VectorAggregateState, { op: "median" }>,
  value: number | string,
  budget?: QueryBudget,
): void {
  state.values.push(value);
  state.memoryBytes = medianMemoryBytes(state, budget);
  enforceMedianBudget(state, budget);
}

function finalizeMedian(
  state: Extract<VectorAggregateState, { op: "median" }>,
): number | string | null {
  if (state.values.length === 0) return null;
  const values = [...state.values].sort(compareMedianValues);
  const middle = Math.floor((values.length - 1) / 2);
  const left = values[middle];
  if (left === undefined) return null;
  if (values.length % 2 === 1) return left;
  const right = values[middle + 1];
  if (typeof left === "number" && typeof right === "number") return (left + right) / 2;
  return left;
}

function compareMedianValues(left: number | string, right: number | string): number {
  if (typeof left !== typeof right) throwAggregateType("median");
  return left < right ? -1 : left > right ? 1 : 0;
}

function enforceMedianBudget(
  state: Extract<VectorAggregateState, { op: "median" }>,
  budget?: QueryBudget,
): void {
  if (budget?.maxMemoryBytes !== undefined) state.memoryBytes = medianMemoryBytes(state, budget);
  if (budget?.maxBufferedRows !== undefined && state.values.length > budget.maxBufferedRows) {
    throwBudget("buffered rows", budget.maxBufferedRows, state.values.length);
  }
  if (budget?.maxMemoryBytes !== undefined && state.memoryBytes > budget.maxMemoryBytes) {
    throwBudget("operator memory bytes", budget.maxMemoryBytes, state.memoryBytes);
  }
}

function medianMemoryBytes(
  state: Extract<VectorAggregateState, { op: "median" }>,
  budget?: QueryBudget,
): number {
  if (budget?.maxMemoryBytes === undefined && state.values.length !== 0) return 0;
  return new TextEncoder().encode(JSON.stringify(state.values)).byteLength;
}

function updateQuantile(
  state: Extract<VectorAggregateState, { op: "quantile" }>,
  value: VectorAggregateValue,
  budget?: QueryBudget,
): void {
  if (value === null) return;
  if (typeof value !== "number") throwAggregateType("quantile");
  addQuantileValue(state, value, budget);
}

function addQuantileValue(
  state: Extract<VectorAggregateState, { op: "quantile" }>,
  value: number,
  budget?: QueryBudget,
): void {
  state.values.push(value);
  state.memoryBytes = quantileMemoryBytes(state, budget);
  enforceQuantileBudget(state, budget);
}

function finalizeQuantile(state: Extract<VectorAggregateState, { op: "quantile" }>): number | null {
  return continuousQuantile(state.values, state.quantile);
}

function enforceQuantileBudget(
  state: Extract<VectorAggregateState, { op: "quantile" }>,
  budget?: QueryBudget,
): void {
  if (budget?.maxMemoryBytes !== undefined) state.memoryBytes = quantileMemoryBytes(state, budget);
  if (budget?.maxBufferedRows !== undefined && state.values.length > budget.maxBufferedRows) {
    throwBudget("buffered rows", budget.maxBufferedRows, state.values.length);
  }
  if (budget?.maxMemoryBytes !== undefined && state.memoryBytes > budget.maxMemoryBytes) {
    throwBudget("operator memory bytes", budget.maxMemoryBytes, state.memoryBytes);
  }
}

function quantileMemoryBytes(
  state: Extract<VectorAggregateState, { op: "quantile" }>,
  budget?: QueryBudget,
): number {
  if (budget?.maxMemoryBytes === undefined && state.values.length !== 0) return 0;
  return new TextEncoder().encode(JSON.stringify(state.values)).byteLength;
}

function createModeAggregateState(): Extract<VectorAggregateState, { op: "mode" }> {
  return { op: "mode", values: new Map(), memoryBytes: 0 };
}

function addModeValue(
  state: Extract<VectorAggregateState, { op: "mode" }>,
  value: Exclude<VectorAggregateValue, null>,
  budget?: QueryBudget,
): void {
  addModeEntry(state, distinctKey(value), value, 1, budget);
}

function addModeEntry(
  state: Extract<VectorAggregateState, { op: "mode" }>,
  key: string,
  value: Exclude<VectorAggregateValue, null>,
  count: number,
  budget?: QueryBudget,
): void {
  const existing = state.values.get(key);
  if (existing === undefined) state.values.set(key, { value, count });
  else existing.count += count;
  state.memoryBytes = modeMemoryBytes(state, budget);
  enforceModeBudget(state, budget);
}

function finalizeMode(state: Extract<VectorAggregateState, { op: "mode" }>): VectorAggregateValue {
  let best: { value: Exclude<VectorAggregateValue, null>; count: number } | undefined;
  for (const entry of state.values.values()) {
    if (best === undefined || entry.count > best.count) best = entry;
  }
  return best?.value ?? null;
}

function enforceModeBudget(
  state: Extract<VectorAggregateState, { op: "mode" }>,
  budget?: QueryBudget,
): void {
  if (budget?.maxMemoryBytes !== undefined) state.memoryBytes = modeMemoryBytes(state, budget);
  if (budget?.maxBufferedRows !== undefined && state.values.size > budget.maxBufferedRows) {
    throwBudget("buffered rows", budget.maxBufferedRows, state.values.size);
  }
  if (budget?.maxMemoryBytes !== undefined && state.memoryBytes > budget.maxMemoryBytes) {
    throwBudget("operator memory bytes", budget.maxMemoryBytes, state.memoryBytes);
  }
}

function modeMemoryBytes(
  state: Extract<VectorAggregateState, { op: "mode" }>,
  budget?: QueryBudget,
): number {
  return distinctMemoryBytes(new Set(state.values.keys()), budget);
}

function throwBudget(metric: string, limit: number, actual: number): never {
  throw new LakeqlError(
    "LAKEQL_BUDGET_EXCEEDED",
    `Query exceeded ${metric} budget (${actual} > ${limit}). Add a partition filter, date filter, h3 filter, or limit.`,
    { metric, limit, actual },
  );
}

function throwAggregateType(op: string): never {
  throw new LakeqlError("LAKEQL_TYPE_ERROR", `Aggregate ${op} requires compatible values`, { op });
}
