import { unmeasuredMemoryBytes } from "./aggregate-memory.js";
import { isTimestampValue, TimestampValue } from "./timestamp.js";
import type {
  VectorAggregateSnapshotValue,
  VectorAggregateState,
  VectorAggregateStateSnapshot,
  VectorAggregateValue,
} from "./vector-aggregate.js";
import { distinctMemoryBytes, distinctSnapshotValues } from "./vector-aggregate-distinct.js";

export function snapshotVectorAggregateState(
  state: VectorAggregateState,
): VectorAggregateStateSnapshot {
  switch (state.op) {
    case "count":
      return { op: "count", count: state.count };
    case "sum":
      return { op: "sum", sum: state.sum };
    case "avg":
      return { op: "avg", sum: state.sum, count: state.count };
    case "var_samp":
    case "var_pop":
    case "stddev_samp":
    case "stddev_pop":
      return { op: state.op, count: state.count, mean: state.mean, m2: state.m2 };
    case "median":
      return { op: "median", values: [...state.values] };
    case "quantile":
      return { op: "quantile", quantile: state.quantile, values: [...state.values] };
    case "min":
    case "max":
      return { op: state.op, value: snapshotValue(state.value) };
    case "count_distinct":
    case "approx_count_distinct":
      return { op: state.op, values: distinctSnapshotValues(state) };
    case "mode": {
      const values: Extract<VectorAggregateStateSnapshot, { op: "mode" }>["values"] = [];
      for (const [key, entry] of state.values) {
        values.push({ key, value: snapshotValue(entry.value), count: entry.count });
      }
      return { op: "mode", values };
    }
    case "first":
    case "last":
    case "any":
      return { op: state.op, seen: state.seen, value: snapshotValue(state.value) };
  }
}

export function vectorAggregateStateFromSnapshot(
  snapshot: VectorAggregateStateSnapshot,
): VectorAggregateState {
  switch (snapshot.op) {
    case "count":
      return { op: "count", count: snapshot.count };
    case "sum":
      return { op: "sum", sum: snapshot.sum };
    case "avg":
      return { op: "avg", sum: snapshot.sum, count: snapshot.count };
    case "var_samp":
    case "var_pop":
    case "stddev_samp":
    case "stddev_pop":
      return { op: snapshot.op, count: snapshot.count, mean: snapshot.mean, m2: snapshot.m2 };
    case "median":
      return { op: "median", values: [...snapshot.values], memoryBytes: 0 };
    case "quantile":
      return {
        op: "quantile",
        quantile: snapshot.quantile,
        values: [...snapshot.values],
        memoryBytes: 0,
      };
    case "min":
    case "max":
      return { op: snapshot.op, value: restoreValue(snapshot.value) };
    case "count_distinct":
    case "approx_count_distinct": {
      const values = new Set(snapshot.values);
      return { op: snapshot.op, values, memoryBytes: distinctMemoryBytes(values) };
    }
    case "mode": {
      const values: Extract<VectorAggregateState, { op: "mode" }>["values"] = new Map();
      for (const entry of snapshot.values) {
        values.set(entry.key, {
          value: restoreValue(entry.value) as Exclude<VectorAggregateValue, null>,
          count: entry.count,
        });
      }
      return { op: "mode", values, memoryBytes: values.size === 0 ? 2 : unmeasuredMemoryBytes };
    }
    case "first":
    case "last":
    case "any":
      return { op: snapshot.op, seen: snapshot.seen, value: restoreValue(snapshot.value) };
  }
}

function snapshotValue(value: VectorAggregateValue): VectorAggregateSnapshotValue {
  if (isTimestampValue(value)) {
    return {
      type: "timestamp",
      epochNanoseconds: value.epochNanoseconds.toString(),
      unit: value.unit,
      isAdjustedToUTC: value.isAdjustedToUTC,
    };
  }
  return typeof value === "bigint" ? { type: "bigint", value: value.toString() } : value;
}

function restoreValue(value: VectorAggregateSnapshotValue): VectorAggregateValue {
  if (isBigintSnapshot(value)) return BigInt(value.value);
  if (isTimestampSnapshot(value)) {
    return new TimestampValue(BigInt(value.epochNanoseconds), value.unit, value.isAdjustedToUTC);
  }
  return value;
}

function isBigintSnapshot(
  value: VectorAggregateSnapshotValue,
): value is { type: "bigint"; value: string } {
  return (
    typeof value === "object" &&
    value !== null &&
    "type" in value &&
    value.type === "bigint" &&
    "value" in value &&
    typeof value.value === "string"
  );
}

function isTimestampSnapshot(value: VectorAggregateSnapshotValue): value is {
  type: "timestamp";
  epochNanoseconds: string;
  unit: TimestampValue["unit"];
  isAdjustedToUTC: boolean;
} {
  return (
    typeof value === "object" &&
    value !== null &&
    "type" in value &&
    value.type === "timestamp" &&
    "epochNanoseconds" in value &&
    typeof value.epochNanoseconds === "string" &&
    "unit" in value &&
    (value.unit === "millis" || value.unit === "micros" || value.unit === "nanos") &&
    "isAdjustedToUTC" in value &&
    typeof value.isAdjustedToUTC === "boolean"
  );
}
