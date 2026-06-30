import {
  type Batch,
  type BatchExprValues,
  batchExprValues,
  batchFromColumns,
  type Selection,
  scalarVectorValue,
  selectedRowIndices,
  type Vector,
  vectorLength,
} from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { Scalar } from "./expr.js";
import type { AggregateExpr, AggregateSpec, QueryBudget } from "./query.js";
import { isTimestampValue, TimestampValue } from "./timestamp.js";
import type { Row } from "./types.js";
import {
  createVectorAggregateStates,
  finalizeVectorAggregateStates,
  mergeVectorAggregateStates,
  restoreVectorAggregateStates,
  snapshotVectorAggregateStates,
  updateVectorAggregateStateValue,
  type VectorAggregateOptions,
  type VectorAggregateState,
  type VectorAggregateStateSnapshots,
  type VectorAggregateStates,
  type VectorAggregateValue,
} from "./vector-aggregate.js";

export interface VectorGroupByOptions extends VectorAggregateOptions {
  maxGroups?: number;
}

export interface VectorGroupByState {
  readonly keys: readonly string[];
  readonly spec: AggregateSpec;
  readonly groups: Map<string, VectorGroup>;
}

export interface VectorGroup {
  keyValues: Scalar[];
  states: VectorAggregateStates;
}

export type VectorGroupBySnapshotValue =
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

export interface VectorGroupByGroupSnapshot {
  keyValues: VectorGroupBySnapshotValue[];
  states: VectorAggregateStateSnapshots;
}

export interface VectorGroupByStateSnapshot {
  groups: VectorGroupByGroupSnapshot[];
}

interface AggregateInput {
  alias: string;
  update(group: VectorGroup, index: number, options: VectorAggregateOptions): void;
}

interface GroupKeyEncoder {
  groupIdAt(index: number): number;
  keyValues(groupId: number): Scalar[];
}

export function createVectorGroupByState(
  keys: readonly string[],
  spec: AggregateSpec,
): VectorGroupByState {
  return {
    keys: [...keys],
    spec,
    groups: new Map(),
  };
}

export function updateVectorGroupByState(
  state: VectorGroupByState,
  batch: Batch,
  selection?: Selection,
  options: VectorGroupByOptions = {},
): number {
  const encodedMatched = updateEncodedVectorGroupByState(state, batch, selection, options);
  if (encodedMatched !== undefined) return encodedMatched;
  const keyReader = vectorGroupKeyReader(state.keys, batch);
  const aggregateInputs = aggregateInputValues(state.spec, batch);
  let matched = 0;
  for (const index of selectedRowIndices(batch.rowCount, selection)) {
    matched += 1;
    const key = keyReader.keyAt(index);
    let group = state.groups.get(key);
    if (group === undefined) {
      if (options.maxGroups !== undefined && state.groups.size >= options.maxGroups) {
        throw new LakeqlError(
          "LAKEQL_GROUP_LIMIT_EXCEEDED",
          `Query exceeded group budget (${state.groups.size + 1} > ${options.maxGroups})`,
          { limit: options.maxGroups, actual: state.groups.size + 1 },
        );
      }
      const keyValues = keyReader.valuesAt(index);
      group = {
        keyValues,
        states: createVectorAggregateStates(state.spec, options),
      };
      state.groups.set(key, group);
      enforceGroupByMemoryBudget(state, options.budget);
    }
    for (const input of aggregateInputs) {
      input.update(group, index, options);
    }
    enforceGroupByMemoryBudget(state, options.budget);
  }
  return matched;
}

function updateEncodedVectorGroupByState(
  state: VectorGroupByState,
  batch: Batch,
  selection: Selection | undefined,
  options: VectorGroupByOptions,
): number | undefined {
  const encoder = createGroupKeyEncoder(state.keys, batch);
  if (encoder === undefined) return undefined;
  const aggregateInputs = aggregateInputValues(state.spec, batch);
  const groupsById: (VectorGroup | undefined)[] = [];
  let matched = 0;
  for (let index = 0; index < batch.rowCount; index += 1) {
    if (selection !== undefined && selection[index] !== 1) continue;
    matched += 1;
    const groupId = encoder.groupIdAt(index);
    let group = groupsById[groupId];
    if (group === undefined) {
      group = getOrCreateGroupByValues(state, encoder.keyValues(groupId), options);
      groupsById[groupId] = group;
    }
    for (const input of aggregateInputs) {
      input.update(group, index, options);
    }
    enforceGroupByMemoryBudget(state, options.budget);
  }
  return matched;
}

function getOrCreateGroupByValues(
  state: VectorGroupByState,
  keyValues: Scalar[],
  options: VectorGroupByOptions,
): VectorGroup {
  const key = groupKey(keyValues);
  let group = state.groups.get(key);
  if (group !== undefined) return group;
  if (options.maxGroups !== undefined && state.groups.size >= options.maxGroups) {
    throw new LakeqlError(
      "LAKEQL_GROUP_LIMIT_EXCEEDED",
      `Query exceeded group budget (${state.groups.size + 1} > ${options.maxGroups})`,
      { limit: options.maxGroups, actual: state.groups.size + 1 },
    );
  }
  group = {
    keyValues,
    states: createVectorAggregateStates(state.spec, options),
  };
  state.groups.set(key, group);
  enforceGroupByMemoryBudget(state, options.budget);
  return group;
}

function createGroupKeyEncoder(keys: readonly string[], batch: Batch): GroupKeyEncoder | undefined {
  if (keys.length === 0) return undefined;
  const vectors = keys.map((key) => {
    const vector = batch.columns[key];
    if (vector === undefined) {
      throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${key}`, { column: key });
    }
    return vector;
  });
  if (vectors.some((vector) => !canEncodeScalarVector(vector))) return undefined;
  if (vectors.length === 1) {
    const vector = vectors[0];
    if (vector === undefined) return undefined;
    if (vector.type === "dict") return dictionaryGroupKeyEncoder(vector);
    return scalarGroupKeyEncoder(vector);
  }
  return compositeGroupKeyEncoder(vectors);
}

function canEncodeScalarVector(vector: Vector): boolean {
  switch (vector.type) {
    case "null":
    case "f64":
    case "i64":
    case "timestamp":
    case "bool":
    case "utf8":
    case "dict":
      return true;
    case "binary":
    case "list":
    case "struct":
    case "map":
      return false;
  }
}

function dictionaryGroupKeyEncoder(vector: Extract<Vector, { type: "dict" }>): GroupKeyEncoder {
  const nullGroupId = vectorLength(vector.dictionary);
  return {
    groupIdAt(index) {
      return vector.valid !== undefined && vector.valid[index] === 0
        ? nullGroupId
        : (vector.indices[index] ?? 0);
    },
    keyValues(groupId) {
      return [groupId === nullGroupId ? null : scalarVectorValue(vector.dictionary, groupId)];
    },
  };
}

function scalarGroupKeyEncoder(vector: Vector): GroupKeyEncoder {
  const ids = new Map<string, number>();
  const values: Scalar[][] = [];
  return {
    groupIdAt(index) {
      const value = scalarVectorValue(vector, index);
      const key = scalarGroupKeyPart(value);
      const existing = ids.get(key);
      if (existing !== undefined) return existing;
      const next = values.length;
      ids.set(key, next);
      values.push([value]);
      return next;
    },
    keyValues(groupId) {
      return values[groupId] ?? [null];
    },
  };
}

function compositeGroupKeyEncoder(vectors: readonly Vector[]): GroupKeyEncoder {
  const ids = new Map<string, number>();
  const values: Scalar[][] = [];
  return {
    groupIdAt(index) {
      const keyValues = vectors.map((vector) => scalarVectorValue(vector, index));
      const key = groupKey(keyValues);
      const existing = ids.get(key);
      if (existing !== undefined) return existing;
      const next = values.length;
      ids.set(key, next);
      values.push(keyValues);
      return next;
    },
    keyValues(groupId) {
      return values[groupId] ?? [];
    },
  };
}

export function getOrCreateVectorGroup(
  state: VectorGroupByState,
  batch: Batch,
  index: number,
  options: VectorGroupByOptions = {},
): VectorGroup {
  const keyReader = vectorGroupKeyReader(state.keys, batch);
  const key = keyReader.keyAt(index);
  let group = state.groups.get(key);
  if (group !== undefined) return group;
  if (options.maxGroups !== undefined && state.groups.size >= options.maxGroups) {
    throw new LakeqlError(
      "LAKEQL_GROUP_LIMIT_EXCEEDED",
      `Query exceeded group budget (${state.groups.size + 1} > ${options.maxGroups})`,
      { limit: options.maxGroups, actual: state.groups.size + 1 },
    );
  }
  group = {
    keyValues: keyReader.valuesAt(index),
    states: createVectorAggregateStates(state.spec, options),
  };
  state.groups.set(key, group);
  enforceGroupByMemoryBudget(state, options.budget);
  return group;
}

export function updateVectorGroupAggregateValue(
  group: VectorGroup,
  alias: string,
  value: VectorAggregateValue,
  options: VectorAggregateOptions = {},
): void {
  const aggregateState = group.states[alias];
  if (aggregateState === undefined) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", `Missing vector aggregate state ${alias}`, {
      alias,
    });
  }
  updateVectorAggregateStateValue(aggregateState, value, options);
}

export function enforceVectorGroupByBudget(state: VectorGroupByState, budget?: QueryBudget): void {
  enforceGroupByMemoryBudget(state, budget);
}

interface VectorGroupKeyReader {
  keyAt(index: number): string;
  valuesAt(index: number): Scalar[];
}

function vectorGroupKeyReader(keys: readonly string[], batch: Batch): VectorGroupKeyReader {
  const vectors = keys.map((key) => {
    const vector = batch.columns[key];
    if (vector === undefined) {
      throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${key}`, { column: key });
    }
    return vector;
  });
  if (vectors.length === 1) {
    const vector = vectors[0];
    if (vector === undefined) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Group key vector is missing");
    }
    return {
      keyAt(index) {
        return vectorGroupKeyPart(vector, index);
      },
      valuesAt(index) {
        return [scalarVectorValue(vector, index)];
      },
    };
  }
  return {
    keyAt(index) {
      let key = "";
      for (const vector of vectors) {
        const part = vectorGroupKeyPart(vector, index);
        key += `${part.length}:${part}`;
      }
      return key;
    },
    valuesAt(index) {
      return vectors.map((vector) => scalarVectorValue(vector, index));
    },
  };
}

function vectorGroupKeyPart(vector: Vector, index: number): string {
  if ("valid" in vector && vector.valid !== undefined && vector.valid[index] === 0) {
    return scalarGroupKeyPart(null);
  }
  switch (vector.type) {
    case "null":
      return scalarGroupKeyPart(null);
    case "f64":
      return scalarGroupKeyPart(vector.values[index] ?? 0);
    case "i64":
      return scalarGroupKeyPart(vector.values[index] ?? 0n);
    case "timestamp":
      return scalarGroupKeyPart(scalarVectorValue(vector, index));
    case "bool":
      return scalarGroupKeyPart(vector.values[index] === 1);
    case "utf8":
      return scalarGroupKeyPart(vector.values[index] ?? "");
    case "binary":
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Binary values are not group keys");
    case "dict":
      return vectorGroupKeyPart(vector.dictionary, vector.indices[index] ?? 0);
    case "list":
    case "struct":
    case "map":
      return groupKey([scalarVectorValue(vector, index)]);
  }
}

export function finalizeVectorGroupByRows(state: VectorGroupByState): Row[] {
  const rows: Row[] = [];
  for (const group of state.groups.values()) {
    const row: Row = {};
    for (let index = 0; index < state.keys.length; index += 1) {
      const key = state.keys[index];
      if (key !== undefined) row[key] = group.keyValues[index] ?? null;
    }
    Object.assign(row, finalizeVectorAggregateStates(group.states));
    rows.push(row);
  }
  return rows;
}

export function finalizeVectorGroupByBatch(state: VectorGroupByState): Batch {
  const columns: Record<string, Scalar[]> = {};
  for (const key of state.keys) columns[key] = [];
  for (const alias of Object.keys(state.spec)) columns[alias] = [];
  for (const row of finalizeVectorGroupByRows(state)) {
    for (const column of Object.keys(columns)) {
      columns[column]?.push((row[column] as Scalar | undefined) ?? null);
    }
  }
  return batchFromColumns(columns);
}

export function snapshotVectorGroupByState(state: VectorGroupByState): VectorGroupByStateSnapshot {
  return {
    groups: [...state.groups.values()].map((group) => ({
      keyValues: group.keyValues.map(snapshotGroupValue),
      states: snapshotVectorAggregateStates(group.states),
    })),
  };
}

export function restoreVectorGroupByState(
  keys: readonly string[],
  spec: AggregateSpec,
  snapshot: VectorGroupByStateSnapshot,
  options: VectorGroupByOptions = {},
): VectorGroupByState {
  const state = createVectorGroupByState(keys, spec);
  for (const groupSnapshot of snapshot.groups) {
    const keyValues = groupSnapshot.keyValues.map(restoreGroupValue);
    state.groups.set(groupKey(keyValues), {
      keyValues,
      states: restoreVectorAggregateStates(groupSnapshot.states, options),
    });
  }
  enforceGroupByMemoryBudget(state, options.budget);
  return state;
}

export function mergeVectorGroupByStates(
  target: VectorGroupByState,
  source: VectorGroupByState,
  options: VectorGroupByOptions = {},
): void {
  if (target.keys.length !== source.keys.length) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Cannot merge group-by states with different keys", {
      target: target.keys,
      source: source.keys,
    });
  }
  for (let index = 0; index < target.keys.length; index += 1) {
    if (target.keys[index] !== source.keys[index]) {
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        "Cannot merge group-by states with different keys",
        {
          target: target.keys,
          source: source.keys,
        },
      );
    }
  }
  for (const [key, sourceGroup] of source.groups) {
    const targetGroup = target.groups.get(key);
    if (targetGroup === undefined) {
      target.groups.set(key, {
        keyValues: [...sourceGroup.keyValues],
        states: restoreVectorAggregateStates(
          snapshotVectorAggregateStates(sourceGroup.states),
          options,
        ),
      });
    } else {
      mergeVectorAggregateStates(targetGroup.states, sourceGroup.states, options);
    }
    enforceGroupByMemoryBudget(target, options.budget);
  }
}

export function vectorGroupByBatch(
  keys: readonly string[],
  spec: AggregateSpec,
  batch: Batch,
  selection?: Selection,
  options: VectorGroupByOptions = {},
): Batch {
  const state = createVectorGroupByState(keys, spec);
  updateVectorGroupByState(state, batch, selection, options);
  return finalizeVectorGroupByBatch(state);
}

function aggregateInputValues(spec: AggregateSpec, batch: Batch): AggregateInput[] {
  return Object.entries(spec).map(([alias, aggregate]) =>
    aggregateInputValue(alias, aggregate, batch),
  );
}

function aggregateInputValue(
  alias: string,
  aggregate: AggregateExpr,
  batch: Batch,
): AggregateInput {
  const direct = directAggregateInput(alias, aggregate, batch);
  if (direct !== undefined) return direct;
  const valueAt = aggregateInputValueAt(aggregate, batch);
  return {
    alias,
    update(group, index, options) {
      updateVectorGroupAggregateValue(group, alias, valueAt(index), options);
    },
  };
}

function directAggregateInput(
  alias: string,
  aggregate: AggregateExpr,
  batch: Batch,
): AggregateInput | undefined {
  if (aggregate.op === "count" && aggregate.column === undefined && aggregate.expr === undefined) {
    return {
      alias,
      update(group) {
        const state = group.states[alias];
        if (state === undefined) throwMissingAggregateState(alias);
        if (state.op !== "count") throwAggregateStateMismatch(alias, "count", state.op);
        state.count += 1;
      },
    };
  }
  if (aggregate.expr !== undefined || aggregate.column === undefined) return undefined;
  const vector = batch.columns[aggregate.column];
  if (vector === undefined) {
    throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${aggregate.column}`, {
      column: aggregate.column,
    });
  }
  if (aggregate.op === "sum" && vector.type === "f64") return directF64SumInput(alias, vector);
  if (aggregate.op === "avg" && vector.type === "f64") return directF64AvgInput(alias, vector);
  return undefined;
}

function directF64SumInput(
  alias: string,
  vector: Extract<Vector, { type: "f64" }>,
): AggregateInput {
  const values = vector.values;
  const valid = vector.valid;
  return {
    alias,
    update(group, index) {
      if (valid !== undefined && valid[index] === 0) return;
      const state = group.states[alias];
      if (state === undefined) throwMissingAggregateState(alias);
      if (state.op !== "sum") throwAggregateStateMismatch(alias, "sum", state.op);
      state.sum += values[index] ?? 0;
    },
  };
}

function directF64AvgInput(
  alias: string,
  vector: Extract<Vector, { type: "f64" }>,
): AggregateInput {
  const values = vector.values;
  const valid = vector.valid;
  return {
    alias,
    update(group, index) {
      if (valid !== undefined && valid[index] === 0) return;
      const state = group.states[alias];
      if (state === undefined) throwMissingAggregateState(alias);
      if (state.op !== "avg") throwAggregateStateMismatch(alias, "avg", state.op);
      state.sum += values[index] ?? 0;
      state.count += 1;
    },
  };
}

function throwMissingAggregateState(alias: string): never {
  throw new LakeqlError("LAKEQL_TYPE_ERROR", `Missing vector aggregate state ${alias}`, {
    alias,
  });
}

function throwAggregateStateMismatch(
  alias: string,
  expected: VectorAggregateState["op"],
  actual: VectorAggregateState["op"],
): never {
  throw new LakeqlError("LAKEQL_TYPE_ERROR", `Aggregate state ${alias} expected ${expected}`, {
    alias,
    expected,
    actual,
  });
}

function aggregateInputValueAt(
  aggregate: AggregateExpr,
  batch: Batch,
): (index: number) => VectorAggregateValue {
  if (aggregate.expr !== undefined) {
    const values = batchExprValues(batch, aggregate.expr);
    return (index) => aggregateValue(values, index);
  }
  if (aggregate.column === undefined) return () => true;
  const vector = batch.columns[aggregate.column];
  if (vector === undefined) {
    throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${aggregate.column}`, {
      column: aggregate.column,
    });
  }
  return (index) => aggregateScalarValue(scalarVectorValue(vector, index));
}

function aggregateValue(values: BatchExprValues, index: number): VectorAggregateValue {
  return aggregateScalarValue(values.valueAt(index));
}

function aggregateScalarValue(value: Scalar): VectorAggregateValue {
  if (value instanceof Uint8Array) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Binary values are not aggregate values");
  }
  return value;
}

function groupKey(values: readonly Scalar[]): string {
  if (values.length === 1) return scalarGroupKeyPart(values[0] ?? null);
  let key = "";
  for (const value of values) {
    const part = scalarGroupKeyPart(value ?? null);
    key += `${part.length}:${part}`;
  }
  return key;
}

function scalarGroupKeyPart(value: Scalar): string {
  if (value === null) return "null:";
  if (typeof value === "bigint") return `bigint:${value}`;
  if (value instanceof Uint8Array) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Binary values are not group keys");
  }
  if (isTimestampValue(value)) return `timestamp:${value.epochNanoseconds}`;
  return `${typeof value}:${value}`;
}

function snapshotGroupValue(value: Scalar): VectorGroupBySnapshotValue {
  if (value instanceof Uint8Array) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Binary values are not group keys");
  }
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

function restoreGroupValue(value: VectorGroupBySnapshotValue): Scalar {
  if (typeof value !== "object" || value === null) return value;
  if (value.type === "timestamp") {
    return new TimestampValue(BigInt(value.epochNanoseconds), value.unit, value.isAdjustedToUTC);
  }
  return BigInt(value.value);
}

function enforceGroupByMemoryBudget(state: VectorGroupByState, budget?: QueryBudget): void {
  if (budget?.maxMemoryBytes === undefined) return;
  const actual = estimateVectorGroupByMemoryBytes(state);
  if (actual > budget.maxMemoryBytes) {
    throw new LakeqlError(
      "LAKEQL_BUDGET_EXCEEDED",
      `Query exceeded operator memory bytes budget (${actual} > ${budget.maxMemoryBytes})`,
      { metric: "operator memory bytes", limit: budget.maxMemoryBytes, actual },
    );
  }
}

function estimateVectorGroupByMemoryBytes(state: VectorGroupByState): number {
  let bytes = 0;
  for (const [key, group] of state.groups) {
    bytes += 64 + key.length * 2 + group.keyValues.length * 16;
    for (const aggregateState of Object.values(group.states)) {
      bytes += estimateAggregateStateBytes(aggregateState);
    }
  }
  return bytes;
}

function estimateAggregateStateBytes(state: VectorAggregateState): number {
  switch (state.op) {
    case "count":
    case "sum":
    case "avg":
    case "var_samp":
    case "var_pop":
    case "stddev_samp":
    case "stddev_pop":
    case "min":
    case "max":
    case "first":
    case "last":
    case "any":
      return 32;
    case "median":
    case "quantile":
      return 32 + state.memoryBytes;
    case "count_distinct":
    case "approx_count_distinct":
      return 32 + state.memoryBytes;
    case "mode":
      return 32 + state.memoryBytes;
  }
}
