import { batchCallExprValues, batchCallPredicateMask } from "./batch-call.js";
import { registerCompactionMetadata } from "./compaction-metadata.js";
import { expandDictionaryMask } from "./dictionary-mask.js";
import { LakeqlError } from "./errors.js";
import type { CompareOp, Expr, Scalar } from "./expr.js";
import { heapSort } from "./heap-sort.js";
import { allocateInt96Storage, readInt96, writeInt96 } from "./int96-storage.js";
import { MATERIALIZATION_CHARGES as c } from "./materialization-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { setRecordValue } from "./record-value.js";
import {
  compareTimestampValues,
  isTimestampValue,
  type TimestampUnit,
  TimestampValue,
  timestampEpochForUnit,
  timestampValueFromIso,
} from "./timestamp.js";
import type { Row } from "./types.js";
import { buildUnionChildren } from "./union-construction.js";
import { admitVectorBuffer } from "./vector-buffer.js";

const UNION_DOMAINS = [
  "null",
  "f64",
  "i64",
  "timestamp",
  "bool",
  "utf8",
  "binary",
  "list",
  "struct",
  "map",
] as const;

const MIN_TIMESTAMP_VECTOR_EPOCH = -(1n << 63n);
const MAX_TIMESTAMP_VECTOR_EPOCH = (1n << 63n) - 1n;

export interface PackedByteVector {
  type: "binary-view" | "utf8-view";
  data: Uint8Array;
  ranges?: Uint32Array;
  fixedLength?: number;
  rowOffset?: number;
  length: number;
  valid?: Uint8Array;
}

// Packed values are individual strings, not streams with an encoding signature.
const packedTextDecoder = new TextDecoder("utf-8", { ignoreBOM: true });

function packedByteValue(vector: PackedByteVector, index: number): Uint8Array {
  const row = (vector.rowOffset ?? 0) + index;
  const start =
    vector.ranges === undefined ? row * (vector.fixedLength ?? 0) : (vector.ranges[row * 2] ?? 0);
  const length =
    vector.ranges === undefined ? (vector.fixedLength ?? 0) : (vector.ranges[row * 2 + 1] ?? 0);
  return vector.data.subarray(start, start + length);
}

export type Vector =
  | { type: "union"; tags: Uint8Array; offsets: Uint32Array; children: readonly Vector[] }
  | PackedByteVector
  | { type: "null"; length: number }
  | { type: "f32"; values: Float32Array; valid?: Uint8Array }
  | { type: "f64"; values: Float64Array; valid?: Uint8Array }
  | { type: "i32"; values: Int32Array; valid?: Uint8Array }
  | { type: "u32"; values: Uint32Array; valid?: Uint8Array }
  | { type: "u8"; values: Uint8Array; valid?: Uint8Array }
  | { type: "i64"; values: BigInt64Array; valid?: Uint8Array }
  | { type: "u64"; values: BigUint64Array; valid?: Uint8Array }
  | {
      type: "timestamp";
      values: BigInt64Array;
      unit: TimestampUnit;
      isAdjustedToUTC: boolean;
      valid?: Uint8Array;
    }
  | {
      type: "timestamp96";
      low: BigUint64Array;
      high: Int32Array;
      unit: TimestampUnit;
      isAdjustedToUTC: boolean;
      valid?: Uint8Array;
    }
  | { type: "bool"; values: Uint8Array; valid?: Uint8Array }
  | { type: "utf8"; values: string[]; valid?: Uint8Array }
  | { type: "binary"; values: Uint8Array[]; valid?: Uint8Array }
  | { type: "dict"; indices: Uint32Array; dictionary: Vector; valid?: Uint8Array }
  | { type: "list"; offsets: Int32Array; child: Vector; valid?: Uint8Array }
  | { type: "struct"; fields: Record<string, Vector>; length: number; valid?: Uint8Array }
  | { type: "map"; offsets: Int32Array; keys: Vector; values: Vector; valid?: Uint8Array };

export interface Batch {
  rowCount: number;
  columns: Record<string, Vector>;
}

export type Selection = Uint8Array;

type VectorValue = unknown;
type VectorShape = Exclude<
  Vector["type"],
  "timestamp96" | "dict" | "f32" | "i32" | "u32" | "u8" | "binary-view" | "utf8-view"
>;

export function batchFromColumns(
  columns: Record<string, ArrayLike<VectorValue>>,
  scope?: MemoryScope,
): Batch {
  let rowCount: number | undefined;
  const vectors: Record<string, Vector> = Object.create(null);
  for (const [name, values] of Object.entries(columns)) {
    if (rowCount === undefined) rowCount = values.length;
    else if (values.length !== rowCount) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Column vectors must have equal length", {
        column: name,
        expectedRows: rowCount,
        actualRows: values.length,
      });
    }
    vectors[name] = vectorFromValues(values, scope);
  }
  return { rowCount: rowCount ?? 0, columns: vectors };
}

export function batchFromVectors(columns: Record<string, Vector>): Batch {
  let rowCount: number | undefined;
  for (const [name, vector] of Object.entries(columns)) {
    const length = vectorLength(vector);
    if (rowCount === undefined) rowCount = length;
    else if (length !== rowCount) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Column vectors must have equal length", {
        column: name,
        expectedRows: rowCount,
        actualRows: length,
      });
    }
  }
  return { rowCount: rowCount ?? 0, columns };
}

export function vectorFromValues(values: ArrayLike<VectorValue>, scope?: MemoryScope): Vector {
  return vectorFromSource({ length: values.length, get: (index) => values[index] }, scope);
}

/** Indexed values must remain stable across inference and construction passes. */
export interface VectorSource {
  readonly length: number;
  get(index: number): VectorValue;
}

export function vectorFromSource(values: VectorSource, scope?: MemoryScope): Vector {
  if (!Number.isSafeInteger(values.length) || values.length < 0) {
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Vector source length must be a non-negative safe integer",
    );
  }
  const { type, valid, strings } = vectorShape(values, scope);
  switch (type) {
    case "union": {
      const ownedScope =
        scope?.childScope() ?? new MemoryScope(new MemoryBudget(Number.MAX_SAFE_INTEGER));
      try {
        const built = buildUnionChildren(
          values,
          UNION_DOMAINS.length,
          (row) => {
            const value = values.get(row);
            return UNION_DOMAINS.indexOf(
              (value == null
                ? "null"
                : vectorShapeForValue(value)) as (typeof UNION_DOMAINS)[number],
            );
          },
          ownedScope,
        );
        const output = ownedScope.allocate(
          c.object + 4 * c.property,
          "union vector descriptor",
          () => ({
            type: "union" as const,
            tags: built.routing.tags,
            offsets: built.routing.offsets,
            children: built.children,
          }),
        ).value;
        registerCompactionMetadata(output, "vector");
        if (scope !== undefined) scope.adopt(ownedScope.retainAllocations(undefined));
        return output;
      } finally {
        ownedScope.dispose();
      }
    }
    case "null":
      return { type, length: values.length };
    case "f64":
      return optionalValidity({ type, values: f64Values(values, scope) }, valid);
    case "i64":
      return optionalValidity({ type, values: i64Values(values, scope) }, valid);
    case "u64":
      return optionalValidity({ type, values: u64Values(values, scope) }, valid);
    case "timestamp": {
      const timestamp = timestampValues(values, scope);
      return optionalValidity(timestamp, valid);
    }
    case "bool":
      return optionalValidity({ type, values: boolValues(values, scope) }, valid);
    case "utf8":
      return optionalValidity({ type, values: strings ?? [] }, valid);
    case "binary":
      return optionalValidity(
        scope === undefined
          ? { type, values: binaryValues(values) }
          : packedBinaryValues(values, scope),
        valid,
      );
    case "list":
      return optionalValidity(
        constructNestedVector(scope, 3, (nested) => listValues(values, nested)),
        valid,
      );
    case "struct":
      return optionalValidity(structValues(values, scope), valid);
    case "map":
      return optionalValidity(
        constructNestedVector(scope, 4, (nested) => mapValues(values, nested)),
        valid,
      );
  }
}

export function materializeBatchRows(batch: Batch): Row[] {
  return materializeSelectedBatchRows(batch);
}

export function materializeBatchRow(batch: Batch, index: number): Row {
  if (index < 0 || index >= batch.rowCount) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Batch row index is out of bounds", {
      index,
      rowCount: batch.rowCount,
    });
  }
  const row: Row = {};
  for (const [name, vector] of Object.entries(batch.columns))
    setRecordValue(row, name, vectorValue(vector, index));
  return row;
}

export function materializeSelectedBatchRows(batch: Batch, selection?: Selection): Row[] {
  const rows: Row[] = [];
  const columns = Object.entries(batch.columns);
  for (let index = 0; index < batch.rowCount; index += 1) {
    if (selection !== undefined && selection[index] !== 1) continue;
    const row: Row = {};
    for (const [name, vector] of columns) {
      setRecordValue(row, name, vectorValue(vector, index));
    }
    rows.push(row);
  }
  return rows;
}

export function selectedRowCount(rowCount: number, selection?: Selection): number {
  if (selection === undefined) return rowCount;
  let count = 0;
  for (let index = 0; index < rowCount; index += 1) {
    if (selection[index] === 1) count += 1;
  }
  return count;
}

export function selectedRowIndices(rowCount: number, selection?: Selection): Iterable<number> {
  if (selection === undefined) return allRowIndices(rowCount);
  return selectedSelectionRowIndices(rowCount, selection);
}

/** Materialize selection indices directly into admitted u32 backing. */
export function selectedRowIndexBuffer(
  rowCount: number,
  selection?: Selection,
  scope?: MemoryScope,
): Uint32Array {
  if (!Number.isSafeInteger(rowCount) || rowCount < 0 || rowCount > 0x100000000) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Selection row count exceeds u32 index capacity");
  }
  if (selection !== undefined && selection.length !== rowCount) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Selection length does not match row count");
  }
  const count = selectedRowCount(rowCount, selection);
  const indices = admitVectorBuffer(scope, count * 4, () => new Uint32Array(count));
  let target = 0;
  for (let index = 0; index < rowCount; index += 1) {
    if (selection === undefined || selection[index] === 1) indices[target++] = index;
  }
  return indices;
}

function* selectedSelectionRowIndices(rowCount: number, selection: Selection): Iterable<number> {
  for (let index = 0; index < rowCount; index += 1) {
    if (selection[index] === 1) yield index;
  }
}

function* allRowIndices(rowCount: number): Iterable<number> {
  for (let index = 0; index < rowCount; index += 1) yield index;
}

export function predicateSelection(
  batch: Batch,
  expr: Expr | undefined,
  scope?: MemoryScope,
): Selection {
  if (scope === undefined) {
    return expr === undefined ? allSelected(batch.rowCount) : predicateMask(batch, expr);
  }
  const evaluation = new MemoryScope(scope.budget, [scope.signal]);
  try {
    const mask =
      expr === undefined
        ? allSelected(batch.rowCount, evaluation)
        : predicateMask(batch, expr, evaluation);
    return scope.adopt(evaluation.retainAllocation(mask)).value;
  } finally {
    evaluation.dispose();
  }
}

export function tryPredicateSelection(
  batch: Batch,
  expr: Expr | undefined,
  scope?: MemoryScope,
): Selection | undefined {
  try {
    return predicateSelection(batch, expr, scope);
  } catch (error) {
    if (
      error instanceof LakeqlError &&
      (error.code === "LAKEQL_UNSUPPORTED_PUSHDOWN" || error.code === "LAKEQL_UNKNOWN_COLUMN")
    ) {
      return undefined;
    }
    throw error;
  }
}

export function vectorValue(
  vector: Vector,
  index: number,
):
  | string
  | number
  | bigint
  | boolean
  | Uint8Array
  | TimestampValue
  | unknown[]
  | Record<string, unknown>
  | null {
  if (index < 0 || index >= vectorLength(vector)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Vector index is out of bounds", {
      index,
      length: vectorLength(vector),
    });
  }
  if ("valid" in vector && vector.valid !== undefined && vector.valid[index] === 0) return null;
  switch (vector.type) {
    case "union": {
      const child = vector.children[vector.tags[index] ?? -1];
      const offset = vector.offsets[index];
      if (child === undefined || offset === undefined)
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Invalid union row");
      return vectorValue(child, offset);
    }
    case "null":
      return null;
    case "f32":
    case "f64":
    case "i32":
    case "u32":
    case "u8":
      return vector.values[index] ?? 0;
    case "i64":
    case "u64":
      return vector.values[index] ?? 0n;
    case "timestamp96":
      return new TimestampValue(
        timestampNanos(readInt96(vector, index), vector.unit),
        vector.unit,
        vector.isAdjustedToUTC,
      );
    case "timestamp":
      return new TimestampValue(
        timestampNanos(vector.values[index] ?? 0n, vector.unit),
        vector.unit,
        vector.isAdjustedToUTC,
      );
    case "bool":
      return vector.values[index] === 1;
    case "utf8":
      return vector.values[index] ?? "";
    case "binary-view":
      return packedByteValue(vector, index);
    case "utf8-view":
      return packedTextDecoder.decode(packedByteValue(vector, index));
    case "binary":
      return vector.values[index] ?? new Uint8Array();
    case "dict":
      return vectorValue(vector.dictionary, vector.indices[index] ?? 0);
    case "list": {
      const start = vector.offsets[index] ?? 0;
      const end = vector.offsets[index + 1] ?? start;
      const out: unknown[] = [];
      for (let childIndex = start; childIndex < end; childIndex += 1) {
        out.push(vectorValue(vector.child, childIndex));
      }
      return out;
    }
    case "struct": {
      const out: Record<string, unknown> = {};
      for (const name in vector.fields) {
        if (Object.hasOwn(vector.fields, name))
          setRecordValue(out, name, vectorValue(vector.fields[name] as Vector, index));
      }
      return out;
    }
    case "map": {
      const start = vector.offsets[index] ?? 0;
      const end = vector.offsets[index + 1] ?? start;
      const out: Record<string, unknown> = {};
      for (let childIndex = start; childIndex < end; childIndex += 1) {
        const key = vectorValue(vector.keys, childIndex);
        if (key === null) continue;
        setRecordValue(out, String(key), vectorValue(vector.values, childIndex));
      }
      return out;
    }
  }
}

export function vectorLength(vector: Vector): number {
  switch (vector.type) {
    case "union":
      return vector.tags.length;
    case "null":
    case "struct":
    case "binary-view":
    case "utf8-view":
      return vector.length;
    case "list":
    case "map":
      return Math.max(0, vector.offsets.length - 1);
    case "f64":
    case "f32":
    case "i32":
    case "u32":
    case "u8":
    case "i64":
    case "u64":
    case "timestamp":
    case "bool":
    case "utf8":
    case "binary":
      return vector.values.length;
    case "timestamp96":
      return vector.low.length;
    case "dict":
      return vector.indices.length;
  }
}

function vectorShape(
  values: VectorSource,
  scope?: MemoryScope,
): {
  type: VectorShape;
  valid?: Uint8Array;
  strings?: string[];
} {
  const construction = scope?.childScope();
  try {
    const result = inferVectorShape(values, construction);
    if (construction !== undefined && result.type !== "union")
      scope?.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    construction?.dispose();
  }
}

function inferVectorShape(
  values: VectorSource,
  scope?: MemoryScope,
): {
  type: VectorShape;
  valid?: Uint8Array;
  strings?: string[];
} {
  let type: VectorShape | undefined;
  let strings: string[] | undefined;
  let valid: Uint8Array | undefined;
  for (let index = 0; index < values.length; index += 1) {
    const value = values.get(index);
    if (value == null) {
      if (valid === undefined) {
        valid = admitVectorBuffer(scope, values.length, () => new Uint8Array(values.length));
        valid.fill(1, 0, index);
      }
      valid[index] = 0;
      continue;
    }
    if (valid !== undefined) valid[index] = 1;
    const next = vectorShapeForValue(value);
    if (next === "utf8") {
      if (strings === undefined) {
        strings = admitVectorBuffer(scope, c.array + values.length * c.arraySlot, () =>
          new Array<string>(values.length).fill(""),
        );
        registerCompactionMetadata(strings, "slots");
      }
      strings[index] = value as string;
    }
    if (type === undefined) {
      type = next;
      continue;
    }
    if (type !== next) return { type: "union" };
  }
  if (type === "i64") {
    let requiresUnsigned = false;
    let hasNegative = false;
    for (let index = 0; index < values.length; index += 1) {
      const value = values.get(index);
      if (typeof value !== "bigint") continue;
      if (value < 0n) hasNegative = true;
      if (value > 0x7fff_ffff_ffff_ffffn) requiresUnsigned = true;
      if (value < -0x8000_0000_0000_0000n || value > 0xffff_ffff_ffff_ffffn) {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Bigint column value exceeds 64-bit range", {
          index,
          value: String(value),
        });
      }
    }
    if (requiresUnsigned && hasNegative) {
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        "Bigint column cannot mix negative and unsigned 64-bit values",
      );
    }
    if (requiresUnsigned) type = "u64";
  }
  const shape: { type: VectorShape; valid?: Uint8Array; strings?: string[] } = {
    type: type ?? "null",
  };
  if (valid !== undefined) shape.valid = valid;
  if (strings !== undefined) shape.strings = strings;
  return shape;
}

function vectorShapeForValue(value: Exclude<VectorValue, null | undefined>): VectorShape {
  if (Array.isArray(value)) return "list";
  if (value instanceof Map) return "map";
  if (value instanceof Uint8Array) return "binary";
  switch (typeof value) {
    case "number":
      return "f64";
    case "bigint":
      return "i64";
    case "object":
      if (value instanceof Date) return "timestamp";
      if (isTimestampValue(value)) return "timestamp";
      return "struct";
    case "boolean":
      return "bool";
    case "string":
      return "utf8";
    default:
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Unsupported vector value type", {
        type: typeof value,
      });
  }
}

function timestampValues(
  values: VectorSource,
  scope?: MemoryScope,
): Extract<Vector, { type: "timestamp" | "timestamp96" }> {
  let minNanos = 0n,
    maxNanos = 0n;
  let unit: TimestampUnit | undefined;
  let isAdjustedToUTC = true;
  // Resolve the batch representation before allocating its backing. A later value
  // must never lose precision because an earlier row declared a coarser unit.
  for (let index = 0; index < values.length; index += 1) {
    const value = values.get(index);
    if (value == null) continue;
    const timestamp = timestampSourceValue(value);
    if (timestamp.epochNanoseconds < minNanos) minNanos = timestamp.epochNanoseconds;
    if (timestamp.epochNanoseconds > maxNanos) maxNanos = timestamp.epochNanoseconds;
    if (unit !== undefined && isAdjustedToUTC !== timestamp.isAdjustedToUTC)
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Incompatible timestamp UTC semantics", {
        index,
      });
    isAdjustedToUTC = timestamp.isAdjustedToUTC;
    const requiredUnit =
      timestamp.epochNanoseconds % 1_000n !== 0n
        ? "nanos"
        : timestamp.epochNanoseconds % 1_000_000n !== 0n
          ? "micros"
          : timestamp.unit;
    if (unit === "nanos" || timestamp.unit === "nanos" || requiredUnit === "nanos") unit = "nanos";
    else if (unit === "micros" || timestamp.unit === "micros" || requiredUnit === "micros")
      unit = "micros";
    else unit = "millis";
  }
  unit ??= "millis";
  const divisor = unit === "millis" ? 1_000_000n : unit === "micros" ? 1000n : 1n;
  const min = minNanos / divisor,
    max = maxNanos / divisor;
  if (min < -(1n << 95n) || max > (1n << 95n) - 1n)
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Timestamp exceeds signed 96-bit vector capacity", {
      vectorShape: "timestamp96",
      storageBits: 96,
      unit,
    });
  const wide = min < MIN_TIMESTAMP_VECTOR_EPOCH || max > MAX_TIMESTAMP_VECTOR_EPOCH;
  const output: Extract<Vector, { type: "timestamp" | "timestamp96" }> = wide
    ? { type: "timestamp96", ...allocateInt96Storage(values.length, scope), unit, isAdjustedToUTC }
    : {
        type: "timestamp",
        values: admitVectorBuffer(scope, values.length * 8, () => new BigInt64Array(values.length)),
        unit,
        isAdjustedToUTC,
      };
  for (let index = 0; index < values.length; index += 1) {
    const value = values.get(index);
    if (value == null) continue;
    const epoch = timestampEpochForUnit(timestampSourceValue(value), unit);
    if (output.type === "timestamp96") writeInt96(output, index, epoch);
    else output.values[index] = epoch;
  }
  return output;
}

function timestampSourceValue(value: VectorValue): TimestampValue {
  if (value instanceof Date) {
    if (!Number.isFinite(value.getTime()))
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Invalid timestamp date");
    return new TimestampValue(BigInt(value.getTime()) * 1_000_000n, "millis", true);
  }
  if (isTimestampValue(value)) return value;
  throw new LakeqlError("LAKEQL_TYPE_ERROR", "Expected timestamp vector value", {
    type: typeof value,
  });
}

function bigintValue(value: VectorValue): bigint {
  if (value == null) return 0n;
  if (typeof value !== "bigint") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Expected bigint vector value", {
      type: typeof value,
    });
  }
  return value;
}

function f64Values(values: VectorSource, scope?: MemoryScope): Float64Array {
  const out = admitVectorBuffer(scope, values.length * 8, () => new Float64Array(values.length));
  for (let index = 0; index < values.length; index += 1) {
    out[index] = Number(values.get(index) ?? 0);
  }
  return out;
}

function i64Values(values: VectorSource, scope?: MemoryScope): BigInt64Array {
  const out = admitVectorBuffer(scope, values.length * 8, () => new BigInt64Array(values.length));
  for (let index = 0; index < values.length; index += 1) {
    out[index] = bigintValue(values.get(index));
  }
  return out;
}

function u64Values(values: VectorSource, scope?: MemoryScope): BigUint64Array {
  const out = admitVectorBuffer(scope, values.length * 8, () => new BigUint64Array(values.length));
  for (let index = 0; index < values.length; index += 1) {
    const value = bigintValue(values.get(index));
    if (value < 0n || value > 0xffff_ffff_ffff_ffffn) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Expected unsigned 64-bit vector value", {
        index,
        value: String(value),
      });
    }
    out[index] = value;
  }
  return out;
}

function boolValues(values: VectorSource, scope?: MemoryScope): Uint8Array {
  const out = admitVectorBuffer(scope, values.length * 1, () => new Uint8Array(values.length));
  for (let index = 0; index < values.length; index += 1) {
    out[index] = values.get(index) === true ? 1 : 0;
  }
  return out;
}

function binaryValues(values: VectorSource): Uint8Array[] {
  const out = new Array<Uint8Array>(values.length);
  for (let index = 0; index < values.length; index += 1) {
    const value = values.get(index);
    out[index] = value instanceof Uint8Array ? value : new Uint8Array();
  }
  return out;
}

function packedBinaryValues(values: VectorSource, scope: MemoryScope): PackedByteVector {
  let bytes = 0;
  for (let index = 0; index < values.length; index += 1) {
    const value = values.get(index);
    if (value instanceof Uint8Array) bytes += value.byteLength;
    if (!Number.isSafeInteger(bytes) || bytes > 0xffffffff) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Binary payload exceeds Uint32 range capacity");
    }
  }
  const ranges = admitVectorBuffer(
    scope,
    values.length * 8,
    () => new Uint32Array(values.length * 2),
  );
  const data = admitVectorBuffer(scope, bytes, () => new Uint8Array(bytes));
  let offset = 0;
  for (let index = 0; index < values.length; index += 1) {
    const value = values.get(index);
    ranges[index * 2] = offset;
    if (value instanceof Uint8Array) {
      data.set(value, offset);
      ranges[index * 2 + 1] = value.byteLength;
      offset += value.byteLength;
    }
  }
  return { type: "binary-view", data, ranges, length: values.length };
}

/** A nested descriptor and its new backing transfer atomically. */
function constructNestedVector<T extends Vector>(
  scope: MemoryScope | undefined,
  properties: number,
  build: (construction: MemoryScope) => T,
): T {
  const construction =
    scope?.childScope() ?? new MemoryScope(new MemoryBudget(Number.MAX_SAFE_INTEGER));
  try {
    const output = construction.allocate(
      c.object + properties * c.property,
      "nested vector descriptor",
      () => build(construction),
    ).value;
    registerCompactionMetadata(output, "vector");
    scope?.adopt(construction.retainAllocations(undefined));
    return output;
  } finally {
    construction.dispose();
  }
}

function listValues(values: VectorSource, scope?: MemoryScope): Extract<Vector, { type: "list" }> {
  const offsets = admitVectorBuffer(
    scope,
    (values.length + 1) * 4,
    () => new Int32Array(values.length + 1),
  );
  let childCount = 0;
  for (let index = 0; index < values.length; index += 1) {
    const value = values.get(index);
    if (value != null) {
      if (!Array.isArray(value)) {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "List vector values must be arrays", {
          index,
          type: typeof value,
        });
      }
      if (value.length > 0x7fffffff - childCount) {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "List children exceed Int32 offset capacity", {
          children: childCount + value.length,
        });
      }
      childCount += value.length;
    }
    offsets[index + 1] = childCount;
  }
  let row = 0;
  const children: VectorSource = {
    length: childCount,
    get(index) {
      if (index < (offsets[row] ?? 0)) row = 0;
      while (row < values.length && index >= (offsets[row + 1] ?? childCount)) row += 1;
      const list = values.get(row);
      return Array.isArray(list) ? list[index - (offsets[row] ?? 0)] : undefined;
    },
  };
  return { type: "list", offsets, child: vectorFromSource(children, scope) };
}

function structValues(
  values: VectorSource,
  scope?: MemoryScope,
): Extract<Vector, { type: "struct" }> {
  const construction =
    scope?.childScope() ?? new MemoryScope(new MemoryBudget(Number.MAX_SAFE_INTEGER));
  const scratch = construction.childScope();
  try {
    const fieldNames = scratch.allocate(
      c.object,
      "struct field discovery",
      () => new Set<string>(),
    ).value;
    for (let index = 0; index < values.length; index += 1) {
      const value = values.get(index);
      if (value == null) continue;
      if (!isPlainRecord(value)) {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Struct vector values must be records", {
          index,
          type: typeof value,
        });
      }
      for (const field in value) {
        if (!Object.hasOwn(value, field) || fieldNames.has(field)) continue;
        scratch.allocate(c.property, "struct field discovery entry", () => {
          fieldNames.add(field);
        });
      }
    }
    const names = scratch.allocate(
      c.array + fieldNames.size * c.arraySlot,
      "struct field order",
      () => [...fieldNames],
    ).value;
    heapSort(names, (a, b) => (a < b ? -1 : a > b ? 1 : 0));
    const fields = construction.allocate(
      c.object + fieldNames.size * c.property,
      "struct fields",
      () => Object.create(null) as Record<string, Vector>,
    ).value;
    registerCompactionMetadata(fields, "columns");
    for (const field of names) {
      fields[field] = vectorFromSource(
        {
          length: values.length,
          get(index) {
            const value = values.get(index);
            return isPlainRecord(value) && field in value ? value[field] : null;
          },
        },
        construction,
      );
    }
    const output = construction.allocate(
      c.object + 3 * c.property,
      "struct vector descriptor",
      () => ({ type: "struct" as const, fields, length: values.length }),
    ).value;
    registerCompactionMetadata(output, "vector");
    scratch.dispose();
    scope?.adopt(construction.retainAllocations(undefined));
    return output;
  } finally {
    construction.dispose();
  }
}

function mapValues(values: VectorSource, scope?: MemoryScope): Extract<Vector, { type: "map" }> {
  const offsets = admitVectorBuffer(
    scope,
    (values.length + 1) * 4,
    () => new Int32Array(values.length + 1),
  );
  let count = 0;
  for (let index = 0; index < values.length; index += 1) {
    const value = values.get(index);
    if (value != null) {
      if (!(value instanceof Map)) {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Map vector values must be Map instances", {
          index,
          type: typeof value,
        });
      }
      if (value.size > 0x7fffffff - count) {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Map entries exceed Int32 offset capacity");
      }
      count += value.size;
    }
    offsets[index + 1] = count;
  }
  return {
    type: "map",
    offsets,
    keys: vectorFromSource(mapEntrySource(values, count, 0), scope),
    values: vectorFromSource(mapEntrySource(values, count, 1), scope),
  };
}

/** Constant retained workspace; inference and conversion may restart their scan. */
function mapEntrySource(values: VectorSource, length: number, component: 0 | 1): VectorSource {
  let row = 0;
  let position = -1;
  let iterator: Iterator<[unknown, unknown]> | undefined;
  let entry: [unknown, unknown] | undefined;
  return {
    length,
    get(index) {
      if (index < position) {
        row = 0;
        position = -1;
        iterator = undefined;
        entry = undefined;
      }
      while (position < index) {
        if (iterator === undefined) {
          if (row >= values.length) return undefined;
          const value = values.get(row++);
          if (!(value instanceof Map)) continue;
          iterator = value.entries();
        }
        const next = iterator.next();
        if (next.done) {
          iterator = undefined;
          continue;
        }
        entry = next.value;
        position += 1;
      }
      return entry?.[component];
    },
  };
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" && value !== null && !Array.isArray(value) && !(value instanceof Map)
  );
}

function optionalValidity<T extends Vector>(vector: T, valid: Uint8Array | undefined): T {
  if (valid === undefined) return vector;
  return { ...vector, valid };
}

type SqlMaskValue = 0 | 1 | 2;

function allSelected(rowCount: number, scope?: MemoryScope): Selection {
  const selection = admitVectorBuffer(scope, rowCount, () => new Uint8Array(rowCount));
  selection.fill(1);
  return selection;
}

function predicateMask(batch: Batch, expr: Expr, scope?: MemoryScope): Uint8Array {
  switch (expr.kind) {
    case "literal":
    case "column":
    case "arithmetic":
      return scalarToPredicateMask(batchExprValues(batch, expr, scope), scope);
    case "compare":
      return compareMasks(
        expr.op,
        batchExprValues(batch, expr.left, scope),
        batchExprValues(batch, expr.right, scope),
        scope,
      );
    case "in":
      return inMask(
        batchExprValues(batch, expr.target, scope),
        expr.values.map((value) => batchExprValues(batch, value, scope)),
        expr.negated,
        scope,
      );
    case "between": {
      const target = batchExprValues(batch, expr.target, scope);
      return sqlAndMasks(
        compareMasks("gte", target, batchExprValues(batch, expr.low, scope), scope),
        compareMasks("lte", target, batchExprValues(batch, expr.high, scope), scope),
        scope,
      );
    }
    case "null-check":
      if (expr.target.kind === "column") {
        const vector = batch.columns[expr.target.name];
        if (vector === undefined) {
          throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${expr.target.name}`, {
            column: expr.target.name,
          });
        }
        return vectorNullCheckMask(vector, expr.negated, scope);
      }
      return nullCheckMask(batchExprValues(batch, expr.target, scope), expr.negated, scope);
    case "logical": {
      const first = expr.operands[0];
      if (first === undefined) {
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Logical predicate requires operands");
      }
      let mask = predicateMask(batch, first, scope);
      for (let index = 1; index < expr.operands.length; index++) {
        const operand = expr.operands[index];
        if (operand === undefined) continue;
        const right = predicateMask(batch, operand, scope);
        mask = expr.op === "and" ? sqlAndMasks(mask, right, scope) : sqlOrMasks(mask, right, scope);
      }
      return mask;
    }
    case "not":
      return sqlNotMask(predicateMask(batch, expr.operand, scope), scope);
    case "call": {
      const args = expr.args.map((arg) => batchExprValues(batch, arg, scope));
      const mask = batchCallPredicateMask(expr.fn, args, scope);
      if (mask !== undefined) return mask;
      return scalarToPredicateMask(
        batchCallExprValues(batch.rowCount, expr.fn, args, (left, right, valueScope) =>
          compareValue("eq", left, right, valueScope),
        ),
        scope,
      );
    }
    case "case":
      return scalarToPredicateMask(batchExprValues(batch, expr, scope), scope);
    case "like":
      throwUnsupportedVectorPredicate(expr.kind);
  }
}

export interface BatchExprValues {
  rowCount: number;
  /** True only when scalar reads allocate no payload; absent means scratch is required. */
  allocationFree?: boolean;
  vector?: Vector;
  literal?: Scalar;
  /** Optional per-value scratch lifetime, distinct from compiled batch masks. */
  valueAt(index: number, valueScope?: MemoryScope): Scalar;
}

export function batchExprValues(batch: Batch, expr: Expr, scope?: MemoryScope): BatchExprValues {
  switch (expr.kind) {
    case "literal":
      return literalValues(batch.rowCount, expr.value);
    case "column": {
      const vector = batch.columns[expr.name];
      if (vector === undefined) {
        throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${expr.name}`, {
          column: expr.name,
        });
      }
      return {
        rowCount: batch.rowCount,
        vector,
        allocationFree: scalarReadAllocationFree(vector),
        valueAt(index, valueScope) {
          return scalarVectorValue(vector, index, valueScope);
        },
      };
    }
    case "arithmetic": {
      const left = batchExprValues(batch, expr.left, scope);
      const right = batchExprValues(batch, expr.right, scope);
      return {
        rowCount: batch.rowCount,
        valueAt(index, valueScope) {
          return arithmeticValue(
            expr.op,
            left.valueAt(index, valueScope),
            right.valueAt(index, valueScope),
          );
        },
      };
    }
    case "case": {
      const count = expr.whens.length;
      const Constructor = count <= 255 ? Uint8Array : count <= 65535 ? Uint16Array : Uint32Array;
      let choices: Uint8Array | Uint16Array | Uint32Array | undefined;
      const whens = expr.whens.map((branch, branchIndex) => {
        const mask = predicateSelection(batch, branch.when, scope);
        try {
          if (choices === undefined && Constructor === Uint8Array) {
            // The first private truth mask already has the required shape.
            choices = mask;
            for (let index = 0; index < batch.rowCount; index++) {
              choices[index] = mask[index] === 1 ? branchIndex : count;
            }
          } else {
            if (choices === undefined) {
              choices = admitVectorBuffer(
                scope,
                batch.rowCount * Constructor.BYTES_PER_ELEMENT,
                () => new Constructor(batch.rowCount),
              );
              choices.fill(count);
            }
            for (let index = 0; index < batch.rowCount; index++) {
              if (choices[index] === count && mask[index] === 1) choices[index] = branchIndex;
            }
          }
        } finally {
          if (mask !== choices) scope?.releaseAllocation(mask);
        }
        return batchExprValues(batch, branch.value, scope);
      });
      const elseValues =
        expr.else === undefined ? undefined : batchExprValues(batch, expr.else, scope);
      return {
        rowCount: batch.rowCount,
        valueAt(index, valueScope) {
          const branch = whens[choices?.[index] ?? count];
          if (branch !== undefined) return branch.valueAt(index, valueScope);
          return elseValues === undefined ? null : elseValues.valueAt(index, valueScope);
        },
      };
    }
    case "call": {
      const args = expr.args.map((arg) => batchExprValues(batch, arg, scope));
      return batchCallExprValues(batch.rowCount, expr.fn, args, (left, right, valueScope) =>
        compareValue("eq", left, right, valueScope),
      );
    }
    case "compare":
    case "in":
    case "between":
    case "null-check":
    case "logical":
    case "not":
    case "like":
      throwUnsupportedVectorPredicate(expr.kind);
  }
}

export function scalarVectorValue(vector: Vector, index: number, scope?: MemoryScope): Scalar {
  if (index < 0 || index >= vectorLength(vector)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Vector index is out of bounds", {
      index,
      length: vectorLength(vector),
    });
  }
  if ("valid" in vector && vector.valid !== undefined && vector.valid[index] === 0) return null;
  switch (vector.type) {
    case "union": {
      const child = vector.children[vector.tags[index] ?? -1];
      const offset = vector.offsets[index];
      if (child === undefined || offset === undefined)
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Invalid union row");
      return scalarVectorValue(child, offset, scope);
    }
    case "null":
      return null;
    case "f32":
    case "f64":
    case "i32":
    case "u32":
    case "u8":
      return vector.values[index] ?? 0;
    case "i64":
    case "u64":
      return admitVectorBuffer(scope, c.bigint, () => vector.values[index] ?? 0n);
    case "timestamp96":
      return admitVectorBuffer(
        scope,
        c.timestamp + c.bigint + c.object,
        () =>
          new TimestampValue(
            timestampNanos(readInt96(vector, index), vector.unit),
            vector.unit,
            vector.isAdjustedToUTC,
          ),
      );
    case "timestamp":
      return admitVectorBuffer(
        scope,
        c.timestamp + c.bigint,
        () =>
          new TimestampValue(
            timestampNanos(vector.values[index] ?? 0n, vector.unit),
            vector.unit,
            vector.isAdjustedToUTC,
          ),
      );
    case "bool":
      return vector.values[index] === 1;
    case "utf8":
      return vector.values[index] ?? "";
    case "binary-view":
      return admitVectorBuffer(scope, c.view, () => packedByteValue(vector, index));
    case "utf8-view": {
      const row = (vector.rowOffset ?? 0) + index;
      const length =
        vector.ranges === undefined ? (vector.fixedLength ?? 0) : (vector.ranges[row * 2 + 1] ?? 0);
      return admitVectorBuffer(
        scope,
        c.view + c.string + c.stringCodeUnit * Math.max(0, length),
        () => packedTextDecoder.decode(packedByteValue(vector, index)),
      );
    }
    case "binary":
      return vector.values[index] ?? admitVectorBuffer(scope, c.view, () => new Uint8Array());
    case "dict":
      return scalarVectorValue(vector.dictionary, vector.indices[index] ?? 0, scope);
    case "list":
    case "struct":
    case "map":
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Vector expression requires a scalar value", {
        vectorType: vector.type,
      });
  }
}

function scalarReadAllocationFree(vector: Vector): boolean {
  switch (vector.type) {
    case "null":
    case "f32":
    case "f64":
    case "i32":
    case "u32":
    case "u8":
    case "bool":
    case "utf8":
      return true;
    case "dict":
      return scalarReadAllocationFree(vector.dictionary);
    case "union":
      return vector.children.every(scalarReadAllocationFree);
    default:
      return false;
  }
}

function literalValues(rowCount: number, value: Scalar): BatchExprValues {
  return { rowCount, literal: value, allocationFree: true, valueAt: () => value };
}

function scalarToPredicateMask(values: BatchExprValues, scope?: MemoryScope): Uint8Array {
  const mask = admitVectorBuffer(scope, values.rowCount, () => new Uint8Array(values.rowCount));
  for (let index = 0; index < values.rowCount; index += 1) {
    const scratch = values.allocationFree ? undefined : scope?.childScope();
    try {
      const value = values.valueAt(index, scratch);
      if (value === null) mask[index] = 2;
      else if (typeof value === "boolean") mask[index] = value ? 1 : 0;
      else {
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          "Predicate expression must evaluate to boolean",
          {
            value,
          },
        );
      }
    } finally {
      scratch?.dispose();
    }
  }
  return mask;
}

function compareMasks(
  op: CompareOp,
  left: BatchExprValues,
  right: BatchExprValues,
  scope?: MemoryScope,
): Uint8Array {
  const fast = fastCompareMasks(op, left, right, scope);
  if (fast !== undefined) return fast;
  const mask = admitVectorBuffer(scope, left.rowCount, () => new Uint8Array(left.rowCount));
  const allocationFree = left.allocationFree && right.allocationFree;
  for (let index = 0; index < left.rowCount; index += 1) {
    const scratch = allocationFree ? undefined : scope?.childScope();
    try {
      mask[index] = sqlMaskValue(
        compareValue(
          op,
          left.valueAt(index, scratch),
          right.valueAt(index, scratch),
          scratch ?? scope,
        ),
      );
    } finally {
      scratch?.dispose();
    }
  }
  return mask;
}

function fastCompareMasks(
  op: CompareOp,
  left: BatchExprValues,
  right: BatchExprValues,
  scope?: MemoryScope,
): Uint8Array | undefined {
  if (left.vector !== undefined && right.literal !== undefined) {
    return compareVectorLiteralMasks(op, left.vector, right.literal, scope);
  }
  if (left.literal !== undefined && right.vector !== undefined) {
    return compareVectorLiteralMasks(flipCompareOp(op), right.vector, left.literal, scope);
  }
  return undefined;
}

function compareVectorLiteralMasks(
  op: CompareOp,
  vector: Vector,
  literal: Scalar,
  scope?: MemoryScope,
): Uint8Array | undefined {
  if (literal === null) return nullCompareMask(vectorLength(vector), scope);
  if (vector.type === "dict") return compareDictLiteralMasks(op, vector, literal, scope);
  if (isNumberVector(vector) && typeof literal === "number") {
    return compareNumberLiteralMasks(op, vector, literal, scope);
  }
  if ((vector.type === "i64" || vector.type === "u64") && typeof literal === "bigint") {
    return compareBigIntLiteralMasks(op, vector, literal, scope);
  }
  if (
    (vector.type === "i64" || vector.type === "u64") &&
    typeof literal === "number" &&
    Number.isSafeInteger(literal)
  ) {
    return compareBigIntLiteralMasks(op, vector, literal, scope);
  }
  if (vector.type === "timestamp") {
    const parsing = scope?.childScope();
    try {
      const timestamp = timestampLiteral(literal, parsing);
      if (timestamp !== undefined)
        return compareTimestampLiteralMasks(op, vector, timestamp, scope);
    } finally {
      parsing?.dispose();
    }
  }
  return undefined;
}

function compareDictLiteralMasks(
  op: CompareOp,
  vector: Extract<Vector, { type: "dict" }>,
  literal: Scalar,
  scope?: MemoryScope,
): Uint8Array | undefined {
  const dictionaryMask = compareVectorLiteralMasks(op, vector.dictionary, literal, scope);
  if (dictionaryMask === undefined) return undefined;
  return expandDictionaryMask(vector, dictionaryMask, scope);
}

type NumberVector = Extract<Vector, { type: "f32" | "f64" | "i32" | "u32" | "u8" }>;

function isNumberVector(vector: Vector): vector is NumberVector {
  return (
    vector.type === "f32" ||
    vector.type === "f64" ||
    vector.type === "i32" ||
    vector.type === "u32" ||
    vector.type === "u8"
  );
}

function compareNumberLiteralMasks(
  op: CompareOp,
  vector: NumberVector,
  literal: number,
  scope?: MemoryScope,
): Uint8Array {
  const mask = admitVectorBuffer(
    scope,
    vector.values.length,
    () => new Uint8Array(vector.values.length),
  );
  const valid = vector.valid;
  for (let index = 0; index < vector.values.length; index += 1) {
    if (valid !== undefined && valid[index] === 0) {
      mask[index] = 2;
      continue;
    }
    const value = vector.values[index] ?? 0;
    mask[index] = compareNumberMaskValue(op, value, literal);
  }
  return mask;
}

function compareBigIntLiteralMasks(
  op: CompareOp,
  vector: Extract<Vector, { type: "i64" | "u64" }>,
  literal: bigint | number,
  scope?: MemoryScope,
): Uint8Array {
  const mask = admitVectorBuffer(
    scope,
    vector.values.length,
    () => new Uint8Array(vector.values.length),
  );
  // Reading a bigint typed-array element creates a scalar. Only one is live
  // at a time; relational comparison also accepts safe integer number literals.
  const scratch = scope?.reserve(
    vector.values.length === 0 ? 0 : c.bigint,
    "bigint comparison scalar",
  );
  try {
    const valid = vector.valid;
    for (let index = 0; index < vector.values.length; index += 1) {
      if (valid !== undefined && valid[index] === 0) {
        mask[index] = 2;
        continue;
      }
      const value = vector.values[index] ?? 0n;
      mask[index] = compareNumberMaskValue(op, value < literal ? -1 : value > literal ? 1 : 0, 0);
    }
    return mask;
  } finally {
    scratch?.release();
  }
}

function compareTimestampLiteralMasks(
  op: CompareOp,
  vector: Extract<Vector, { type: "timestamp" }>,
  literal: TimestampValue,
  scope?: MemoryScope,
): Uint8Array {
  const mask = admitVectorBuffer(
    scope,
    vector.values.length,
    () => new Uint8Array(vector.values.length),
  );
  if (vector.values.length === 0) return mask;
  const scratch = scope?.reserve(4 * c.bigint, "timestamp comparison workspace");
  try {
    const valid = vector.valid;
    const target = timestampEpochForUnit(literal, vector.unit);
    const remainder = literal.epochNanoseconds - timestampNanos(target, vector.unit);
    // Division truncates toward zero. When stored ticks equal the quotient,
    // the signed remainder determines their exact order relative to the literal.
    const equalTickOrder = remainder > 0n ? -1 : remainder < 0n ? 1 : 0;
    for (let index = 0; index < vector.values.length; index += 1) {
      if (valid !== undefined && valid[index] === 0) {
        mask[index] = 2;
        continue;
      }
      const value = vector.values[index] ?? 0n;
      const order = value < target ? -1 : value > target ? 1 : equalTickOrder;
      mask[index] = compareNumberMaskValue(op, order, 0);
    }
    return mask;
  } finally {
    scratch?.release();
  }
}

function compareNumberMaskValue(op: CompareOp, left: number, right: number): SqlMaskValue {
  switch (op) {
    case "eq":
      return left === right ? 1 : 0;
    case "ne":
      return left !== right ? 1 : 0;
    case "lt":
      return left < right ? 1 : 0;
    case "lte":
      return left <= right ? 1 : 0;
    case "gt":
      return left > right ? 1 : 0;
    case "gte":
      return left >= right ? 1 : 0;
  }
}

function nullCompareMask(rowCount: number, scope?: MemoryScope): Uint8Array {
  const mask = admitVectorBuffer(scope, rowCount, () => new Uint8Array(rowCount));
  mask.fill(2);
  return mask;
}

function flipCompareOp(op: CompareOp): CompareOp {
  switch (op) {
    case "lt":
      return "gt";
    case "lte":
      return "gte";
    case "gt":
      return "lt";
    case "gte":
      return "lte";
    case "eq":
    case "ne":
      return op;
  }
}

function inMask(
  target: BatchExprValues,
  values: BatchExprValues[],
  negated: boolean,
  scope?: MemoryScope,
): Uint8Array {
  const mask = admitVectorBuffer(scope, target.rowCount, () => new Uint8Array(target.rowCount));
  if (numericLiteralInMask(target, values, negated, mask, scope)) return mask;
  const allocationFree = target.allocationFree && values.every((value) => value.allocationFree);
  for (let index = 0; index < target.rowCount; index++) {
    const scratch = allocationFree ? undefined : scope?.childScope();
    try {
      const targetValue = target.valueAt(index, scratch);
      let found = false;
      let sawNull = false;
      let comparisonFailed = false;
      let comparisonError: unknown;
      // Evaluate every candidate even after a match. Defer comparison errors
      // until evaluation completes, preserving the prior eager error ordering.
      // Only the target and one candidate normally need to stay live together.
      for (const candidate of values) {
        const valueScope = candidate.allocationFree ? undefined : scratch?.childScope();
        let keepFailedValue = false;
        try {
          const value = candidate.valueAt(index, valueScope);
          if (targetValue !== null && !found && !comparisonFailed) {
            try {
              const comparison = compareValue(
                "eq",
                targetValue,
                value,
                valueScope ?? scratch ?? scope,
              );
              found = comparison === true;
              sawNull ||= comparison === null;
            } catch (error) {
              comparisonFailed = true;
              comparisonError = error;
              keepFailedValue = true;
            }
          }
        } finally {
          if (!keepFailedValue) valueScope?.dispose();
        }
      }
      if (comparisonFailed) throw comparisonError;
      const result = targetValue === null ? null : found ? true : sawNull ? null : false;
      mask[index] = sqlMaskValue(negated ? sqlNot(result) : result);
    } finally {
      scratch?.dispose();
    }
  }
  return mask;
}

function numericLiteralInMask(
  target: BatchExprValues,
  values: BatchExprValues[],
  negated: boolean,
  mask: Uint8Array,
  scope?: MemoryScope,
): boolean {
  if (target.vector === undefined || !isNumberVector(target.vector)) return false;
  let count = 0;
  let hasNull = false;
  for (const value of values) {
    if (!("literal" in value)) return false;
    if (value.literal === null) hasNull = true;
    else if (typeof value.literal === "number" && Number.isFinite(value.literal)) count++;
    else return false;
  }
  const depth = Math.ceil(Math.log2(count + 1));
  if (count < 2 || target.rowCount * values.length <= 4 * (count + target.rowCount) * depth)
    return false;
  const bytes = count * Float64Array.BYTES_PER_ELEMENT;
  if (scope !== undefined && bytes > scope.budget.availableBytes) return false;
  const sorted = admitVectorBuffer(scope, bytes, () => new Float64Array(count));
  try {
    let offset = 0;
    for (const value of values)
      if (typeof value.literal === "number") sorted[offset++] = value.literal;
    sorted.sort();
    for (let row = 0; row < target.rowCount; row++) {
      const value = target.valueAt(row);
      let result: boolean | null;
      if (value === null) result = null;
      else {
        const number = value as number;
        let lo = 0;
        let hi = sorted.length;
        while (lo < hi) {
          const mid = lo + Math.floor((hi - lo) / 2);
          if ((sorted[mid] ?? 0) < number) lo = mid + 1;
          else hi = mid;
        }
        // Preserve the existing scalar comparator's NaN behavior.
        const found = Number.isNaN(number) || sorted[lo] === number;
        result = found ? true : hasNull ? null : false;
      }
      mask[row] = sqlMaskValue(negated ? sqlNot(result) : result);
    }
    return true;
  } finally {
    scope?.releaseAllocation(sorted);
  }
}

function nullCheckMask(values: BatchExprValues, negated: boolean, scope?: MemoryScope): Uint8Array {
  const mask = admitVectorBuffer(scope, values.rowCount, () => new Uint8Array(values.rowCount));
  for (let index = 0; index < values.rowCount; index += 1) {
    const scratch = values.allocationFree ? undefined : scope?.childScope();
    try {
      const result = values.valueAt(index, scratch) === null;
      mask[index] = (negated ? !result : result) ? 1 : 0;
    } finally {
      scratch?.dispose();
    }
  }
  return mask;
}

function vectorNullCheckMask(vector: Vector, negated: boolean, scope?: MemoryScope): Uint8Array {
  const rowCount = vectorLength(vector);
  const mask = admitVectorBuffer(scope, rowCount, () => new Uint8Array(rowCount));
  for (let index = 0; index < rowCount; index += 1) {
    const isNull = vectorValue(vector, index) === null;
    mask[index] = (negated ? !isNull : isNull) ? 1 : 0;
  }
  return mask;
}

// Predicate masks are private evaluation outputs, never borrowed column buffers.
// Consume the right workspace and reuse the left backing for the parent result.
function sqlAndMasks(left: Uint8Array, right: Uint8Array, scope?: MemoryScope): Uint8Array {
  try {
    for (let index = 0; index < left.length; index++) {
      left[index] = sqlMaskValue(sqlAnd(maskValue(left[index]), maskValue(right[index])));
    }
    return left;
  } finally {
    scope?.releaseAllocation(right);
  }
}

function sqlOrMasks(left: Uint8Array, right: Uint8Array, scope?: MemoryScope): Uint8Array {
  try {
    for (let index = 0; index < left.length; index++) {
      left[index] = sqlMaskValue(sqlOr(maskValue(left[index]), maskValue(right[index])));
    }
    return left;
  } finally {
    scope?.releaseAllocation(right);
  }
}

function sqlNotMask(input: Uint8Array, _scope?: MemoryScope): Uint8Array {
  for (let index = 0; index < input.length; index++) {
    input[index] = sqlMaskValue(sqlNot(maskValue(input[index])));
  }
  return input;
}

function compareValue(
  op: CompareOp,
  left: Scalar,
  right: Scalar,
  scope?: MemoryScope,
): boolean | null {
  if (left === null || right === null) return null;
  if (left instanceof Uint8Array || right instanceof Uint8Array) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Cannot compare binary values", {
      leftType: scalarType(left),
      rightType: scalarType(right),
    });
  }
  if (isTimestampValue(left) || isTimestampValue(right)) {
    const parsing = scope?.childScope();
    try {
      const leftTimestamp = timestampLiteral(left, parsing);
      const rightTimestamp = timestampLiteral(right, parsing);
      if (leftTimestamp === undefined || rightTimestamp === undefined) {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Cannot compare timestamp with non-timestamp", {
          leftType: scalarType(left),
          rightType: scalarType(right),
        });
      }
      return compareOrder(op, compareTimestampValues(leftTimestamp, rightTimestamp));
    } finally {
      parsing?.dispose();
    }
  }

  if (typeof left !== typeof right && !(isNumberLike(left) && isNumberLike(right))) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Cannot compare values of different types", {
      leftType: typeof left,
      rightType: typeof right,
    });
  }
  const order = left < right ? -1 : left > right ? 1 : 0;
  return compareOrder(op, order);
}

function compareOrder(op: CompareOp, order: number): boolean {
  switch (op) {
    case "eq":
      return order === 0;
    case "ne":
      return order !== 0;
    case "lt":
      return order < 0;
    case "lte":
      return order <= 0;
    case "gt":
      return order > 0;
    case "gte":
      return order >= 0;
  }
}

function arithmeticValue(
  op: "add" | "sub" | "mul" | "div" | "mod",
  left: Scalar,
  right: Scalar,
): Scalar {
  if (left === null || right === null) return null;
  if (typeof left !== "number" || typeof right !== "number") {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Arithmetic expressions require numeric values", {
      leftType: typeof left,
      rightType: typeof right,
    });
  }
  switch (op) {
    case "add":
      return left + right;
    case "sub":
      return left - right;
    case "mul":
      return left * right;
    case "div":
      return left / right;
    case "mod":
      return left % right;
  }
}

function sqlAnd(left: boolean | null, right: boolean | null): boolean | null {
  if (left === false || right === false) return false;
  if (left === null || right === null) return null;
  return true;
}

function sqlOr(left: boolean | null, right: boolean | null): boolean | null {
  if (left === true || right === true) return true;
  if (left === null || right === null) return null;
  return false;
}

function sqlNot(value: boolean | null): boolean | null {
  return value === null ? null : !value;
}

function sqlMaskValue(value: boolean | null): SqlMaskValue {
  if (value === true) return 1;
  if (value === false) return 0;
  return 2;
}

function maskValue(value: number | undefined): boolean | null {
  if (value === 1) return true;
  if (value === 0) return false;
  return null;
}

function isNumberLike(value: Scalar): boolean {
  return typeof value === "number" || typeof value === "bigint";
}

function timestampLiteral(value: Scalar, scope?: MemoryScope): TimestampValue | undefined {
  if (isTimestampValue(value)) return value;
  if (typeof value === "string") return timestampValueFromIso(value, scope);
  return undefined;
}

function timestampNanos(value: bigint, unit: TimestampUnit): bigint {
  switch (unit) {
    case "millis":
      return value * 1_000_000n;
    case "micros":
      return value * 1_000n;
    case "nanos":
      return value;
  }
}

function scalarType(value: Scalar): string {
  if (value instanceof Uint8Array) return "binary";
  return isTimestampValue(value) ? "timestamp" : typeof value;
}

function throwUnsupportedVectorPredicate(kind: Expr["kind"]): never {
  throw new LakeqlError(
    "LAKEQL_UNSUPPORTED_PUSHDOWN",
    `Vector predicate evaluation does not support ${kind} expressions`,
    { kind },
  );
}
