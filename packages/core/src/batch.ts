import { batchCallExprValues, batchCallPredicateMask } from "./batch-call.js";
import { LakeqlError } from "./errors.js";
import type { CompareOp, Expr, Scalar } from "./expr.js";
import type { Row } from "./types.js";

export type Vector =
  | { type: "null"; length: number }
  | { type: "f64"; values: Float64Array; valid?: Uint8Array }
  | { type: "i64"; values: BigInt64Array; valid?: Uint8Array }
  | { type: "bool"; values: Uint8Array; valid?: Uint8Array }
  | { type: "utf8"; values: string[]; valid?: Uint8Array }
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
type VectorShape = Exclude<Vector["type"], "dict">;

export function batchFromColumns(columns: Record<string, ArrayLike<VectorValue>>): Batch {
  let rowCount: number | undefined;
  const vectors: Record<string, Vector> = {};
  for (const [name, values] of Object.entries(columns)) {
    if (rowCount === undefined) rowCount = values.length;
    else if (values.length !== rowCount) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Column vectors must have equal length", {
        column: name,
        expectedRows: rowCount,
        actualRows: values.length,
      });
    }
    vectors[name] = vectorFromValues(values);
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

export function vectorFromValues(values: ArrayLike<VectorValue>): Vector {
  const { type, valid } = vectorShape(values);
  switch (type) {
    case "null":
      return { type, length: values.length };
    case "f64":
      return optionalValidity({ type, values: f64Values(values) }, valid);
    case "i64":
      return optionalValidity({ type, values: i64Values(values) }, valid);
    case "bool":
      return optionalValidity({ type, values: boolValues(values) }, valid);
    case "utf8":
      return optionalValidity({ type, values: utf8Values(values) }, valid);
    case "list":
      return optionalValidity(listValues(values), valid);
    case "struct":
      return optionalValidity(structValues(values), valid);
    case "map":
      return optionalValidity(mapValues(values), valid);
  }
}

export function materializeBatchRows(batch: Batch): Row[] {
  return materializeSelectedBatchRows(batch);
}

export function materializeSelectedBatchRows(batch: Batch, selection?: Selection): Row[] {
  const rows: Row[] = [];
  const columns = Object.entries(batch.columns);
  for (let index = 0; index < batch.rowCount; index += 1) {
    if (selection !== undefined && selection[index] !== 1) continue;
    const row: Row = {};
    for (const [name, vector] of columns) {
      row[name] = vectorValue(vector, index);
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

function* selectedSelectionRowIndices(rowCount: number, selection: Selection): Iterable<number> {
  for (let index = 0; index < rowCount; index += 1) {
    if (selection[index] === 1) yield index;
  }
}

function* allRowIndices(rowCount: number): Iterable<number> {
  for (let index = 0; index < rowCount; index += 1) yield index;
}

export function predicateSelection(batch: Batch, expr: Expr | undefined): Selection {
  if (expr === undefined) return allSelected(batch.rowCount);
  return predicateMask(batch, expr);
}

export function tryPredicateSelection(batch: Batch, expr: Expr | undefined): Selection | undefined {
  try {
    return predicateSelection(batch, expr);
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
): string | number | bigint | boolean | unknown[] | Record<string, unknown> | null {
  if (index < 0 || index >= vectorLength(vector)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Vector index is out of bounds", {
      index,
      length: vectorLength(vector),
    });
  }
  if ("valid" in vector && vector.valid !== undefined && vector.valid[index] === 0) return null;
  switch (vector.type) {
    case "null":
      return null;
    case "f64":
      return vector.values[index] ?? 0;
    case "i64":
      return vector.values[index] ?? 0n;
    case "bool":
      return vector.values[index] === 1;
    case "utf8":
      return vector.values[index] ?? "";
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
      for (const [name, field] of Object.entries(vector.fields)) {
        out[name] = vectorValue(field, index);
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
        out[String(key)] = vectorValue(vector.values, childIndex);
      }
      return out;
    }
  }
}

export function vectorLength(vector: Vector): number {
  switch (vector.type) {
    case "null":
    case "struct":
      return vector.length;
    case "list":
    case "map":
      return Math.max(0, vector.offsets.length - 1);
    case "f64":
    case "i64":
    case "bool":
    case "utf8":
      return vector.values.length;
    case "dict":
      return vector.indices.length;
  }
}

function vectorShape(values: ArrayLike<VectorValue>): {
  type: VectorShape;
  valid?: Uint8Array;
} {
  let type: VectorShape | undefined;
  let valid: Uint8Array | undefined;
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    if (value == null) {
      if (valid === undefined) {
        valid = new Uint8Array(values.length);
        valid.fill(1, 0, index);
      }
      valid[index] = 0;
      continue;
    }
    if (valid !== undefined) valid[index] = 1;
    const next = vectorShapeForValue(value);
    if (type === undefined) {
      type = next;
      continue;
    }
    if (type !== next) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Column contains mixed vector types", {
        expected: type,
        actual: next,
        index,
      });
    }
  }
  const shape: { type: VectorShape; valid?: Uint8Array } = { type: type ?? "null" };
  if (valid !== undefined) shape.valid = valid;
  return shape;
}

function vectorShapeForValue(value: Exclude<VectorValue, null | undefined>): VectorShape {
  if (Array.isArray(value)) return "list";
  if (value instanceof Map) return "map";
  switch (typeof value) {
    case "number":
      return "f64";
    case "bigint":
      return "i64";
    case "boolean":
      return "bool";
    case "string":
      return "utf8";
    case "object":
      return "struct";
    default:
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Unsupported vector value type", {
        type: typeof value,
      });
  }
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

function f64Values(values: ArrayLike<VectorValue>): Float64Array {
  const out = new Float64Array(values.length);
  for (let index = 0; index < values.length; index += 1) {
    out[index] = Number(values[index] ?? 0);
  }
  return out;
}

function i64Values(values: ArrayLike<VectorValue>): BigInt64Array {
  const out = new BigInt64Array(values.length);
  for (let index = 0; index < values.length; index += 1) {
    out[index] = bigintValue(values[index]);
  }
  return out;
}

function boolValues(values: ArrayLike<VectorValue>): Uint8Array {
  const out = new Uint8Array(values.length);
  for (let index = 0; index < values.length; index += 1) {
    out[index] = values[index] === true ? 1 : 0;
  }
  return out;
}

function utf8Values(values: ArrayLike<VectorValue>): string[] {
  const out = new Array<string>(values.length);
  for (let index = 0; index < values.length; index += 1) {
    out[index] = values[index] == null ? "" : String(values[index]);
  }
  return out;
}

function listValues(values: ArrayLike<VectorValue>): Extract<Vector, { type: "list" }> {
  const offsets = new Int32Array(values.length + 1);
  const childValues: unknown[] = [];
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    if (value != null) {
      if (!Array.isArray(value)) {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "List vector values must be arrays", {
          index,
          type: typeof value,
        });
      }
      childValues.push(...value);
    }
    offsets[index + 1] = childValues.length;
  }
  return { type: "list", offsets, child: vectorFromValues(childValues) };
}

function structValues(values: ArrayLike<VectorValue>): Extract<Vector, { type: "struct" }> {
  const fieldNames = new Set<string>();
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    if (value == null) continue;
    if (!isPlainRecord(value)) {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Struct vector values must be records", {
        index,
        type: typeof value,
      });
    }
    for (const field of Object.keys(value)) fieldNames.add(field);
  }
  const fields: Record<string, Vector> = {};
  for (const field of [...fieldNames].sort()) {
    const fieldValues = new Array<unknown>(values.length);
    for (let index = 0; index < values.length; index += 1) {
      const value = values[index];
      fieldValues[index] = isPlainRecord(value) && field in value ? value[field] : null;
    }
    fields[field] = vectorFromValues(fieldValues);
  }
  return { type: "struct", fields, length: values.length };
}

function mapValues(values: ArrayLike<VectorValue>): Extract<Vector, { type: "map" }> {
  const offsets = new Int32Array(values.length + 1);
  const keys: unknown[] = [];
  const mapValues: unknown[] = [];
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    if (value != null) {
      if (!(value instanceof Map)) {
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Map vector values must be Map instances", {
          index,
          type: typeof value,
        });
      }
      for (const [key, inner] of value.entries()) {
        keys.push(key);
        mapValues.push(inner);
      }
    }
    offsets[index + 1] = keys.length;
  }
  return {
    type: "map",
    offsets,
    keys: vectorFromValues(keys),
    values: vectorFromValues(mapValues),
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

function allSelected(rowCount: number): Selection {
  const selection = new Uint8Array(rowCount);
  selection.fill(1);
  return selection;
}

function predicateMask(batch: Batch, expr: Expr): Uint8Array {
  switch (expr.kind) {
    case "literal":
    case "column":
    case "arithmetic":
      return scalarToPredicateMask(batchExprValues(batch, expr));
    case "compare":
      return compareMasks(
        expr.op,
        batchExprValues(batch, expr.left),
        batchExprValues(batch, expr.right),
      );
    case "in":
      return inMask(
        batchExprValues(batch, expr.target),
        expr.values.map((value) => batchExprValues(batch, value)),
        expr.negated,
      );
    case "between": {
      const target = batchExprValues(batch, expr.target);
      return sqlAndMasks(
        compareMasks("gte", target, batchExprValues(batch, expr.low)),
        compareMasks("lte", target, batchExprValues(batch, expr.high)),
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
        return vectorNullCheckMask(vector, expr.negated);
      }
      return nullCheckMask(batchExprValues(batch, expr.target), expr.negated);
    case "logical": {
      const masks = expr.operands.map((operand) => predicateMask(batch, operand));
      return expr.op === "and" ? masks.reduce(sqlAndMasks) : masks.reduce(sqlOrMasks);
    }
    case "not":
      return sqlNotMask(predicateMask(batch, expr.operand));
    case "call": {
      const args = expr.args.map((arg) => batchExprValues(batch, arg));
      const mask = batchCallPredicateMask(expr.fn, args);
      if (mask !== undefined) return mask;
      return scalarToPredicateMask(
        batchCallExprValues(batch.rowCount, expr.fn, args, (left, right) =>
          compareValue("eq", left, right),
        ),
      );
    }
    case "case":
      return scalarToPredicateMask(batchExprValues(batch, expr));
    case "like":
      throwUnsupportedVectorPredicate(expr.kind);
  }
}

export interface BatchExprValues {
  rowCount: number;
  vector?: Vector;
  literal?: Scalar;
  valueAt(index: number): Scalar;
}

export function batchExprValues(batch: Batch, expr: Expr): BatchExprValues {
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
        valueAt(index) {
          return scalarVectorValue(vector, index);
        },
      };
    }
    case "arithmetic": {
      const left = batchExprValues(batch, expr.left);
      const right = batchExprValues(batch, expr.right);
      return {
        rowCount: batch.rowCount,
        valueAt(index) {
          return arithmeticValue(expr.op, left.valueAt(index), right.valueAt(index));
        },
      };
    }
    case "case": {
      const whens = expr.whens.map((branch) => ({
        mask: predicateMask(batch, branch.when),
        values: batchExprValues(batch, branch.value),
      }));
      const elseValues = expr.else === undefined ? undefined : batchExprValues(batch, expr.else);
      return {
        rowCount: batch.rowCount,
        valueAt(index) {
          for (const branch of whens) {
            if (branch.mask[index] === 1) return branch.values.valueAt(index);
          }
          return elseValues === undefined ? null : elseValues.valueAt(index);
        },
      };
    }
    case "call": {
      const args = expr.args.map((arg) => batchExprValues(batch, arg));
      return batchCallExprValues(batch.rowCount, expr.fn, args, (left, right) =>
        compareValue("eq", left, right),
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

export function scalarVectorValue(vector: Vector, index: number): Scalar {
  if (index < 0 || index >= vectorLength(vector)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Vector index is out of bounds", {
      index,
      length: vectorLength(vector),
    });
  }
  if ("valid" in vector && vector.valid !== undefined && vector.valid[index] === 0) return null;
  switch (vector.type) {
    case "null":
      return null;
    case "f64":
      return vector.values[index] ?? 0;
    case "i64":
      return vector.values[index] ?? 0n;
    case "bool":
      return vector.values[index] === 1;
    case "utf8":
      return vector.values[index] ?? "";
    case "dict":
      return scalarVectorValue(vector.dictionary, vector.indices[index] ?? 0);
    case "list":
    case "struct":
    case "map":
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Vector expression requires a scalar value", {
        vectorType: vector.type,
      });
  }
}

function literalValues(rowCount: number, value: Scalar): BatchExprValues {
  return { rowCount, literal: value, valueAt: () => value };
}

function scalarToPredicateMask(values: BatchExprValues): Uint8Array {
  const mask = new Uint8Array(values.rowCount);
  for (let index = 0; index < values.rowCount; index += 1) {
    const value = values.valueAt(index);
    if (value === null) mask[index] = 2;
    else if (typeof value === "boolean") mask[index] = value ? 1 : 0;
    else {
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Predicate expression must evaluate to boolean", {
        value,
      });
    }
  }
  return mask;
}

function compareMasks(op: CompareOp, left: BatchExprValues, right: BatchExprValues): Uint8Array {
  const fast = fastCompareMasks(op, left, right);
  if (fast !== undefined) return fast;
  const mask = new Uint8Array(left.rowCount);
  for (let index = 0; index < left.rowCount; index += 1) {
    mask[index] = sqlMaskValue(compareValue(op, left.valueAt(index), right.valueAt(index)));
  }
  return mask;
}

function fastCompareMasks(
  op: CompareOp,
  left: BatchExprValues,
  right: BatchExprValues,
): Uint8Array | undefined {
  if (left.vector !== undefined && right.literal !== undefined) {
    return compareVectorLiteralMasks(op, left.vector, right.literal);
  }
  if (left.literal !== undefined && right.vector !== undefined) {
    return compareVectorLiteralMasks(flipCompareOp(op), right.vector, left.literal);
  }
  return undefined;
}

function compareVectorLiteralMasks(
  op: CompareOp,
  vector: Vector,
  literal: Scalar,
): Uint8Array | undefined {
  if (literal === null) return nullCompareMask(vectorLength(vector));
  if (vector.type === "dict") return compareDictLiteralMasks(op, vector, literal);
  if (vector.type === "f64" && typeof literal === "number") {
    return compareF64LiteralMasks(op, vector, literal);
  }
  if (vector.type === "i64" && typeof literal === "bigint") {
    return compareI64LiteralMasks(op, vector, literal);
  }
  if (vector.type === "i64" && typeof literal === "number" && Number.isSafeInteger(literal)) {
    return compareI64LiteralMasks(op, vector, BigInt(literal));
  }
  return undefined;
}

function compareDictLiteralMasks(
  op: CompareOp,
  vector: Extract<Vector, { type: "dict" }>,
  literal: Scalar,
): Uint8Array | undefined {
  const dictionaryMask = compareVectorLiteralMasks(op, vector.dictionary, literal);
  if (dictionaryMask === undefined) return undefined;
  const mask = new Uint8Array(vector.indices.length);
  const valid = vector.valid;
  for (let index = 0; index < vector.indices.length; index += 1) {
    if (valid !== undefined && valid[index] === 0) {
      mask[index] = 2;
      continue;
    }
    mask[index] = dictionaryMask[vector.indices[index] ?? 0] ?? 2;
  }
  return mask;
}

function compareF64LiteralMasks(
  op: CompareOp,
  vector: Extract<Vector, { type: "f64" }>,
  literal: number,
): Uint8Array {
  const mask = new Uint8Array(vector.values.length);
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

function compareI64LiteralMasks(
  op: CompareOp,
  vector: Extract<Vector, { type: "i64" }>,
  literal: bigint,
): Uint8Array {
  const mask = new Uint8Array(vector.values.length);
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

function nullCompareMask(rowCount: number): Uint8Array {
  const mask = new Uint8Array(rowCount);
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

function inMask(target: BatchExprValues, values: BatchExprValues[], negated: boolean): Uint8Array {
  const mask = new Uint8Array(target.rowCount);
  for (let index = 0; index < target.rowCount; index += 1) {
    const result = inValue(
      target.valueAt(index),
      values.map((value) => value.valueAt(index)),
    );
    mask[index] = sqlMaskValue(negated ? sqlNot(result) : result);
  }
  return mask;
}

function nullCheckMask(values: BatchExprValues, negated: boolean): Uint8Array {
  const mask = new Uint8Array(values.rowCount);
  for (let index = 0; index < values.rowCount; index += 1) {
    const result = values.valueAt(index) === null;
    mask[index] = (negated ? !result : result) ? 1 : 0;
  }
  return mask;
}

function vectorNullCheckMask(vector: Vector, negated: boolean): Uint8Array {
  const rowCount = vectorLength(vector);
  const mask = new Uint8Array(rowCount);
  for (let index = 0; index < rowCount; index += 1) {
    const isNull = vectorValue(vector, index) === null;
    mask[index] = (negated ? !isNull : isNull) ? 1 : 0;
  }
  return mask;
}

function sqlAndMasks(left: Uint8Array, right: Uint8Array): Uint8Array {
  const mask = new Uint8Array(left.length);
  for (let index = 0; index < left.length; index += 1) {
    mask[index] = sqlMaskValue(sqlAnd(maskValue(left[index]), maskValue(right[index])));
  }
  return mask;
}

function sqlOrMasks(left: Uint8Array, right: Uint8Array): Uint8Array {
  const mask = new Uint8Array(left.length);
  for (let index = 0; index < left.length; index += 1) {
    mask[index] = sqlMaskValue(sqlOr(maskValue(left[index]), maskValue(right[index])));
  }
  return mask;
}

function sqlNotMask(input: Uint8Array): Uint8Array {
  const mask = new Uint8Array(input.length);
  for (let index = 0; index < input.length; index += 1) {
    mask[index] = sqlMaskValue(sqlNot(maskValue(input[index])));
  }
  return mask;
}

function compareValue(op: CompareOp, left: Scalar, right: Scalar): boolean | null {
  if (left === null || right === null) return null;
  if (typeof left !== typeof right && !(isNumberLike(left) && isNumberLike(right))) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Cannot compare values of different types", {
      leftType: typeof left,
      rightType: typeof right,
    });
  }
  const order = left < right ? -1 : left > right ? 1 : 0;
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

function inValue(target: Scalar, values: Scalar[]): boolean | null {
  if (target === null) return null;
  let sawNull = false;
  for (const value of values) {
    const comparison = compareValue("eq", target, value);
    if (comparison === true) return true;
    if (comparison === null) sawNull = true;
  }
  return sawNull ? null : false;
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

function throwUnsupportedVectorPredicate(kind: Expr["kind"]): never {
  throw new LakeqlError(
    "LAKEQL_UNSUPPORTED_PUSHDOWN",
    `Vector predicate evaluation does not support ${kind} expressions`,
    { kind },
  );
}
