import {
  type Batch,
  type Selection,
  type Vector,
  vectorFromValues,
  vectorLength,
  vectorValue,
} from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { OrderByTerm } from "./query.js";
import { compareTimestampValues, isTimestampValue, type TimestampValue } from "./timestamp.js";

export interface VectorTopKOptions {
  offset?: number;
  limit: number;
}

export function vectorOrderByBatch(
  batch: Batch,
  orderBy: readonly OrderByTerm[],
  selection?: Selection,
): Batch {
  return gatherBatch(batch, vectorSortIndices(batch, orderBy, selection));
}

export function vectorTopKBatch(
  batch: Batch,
  orderBy: readonly OrderByTerm[],
  options: VectorTopKOptions,
  selection?: Selection,
): Batch {
  validateTopKOptions(options);
  const offset = options.offset ?? 0;
  const topK = offset + options.limit;
  if (topK === 0) return gatherBatch(batch, []);
  const indices = vectorTopKIndices(batch, orderBy, topK, selection).sort((left, right) =>
    compareBatchRows(batch, left, right, normalizeVectorOrderBy(orderBy)),
  );
  return gatherBatch(batch, indices.slice(offset, offset + options.limit));
}

export function vectorSortIndices(
  batch: Batch,
  orderBy: readonly OrderByTerm[],
  selection?: Selection,
): number[] {
  const normalized = normalizeVectorOrderBy(orderBy);
  validateOrderColumns(batch, normalized);
  const indices = selectedIndices(batch.rowCount, selection);
  indices.sort((left, right) => compareBatchRows(batch, left, right, normalized));
  return indices;
}

export function vectorTopKIndices(
  batch: Batch,
  orderBy: readonly OrderByTerm[],
  topK: number,
  selection?: Selection,
): number[] {
  if (!Number.isInteger(topK) || topK < 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "topK must be a non-negative integer", { topK });
  }
  const normalized = normalizeVectorOrderBy(orderBy);
  validateOrderColumns(batch, normalized);
  const kept: number[] = [];
  for (const index of selectedIndices(batch.rowCount, selection)) {
    if (topK === 0) break;
    if (kept.length < topK) {
      kept.push(index);
      continue;
    }
    const worstIndex = worstKeptIndex(batch, kept, normalized);
    const worst = kept[worstIndex];
    if (worst !== undefined && compareBatchRows(batch, index, worst, normalized) < 0) {
      kept[worstIndex] = index;
    }
  }
  return kept;
}

export function gatherBatch(batch: Batch, indices: readonly number[]): Batch {
  const columns: Record<string, Vector> = {};
  for (const [name, vector] of Object.entries(batch.columns)) {
    columns[name] = gatherVector(vector, indices);
  }
  return { rowCount: indices.length, columns };
}

export function concatBatches(batches: readonly Batch[]): Batch {
  if (batches.length === 0) return { rowCount: 0, columns: {} };
  const first = batches[0];
  if (first === undefined) return { rowCount: 0, columns: {} };
  const columnNames = Object.keys(first.columns);
  const columns: Record<string, Vector> = {};
  for (const name of columnNames) {
    const vectors = batches.map((batch) => {
      const vector = batch.columns[name];
      if (vector === undefined) {
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          "Cannot concatenate batches with different columns",
          {
            column: name,
          },
        );
      }
      return vector;
    });
    columns[name] = concatVectors(name, vectors);
  }
  for (const batch of batches) {
    for (const name of Object.keys(batch.columns)) {
      if (!(name in first.columns)) {
        throw new LakeqlError(
          "LAKEQL_TYPE_ERROR",
          "Cannot concatenate batches with different columns",
          {
            column: name,
          },
        );
      }
    }
  }
  return {
    rowCount: batches.reduce((sum, batch) => sum + batch.rowCount, 0),
    columns,
  };
}

function normalizeVectorOrderBy(orderBy: readonly OrderByTerm[]): OrderByTerm[] {
  if (!Array.isArray(orderBy) || orderBy.length === 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "orderBy must contain at least one term");
  }
  return orderBy.map((term) => {
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

function validateOrderColumns(batch: Batch, orderBy: readonly OrderByTerm[]): void {
  for (const term of orderBy) {
    if (batch.columns[term.column] === undefined) {
      throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${term.column}`, {
        column: term.column,
      });
    }
  }
}

function compareBatchRows(
  batch: Batch,
  leftIndex: number,
  rightIndex: number,
  orderBy: readonly OrderByTerm[],
): number {
  for (const term of orderBy) {
    const vector = batch.columns[term.column];
    if (vector === undefined) {
      throw new LakeqlError("LAKEQL_UNKNOWN_COLUMN", `Unknown column ${term.column}`, {
        column: term.column,
      });
    }
    const comparison = compareSortValues(
      vectorValue(vector, leftIndex),
      vectorValue(vector, rightIndex),
      term,
    );
    if (comparison !== 0) return comparison;
  }
  return leftIndex - rightIndex;
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
        {
          column: term.column,
        },
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

function selectedIndices(rowCount: number, selection?: Selection): number[] {
  const indices: number[] = [];
  for (let index = 0; index < rowCount; index += 1) {
    if (selection !== undefined && selection[index] !== 1) continue;
    indices.push(index);
  }
  return indices;
}

function worstKeptIndex(
  batch: Batch,
  kept: readonly number[],
  orderBy: readonly OrderByTerm[],
): number {
  let worst = 0;
  for (let index = 1; index < kept.length; index += 1) {
    const candidate = kept[index];
    const currentWorst = kept[worst];
    if (
      candidate !== undefined &&
      currentWorst !== undefined &&
      compareBatchRows(batch, candidate, currentWorst, orderBy) > 0
    ) {
      worst = index;
    }
  }
  return worst;
}

function gatherVector(vector: Vector, indices: readonly number[]): Vector {
  const valid =
    "valid" in vector && vector.valid !== undefined
      ? gatherValid(vector.valid, indices)
      : undefined;
  switch (vector.type) {
    case "null":
      return { type: "null", length: indices.length };
    case "f32":
      return optionalValidity(
        {
          type: vector.type,
          values: Float32Array.from(indices, (index) => vector.values[index] ?? 0),
        },
        valid,
      );
    case "f64":
      return optionalValidity(
        {
          type: vector.type,
          values: Float64Array.from(indices, (index) => vector.values[index] ?? 0),
        },
        valid,
      );
    case "i32":
      return optionalValidity(
        {
          type: vector.type,
          values: Int32Array.from(indices, (index) => vector.values[index] ?? 0),
        },
        valid,
      );
    case "u32":
      return optionalValidity(
        {
          type: vector.type,
          values: Uint32Array.from(indices, (index) => vector.values[index] ?? 0),
        },
        valid,
      );
    case "u8":
      return optionalValidity(
        {
          type: vector.type,
          values: Uint8Array.from(indices, (index) => vector.values[index] ?? 0),
        },
        valid,
      );
    case "i64":
      return optionalValidity(
        {
          type: vector.type,
          values: BigInt64Array.from(indices, (index) => vector.values[index] ?? 0n),
        },
        valid,
      );
    case "timestamp":
      return optionalValidity(
        {
          type: vector.type,
          values: BigInt64Array.from(indices, (index) => vector.values[index] ?? 0n),
          unit: vector.unit,
          isAdjustedToUTC: vector.isAdjustedToUTC,
        },
        valid,
      );
    case "bool":
      return optionalValidity(
        {
          type: vector.type,
          values: Uint8Array.from(indices, (index) => vector.values[index] ?? 0),
        },
        valid,
      );
    case "utf8":
      return optionalValidity(
        { type: vector.type, values: indices.map((index) => vector.values[index] ?? "") },
        valid,
      );
    case "binary":
      return optionalValidity(
        {
          type: vector.type,
          values: indices.map((index) => vector.values[index] ?? new Uint8Array()),
        },
        valid,
      );
    case "dict":
      return optionalValidity(
        {
          type: "dict",
          indices: Uint32Array.from(indices, (index) => vector.indices[index] ?? 0),
          dictionary: vector.dictionary,
        },
        valid,
      );
    case "list":
    case "struct":
    case "map":
      return vectorFromValues(indices.map((index) => vectorValue(vector, index)));
  }
}

function concatVectors(name: string, vectors: readonly Vector[]): Vector {
  const first = vectors[0];
  if (first === undefined) throw new LakeqlError("LAKEQL_TYPE_ERROR", "No vectors to concatenate");
  for (const vector of vectors) {
    if (vector.type !== first.type) {
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        "Cannot concatenate vectors with different types",
        {
          column: name,
          expected: first.type,
          actual: vector.type,
        },
      );
    }
  }
  const valid = concatValid(vectors);
  switch (first.type) {
    case "null":
      return {
        type: "null",
        length: vectors.reduce((sum, vector) => sum + vectorLength(vector), 0),
      };
    case "f32":
      return optionalValidity(
        {
          type: first.type,
          values: concatTypedArrays(
            vectors.map((vector) => requireVectorType(vector, "f32").values),
          ),
        },
        valid,
      );
    case "f64":
      return optionalValidity(
        {
          type: first.type,
          values: concatTypedArrays(
            vectors.map((vector) => requireVectorType(vector, "f64").values),
          ),
        },
        valid,
      );
    case "i32":
      return optionalValidity(
        {
          type: first.type,
          values: concatTypedArrays(
            vectors.map((vector) => requireVectorType(vector, "i32").values),
          ),
        },
        valid,
      );
    case "u32":
      return optionalValidity(
        {
          type: first.type,
          values: concatTypedArrays(
            vectors.map((vector) => requireVectorType(vector, "u32").values),
          ),
        },
        valid,
      );
    case "u8":
      return optionalValidity(
        {
          type: first.type,
          values: concatTypedArrays(
            vectors.map((vector) => requireVectorType(vector, "u8").values),
          ),
        },
        valid,
      );
    case "i64":
      return optionalValidity(
        {
          type: first.type,
          values: concatBigIntArrays(
            vectors.map((vector) => requireVectorType(vector, "i64").values),
          ),
        },
        valid,
      );
    case "timestamp": {
      const timestampVectors = vectors.map((vector) => requireVectorType(vector, "timestamp"));
      if (
        timestampVectors.some(
          (vector) =>
            vector.unit !== first.unit || vector.isAdjustedToUTC !== first.isAdjustedToUTC,
        )
      ) {
        return vectorFromValues(
          vectors.flatMap((vector) =>
            Array.from({ length: vectorLength(vector) }, (_, index) => vectorValue(vector, index)),
          ),
        );
      }
      return optionalValidity(
        {
          type: first.type,
          values: concatBigIntArrays(timestampVectors.map((vector) => vector.values)),
          unit: first.unit,
          isAdjustedToUTC: first.isAdjustedToUTC,
        },
        valid,
      );
    }
    case "bool":
      return optionalValidity(
        {
          type: first.type,
          values: concatTypedArrays(
            vectors.map((vector) => requireVectorType(vector, "bool").values),
          ),
        },
        valid,
      );
    case "utf8":
      return optionalValidity(
        {
          type: first.type,
          values: vectors.flatMap((vector) => [...requireVectorType(vector, "utf8").values]),
        },
        valid,
      );
    case "binary":
      return optionalValidity(
        {
          type: first.type,
          values: vectors.flatMap((vector) => [...requireVectorType(vector, "binary").values]),
        },
        valid,
      );
    case "dict":
      return concatDictVectors(
        vectors.map((vector) => requireVectorType(vector, "dict")),
        valid,
      );
    case "list":
    case "struct":
    case "map":
      return vectorFromValues(
        vectors.flatMap((vector) =>
          Array.from({ length: vectorLength(vector) }, (_, index) => vectorValue(vector, index)),
        ),
      );
  }
}

function concatDictVectors(
  vectors: readonly Extract<Vector, { type: "dict" }>[],
  valid: Uint8Array | undefined,
): Vector {
  const first = vectors[0];
  if (first === undefined) throw new LakeqlError("LAKEQL_TYPE_ERROR", "No vectors to concatenate");
  if (vectors.every((vector) => vector.dictionary === first.dictionary)) {
    return optionalValidity(
      {
        type: "dict",
        indices: concatTypedArrays(vectors.map((vector) => vector.indices)),
        dictionary: first.dictionary,
      },
      valid,
    );
  }
  return vectorFromValues(
    vectors.flatMap((vector) =>
      Array.from({ length: vectorLength(vector) }, (_, index) => vectorValue(vector, index)),
    ),
  );
}

function requireVectorType<T extends Vector["type"]>(
  vector: Vector,
  type: T,
): Extract<Vector, { type: T }> {
  if (vector.type !== type) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Unexpected vector type", {
      expected: type,
      actual: vector.type,
    });
  }
  return vector as Extract<Vector, { type: T }>;
}

function concatTypedArrays<
  T extends Float32Array | Float64Array | Int32Array | Uint8Array | Uint32Array,
>(arrays: readonly T[]): T {
  const length = arrays.reduce((sum, array) => sum + array.length, 0);
  const first = arrays[0];
  if (first === undefined) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "No arrays to concatenate");
  }
  const ArrayConstructor = first.constructor as { new (length: number): T };
  const out = new ArrayConstructor(length);
  let offset = 0;
  for (const array of arrays) {
    out.set(array, offset);
    offset += array.length;
  }
  return out;
}

function concatBigIntArrays(arrays: readonly BigInt64Array[]): BigInt64Array {
  const length = arrays.reduce((sum, array) => sum + array.length, 0);
  const out = new BigInt64Array(length);
  let offset = 0;
  for (const array of arrays) {
    out.set(array, offset);
    offset += array.length;
  }
  return out;
}

function concatValid(vectors: readonly Vector[]): Uint8Array | undefined {
  const hasNull = vectors.some((vector) => "valid" in vector && vector.valid !== undefined);
  if (!hasNull) return undefined;
  const out = new Uint8Array(vectors.reduce((sum, vector) => sum + vectorLength(vector), 0));
  let offset = 0;
  for (const vector of vectors) {
    const length = vectorLength(vector);
    if (!("valid" in vector) || vector.valid === undefined) out.fill(1, offset, offset + length);
    else out.set(vector.valid, offset);
    offset += length;
  }
  return out;
}

function gatherValid(valid: Uint8Array, indices: readonly number[]): Uint8Array | undefined {
  let hasNull = false;
  const out = new Uint8Array(indices.length);
  for (let index = 0; index < indices.length; index += 1) {
    const present = valid[indices[index] ?? 0] === 1;
    if (!present) hasNull = true;
    out[index] = present ? 1 : 0;
  }
  return hasNull ? out : undefined;
}

function optionalValidity<T extends Vector>(vector: T, valid: Uint8Array | undefined): T {
  if (valid === undefined) return vector;
  return { ...vector, valid };
}

function validateTopKOptions(options: VectorTopKOptions): void {
  if (!Number.isInteger(options.limit) || options.limit < 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "limit must be a non-negative integer", {
      limit: options.limit,
    });
  }
  if (options.offset !== undefined && (!Number.isInteger(options.offset) || options.offset < 0)) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "offset must be a non-negative integer", {
      offset: options.offset,
    });
  }
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
