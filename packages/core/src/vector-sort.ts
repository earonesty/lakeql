import {
  type Batch,
  type Selection,
  selectedRowIndexBuffer,
  type Vector,
  vectorFromValues,
  vectorLength,
  vectorValue,
} from "./batch.js";
import { type CollectionVector, collectionConcatInputs } from "./collection-concat.js";
import { type ConcatColumnPlan, type ConcatPlan, planConcat } from "./concat-plan.js";
import { LakeqlError } from "./errors.js";
import { sharedGatherPlan } from "./gather-plan.js";
import { gatherInt96Storage } from "./int96-storage.js";
import type { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import type { MemoryStageScheduler } from "./memory-stage-scheduler.js";
import { expandNestedDictionary, nestedDictionaryLeaf } from "./nested-dictionary.js";
import {
  compactGatherBytes,
  compactPackedGather,
  compactScalarDictionary,
  concatAdaptivePackedDictionaries,
  concatPackedVectors,
  concatScalarDictionaries,
  packedGatherLeaf,
} from "./packed-gather.js";
import type { OrderByTerm } from "./query.js";
import { setRecordValue } from "./record-value.js";
import { planSelectedConcat } from "./selected-concat.js";
import { structConcatInputs } from "./struct-concat.js";
import { compareTimestampValues, isTimestampValue, type TimestampValue } from "./timestamp.js";
import { compactOwnedUnionBatch, needsUnionCompaction } from "./union-compaction.js";
import { hasUnionLeaf, planUnionConcat } from "./union-concat.js";
import { admitVectorBuffer } from "./vector-buffer.js";
import { selectVector } from "./vector-selection.js";

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
  const indices = vectorTopKIndices(batch, orderBy, topK, selection);
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

/** Own candidate backing and admit a fixed-capacity index heap before selection. */
export function vectorTopKOwnedBatch(
  input: OwnedMemory<Batch>,
  orderBy: readonly OrderByTerm[],
  options: VectorTopKOptions,
  selection?: Selection,
): OwnedMemory<Batch> {
  validateTopKOptions(options);
  const batch = input.value;
  if (!Number.isSafeInteger(batch.rowCount) || batch.rowCount < 0 || batch.rowCount > 0x100000000) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Top-k indices exceed Uint32 capacity");
  }
  const normalized = normalizeVectorOrderBy(orderBy);
  validateOrderColumns(batch, normalized);
  const offset = options.offset ?? 0;
  let selectedRows = batch.rowCount;
  if (selection !== undefined && options.limit > 0) {
    selectedRows = 0;
    for (let row = 0; row < batch.rowCount; row++) {
      if (selection[row] === 1) selectedRows++;
    }
  }
  const capacity = options.limit === 0 ? 0 : Math.min(selectedRows, offset + options.limit);
  const scope = new MemoryScope(input.budget);
  try {
    scope.adopt(input.retain());
    const heap = scope.allocate(
      capacity * 4,
      "top-k candidate indices",
      () => new Uint32Array(capacity),
    ).value;
    const compare = (left: number, right: number) =>
      compareBatchRows(batch, left, right, normalized);
    // Detect monotonic selected input without allocating sort keys. Stop as soon
    // as both directions occur; mixed input uses the bounded heap below.
    let direction = 0;
    let previous = -1;
    if (capacity > 0) {
      for (let row = 0; row < batch.rowCount; row++) {
        if (selection !== undefined && selection[row] !== 1) continue;
        if (previous >= 0) {
          const next = Math.sign(compare(previous, row));
          if (direction !== 0 && next !== direction) {
            direction = 2;
            break;
          }
          direction = next;
        }
        previous = row;
      }
    }
    if (direction !== 2) {
      let ordinal = 0;
      for (let row = 0; row < batch.rowCount && capacity > 0; row++) {
        if (selection !== undefined && selection[row] !== 1) continue;
        if (direction <= 0) {
          heap[ordinal++] = row;
          if (ordinal === capacity) break;
        } else {
          if (ordinal >= selectedRows - capacity) heap[selectedRows - ordinal - 1] = row;
          ordinal++;
        }
      }
      return gatherOwnedBatch(input, heap.subarray(offset, offset + options.limit));
    }
    let size = 0;
    for (let row = 0; capacity > 0 && row < batch.rowCount; row++) {
      if (selection !== undefined && selection[row] !== 1) continue;
      if (size < capacity) {
        let index = size++;
        while (index > 0) {
          const parent = Math.floor((index - 1) / 2);
          if (compare(row, heap[parent] as number) <= 0) break;
          heap[index] = heap[parent] as number;
          index = parent;
        }
        heap[index] = row;
      } else if (compare(row, heap[0] as number) < 0) {
        let index = 0;
        while (index * 2 + 1 < size) {
          const left = index * 2 + 1;
          const right = left + 1;
          const child =
            right < size && compare(heap[right] as number, heap[left] as number) > 0 ? right : left;
          if (compare(row, heap[child] as number) >= 0) break;
          heap[index] = heap[child] as number;
          index = child;
        }
        heap[index] = row;
      }
    }
    const selected = heap.subarray(0, size);
    // In-place heap sort keeps ordering workspace inside the admitted buffer.
    for (let end = size - 1; end > 0; end--) {
      const value = heap[end] as number;
      heap[end] = heap[0] as number;
      let index = 0;
      while (index * 2 + 1 < end) {
        const left = index * 2 + 1;
        const right = left + 1;
        const child =
          right < end && compare(heap[right] as number, heap[left] as number) > 0 ? right : left;
        if (compare(value, heap[child] as number) >= 0) break;
        heap[index] = heap[child] as number;
        index = child;
      }
      heap[index] = value;
    }
    return gatherOwnedBatch(input, selected.subarray(offset, offset + options.limit));
  } finally {
    scope.dispose();
  }
}

/**
 * Returns the `topK` first selected rows in order using O(k) retained row indices. Candidates
 * are periodically sorted and trimmed for O(n log(k + 1)) comparisons. Sort keys are read from
 * the input vectors during comparisons, without a batch-sized cache per order term.
 * Ties break on row index, as in `vectorSortIndices`.
 */
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
  if (topK === 0) return [];
  const compare = (left: number, right: number) => compareBatchRows(batch, left, right, normalized);
  const trimAt = topK * 2 + 64;
  const kept: number[] = [];
  for (let index = 0; index < batch.rowCount; index += 1) {
    if (selection !== undefined && selection[index] !== 1) continue;
    kept.push(index);
    if (kept.length >= trimAt) {
      kept.sort(compare);
      kept.length = topK;
    }
  }
  kept.sort(compare);
  if (kept.length > topK) kept.length = topK;
  return kept;
}

/** Nested and packed outputs alias input backing; their owners must outlive the output. */
export function gatherBatch(
  batch: Batch,
  indices: readonly number[] | Uint32Array,
  scope?: MemoryScope,
): Batch {
  return gatherBatchStorage(batch, indices, scope, false);
}

/** Gather selected rows, releasing index workspace after output construction. */
export function gatherSelectedBatch(
  batch: Batch,
  selection?: Selection,
  scope?: MemoryScope,
): Batch {
  const indices = selectedRowIndexBuffer(batch.rowCount, selection, scope);
  try {
    // gatherBatch copies indices for dictionary views; output never aliases this workspace.
    return gatherBatch(batch, indices, scope);
  } finally {
    scope?.releaseAllocation(indices);
  }
}

function validateGatherIndices(batch: Batch, indices: readonly number[] | Uint32Array): void {
  for (const index of indices) {
    if (!Number.isSafeInteger(index) || index < 0 || index >= batch.rowCount) {
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Gather index is outside the batch", {
        index,
        rowCount: batch.rowCount,
      });
    }
  }
}

function gatherBatchStorage(
  batch: Batch,
  indices: readonly number[] | Uint32Array,
  scope: MemoryScope | undefined,
  compact: boolean,
): Batch {
  validateGatherIndices(batch, indices);
  let sharedIndices: Uint32Array | undefined;
  const selectionIndices = () => {
    if (sharedIndices !== undefined) return sharedIndices;
    if (indices.some((index) => index > 0xffffffff)) {
      throw new LakeqlError(
        "LAKEQL_VALIDATION_ERROR",
        "Gather index exceeds dictionary Uint32 capacity",
      );
    }
    sharedIndices = admitVectorBuffer(scope, indices.length * 4, () => Uint32Array.from(indices));
    return sharedIndices;
  };
  const columns: Record<string, Vector> = {};
  for (const [name, vector] of Object.entries(batch.columns)) {
    if (compact && scope !== undefined && packedGatherLeaf(vector) !== undefined) {
      columns[name] = compactPackedGather(vector, indices, scope);
      continue;
    }
    if (compact && scope !== undefined && vector.type === "dict") {
      columns[name] = compactScalarDictionary(vector, indices, scope);
      continue;
    }
    if (
      vector.type === "union" ||
      vector.type === "list" ||
      vector.type === "map" ||
      vector.type === "struct" ||
      vector.type === "utf8" ||
      vector.type === "binary" ||
      vector.type === "binary-view" ||
      vector.type === "utf8-view"
    ) {
      columns[name] = { type: "dict", indices: selectionIndices(), dictionary: vector };
    } else {
      columns[name] = gatherVector(vector, indices, scope);
    }
  }
  return { rowCount: indices.length, columns };
}

/** Gather into independently owned output, retaining source ownership only for shared values.
 * The caller still owns input and must release it. Cross-job handoff requires a shared pool.
 */
export function gatherOwnedBatch(
  input: OwnedMemory<Batch>,
  indices: readonly number[] | Uint32Array,
  budget: MemoryBudget = input.budget,
): OwnedMemory<Batch> {
  const scope = new MemoryScope(budget);
  try {
    // Keep source storage admitted throughout output construction, including overlap.
    const source = scope.adopt(input.retainIn(budget));
    validateGatherIndices(input.value, indices);
    const compactBytes = compactGatherBytes(input.value, indices);
    const compact =
      compactBytes !== undefined &&
      compactBytes < input.bytes &&
      compactBytes <= budget.availableBytes;
    const plan =
      compact && compactBytes !== undefined
        ? { typedBytes: compactBytes, retainsInput: false, createsJavaScriptArrays: false }
        : sharedGatherPlan(input.value, indices);
    scope.admit(plan.typedBytes, "gather typed output");
    const batch = gatherBatchStorage(input.value, indices, scope, compact);
    if (!plan.retainsInput) source.release();
    return scope.retainAllocations(batch);
  } finally {
    scope.dispose();
  }
}

export function concatBatches(
  batches: readonly Batch[],
  scope?: MemoryScope,
  prepared?: ConcatPlan,
): Batch {
  if (batches.length === 0) return { rowCount: 0, columns: {} };
  const first = batches[0];
  if (first === undefined) return { rowCount: 0, columns: {} };
  const columnNames = Object.keys(first.columns);
  const columns: Record<string, Vector> = {};
  const indexCopies: DictionaryIndexCopies = new Map();
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
    setRecordValue(
      columns,
      name,
      concatVectors(name, vectors, scope, indexCopies, prepared?.columns.get(name)),
    );
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

/** Concatenate with admitted output backing and independent ownership of aliased inputs. */
export function concatOwnedBatches(
  inputs: readonly OwnedMemory<Batch>[],
  budget?: MemoryBudget,
): OwnedMemory<Batch> {
  const target = budget ?? inputs[0]?.budget;
  if (target === undefined) {
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Empty owned concat requires a memory budget");
  }
  const scope = new MemoryScope(target);
  try {
    const sources = inputs.map((input) => scope.adopt(input.retainIn(target)));
    const batches = inputs.map((input) => input.value);
    const plan = planConcat(batches);
    if (plan !== undefined) scope.admit(plan.bytes, "concat typed output");
    const batch = concatBatches(batches, scope, plan);
    releaseDetachedConcatSources(batch, batches, sources);
    const owner = scope.retainAllocations(batch);
    if (!needsUnionCompaction(batch)) return owner;
    try {
      return compactOwnedUnionBatch(owner, target);
    } finally {
      owner.release();
    }
  } finally {
    scope.dispose();
  }
}

/** Schedule only complete physical plans. Inputs remain immutable and pinned from
 * submission through execution; unsupported shapes reject before queueing.
 */
export async function concatScheduledBatches(
  inputs: readonly OwnedMemory<Batch>[],
  scheduler: MemoryStageScheduler,
  budget: MemoryBudget,
  signal?: AbortSignal,
): Promise<OwnedMemory<Batch>> {
  if (signal?.aborted) throw new LakeqlError("LAKEQL_ABORTED", "Concat aborted before planning");
  const batches = inputs.map((input) => input.value);
  const plan = planConcat(batches);
  if (plan === undefined)
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Concat shape has no complete allocation plan");
  return scheduler.run({
    budget,
    bytes: plan.bytes,
    purpose: "planned concat",
    inputs,
    ...(signal === undefined ? {} : { signal }),
    execute(scope, _signal, pins) {
      const output = concatBatches(batches, scope, plan);
      releaseDetachedConcatSources(output, batches, pins);
      return output;
    },
  });
}

const noSharedDictionaries: ReadonlySet<Vector> = new Set();

function releaseDetachedConcatSources(
  output: Batch,
  batches: readonly Batch[],
  sources: readonly OwnedMemory<unknown>[],
): void {
  // Some output shapes always borrow input payload. Their presence settles
  // ownership without discovering every input descriptor (notably append trees).
  for (const name in output.columns)
    if (Object.hasOwn(output.columns, name)) {
      const vector = output.columns[name];
      if (vector !== undefined && concatSharesValues(vector, noSharedDictionaries)) return;
    }
  const sharedDictionaries = new Set<Vector>();
  for (const batch of batches)
    for (const vector of Object.values(batch.columns))
      collectInputVectors(vector, sharedDictionaries);
  if (
    !Object.values(output.columns).some((vector) => concatSharesValues(vector, sharedDictionaries))
  )
    for (const source of sources) source.release();
}

function concatSharesValues(vector: Vector, sharedDictionaries: ReadonlySet<Vector>): boolean {
  switch (vector.type) {
    case "union":
      return true;
    case "dict":
      return (
        sharedDictionaries.has(vector.dictionary) ||
        concatSharesValues(vector.dictionary, sharedDictionaries)
      );
    case "binary":
    case "utf8":
      return true;
    case "list":
      return concatSharesValues(vector.child, sharedDictionaries);
    case "struct":
      return Object.values(vector.fields).some((field) =>
        concatSharesValues(field, sharedDictionaries),
      );
    case "map":
      return (
        concatSharesValues(vector.keys, sharedDictionaries) ||
        concatSharesValues(vector.values, sharedDictionaries)
      );
    default:
      return false;
  }
}

function collectInputVectors(vector: Vector, dictionaries: Set<Vector>): void {
  // Null-shape normalization can wrap a direct source as a dictionary too.
  dictionaries.add(vector);
  switch (vector.type) {
    case "union":
      for (const child of vector.children) collectInputVectors(child, dictionaries);
      break;
    case "dict":
      dictionaries.add(vector.dictionary);
      collectInputVectors(vector.dictionary, dictionaries);
      break;
    case "struct":
      for (const field of Object.values(vector.fields)) collectInputVectors(field, dictionaries);
      break;
    case "list":
      collectInputVectors(vector.child, dictionaries);
      break;
    case "map":
      collectInputVectors(vector.keys, dictionaries);
      collectInputVectors(vector.values, dictionaries);
      break;
  }
}

/** Validates sort terms and removes equivalent terms without changing their precedence. */
function normalizeVectorOrderBy(orderBy: readonly OrderByTerm[]): OrderByTerm[] {
  if (!Array.isArray(orderBy) || orderBy.length === 0) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "orderBy must contain at least one term");
  }
  const normalized: OrderByTerm[] = [];
  const seen = new Set<string>();
  for (const term of orderBy) {
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
    const key = JSON.stringify([term.column, direction, nulls]);
    if (seen.has(key)) continue;
    seen.add(key);
    normalized.push({ column: term.column, direction, nulls });
  }
  return normalized;
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

function gatherVector(
  vector: Vector,
  indices: readonly number[] | Uint32Array,
  scope?: MemoryScope,
): Vector {
  const valid =
    "valid" in vector && vector.valid !== undefined
      ? gatherValid(vector.valid, indices, scope)
      : undefined;
  switch (vector.type) {
    case "union":
      return {
        type: "dict",
        indices: admitVectorBuffer(scope, indices.length * 4, () => Uint32Array.from(indices)),
        dictionary: vector,
      };
    case "null":
      return { type: "null", length: indices.length };
    case "f32":
      return optionalValidity(
        {
          type: vector.type,
          values: admitVectorBuffer(scope, indices.length * 4, () =>
            Float32Array.from(indices, (index) => vector.values[index] ?? 0),
          ),
        },
        valid,
      );
    case "f64":
      return optionalValidity(
        {
          type: vector.type,
          values: admitVectorBuffer(scope, indices.length * 8, () =>
            Float64Array.from(indices, (index) => vector.values[index] ?? 0),
          ),
        },
        valid,
      );
    case "i32":
      return optionalValidity(
        {
          type: vector.type,
          values: admitVectorBuffer(scope, indices.length * 4, () =>
            Int32Array.from(indices, (index) => vector.values[index] ?? 0),
          ),
        },
        valid,
      );
    case "u32":
      return optionalValidity(
        {
          type: vector.type,
          values: admitVectorBuffer(scope, indices.length * 4, () =>
            Uint32Array.from(indices, (index) => vector.values[index] ?? 0),
          ),
        },
        valid,
      );
    case "u8":
      return optionalValidity(
        {
          type: vector.type,
          values: admitVectorBuffer(scope, indices.length * 1, () =>
            Uint8Array.from(indices, (index) => vector.values[index] ?? 0),
          ),
        },
        valid,
      );
    case "i64":
      return optionalValidity(
        {
          type: vector.type,
          values: admitVectorBuffer(scope, indices.length * 8, () =>
            BigInt64Array.from(indices, (index) => vector.values[index] ?? 0n),
          ),
        },
        valid,
      );
    case "u64":
      return optionalValidity(
        {
          type: vector.type,
          values: admitVectorBuffer(scope, indices.length * 8, () =>
            BigUint64Array.from(indices, (index) => vector.values[index] ?? 0n),
          ),
        },
        valid,
      );
    case "timestamp96":
      return optionalValidity(
        {
          type: "timestamp96",
          ...gatherInt96Storage(vector, indices, scope),
          unit: vector.unit,
          isAdjustedToUTC: vector.isAdjustedToUTC,
        },
        valid,
      );
    case "timestamp":
      return optionalValidity(
        {
          type: vector.type,
          values: admitVectorBuffer(scope, indices.length * 8, () =>
            BigInt64Array.from(indices, (index) => vector.values[index] ?? 0n),
          ),
          unit: vector.unit,
          isAdjustedToUTC: vector.isAdjustedToUTC,
        },
        valid,
      );
    case "bool":
      return optionalValidity(
        {
          type: vector.type,
          values: admitVectorBuffer(scope, indices.length * 1, () =>
            Uint8Array.from(indices, (index) => vector.values[index] ?? 0),
          ),
        },
        valid,
      );
    case "utf8":
      return optionalValidity(
        { type: vector.type, values: Array.from(indices, (index) => vector.values[index] ?? "") },
        valid,
      );
    case "binary-view":
    case "utf8-view":
      return {
        type: "dict",
        indices: admitVectorBuffer(scope, indices.length * 4, () => Uint32Array.from(indices)),
        dictionary: vector,
      };
    case "binary":
      return optionalValidity(
        {
          type: vector.type,
          values: Array.from(indices, (index) => vector.values[index] ?? new Uint8Array()),
        },
        valid,
      );
    case "dict":
      return optionalValidity(
        {
          type: "dict",
          indices: admitVectorBuffer(scope, indices.length * 4, () =>
            Uint32Array.from(indices, (index) => vector.indices[index] ?? 0),
          ),
          dictionary: vector.dictionary,
        },
        valid,
      );
    case "struct":
    case "list":
    case "map":
      return vectorFromValues(
        Array.from(indices, (index) => vectorValue(vector, index)),
        scope,
      );
  }
}

type DictionaryIndexCopies = Map<
  Uint32Array,
  { sources: readonly Uint32Array[]; output: Uint32Array }
>;

function concatVectors(
  name: string,
  vectors: readonly Vector[],
  scope: MemoryScope | undefined,
  indexCopies: DictionaryIndexCopies,
  prepared?: ConcatColumnPlan,
): Vector {
  if (prepared?.union !== undefined) return prepared.union.execute(scope);
  if (vectors.some(hasUnionLeaf)) return planUnionConcat(vectors).execute(scope);
  if (prepared?.selected !== undefined) return prepared.selected.execute(scope);
  const first = vectors[0];
  if (first === undefined) throw new LakeqlError("LAKEQL_TYPE_ERROR", "No vectors to concatenate");
  if (
    vectors.some((vector) => vector.type === "dict" && concatLogicalShape(vector) === "null") &&
    vectors.some((vector) => concatLogicalShape(vector) !== "null")
  ) {
    return concatVectors(
      name,
      vectors.map((vector) =>
        concatLogicalShape(vector) === "null"
          ? { type: "null", length: vectorLength(vector) }
          : vector,
      ),
      scope,
      indexCopies,
    );
  }
  if (vectors.some((vector) => concatLogicalShape(vector) === "timestamp96")) {
    const selections = vectors.map(selectVector);
    if (
      selections.every(
        ({ leaf }) =>
          leaf.type === "timestamp" || leaf.type === "timestamp96" || leaf.type === "null",
      )
    ) {
      const plan = planSelectedConcat(selections);
      if (plan !== undefined) return plan.execute(scope);
    }
  }
  const concrete = vectors.find((vector) => vector.type !== "null");
  if (concrete !== undefined && vectors.some((vector) => vector.type === "null")) {
    const temporary: ArrayBufferView[] = [];
    const mask = (length: number) => {
      const value = admitVectorBuffer(scope, length, () => new Uint8Array(length));
      temporary.push(value);
      return value;
    };
    try {
      const normalized = vectors.map((vector): Vector => {
        if (vector.type !== "null") return vector;
        const valid = mask(vector.length);
        if (concrete.type === "struct") {
          const fields: Record<string, Vector> = {};
          for (const name of Object.keys(concrete.fields))
            setRecordValue(fields, name, { type: "null", length: vector.length });
          return { type: "struct", fields, length: vector.length, valid };
        }
        if (concrete.type === "list" || concrete.type === "map") {
          const offsets = admitVectorBuffer(
            scope,
            (vector.length + 1) * 4,
            () => new Int32Array(vector.length + 1),
          );
          temporary.push(offsets);
          return concrete.type === "list"
            ? { type: "list", offsets, child: concrete.child, valid }
            : { type: "map", offsets, keys: concrete.keys, values: concrete.values, valid };
        }
        const indices = admitVectorBuffer(
          scope,
          vector.length * 4,
          () => new Uint32Array(vector.length),
        );
        temporary.push(indices);
        return {
          type: "dict",
          indices,
          dictionary: concrete.type === "dict" ? concrete.dictionary : concrete,
          valid,
        };
      });
      // Normalization indices are scratch, so their identities cannot enter the
      // outer batch cache after this construction stage releases them.
      return concatVectors(name, normalized, scope, new Map());
    } finally {
      if (scope !== undefined && !scope.disposed)
        for (const value of temporary) scope.releaseAllocation(value);
    }
  }
  if (
    vectors.some((vector) => vector.type !== first.type) &&
    vectors.some((vector) => vector.type === "dict")
  ) {
    const shapes = new Set(vectors.map(concatLogicalShape).filter((shape) => shape !== "null"));
    if (shapes.size <= 1) {
      const temporary: Uint32Array[] = [];
      try {
        const dictionaries = vectors.map((vector) => {
          if (vector.type === "dict") return vector;
          const indices = admitVectorBuffer(scope, vectorLength(vector) * 4, () =>
            Uint32Array.from({ length: vectorLength(vector) }, (_, index) => index),
          );
          temporary.push(indices);
          return { type: "dict" as const, indices, dictionary: vector };
        });
        // Temporary identity arrays must not escape through the batch-level cache.
        return concatDictVectors(dictionaries, concatValid(dictionaries, scope), scope, new Map());
      } finally {
        if (scope !== undefined && !scope.disposed)
          for (const indices of temporary) scope.releaseAllocation(indices);
      }
    }
  }
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
  const valid =
    first.type === "timestamp" || first.type === "timestamp96"
      ? undefined
      : concatValid(vectors, scope);
  switch (first.type) {
    case "union":
      throw new LakeqlError(
        "LAKEQL_TYPE_ERROR",
        "Union concatenation requires domain reconciliation",
      );
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
            scope,
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
            scope,
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
            scope,
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
            scope,
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
            scope,
          ),
        },
        valid,
      );
    case "i64":
      return optionalValidity(
        {
          type: first.type,
          values: concatTypedArrays(
            vectors.map((vector) => requireVectorType(vector, "i64").values),
            scope,
          ),
        },
        valid,
      );
    case "u64":
      return optionalValidity(
        {
          type: first.type,
          values: concatTypedArrays(
            vectors.map((vector) => requireVectorType(vector, "u64").values),
            scope,
          ),
        },
        valid,
      );
    case "timestamp96": {
      const plan = planSelectedConcat(vectors.map(selectVector));
      if (plan === undefined)
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Incompatible wide timestamp concatenation");
      return plan.execute(scope);
    }
    case "timestamp": {
      const timestamps = vectors.map((vector) => requireVectorType(vector, "timestamp"));
      if (
        timestamps.every(
          (vector) =>
            vector.unit === first.unit && vector.isAdjustedToUTC === first.isAdjustedToUTC,
        )
      )
        return optionalValidity(
          {
            type: "timestamp",
            values: concatTypedArrays(
              timestamps.map((vector) => vector.values),
              scope,
            ),
            unit: first.unit,
            isAdjustedToUTC: first.isAdjustedToUTC,
          },
          concatValid(vectors, scope),
        );
      const plan = planSelectedConcat(vectors.map(selectVector));
      if (plan === undefined)
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Incompatible timestamp concatenation");
      return plan.execute(scope);
    }
    case "bool":
      return optionalValidity(
        {
          type: first.type,
          values: concatTypedArrays(
            vectors.map((vector) => requireVectorType(vector, "bool").values),
            scope,
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
        scope,
        indexCopies,
        prepared,
      );
    case "binary-view":
    case "utf8-view": {
      const packed = concatPackedVectors(vectors, valid, scope, prepared?.directPacked);
      if (packed === undefined)
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Incompatible packed concat");
      return packed;
    }
    case "struct": {
      const inputs = structConcatInputs(vectors);
      if (inputs !== undefined) {
        const fields = concatBatches(inputs, scope, prepared?.children);
        return optionalValidity(
          { type: "struct", fields: fields.columns, length: fields.rowCount },
          valid,
        );
      }
      return vectorFromValues(
        vectors.flatMap((vector) =>
          Array.from({ length: vectorLength(vector) }, (_, index) => vectorValue(vector, index)),
        ),
        scope,
      );
    }
    case "list":
    case "map": {
      const collections = vectors as CollectionVector[];
      const plan = collectionConcatInputs(collections);
      const offsets = admitVectorBuffer(
        scope,
        (plan.rows + 1) * 4,
        () => new Int32Array(plan.rows + 1),
      );
      let row = 0;
      let base = 0;
      for (const vector of collections) {
        const start = vector.offsets[0] ?? 0;
        for (let index = 1; index < vector.offsets.length; index++)
          offsets[++row] = base + (vector.offsets[index] ?? start) - start;
        base = offsets[row] ?? base;
      }
      const children = concatBatches(plan.batches, scope, prepared?.children).columns;
      if (first.type === "list") {
        const child = children.child;
        if (child === undefined) throw new Error("Missing list child");
        return optionalValidity({ type: "list", offsets, child }, valid);
      }
      const { keys, values } = children;
      if (keys === undefined || values === undefined) throw new Error("Missing map children");
      return optionalValidity({ type: "map", offsets, keys, values }, valid);
    }
  }
}

function concatLogicalShape(vector: Vector): Vector["type"] {
  while (vector.type === "dict") vector = vector.dictionary;
  if (vector.type === "utf8-view") return "utf8";
  if (vector.type === "binary-view") return "binary";
  return vector.type;
}

function concatDictVectors(
  vectors: readonly Extract<Vector, { type: "dict" }>[],
  valid: Uint8Array | undefined,
  scope: MemoryScope | undefined,
  indexCopies: DictionaryIndexCopies,
  prepared?: ConcatColumnPlan,
): Vector {
  const first = vectors[0];
  if (first === undefined) throw new LakeqlError("LAKEQL_TYPE_ERROR", "No vectors to concatenate");
  if (vectors.every((vector) => vector.dictionary === first.dictionary)) {
    const cached = indexCopies.get(first.indices);
    let indices: Uint32Array;
    if (
      cached !== undefined &&
      cached.sources.length === vectors.length &&
      vectors.every((vector, index) => vector.indices === cached.sources[index])
    ) {
      indices = cached.output;
    } else {
      const sources = vectors.map((vector) => vector.indices);
      indices = concatTypedArrays(sources, scope);
      indexCopies.set(first.indices, { sources, output: indices });
    }
    return optionalValidity(
      {
        type: "dict",
        indices,
        dictionary: first.dictionary,
      },
      valid,
    );
  }
  const scalar = concatScalarDictionaries(vectors, valid, scope, prepared?.scalar);
  if (scalar !== undefined) return scalar;
  const packed = concatAdaptivePackedDictionaries(vectors, valid, scope, prepared?.packed);
  if (packed !== undefined) return packed;
  const leaf = nestedDictionaryLeaf(first);
  if (
    leaf !== undefined &&
    vectors.every((vector) => nestedDictionaryLeaf(vector)?.type === leaf.type)
  ) {
    const expanded: ReturnType<typeof expandNestedDictionary>[] = [];
    try {
      for (const vector of vectors) expanded.push(expandNestedDictionary(vector, scope));
      const result = concatVectors(
        "nested dictionary",
        expanded.map((item) => item.vector),
        scope,
        new Map(),
      );
      // Expanded vectors include all dictionary-level nulls. The earlier outer
      // mask has been superseded by the recursive concat's complete mask.
      if (valid !== undefined) scope?.tryReleaseAllocation(valid);
      return result;
    } finally {
      for (const item of expanded) item.release();
    }
  }
  return vectorFromValues(
    vectors.flatMap((vector) =>
      Array.from({ length: vectorLength(vector) }, (_, index) => vectorValue(vector, index)),
    ),
    scope,
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
  T extends
    | Float32Array
    | Float64Array
    | Int32Array
    | Uint8Array
    | Uint32Array
    | BigInt64Array
    | BigUint64Array,
>(arrays: readonly T[], scope?: MemoryScope): T {
  const length = arrays.reduce((sum, array) => sum + array.length, 0);
  const first = arrays[0];
  if (first === undefined) {
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "No arrays to concatenate");
  }
  const ArrayConstructor = first.constructor as { new (length: number): T };
  const out = admitVectorBuffer(
    scope,
    length * first.BYTES_PER_ELEMENT,
    () => new ArrayConstructor(length),
  );
  const set = out.set as (source: T, offset: number) => void;
  let offset = 0;
  for (const array of arrays) {
    set.call(out, array, offset);
    offset += array.length;
  }
  return out;
}

function concatValid(vectors: readonly Vector[], scope?: MemoryScope): Uint8Array | undefined {
  const hasNull = vectors.some((vector) => "valid" in vector && vector.valid !== undefined);
  if (!hasNull) return undefined;
  const length = vectors.reduce((sum, vector) => sum + vectorLength(vector), 0);
  const out = admitVectorBuffer(scope, length, () => new Uint8Array(length));
  let offset = 0;
  for (const vector of vectors) {
    const length = vectorLength(vector);
    if (!("valid" in vector) || vector.valid === undefined) out.fill(1, offset, offset + length);
    else out.set(vector.valid, offset);
    offset += length;
  }
  return out;
}

function gatherValid(
  valid: Uint8Array,
  indices: readonly number[] | Uint32Array,
  scope?: MemoryScope,
): Uint8Array | undefined {
  // Avoid reserving a mask that would immediately be discarded for all-present output.
  if (indices.every((index) => valid[index] === 1)) return undefined;
  const out = admitVectorBuffer(scope, indices.length, () => new Uint8Array(indices.length));
  for (let index = 0; index < indices.length; index += 1) {
    const present = valid[indices[index] ?? 0] === 1;
    out[index] = present ? 1 : 0;
  }
  return out;
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
