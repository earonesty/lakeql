import { type Vector, vectorLength } from "./batch.js";
import { LakeqlError } from "./errors.js";

/** Replayable selection over immutable backing. -1 denotes a null row. Traversal
 * allocates no index array; descriptor storage scales with physical nesting.
 */
export interface VectorSelection {
  readonly leaf: Vector;
  readonly length: number;
  readonly mayHaveNull: boolean;
  collectionLength(): number;
  forEach(visit: (index: number, row: number) => void): void;
}

function indexResolver(vector: Vector): (index: number) => number {
  const length = vectorLength(vector);
  const valid = "valid" in vector ? vector.valid : undefined;
  const child = vector.type === "dict" ? indexResolver(vector.dictionary) : undefined;
  const indices = vector.type === "dict" ? vector.indices : undefined;
  const nulls = vector.type === "null";
  return (index) => {
    if (!Number.isInteger(index) || index < 0 || index >= length)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Selected vector index out of bounds");
    if (nulls || (valid !== undefined && valid[index] !== 1)) return -1;
    return child === undefined ? index : child(indices?.[index] ?? 0);
  };
}

function selection(
  source: Vector,
  length: number,
  indices: (visit: (index: number, row: number) => void) => void,
  inheritedNulls = false,
): VectorSelection {
  let leaf = source;
  let mayHaveNull = inheritedNulls;
  for (;;) {
    mayHaveNull ||= leaf.type === "null" || ("valid" in leaf && leaf.valid !== undefined);
    if (leaf.type !== "dict") break;
    leaf = leaf.dictionary;
  }
  const resolve = indexResolver(source);
  let childLength: number | undefined;
  const result: VectorSelection = {
    leaf,
    length,
    mayHaveNull,
    collectionLength() {
      if (childLength !== undefined) return childLength;
      let count = 0;
      forEachSelectedRange(result, (start, end) => {
        count += end - start;
        if (count > 0x7fffffff)
          throw new LakeqlError(
            "LAKEQL_VALIDATION_ERROR",
            "Selected collection exceeds Int32 offsets",
          );
      });
      childLength = count;
      return count;
    },
    forEach(visit) {
      indices((index, row) => visit(index < 0 ? -1 : resolve(index), row));
    },
  };
  return result;
}

export function selectVector(vector: Vector): VectorSelection {
  const length = vectorLength(vector);
  return selection(vector, length, (visit) => {
    for (let row = 0; row < length; row++) visit(row, row);
  });
}

export function selectVectorIndices(
  vector: Vector,
  indices: readonly number[] | Uint32Array,
): VectorSelection {
  return selection(vector, indices.length, (visit) => {
    for (let row = 0; row < indices.length; row++) {
      const index = indices[row];
      if (index === undefined || !Number.isSafeInteger(index) || index < 0)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Selected vector index out of bounds");
      visit(index, row);
    }
  });
}

export function selectStructField(parent: VectorSelection, name: string): VectorSelection {
  if (parent.leaf.type !== "struct")
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Expected selected struct");
  const field = Object.hasOwn(parent.leaf.fields, name) ? parent.leaf.fields[name] : undefined;
  if (field === undefined)
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Missing selected struct field");
  return selection(field, parent.length, (visit) => parent.forEach(visit), parent.mayHaveNull);
}

/** Traverse validated ranges without allocating a pair for each selected row. */
export function forEachSelectedRange(
  parent: VectorSelection,
  visit: (start: number, end: number, row: number) => void,
): void {
  const leaf = parent.leaf;
  if (leaf.type !== "list" && leaf.type !== "map")
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Expected selected collection");
  const offsets = leaf.offsets;
  const limit =
    leaf.type === "list"
      ? vectorLength(leaf.child)
      : Math.min(vectorLength(leaf.keys), vectorLength(leaf.values));
  parent.forEach((index, row) => {
    if (index < 0) {
      visit(0, 0, row);
      return;
    }
    const start = offsets[index];
    const end = offsets[index + 1];
    if (start === undefined || end === undefined || start < 0 || end < start || end > limit)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid selected collection offsets");
    visit(start, end, row);
  });
}

export function selectCollectionChild(
  parent: VectorSelection,
  name: "child" | "keys" | "values",
): VectorSelection {
  const leaf = parent.leaf;
  const child =
    leaf.type === "list" && name === "child"
      ? leaf.child
      : leaf.type === "map" && name === "keys"
        ? leaf.keys
        : leaf.type === "map" && name === "values"
          ? leaf.values
          : undefined;
  if (child === undefined)
    throw new LakeqlError("LAKEQL_TYPE_ERROR", "Missing selected collection child");
  const length = parent.collectionLength();
  return selection(child, length, (visit) => {
    let row = 0;
    forEachSelectedRange(parent, (start, end) => {
      for (let entry = start; entry < end; entry++) visit(entry, row++);
    });
  });
}
