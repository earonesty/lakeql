import { type Vector, vectorLength } from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { MemoryScope } from "./memory-scope.js";
import { setRecordValue } from "./record-value.js";
import { admitVectorBuffer } from "./vector-buffer.js";
import { selectVector } from "./vector-selection.js";

export function nestedDictionaryLeaf(
  vector: Vector,
): Extract<Vector, { type: "struct" | "list" | "map" }> | undefined {
  while (vector.type === "dict") vector = vector.dictionary;
  return vector.type === "struct" || vector.type === "list" || vector.type === "map"
    ? vector
    : undefined;
}

/** Exact new selection metadata for nested expansion; child payload is borrowed.
 * No selected-index or payload arrays are allocated while planning.
 */
export function nestedDictionaryExpansionPlan(vector: Extract<Vector, { type: "dict" }>) {
  const leaf = nestedDictionaryLeaf(vector);
  if (leaf === undefined) throw new LakeqlError("LAKEQL_TYPE_ERROR", "Expected nested dictionary");
  const selection = selectVector(vector);
  const rows = selection.length;
  let entries = 0;
  let nullable = false;
  selection.forEach((index) => {
    if (index < 0) {
      nullable = true;
      return;
    }
    if (leaf.type !== "struct") {
      const start = leaf.offsets[index];
      const end = leaf.offsets[index + 1];
      const limit =
        leaf.type === "list"
          ? vectorLength(leaf.child)
          : Math.min(vectorLength(leaf.keys), vectorLength(leaf.values));
      if (start === undefined || end === undefined || start < 0 || end < start || end > limit)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid nested dictionary offsets");
      entries += end - start;
      if (entries > 0x7fffffff)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Nested dictionary exceeds Int32 offsets");
    }
  });
  const count = leaf.type === "struct" ? rows : entries;
  const bytes = count * 4 + (nullable ? rows : 0) + (leaf.type === "struct" ? 0 : (rows + 1) * 4);
  if (!Number.isSafeInteger(bytes))
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Nested dictionary exceeds safe integer capacity",
    );
  return { leaf, rows, entries, nullable, count, bytes, selection };
}

/** Expand only selected nested rows into typed selection/offset metadata. Child
 * payload remains borrowed until the recursive concat copies or pins it.
 */
export function expandNestedDictionary(
  vector: Extract<Vector, { type: "dict" }>,
  scope?: MemoryScope,
): { vector: Vector; release(): void } {
  const { leaf, rows, nullable, count, bytes, selection } = nestedDictionaryExpansionPlan(vector);
  const stage = scope?.childStage(bytes, "nested dictionary selection");
  try {
    const indices = admitVectorBuffer(stage, count * 4, () => new Uint32Array(count));
    const valid = nullable ? admitVectorBuffer(stage, rows, () => new Uint8Array(rows)) : undefined;
    const offsets =
      leaf.type === "struct"
        ? undefined
        : admitVectorBuffer(stage, (rows + 1) * 4, () => new Int32Array(rows + 1));
    let position = 0;
    selection.forEach((index, row) => {
      if (valid !== undefined) valid[row] = index < 0 ? 0 : 1;
      if (leaf.type === "struct") indices[row] = Math.max(0, index);
      else {
        if (index >= 0)
          for (
            let entry = leaf.offsets[index] ?? 0;
            entry < (leaf.offsets[index + 1] ?? 0);
            entry++
          )
            indices[position++] = entry;
        if (offsets !== undefined) offsets[row + 1] = position;
      }
    });
    const validity = valid === undefined ? {} : { valid };
    let output: Vector;
    if (leaf.type === "struct") {
      const fields: Record<string, Vector> = {};
      for (const [name, dictionary] of Object.entries(leaf.fields))
        setRecordValue(fields, name, { type: "dict", indices, dictionary, ...validity });
      output = { type: "struct", fields, length: rows, ...validity };
    } else {
      if (offsets === undefined) throw new Error("Missing nested offsets");
      output =
        leaf.type === "list"
          ? {
              type: "list",
              offsets,
              child: { type: "dict", indices, dictionary: leaf.child },
              ...validity,
            }
          : {
              type: "map",
              offsets,
              keys: { type: "dict", indices, dictionary: leaf.keys },
              values: { type: "dict", indices, dictionary: leaf.values },
              ...validity,
            };
    }
    return { vector: output, release: () => stage?.dispose() };
  } catch (error) {
    stage?.dispose();
    throw error;
  }
}
