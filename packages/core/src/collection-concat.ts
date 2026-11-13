import { type Batch, type Vector, vectorLength } from "./batch.js";
import { LakeqlError } from "./errors.js";
import { sliceVector } from "./vector-slice.js";

export type CollectionVector = Extract<Vector, { type: "list" | "map" }>;

/** Rebase contiguous child windows, retaining null-parent spans as hidden data. */
export function collectionConcatInputs(vectors: readonly CollectionVector[]): {
  batches: Batch[];
  rows: number;
  entries: number;
} {
  let rows = 0;
  let entries = 0;
  const batches: Batch[] = [];
  for (const vector of vectors) {
    if (vector.offsets.length === 0)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Collection offsets require an endpoint");
    const start = vector.offsets[0] ?? 0;
    const end = vector.offsets[vector.offsets.length - 1] ?? 0;
    const limit =
      vector.type === "list"
        ? vectorLength(vector.child)
        : Math.min(vectorLength(vector.keys), vectorLength(vector.values));
    let previous = start;
    for (const offset of vector.offsets) {
      if (offset < 0 || offset < previous || offset > limit)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid collection offsets");
      previous = offset;
    }
    rows += vector.offsets.length - 1;
    entries += end - start;
    if (!Number.isSafeInteger(rows) || entries > 0x7fffffff)
      throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Collection concat exceeds Int32 offsets");
    const columns =
      vector.type === "list"
        ? { child: sliceVector(vector.child, start, end) }
        : {
            keys: sliceVector(vector.keys, start, end),
            values: sliceVector(vector.values, start, end),
          };
    batches.push({ rowCount: end - start, columns });
  }
  const populated = batches.filter((batch) => batch.rowCount > 0);
  return { batches: populated.length > 0 ? populated : batches.slice(0, 1), rows, entries };
}
