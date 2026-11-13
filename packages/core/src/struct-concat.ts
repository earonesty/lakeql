import { type Batch, type Vector, vectorLength } from "./batch.js";

/** Matching struct layouts can concatenate fields without materializing rows.
 * A different field set requires schema normalization before this operator.
 */
export function structConcatInputs(vectors: readonly Vector[]): Batch[] | undefined {
  const first = vectors[0];
  if (first?.type !== "struct") return undefined;
  const names = Object.keys(first.fields);
  const batches: Batch[] = [];
  for (const vector of vectors) {
    if (vector.type !== "struct" || Object.keys(vector.fields).length !== names.length)
      return undefined;
    if (
      names.some(
        (name) =>
          !Object.hasOwn(vector.fields, name) ||
          vectorLength(vector.fields[name] as Vector) !== vector.length,
      )
    )
      return undefined;
    batches.push({ rowCount: vector.length, columns: vector.fields });
  }
  return batches;
}
