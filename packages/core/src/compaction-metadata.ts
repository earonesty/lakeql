import type { Vector } from "./batch.js";

// Only records whose allocation excludes all referenced child storage qualify.
const metadata = new WeakMap<
  object,
  "batch" | "columns" | "vector" | "slots" | "children" | "construction"
>();

export function registerCompactionMetadata(
  value: object,
  kind: "batch" | "columns" | "vector" | "slots" | "children" | "construction",
): void {
  metadata.set(value, kind);
}

/** Unknown allocations remain conservative. Known descriptors own no payload. */
export function retainsCompactionMetadata(value: unknown, vectors: ReadonlySet<Vector>): boolean {
  if (typeof value !== "object" || value === null) return true;
  const kind = metadata.get(value);
  if (kind === undefined) return true;
  if (kind === "batch" || kind === "construction") return false;
  if (kind === "vector") return vectors.has(value as Vector);
  if (kind === "children") {
    for (const vector of vectors)
      if (vector.type === "union" && vector.children === value) return true;
    return false;
  }
  if (kind === "slots") {
    for (const vector of vectors)
      if ((vector.type === "utf8" || vector.type === "binary") && vector.values === value)
        return true;
    return false;
  }
  // A former columns record can become the field directory of a struct vector.
  for (const vector of vectors)
    if (vector.type === "struct" && vector.fields === value) return true;
  return false;
}
