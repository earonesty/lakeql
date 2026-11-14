import type { Batch } from "./batch.js";

/** Output allocation/alias capabilities of the non-compacting gather implementation.
 * typedBytes excludes JS arrays, object headers, and ownership bookkeeping.
 */
export interface GatherAllocationPlan {
  typedBytes: number;
  retainsInput: boolean;
  createsJavaScriptArrays: boolean;
}

export function sharedGatherPlan(
  batch: Batch,
  indices: readonly number[] | Uint32Array,
): GatherAllocationPlan {
  let typedBytes = 0;
  let sharedIndices = false;
  let retainsInput = false;
  for (const vector of Object.values(batch.columns)) {
    switch (vector.type) {
      case "decimal":
      case "union":
      case "list":
      case "map":
      case "struct":
      case "binary-view":
      case "utf8-view":
      case "utf8":
      case "binary":
        sharedIndices = true;
        retainsInput = true;
        continue;
      case "null":
        continue;
      case "dict":
        typedBytes += indices.length * 4;
        retainsInput = true;
        break;
      case "timestamp96":
        typedBytes += indices.length * 12;
        break;
      default:
        typedBytes += indices.length * vector.values.BYTES_PER_ELEMENT;
    }
    if (vector.valid !== undefined && indices.some((index) => vector.valid?.[index] !== 1))
      typedBytes += indices.length;
  }
  if (sharedIndices) typedBytes += indices.length * 4;
  return { typedBytes, retainsInput, createsJavaScriptArrays: false };
}
