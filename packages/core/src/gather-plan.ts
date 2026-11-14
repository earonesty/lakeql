import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Batch } from "./batch.js";

/** Output allocation/alias capabilities of the non-compacting gather implementation.
 * typedBytes covers backing; viewBytes covers retained typed-array objects.
 * JS arrays, other object headers, and ownership bookkeeping remain separate.
 */
export interface GatherAllocationPlan {
  typedBytes: number;
  viewBytes: number;
  retainsInput: boolean;
  createsJavaScriptArrays: boolean;
}

export function sharedGatherPlan(
  batch: Batch,
  indices: readonly number[] | Uint32Array,
): GatherAllocationPlan {
  let typedBytes = 0;
  let viewBytes = 0;
  let sharedIndices = false;
  let retainsInput = false;
  for (const name in batch.columns) {
    if (!Object.hasOwn(batch.columns, name)) continue;
    const vector = batch.columns[name];
    if (vector === undefined) continue;
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
        viewBytes += c.view;
        retainsInput = true;
        break;
      case "timestamp96":
        typedBytes += indices.length * 12;
        viewBytes += 2 * c.view;
        break;
      default:
        typedBytes += indices.length * vector.values.BYTES_PER_ELEMENT;
        viewBytes += c.view;
    }
    if (vector.valid !== undefined && indices.some((index) => vector.valid?.[index] !== 1)) {
      typedBytes += indices.length;
      viewBytes += c.view;
    }
  }
  if (sharedIndices) {
    typedBytes += indices.length * 4;
    viewBytes += c.view;
  }
  return { typedBytes, viewBytes, retainsInput, createsJavaScriptArrays: false };
}
