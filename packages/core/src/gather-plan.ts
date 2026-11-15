import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Batch } from "./batch.js";
import {
  GATHER_DESCRIPTOR_BASE_BYTES,
  gatherColumnEntryBytes,
  gatherVectorDescriptorBytes,
} from "./gather-descriptors.js";

/** Output allocation/alias capabilities of the non-compacting gather implementation.
 * typedBytes covers backing; viewBytes covers retained typed-array objects.
 * descriptorBytes covers the batch, directory and vector containers.
 * Ownership bookkeeping remains separate.
 */
export interface GatherAllocationPlan {
  typedBytes: number;
  viewBytes: number;
  descriptorBytes: number;
  retainsInput: boolean;
  createsJavaScriptArrays: boolean;
}

export function sharedGatherPlan(
  batch: Batch,
  indices: readonly number[] | Uint32Array,
): GatherAllocationPlan {
  let typedBytes = 0;
  let viewBytes = 0;
  let descriptorBytes = GATHER_DESCRIPTOR_BASE_BYTES;
  let sharedIndices = false;
  let retainsInput = false;
  for (const name in batch.columns) {
    if (!Object.hasOwn(batch.columns, name)) continue;
    const vector = batch.columns[name];
    if (vector === undefined) continue;
    descriptorBytes += gatherColumnEntryBytes(name);
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
        descriptorBytes += gatherVectorDescriptorBytes(vector, false, false);
        continue;
      case "null":
        descriptorBytes += gatherVectorDescriptorBytes(vector, false, false);
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
    const hasNull =
      vector.valid !== undefined && indices.some((index) => vector.valid?.[index] !== 1);
    descriptorBytes += gatherVectorDescriptorBytes(vector, hasNull, false);
    if (hasNull) {
      typedBytes += indices.length;
      viewBytes += c.view;
    }
  }
  if (sharedIndices) {
    typedBytes += indices.length * 4;
    viewBytes += c.view;
  }
  return { typedBytes, viewBytes, descriptorBytes, retainsInput, createsJavaScriptArrays: false };
}
