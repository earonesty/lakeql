import type { MemoryScope } from "./memory-scope.js";
import { admitVectorBuffer } from "./vector-buffer.js";

/** Expand SQL truth values and consume the temporary dictionary mask.
 * The row mask owns independent backing and may outlive dictionary workspace.
 */
export function expandDictionaryMask(
  vector: { indices: Uint32Array; valid?: Uint8Array },
  dictionaryMask: Uint8Array,
  scope?: MemoryScope,
): Uint8Array {
  try {
    const mask = admitVectorBuffer(
      scope,
      vector.indices.length,
      () => new Uint8Array(vector.indices.length),
    );
    for (let index = 0; index < vector.indices.length; index++) {
      mask[index] =
        vector.valid?.[index] === 0 ? 2 : (dictionaryMask[vector.indices[index] ?? 0] ?? 2);
    }
    return mask;
  } finally {
    scope?.releaseAllocation(dictionaryMask);
  }
}
