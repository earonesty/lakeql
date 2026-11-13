import {
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type MemoryScope,
  type Vector,
  vectorLength,
} from "lakeql-core";
import { nestedContainerOffsets } from "./nested-container-offsets.js";
import type { indexNestedNode } from "./nested-node-index.js";

type NodeIndex = ReturnType<typeof indexNestedNode>;

/** Assemble an already aligned key/value entry stream without copying either payload.
 * Schema/cross-column validation precedes this operation; key semantics remain core map semantics.
 */
export function nestedMapVector(
  parent: NodeIndex,
  entries: NodeIndex,
  keys: Vector,
  values: Vector,
  scope: MemoryScope,
): Vector {
  if (vectorLength(keys) !== vectorLength(values))
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Unaligned nested map keys and values");
  const construction = scope.childScope();
  try {
    const offsets = nestedContainerOffsets(parent, entries, vectorLength(keys), construction);
    const vector = construction.allocate(
      c.object + 5 * c.property,
      "nested map vector",
      (): Vector => ({
        type: "map",
        offsets,
        keys,
        values,
        valid: parent.valid,
      }),
    ).value;
    scope.adopt(construction.retainAllocations(undefined));
    return vector;
  } finally {
    construction.dispose();
  }
}
