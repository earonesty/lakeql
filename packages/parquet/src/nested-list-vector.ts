import {
  MATERIALIZATION_CHARGES as c,
  type MemoryScope,
  type Vector,
  vectorLength,
} from "lakeql-core";
import { nestedContainerOffsets } from "./nested-container-offsets.js";
import type { indexNestedNode } from "./nested-node-index.js";

type NodeIndex = ReturnType<typeof indexNestedNode>;

/** Assemble one repeated dimension from aligned schema-node occurrences. Child values
 * and parent validity are borrowed. Continued first/last containers remain page fragments.
 */
export function nestedListVector(
  parent: NodeIndex,
  child: NodeIndex,
  values: Vector,
  scope: MemoryScope,
): Vector {
  const construction = scope.childScope();
  try {
    const offsets = nestedContainerOffsets(parent, child, vectorLength(values), construction);
    const vector = construction.allocate(
      c.object + 4 * c.property,
      "nested list vector",
      (): Vector => ({
        type: "list",
        offsets,
        child: values,
        valid: parent.valid,
      }),
    ).value;
    scope.adopt(construction.retainAllocations(undefined));
    return vector;
  } finally {
    construction.dispose();
  }
}
