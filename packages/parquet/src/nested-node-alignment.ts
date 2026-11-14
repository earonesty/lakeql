import type { ColumnDecoder } from "hyparquet/src/types.js";
import { LakeqlError, type MemoryScope } from "lakeql-core";
import type { readNestedLevels } from "./nested-levels.js";
import { indexNestedNode } from "./nested-node-index.js";

export interface NestedNodeSource {
  schemaPath: ColumnDecoder["schemaPath"];
  levels: ReturnType<typeof readNestedLevels>;
}

/** Compare a common schema node across independently shredded columns. Descendant
 * event counts may differ; node validity and projected repetition boundaries may not.
 * Run after per-column schema transition validation. All index scratch is released.
 */
export function validateNestedNodeAlignment(
  left: NestedNodeSource,
  right: NestedNodeSource,
  depth: number,
  scope: MemoryScope,
): void {
  const invalid = () =>
    new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Nested node alignment mismatch");
  if (
    !Number.isInteger(depth) ||
    depth < 0 ||
    depth >= left.schemaPath.length ||
    depth >= right.schemaPath.length
  )
    throw invalid();
  let repetitions = 0;
  for (let i = 0; i <= depth; i++) {
    const a = left.schemaPath[i]?.element,
      b = right.schemaPath[i]?.element;
    if (!a || !b || a.name !== b.name || a.repetition_type !== b.repetition_type) throw invalid();
    if (a.repetition_type === "REPEATED") repetitions++;
  }
  const scratch = scope.childScope();
  try {
    const a = indexNestedNode(left.levels, left.schemaPath, depth, scratch);
    const b = indexNestedNode(right.levels, right.schemaPath, depth, scratch);
    if (a.continuesPrevious !== b.continuesPrevious || a.valid.length !== b.valid.length)
      throw invalid();
    for (let i = 0; i < a.valid.length; i++) {
      const ar = left.levels.repetition?.[a.eventOffsets[i] ?? 0] ?? 0;
      const br = right.levels.repetition?.[b.eventOffsets[i] ?? 0] ?? 0;
      if (a.valid[i] !== b.valid[i] || Math.min(ar, repetitions) !== Math.min(br, repetitions))
        throw invalid();
    }
  } finally {
    scratch.dispose();
  }
}
