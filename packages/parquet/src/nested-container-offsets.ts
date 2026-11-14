import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";
import type { indexNestedNode } from "./nested-node-index.js";

type NodeIndex = ReturnType<typeof indexNestedNode>;

/** Shared list/map routing with signed core offsets and atomic failure cleanup. */
export function nestedContainerOffsets(
  parent: NodeIndex,
  child: NodeIndex,
  childCount: number,
  scope: MemoryScope,
): Int32Array {
  const invalid = () =>
    new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Unaligned nested container nodes");
  const rows = parent.valid.length;
  const children = child.valid.length;
  if (
    children > 0x7fffffff ||
    parent.eventOffsets.length !== rows + 1 ||
    child.eventOffsets.length !== children + 1 ||
    childCount !== children ||
    parent.eventOffsets[rows] !== child.eventOffsets[children]
  )
    throw invalid();
  const construction = scope.childScope();
  try {
    const offsets = construction.allocate(
      (rows + 1) * 4 + c.view,
      "nested container offsets",
      () => new Int32Array(rows + 1),
    ).value;
    let next = 0;
    for (let row = 0; row < rows; row++) {
      const start = parent.eventOffsets[row] ?? 0;
      const end = parent.eventOffsets[row + 1] ?? 0;
      if (start >= end) throw invalid();
      while (next < children && (child.eventOffsets[next] ?? 0) < end) {
        if (
          !parent.valid[row] ||
          (child.eventOffsets[next] ?? 0) < start ||
          (child.eventOffsets[next] ?? 0) >= (child.eventOffsets[next + 1] ?? 0)
        )
          throw invalid();
        next++;
      }
      offsets[row + 1] = next;
    }
    if (next !== children) throw invalid();
    scope.adopt(construction.retainAllocations(undefined));
    return offsets;
  } finally {
    construction.dispose();
  }
}
