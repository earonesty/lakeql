import { type Vector, type VectorSource, vectorFromSource } from "./batch.js";
import { registerCompactionMetadata } from "./compaction-metadata.js";
import { MATERIALIZATION_CHARGES as c } from "./materialization-plan.js";
import type { MemoryScope } from "./memory-scope.js";
import { buildUnionRouting, type UnionRouting } from "./union-routing.js";

/** Compact routing and child vectors; construction-only indices do not escape. */
export interface UnionConstruction {
  readonly routing: Pick<UnionRouting, "tags" | "offsets">;
  readonly children: readonly Vector[];
}

/** Source and classifier must remain stable for the duration of construction.
 * Child reads use grouped row indices, avoiding per-child full-source scans.
 * Child and routing allocations transfer together only after every child succeeds.
 */
export function buildUnionChildren(
  source: VectorSource,
  childCount: number,
  classify: (row: number) => number,
  scope: MemoryScope,
): UnionConstruction {
  const construction = scope.childScope();
  const scratch = construction.childScope();
  try {
    const routing = buildUnionRouting(source.length, childCount, classify, construction, scratch);
    const children = construction.allocate(
      c.array + childCount * c.arraySlot,
      "union child vector directory",
      () => new Array<Vector>(childCount),
    ).value;
    registerCompactionMetadata(children, "children");
    for (let child = 0; child < childCount; child++) {
      const begin = routing.childStarts[child] ?? 0;
      const end = routing.childStarts[child + 1] ?? begin;
      children[child] = vectorFromSource(
        {
          length: end - begin,
          get: (index) => source.get(routing.sourceRows[begin + index] ?? 0),
        },
        construction,
      );
    }
    const result = construction.allocate(
      2 * c.object + 4 * c.property,
      "union construction",
      () => ({
        routing: { tags: routing.tags, offsets: routing.offsets },
        children,
      }),
    ).value;
    registerCompactionMetadata(result, "construction");
    scratch.dispose();
    scope.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    construction.dispose();
  }
}
