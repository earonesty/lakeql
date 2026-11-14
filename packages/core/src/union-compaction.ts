import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Batch, Vector } from "./batch.js";
import { registerCompactionMetadata, retainsCompactionMetadata } from "./compaction-metadata.js";
import type { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory, retainSelectedAllocations } from "./memory-scope.js";
import { setRecordValue } from "./record-value.js";
import { compactSelectedUnion } from "./selected-union.js";
import {
  isUnionConcatRoutingAllocation,
  planUnionConcat,
  retainsUnionConcatRouting,
} from "./union-concat.js";
import { flattenUnionConcatInputs } from "./union-flatten.js";
import { isOwnedUnionRoutingAllocation } from "./union-routing-ownership.js";
import { indexVectorStorage, reachableVectors } from "./vector-reachability.js";

/** Bound routing history, including selections wrapping prior concatenations. */
export function needsUnionCompaction(batch: Batch): boolean {
  // A balanced concat needs far fewer levels. Permit logarithmic history slack
  // so ordinary appends do not pay for rebuilding all routing each time.
  const limit = 2 + Math.ceil(Math.log2(Math.max(2, batch.rowCount)));
  const exceeds = (vector: Vector, depth: number): boolean => {
    if (vector.type !== "union" && vector.type !== "dict") return false;
    if (depth >= limit) return true;
    if (vector.type === "dict") return exceeds(vector.dictionary, depth + 1);
    for (const child of vector.children) if (exceeds(child, depth + 1)) return true;
    return false;
  };
  for (const name in batch.columns)
    if (Object.hasOwn(batch.columns, name)) {
      const vector = batch.columns[name];
      if (vector !== undefined && exceeds(vector, 1)) return true;
    }
  return false;
}

/** Rebuild routing while sharing child payload, then drop provably obsolete routing.
 * Every stage is synchronous and admitted under the destination's actual budget.
 */
export function compactOwnedUnionBatch(
  input: OwnedMemory<Batch>,
  budget: MemoryBudget = input.budget,
): OwnedMemory<Batch> {
  const guard = input.retain();
  const scope = new MemoryScope(budget);
  const scratch = scope.childScope();
  try {
    const original = guard.value;
    let count = 0;
    for (const name in original.columns) if (Object.hasOwn(original.columns, name)) count++;
    const columns = scope.allocate(
      c.object + count * c.property,
      "compacted batch columns",
      () => Object.create(null) as Record<string, Vector>,
    ).value;
    registerCompactionMetadata(columns, "columns");
    for (const name in original.columns)
      if (Object.hasOwn(original.columns, name)) {
        const vector = original.columns[name];
        if (vector === undefined) continue;
        const trustedConcat = Boolean(isUnionConcatRoutingAllocation(vector));
        if (trustedConcat) {
          const flat = flattenUnionConcatInputs([vector], scratch);
          // Opaque routing can encode arbitrary selections; flattening its children
          // would change row order. Resolve the original logical rows instead.
          const selected = flat.some(
            (leaf) =>
              leaf.type === "dict" ||
              (leaf.type === "union" &&
                leaf.children.some((child) => child.type === "dict" || child.type === "union")),
          );
          setRecordValue(
            columns,
            name,
            selected ? compactSelectedUnion(vector, scope) : planUnionConcat(flat).execute(scope),
          );
        } else if (vector.type === "dict" || vector.type === "union")
          setRecordValue(columns, name, compactSelectedUnion(vector, scope));
        else setRecordValue(columns, name, vector);
      }
    const batch = scope.allocate(c.object + 2 * c.property, "compacted batch", () => ({
      rowCount: original.rowCount,
      columns,
    })).value;
    registerCompactionMetadata(batch, "batch");
    function* roots(): Iterable<Vector> {
      for (const name in columns)
        if (Object.hasOwn(columns, name)) {
          const vector = columns[name];
          if (vector !== undefined) yield vector;
        }
    }
    const index = indexVectorStorage(reachableVectors(roots(), scratch), scratch);
    retainSelectedAllocations(guard, scope, (value) => {
      // Atomic typed allocations can only back supported vectors through a
      // reachable buffer. Preserve the entire backing even for a tiny alias.
      if (!retainsCompactionMetadata(value, index.vectors)) return false;
      if (ArrayBuffer.isView(value)) return index.buffers.has(value.buffer);
      return !isOwnedUnionRoutingAllocation(value) || retainsUnionConcatRouting(value, index);
    });
    scratch.dispose();
    return scope.retainAllocations(batch);
  } finally {
    scope.dispose();
    guard.release();
  }
}
