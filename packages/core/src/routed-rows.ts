import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Vector, vectorLength } from "./batch.js";
import { LakeqlError } from "./errors.js";
import type { MemoryScope } from "./memory-scope.js";
import { reachableVectors } from "./vector-reachability.js";

/** Visit terminal physical rows through arbitrary dictionary/union selections.
 * Nulls use undefined/-1. Payload values are never materialized. Graph workspace
 * is admitted and released; routing is iterative, with a finite cycle bound.
 * The caller owns any allocations or effects performed by its visitor.
 */
export function visitRoutedRows(
  source: Vector,
  scope: MemoryScope,
  visit: (leaf: Vector | undefined, index: number, row: number) => void,
): void {
  const scratch = scope.childScope();
  try {
    const roots = scratch.allocate(c.array + c.arraySlot, "routed row roots", () => [source]).value;
    const nodes = reachableVectors(roots, scratch);
    const count = vectorLength(source);
    const invalid = () =>
      new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid selected union routing");
    if (!Number.isSafeInteger(count) || count < 0) throw invalid();
    for (let row = 0; row < count; row++) {
      let vector = source;
      let index = row;
      let steps = 0;
      for (;;) {
        if (++steps > nodes.size)
          throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Cyclic selected union routing");
        const length = vectorLength(vector);
        if (
          !Number.isSafeInteger(length) ||
          length < 0 ||
          !Number.isSafeInteger(index) ||
          index < 0 ||
          index >= length
        )
          throw invalid();
        if (
          vector.type === "null" ||
          ("valid" in vector && vector.valid !== undefined && vector.valid[index] !== 1)
        ) {
          visit(undefined, -1, row);
          break;
        }
        if (vector.type === "dict") {
          const selected = vector.indices[index];
          if (selected === undefined) throw invalid();
          index = selected;
          vector = vector.dictionary;
        } else if (vector.type === "union") {
          const tag = vector.tags[index];
          const offset = vector.offsets[index];
          const child = tag === undefined ? undefined : vector.children[tag];
          if (child === undefined || offset === undefined) throw invalid();
          index = offset;
          vector = child;
        } else {
          visit(vector, index, row);
          break;
        }
      }
    }
  } finally {
    scratch.dispose();
  }
}
