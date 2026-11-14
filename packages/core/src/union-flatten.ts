import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Vector } from "./batch.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";
import { isUnionConcatRoutingAllocation } from "./union-concat.js";

/** Flatten only immutable concat-created routing, whose children are contiguous
 * in source order. Arbitrary unions, dictionary selections and slices stay opaque.
 * Traversal frames and the exact output directory are admitted; no per-leaf scratch escapes.
 */
export function flattenUnionConcatInputs(
  inputs: readonly Vector[],
  scope: MemoryScope,
): readonly Vector[] {
  const construction = scope.childScope();
  const scratch = construction.childScope();
  interface Frame {
    children: readonly Vector[];
    index: number;
    parent: OwnedMemory<Frame> | undefined;
  }
  const push = (
    children: readonly Vector[],
    parent: OwnedMemory<Frame> | undefined,
  ): OwnedMemory<Frame> =>
    scratch.allocate(c.object + 3 * c.property, "concat flatten frame", () => ({
      children,
      index: 0,
      parent,
    }));
  const visit = (accept: (vector: Vector) => void): void => {
    let frame: OwnedMemory<Frame> | undefined = push(inputs, undefined);
    while (frame !== undefined) {
      const state: Frame = frame.value;
      if (state.index === state.children.length) {
        const parent: OwnedMemory<Frame> | undefined = state.parent;
        frame.release();
        frame = parent;
        continue;
      }
      const vector = state.children[state.index++];
      if (vector === undefined) continue;
      if (isUnionConcatRoutingAllocation(vector)) frame = push(vector.children, frame);
      else accept(vector);
    }
  };
  try {
    // Inputs are immutable: count first, then fill one exactly admitted directory.
    // Avoid a separately owned linked record for every terminal child.
    let count = 0;
    visit(() => count++);
    const output = construction.allocate(
      c.array + count * c.arraySlot,
      "flattened concat inputs",
      () => new Array<Vector>(count),
    ).value;
    let index = 0;
    visit((vector) => {
      output[index++] = vector;
    });
    scratch.dispose();
    scope.adopt(construction.retainAllocations(undefined));
    return output;
  } finally {
    construction.dispose();
  }
}
