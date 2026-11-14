import { type Vector, vectorLength } from "./batch.js";
import { LakeqlError } from "./errors.js";
import { MATERIALIZATION_CHARGES as c } from "./materialization-plan.js";
import type { MemoryScope } from "./memory-scope.js";
import { registerUnionRoutingAllocation } from "./union-routing-ownership.js";
import type { VectorStorageIndex } from "./vector-reachability.js";

const concatRoutingAllocations = new WeakSet<object>();

/** Concatenate immutable vectors by routing into shared children. A bounded
 * fanout tree supports any input count without overflowing byte-sized tags.
 * Input ownership must remain pinned; only routing and directories are new.
 */
export function planUnionConcat(vectors: readonly Vector[]): {
  bytes: number;
  execute(scope?: MemoryScope): Vector;
} {
  if (vectors.length === 0) throw new LakeqlError("LAKEQL_TYPE_ERROR", "Empty union concatenation");
  const rows = (start: number, end: number): number => {
    let length = 0;
    for (let i = start; i < end; i++) {
      const vector = vectors[i];
      if (vector === undefined) throw new LakeqlError("LAKEQL_TYPE_ERROR", "Missing union input");
      const count = vectorLength(vector);
      if (!Number.isSafeInteger(count) || count < 0)
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Invalid union input length");
      length += count;
    }
    if (!Number.isSafeInteger(length) || length > 0xffffffff)
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Union row extent exceeds u32 capacity");
    return length;
  };
  const charge = (length: number, children: number) =>
    5 * length + 2 * c.view + c.object + 4 * c.property + c.array + children * c.arraySlot;
  function measure(start: number, end: number): number {
    const length = rows(start, end);
    if (end - start === 1) return 0;
    const stride = childStride(end - start);
    let bytes = charge(length, Math.ceil((end - start) / stride));
    for (let begin = start; begin < end; begin += stride)
      bytes += measure(begin, Math.min(end, begin + stride));
    if (!Number.isSafeInteger(bytes))
      throw new LakeqlError("LAKEQL_TYPE_ERROR", "Union allocation extent exceeds safe capacity");
    return bytes;
  }
  const bytes = measure(0, vectors.length);
  return {
    bytes,
    execute(scope) {
      const stage = scope?.childStage(bytes, "union concatenation");
      function build(start: number, end: number): Vector {
        if (end - start === 1) return vectors[start] as Vector;
        const length = rows(start, end);
        const stride = childStride(end - start);
        const count = Math.ceil((end - start) / stride);
        const create = (): Extract<Vector, { type: "union" }> => ({
          type: "union",
          tags: new Uint8Array(length),
          offsets: new Uint32Array(length),
          children: new Array<Vector>(count),
        });
        const result =
          stage === undefined
            ? create()
            : stage.allocate(charge(length, count), "union concatenation node", create).value;
        let row = 0;
        for (let child = 0, begin = start; begin < end; child++, begin += stride) {
          const value = build(begin, Math.min(end, begin + stride));
          (result.children as Vector[])[child] = value;
          const size = vectorLength(value);
          result.tags.fill(child, row, row + size);
          for (let index = 0; index < size; index++) result.offsets[row + index] = index;
          row += size;
        }
        concatRoutingAllocations.add(result);
        registerUnionRoutingAllocation(result);
        return result;
      }
      try {
        const output = build(0, vectors.length);
        if (stage !== undefined) scope?.adopt(stage.retainAllocations(undefined));
        return output;
      } finally {
        stage?.dispose();
      }
    },
  };
}

/** Shape-only inspection must not touch dictionary payload or construct selectors. */
export function hasUnionLeaf(vector: Vector): boolean {
  while (vector.type === "dict") vector = vector.dictionary;
  return vector.type === "union";
}

/** Fill lower levels before adding height; avoid hundreds of tiny sibling nodes. */
function childStride(inputs: number): number {
  let stride = 1;
  while (Math.ceil(inputs / stride) > 256) stride *= 256;
  return stride;
}

/** Only this module's atomic routing allocations may be discarded by reachability.
 * Their charge covers node routing and directories, never borrowed child payload.
 */
export function isUnionConcatRoutingAllocation(
  value: unknown,
): value is Extract<Vector, { type: "union" }> {
  return typeof value === "object" && value !== null && concatRoutingAllocations.has(value);
}

/** Conservative alias check for a routing allocation after descriptor slicing.
 * Identity alone is insufficient: slices retain the original backing buffers.
 */
export function retainsUnionConcatRouting(
  allocation: Extract<Vector, { type: "union" }>,
  index: VectorStorageIndex,
): boolean {
  return (
    index.vectors.has(allocation) ||
    index.childDirectories.has(allocation.children) ||
    index.buffers.has(allocation.tags.buffer) ||
    index.buffers.has(allocation.offsets.buffer)
  );
}
