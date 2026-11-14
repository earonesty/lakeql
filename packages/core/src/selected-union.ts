import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Vector, vectorLength } from "./batch.js";
import { registerCompactionMetadata } from "./compaction-metadata.js";
import { LakeqlError } from "./errors.js";
import type { MemoryScope } from "./memory-scope.js";
import { visitRoutedRows } from "./routed-rows.js";
import { planUnionConcat } from "./union-concat.js";
import { registerUnionRoutingAllocation } from "./union-routing-ownership.js";

/** Rebuild dictionary/union routing without materializing terminal payload.
 * Byte tags admit at most 256 children; blocks of <=256 rows meet that physical
 * capability regardless of how many different domains occur in the input.
 */
export function compactSelectedUnion(source: Vector, scope: MemoryScope): Vector {
  const length = vectorLength(source);
  if (!Number.isSafeInteger(length) || length < 0 || length > 0xffffffff)
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid selected union extent");
  const construction = scope.childScope();
  const scratch = construction.childScope();
  let blockScratch: MemoryScope | undefined;
  try {
    const nulls = construction.allocate(c.object + 2 * c.property, "selected null domain", () => ({
      type: "null" as const,
      length: 1,
    })).value;
    registerCompactionMetadata(nulls, "vector");
    if (length === 0) {
      const empty: Vector = { type: "null", length: 0 };
      return empty;
    }
    const blocks = scratch.allocate(
      c.array + Math.ceil(length / 256) * c.arraySlot,
      "selected union blocks",
      () => new Array<Vector>(Math.ceil(length / 256)),
    ).value;
    let block: Extract<Vector, { type: "union" }> | undefined;
    let domains: Map<Vector | undefined, number> | undefined;
    visitRoutedRows(source, scratch, (leaf, index, row) => {
      const offset = row % 256;
      if (offset === 0) {
        const size = Math.min(256, length - row);
        blockScratch = scratch.childScope();
        domains = blockScratch.allocate(
          c.object,
          "selected union domain map",
          () => new Map<Vector | undefined, number>(),
        ).value;
        block = construction.allocate(
          5 * size + 2 * c.view + c.object + 4 * c.property + c.array + size * c.arraySlot,
          "selected union routing",
          () => ({
            type: "union" as const,
            tags: new Uint8Array(size),
            offsets: new Uint32Array(size),
            children: new Array<Vector>(size).fill(nulls),
          }),
        ).value;
        blocks[Math.floor(row / 256)] = block;
      }
      if (block === undefined || domains === undefined || blockScratch === undefined)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Missing selected routing block");
      let tag = domains.get(leaf);
      if (tag === undefined) {
        tag = domains.size;
        const nextTag = tag;
        blockScratch.allocate(c.property, "selected union domain entry", () => {
          domains?.set(leaf, nextTag);
        });
        (block.children as Vector[])[tag] = leaf ?? nulls;
      }
      block.tags[offset] = tag;
      block.offsets[offset] = leaf === undefined ? 0 : index;
      if (offset === block.tags.length - 1) {
        // Keep the admitted maximum directory capacity charged conservatively.
        (block.children as Vector[]).length = domains.size;
        registerUnionRoutingAllocation(block);
        blockScratch.dispose();
        blockScratch = undefined;
      }
    });
    const output = planUnionConcat(blocks).execute(construction);
    scratch.dispose();
    scope.adopt(construction.retainAllocations(undefined));
    return output;
  } finally {
    construction.dispose();
  }
}
