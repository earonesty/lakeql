import { LakeqlError } from "./errors.js";
import { MATERIALIZATION_CHARGES as c } from "./materialization-plan.js";
import type { MemoryScope } from "./memory-scope.js";

/** Dense child-local offsets plus a grouped source-row index for linear child reads. */
export interface UnionRouting {
  readonly tags: Uint8Array;
  readonly offsets: Uint32Array;
  readonly sourceRows: Uint32Array;
  /** Child k occupies sourceRows[childStarts[k] .. childStarts[k + 1]). */
  readonly childStarts: Uint32Array;
}

/** Classify once per row. Tags are stable domain IDs, including any null domain.
 * No row objects or per-child JS arrays are constructed. All buffers are admitted
 * before classification, and classifier failures roll back the entire operation.
 */
export function buildUnionRouting(
  length: number,
  childCount: number,
  classify: (row: number) => number,
  scope: MemoryScope,
  constructionScratch?: MemoryScope,
): UnionRouting {
  if (
    !Number.isSafeInteger(length) ||
    length < 0 ||
    length > 0xffffffff ||
    !Number.isInteger(childCount) ||
    childCount < 1 ||
    childCount > 256
  )
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid union routing extent");
  const retained = 9 * length + 4 * (childCount + 1) + 4 * c.view + c.object + 4 * c.property;
  const scratchBytes = 4 * childCount + c.view;
  const stage = scope.childStage(retained + scratchBytes, "union routing");
  try {
    const counts = stage.allocate(
      scratchBytes,
      "union child counts",
      () => new Uint32Array(childCount),
    ).value;
    const tags = stage.allocate(length + c.view, "union tags", () => new Uint8Array(length));
    const offsets = stage.allocate(
      4 * length + c.view,
      "union offsets",
      () => new Uint32Array(length),
    );
    const sourceRows = stage.allocate(
      4 * length + c.view,
      "union source rows",
      () => new Uint32Array(length),
    );
    const childStarts = stage.allocate(
      4 * (childCount + 1) + c.view,
      "union child starts",
      () => new Uint32Array(childCount + 1),
    );
    const routing = stage.allocate(c.object + 4 * c.property, "union routing descriptor", () => {
      for (let row = 0; row < length; row++) {
        const tag = classify(row);
        if (!Number.isInteger(tag) || tag < 0 || tag >= childCount)
          throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid union child tag", { row, tag });
        tags.value[row] = tag;
        offsets.value[row] = counts[tag] ?? 0;
        counts[tag] = (counts[tag] ?? 0) + 1;
      }
      for (let child = 0; child < childCount; child++)
        childStarts.value[child + 1] = (childStarts.value[child] ?? 0) + (counts[child] ?? 0);
      for (let row = 0; row < length; row++)
        sourceRows.value[
          (childStarts.value[tags.value[row] ?? 0] ?? 0) + (offsets.value[row] ?? 0)
        ] = row;
      return {
        tags: tags.value,
        offsets: offsets.value,
        sourceRows: sourceRows.value,
        childStarts: childStarts.value,
      };
    });
    scope.adopt(tags.retain());
    scope.adopt(offsets.retain());
    const scratch = constructionScratch ?? scope;
    scratch.adopt(sourceRows.retain());
    scratch.adopt(childStarts.retain());
    return scratch.adopt(routing.retain()).value;
  } finally {
    stage.dispose();
  }
}
