import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";
import type { readNestedLevels } from "./nested-levels.js";

/** Index page-local top-level row segments, preserving a possible leading continuation.
 * Offsets address level events and compact present values respectively. The terminal
 * segment may continue on the next page; only the column/page coordinator can seal it.
 */
export function indexNestedRows(
  levels: ReturnType<typeof readNestedLevels>,
  maxDefinition: number,
  scope: MemoryScope,
) {
  const invalid = () => new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid nested row levels");
  const count = levels.repetition?.length ?? levels.definition?.length ?? levels.presentCount;
  if (
    !Number.isSafeInteger(maxDefinition) ||
    maxDefinition < 0 ||
    maxDefinition > 0xffffffff ||
    !Number.isSafeInteger(count) ||
    count < 0 ||
    count > 0xffffffff ||
    (levels.definition !== undefined && levels.definition.length !== count)
  )
    throw invalid();
  let rowsStarted = 0;
  let present = 0;
  // Validate before allocation. A continuation is a segment, not a newly started row.
  for (let i = 0; i < count; i++) {
    if ((levels.repetition?.[i] ?? 0) === 0) rowsStarted++;
    const def = levels.definition?.[i] ?? maxDefinition;
    if (def > maxDefinition) throw invalid();
    if (def === maxDefinition) present++;
  }
  if (rowsStarted !== levels.rowsStarted || present !== levels.presentCount) throw invalid();
  const continuesPrevious = count > 0 && (levels.repetition?.[0] ?? 0) !== 0;
  const segments = rowsStarted + Number(continuesPrevious);
  const construction = scope.childScope();
  try {
    const allocate = () =>
      construction.allocate(
        (segments + 1) * 4 + c.view,
        "nested row offsets",
        () => new Uint32Array(segments + 1),
      ).value;
    const eventOffsets = allocate();
    const valueOffsets = allocate();
    let segment = 0;
    let value = 0;
    for (let i = 0; i < count; i++) {
      if (i > 0 && (levels.repetition?.[i] ?? 0) === 0) {
        segment++;
        eventOffsets[segment] = i;
        valueOffsets[segment] = value;
      }
      if ((levels.definition?.[i] ?? maxDefinition) === maxDefinition) value++;
    }
    eventOffsets[segments] = count;
    valueOffsets[segments] = value;
    const result = construction.allocate(c.object + 3 * c.property, "nested row index", () => ({
      eventOffsets,
      valueOffsets,
      continuesPrevious,
    })).value;
    scope.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    construction.dispose();
  }
}
