import { getMaxDefinitionLevel } from "hyparquet/src/schema.js";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import {
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type MemoryScope,
  type Vector,
  vectorLength,
} from "lakeql-core";
import type { readNestedLevels } from "./nested-levels.js";

/** Expand optional leaf nulls only where the parent exists. Absent repeated elements
 * and absent ancestors produce no leaf slot. Compact payload remains borrowed.
 */
export function nestedLeafSlots(
  levels: ReturnType<typeof readNestedLevels>,
  schemaPath: ColumnDecoder["schemaPath"],
  values: Vector,
  scope: MemoryScope,
): Vector {
  const invalid = () => new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid nested leaf slots");
  const leaf = schemaPath.at(-1)?.element;
  if (leaf === undefined) throw invalid();
  const maximum = getMaxDefinitionLevel(schemaPath);
  const threshold = maximum - Number(leaf.repetition_type === "OPTIONAL");
  const count = levels.definition?.length ?? levels.repetition?.length ?? levels.presentCount;
  if (levels.repetition !== undefined && levels.repetition.length !== count) throw invalid();
  let slots = 0;
  let present = 0;
  for (let i = 0; i < count; i++) {
    const def = levels.definition?.[i] ?? maximum;
    if (def > maximum) throw invalid();
    if (def >= threshold) slots++;
    if (def === maximum) present++;
  }
  if (present !== levels.presentCount || present !== vectorLength(values)) throw invalid();
  if (slots === present) return values;
  const construction = scope.childScope();
  try {
    const indices = construction.allocate(
      slots * 4 + c.view,
      "nested leaf slot indices",
      () => new Uint32Array(slots),
    ).value;
    const valid = construction.allocate(
      slots + c.view,
      "nested leaf slot validity",
      () => new Uint8Array(slots),
    ).value;
    let slot = 0;
    let value = 0;
    for (let i = 0; i < count; i++) {
      const def = levels.definition?.[i] ?? maximum;
      if (def < threshold) continue;
      if (def === maximum) {
        indices[slot] = value++;
        valid[slot] = 1;
      }
      slot++;
    }
    const output = construction.allocate(
      c.object + 4 * c.property,
      "nested leaf slot vector",
      (): Vector => ({
        type: "dict",
        indices,
        valid,
        dictionary: values,
      }),
    ).value;
    scope.adopt(construction.retainAllocations(undefined));
    return output;
  } finally {
    construction.dispose();
  }
}
