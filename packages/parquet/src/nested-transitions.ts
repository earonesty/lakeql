import type { ColumnDecoder } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";
import type { readNestedLevels } from "./nested-levels.js";

/** Validate Dremel repeated-ancestor continuity. Carry the returned definition level
 * into the next page; empty pages preserve it. Undefined means no preceding event.
 * Scratch depends on schema repetition depth, not page or row size.
 */
export function validateNestedTransitions(
  levels: ReturnType<typeof readNestedLevels>,
  schemaPath: ColumnDecoder["schemaPath"],
  previousDefinition: number | undefined,
  scope: MemoryScope,
): number | undefined {
  const invalid = () =>
    new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid nested level transition");
  let maxDefinition = 0;
  let maxRepetition = 0;
  for (const { element } of schemaPath) {
    if (element.repetition_type === "OPTIONAL" || element.repetition_type === "REPEATED")
      maxDefinition++;
    if (element.repetition_type === "REPEATED") maxRepetition++;
  }
  if (
    previousDefinition !== undefined &&
    (!Number.isInteger(previousDefinition) ||
      previousDefinition < 0 ||
      previousDefinition > maxDefinition)
  )
    throw invalid();
  const count = levels.repetition?.length ?? levels.definition?.length ?? levels.presentCount;
  if (
    !Number.isSafeInteger(count) ||
    count < 0 ||
    (levels.definition !== undefined && levels.definition.length !== count) ||
    (maxRepetition > 0 && levels.repetition === undefined && count > 0) ||
    (maxDefinition > 0 && levels.definition === undefined && count > 0)
  )
    throw invalid();
  const scratch = scope.childScope();
  try {
    const thresholds =
      maxRepetition === 0
        ? undefined
        : scratch.allocate(
            maxRepetition * 4 + c.view,
            "nested repeated ancestor thresholds",
            () => new Uint32Array(maxRepetition),
          ).value;
    let definition = 0;
    let repetition = 0;
    for (const { element } of schemaPath) {
      if (element.repetition_type === "OPTIONAL" || element.repetition_type === "REPEATED")
        definition++;
      if (element.repetition_type === "REPEATED" && thresholds)
        thresholds[repetition++] = definition;
    }
    let previous = previousDefinition;
    let rows = 0;
    let present = 0;
    for (let i = 0; i < count; i++) {
      const rep = levels.repetition?.[i] ?? 0;
      const def = levels.definition?.[i] ?? maxDefinition;
      if (rep > maxRepetition || def > maxDefinition) throw invalid();
      if (rep > 0) {
        const threshold = thresholds?.[rep - 1];
        if (
          threshold === undefined ||
          previous === undefined ||
          previous < threshold ||
          def < threshold
        )
          throw invalid();
      } else rows++;
      if (def === maxDefinition) present++;
      previous = def;
    }
    if (rows !== levels.rowsStarted || present !== levels.presentCount) throw invalid();
    return previous;
  } finally {
    scratch.dispose();
  }
}
