import type { ColumnDecoder } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";
import type { readNestedLevels } from "./nested-levels.js";

/** Page-local occurrences of one schema node, suitable for parent container assembly.
 * Optional nodes include absent/null slots when their parent exists; repeated nodes
 * include only actual elements. A leading occurrence can continue from a prior page.
 * An optional same-budget validity scope pins only the validity buffer for output.
 * Event offsets are routing boundaries, not proof that the last occurrence is sealed.
 */
export function indexNestedNode(
  levels: ReturnType<typeof readNestedLevels>,
  schemaPath: ColumnDecoder["schemaPath"],
  depth: number,
  scope: MemoryScope,
  validityScope?: MemoryScope,
) {
  const invalid = () => new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid nested schema node");
  if (!Number.isInteger(depth) || depth < 0 || depth >= schemaPath.length) throw invalid();
  let definition = 0;
  let repetition = 0;
  let parentDefinition = 0;
  for (let i = 0; i <= depth; i++) {
    parentDefinition = definition;
    const kind = schemaPath[i]?.element.repetition_type;
    if (kind === "REPEATED" || kind === "OPTIONAL") definition++;
    if (kind === "REPEATED") repetition++;
  }
  const repeated = schemaPath[depth]?.element.repetition_type === "REPEATED";
  const threshold = repeated ? definition : parentDefinition;
  const count = levels.repetition?.length ?? levels.definition?.length ?? levels.presentCount;
  if (
    !Number.isSafeInteger(count) ||
    count < 0 ||
    count > 0xffffffff ||
    (levels.definition !== undefined && levels.definition.length !== count)
  )
    throw invalid();
  // Call after schema transition validation. Deeper repetition continues the same node.
  const starts = (i: number) =>
    (i === 0 || (levels.repetition?.[i] ?? 0) <= repetition) &&
    (levels.definition?.[i] ?? definition) >= threshold;
  let slots = 0;
  for (let i = 0; i < count; i++) if (starts(i)) slots++;
  const construction = scope.childScope();
  try {
    const eventOffsets = construction.allocate(
      (slots + 1) * 4 + c.view,
      "nested node event offsets",
      () => new Uint32Array(slots + 1),
    ).value;
    const valid = construction.allocate(
      slots + c.view,
      "nested node validity",
      () => new Uint8Array(slots),
    ).value;
    let slot = 0;
    for (let i = 0; i < count; i++)
      if (starts(i)) {
        eventOffsets[slot] = i;
        valid[slot++] = Number((levels.definition?.[i] ?? definition) >= definition);
      }
    eventOffsets[slots] = count;
    const continuesPrevious =
      slots > 0 && eventOffsets[0] === 0 && (levels.repetition?.[0] ?? 0) > repetition;
    const result = construction.allocate(c.object + 3 * c.property, "nested node index", () => ({
      eventOffsets,
      valid,
      continuesPrevious,
    })).value;
    if (validityScope) {
      const pin = construction.retainAllocation(valid);
      try {
        validityScope.adopt(pin);
      } catch (error) {
        pin.release();
        throw error;
      }
    }
    scope.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    construction.dispose();
  }
}
