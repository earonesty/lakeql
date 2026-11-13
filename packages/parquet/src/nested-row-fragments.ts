import { getMaxDefinitionLevel, getMaxRepetitionLevel } from "hyparquet/src/schema.js";
import type { ColumnDecoder } from "hyparquet/src/types.js";
import {
  MATERIALIZATION_CHARGES as c,
  LakeqlError,
  type MemoryScope,
  planUnionConcat,
  type Vector,
  vectorLength,
} from "lakeql-core";
import type { NestedColumnRow, NestedRowFragment } from "./nested-column-rows.js";

/** Join sealed row level metadata and route into shared compact payloads. The row owner
 * must remain live. All new level/routing storage is admitted; no row objects are decoded.
 */
export function joinNestedRowFragments(
  row: NestedColumnRow,
  schemaPath: ColumnDecoder["schemaPath"],
  scope: MemoryScope,
) {
  return joinNestedFragments(row.fragments, row.rowCount ?? 1, schemaPath, scope);
}

/** Join a bounded sequence of complete rows; adjacent spans of one page share
 * one payload route. Input fragment/page owners must remain live. */
export function joinNestedFragments(
  fragments: readonly NestedRowFragment[],
  rowCount: number,
  schemaPath: ColumnDecoder["schemaPath"],
  scope: MemoryScope,
) {
  const invalid = () => new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid sealed nested row");
  if (!Number.isSafeInteger(rowCount) || rowCount < 1) throw invalid();
  const maxDef = getMaxDefinitionLevel(schemaPath),
    maxRep = getMaxRepetitionLevel(schemaPath);
  let count = 0,
    present = 0,
    starts = 0,
    runs = 0;
  let previous: NestedRowFragment | undefined;
  for (const fragment of fragments) {
    if (!contiguous(previous, fragment)) runs++;
    previous = fragment;
    const levels = fragment.page.physical.levels;
    const available = levels.repetition?.length ?? levels.definition?.length ?? levels.presentCount;
    if (
      ![fragment.eventStart, fragment.eventEnd, fragment.valueStart, fragment.valueEnd].every(
        Number.isSafeInteger,
      ) ||
      fragment.eventStart < 0 ||
      fragment.eventEnd <= fragment.eventStart ||
      fragment.eventEnd > available ||
      fragment.valueStart < 0 ||
      fragment.valueEnd < fragment.valueStart ||
      fragment.valueEnd > vectorLength(fragment.page.values)
    )
      throw invalid();
    let values = 0;
    for (let i = fragment.eventStart; i < fragment.eventEnd; i++) {
      const rep = levels.repetition?.[i] ?? 0,
        def = levels.definition?.[i] ?? maxDef;
      if (rep > maxRep || def > maxDef) throw invalid();
      if (count + i - fragment.eventStart === 0 && rep !== 0) throw invalid();
      if (rep === 0) starts++;
      if (def === maxDef) values++;
    }
    if (values !== fragment.valueEnd - fragment.valueStart) throw invalid();
    count += fragment.eventEnd - fragment.eventStart;
    present += values;
  }
  if (starts !== rowCount || count > 0xffffffff || present > 0xffffffff) throw invalid();
  const construction = scope.childScope();
  try {
    const allocateLevels = (maximum: number) =>
      maximum === 0
        ? undefined
        : construction.allocate(
            count * (maximum <= 255 ? 1 : 4) + c.view,
            "sealed row levels",
            () => (maximum <= 255 ? new Uint8Array(count) : new Uint32Array(count)),
          ).value;
    const repetition = allocateLevels(maxRep),
      definition = allocateLevels(maxDef);
    const vectors = construction.allocate(
      c.array + runs * c.arraySlot,
      "sealed row payload directory",
      () => new Array<Vector>(runs),
    ).value;
    let event = 0;
    for (const fragment of fragments) {
      if (!fragment) throw invalid();
      const levels = fragment.page.physical.levels;
      for (let i = fragment.eventStart; i < fragment.eventEnd; i++, event++) {
        if (repetition) repetition[event] = levels.repetition?.[i] ?? 0;
        if (definition) definition[event] = levels.definition?.[i] ?? maxDef;
      }
    }
    let slot = 0;
    for (let f = 0; f < fragments.length; f++) {
      const fragment = fragments[f];
      if (!fragment) throw invalid();
      let last = fragment;
      while (contiguous(last, fragments[f + 1])) {
        last = fragments[++f] as NestedRowFragment;
      }
      const valueEnd = last.valueEnd;
      if (fragment.valueStart === 0 && valueEnd === vectorLength(fragment.page.values)) {
        vectors[slot] = fragment.page.values;
      } else {
        const indices = construction.allocate(
          (valueEnd - fragment.valueStart) * 4 + c.view,
          "sealed row payload indices",
          () =>
            Uint32Array.from(
              { length: valueEnd - fragment.valueStart },
              (_, i) => fragment.valueStart + i,
            ),
        ).value;
        vectors[slot] = construction.allocate(
          c.object + 3 * c.property,
          "sealed row payload routing",
          (): Vector => ({
            type: "dict",
            indices,
            dictionary: fragment.page.values,
          }),
        ).value;
      }
      slot++;
    }
    const values = planUnionConcat(vectors).execute(construction);
    const levels = construction.allocate(
      c.object + 5 * c.property,
      "sealed row level descriptor",
      () => ({
        repetition,
        definition,
        rowsStarted: rowCount,
        presentCount: present,
        valueOffset: 0,
      }),
    ).value;
    const result = construction.allocate(c.object + 2 * c.property, "joined nested row", () => ({
      levels,
      values,
    })).value;
    scope.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    construction.dispose();
  }
}

function contiguous(
  left: NestedRowFragment | undefined,
  right: NestedRowFragment | undefined,
): boolean {
  return (
    left !== undefined &&
    right !== undefined &&
    left.page === right.page &&
    left.eventEnd === right.eventStart &&
    left.valueEnd === right.valueStart
  );
}
