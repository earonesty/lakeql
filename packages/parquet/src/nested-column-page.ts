import { getMaxDefinitionLevel } from "hyparquet/src/schema.js";
import type { ColumnDecoder, PageHeader } from "hyparquet/src/types.js";
import {
  MATERIALIZATION_CHARGES as c,
  type MemoryScope,
  type Vector,
  vectorLength,
} from "lakeql-core";
import { nestedLeafSlots } from "./nested-leaf-slots.js";
import { nestedLeafVector } from "./nested-leaf-vector.js";
import { readNestedPage } from "./nested-page.js";
import { indexNestedRows } from "./nested-row-index.js";
import { validateNestedTransitions } from "./nested-transitions.js";

/** Atomic physical/logical column-page step. Commit nextDefinition only on success.
 * Input bytes and a supplied logical dictionary remain separately caller-owned.
 * Unsupported physical or logical capabilities return undefined with no retained allocation.
 */
export function decodeCompactNestedColumnPage(
  bytes: Uint8Array,
  header: PageHeader,
  decoder: ColumnDecoder,
  previousDefinition: number | undefined,
  scope: MemoryScope,
  dictionary?: Vector,
) {
  const construction = scope.childScope();
  try {
    const physical = readNestedPage(
      bytes,
      header,
      decoder,
      previousDefinition === undefined,
      construction,
      dictionary === undefined ? undefined : vectorLength(dictionary),
    );
    if (physical === undefined) return undefined;
    const nextDefinition = validateNestedTransitions(
      physical.levels,
      decoder.schemaPath,
      previousDefinition,
      construction,
    );
    const rows = indexNestedRows(
      physical.levels,
      getMaxDefinitionLevel(decoder.schemaPath),
      construction,
    );
    const values = nestedLeafVector(physical, decoder, construction, dictionary);
    if (values === undefined) return undefined;
    const result = construction.allocate(c.object + 4 * c.property, "nested column page", () => ({
      physical,
      rows,
      values,
      nextDefinition,
    })).value;
    scope.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    construction.dispose();
  }
}

/** Leaf-slot expansion is an explicit capability for callers that consume leaf
 * vectors directly. Schema assembly consumes compact values and physical levels. */
export function decodeNestedColumnPage(...args: Parameters<typeof decodeCompactNestedColumnPage>) {
  const [bytes, header, decoder, previousDefinition, scope, dictionary] = args;
  const construction = scope.childScope();
  try {
    const page = decodeCompactNestedColumnPage(
      bytes,
      header,
      decoder,
      previousDefinition,
      construction,
      dictionary,
    );
    if (!page) return undefined;
    const slots = nestedLeafSlots(
      page.physical.levels,
      decoder.schemaPath,
      page.values,
      construction,
    );
    const result = construction.allocate(
      c.object + 5 * c.property,
      "expanded nested column page",
      () => ({ ...page, slots }),
    ).value;
    scope.adopt(construction.retainAllocations(undefined));
    return result;
  } finally {
    construction.dispose();
  }
}
