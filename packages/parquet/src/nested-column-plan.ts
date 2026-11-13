import type { RowGroup, SchemaTree } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES as c, type MemoryScope } from "lakeql-core";
import type { Int96Interpretation, TemporalColumnDecoder } from "./int96-interpretation.js";
import type { NestedProjectedColumn } from "./nested-projected-batches.js";
import { lakeqlParquetParsers } from "./parsers.js";

/** Admit selected decoder descriptors and ancestor paths before building them. */
export function nestedColumnPlan(
  group: RowGroup,
  lookup: (path: readonly string[]) => SchemaTree[],
  names: readonly string[],
  scope: MemoryScope,
  int96Interpretation: Int96Interpretation = "julian-nanos",
): NestedProjectedColumn[] {
  let count = 0;
  for (const chunk of group.columns)
    if (chunk.meta_data && names.includes(chunk.meta_data.path_in_schema[0] ?? "")) count++;
  const columns = scope.allocate(
    c.array + count * (c.arraySlot + c.object * 2 + 9 * c.property),
    "nested row-group descriptors",
    () => [] as NestedProjectedColumn[],
  ).value;
  for (const chunk of group.columns) {
    const column = chunk.meta_data;
    if (!column || !names.includes(column.path_in_schema[0] ?? "")) continue;
    const schemaPath = scope.allocate(
      c.array + (column.path_in_schema.length + 1) * c.arraySlot,
      "nested decoder path",
      () => lookup(column.path_in_schema),
    ).value;
    const element = schemaPath.at(-1)?.element;
    if (!element) throw Error("Missing nested leaf");
    const decoder: TemporalColumnDecoder = {
      int96Interpretation,
      type: column.type,
      codec: column.codec,
      element,
      schemaPath,
      pathInSchema: column.path_in_schema,
      parsers: lakeqlParquetParsers,
    };
    columns.push({ metadata: column, decoder });
  }
  return columns;
}
