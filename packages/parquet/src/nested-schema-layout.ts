import type { SchemaTree } from "hyparquet/src/types.js";
import { MATERIALIZATION_CHARGES as c, LakeqlError, type MemoryScope } from "lakeql-core";

export type NestedSchemaLayout =
  | { kind: "leaf" }
  | { kind: "struct"; children: readonly SchemaTree[] }
  | { kind: "list"; repeated: SchemaTree; element: SchemaTree }
  | { kind: "map"; entries: SchemaTree; key: SchemaTree; value: SchemaTree | undefined };

/** Normalize physical schema layouts using Parquet logical-type compatibility rules.
 * Schema nodes remain borrowed. Unknown group annotations decline before allocation.
 * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#nested-types
 */
export function nestedSchemaLayout(
  node: SchemaTree,
  parent: SchemaTree | undefined,
  scope: MemoryScope,
): NestedSchemaLayout | undefined {
  const converted = node.element.converted_type,
    logical = node.element.logical_type?.type;
  const parentMap =
    parent?.element.converted_type === "MAP" ||
    parent?.element.logical_type?.type === "MAP" ||
    parent?.element.converted_type === "MAP_KEY_VALUE";
  const list = converted === "LIST" || logical === "LIST";
  const map =
    converted === "MAP" || logical === "MAP" || (converted === "MAP_KEY_VALUE" && !parentMap);
  const invalid = () =>
    new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid nested container schema");
  if (list && map) throw invalid();
  if (list || map) {
    const repeated = node.children[0];
    if (
      node.element.type !== undefined ||
      node.children.length !== 1 ||
      !repeated ||
      repeated.element.repetition_type !== "REPEATED"
    )
      throw invalid();
    if (list) {
      const child = repeated.children[0];
      const preserveGroup =
        repeated.element.type !== undefined ||
        repeated.children.length > 1 ||
        child?.element.repetition_type === "REPEATED" ||
        repeated.element.name === "array" ||
        (repeated.element.name.length === node.element.name.length + 6 &&
          repeated.element.name.startsWith(node.element.name) &&
          repeated.element.name.endsWith("_tuple"));
      if (!preserveGroup && !child) throw invalid();
      return scope.allocate(
        c.object + 3 * c.property,
        "nested list layout",
        (): NestedSchemaLayout => ({
          kind: "list",
          repeated,
          element: preserveGroup ? repeated : (child as SchemaTree),
        }),
      ).value;
    }
    const key = repeated.children[0],
      value = repeated.children[1];
    if (
      node.element.repetition_type === "REPEATED" ||
      repeated.element.type !== undefined ||
      !key ||
      repeated.children.length > 2 ||
      (key.element.repetition_type !== "REQUIRED" && key.element.repetition_type !== "OPTIONAL") ||
      value?.element.repetition_type === "REPEATED"
    )
      throw invalid();
    return scope.allocate(
      c.object + 4 * c.property,
      "nested map layout",
      (): NestedSchemaLayout => ({
        kind: "map",
        entries: repeated,
        key,
        value,
      }),
    ).value;
  }
  if (node.element.type !== undefined) {
    if (node.children.length !== 0) throw invalid();
    return scope.allocate(
      c.object + c.property,
      "nested leaf layout",
      (): NestedSchemaLayout => ({ kind: "leaf" }),
    ).value;
  }
  if (logical !== undefined || (converted !== undefined && converted !== "MAP_KEY_VALUE"))
    return undefined;
  return scope.allocate(
    c.object + 2 * c.property,
    "nested struct layout",
    (): NestedSchemaLayout => ({
      kind: "struct",
      children: node.children,
    }),
  ).value;
}
