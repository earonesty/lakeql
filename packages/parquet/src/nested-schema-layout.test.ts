import { getSchemaPath } from "hyparquet/src/schema.js";
import type { SchemaElement } from "hyparquet/src/types.js";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { nestedSchemaLayout } from "./nested-schema-layout.js";

it.each([
  { name: "list", fields: 1, repeatedChild: false, unwrap: true },
  { name: "array", fields: 1, repeatedChild: false, unwrap: false },
  { name: "items_tuple", fields: 1, repeatedChild: false, unwrap: false },
  { name: "list", fields: 2, repeatedChild: false, unwrap: false },
  { name: "list", fields: 1, repeatedChild: true, unwrap: false },
])("recognizes legacy list shape $name/$fields/$repeatedChild", ({
  name,
  fields,
  repeatedChild,
  unwrap,
}) => {
  const schema: SchemaElement[] = [
    { name: "root", num_children: 1 },
    { name: "items", num_children: 1, repetition_type: "OPTIONAL", logical_type: { type: "LIST" } },
    { name, num_children: fields, repetition_type: "REPEATED" },
    { name: "a", type: "INT32", repetition_type: repeatedChild ? "REPEATED" : "OPTIONAL" },
    ...(fields === 2
      ? [{ name: "b", type: "INT32" as const, repetition_type: "REQUIRED" as const }]
      : []),
  ];
  const path = getSchemaPath(schema, ["items"]);
  const node = path[1];
  if (!node) throw Error("Missing node");
  const scope = new MemoryScope(new MemoryBudget(4096));
  const layout = nestedSchemaLayout(node, path[0], scope);
  if (layout?.kind !== "list") throw Error("Missing list layout");
  expect(layout.element).toBe(unwrap ? node.children[0]?.children[0] : node.children[0]);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => nestedSchemaLayout(node, path[0], tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it("recognizes map keys by position, accepts omitted values/optional keys, and rejects repeated keys", () => {
  const schema: SchemaElement[] = [
    { name: "root", num_children: 1 },
    { name: "m", converted_type: "MAP_KEY_VALUE", repetition_type: "OPTIONAL", num_children: 1 },
    { name: "legacy_entries", repetition_type: "REPEATED", num_children: 1 },
    { name: "legacy_key", type: "INT32", repetition_type: "REQUIRED" },
  ];
  const path = getSchemaPath(schema, ["m"]);
  const node = path[1];
  if (!node) throw Error("Missing map");
  const scope = new MemoryScope(new MemoryBudget(4096));
  const layout = nestedSchemaLayout(node, path[0], scope);
  expect(layout?.kind).toBe("map");
  if (layout?.kind !== "map") throw Error("Missing map layout");
  expect(layout.value).toBeUndefined();
  expect(layout.key.element.name).toBe("legacy_key");
  scope.dispose();
  layout.key.element.repetition_type = "OPTIONAL";
  const optional = new MemoryScope(new MemoryBudget(4096));
  expect(nestedSchemaLayout(node, path[0], optional)?.kind).toBe("map");
  optional.dispose();
  layout.key.element.repetition_type = "REPEATED";
  const rejected = new MemoryScope(new MemoryBudget(0));
  expect(() => nestedSchemaLayout(node, path[0], rejected)).toThrow(/Invalid/);
  expect(rejected.budget.peakBytes).toBe(0);
  rejected.dispose();
});
