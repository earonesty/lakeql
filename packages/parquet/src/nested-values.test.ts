import type { SchemaElement } from "hyparquet";
import { getSchemaPath } from "hyparquet/src/schema.js";
import { expect, it } from "vitest";
import { nestedValueNormalizer } from "./nested-values.js";

const map: SchemaElement[] = [
  { name: "attrs", converted_type: "MAP", repetition_type: "OPTIONAL", num_children: 1 },
  { name: "key_value", repetition_type: "REPEATED", num_children: 2 },
  { name: "key", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "REQUIRED" },
  { name: "value", type: "INT32", repetition_type: "OPTIONAL" },
];
function converter(fields: SchemaElement[]) {
  const schema = [{ name: "root", num_children: 1 }, ...fields];
  const tree = getSchemaPath(schema, [fields[0]?.name ?? ""]).at(-1);
  if (!tree) throw new Error("missing schema");
  return nestedValueNormalizer(tree);
}
it("preserves empty maps, absent keys, null maps and null values inside lists", () => {
  const normalize = converter([
    { name: "items", converted_type: "LIST", repetition_type: "OPTIONAL", num_children: 1 },
    { name: "list", repetition_type: "REPEATED", num_children: 1 },
    ...map,
  ]);
  const source = [{}, { a: 1 }, null, { b: null }];
  expect(normalize?.(source)).toEqual([
    new Map(),
    new Map([["a", 1]]),
    null,
    new Map([["b", null]]),
  ]);
  expect(source).toEqual([{}, { a: 1 }, null, { b: null }]);
  expect(normalize?.(null)).toBeNull();
  expect(normalize?.([])).toEqual([]);
});
it("normalizes maps within repeated structs without inventing missing fields", () => {
  const normalize = converter([
    { name: "items", repetition_type: "REPEATED", num_children: 2 },
    { name: "id", type: "INT32" },
    ...map,
  ]);
  const source = [{ id: 1, attrs: {} }, { id: 2, attrs: { a: 3 } }, { id: 3 }, null];
  expect(normalize?.(source)).toEqual([
    { id: 1, attrs: new Map() },
    { id: 2, attrs: new Map([["a", 3]]) },
    { id: 3 },
    null,
  ]);
});
it("preserves existing map identity and skips scalar-only schemas", () => {
  const normalize = converter(map);
  const value = new Map([["a", 1]]);
  expect(normalize?.(value)).toBe(value);
  expect(normalize?.(undefined)).toBeUndefined();
  expect(converter([{ name: "value", type: "INT32" }])).toBeUndefined();
});
it("normalizes nested map values recursively", () => {
  const normalize = converter([
    { name: "outer", converted_type: "MAP", repetition_type: "OPTIONAL", num_children: 1 },
    { name: "key_value", repetition_type: "REPEATED", num_children: 2 },
    { name: "key", type: "BYTE_ARRAY", converted_type: "UTF8", repetition_type: "REQUIRED" },
    { ...map[0], name: "value" },
    ...map.slice(1),
  ]);
  expect(normalize?.({ a: {}, b: { c: null } })).toEqual(
    new Map([
      ["a", new Map()],
      ["b", new Map([["c", null]])],
    ]),
  );
});
