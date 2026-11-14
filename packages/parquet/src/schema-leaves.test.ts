import type { SchemaElement } from "hyparquet";
import { expect, it } from "vitest";
import { schemaLeafIndex } from "./schema-leaves.js";

it("indexes nested physical leaves without conflating dotted names", () => {
  const schema: SchemaElement[] = [
    { name: "root", num_children: 3 },
    { name: "a", num_children: 2 },
    { name: "b", type: "INT32" },
    { name: "c", num_children: 1 },
    { name: "d", type: "INT64" },
    { name: "a.b", type: "INT32" },
    { name: "tail", type: "DOUBLE" },
  ];
  const index = schemaLeafIndex(schema);
  expect([...index.keys()]).toEqual(['["a","b"]', '["a","c","d"]', '["a.b"]', '["tail"]']);
  expect(index.get('["a","c","d"]')).toBe(schema[4]);
});

it("rejects incomplete, excess, duplicate and malformed schema entries", () => {
  for (const schema of [
    [],
    [{ name: "root", num_children: 1 }],
    [{ name: "root" }, { name: "x", type: "INT32" }],
    [
      { name: "root", num_children: 2 },
      { name: "x", type: "INT32" },
      { name: "x", type: "INT32" },
    ],
    [{ name: "root", num_children: -1 }],
    [{ name: "root", num_children: 1.5 }],
  ] as SchemaElement[][])
    expect(() => schemaLeafIndex(schema)).toThrow(/schema tree/);
});

it("indexes a wide schema in one pass and avoids recursive stack growth", () => {
  const width = 10000;
  const schema: SchemaElement[] = [{ name: "root", num_children: width }];
  for (let i = 0; i < width; i++) schema.push({ name: `x${i}`, type: "INT32" });
  expect(schemaLeafIndex(schema).size).toBe(width);
  const nested: SchemaElement[] = Array.from({ length: 10000 }, (_, i) => ({
    name: `g${i}`,
    num_children: 1,
  }));
  nested.push({ name: "leaf", type: "INT32" });
  expect(schemaLeafIndex(nested).size).toBe(1);
});
