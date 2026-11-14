import type { SchemaElement } from "hyparquet";
import { getSchemaPath } from "hyparquet/src/schema.js";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { schemaPathIndex } from "./schema-path-index.js";

it("matches upstream trees, subtree counts and paths while reusing nodes", () => {
  const schema: SchemaElement[] = [
    { name: "root", num_children: 2 },
    { name: "list", num_children: 2 },
    { name: "x", type: "INT32", repetition_type: "REPEATED" },
    { name: "nested", num_children: 1 },
    { name: "y", type: "INT64" },
    { name: "list.x", type: "DOUBLE" },
  ];
  const lookup = schemaPathIndex(schema);
  for (const path of [[], ["list"], ["list", "x"], ["list", "nested", "y"], ["list.x"]]) {
    expect(lookup(path)).toEqual(getSchemaPath(schema, path));
    expect(lookup(path)[0]).toBe(lookup([])[0]);
  }
  expect(() => lookup(["missing"])).toThrow(/not found/);
});

it("rejects incomplete, excess and ambiguous schema trees", () => {
  for (const schema of [
    [],
    [{ name: "root", num_children: 1 }],
    [{ name: "root" }, { name: "x" }],
    [{ name: "root", num_children: 2 }, { name: "x" }, { name: "x" }],
    [{ name: "root", num_children: NaN }],
  ] as SchemaElement[][])
    expect(() => schemaPathIndex(schema)).toThrow();
});

it("constructs deep trees without retaining every ancestor prefix", () => {
  const depth = 10000;
  const schema: SchemaElement[] = Array.from({ length: depth }, (_, index) => ({
    name: `g${index}`,
    num_children: 1,
  }));
  schema.push({ name: "value", type: "INT32" });
  const lookup = schemaPathIndex(schema);
  const names = schema.slice(1).map((element) => element.name);
  const nodes = lookup(names);
  expect(nodes).toHaveLength(depth + 1);
  expect(nodes[0]?.count).toBe(depth + 1);
  const leaf = nodes.at(-1);
  expect(leaf?.path).toEqual(names);
  // No path array is memoized on a node, nor can caller mutation affect peers.
  const path = leaf?.path;
  path?.pop();
  expect(leaf?.path).toEqual(names);
  for (const node of nodes)
    expect(Object.getOwnPropertyDescriptor(node, "path")?.get).toBeTypeOf("function");
});

it("admits schema construction at the exact peak and rolls back failures", () => {
  const schema: SchemaElement[] = [
    { name: "root", num_children: 2 },
    { name: "a", type: "INT32" },
    { name: "b", type: "INT64" },
  ];
  const baseline = new MemoryScope(new MemoryBudget(65536));
  const lookup = schemaPathIndex(schema, baseline);
  expect(lookup(["b"])[1]?.element.type).toBe("INT64");
  const peak = baseline.budget.peakBytes;
  expect(baseline.budget.usedBytes).toBeGreaterThan(0);
  expect(baseline.budget.usedBytes).toBeLessThan(peak);
  baseline.dispose();
  expect(baseline.budget.usedBytes).toBe(0);
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(schemaPathIndex(schema, exact)([])[0]?.count).toBe(3);
  exact.dispose();
  expect(exact.budget.usedBytes).toBe(0);
  const short = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => schemaPathIndex(schema, short)).toThrow();
  expect(short.budget.usedBytes).toBe(0);
  short.dispose();
  const malformed = new MemoryScope(new MemoryBudget(65536));
  expect(() => schemaPathIndex([{ name: "root", num_children: 1 }], malformed)).toThrow(
    /Incomplete/,
  );
  expect(malformed.budget.usedBytes).toBe(0);
  malformed.dispose();
});
