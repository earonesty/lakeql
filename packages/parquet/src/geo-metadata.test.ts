import type { SchemaElement } from "hyparquet";
import { MATERIALIZATION_CHARGES as c, MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { markGeoMetadata } from "./geo-metadata.js";
import { jsonAllocationBytes } from "./json-allocation.js";

const schema = (): SchemaElement[] => [
  { name: "root", num_children: 1 },
  { name: "geometry", type: "BYTE_ARRAY" },
];
it("retains only annotations after releasing parsed GeoParquet JSON", () => {
  const text = JSON.stringify({
    columns: {
      geometry: {
        encoding: "WKB",
        edges: "spherical",
        crs: { id: { authority: "EPSG", code: "4326" } },
      },
    },
    unused: Array(100).fill("ignored"),
  });
  const retained = c.object + 2 * c.property + c.string + 9 * c.stringCodeUnit;
  const peak = jsonAllocationBytes(text) + retained;
  const scope = new MemoryScope(new MemoryBudget(peak));
  const elements = schema();
  markGeoMetadata(elements, [{ key: "geo", value: text }], scope);
  expect(elements[1]?.logical_type).toEqual({ type: "GEOGRAPHY", crs: "EPSG:4326" });
  expect(scope.budget.usedBytes).toBe(retained);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(jsonAllocationBytes(text) - 1));
  const spy = vi.spyOn(JSON, "parse");
  try {
    expect(() => markGeoMetadata(schema(), [{ key: "geo", value: text }], tight)).toThrow(/budget/);
    expect(spy).not.toHaveBeenCalled();
    expect(tight.budget.usedBytes).toBe(0);
  } finally {
    spy.mockRestore();
    tight.dispose();
  }
});
it("skips complete nested subtrees and preserves existing annotations", () => {
  const elements: SchemaElement[] = [
    { name: "root", num_children: 3 },
    { name: "group", num_children: 1 },
    { name: "nested", num_children: 1 },
    { name: "geometry", type: "BYTE_ARRAY" },
    { name: "geometry", type: "BYTE_ARRAY" },
    { name: "already", type: "BYTE_ARRAY", logical_type: { type: "STRING" } },
  ];
  markGeoMetadata(elements, [
    {
      key: "geo",
      value: JSON.stringify({
        columns: { geometry: { encoding: "WKB" }, already: { encoding: "WKB" } },
      }),
    },
  ]);
  expect(elements[3]?.logical_type).toBeUndefined();
  expect(elements[4]?.logical_type).toEqual({ type: "GEOMETRY", crs: undefined });
  expect(elements[5]?.logical_type).toEqual({ type: "STRING" });
});
it("uses no allocation when geo metadata is absent", () => {
  const scope = new MemoryScope(new MemoryBudget(0));
  markGeoMetadata(schema(), [], scope);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
