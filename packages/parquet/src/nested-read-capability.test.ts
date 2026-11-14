import { parquetMetadata } from "hyparquet";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { nestedListFile } from "./nested-list-worker-fixture.js";
import { canReadNestedRowGroupBatches } from "./nested-read-capability.js";

it("preflights selected schema and columns with atomic planning cleanup", () => {
  const metadata = parquetMetadata(nestedListFile.slice().buffer);
  const scope = new MemoryScope(new MemoryBudget(65536));
  try {
    expect(canReadNestedRowGroupBatches(metadata, { batchSize: 2 }, scope)).toBe(true);
    expect(scope.budget.usedBytes).toBe(0);
    for (const columns of [[], ["missing"]]) {
      expect(canReadNestedRowGroupBatches(metadata, { batchSize: 2, columns }, scope)).toBe(false);
      expect(scope.budget.usedBytes).toBe(0);
    }
    const column = metadata.row_groups[0]?.columns[0]?.meta_data;
    if (!column) throw Error("Missing column");
    column.codec = "LZO";
    expect(canReadNestedRowGroupBatches(metadata, { batchSize: 2, columns: ["a"] }, scope)).toBe(
      false,
    );
    expect(canReadNestedRowGroupBatches(metadata, { batchSize: 2, columns: ["b"] }, scope)).toBe(
      true,
    );
    expect(scope.budget.usedBytes).toBe(0);
  } finally {
    scope.dispose();
  }
});

it("does not turn budget or malformed-schema failures into capability fallback", () => {
  const metadata = parquetMetadata(nestedListFile.slice().buffer);
  const denied = new MemoryScope(new MemoryBudget(0));
  expect(() => canReadNestedRowGroupBatches(metadata, { batchSize: 2 }, denied)).toThrow(/budget/i);
  expect(denied.budget.usedBytes).toBe(0);
  denied.dispose();
  const scope = new MemoryScope(new MemoryBudget(65536));
  try {
    expect(() =>
      canReadNestedRowGroupBatches({ ...metadata, schema: [] }, { batchSize: 2 }, scope),
    ).toThrow(/root/);
    expect(scope.budget.usedBytes).toBe(0);
  } finally {
    scope.dispose();
  }
});
