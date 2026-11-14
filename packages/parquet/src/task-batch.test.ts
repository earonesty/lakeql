import { batchFromColumns, MemoryBudget, MemoryScope, materializeBatchRows } from "lakeql-core";
import { describe, expect, it } from "vitest";
import { batchWithPartitionValues, projectTaskBatch } from "./task-batch.js";

describe("task vector transformations", () => {
  it("reuses physical vectors and stores repeated partition text once", () => {
    const budget = new MemoryBudget(64 * 1024);
    const scope = new MemoryScope(budget);
    const input = batchFromColumns({ id: Array.from({ length: 256 }, (_, i) => i) });
    const text = "界".repeat(4096);
    try {
      const augmented = batchWithPartitionValues(
        input,
        { partition: text, id: "shadow", ...JSON.parse('{"__proto__":"safe"}') },
        scope,
      );
      const result = projectTaskBatch(
        augmented,
        ["id", "partition", "__proto__", "missing", "id"],
        scope,
      );
      expect(result.columns.id).toBe(input.columns.id);
      expect(result.columns.partition?.type).toBe("dict");
      expect(materializeBatchRows(result)[255]).toEqual({
        id: 255,
        partition: text,
        ["__proto__"]: "safe",
        missing: null,
      });
      expect(budget.usedBytes).toBeLessThan(64 * 1024);
    } finally {
      scope.dispose();
    }
    expect(budget.usedBytes).toBe(0);
  });

  it("releases failed partition vector admission with the batch scope", () => {
    const budget = new MemoryBudget(1024);
    const scope = new MemoryScope(budget);
    try {
      expect(() =>
        batchWithPartitionValues(batchFromColumns({ id: [1] }), { p: "x".repeat(4096) }, scope),
      ).toThrow();
    } finally {
      scope.dispose();
    }
    expect(budget.usedBytes).toBe(0);
  });
});

it("scans a real Parquet partition column without expanding repeated text", async () => {
  const { memoryStore } = await import("lakeql-core");
  const { writeParquet, scanParquetTaskColumnBatches } = await import("./index.js");
  const store = memoryStore();
  await writeParquet(store, "partition-vector.parquet", {
    rowGroupSize: [256],
    columnData: [{ name: "id", type: "INT32", data: Array.from({ length: 256 }, (_, i) => i) }],
  });
  const budget = new MemoryBudget(128 * 1024);
  const text = "界".repeat(4096);
  let count = 0;
  for await (const { batch } of scanParquetTaskColumnBatches(
    store,
    {
      path: "partition-vector.parquet",
      rowGroupRanges: [{ start: 0, end: 1 }],
      projectedColumns: ["id", "partition"],
      partitionValues: { partition: text },
    },
    { memoryBudget: budget, batchSize: 256 },
  )) {
    expect(batch.columns.partition?.type).toBe("dict");
    expect(materializeBatchRows(batch)[0]?.partition).toBe(text);
    count += batch.rowCount;
  }
  expect(count).toBe(256);
  expect(budget.usedBytes).toBe(0);
});
