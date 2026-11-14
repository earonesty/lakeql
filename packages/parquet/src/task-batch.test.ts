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

it.each([
  false,
  true,
])("scans a real Parquet partition column with scalar residual %s", async (scalar) => {
  const { memoryStore, like } = await import("lakeql-core");
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
      ...(scalar ? { residualPredicate: like("partition", "界%") } : {}),
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

it.each([
  "vector",
  "scalar",
  "empty",
] as const)("filters partition vectors through the %s residual path", async (kind) => {
  const { memoryStore, eq, like } = await import("lakeql-core");
  const { writeParquet, scanParquetTaskColumnBatches } = await import("./index.js");
  const store = memoryStore();
  await writeParquet(store, "partition-filter.parquet", {
    rowGroupSize: [3],
    columnData: [{ name: "id", type: "INT32", data: [1, 2, 3] }],
  });
  const budget = new MemoryBudget(128 * 1024);
  const iterator = scanParquetTaskColumnBatches(
    store,
    {
      path: "partition-filter.parquet",
      rowGroupRanges: [{ start: 0, end: 1 }],
      projectedColumns: ["id", "partition"],
      partitionValues: { partition: "west" },
      residualPredicate:
        kind === "scalar"
          ? like("partition", "w%")
          : eq("partition", kind === "empty" ? "east" : "west"),
    },
    { memoryBudget: budget, batchSize: 3 },
  )[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (kind === "empty") expect(first.done).toBe(true);
  else {
    expect(first.done).toBe(false);
    if (first.done) throw new Error("Expected a selected batch");
    expect(materializeBatchRows(first.value.batch)).toEqual(
      [1, 2, 3].map((id) => ({ id, partition: "west" })),
    );
    expect(budget.usedBytes).toBeGreaterThan(0);
  }
  await iterator.return?.();
  expect(budget.usedBytes).toBe(0);
});
