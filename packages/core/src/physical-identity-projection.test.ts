import { expect, it } from "vitest";
import { batchFromColumns, materializeBatchRows } from "./batch.js";
import { col, lit } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import {
  CpuPhysicalBackend,
  executePlannedPhysicalFragment,
  type PhysicalOperator,
  physicalInputFromBatch,
  planPhysicalFragment,
} from "./physical.js";
import { vectorProjectBatch } from "./vector-project.js";

const batch = batchFromColumns({ a: [1, null], b: [["猫"], []] });
async function run(operator: PhysicalOperator, scope: MemoryScope) {
  const backend = new CpuPhysicalBackend();
  const plan = planPhysicalFragment(
    {
      id: "identity-projection",
      input: physicalInputFromBatch(batch),
      operators: [operator],
      output: { kind: "batch" },
      estimates: { rowCount: 2, inputBytes: 64, outputBytes: 64 },
    },
    [backend],
    { requireAllocationScope: true },
  );
  return executePlannedPhysicalFragment(
    plan,
    [backend],
    { kind: "batch", batch },
    { allocationScope: scope },
  );
}

it.each([
  undefined,
  [],
  ["*"],
  ["a", "b"],
  ["b", "*", "a"],
])("borrows an identity projection with zero new allocation for select=%j", async (select) => {
  const scope = new MemoryScope(new MemoryBudget(0));
  try {
    const result = await run({ kind: "project", select }, scope);
    expect(result.output.kind).toBe("batch");
    if (result.output.kind !== "batch") throw new Error("Missing batch");
    expect(result.output.batch).toBe(batch);
    expect(result.metrics.outputRows).toBe(2);
    expect(scope.budget.peakBytes).toBe(0);
  } finally {
    scope.dispose();
  }
});

it.each([
  { kind: "project", select: ["b", "a"] },
  { kind: "project", select: ["a"] },
  { kind: "project", projections: { a: lit(7) } },
  { kind: "project", projections: { alias: col("a") } },
] satisfies PhysicalOperator[])("retains nonidentity projection semantics: %j", async (operator) => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  try {
    const result = await run(operator, scope);
    if (result.output.kind !== "batch") throw new Error("Missing batch");
    expect(result.output.batch).not.toBe(batch);
    const expected = vectorProjectBatch(
      batch,
      operator.select,
      "projections" in operator ? operator.projections : undefined,
    );
    expect(Object.keys(result.output.batch.columns)).toEqual(Object.keys(expected.columns));
    expect(materializeBatchRows(result.output.batch)).toEqual(materializeBatchRows(expected));
    expect(scope.budget.usedBytes).toBeGreaterThan(0);
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it("preserves missing-column errors beside a wildcard and public directory independence", async () => {
  const scope = new MemoryScope(new MemoryBudget(4096));
  try {
    await expect(run({ kind: "project", select: ["*", "missing"] }, scope)).rejects.toThrow(
      /column/i,
    );
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
  const projected = vectorProjectBatch(batch);
  expect(projected).not.toBe(batch);
  expect(projected.columns).not.toBe(batch.columns);
});
