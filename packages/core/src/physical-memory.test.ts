import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { batchFromColumns, materializeBatchRows } from "./batch.js";
import { lt } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import {
  CpuPhysicalBackend,
  executePlannedPhysicalFragment,
  type PhysicalFragment,
  physicalInputFromBatch,
  planPhysicalFragment,
} from "./physical.js";

it.each([
  false,
  true,
])("carries scoped top-k allocations through execution (filtered=%s)", async (filtered) => {
  async function run(maxBytes: number) {
    const budget = new MemoryBudget(maxBytes);
    const scope = new MemoryScope(budget);
    try {
      const batch = batchFromColumns({ id: [9, 3, 8, 1] }, scope);
      const fragment: PhysicalFragment = {
        id: "owned-top-k",
        input: physicalInputFromBatch(batch),
        operators: [
          ...(filtered ? [{ kind: "select" as const, predicate: lt("id", 9) }] : []),
          { kind: "top-k", orderBy: [{ column: "id" }], limit: 2 },
        ],
        output: { kind: "batch" },
        estimates: { rowCount: 4, inputBytes: 32, outputBytes: 16 },
      };
      const backends = [new CpuPhysicalBackend()];
      const plan = planPhysicalFragment(fragment, backends, { requireAllocationScope: true });
      const result = await executePlannedPhysicalFragment(
        plan,
        backends,
        { kind: "batch", batch },
        { allocationScope: scope },
      );
      if (result.output.kind !== "batch") throw new Error("missing batch");
      const output = scope.retainAllocations(result.output.batch);
      scope.dispose();
      expect(budget.usedBytes).toBe(48 + c.view); // Input plus result; no selection mask survives.
      expect(materializeBatchRows(output.value)).toEqual([{ id: 1 }, { id: 3 }]);
      output.release();
      return budget.peakBytes;
    } finally {
      scope.dispose();
      expect(budget.usedBytes).toBe(0);
    }
  }
  const peak = await run(65536);
  expect(await run(peak)).toBe(peak);
  await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});

it("rejects an unscoped backend during planning and before dispatch", async () => {
  const batch = batchFromColumns({ id: [2, 1] });
  const fragment: PhysicalFragment = {
    id: "scope-contract",
    input: physicalInputFromBatch(batch),
    operators: [{ kind: "top-k", orderBy: [{ column: "id" }], limit: 1 }],
    output: { kind: "batch" },
    estimates: { rowCount: 2, inputBytes: 16, outputBytes: 8 },
  };
  class BorrowedBackend extends CpuPhysicalBackend {
    override capabilities() {
      return { ...super.capabilities(), allocationScopeOperators: [] };
    }
  }
  const backend = new BorrowedBackend();
  expect(() =>
    planPhysicalFragment(fragment, [backend], { requireAllocationScope: true }),
  ).toThrow();
  const plan = planPhysicalFragment(fragment, [backend]);
  const scope = new MemoryScope(new MemoryBudget(64));
  await expect(
    executePlannedPhysicalFragment(
      plan,
      [backend],
      { kind: "batch", batch },
      { allocationScope: scope },
    ),
  ).rejects.toThrow();
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});

it("propagates projection allocation scopes through fused top-k execution", async () => {
  const descriptors =
    2 * c.object +
    2 * c.property +
    ["id", "copy"].reduce(
      (bytes, name) => bytes + c.property + c.string + name.length * c.stringCodeUnit,
      0,
    );
  const orderingScratch = c.array + 3 * c.object + 5 * c.property + c.arraySlot;
  const scope = new MemoryScope(
    new MemoryBudget(
      Math.max(
        40 + c.view + descriptors,
        24 + orderingScratch,
        48 + 2 * c.view + c.array + c.object + 3 * c.property + c.arraySlot,
      ),
    ),
  );
  try {
    const batch = batchFromColumns({ id: [3, 1, 2] }, scope);
    const backend = new CpuPhysicalBackend();
    const fragment: PhysicalFragment = {
      id: "scoped-projection",
      input: physicalInputFromBatch(batch),
      operators: [
        { kind: "top-k", orderBy: [{ column: "id" }], limit: 2 },
        { kind: "project", select: ["id"], projections: { copy: { kind: "column", name: "id" } } },
      ],
      output: { kind: "batch" },
      estimates: { rowCount: 3, inputBytes: 24, outputBytes: 32 },
    };
    const plan = planPhysicalFragment(fragment, [backend], { requireAllocationScope: true });
    const result = await executePlannedPhysicalFragment(
      plan,
      [backend],
      { kind: "batch", batch },
      { allocationScope: scope },
    );
    if (result.output.kind !== "batch") throw new Error("missing output");
    expect(materializeBatchRows(result.output.batch)).toEqual([
      { id: 1, copy: 1 },
      { id: 2, copy: 2 },
    ]);
    expect(result.output.batch.columns.copy).toBe(result.output.batch.columns.id);
    expect(scope.budget.usedBytes).toBe(40 + c.view + descriptors);
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it.each([
  "selection",
  "indices",
] as const)("admits physical %s output without a JS index array", async (kind) => {
  const batch = batchFromColumns({ id: [3, 1, 2, 4] });
  const backend = new CpuPhysicalBackend();
  const fragment: PhysicalFragment = {
    id: "scoped-selection-output",
    input: physicalInputFromBatch(batch),
    operators: [{ kind: "select", predicate: lt("id", 3) }],
    output: { kind },
    estimates: { rowCount: 4, inputBytes: 32, outputBytes: 8 },
  };
  const plan = planPhysicalFragment(fragment, [backend], { requireAllocationScope: true });
  async function run(limit: number) {
    const scope = new MemoryScope(new MemoryBudget(limit));
    try {
      const result = await executePlannedPhysicalFragment(
        plan,
        [backend],
        { kind: "batch", batch },
        { allocationScope: scope },
      );
      if (result.output.kind === "selection")
        expect(Array.from(result.output.selection)).toEqual([0, 1, 1, 0]);
      else if (result.output.kind === "indices")
        expect(Array.from(result.output.indices)).toEqual([1, 2]);
      else throw new Error("unexpected output");
      return scope.budget.peakBytes;
    } finally {
      scope.dispose();
      expect(scope.budget.usedBytes).toBe(0);
    }
  }
  const exact = kind === "selection" ? c.view + 4 : 2 * c.view + 12;
  expect(await run(exact)).toBe(exact);
  await expect(run(exact - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});
