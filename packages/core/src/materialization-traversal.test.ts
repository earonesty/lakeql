import { expect, it, vi } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Batch, Vector } from "./batch.js";
import { planBatchMaterialization } from "./materialization-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

function nested(rows: number, depth: number): Batch {
  let vector: Vector = { type: "i32", values: new Int32Array(rows) };
  for (let level = 1; level < depth; level++)
    vector = { type: "struct", fields: { child: vector }, length: rows };
  return { rowCount: rows, columns: { value: vector } };
}

it.each([1, 1000, 10000])("reuses depth admission across %i rows", (rows) => {
  const budget = new MemoryBudget(4096);
  const scope = new MemoryScope(budget);
  const reserve = vi.spyOn(MemoryBudget.prototype, "reserve");
  try {
    const plan = planBatchMaterialization(nested(rows, 4), undefined, scope);
    expect(plan.rows).toBe(rows);
    expect(
      reserve.mock.calls.filter((call) => call[1] === "materialization traversal depth"),
    ).toHaveLength(4);
    expect(budget.peakBytes).toBe(c.object + 4 * c.property);
    expect(budget.usedBytes).toBe(0);
  } finally {
    reserve.mockRestore();
    scope.dispose();
  }
});

it("plans repeated siblings inside an exact fixed admission and rejects one byte below depth", () => {
  const batch = nested(1000, 4);
  for (const bytes of [c.object + 4 * c.property, c.object + 4 * c.property - 1]) {
    const budget = new MemoryBudget(bytes);
    const stage = new MemoryScope(budget);
    stage.admit(bytes, "planning workspace");
    try {
      if (bytes === c.object + 4 * c.property)
        expect(planBatchMaterialization(batch, undefined, stage).rows).toBe(1000);
      else
        expect(() => planBatchMaterialization(batch, undefined, stage)).toThrow(/budget|admitted/);
    } finally {
      stage.dispose();
    }
    expect(budget.usedBytes).toBe(0);
  }
});

it("releases the depth high-water charge on cycles and invalid nested indices", () => {
  const cycle: Vector = { type: "struct", fields: {}, length: 1 };
  cycle.fields.self = cycle;
  for (const vector of [
    cycle,
    {
      type: "dict",
      indices: Uint32Array.of(9),
      dictionary: { type: "i32", values: Int32Array.of(1) },
    } satisfies Vector,
  ]) {
    const scope = new MemoryScope(new MemoryBudget(4096));
    expect(() =>
      planBatchMaterialization({ rowCount: 1, columns: { value: vector } }, undefined, scope),
    ).toThrow();
    expect(scope.budget.usedBytes).toBe(0);
    scope.dispose();
  }
});

it("does not admit frames for excluded rows or null payloads", () => {
  const batch: Batch = {
    rowCount: 1000,
    columns: {
      value: {
        type: "i32",
        values: new Int32Array(1000),
        valid: new Uint8Array(1000),
      },
    },
  };
  for (const selection of [undefined, new Uint8Array(1000)]) {
    const scope = new MemoryScope(new MemoryBudget(c.object));
    scope.admit(c.object, "empty traversal capacity");
    expect(planBatchMaterialization(batch, selection, scope).rows).toBe(selection ? 0 : 1000);
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});
