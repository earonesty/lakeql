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

it.each([1, 32768])("avoids cycle-set churn for %i leaf cells", (rows) => {
  const vector: Vector = { type: "i32", values: new Int32Array(rows) };
  const batch: Batch = { rowCount: rows, columns: { value: vector } };
  const remove = Set.prototype.delete;
  let leafDeletes = 0;
  const spy = vi.spyOn(Set.prototype, "delete").mockImplementation(function (value) {
    if (value === vector) leafDeletes++;
    return remove.call(this, value);
  });
  const scope = new MemoryScope(new MemoryBudget(c.object + c.property));
  try {
    expect(planBatchMaterialization(batch, undefined, scope).rows).toBe(rows);
    expect(leafDeletes).toBe(0);
    expect(scope.budget.peakBytes).toBe(c.object + c.property);
    expect(scope.budget.usedBytes).toBe(0);
  } finally {
    spy.mockRestore();
    scope.dispose();
  }
});

it.each([
  "struct",
  "dict",
  "list",
  "map",
  "union",
] as const)("still rejects cycles through %s vectors", (type) => {
  const leaf: Vector = { type: "i32", values: Int32Array.of(1) };
  const children: Vector[] = [leaf];
  const vector: Vector =
    type === "struct"
      ? { type, fields: {}, length: 1 }
      : type === "dict"
        ? { type, dictionary: leaf, indices: Uint32Array.of(0) }
        : type === "list"
          ? { type, child: leaf, offsets: Int32Array.of(0, 1) }
          : type === "map"
            ? { type, keys: leaf, values: leaf, offsets: Int32Array.of(0, 1) }
            : { type, children, tags: Uint8Array.of(0), offsets: Uint32Array.of(0) };
  switch (vector.type) {
    case "struct":
      vector.fields.self = vector;
      break;
    case "dict":
      vector.dictionary = vector;
      break;
    case "list":
      vector.child = vector;
      break;
    case "map":
      vector.values = vector;
      break;
    case "union":
      children[0] = vector;
      break;
  }
  const scope = new MemoryScope(new MemoryBudget(4096));
  try {
    expect(() =>
      planBatchMaterialization({ rowCount: 1, columns: { value: vector } }, undefined, scope),
    ).toThrow(/Cyclic/);
    expect(scope.budget.usedBytes).toBe(0);
  } finally {
    scope.dispose();
  }
});
