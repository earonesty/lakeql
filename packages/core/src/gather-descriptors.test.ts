import { expect, it, vi } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Batch, materializeBatchRows, type Vector } from "./batch.js";
import { sharedGatherPlan } from "./gather-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { compactGatherPlan } from "./packed-gather.js";
import { gatherBatch, gatherOwnedBatch } from "./vector-sort.js";

function observedDescriptors(batch: Batch): number {
  const record = (value: object) => c.object + Object.keys(value).length * c.property;
  return (
    record(batch) +
    record(batch.columns) +
    Object.entries(batch.columns).reduce(
      (bytes, [name, vector]) => bytes + c.string + name.length * c.stringCodeUnit + record(vector),
      0,
    )
  );
}

it.each([
  0, 1, 32, 256,
])("admits containers before backing for %s-column gathers, including empty rows", (width) => {
  for (const rows of [0, 2]) {
    const columns: Record<string, Vector> = Object.create(null);
    for (let column = 0; column < width; column++)
      columns[`列${column}`] = {
        type: "f64",
        values: Float64Array.from({ length: rows }, (_, i) => i + column),
      };
    // Input descriptors/backing are caller-owned fixtures. Only newly gathered output is measured.
    const batch = { rowCount: rows, columns };
    const indices = rows === 0 ? [] : [1, 0, 1];
    const plan = sharedGatherPlan(batch, indices);
    const required = plan.typedBytes + plan.viewBytes + plan.descriptorBytes;
    const pool = new MemoryBudget(required);
    const scope = new MemoryScope(pool);
    const input = scope.retainAllocations(batch);
    scope.dispose();
    const from = vi.spyOn(Float64Array, "from");
    try {
      const short = pool.child(required - 1);
      expect(() => gatherOwnedBatch(input, indices, short)).toThrow(/budget/i);
      expect(from).not.toHaveBeenCalled();
      expect(short.usedBytes).toBe(0);
      const output = gatherOwnedBatch(input, indices);
      try {
        expect(plan.descriptorBytes).toBe(observedDescriptors(output.value));
        expect(output.bytes).toBe(required);
        expect(pool.peakBytes).toBe(required);
        expect(output.value.rowCount).toBe(indices.length);
        expect(Object.keys(output.value.columns)).toEqual(Object.keys(columns));
      } finally {
        output.release();
      }
    } finally {
      from.mockRestore();
      input.release();
    }
    expect(pool.usedBytes).toBe(0);
  }
});

it.each([
  false,
  true,
])("transfers descriptor ownership through shared/compact gathers, compact=%s", (compact) => {
  const pool = new MemoryBudget(65536);
  const source = new MemoryScope(pool);
  const data = new TextEncoder().encode("a".repeat(64) + "b".repeat(64));
  const batch: Batch = {
    rowCount: 2,
    columns: {
      text: { type: "utf8-view", data, fixedLength: 64, length: 2 },
    },
  };
  source.allocate(4096, "fixture backing and padding", () => data);
  const input = source.retainAllocations(batch);
  source.dispose();
  const positions = [1, 0];
  const plan = compact ? compactGatherPlan(batch, positions) : sharedGatherPlan(batch, positions);
  if (plan === undefined) throw new Error("missing plan");
  const bytes = plan.typedBytes + plan.viewBytes + plan.descriptorBytes;
  const output = gatherOwnedBatch(input, positions, pool.child(input.bytes + bytes));
  expect(output.value.columns.text?.type).toBe(compact ? "utf8-view" : "dict");
  expect(plan.descriptorBytes).toBe(observedDescriptors(output.value));
  expect(output.bytes).toBe(bytes + (compact ? 0 : input.bytes));
  input.release();
  const selected = gatherOwnedBatch(output, [0], pool);
  output.release();
  try {
    expect(selected.value.columns.text?.type).toBe("utf8-view");
    expect(materializeBatchRows(selected.value)).toEqual([{ text: "b".repeat(64) }]);
    expect(pool.usedBytes).toBe(selected.bytes);
  } finally {
    selected.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it("keeps constructing descriptors charged through parent-stage disposal", () => {
  const batch: Batch = {
    rowCount: 2,
    columns: { value: { type: "f64", values: Float64Array.of(1, 2) } },
  };
  const plan = sharedGatherPlan(batch, [0]);
  const pool = new MemoryBudget(plan.typedBytes + plan.viewBytes + plan.descriptorBytes);
  const parent = new MemoryScope(pool);
  const original = Float64Array.from;
  let calls = 0;
  const from = vi.spyOn(Float64Array, "from").mockImplementation(function (...args) {
    calls++;
    parent.dispose();
    const live = plan.descriptorBytes + plan.typedBytes;
    expect(pool.usedBytes).toBe(live);
    expect(() =>
      pool.reserve(pool.maxBytes - live + 1, "cannot reuse constructing descriptors"),
    ).toThrow(/budget/i);
    return original.apply(this, args);
  });
  try {
    expect(() => gatherBatch(batch, [0], parent)).toThrow(/disposed/);
    expect(calls).toBe(1);
    expect(pool.usedBytes).toBe(0);
  } finally {
    from.mockRestore();
    parent.dispose();
  }
});

it("rejects an empty gather below its batch and directory container extent", () => {
  const pool = new MemoryBudget(2 * c.object + 2 * c.property - 1);
  const scope = new MemoryScope(pool);
  const input = scope.retainAllocations({ rowCount: 0, columns: {} });
  scope.dispose();
  try {
    expect(() => gatherOwnedBatch(input, [])).toThrow(/budget/i);
    expect(pool.usedBytes).toBe(0);
  } finally {
    input.release();
  }
});
