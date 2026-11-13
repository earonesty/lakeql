import { expect, it } from "vitest";
import { batchFromColumns, materializeBatchRows } from "./batch.js";
import { sharedGatherPlan } from "./gather-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { gatherOwnedBatch } from "./vector-sort.js";

it("admits mixed shared shapes atomically and shares one selection index buffer", () => {
  const root = new MemoryBudget(4096);
  const producer = root.child();
  const scope = new MemoryScope(producer);
  const batch = batchFromColumns(
    {
      scalar: [1, null, 3],
      text: ["a", null, "c"],
      bytes: [Uint8Array.of(1), null, Uint8Array.of(3)],
      list: [[1], [], null],
      struct: [{ a: 1 }, null, { a: 3 }],
      map: [new Map([["a", 1]]), new Map(), null],
    },
    scope,
  );
  const input = scope.retainAllocations(batch);
  scope.dispose();
  const indices = [2, 1, 2, 0];
  const plan = sharedGatherPlan(batch, indices);
  // Scalar values/validity + one shared text/nested/packed index buffer.
  expect(plan).toEqual({
    typedBytes: 32 + 4 + 16,
    retainsInput: true,
    createsJavaScriptArrays: false,
  });
  const exact = root.child(input.bytes + plan.typedBytes);
  const output = gatherOwnedBatch(input, indices, exact);
  expect(output.bytes).toBe(input.bytes + plan.typedBytes);
  const rows = materializeBatchRows(batch);
  expect(materializeBatchRows(output.value)).toEqual(indices.map((index) => rows[index]));
  output.release();
  const tight = root.child(input.bytes + plan.typedBytes - 1);
  expect(() => gatherOwnedBatch(input, indices, tight)).toThrow("budget");
  expect(tight.usedBytes).toBe(0);
  expect(tight.peakBytes).toBe(input.bytes); // No partially allocated output.
  input.release();
  expect(root.usedBytes).toBe(0);
});

it("plans independent dictionary indices and avoids validity for all-present selections", () => {
  const batch = {
    rowCount: 2,
    columns: {
      a: {
        type: "dict" as const,
        indices: Uint32Array.of(0, 1),
        dictionary: { type: "f64" as const, values: Float64Array.of(1, 2) },
        valid: Uint8Array.of(1, 0),
      },
      b: {
        type: "dict" as const,
        indices: Uint32Array.of(1, 0),
        dictionary: { type: "f64" as const, values: Float64Array.of(1, 2) },
      },
    },
  };
  expect(sharedGatherPlan(batch, [0, 0]).typedBytes).toBe(16);
  expect(sharedGatherPlan(batch, [0, 1]).typedBytes).toBe(18);
  expect(sharedGatherPlan(batch, []).typedBytes).toBe(0);
});
