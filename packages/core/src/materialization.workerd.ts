import { expect, it } from "vitest";
import { batchFromVectors } from "./batch.js";
import { materializeOwnedBatchRows, planBatchMaterialization } from "./materialization-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it("admits Worker materialization before decoding and retains owned rows after scan disposal", () => {
  const data = new TextEncoder().encode("猫");
  const batch = batchFromVectors({ text: { type: "utf8-view", data, length: 1, fixedLength: 3 } });
  const plan = planBatchMaterialization(batch);
  const budget = new MemoryBudget(plan.allocationBytes + data.length);
  const scope = new MemoryScope(budget);
  scope.allocate(data.length, "input", () => data);
  const input = scope.retainAllocations(batch);
  const result = materializeOwnedBatchRows(input);
  input.release();
  scope.dispose();
  expect(budget.usedBytes).toBe(plan.retainedBytes);
  expect(budget.peakBytes).toBe(plan.allocationBytes + data.length);
  expect(result.value).toEqual([{ text: "猫" }]);
  result.release();
  expect(budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(plan.allocationBytes - 1));
  expect(() => plan.execute(tight)).toThrow(/budget/);
  expect(tight.budget.peakBytes).toBe(0);
  tight.dispose();
});

it("keeps borrowed binary backing alive after the materialization stage closes", () => {
  const data = Uint8Array.of(1, 2, 3);
  const batch = batchFromVectors({
    bytes: { type: "binary-view", data, length: 1, fixedLength: 3 },
  });
  const plan = planBatchMaterialization(batch);
  const budget = new MemoryBudget(plan.allocationBytes + data.length);
  const scope = new MemoryScope(budget);
  scope.allocate(data.length, "binary backing", () => data);
  const input = scope.retainAllocations(batch);
  const output = materializeOwnedBatchRows(input);
  input.release();
  scope.dispose();
  expect(output.value).toEqual([{ bytes: data }]);
  expect(budget.usedBytes).toBe(plan.retainedBytes + data.length);
  const retained = output.retain();
  output.release();
  expect(retained.value).toEqual([{ bytes: data }]);
  retained.release();
  expect(budget.usedBytes).toBe(0);
});
