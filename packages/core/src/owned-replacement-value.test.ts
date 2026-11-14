import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { OwnedReplacementValue } from "./owned-replacement-value.js";

it("detaches mutable values and reuses one retained ownership slot", () => {
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const slot = new OwnedReplacementValue(scope);
  const input = new Uint8Array([1]);
  const output = slot.replace(input);
  input[0] = 9;
  expect(output[0]).toBe(1);
  const used = pool.usedBytes;
  for (let index = 0; index < 100; index++) slot.replace(input);
  expect(pool.usedBytes).toBe(used);
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
  expect(() => slot.replace(input)).toThrow(/disposed/);
});

it("admits old/new overlap and preserves old ownership when replacement fails", () => {
  const measured = new MemoryBudget(65536);
  const scope = new MemoryScope(measured);
  const slot = new OwnedReplacementValue(scope);
  slot.replace("x".repeat(100));
  slot.replace("y".repeat(100));
  const required = measured.peakBytes;
  scope.dispose();
  for (const cap of [required - 1, required]) {
    const budget = new MemoryBudget(cap);
    const target = new MemoryScope(budget);
    const candidate = new OwnedReplacementValue(target);
    candidate.replace("x".repeat(100));
    const prior = budget.usedBytes;
    if (cap < required) expect(() => candidate.replace("y".repeat(100))).toThrow(/budget/i);
    else candidate.replace("y".repeat(100));
    expect(budget.usedBytes).toBe(prior);
    target.dispose();
    expect(budget.usedBytes).toBe(0);
  }
});
