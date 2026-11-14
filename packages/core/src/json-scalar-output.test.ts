import { expect, it, vi } from "vitest";
import { JsonScalarFormatter } from "./json-scalar-output.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { scalarStringAllocationBytes } from "./scalar-cast-allocation.js";

it("reuses one scalar reservation and grows without double charging old capacity", () => {
  const large = 10n ** 1000n;
  const extent = scalarStringAllocationBytes(large);
  const pool = new MemoryBudget(extent);
  const scope = new MemoryScope(pool);
  const reserve = vi.spyOn(scope, "reserve");
  const formatter = new JsonScalarFormatter(scope);
  try {
    for (let i = 0; i < 1000; i++)
      expect(formatter.consume(i + 0.25, (text) => text.length)).toBe(String(i + 0.25).length);
    expect(reserve).toHaveBeenCalledTimes(1);
    expect(formatter.consume(large, (text) => text.length)).toBe(1001);
    expect(reserve).toHaveBeenCalledTimes(2);
    expect(pool.usedBytes).toBe(extent);
    formatter.consume(0.25, (text) => {
      expect(text).toBe("0.25");
    });
    expect(reserve).toHaveBeenCalledTimes(2);
    expect(pool.peakBytes).toBe(extent);
  } finally {
    formatter.dispose();
    scope.dispose();
  }
  expect(pool.usedBytes).toBe(0);
});

it("failed workspace growth does not invoke the consumer or leak old capacity", () => {
  const pool = new MemoryBudget(scalarStringAllocationBytes(0.25));
  const scope = new MemoryScope(pool);
  const formatter = new JsonScalarFormatter(scope);
  formatter.consume(0.25, (text) => text.length);
  const consume = vi.fn(() => 0);
  expect(() => formatter.consume(10n ** 1000n, consume)).toThrow(/budget/i);
  expect(consume).not.toHaveBeenCalled();
  expect(pool.usedBytes).toBe(0);
  formatter.dispose();
  formatter.dispose();
  scope.dispose();
});
