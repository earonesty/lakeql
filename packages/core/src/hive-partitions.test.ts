import { expect, it } from "vitest";
import { parseHivePartitions } from "./hive-partitions.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it("preserves decoding, duplicate keys, equals signs and prototype-named partitions", () => {
  const result = parseHivePartitions(
    "/plain/=bad/empty=/x=old/x=%E7%8C%AB/__proto__=safe/constructor=a=b/",
  );
  expect(result).toEqual(JSON.parse('{"x":"猫","__proto__":"safe","constructor":"a=b"}'));
  expect(Object.getPrototypeOf(result)).toBe(Object.prototype);
  expect(Object.hasOwn(result, "__proto__")).toBe(true);
  expect(() => parseHivePartitions("x=%ZZ/x=valid")).toThrow(URIError);
});

it("admits retained partition storage and decoding overlap at exact boundaries", () => {
  const run = (capacity: number, prefix = "") => {
    const budget = new MemoryBudget(capacity);
    const scope = new MemoryScope(budget);
    try {
      expect(parseHivePartitions(`${prefix}x=%E7%8C%AB/y=a=b`, scope)).toEqual({
        x: "猫",
        y: "a=b",
      });
      expect(budget.usedBytes).toBeGreaterThan(0);
      return budget.peakBytes;
    } finally {
      scope.dispose();
      expect(budget.usedBytes).toBe(0);
    }
  };
  const peak = run(65536);
  expect(run(peak)).toBe(peak);
  expect(run(peak, "plain/".repeat(10000))).toBe(peak);
  expect(() => run(peak - 1)).toThrowError(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
});

it("rolls back malformed decoding and budget failures without releasing sibling allocations", () => {
  const budget = new MemoryBudget(4096);
  const scope = new MemoryScope(budget);
  scope.allocate(8, "sibling", () => undefined);
  expect(() => parseHivePartitions("x=ok/y=%ZZ", scope)).toThrow(URIError);
  expect(budget.usedBytes).toBe(8);
  expect(() => parseHivePartitions(`x=ok/y=${"a".repeat(4096)}`, scope)).toThrowError(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
  expect(budget.usedBytes).toBe(8);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});
