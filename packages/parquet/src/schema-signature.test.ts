import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { stableSchemaSignature } from "./schema-signature.js";

function legacy(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(legacy).join(",")}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .filter((key) => record[key] !== undefined)
    .map((key) => `${JSON.stringify(key)}:${legacy(record[key])}`)
    .join(",")}}`;
}

it("preserves canonical schema signatures including escaped keys and undefined fields", () => {
  const shared = { precision: 9, scale: 2 };
  for (const value of [
    null,
    true,
    7,
    -0,
    NaN,
    Infinity,
    "猫😀\n\ud800",
    [],
    [undefined, null, 7],
    {
      z: undefined,
      b: shared,
      a: shared,
      children: [{ name: 'x"y', logical_type: { DECIMAL: shared } }],
    },
    JSON.parse('{"__proto__":{"STRING":{}},"constructor":null}'),
  ]) {
    expect(stableSchemaSignature(value)).toBe(legacy(value));
  }
});

it("serializes deep schema shapes without recursive calls", () => {
  let value: unknown = { type: "INT32" };
  for (let depth = 0; depth < 20000; depth++) value = { children: [value] };
  const text = stableSchemaSignature(value);
  expect(text).toBe(`${'{"children":['.repeat(20000)}{"type":"INT32"}${"]}".repeat(20000)}`);
});

it("admits output overlap at exact capacity and rolls back sorting/serialization failure", () => {
  const value = {
    z: "猫😀\n",
    a: [
      { type: "INT32", name: "x" },
      { logical_type: { STRING: {} }, name: "y" },
    ],
  };
  const run = (capacity: number) => {
    const pool = new MemoryBudget(capacity);
    const scope = new MemoryScope(pool);
    scope.allocate(8, "sibling", () => undefined);
    try {
      expect(stableSchemaSignature(value, scope)).toBe(legacy(value));
      expect(pool.usedBytes).toBeGreaterThan(8);
      return pool.peakBytes;
    } catch (error) {
      expect(pool.usedBytes).toBe(8);
      throw error;
    } finally {
      scope.dispose();
      expect(pool.usedBytes).toBe(0);
    }
  };
  const peak = run(65536);
  expect(run(peak)).toBe(peak);
  expect(() => run(peak - 1)).toThrowError(
    expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }),
  );
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const cycle: Record<string, unknown> = {};
  cycle.child = cycle;
  expect(() => stableSchemaSignature(cycle, scope)).toThrowError(
    expect.objectContaining({ code: "LAKEQL_VALIDATION_ERROR" }),
  );
  expect(pool.usedBytes).toBe(0);
  expect(() => stableSchemaSignature({ number: 1n }, scope)).toThrow(TypeError);
  expect(pool.usedBytes).toBe(0);
  scope.dispose();
});
