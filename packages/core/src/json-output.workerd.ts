import { expect, it } from "vitest";
import { encodeOwnedJsonOutput } from "./json-output-encoding.js";
import { jsonOutputStream } from "./json-output-stream.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it.each([
  64, 128,
])("streams deeply nested JSON under a %i MiB Worker reservation pool", async (mib) => {
  let value: unknown = "雪😀";
  for (let i = 0; i < 20000; i++) value = [value];
  const expected = `${"[".repeat(20000)}"雪😀"${"]".repeat(20000)}\n`;
  const pool = new MemoryBudget(mib * 1024 * 1024);
  async function* rows() {
    yield value as Record<string, unknown>;
  }
  await expect(new Response(jsonOutputStream(rows(), pool, "ndjson")).text()).resolves.toBe(
    expected,
  );
  const peak = pool.peakBytes;
  expect(peak).toBeLessThanOrEqual(pool.maxBytes);
  expect(pool.usedBytes).toBe(0);
  const tight = new MemoryBudget(peak - 1);
  await expect(
    new Response(jsonOutputStream(rows(), tight, "ndjson")).text(),
  ).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  expect(tight.usedBytes).toBe(0);
  const exact = new MemoryBudget(peak);
  await expect(new Response(jsonOutputStream(rows(), exact, "ndjson")).text()).resolves.toBe(
    expected,
  );
  expect(exact.usedBytes).toBe(0);
});

it("releases nested writer capacity after a Worker output admission failure", () => {
  const scope = new MemoryScope(new MemoryBudget(1024));
  expect(() => encodeOwnedJsonOutput({ values: ["x".repeat(4096)] }, scope)).toThrow(/budget/i);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("sizes growing JSON scalars in an exact Worker stage", async () => {
  const { jsonOutputUtf8Bytes } = await import("./json-output-size.js");
  const value = [0.25, 10n ** 1000n, { nested: [10n ** 2000n, 0.5] }];
  const expected = new TextEncoder().encode(
    JSON.stringify([0.25, String(10n ** 1000n), { nested: [String(10n ** 2000n), 0.5] }]),
  ).length;
  const measured = new MemoryScope(new MemoryBudget(100000));
  expect(jsonOutputUtf8Bytes(value, measured)).toBe(expected);
  const peak = measured.budget.peakBytes;
  measured.dispose();
  for (const capacity of [peak, peak - 1]) {
    const scope = new MemoryScope(new MemoryBudget(capacity));
    scope.admit(capacity, "JSON sizing workspace");
    try {
      if (capacity === peak) expect(jsonOutputUtf8Bytes(value, scope)).toBe(expected);
      else expect(() => jsonOutputUtf8Bytes(value, scope)).toThrow(/budget|extent/i);
    } finally {
      scope.dispose();
    }
    expect(scope.budget.usedBytes).toBe(0);
  }
});
