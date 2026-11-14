import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { encodeOwnedCsvOutput } from "./csv-output-encoding.js";
import { decimalFormatAllocationBytes, decimalFromParts } from "./decimal-value.js";
import { jsonSafeValue } from "./evaluator.js";
import { encodeOwnedJsonOutput } from "./json-output-encoding.js";
import { jsonOutputUtf8Bytes } from "./json-output-size.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import { serializedValueBytes } from "./serialized-size.js";

const fixtures = [
  [123456789987654321n, 9, "123456789.987654321"],
  [-123456789987654321n, 9, "-123456789.987654321"],
  [9876543210123456789n, 9, "9876543210.123456789"],
  [-9876543210123456789n, 9, "-9876543210.123456789"],
  [0n, Number.MAX_SAFE_INTEGER, "0"],
  [1n, Number.MAX_SAFE_INTEGER, "1e-9007199254740991"],
] as const;

function checkOwned(
  encode: (scope: MemoryScope) => OwnedMemory<Uint8Array>,
  expected: string,
): void {
  const bytes = new TextEncoder().encode(expected);
  const pool = new MemoryBudget(1024 * 1024);
  const scope = new MemoryScope(pool);
  const output = encode(scope);
  expect(output.value).toEqual(bytes);
  const peak = pool.peakBytes;
  scope.dispose();
  expect(pool.usedBytes).toBe(output.bytes);
  output.release();
  expect(pool.usedBytes).toBe(0);
  const exact = new MemoryScope(new MemoryBudget(peak));
  const repeated = encode(exact);
  expect(repeated.value).toEqual(bytes);
  repeated.release();
  exact.dispose();
  expect(exact.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => encode(tight)).toThrow(/budget/i);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
}

it.each(fixtures)("serializes exact decimal %s at scale %s", (coefficient, scale, expected) => {
  const decimal = decimalFromParts(coefficient, scale);
  const row = { value: decimal, nested: [decimal, null] };
  const safe = { value: expected, nested: [expected, null] };
  const json = JSON.stringify(safe);
  expect(jsonSafeValue(row)).toEqual(safe);
  expect(JSON.stringify(row)).toBe(json);
  expect(jsonOutputUtf8Bytes(row)).toBe(new TextEncoder().encode(json).length);
  expect(serializedValueBytes(row)).toBe(new TextEncoder().encode(json).length);
  checkOwned((scope) => encodeOwnedJsonOutput(row, scope, { suffix: "\n" }), `${json}\n`);
  const nestedCsv = JSON.stringify([expected, null]).replaceAll('"', '""');
  checkOwned(
    (scope) => encodeOwnedCsvOutput(row, ["value", "nested"], true, scope),
    `value,nested\n${expected},"${nestedCsv}"\n`,
  );
});

it.each([1000, 10000])("reuses bounded decimal formatting across %i values", (count) => {
  const decimal = decimalFromParts(123456789987654321n, 9);
  const input = Array.from({ length: count }, () => decimal);
  const expected = JSON.stringify(Array.from({ length: count }, () => "123456789.987654321"));
  const bytes = new TextEncoder().encode(expected);
  const workspace = decimalFormatAllocationBytes(decimal);
  const pool = new MemoryBudget(bytes.length + c.view + workspace + 4096);
  const scope = new MemoryScope(pool);
  const output = encodeOwnedJsonOutput(input, scope);
  expect(output.value).toEqual(bytes);
  expect(pool.peakBytes).toBeGreaterThan(output.bytes + workspace);
  expect(pool.peakBytes).toBeLessThan(output.bytes + workspace + 4096);
  expect(pool.usedBytes).toBe(output.bytes);
  output.release();
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
});
