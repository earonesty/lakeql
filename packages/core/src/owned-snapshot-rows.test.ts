import { expect, it } from "vitest";
import { jsonSafeValue } from "./evaluator.js";
import { stableStringify } from "./manifest.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import {
  snapshotOwnedRows,
  snapshotScalarSerializedBytes,
  snapshotScalarWithAdmission,
} from "./owned-snapshot-rows.js";

it("owns snapshot scalar conversions and safely preserves own prototype keys", () => {
  const row = {
    text: "雪",
    bytes: new Uint8Array([0, 255]),
    big: 9007199254740993n,
    small: 3n,
    nil: null,
  };
  Object.defineProperty(row, "__proto__", { value: "data", enumerable: true });
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const output = snapshotOwnedRows([row], scope);
  expect(output.value[0]).toEqual(jsonSafeValue(row));
  row.bytes.fill(4);
  expect(output.value[0]?.bytes).toBe("00ff");
  scope.dispose();
  expect(pool.usedBytes).toBe(output.bytes);
  output.release();
  expect(pool.usedBytes).toBe(0);
});

it("admits exact snapshot capacity and cleans failed construction without releasing siblings", () => {
  const rows = [{ text: "x".repeat(1000), big: 12345678901234567890n }];
  const measured = new MemoryBudget(65536);
  const source = new MemoryScope(measured);
  snapshotOwnedRows(rows, source).release();
  const required = measured.peakBytes;
  source.dispose();
  for (const cap of [required - 1, required]) {
    const pool = new MemoryBudget(cap + 17);
    const sibling = pool.reserve(17, "sibling");
    const scope = new MemoryScope(pool.child(cap));
    if (cap < required) expect(() => snapshotOwnedRows(rows, scope)).toThrow(/budget/i);
    else snapshotOwnedRows(rows, scope).release();
    scope.dispose();
    expect(pool.usedBytes).toBe(17);
    sibling.release();
  }
});

it("rejects nested and cyclic values before normalization and releases partial snapshots", () => {
  const cycle: Record<string, unknown> = {};
  cycle.self = cycle;
  for (const value of [cycle, [1, 2], undefined]) {
    const pool = new MemoryBudget(65536);
    const scope = new MemoryScope(pool);
    expect(() => snapshotOwnedRows([{ first: "admitted", bad: value }], scope)).toThrow(
      /JSON scalars/,
    );
    expect(pool.usedBytes).toBe(0);
    scope.dispose();
  }
});

it("admits scalar byte conversion at exact capacity before constructing text", () => {
  const input = new Uint8Array(128).fill(255);
  const measured = new MemoryBudget(65536);
  const scope = new MemoryScope(measured);
  expect(snapshotScalarWithAdmission(input, scope)).toBe("ff".repeat(128));
  const required = measured.peakBytes;
  scope.dispose();
  for (const cap of [required - 1, required]) {
    const target = new MemoryScope(new MemoryBudget(cap));
    if (cap < required) expect(() => snapshotScalarWithAdmission(input, target)).toThrow(/budget/i);
    else expect(snapshotScalarWithAdmission(input, target)).toBe("ff".repeat(128));
    target.dispose();
    expect(target.budget.usedBytes).toBe(0);
  }
});

it.each([
  null,
  true,
  1e308,
  Infinity,
  123n,
  9007199254740993n,
  new Uint8Array([0, 255]),
  "雪\ud800",
])("measures normalized snapshot scalar bytes without retaining scratch: %s", (value) => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const expected = new TextEncoder().encode(stableStringify(jsonSafeValue(value))).byteLength;
  expect(snapshotScalarSerializedBytes(value, scope)).toBe(expected);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});
