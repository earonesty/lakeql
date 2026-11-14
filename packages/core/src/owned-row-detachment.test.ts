import { expect, it } from "vitest";
import { DecimalValue } from "./decimal-value.js";
import { MemoryBudget } from "./memory-budget.js";
import { detachOwnedRow } from "./owned-row-detachment.js";
import { TimestampValue } from "./timestamp.js";

it("detaches nested containers, byte slices, timestamps and prototype-like field names", () => {
  const budget = new MemoryBudget(65536);
  const bytes = new Uint8Array([1, 2, 3, 4]);
  const timestamp = new TimestampValue(123n, "nanos");
  const decimal = new DecimalValue(123n, 2);
  const nested = { text: "before", bytes: bytes.subarray(1, 3) };
  const row = { nested, values: [nested, null], timestamp, decimal, integer: 1n };
  Object.defineProperty(row, "__proto__", { value: { safe: true }, enumerable: true });
  const owner = detachOwnedRow(row, budget);
  nested.text = "after";
  bytes.fill(0);
  Object.assign(timestamp, { epochNanoseconds: 999n });
  expect(owner.value.nested).toEqual({ text: "before", bytes: new Uint8Array([2, 3]) });
  expect(owner.value.timestamp).toEqual(new TimestampValue(123n, "nanos"));
  expect(owner.value.decimal).toBe(decimal);
  expect(Object.getPrototypeOf(owner.value)).toBe(Object.prototype);
  expect(Object.hasOwn(owner.value, "__proto__")).toBe(true);
  expect(Object.getOwnPropertyDescriptor(owner.value, "__proto__")?.value).toEqual({ safe: true });
  expect(budget.usedBytes).toBe(owner.bytes);
  owner.release();
  expect(budget.usedBytes).toBe(0);
});

it("admits the measured simultaneous extent exactly and unwinds one byte below", () => {
  const row = { payload: new Uint8Array(10000), nested: [1, "text", { a: true }] };
  const measured = new MemoryBudget(65536);
  detachOwnedRow(row, measured).release();
  const required = measured.peakBytes;
  for (const cap of [required - 1, required]) {
    const pool = new MemoryBudget(cap + 37);
    const sibling = pool.reserve(37, "sibling");
    const budget = pool.child(cap);
    if (cap < required) expect(() => detachOwnedRow(row, budget)).toThrow(/budget/i);
    else {
      const owner = detachOwnedRow(row, budget);
      expect(owner.value).toEqual(row);
      owner.release();
    }
    expect(pool.usedBytes).toBe(37);
    sibling.release();
  }
});

it("rejects cycles and accessors without invoking user code or leaking", () => {
  const budget = new MemoryBudget(65536);
  const cycle: Record<string, unknown> = {};
  cycle.self = cycle;
  expect(() => detachOwnedRow(cycle, budget)).toThrow(/Cyclic/);
  let calls = 0;
  expect(() =>
    detachOwnedRow(
      {
        get payload() {
          calls++;
          return "unexpected";
        },
      },
      budget,
    ),
  ).toThrow(/plain data/);
  expect(calls).toBe(0);
  expect(budget.usedBytes).toBe(0);
});
