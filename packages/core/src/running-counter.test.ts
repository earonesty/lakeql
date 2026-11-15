import { expect, it } from "vitest";
import { RunningCounter } from "./running-counter.js";

it("tracks additions, replacements, removals and reset without retaining payloads", () => {
  const counter = new RunningCounter();
  const values = ["a", "雪", '"\\\n', 17];
  const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).length;
  for (let i = 0; i < values.length; i++) {
    counter.add(bytes(values[i]));
    expect(counter.listBytes).toBe(bytes(values.slice(0, i + 1)));
  }
  counter.remove(bytes(values[1]));
  values.splice(1, 1);
  expect(counter.items).toBe(values.length);
  expect(counter.listBytes).toBe(bytes(values));
  counter.clear();
  expect(counter.bytes).toBe(0);
  expect(counter.items).toBe(0);
  expect(counter.listBytes).toBe(2);
});

it("rejects invalid adjustments atomically", () => {
  const counter = new RunningCounter();
  counter.add(10, 2);
  for (const invalid of [-1, NaN, Infinity, 0.5]) {
    expect(() => counter.add(invalid)).toThrow(/counter/);
    expect(() => counter.remove(0, invalid)).toThrow(/counter/);
    expect(counter.bytes).toBe(10);
    expect(counter.items).toBe(2);
  }
  expect(() => counter.remove(11)).toThrow(/counter/);
  expect(() => counter.add(Number.MAX_SAFE_INTEGER)).toThrow(/counter/);
  expect(counter.bytes).toBe(10);
});
