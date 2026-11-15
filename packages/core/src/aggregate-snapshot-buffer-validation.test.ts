import { expect, it } from "vitest";
import type { VectorAggregateStateSnapshot } from "./vector-aggregate.js";
import { vectorAggregateStateFromSnapshot } from "./vector-aggregate-snapshot.js";

it.each([
  "median",
  "quantile",
] as const)("validates every %s checkpoint entry without a scope", (op) => {
  for (const value of [null, undefined, true, {}, [], 1n, NaN, Infinity, -Infinity]) {
    const snapshot = {
      op,
      quantile: 0.5,
      values: [1, value, 2],
    } as unknown as VectorAggregateStateSnapshot;
    expect(() => vectorAggregateStateFromSnapshot(snapshot)).toThrow(/checkpoint scalar/);
  }
  const snapshot = { op, quantile: 0.5, values: [1, 2] } as VectorAggregateStateSnapshot;
  const restored = vectorAggregateStateFromSnapshot(snapshot);
  expect(restored).toMatchObject({ values: [1, 2] });
  expect((restored as { values: unknown }).values).not.toBe(snapshot.values);
});

it("accepts median text but rejects text in numeric quantile checkpoints", () => {
  expect(vectorAggregateStateFromSnapshot({ op: "median", values: ["a", "b"] })).toMatchObject({
    values: ["a", "b"],
  });
  expect(() =>
    vectorAggregateStateFromSnapshot({
      op: "quantile",
      quantile: 0.5,
      values: ["a"],
    } as unknown as VectorAggregateStateSnapshot),
  ).toThrow(/checkpoint scalar/);
});
