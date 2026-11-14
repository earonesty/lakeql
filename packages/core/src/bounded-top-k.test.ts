import { describe, expect, it } from "vitest";
import { BoundedTopK } from "./bounded-top-k.js";
import type { OrderByTerm } from "./query.js";
import { compareRows } from "./query-sort.js";

describe("bounded stable top-k", () => {
  it("matches stable full sorting with ties, nulls, directions, and multiple keys", () => {
    const rows = Array.from({ length: 1000 }, (_, id) => ({
      id,
      a: id % 11 === 0 ? null : (id * 17) % 29,
      b: `v${id % 3}`,
    }));
    for (const direction of ["asc", "desc"] as const) {
      for (const nulls of ["first", "last"] as const) {
        const order: OrderByTerm[] = [
          { column: "a", direction, nulls },
          { column: "b", direction: "desc" },
        ];
        const compare = (a: (typeof rows)[number], b: (typeof rows)[number]) =>
          compareRows(a, b, order);
        const sorted = [...rows].sort(compare);
        for (const k of [0, 1, 2, 10, 127, 500, 1000, 1001]) {
          const heap = new BoundedTopK(k, compare);
          for (const row of rows) {
            heap.add(row);
            expect(heap.size).toBeLessThanOrEqual(k);
          }
          expect(heap.drainSorted()).toEqual(sorted.slice(0, k));
          expect(heap.size).toBe(0);
        }
      }
    }
  });

  it("keeps early ties when replacing a later worst candidate", () => {
    const rows = [
      { id: 0, score: 2 },
      { id: 1, score: 2 },
      { id: 2, score: 1 },
    ];
    const heap = new BoundedTopK<(typeof rows)[number]>(2, (a, b) => a.score - b.score);
    for (const row of rows) heap.add(row);
    expect(heap.drainSorted()).toEqual([rows[2], rows[0]]);
  });

  it("bounds comparison growth and rejects invalid capacities", () => {
    let comparisons = 0;
    const heap = new BoundedTopK<number>(1000, (a, b) => {
      comparisons += 1;
      return a - b;
    });
    for (let i = 20_000; i > 0; i -= 1) heap.add(i);
    expect(heap.drainSorted()).toEqual(Array.from({ length: 1000 }, (_, i) => i + 1));
    expect(comparisons).toBeLessThan(500_000);
    for (const k of [-1, 1.5, Infinity, NaN, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => new BoundedTopK(k, (a: number, b: number) => a - b)).toThrow();
    }
  });
});

it.each([0, -0, NaN])("rejects later ties without replacing earlier values: %s", (comparison) => {
  const heap = new BoundedTopK<number>(1, () => comparison);
  heap.add(7);
  const rejection = heap.add(8);
  expect(rejection).toEqual({ accepted: false });
  expect(heap.add(9)).toBe(rejection);
  expect(Object.isFrozen(rejection)).toBe(true);
  expect(heap.drainSorted()).toEqual([7]);
});

it("reuses rejection results while still accepting a better candidate", () => {
  const heap = new BoundedTopK<number>(2, (a, b) => a - b);
  heap.add(3);
  heap.add(4);
  const rejection = heap.add(5);
  for (let value = 6; value < 1000; value++) expect(heap.add(value)).toBe(rejection);
  expect(heap.add(2)).toEqual({ accepted: true, removed: [4] });
  expect(heap.drainSorted()).toEqual([2, 3]);
});
