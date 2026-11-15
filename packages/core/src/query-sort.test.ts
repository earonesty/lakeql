import { expect, it } from "vitest";
import { BoundedSelection, mergeSortRuns } from "./query-sort.js";

it.each([
  0, 1, 2, 7, 31, 257, 1000,
])("matches a stable full sort while retaining at most k=%i", (k) => {
  for (const distribution of ["ascending", "descending", "mixed", "equal"]) {
    const rows = Array.from({ length: 513 }, (_, id) => ({
      id,
      key:
        distribution === "ascending"
          ? id
          : distribution === "descending"
            ? -id
            : distribution === "equal"
              ? 1
              : (id * 7919) % 37,
    }));
    const compare = (a: (typeof rows)[number], b: (typeof rows)[number]) => a.key - b.key;
    const selection = new BoundedSelection(k, compare);
    for (const row of rows) {
      selection.add(row);
      expect(selection.size).toBeLessThanOrEqual(k);
    }
    expect(selection.finish()).toEqual([...rows].sort(compare).slice(0, k));
    expect(selection.size).toBe(0);
    expect([...selection.values()]).toEqual([]);
  }
});

it("preserves earlier ties when a better row displaces the latest worst candidate", () => {
  const rows = [
    { key: 2, id: "first" },
    { key: 2, id: "second" },
    { key: 1, id: "third" },
  ];
  const selection = new BoundedSelection(2, (a: (typeof rows)[number], b) => a.key - b.key);
  for (const row of rows) selection.add(row);
  expect(selection.finish().map((row) => row.id)).toEqual(["third", "first"]);
});

it("uses logarithmic candidate maintenance for adverse full-replacement input", () => {
  for (const k of [8, 64, 512]) {
    let comparisons = 0;
    const rows = 8192;
    const selection = new BoundedSelection(k, (a: number, b) => {
      comparisons++;
      return a - b;
    });
    for (let value = rows; value > 0; value--) selection.add(value);
    expect(selection.finish()).toEqual(Array.from({ length: k }, (_, i) => i + 1));
    expect(comparisons).toBeLessThan((rows + k) * (2 * Math.ceil(Math.log2(k)) + 2));
    if (k >= 64) expect(comparisons).toBeLessThan((rows * k) / 2);
  }
});

it("handles empty, unequal and equal-key sort runs in encounter order", () => {
  const runs = [
    [],
    [
      { key: 1, id: "a" },
      { key: 3, id: "b" },
    ],
    [{ key: 1, id: "c" }],
    [],
    [
      { key: 1, id: "d" },
      { key: 2, id: "e" },
    ],
  ];
  const reference = runs.flat().sort((a, b) => a.key - b.key);
  expect(mergeSortRuns(runs, [{ column: "key", direction: "asc" }])).toEqual(reference);
  expect(mergeSortRuns([], [{ column: "key" }])).toEqual([]);
});

it.each([
  -1,
  0.5,
  NaN,
  Infinity,
  Number.MAX_SAFE_INTEGER + 1,
])("rejects invalid capacity %s", (k) => {
  expect(() => new BoundedSelection(k, (a: number, b) => a - b)).toThrow(/safe integer/);
});
