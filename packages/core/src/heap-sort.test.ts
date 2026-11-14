import { expect, it } from "vitest";
import { heapSort } from "./heap-sort.js";

it("matches lexical ordering for dense field names including Unicode and duplicates", () => {
  const values = ["z", "", "__proto__", "a", "a", "😀", "\ud800", "é", "10", "2"];
  const expected = [...values].sort();
  heapSort(values, (a, b) => (a < b ? -1 : a > b ? 1 : 0));
  expect(values).toEqual(expected);
});

it("sorts dense typed arrays across empty, singleton, repeated, and wide inputs", () => {
  for (const size of [0, 1, 2, 3, 255, 1024]) {
    const values = Uint32Array.from({ length: size }, (_, i) => (i * 7919 + 13) % 97);
    const expected = Array.from(values).sort((a, b) => a - b);
    heapSort(values, (a, b) => a - b);
    expect(Array.from(values)).toEqual(expected);
  }
});
