import { expect, it } from "vitest";
import { visitLevelRuns } from "./level-runs.js";

function reader(bytes: number[]) {
  return { view: new DataView(Uint8Array.from(bytes).buffer), offset: 0 };
}

it.each([
  0, 1, 2, 8, 16, 32,
])("visits RLE level domains of width %i with bounded output", (width) => {
  const value = width === 32 ? 0xffffffff : 2 ** width - 1;
  const bytes = [
    6,
    ...Array.from(
      { length: Math.ceil(width / 8) },
      (_, i) => Math.floor(value / 2 ** (8 * i)) & 255,
    ),
  ];
  const input = reader(bytes);
  const visits: number[][] = [];
  visitLevelRuns(input, width, 3, (...run) => visits.push(run), bytes.length, true);
  expect(visits).toEqual([[value, 0, 3]]);
  expect(input.offset).toBe(bytes.length);
});

it("bounds padded packed runs to requested levels and honors explicit extents", () => {
  const input = reader([3, 0b11100100, 0b11100100, 99]);
  const levels: number[] = [];
  visitLevelRuns(input, 2, 5, (value) => levels.push(value), 3, true);
  expect(levels).toEqual([0, 1, 2, 3, 0]);
  expect(input.offset).toBe(3);
});

it("rejects invalid dimensions before reading or visiting", () => {
  for (const [width, length, offset] of [
    [-1, 1, 0],
    [33, 1, 0],
    [1.5, 1, 0],
    [1, -1, 0],
    [1, 0x100000000, 0],
    [1, 1, -1],
  ]) {
    const input = reader([2, 0]);
    input.offset = offset;
    let called = false;
    expect(() =>
      visitLevelRuns(
        input,
        width,
        length,
        () => {
          called = true;
        },
        2,
      ),
    ).toThrow(/Malformed/);
    expect(called).toBe(false);
    expect(input.offset).toBe(offset);
  }
});

it("rejects truncated runs, oversized RLE output, and trailing encoded bytes", () => {
  for (const bytes of [[0], [4, 1], [3], [2], [2, 2], [2, 0, 99]])
    expect(() => visitLevelRuns(reader(bytes), 1, 1, () => {}, bytes.length, true)).toThrow(
      /Malformed/,
    );
});
