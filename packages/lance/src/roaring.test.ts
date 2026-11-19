import { describe, expect, it } from "vitest";
import { roaringContains } from "./roaring.js";

describe("portable Roaring fragment coverage", () => {
  it("checks array containers without expanding row sets", () => {
    const bytes = new Uint8Array(22);
    const view = new DataView(bytes.buffer);
    view.setUint32(0, 12346, true);
    view.setUint32(4, 1, true);
    view.setUint16(8, 2, true);
    view.setUint16(10, 2, true);
    view.setUint32(12, 16, true);
    [0, 7, 65535].forEach((v, i) => {
      view.setUint16(16 + i * 2, v, true);
    });
    const contains = roaringContains(bytes);
    expect([0, 7, 65535].map((v) => contains(2 * 65536 + v))).toEqual([true, true, true]);
    expect(contains(7)).toBe(false);
    expect(contains(2 * 65536 + 8)).toBe(false);
    expect(contains(2 ** 32)).toBe(false);
    view.setUint32(12, 17, true);
    expect(() => roaringContains(bytes)).toThrow(/Invalid portable Roaring/);
  });
  it("supports run containers with the small-directory layout", () => {
    const bytes = new Uint8Array(15);
    const view = new DataView(bytes.buffer);
    view.setUint32(0, 12347, true);
    bytes[4] = 1;
    view.setUint16(5, 0, true);
    view.setUint16(7, 4, true);
    view.setUint16(9, 1, true);
    view.setUint16(11, 10, true);
    view.setUint16(13, 4, true);
    const contains = roaringContains(bytes);
    expect([9, 10, 14, 15].map(contains)).toEqual([false, true, true, false]);
    view.setUint16(13, 5, true);
    expect(() => roaringContains(bytes)).toThrow(/Invalid portable Roaring/);
  });
  it("supports dense bitset containers and rejects truncation", () => {
    const bytes = new Uint8Array(16 + 8192);
    const view = new DataView(bytes.buffer);
    view.setUint32(0, 12346, true);
    view.setUint32(4, 1, true);
    view.setUint16(10, 65535, true);
    view.setUint32(12, 16, true);
    bytes.fill(255, 16);
    const contains = roaringContains(bytes);
    expect(contains(0)).toBe(true);
    expect(contains(65535)).toBe(true);
    expect(contains(65536)).toBe(false);
    expect(() => roaringContains(bytes.subarray(0, -1))).toThrow(/Invalid portable Roaring/);
    expect(() => roaringContains(new Uint8Array(3))).toThrow(/Invalid portable Roaring/);
  });
});
