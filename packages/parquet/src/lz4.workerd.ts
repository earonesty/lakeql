import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { scopedParquetCompressors } from "./compressors.js";

it("decodes LZ4 raw under exact output admission in workerd", () => {
  const input = Uint8Array.of(0x13, 97, 1, 0, 0x50, 98, 99, 100, 101, 102);
  const scope = new MemoryScope(new MemoryBudget(13));
  scope.admit(13, "LZ4 block");
  expect(codec(scope)(input, 13)).toEqual(new TextEncoder().encode("aaaaaaaabcdef"));
  expect(scope.budget.usedBytes).toBe(13);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(12));
  expect(() => codec(tight)(input, 13)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

function codec(scope: MemoryScope) {
  const decode = scopedParquetCompressors(scope).LZ4_RAW;
  if (decode === undefined) throw new Error("Missing LZ4 raw codec");
  return decode;
}

it("admits one output for legacy Hadoop and raw LZ4 in workerd", () => {
  for (const input of [
    Uint8Array.of(0, 0, 0, 1, 0, 0, 0, 2, 0x10, 120),
    Uint8Array.of(0x10, 120),
  ]) {
    const scope = new MemoryScope(new MemoryBudget(1));
    const decode = scopedParquetCompressors(scope).LZ4;
    if (!decode) throw new Error("Missing legacy LZ4");
    expect(decode(input, 1)).toEqual(Uint8Array.of(120));
    expect(scope.budget.usedBytes).toBe(1);
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
    const tight = new MemoryScope(new MemoryBudget(0));
    const limited = scopedParquetCompressors(tight).LZ4;
    if (!limited) throw new Error("Missing legacy LZ4");
    expect(() => limited(input, 1)).toThrow(/budget/);
    expect(tight.budget.usedBytes).toBe(0);
    tight.dispose();
  }
});
