import { expect, it } from "vitest";
import { batchFromColumns, materializeBatchRows } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { vectorTopKBatch, vectorTopKOwnedBatch } from "./vector-sort.js";

it("matches reference top-k across selection, nulls, ties, offsets, and limits", () => {
  const budget = new MemoryBudget(1 << 20);
  const scope = new MemoryScope(budget);
  const batch = batchFromColumns(
    {
      id: Array.from({ length: 129 }, (_, i) => i),
      score: Array.from({ length: 129 }, (_, i) => (i % 7 === 0 ? null : (i * 17) % 23)),
    },
    scope,
  );
  const input = scope.retainAllocations(batch);
  scope.dispose();
  for (const direction of ["asc", "desc"] as const) {
    for (const nulls of ["first", "last"] as const) {
      for (const limit of [0, 1, 7, 128, 200]) {
        for (const offset of [0, 3, 130]) {
          for (const selection of [
            undefined,
            Uint8Array.from({ length: 129 }, (_, i) => (i % 3 === 0 ? 1 : 0)),
          ]) {
            const order = [{ column: "score", direction, nulls }];
            const options = { limit, offset };
            const expected = materializeBatchRows(
              vectorTopKBatch(batch, order, options, selection),
            );
            const output = vectorTopKOwnedBatch(input, order, options, selection);
            expect(materializeBatchRows(output.value)).toEqual(expected);
            output.release();
            expect(budget.usedBytes).toBe(input.bytes);
          }
        }
      }
    }
  }
  input.release();
  expect(budget.usedBytes).toBe(0);
});

it("admits heap and output overlap at the exact cap and cleans up on failure", () => {
  function run(maxBytes: number) {
    const budget = new MemoryBudget(maxBytes);
    const scope = new MemoryScope(budget);
    const batch = batchFromColumns({ id: [9, 2, 7, 1] }, scope);
    const input = scope.retainAllocations(batch);
    scope.dispose();
    try {
      const output = vectorTopKOwnedBatch(input, [{ column: "id" }], { limit: 2 });
      input.release();
      expect(materializeBatchRows(output.value)).toEqual([{ id: 1 }, { id: 2 }]);
      output.release();
      return budget.peakBytes;
    } finally {
      input.release();
      expect(budget.usedBytes).toBe(0);
    }
  }
  const peak = run(65536);
  expect(run(peak)).toBe(peak);
  expect(() => run(peak - 1)).toThrow();
});

it("sizes the admitted heap from actual selected rows rather than an oversized limit", () => {
  const root = new MemoryBudget(65536);
  const scope = new MemoryScope(root);
  const batch = batchFromColumns({ id: Array.from({ length: 128 }, (_, i) => i) }, scope);
  const input = scope.retainAllocations(batch);
  scope.dispose();
  const selected = new Uint8Array(128);
  selected[73] = 1;
  // One index plus one Float64 output, in addition to the pinned input.
  const child = root.child(input.bytes + 4 + 8);
  const pin = input.retainIn(child);
  const output = vectorTopKOwnedBatch(pin, [{ column: "id" }], { limit: 1000000 }, selected);
  expect(materializeBatchRows(output.value)).toEqual([{ id: 73 }]);
  expect(child.peakBytes).toBe(input.bytes + 12);
  output.release();
  pin.release();
  expect(child.usedBytes).toBe(0);
  input.release();
  expect(root.usedBytes).toBe(0);
});

it.each([
  "ascending",
  "descending",
  "equal",
])("preserves reference results for %s selected input", (shape) => {
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  const batch = batchFromColumns(
    {
      id: Array.from({ length: 64 }, (_, i) => i),
      score: Array.from({ length: 64 }, (_, i) =>
        shape === "ascending" ? i : shape === "descending" ? -i : 1,
      ),
    },
    scope,
  );
  const input = scope.retainAllocations(batch);
  scope.dispose();
  for (const direction of ["asc", "desc"] as const) {
    for (const selection of [
      undefined,
      Uint8Array.from({ length: 64 }, (_, i) => (i % 3 === 0 ? 1 : 0)),
      new Uint8Array(64),
    ]) {
      for (const options of [
        { limit: 7, offset: 3 },
        { limit: 100 },
        { limit: 0 },
        { limit: 2, offset: 70 },
      ]) {
        const order = [{ column: "score", direction }];
        const output = vectorTopKOwnedBatch(input, order, options, selection);
        expect(materializeBatchRows(output.value)).toEqual(
          materializeBatchRows(vectorTopKBatch(batch, order, options, selection)),
        );
        output.release();
      }
    }
  }
  input.release();
  expect(budget.usedBytes).toBe(0);
});
