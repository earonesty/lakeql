import { expect, it } from "vitest";
import { type Batch, vectorFromValues, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import { compactOwnedUnionBatch } from "./union-compaction.js";
import { concatOwnedBatches, gatherOwnedBatch } from "./vector-sort.js";

it.each([
  64, 128,
])("isolates concurrent nested selection jobs in a %i MiB Worker pool", async (mib) => {
  const pool = new MemoryBudget(mib * 1024 * 1024);
  const cacheScope = new MemoryScope(pool.child(256 * 1024));
  const cache = cacheScope.allocate(
    256 * 1024,
    "shared cached backing",
    () => new Uint8Array(256 * 1024),
  );
  cache.value[0] = 73;
  try {
    await Promise.all(
      Array.from({ length: 4 }, async (_, job) => {
        const budget = pool.child(32 * 1024);
        const input = (values: unknown[]): OwnedMemory<Batch> => {
          const scope = new MemoryScope(budget);
          try {
            return scope.retainAllocations({
              rowCount: values.length,
              columns: { v: vectorFromValues(values, scope) },
            });
          } finally {
            scope.dispose();
          }
        };
        const valueAt = (row: number) => ({ job, nested: { values: [row, `row-${row}`, null] } });
        let output = input(["seed", 0]);
        try {
          for (let row = 1; row <= 128; row++) {
            const next = input([valueAt(row)]);
            const joined = (() => {
              try {
                return concatOwnedBatches([output, next], budget);
              } finally {
                next.release();
              }
            })();
            try {
              const selected = gatherOwnedBatch(joined, [2, 0], budget);
              output.release();
              output = selected;
            } finally {
              joined.release();
            }
            const vector = output.value.columns.v;
            if (vector === undefined) throw Error("Missing output vector");
            expect(vectorValue(vector, 0)).toEqual(valueAt(row));
            expect(vectorValue(vector, 1)).toEqual(row === 1 ? "seed" : valueAt(row - 1));
            expect(budget.usedBytes).toBeLessThan(16 * 1024);
            if (row === 64) {
              const rejected = pool.child(1024);
              expect(() => compactOwnedUnionBatch(output, rejected)).toThrow(/budget/);
              expect(rejected.usedBytes).toBe(0);
              expect(vectorValue(vector, 0)).toEqual(valueAt(row));
            }
            // Cooperatively interleave jobs while each retains its current output.
            await Promise.resolve();
          }
        } finally {
          output.release();
        }
        expect(budget.usedBytes).toBe(0);
        expect(budget.peakBytes).toBeLessThanOrEqual(32 * 1024);
      }),
    );
    expect(cache.value[0]).toBe(73);
    expect(pool.usedBytes).toBe(256 * 1024);
    expect(pool.peakBytes).toBeLessThanOrEqual(256 * 1024 + 4 * 32 * 1024);
  } finally {
    cacheScope.dispose();
  }
  expect(pool.usedBytes).toBe(0);
});
