import { expect, it } from "vitest";
import { materializeBatchRows } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { concatOwnedBatches } from "./vector-sort.js";

it.each([1670, 1669])("preserves dictionary text and ownership in workerd at cap %i", (cap) => {
  const root = new MemoryBudget(cap);
  const inputs = [0, 1].map((part) => {
    const scope = new MemoryScope(root);
    const indices = scope.allocate(400, "input indices", () =>
      Uint32Array.from({ length: 100 }, (_, i) => i % 2),
    ).value;
    // Source strings are borrowed; the cap covers typed input and construction backing.
    const owner = scope.retainAllocations({
      rowCount: 100,
      columns: {
        value: {
          type: "dict" as const,
          indices,
          dictionary: { type: "utf8" as const, values: [`\ufeffé-${part}`, "😀"] },
        },
      },
    });
    scope.dispose();
    return owner;
  });
  try {
    if (cap === 1669) expect(() => concatOwnedBatches(inputs)).toThrow("budget");
    else {
      const output = concatOwnedBatches(inputs);
      try {
        expect(output.value.columns.value?.type).toBe("dict");
        for (const input of inputs) input.release();
        expect(root.usedBytes).toBe(854);
        expect(root.peakBytes).toBe(1670);
        expect(materializeBatchRows(output.value)).toEqual(
          Array.from({ length: 200 }, (_, i) => ({
            value: i % 2 ? "😀" : `\ufeffé-${i < 100 ? 0 : 1}`,
          })),
        );
      } finally {
        output.release();
      }
    }
  } finally {
    for (const input of inputs) input.release();
  }
  expect(root.usedBytes).toBe(0);
});
