import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Batch, materializeBatchRows } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { gatherSelectedBatch } from "./vector-sort.js";

const input: Batch = {
  rowCount: 3,
  columns: {
    text: {
      type: "utf8-view",
      data: new TextEncoder().encode("onetwothree"),
      ranges: new Uint32Array([0, 3, 3, 6, 6, 11]),
      length: 3,
    },
  },
};

it("releases temporary indices while retaining independent dictionary output indices", () => {
  const budget = new MemoryBudget(c.view + 16);
  const scope = new MemoryScope(budget);
  try {
    const output = gatherSelectedBatch(input, new Uint8Array([1, 0, 1]), scope);
    expect(budget.peakBytes).toBe(c.view + 16); // Temporary and output index buffers overlap.
    expect(budget.usedBytes).toBe(8);
    expect(materializeBatchRows(output)).toEqual([{ text: "one" }, { text: "three" }]);
    const owner = scope.retainAllocations(output);
    scope.dispose();
    try {
      expect(budget.usedBytes).toBe(8);
      expect(materializeBatchRows(owner.value)).toEqual([{ text: "one" }, { text: "three" }]);
    } finally {
      owner.release();
    }
    expect(budget.usedBytes).toBe(0);
  } finally {
    scope.dispose();
  }
});

it("releases index workspace when output admission fails one byte below overlap", () => {
  const budget = new MemoryBudget(c.view + 15);
  const scope = new MemoryScope(budget);
  try {
    expect(() => gatherSelectedBatch(input, new Uint8Array([1, 0, 1]), scope)).toThrow();
    expect(budget.usedBytes).toBe(0);
    expect(budget.peakBytes).toBe(c.view + 8);
  } finally {
    scope.dispose();
  }
});
