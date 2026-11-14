import { expect, it } from "vitest";
import { materializeBatchRows, type Vector, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { compactGatherPlan } from "./packed-gather.js";
import { gatherOwnedBatch } from "./vector-sort.js";

const leaves: Vector[] = [
  { type: "null", length: 3 },
  { type: "f32", values: Float32Array.of(1.5, 2.5, 3.5) },
  { type: "f64", values: Float64Array.of(1.5, 2.5, 3.5) },
  { type: "i32", values: Int32Array.of(-1, 2, 3) },
  { type: "u32", values: Uint32Array.of(1, 2, 3) },
  { type: "u8", values: Uint8Array.of(1, 2, 3) },
  { type: "i64", values: BigInt64Array.of(-1n, 9007199254740993n, 3n) },
  { type: "u64", values: BigUint64Array.of(1n, 9007199254740993n, 3n) },
  { type: "timestamp", values: BigInt64Array.of(1n, 2n, 3n), unit: "ns", isAdjustedToUTC: true },
  { type: "bool", values: Uint8Array.of(1, 0, 1) },
  { type: "binary", values: [Uint8Array.of(1, 2), new Uint8Array(), Uint8Array.of(3)] },
  {
    type: "binary-view",
    data: Uint8Array.of(99, 1, 2, 3),
    fixedLength: 1,
    rowOffset: 1,
    length: 3,
  },
  {
    type: "utf8-view",
    data: new TextEncoder().encode("aé"),
    ranges: Uint32Array.of(0, 1, 1, 2, 3, 0),
    length: 3,
  },
];

for (const leaf of leaves) {
  for (const depth of [0, 1, 2]) {
    it(`matches measured compact backing for ${leaf.type} at dictionary depth ${depth}`, () => {
      let vector: Vector = leaf.type === "null" ? leaf : { ...leaf, valid: Uint8Array.of(1, 0, 1) };
      for (let i = 0; i < depth; i++)
        vector = {
          type: "dict",
          indices: Uint32Array.of(2, 0, 1),
          dictionary: vector,
          valid: Uint8Array.of(1, 1, 0),
        };
      for (const indices of [[], [0], [1], [2, 0, 2, 1]]) {
        const batch = { rowCount: 3, columns: { value: vector } };
        const plan = compactGatherPlan(batch, indices);
        const expectedBytes = plan === undefined ? undefined : plan.typedBytes + plan.viewBytes;
        expect(expectedBytes).toBeDefined();
        if (expectedBytes === undefined) throw new Error("Missing shape estimate");
        // Borrowed vector fixtures; owned padding forces the detachable path.
        // The test measures new output, not complete input heap accounting.
        const budget = new MemoryBudget(4096 + expectedBytes);
        const scope = new MemoryScope(budget);
        scope.allocate(4096, "source backing", () => new Uint8Array(4096));
        const input = scope.retainAllocations(batch);
        scope.dispose();
        const output = gatherOwnedBatch(input, indices);
        expect(output.bytes).toBe(expectedBytes);
        expect(budget.peakBytes).toBe(4096 + expectedBytes);
        expect(materializeBatchRows(output.value)).toEqual(
          indices.map((index) => ({ value: vectorValue(vector, index) })),
        );
        input.release();
        expect(budget.usedBytes).toBe(expectedBytes);
        output.release();
        expect(budget.usedBytes).toBe(0);
      }
    });
  }
}
