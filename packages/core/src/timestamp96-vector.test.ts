import { expect, it } from "vitest";
import {
  batchFromVectors,
  predicateSelection,
  scalarVectorValue,
  type Vector,
  vectorValue,
} from "./batch.js";
import { planConcat } from "./concat-plan.js";
import { gte } from "./expr.js";
import { allocateInt96Storage, readInt96, writeInt96 } from "./int96-storage.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { type TimestampUnit, timestampFromEpoch } from "./timestamp.js";
import {
  createVectorGroupByState,
  finalizeVectorGroupByRows,
  updateVectorGroupByState,
} from "./vector-group-by.js";
import { retainedVectorBytes } from "./vector-retained-bytes.js";
import { sliceVector } from "./vector-slice.js";
import { concatOwnedBatches, gatherOwnedBatch, vectorSortIndices } from "./vector-sort.js";

function wide(
  values: bigint[],
  unit: TimestampUnit = "nanos",
): Extract<Vector, { type: "timestamp96" }> {
  const storage = allocateInt96Storage(values.length);
  for (const [index, value] of values.entries()) writeInt96(storage, index, value);
  return { type: "timestamp96", ...storage, unit, isAdjustedToUTC: true };
}

it("preserves wide timestamp scalar access, borrowed slices, ordering and predicates", () => {
  const value = 1n << 64n;
  const vector = wide([value, -value, value + 1n]);
  const batch = batchFromVectors({ at: vector });
  expect(vectorValue(vector, 0)).toMatchObject({ epochNanoseconds: value });
  expect(scalarVectorValue(vector, 1)).toMatchObject({ epochNanoseconds: -value });
  expect(vectorSortIndices(batch, [{ column: "at", direction: "asc" }])).toEqual([1, 0, 2]);
  expect(predicateSelection(batch, gte("at", timestampFromEpoch(value, "nanos")))).toEqual(
    Uint8Array.of(1, 0, 1),
  );
  const slice = sliceVector(vector, 1, 2);
  expect(vectorValue(slice, 0)).toMatchObject({ epochNanoseconds: -value });
  expect(retainedVectorBytes(slice)).toBe(36);
  expect(retainedVectorBytes(vector)).toBe(36);
});

it("admits wide gather exactly and retains detached output after input release", () => {
  const vector = wide([-(1n << 95n), (1n << 95n) - 1n]);
  for (const cap of [35, 36]) {
    const scope = new MemoryScope(new MemoryBudget(cap));
    const input = scope.retainAllocations(batchFromVectors({ at: vector }));
    try {
      if (cap === 35) {
        expect(() => gatherOwnedBatch(input, [1, 0, 1])).toThrow(/budget/);
        expect(scope.budget.peakBytes).toBe(0);
      } else {
        const output = gatherOwnedBatch(input, [1, 0, 1]);
        input.release();
        scope.dispose();
        expect(scope.budget.usedBytes).toBe(36);
        expect(vectorValue(output.value.columns.at ?? vector, 1)).toMatchObject({
          epochNanoseconds: -(1n << 95n),
        });
        output.release();
      }
    } finally {
      input.release();
      scope.dispose();
    }
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it("plans and scales wide timestamp concatenation without object intermediates", () => {
  const vectors = [wide([1n << 64n], "micros"), wide([7n])];
  const batches = vectors.map((at) => batchFromVectors({ at }));
  expect(planConcat(batches)?.bytes).toBe(24);
  const scope = new MemoryScope(new MemoryBudget(24));
  const inputs = batches.map((batch) => scope.retainAllocations(batch));
  const output = concatOwnedBatches(inputs);
  const at = output.value.columns.at;
  if (at?.type !== "timestamp96") throw new Error("Missing wide output");
  expect(readInt96(at, 0)).toBe((1n << 64n) * 1000n);
  expect(readInt96(at, 1)).toBe(7n);
  expect(at.unit).toBe("nanos");
  output.release();
  for (const input of inputs) input.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  expect(() =>
    planConcat([
      batchFromVectors({ at: wide([(1n << 95n) - 1n], "millis") }),
      batchFromVectors({ at: wide([7n]) }),
    ]),
  ).toThrow(/96-bit/);
});

it("uses full wide epochs for grouping keys", () => {
  const value = 1n << 64n;
  const state = createVectorGroupByState(["at"], { rows: { op: "count" } });
  updateVectorGroupByState(state, batchFromVectors({ at: wide([value, value + 1n, value]) }));
  expect(finalizeVectorGroupByRows(state)).toEqual([
    { at: timestampFromEpoch(value, "nanos"), rows: 2 },
    { at: timestampFromEpoch(value + 1n, "nanos"), rows: 1 },
  ]);
});

it("automatically constructs wide batches at exact capacity and rejects beyond signed 96 bits", async () => {
  const { vectorFromValues } = await import("./batch.js");
  for (const unit of ["millis", "micros", "nanos"] as const) {
    const min = -(1n << 95n),
      max = (1n << 95n) - 1n;
    const input = [timestampFromEpoch(min, unit), timestampFromEpoch(max, unit)];
    for (const cap of [23, 24]) {
      const scope = new MemoryScope(new MemoryBudget(cap));
      if (cap === 23) {
        expect(() => vectorFromValues(input, scope)).toThrow(/budget/);
        expect(scope.budget.peakBytes).toBe(0);
      } else {
        const vector = vectorFromValues(input, scope);
        expect(vector.type).toBe("timestamp96");
        expect([vectorValue(vector, 0), vectorValue(vector, 1)]).toEqual(input);
        expect(scope.budget.usedBytes).toBe(24);
      }
      scope.dispose();
      expect(scope.budget.usedBytes).toBe(0);
    }
    for (const value of [min - 1n, max + 1n]) {
      const scope = new MemoryScope(new MemoryBudget(12));
      expect(() => vectorFromValues([timestampFromEpoch(value, unit)], scope)).toThrow(/96-bit/);
      expect(scope.budget.peakBytes).toBe(0);
      scope.dispose();
    }
  }
});

it("concatenates mixed widths in either order and reconstructs grouped wide batches", async () => {
  const { batchFromColumns } = await import("./batch.js");
  const { concatBatches } = await import("./vector-sort.js");
  const { finalizeVectorGroupByBatch } = await import("./vector-group-by.js");
  const large = timestampFromEpoch(1n << 64n, "nanos");
  const small = timestampFromEpoch(3n, "nanos");
  const batches = [batchFromColumns({ at: [small] }), batchFromColumns({ at: [large] })];
  for (const input of [batches, [...batches].reverse()]) {
    expect(planConcat(input)?.bytes).toBe(24);
    const scope = new MemoryScope(new MemoryBudget(24));
    const owners = input.map((batch) => scope.retainAllocations(batch));
    const output = concatOwnedBatches(owners);
    expect(output.value.columns.at?.type).toBe("timestamp96");
    expect(concatBatches(input)).toEqual(output.value);
    const state = createVectorGroupByState(["at"], { rows: { op: "count" } });
    updateVectorGroupByState(state, output.value);
    const grouped = finalizeVectorGroupByBatch(state);
    expect(grouped.columns.at?.type).toBe("timestamp96");
    expect(grouped.rowCount).toBe(2);
    output.release();
    for (const owner of owners) owner.release();
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it("preserves shared wide timestamp dictionaries within the planned index extent", () => {
  const dictionary = wide([1n << 64n, 3n]);
  const batches = [0, 1].map((index) =>
    batchFromVectors({
      at: { type: "dict", dictionary, indices: Uint32Array.of(index, index) },
    }),
  );
  expect(planConcat(batches)?.bytes).toBe(16);
  for (const cap of [15, 16]) {
    const scope = new MemoryScope(new MemoryBudget(cap));
    const inputs = batches.map((batch) => scope.retainAllocations(batch));
    if (cap === 15) expect(() => concatOwnedBatches(inputs)).toThrow(/budget/);
    else {
      const output = concatOwnedBatches(inputs);
      expect(output.value.columns.at).toMatchObject({ type: "dict", dictionary });
      const at = output.value.columns.at;
      if (at === undefined) throw new Error("Missing timestamp output");
      expect(vectorValue(at, 2)).toMatchObject({ epochNanoseconds: 3n });
      output.release();
    }
    for (const input of inputs) input.release();
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});
