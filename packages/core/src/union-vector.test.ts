import { expect, it } from "vitest";
import {
  materializeBatchRows,
  scalarVectorValue,
  type Vector,
  vectorLength,
  vectorValue,
} from "./batch.js";
import { sharedGatherPlan } from "./gather-plan.js";
import { planBatchMaterialization } from "./materialization-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { serializedVectorBytes } from "./serialized-size.js";
import { buildUnionChildren } from "./union-construction.js";
import { retainedVectorBytes } from "./vector-retained-bytes.js";
import { sliceVector } from "./vector-slice.js";
import { gatherBatch } from "./vector-sort.js";

it("preserves union values through traversal, slices, gathers, and admitted materialization", () => {
  const values = ["雪", [1, null], null, "end"];
  const scope = new MemoryScope(new MemoryBudget(65536));
  const built = buildUnionChildren(
    { length: values.length, get: (row) => values[row] },
    3,
    (row) => [0, 1, 2, 0][row] ?? 0,
    scope,
  );
  const vector: Vector = {
    type: "union",
    tags: built.routing.tags,
    offsets: built.routing.offsets,
    children: built.children,
  };
  expect(vectorLength(vector)).toBe(4);
  expect(values.map((_, row) => vectorValue(vector, row))).toEqual(values);
  expect(scalarVectorValue(vector, 0)).toBe("雪");
  expect(() => scalarVectorValue(vector, 1)).toThrow(/scalar/);
  expect(scalarVectorValue(vector, 2)).toBe(null);
  const slice = sliceVector(vector, 1, 4);
  expect(materializeBatchRows({ rowCount: 3, columns: { value: slice } })).toEqual(
    values.slice(1).map((value) => ({ value })),
  );
  expect(retainedVectorBytes(slice)).toBe(retainedVectorBytes(vector));
  const batch = { rowCount: 4, columns: { a: vector, b: vector } };
  const indices = [3, 1, 3];
  const plan = sharedGatherPlan(batch, indices);
  expect(plan).toEqual({ typedBytes: 12, retainsInput: true, createsJavaScriptArrays: false });
  const gatherScope = new MemoryScope(new MemoryBudget(12));
  const gathered = gatherBatch(batch, indices, gatherScope);
  expect(materializeBatchRows(gathered)).toEqual(
    indices.map((row) => ({ a: values[row], b: values[row] })),
  );
  expect(gatherScope.budget.usedBytes).toBe(12);
  gatherScope.dispose();
  const materialization = planBatchMaterialization({ rowCount: 4, columns: { value: vector } });
  const output = materialization.execute(scope);
  expect(output.value).toEqual(values.map((value) => ({ value })));
  for (let row = 0; row < values.length; row++)
    expect(serializedVectorBytes(vector, row)).toBe(
      new TextEncoder().encode(JSON.stringify(values[row])).length,
    );
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("rejects malformed union rows rather than substituting values", () => {
  for (const vector of [
    {
      type: "union" as const,
      tags: Uint8Array.of(1),
      offsets: Uint32Array.of(0),
      children: [{ type: "null" as const, length: 1 }],
    },
    {
      type: "union" as const,
      tags: Uint8Array.of(0),
      offsets: new Uint32Array(),
      children: [{ type: "null" as const, length: 1 }],
    },
    {
      type: "union" as const,
      tags: Uint8Array.of(0),
      offsets: Uint32Array.of(2),
      children: [{ type: "null" as const, length: 1 }],
    },
  ]) {
    expect(() => vectorValue(vector, 0)).toThrow();
    expect(() => scalarVectorValue(vector, 0)).toThrow();
    expect(() => planBatchMaterialization({ rowCount: 1, columns: { value: vector } })).toThrow();
  }
});

it("infers recursive unions under exact admission and rolls back exhausted construction", async () => {
  const { vectorFromValues } = await import("./batch.js");
  const values = [[1, "two", null], { nested: [false, { x: "value" }] }, null, "tail"];
  const scope = new MemoryScope(new MemoryBudget(65536));
  const vector = vectorFromValues(values, scope);
  expect(vector.type).toBe("union");
  expect(values.map((_, i) => vectorValue(vector, i))).toEqual(values);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(vectorFromValues(values, exact).type).toBe("union");
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => vectorFromValues(values, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
  expect(vectorFromValues([1, 2, 3]).type).toBe("f64");
  expect(vectorFromValues(["a", "b"]).type).toBe("utf8");
});
