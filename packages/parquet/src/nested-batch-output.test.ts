import {
  MemoryBudget,
  MemoryScope,
  type OwnedMemory,
  type Vector,
  vectorFromValues,
  vectorValue,
} from "lakeql-core";
import { expect, it } from "vitest";
import { readNestedBatchOutputs } from "./nested-batch-output.js";

it("transfers public batch owners with offsets and predicate metadata across source close", async () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  let closed = false;
  async function* source(): AsyncGenerator<OwnedMemory<Vector>> {
    try {
      for (const values of [[{ a: 1 }, { a: 2 }], [{ a: 3 }]]) {
        const stage = scope.childScope();
        const owner = stage.retainAllocations(vectorFromValues(values, stage));
        stage.dispose();
        try {
          yield owner;
        } finally {
          owner.release();
        }
      }
    } finally {
      closed = true;
    }
  }
  const iterator = readNestedBatchOutputs(source(), scope, 7, true);
  const first = await iterator.next(),
    second = await iterator.next();
  if (first.done || second.done) throw Error("Missing outputs");
  await iterator.return(undefined);
  scope.dispose();
  try {
    expect(closed).toBe(true);
    expect(first.value.value.rowOffset).toBe(7);
    expect(second.value.value.rowOffset).toBe(9);
    expect(first.value.value.residualPredicateSatisfied).toBe(true);
    const a = first.value.value.batch.columns.a;
    if (!a) throw Error("Missing field");
    expect(vectorValue(a, 1)).toBe(2);
  } finally {
    first.value.release();
    second.value.release();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it.each([
  "admission",
  "shape",
  "overflow",
])("cleans bridge inputs after %s rejection", async (mode) => {
  const pool = new MemoryBudget(65536);
  const input = new MemoryScope(pool.child(32768));
  const output = new MemoryScope(pool.child(mode === "admission" ? 0 : 32768));
  let closed = false;
  async function* source(): AsyncGenerator<OwnedMemory<Vector>> {
    const stage = input.childScope();
    const vector = vectorFromValues(mode === "shape" ? [1] : [{ a: 1 }], stage);
    const owner = stage.retainAllocations(vector);
    stage.dispose();
    try {
      yield owner;
    } finally {
      owner.release();
      closed = true;
    }
  }
  const iterator = readNestedBatchOutputs(
    source(),
    output,
    mode === "overflow" ? Number.MAX_SAFE_INTEGER : 0,
    false,
  );
  await expect(iterator.next()).rejects.toThrow(
    mode === "admission" ? /budget/ : mode === "shape" ? /all-present/ : /overflow/,
  );
  expect(closed).toBe(true);
  expect(pool.usedBytes).toBe(0);
  input.dispose();
  output.dispose();
});
