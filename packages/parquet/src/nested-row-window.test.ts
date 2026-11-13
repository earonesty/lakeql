import {
  MemoryBudget,
  MemoryScope,
  type OwnedMemory,
  type Vector,
  vectorFromValues,
  vectorValue,
} from "lakeql-core";
import { expect, it } from "vitest";
import { readNestedRowWindow } from "./nested-row-window.js";

it.each([
  [0, 0],
  [0, 1],
  [2, 4],
  [4, 5],
])("reads exactly window [%i,%i)", async (start, end) => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  let advances = 0,
    closed = false;
  async function* source(): AsyncGenerator<OwnedMemory<Vector>> {
    try {
      for (let i = 0; i < 5; i++) {
        advances++;
        const stage = scope.childScope();
        const owner = stage.retainAllocations(vectorFromValues([i], stage));
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
  const values: unknown[] = [];
  for await (const row of readNestedRowWindow(source(), start, end))
    values.push(vectorValue(row.value, 0));
  expect(values).toEqual(Array.from({ length: end - start }, (_, i) => start + i));
  expect(advances).toBe(end);
  expect(closed).toBe(end !== 0);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("rejects invalid windows before touching the source and detects truncated input", async () => {
  let started = false;
  async function* source(): AsyncGenerator<OwnedMemory<Vector>> {
    started = true;
    yield* [] as OwnedMemory<Vector>[];
  }
  for (const [start, end] of [
    [-1, 1],
    [2, 1],
    [0, Infinity],
    [0.5, 2],
  ])
    await expect(readNestedRowWindow(source(), start, end).next()).rejects.toThrow(/Invalid/);
  expect(started).toBe(false);
  await expect(readNestedRowWindow(source(), 0, 1).next()).rejects.toThrow(/exceeds/);
});
