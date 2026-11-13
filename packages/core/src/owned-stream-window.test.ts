import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { readOwnedStreamWindow } from "./owned-stream-window.js";

function stream(chunks: number[][]) {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(Uint8Array.from(chunk));
      controller.close();
    },
  });
}

it("collects a cross-chunk window in exact output capacity and drains the suffix", async () => {
  const budget = new MemoryBudget(3);
  const scope = new MemoryScope(budget);
  const owner = await readOwnedStreamWindow(stream([[0, 1], [], [2, 3], [4, 5, 6]]), scope, {
    offset: 2,
    length: 3,
    maxBytes: 7,
  });
  expect([...owner.value]).toEqual([2, 3, 4]);
  expect(owner.value.byteOffset).toBe(0);
  expect(owner.value.buffer.byteLength).toBe(3);
  expect(budget.peakBytes).toBe(3);
  owner.release();
  expect(budget.usedBytes).toBe(0);
  scope.dispose();
});

it("rejects truncation and excess suffix bytes without leaking output", async () => {
  for (const [chunks, maxBytes] of [
    [[0, 1], 8],
    [[0, 1, 2, 3, 4], 4],
  ] as const) {
    const scope = new MemoryScope(new MemoryBudget(3));
    await expect(
      readOwnedStreamWindow(stream([[...chunks]]), scope, { offset: 1, length: 3, maxBytes }),
    ).rejects.toThrow();
    expect(scope.budget.usedBytes).toBe(0);
    scope.dispose();
  }
});

it("does not allocate output against a smaller transferred reservation", async () => {
  const scope = new MemoryScope(new MemoryBudget(3));
  const reservation = scope.reserve(1, "probe");
  await expect(
    readOwnedStreamWindow(stream([[1, 2, 3]]), scope, {
      offset: 0,
      length: 3,
      maxBytes: 3,
      initialReservation: reservation,
    }),
  ).rejects.toThrow(/reservation/);
  expect(scope.budget.usedBytes).toBe(1);
  reservation.release();
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
