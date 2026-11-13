import { describe, expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { readOwnedStream } from "./owned-stream.js";

function chunks(...parts: number[][]): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      for (const part of parts) controller.enqueue(Uint8Array.from(part));
      controller.close();
    },
  });
}

describe("owned stream admission", () => {
  it("reserves known decoded length once and releases all ownership", async () => {
    const budget = new MemoryBudget(4);
    const scope = new MemoryScope(budget);
    const output = await readOwnedStream(chunks([1], [2, 3, 4]), scope, {
      maxBytes: 4,
      knownLength: 4,
    });
    expect([...output.value]).toEqual([1, 2, 3, 4]);
    expect(budget.peakBytes).toBe(4);
    output.release();
    expect(budget.usedBytes).toBe(0);
    scope.dispose();
  });

  it("charges both buffers during growth and cleans up rejected growth", async () => {
    const budget = new MemoryBudget(5);
    const scope = new MemoryScope(budget);
    await expect(
      readOwnedStream(chunks([1, 2], [3, 4]), scope, { maxBytes: 4 }),
    ).rejects.toMatchObject({
      code: "LAKEQL_BUDGET_EXCEEDED",
    });
    expect(budget.usedBytes).toBe(0);
    expect(budget.peakBytes).toBe(2);
    scope.dispose();
  });

  it("cancels oversized and invalid inputs without reading their bodies", async () => {
    for (const options of [
      { maxBytes: 3, knownLength: 4 },
      { maxBytes: -1 },
      { maxBytes: Infinity },
    ]) {
      let cancelled = 0;
      const stream = new ReadableStream<Uint8Array>({
        cancel() {
          cancelled += 1;
        },
      });
      const budget = new MemoryBudget(10);
      const scope = new MemoryScope(budget);
      await expect(readOwnedStream(stream, scope, options)).rejects.toBeDefined();
      expect(cancelled).toBe(1);
      expect(budget.peakBytes).toBe(0);
      scope.dispose();
    }
  });

  it("retains full backing when an advertised length exceeds delivered data", async () => {
    const budget = new MemoryBudget(8);
    const scope = new MemoryScope(budget);
    const output = await readOwnedStream(chunks([7]), scope, { maxBytes: 8, knownLength: 8 });
    expect([...output.value]).toEqual([7]);
    expect(output.bytes).toBe(8);
    expect(budget.usedBytes).toBe(8);
    scope.dispose();
    expect(budget.usedBytes).toBe(0);
  });
});

it("falls back from geometric growth to exact capacity while charging copy overlap", async () => {
  const budget = new MemoryBudget(9);
  const scope = new MemoryScope(budget);
  const output = await readOwnedStream(chunks([1, 2, 3, 4], [5]), scope, { maxBytes: 16 });
  expect([...output.value]).toEqual([1, 2, 3, 4, 5]);
  expect(output.bytes).toBe(5);
  expect(budget.peakBytes).toBe(9);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});

it("shrinks transferred preflight capacity to a learned decoded length", async () => {
  const budget = new MemoryBudget(8);
  const scope = new MemoryScope(budget);
  const initialReservation = budget.reserve(8, "response preflight");
  try {
    const output = await readOwnedStream(chunks([1], [2, 3]), scope, {
      maxBytes: 8,
      knownLength: 3,
      initialReservation,
    });
    expect([...output.value]).toEqual([1, 2, 3]);
    expect(output.bytes).toBe(3);
    expect(budget.usedBytes).toBe(3);
    expect(budget.peakBytes).toBe(8);
  } finally {
    initialReservation.release();
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it("retains a trimmed view independently across jobs without losing its full backing charge", async () => {
  const pool = new MemoryBudget(16);
  const source = new MemoryScope(pool.child(8));
  const target = pool.child(8);
  const output = await readOwnedStream(chunks([9, 8]), source, { maxBytes: 8, knownLength: 8 });
  const retained = output.retain();
  const transferred = output.retainIn(target);
  source.dispose();
  try {
    expect([...retained.value]).toEqual([9, 8]);
    expect([...transferred.value]).toEqual([9, 8]);
    expect(transferred.bytes).toBe(8);
    expect(pool.usedBytes).toBe(8);
    expect(target.usedBytes).toBe(8);
    retained.release();
    expect(pool.usedBytes).toBe(8);
  } finally {
    retained.release();
    transferred.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("cancels a pending read and releases ownership (cancel rejects=%s)", async (rejectCancel) => {
  let waiting!: () => void;
  const pending = new Promise<void>((resolve) => {
    waiting = resolve;
  });
  let cancelled = 0;
  const stream = new ReadableStream<Uint8Array>(
    {
      start(controller) {
        controller.enqueue(Uint8Array.of(1, 2, 3, 4));
      },
      pull() {
        waiting();
      },
      cancel() {
        cancelled++;
        if (rejectCancel) throw Error("cancel failed");
      },
    },
    { highWaterMark: 0 },
  );
  const budget = new MemoryBudget(16);
  const scope = new MemoryScope(budget);
  const read = readOwnedStream(stream, scope, { maxBytes: 16 });
  const rejection = expect(read).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  await pending;
  expect(budget.usedBytes).toBe(4);
  scope.dispose();
  await rejection;
  expect(cancelled).toBe(1);
  expect(stream.locked).toBe(false);
  expect(budget.usedBytes).toBe(0);
});

it("preserves a source failure after partial delivery and releases the reader lock", async () => {
  const failure = new Error("remote body interrupted");
  const stream = new ReadableStream<Uint8Array>(
    {
      start(controller) {
        controller.enqueue(Uint8Array.of(1, 2));
      },
      pull(controller) {
        controller.error(failure);
      },
    },
    { highWaterMark: 0 },
  );
  const scope = new MemoryScope(new MemoryBudget(8));
  await expect(readOwnedStream(stream, scope, { maxBytes: 8 })).rejects.toBe(failure);
  expect(scope.budget.peakBytes).toBe(2);
  expect(scope.budget.usedBytes).toBe(0);
  expect(stream.locked).toBe(false);
  scope.dispose();
});

it("keeps the decoded-size error when source cancellation also fails", async () => {
  let cancelled = 0;
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(Uint8Array.of(1, 2, 3));
      controller.enqueue(Uint8Array.of(4, 5));
    },
    cancel() {
      cancelled++;
      throw Error("cancel failed");
    },
  });
  const scope = new MemoryScope(new MemoryBudget(16));
  await expect(
    readOwnedStream(stream, scope, { maxBytes: 4, metric: "decoded response" }),
  ).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
    details: { limit: 4, actual: 5, metric: "decoded response" },
  });
  expect(cancelled).toBe(1);
  expect(stream.locked).toBe(false);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it.each([null, "empty"])("accepts %s bodies under a zero-byte budget", async (kind) => {
  const scope = new MemoryScope(new MemoryBudget(0));
  const output = await readOwnedStream(kind === null ? null : chunks(), scope, { maxBytes: 0 });
  expect(output.value.byteLength).toBe(0);
  expect(output.bytes).toBe(0);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it.each([
  { maxBytes: -1, code: "LAKEQL_VALIDATION_ERROR" },
  { maxBytes: 2, knownLength: 3, code: "LAKEQL_BUDGET_EXCEEDED" },
])("preserves preflight $code when cancelling the source rejects", async (options) => {
  const scope = new MemoryScope(new MemoryBudget(8));
  const stream = new ReadableStream<Uint8Array>({
    cancel() {
      throw Error("cancel failed");
    },
  });
  await expect(readOwnedStream(stream, scope, options)).rejects.toMatchObject({
    code: options.code,
  });
  expect(scope.budget.peakBytes).toBe(0);
  expect(stream.locked).toBe(false);
  scope.dispose();
});
