import { expect, it } from "vitest";
import { iteratorReadableStream } from "./iterator-stream.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it("releases a suspended producer's allocation on encoding failure", async () => {
  const pool = new MemoryBudget(32);
  let released = 0;
  async function* rows() {
    const scope = new MemoryScope(pool);
    try {
      scope.allocate(32, "held input", () => new Uint8Array(32));
      yield { value: 1 };
    } finally {
      scope.dispose();
      released++;
    }
  }
  const iterator = rows();
  const failure = new Error("encoding failed");
  const stream = iteratorReadableStream<Uint8Array>(iterator, {
    async pull() {
      await iterator.next();
      expect(pool.usedBytes).toBe(32);
      throw failure;
    },
  });
  await expect(stream.getReader().read()).rejects.toBe(failure);
  expect(pool.usedBytes).toBe(0);
  expect(released).toBe(1);
});

it("keeps the encoding error when cleanup also rejects", async () => {
  const failure = new Error("encoding");
  let returns = 0;
  const iterator: AsyncIterator<unknown> = {
    async next() {
      return { done: false, value: 1 };
    },
    async return() {
      returns++;
      throw new Error("cleanup");
    },
  };
  const stream = iteratorReadableStream(iterator, {
    pull() {
      throw failure;
    },
  });
  await expect(stream.getReader().read()).rejects.toBe(failure);
  expect(returns).toBe(1);
});

it("closes once on cancellation and propagates cleanup failures", async () => {
  const failure = new Error("cleanup");
  let returns = 0;
  const iterator: AsyncIterator<unknown> = {
    async next() {
      return { done: true, value: undefined };
    },
    async return() {
      returns++;
      throw failure;
    },
  };
  const stream = iteratorReadableStream(iterator, {});
  await expect(stream.cancel()).rejects.toBe(failure);
  expect(returns).toBe(1);
});

it("shares cleanup when cancellation races with an in-flight pull", async () => {
  let unblock = () => {};
  let signalStarted = () => {};
  const pending = new Promise<void>((resolve) => {
    unblock = resolve;
  });
  const started = new Promise<void>((resolve) => {
    signalStarted = resolve;
  });
  const pool = new MemoryBudget(16);
  let releases = 0;
  async function* producer() {
    const scope = new MemoryScope(pool);
    try {
      scope.allocate(16, "in-flight row", () => new Uint8Array(16));
      signalStarted();
      await pending;
      yield 1;
    } finally {
      scope.dispose();
      releases++;
    }
  }
  const iterator = producer();
  const stream = iteratorReadableStream<number>(iterator, {
    async pull(controller) {
      const next = await iterator.next();
      if (next.done) controller.close();
      else controller.enqueue(next.value);
    },
  });
  await started;
  expect(pool.usedBytes).toBe(16);
  const cancellation = stream.cancel();
  unblock();
  await cancellation;
  expect(pool.usedBytes).toBe(0);
  expect(releases).toBe(1);
});
