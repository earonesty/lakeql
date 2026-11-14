import { expect, it } from "vitest";
import { checkedAsyncIterable } from "./checked-async-iterable.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it.each([
  "empty",
  "value",
] as const)("checks cancellation after delayed %s and closes the source", async (mode) => {
  const controller = new AbortController();
  let finish!: () => void;
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  let closed = false,
    discarded = 0;
  async function* source() {
    try {
      await pending;
      if (mode === "value") yield 7;
    } finally {
      closed = true;
    }
  }
  const iterator = checkedAsyncIterable(source(), controller.signal, () => {
    discarded++;
  })[Symbol.asyncIterator]();
  const next = iterator.next();
  const checked = expect(next).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  controller.abort();
  expect(closed).toBe(false);
  finish();
  await checked;
  expect(closed).toBe(true);
  expect(discarded).toBe(mode === "value" ? 1 : 0);
});

it("closes on consumer early return without discarding a handed-off value", async () => {
  let closed = false,
    discarded = 0;
  async function* source() {
    try {
      yield 1;
      yield 2;
    } finally {
      closed = true;
    }
  }
  for await (const value of checkedAsyncIterable(source(), undefined, () => {
    discarded++;
  })) {
    expect(value).toBe(1);
    break;
  }
  expect(closed).toBe(true);
  expect(discarded).toBe(0);
});

it("does not request another input when cancelled while the consumer holds a value", async () => {
  const controller = new AbortController();
  let advances = 0,
    closed = false;
  async function* source() {
    try {
      advances++;
      yield 1;
      advances++;
      yield 2;
    } finally {
      closed = true;
    }
  }
  const iterator = checkedAsyncIterable(source(), controller.signal)[Symbol.asyncIterator]();
  expect(await iterator.next()).toEqual({ value: 1, done: false });
  controller.abort();
  await expect(iterator.next()).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(advances).toBe(1);
  expect(closed).toBe(true);
});

it("releases an owned value allocated by a non-cooperative source after cancellation", async () => {
  const pool = new MemoryBudget(1024);
  const controller = new AbortController();
  let finish!: () => void;
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  async function* source() {
    const scope = new MemoryScope(pool);
    try {
      await pending;
      yield scope.allocate(128, "late scan allocation", () => new Uint8Array(128)).retain();
    } finally {
      scope.dispose();
    }
  }
  const iterator = checkedAsyncIterable(source(), controller.signal, (owner) => owner.release())[
    Symbol.asyncIterator
  ]();
  const next = iterator.next();
  const checked = expect(next).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  controller.abort();
  finish();
  await checked;
  expect(pool.peakBytes).toBe(128);
  expect(pool.usedBytes).toBe(0);
});

it("returns the original iterable when no cancellation signal is configured", () => {
  async function* source() {
    yield 1;
  }
  const values = source();
  expect(checkedAsyncIterable(values)).toBe(values);
});
