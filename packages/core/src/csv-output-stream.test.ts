import { expect, it } from "vitest";
import { csvOutputStream } from "./csv-output-stream.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it("infers columns, preserves quoting, and releases output on completion", async () => {
  const pool = new MemoryBudget(65536);
  async function* rows() {
    yield { a: '雪,"', b: 1 };
    yield { a: "", b: null };
  }
  expect(await new Response(csvOutputStream(rows(), pool, {})).text()).toBe('a,b\n"雪,""",1\n,\n');
  expect(pool.usedBytes).toBe(0);
});
it.each([
  true,
  false,
])("emits only the requested header for empty input (header=%s)", async (header) => {
  const pool = new MemoryBudget(65536);
  async function* rows() {}
  expect(
    await new Response(csvOutputStream(rows(), pool, { columns: ["a", "b"], header })).text(),
  ).toBe(header ? "a,b\n" : "");
  expect(pool.usedBytes).toBe(0);
});
it("releases the inferred directory, queued output and suspended input on cancellation", async () => {
  const pool = new MemoryBudget(65536);
  let closed = 0;
  async function* rows() {
    const scope = new MemoryScope(pool);
    try {
      scope.allocate(32, "source", () => new Uint8Array(32));
      yield { a: "value" };
    } finally {
      scope.dispose();
      closed++;
    }
  }
  const stream = csvOutputStream(rows(), pool, {});
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(pool.usedBytes).toBeGreaterThan(32);
  await stream.cancel();
  expect(closed).toBe(1);
  expect(pool.usedBytes).toBe(0);
});
it("closes the producer after cell admission fails", async () => {
  const pool = new MemoryBudget(256);
  let closed = 0;
  async function* rows() {
    const scope = new MemoryScope(pool);
    try {
      scope.allocate(32, "source", () => new Uint8Array(32));
      yield { a: "value" };
    } finally {
      scope.dispose();
      closed++;
    }
  }
  await expect(new Response(csvOutputStream(rows(), pool, {})).text()).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  expect(closed).toBe(1);
  expect(pool.usedBytes).toBe(0);
});
it("cancels a pending read without preparing its eventual row", async () => {
  const pool = new MemoryBudget(256);
  let unblock = () => {},
    signal = () => {};
  const pending = new Promise<void>((resolve) => {
    unblock = resolve;
  });
  const started = new Promise<void>((resolve) => {
    signal = resolve;
  });
  let accessed = 0,
    closed = 0;
  async function* rows() {
    try {
      signal();
      await pending;
      yield {
        get a() {
          accessed++;
          return "value";
        },
      };
    } finally {
      closed++;
    }
  }
  const stream = csvOutputStream(rows(), pool, {});
  await started;
  const cancelled = stream.cancel();
  await Promise.resolve();
  unblock();
  await cancelled;
  expect(accessed).toBe(0);
  expect(closed).toBe(1);
  expect(pool.usedBytes).toBe(0);
});
