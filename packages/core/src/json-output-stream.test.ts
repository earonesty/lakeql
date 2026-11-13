import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { jsonOutputStream } from "./json-output-stream.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it.each([
  "json",
  "ndjson",
] as const)("completes empty and populated %s streams without retained charges", async (format) => {
  for (const values of [[], [{ value: "雪😀" }, { value: null }]]) {
    const pool = new MemoryBudget(1024);
    async function* rows() {
      yield* values;
    }
    const result = await new Response(jsonOutputStream(rows(), pool, format)).text();
    expect(result).toBe(
      format === "json"
        ? JSON.stringify(values)
        : values.map((value) => `${JSON.stringify(value)}\n`).join(""),
    );
    expect(pool.usedBytes).toBe(0);
  }
});

it("charges a queued row until cancellation releases both output and suspended input", async () => {
  const pool = new MemoryBudget(1024);
  let released = 0;
  async function* rows() {
    const scope = new MemoryScope(pool);
    try {
      scope.allocate(32, "source row", () => new Uint8Array(32));
      yield { value: 1 };
    } finally {
      scope.dispose();
      released++;
    }
  }
  const stream = jsonOutputStream(rows(), pool, "ndjson");
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(pool.usedBytes).toBe(32 + c.view + new TextEncoder().encode('{"value":1}\n').length);
  await stream.cancel();
  expect(pool.usedBytes).toBe(0);
  expect(released).toBe(1);
});

it.each([
  "json",
  "ndjson",
] as const)("releases source memory when %s output exceeds the shared budget", async (format) => {
  const pool = new MemoryBudget(c.view + 33);
  let released = 0;
  async function* rows() {
    const scope = new MemoryScope(pool);
    try {
      scope.allocate(32, "source row", () => new Uint8Array(32));
      yield { value: "large output" };
    } finally {
      scope.dispose();
      released++;
    }
  }
  await expect(new Response(jsonOutputStream(rows(), pool, format)).text()).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  expect(pool.usedBytes).toBe(0);
  expect(released).toBe(1);
});

it("does not encode a row arriving after cancellation", async () => {
  const pool = new MemoryBudget(32);
  let unblock = () => {};
  let signalStarted = () => {};
  const pending = new Promise<void>((resolve) => {
    unblock = resolve;
  });
  const started = new Promise<void>((resolve) => {
    signalStarted = resolve;
  });
  let released = 0;
  let accessed = 0;
  async function* rows() {
    const scope = new MemoryScope(pool);
    try {
      scope.allocate(32, "pending source", () => new Uint8Array(32));
      signalStarted();
      await pending;
      yield {
        get value() {
          accessed++;
          return 1;
        },
      };
    } finally {
      scope.dispose();
      released++;
    }
  }
  const stream = jsonOutputStream(rows(), pool, "ndjson");
  await started;
  const cancellation = stream.cancel();
  // The iterator cleanup starts on a microtask, before the pending read resolves.
  await Promise.resolve();
  unblock();
  await cancellation;
  expect(accessed).toBe(0);
  expect(released).toBe(1);
  expect(pool.usedBytes).toBe(0);
});

it("releases handed-off output while a consumer retains its bytes", async () => {
  const pool = new MemoryBudget(1024);
  let signalNext = () => {};
  let unblock = () => {};
  const started = new Promise<void>((resolve) => {
    signalNext = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    unblock = resolve;
  });
  async function* rows() {
    yield { value: 1 };
    signalNext();
    await pending;
  }
  const reader = jsonOutputStream(rows(), pool, "ndjson").getReader();
  const chunk = await reader.read();
  await started;
  expect(pool.usedBytes).toBe(0);
  expect(new TextDecoder().decode(chunk.value)).toBe('{"value":1}\n');
  unblock();
  await expect(reader.read()).resolves.toMatchObject({ done: true });
  expect(pool.usedBytes).toBe(0);
});

it("admits empty JSON framing at the exact cap and rejects one byte less", async () => {
  async function* rows() {}
  const pool = new MemoryBudget(c.view + 1);
  expect(await new Response(jsonOutputStream(rows(), pool, "json")).text()).toBe("[]");
  expect(pool.usedBytes).toBe(0);
  const tight = new MemoryBudget(c.view);
  await expect(new Response(jsonOutputStream(rows(), tight, "json")).text()).rejects.toMatchObject({
    code: "LAKEQL_BUDGET_EXCEEDED",
  });
  expect(tight.usedBytes).toBe(0);
});
