import { expect, it } from "vitest";
import { type Batch, materializeBatchRows } from "./batch.js";
import { concatAllocationBytes } from "./concat-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { MemoryStageScheduler } from "./memory-stage-scheduler.js";
import { concatBatches } from "./vector-sort.js";

function gate() {
  let open!: () => void;
  const promise = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { promise, open };
}

it("runs a planned typed concat without double admission and releases detached inputs", async () => {
  const root = new MemoryBudget(24);
  const source = new MemoryScope(root.child());
  const batches: Batch[] = [10, 20].map((value) => ({
    rowCount: 1,
    columns: {
      x: {
        type: "dict",
        indices: source.allocate(4, "input indices", () => Uint32Array.of(0)).value,
        dictionary: {
          type: "i32",
          values: source.allocate(4, "input values", () => Int32Array.of(value)).value,
        },
      },
    },
  }));
  const inputs = batches.map((batch) => source.retainAllocations(batch));
  source.dispose();
  const bytes = concatAllocationBytes(batches);
  if (bytes === undefined) throw new Error("Missing concat plan");
  expect(bytes).toBe(8);
  const scheduler = new MemoryStageScheduler(root);
  const running = scheduler.run({
    budget: root.child(24),
    bytes,
    purpose: "typed concat",
    inputs,
    execute(scope, _signal, pins) {
      const result = concatBatches(batches, scope);
      expect(result.columns.x?.type).toBe("i32");
      for (const pin of pins) pin.release();
      return result;
    },
  });
  for (const input of inputs) input.release();
  const output = await running;
  expect(materializeBatchRows(output.value)).toEqual([{ x: 10 }, { x: 20 }]);
  expect(root.peakBytes).toBe(24);
  expect(root.usedBytes).toBe(8);
  output.release();
  expect(root.usedBytes).toBe(0);
});

it("queues whole stages before starting work and admits them after transient release", async () => {
  const root = new MemoryBudget(12);
  const scheduler = new MemoryStageScheduler(root);
  const transport = gate();
  const events: string[] = [];
  const first = scheduler.run({
    budget: root.child(12),
    bytes: 12,
    purpose: "first",
    async execute(scope) {
      events.push("first");
      const temporary = scope.allocate(12, "transport", () => new Uint8Array(12));
      await transport.promise;
      temporary.release();
      return "done";
    },
  });
  const second = scheduler.run({
    budget: root.child(12),
    bytes: 12,
    purpose: "second",
    execute(scope) {
      events.push("second");
      return scope.allocate(12, "output", () => new Uint8Array(12)).value;
    },
  });
  expect(events).toEqual(["first"]);
  expect(scheduler.queuedStages).toBe(1);
  expect(root.usedBytes).toBe(12);
  transport.open();
  const outputs = await Promise.all([first, second]);
  expect(events).toEqual(["first", "second"]);
  expect(root.peakBytes).toBe(12);
  expect(root.usedBytes).toBe(12);
  for (const output of outputs) output.release();
  expect(root.usedBytes).toBe(0);
});

it("rejects queued work when a completed stage hands its memory to a consumer", async () => {
  const root = new MemoryBudget(8);
  const scheduler = new MemoryStageScheduler(root);
  const transport = gate();
  let invoked = false;
  const first = scheduler.run({
    budget: root.child(),
    bytes: 8,
    purpose: "retained output",
    async execute(scope) {
      const value = scope.allocate(8, "output", () => new Uint8Array(8)).value;
      await transport.promise;
      return value;
    },
  });
  const second = scheduler.run({
    budget: root.child(),
    bytes: 1,
    purpose: "blocked by output",
    execute() {
      invoked = true;
    },
  });
  const rejected = expect(second).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  transport.open();
  const output = await first;
  await rejected;
  expect(invoked).toBe(false);
  expect(scheduler.queuedStages).toBe(0);
  expect(root.usedBytes).toBe(8);
  output.release();
  expect(root.usedBytes).toBe(0);
});

it("runs independently fitting stages concurrently within the shared parent", async () => {
  const root = new MemoryBudget(16);
  const scheduler = new MemoryStageScheduler(root);
  const transport = gate();
  const runs = [0, 1].map(() =>
    scheduler.run({
      budget: root.child(8),
      bytes: 8,
      purpose: "parallel stage",
      async execute(scope) {
        const value = scope.allocate(8, "output", () => new Uint8Array(8)).value;
        await transport.promise;
        return value;
      },
    }),
  );
  expect(scheduler.activeStages).toBe(2);
  expect(root.usedBytes).toBe(16);
  transport.open();
  for (const output of await Promise.all(runs)) output.release();
  expect(root.usedBytes).toBe(0);
});

it("removes canceled queued work and preserves in-flight charges until settlement", async () => {
  const root = new MemoryBudget(8);
  const scheduler = new MemoryStageScheduler(root);
  const transport = gate();
  const active = new AbortController();
  const waiting = new AbortController();
  let transportSignal: AbortSignal | undefined;
  const first = scheduler.run({
    budget: root.child(),
    bytes: 8,
    purpose: "transport",
    signal: active.signal,
    async execute(scope, signal) {
      transportSignal = signal;
      return (
        await scope.allocateAsync(8, "transport", async () => {
          await transport.promise;
          return new Uint8Array(8);
        })
      ).value;
    },
  });
  const firstRejected = expect(first).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  const second = scheduler.run({
    budget: root.child(),
    bytes: 8,
    purpose: "queued",
    signal: waiting.signal,
    execute() {
      throw new Error("Canceled stage started");
    },
  });
  const secondRejected = expect(second).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  waiting.abort();
  await secondRejected;
  expect(scheduler.queuedStages).toBe(0);
  active.abort();
  expect(transportSignal?.aborted).toBe(true);
  expect(root.usedBytes).toBe(8);
  expect(scheduler.activeStages).toBe(1);
  transport.open();
  await firstRejected;
  expect(root.usedBytes).toBe(0);
  expect(scheduler.activeStages).toBe(0);
});

it("pins retained inputs through transport and preserves aliases at output handoff", async () => {
  const root = new MemoryBudget(12);
  const source = new MemoryScope(root.child());
  const value = source.allocate(8, "input", () => Uint8Array.of(1, 2, 3, 4, 5, 6, 7, 8)).value;
  const input = source.retainAllocations(value);
  source.dispose();
  const scheduler = new MemoryStageScheduler(root);
  const transport = gate();
  const running = scheduler.run({
    budget: root.child(12),
    bytes: 4,
    purpose: "input and scratch",
    inputs: [input],
    async execute(scope) {
      const scratch = scope.allocate(4, "scratch", () => new Uint8Array(4));
      await transport.promise;
      scratch.release();
      return value.subarray(1, 3);
    },
  });
  input.release();
  expect(root.usedBytes).toBe(12);
  transport.open();
  const output = await running;
  expect([...output.value]).toEqual([2, 3]);
  expect(root.usedBytes).toBe(8);
  output.release();
  expect(root.usedBytes).toBe(0);
});

it("rejects impossible stages immediately even while other stages are running", async () => {
  const root = new MemoryBudget(16);
  const scheduler = new MemoryStageScheduler(root);
  const transport = gate();
  const running = scheduler.run({
    budget: root.child(),
    bytes: 1,
    purpose: "active",
    async execute() {
      await transport.promise;
    },
  });
  await expect(
    scheduler.run({
      budget: root.child(4),
      bytes: 5,
      purpose: "impossible child",
      execute() {
        throw new Error("Impossible stage started");
      },
    }),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(scheduler.queuedStages).toBe(0);
  transport.open();
  (await running).release();
  expect(root.usedBytes).toBe(0);
});

it("bounds admitted plus queued stage bookkeeping and rejects same-job recursive admission", async () => {
  const root = new MemoryBudget(8);
  const scheduler = new MemoryStageScheduler(root, 2);
  const transport = gate();
  const job = root.child();
  const running = scheduler.run({
    budget: job,
    bytes: 8,
    purpose: "first",
    async execute() {
      await expect(
        scheduler.run({ budget: job, bytes: 0, purpose: "nested", execute() {} }),
      ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
      await transport.promise;
    },
  });
  const queued = scheduler.run({ budget: root.child(), bytes: 1, purpose: "second", execute() {} });
  await expect(
    scheduler.run({ budget: root.child(), bytes: 0, purpose: "third", execute() {} }),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(scheduler.queuedStages).toBe(1);
  transport.open();
  for (const output of await Promise.all([running, queued])) output.release();
  expect(root.usedBytes).toBe(0);
});

it("drains after execution failure and rejects allocations beyond the admitted extent", async () => {
  const root = new MemoryBudget(8);
  const scheduler = new MemoryStageScheduler(root);
  const transport = gate();
  const first = scheduler.run({
    budget: root.child(),
    bytes: 8,
    purpose: "failing stage",
    async execute(scope) {
      scope.allocate(8, "output", () => new Uint8Array(8));
      await transport.promise;
      scope.allocate(1, "unplanned", () => new Uint8Array(1));
    },
  });
  const failed = expect(first).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  const second = scheduler.run({
    budget: root.child(),
    bytes: 8,
    purpose: "next",
    execute() {
      return 42;
    },
  });
  transport.open();
  await failed;
  const output = await second;
  expect(output.value).toBe(42);
  output.release();
  expect(root.usedBytes).toBe(0);
});

it("cleans up cancellation triggered synchronously by cache reclamation during admission", async () => {
  const root = new MemoryBudget(8);
  const cache = root.reserve(8, "cache");
  const scheduler = new MemoryStageScheduler(root);
  const controller = new AbortController();
  const job = root.child();
  const detach = root.registerReclaimer(() => {
    controller.abort();
    cache.release();
  });
  await expect(
    scheduler.run({
      budget: job,
      bytes: 8,
      purpose: "admission",
      signal: controller.signal,
      execute() {
        throw new Error("Canceled admission executed");
      },
    }),
  ).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(root.usedBytes).toBe(0);
  expect(scheduler.activeStages).toBe(0);
  expect(scheduler.queuedStages).toBe(0);
  detach();
  const output = await scheduler.run({
    budget: job,
    bytes: 8,
    purpose: "reuse job",
    execute() {
      return 1;
    },
  });
  output.release();
  expect(root.usedBytes).toBe(0);
});

it("keeps completed input and scratch buffers charged after abort until execution settles", async () => {
  const root = new MemoryBudget(12);
  const source = new MemoryScope(root.child());
  const value = source.allocate(8, "input", () => new Uint8Array(8)).value;
  const input = source.retainAllocations(value);
  source.dispose();
  const scheduler = new MemoryStageScheduler(root);
  const transport = gate();
  const controller = new AbortController();
  const running = scheduler.run({
    budget: root.child(),
    bytes: 4,
    purpose: "abort",
    inputs: [input],
    signal: controller.signal,
    async execute(scope) {
      scope.allocate(4, "live scratch", () => new Uint8Array(4));
      await transport.promise;
      return value;
    },
  });
  const rejected = expect(running).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  input.release();
  controller.abort();
  expect(root.usedBytes).toBe(12);
  transport.open();
  await rejected;
  expect(root.usedBytes).toBe(0);
});

it("rejects invalid extents, foreign pools and pre-aborted requests without executing", async () => {
  const root = new MemoryBudget(8);
  const scheduler = new MemoryStageScheduler(root);
  const execute = () => {
    throw new Error("Invalid request executed");
  };
  for (const bytes of [-1, 0.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    await expect(
      scheduler.run({ budget: root.child(), bytes, purpose: "invalid", execute }),
    ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
  }
  await expect(
    scheduler.run({ budget: new MemoryBudget(8), bytes: 1, purpose: "foreign", execute }),
  ).rejects.toMatchObject({ code: "LAKEQL_VALIDATION_ERROR" });
  await expect(
    scheduler.run({
      budget: root.child(),
      bytes: 1,
      purpose: "aborted",
      signal: AbortSignal.abort(),
      execute,
    }),
  ).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(() => new MemoryStageScheduler(root, 0)).toThrow(/capacity/);
  expect(() => new MemoryStageScheduler(root.child())).toThrow(/root/);
  expect(root.usedBytes).toBe(0);
});

it("propagates stage cancellation through child scope signals while guarding live scratch", async () => {
  const root = new MemoryBudget(8);
  const scheduler = new MemoryStageScheduler(root);
  const controller = new AbortController();
  const transport = gate();
  let nested: MemoryScope | undefined;
  const read = scheduler.run({
    budget: root.child(),
    bytes: 8,
    purpose: "nested transport cancellation",
    signal: controller.signal,
    async execute(scope) {
      nested = scope.childScope();
      const scratch = nested.allocate(8, "scratch", () => new Uint8Array(8).fill(42)).value;
      await transport.promise;
      expect([...scratch]).toEqual(Array(8).fill(42));
      expect(root.usedBytes).toBe(8);
      return scratch;
    },
  });
  const rejected = expect(read).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  controller.abort();
  expect(nested?.signal.aborted).toBe(true);
  expect(nested?.disposed).toBe(true);
  expect(root.usedBytes).toBe(8);
  expect(scheduler.activeStages).toBe(1);
  transport.open();
  await rejected;
  expect(root.usedBytes).toBe(0);
  expect(scheduler.activeStages).toBe(0);
});
