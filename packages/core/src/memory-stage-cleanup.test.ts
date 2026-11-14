import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryStageScheduler } from "./memory-stage-scheduler.js";

it("rejects failed cleanup and advances queued work without retaining the job slot", async () => {
  const pool = new MemoryBudget(8);
  const scheduler = new MemoryStageScheduler(pool);
  const job = pool.child(8);
  let finish!: () => void;
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const first = scheduler.run({
    budget: job,
    bytes: 8,
    purpose: "first",
    async execute(scope) {
      const original = scope.allocate(8, "payload", () => ({})).retain();
      scope.adopt({
        value: original.value,
        bytes: original.bytes,
        budget: original.budget,
        retain: original.retain,
        retainIn: original.retainIn,
        release() {
          original.release();
          throw new Error("cleanup failure");
        },
      });
      await gate;
      return "first output";
    },
  });
  const next = scheduler.run({
    budget: pool.child(8),
    bytes: 8,
    purpose: "next",
    execute() {
      return "next output";
    },
  });
  expect(scheduler.queuedStages).toBe(1);
  finish();
  await expect(first).rejects.toThrow("cleanup failure");
  const output = await next;
  expect(output.value).toBe("next output");
  output.release();
  const retry = await scheduler.run({
    budget: job,
    bytes: 8,
    purpose: "reuse job",
    execute() {
      return "retry";
    },
  });
  retry.release();
  expect(scheduler.activeStages).toBe(0);
  expect(scheduler.queuedStages).toBe(0);
  expect(pool.usedBytes).toBe(0);
});

it("defers reentrant scheduling until a failed output has released its backing", async () => {
  const { MemoryScope } = await import("./memory-scope.js");
  const pool = new MemoryBudget(16);
  const source = new MemoryScope(pool);
  const original = source.allocate(8, "input", () => ({}));
  const scheduler = new MemoryStageScheduler(pool);
  let reentrant: ReturnType<typeof scheduler.run<string>> | undefined;
  const input = {
    value: original.value,
    bytes: original.bytes,
    budget: original.budget,
    retain: original.retain,
    release: original.release,
    retainIn(budget: MemoryBudget) {
      const pin = original.retainIn(budget);
      return {
        ...pin,
        release() {
          pin.release();
          reentrant = scheduler.run({
            budget: pool.child(8),
            bytes: 8,
            purpose: "reentrant",
            execute() {
              return "progress";
            },
          });
          void reentrant.catch(() => undefined);
          throw new Error("input cleanup failure");
        },
      };
    },
  };
  await expect(
    scheduler.run({
      budget: pool.child(16),
      bytes: 8,
      purpose: "failed publication",
      inputs: [input],
      execute(scope) {
        return scope.allocate(8, "output", () => new Uint8Array(8)).value;
      },
    }),
  ).rejects.toThrow("input cleanup failure");
  if (reentrant === undefined) throw new Error("Missing queued stage");
  const result = await reentrant;
  expect(result.value).toBe("progress");
  result.release();
  source.dispose();
  expect(pool.usedBytes).toBe(0);
  expect(scheduler.activeStages).toBe(0);
});

it.each([
  false,
  true,
])("keeps cancellation ownership through settlement, rejected pin=%s", async (rejectPin) => {
  const pool = new MemoryBudget(8);
  const scheduler = new MemoryStageScheduler(pool);
  const controller = new AbortController();
  let finish!: () => void;
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const first = scheduler.run({
    budget: pool.child(8),
    bytes: 8,
    purpose: "cancelled",
    signal: controller.signal,
    async execute(scope) {
      const original = scope.allocate(8, "pending workspace", () => new Uint8Array(8)).retain();
      scope.adopt({
        value: original.value,
        bytes: original.bytes,
        budget: original.budget,
        retain() {
          if (rejectPin) throw new Error("guard pin failure");
          return original.retain();
        },
        retainIn: original.retainIn,
        release() {
          original.release();
          throw new Error("cancel cleanup failure");
        },
      });
      await gate;
      return "cancelled output";
    },
  });
  const rejected = expect(first).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  controller.abort();
  expect(pool.usedBytes).toBe(8);
  expect(scheduler.activeStages).toBe(1);
  const next = scheduler.run({
    budget: pool.child(8),
    bytes: 8,
    purpose: "after cancellation",
    execute() {
      return "next";
    },
  });
  expect(scheduler.queuedStages).toBe(1);
  finish();
  await rejected;
  const result = await next;
  result.release();
  expect(scheduler.activeStages).toBe(0);
  expect(scheduler.queuedStages).toBe(0);
  expect(pool.usedBytes).toBe(0);
});

it("releases every transferred input after acquisition fails and reuses the job", async () => {
  const { MemoryScope } = await import("./memory-scope.js");
  const pool = new MemoryBudget(24);
  const source = new MemoryScope(pool);
  const original = source.allocate(8, "input", () => ({}));
  const job = pool.child(24);
  const scheduler = new MemoryStageScheduler(pool);
  const first = {
    ...original,
    retainIn(budget: MemoryBudget) {
      const held = original.retainIn(budget);
      return {
        ...held,
        release() {
          held.release();
          throw new Error("release failure");
        },
      };
    },
  };
  const failure = new Error("input acquisition failure");
  const second = {
    ...original,
    retainIn() {
      throw failure;
    },
  };
  await expect(
    scheduler.run({
      budget: job,
      bytes: 0,
      purpose: "invalid inputs",
      inputs: [first, second],
      execute() {
        return 1;
      },
    }),
  ).rejects.toBe(failure);
  expect(job.usedBytes).toBe(0);
  const retry = await scheduler.run({
    budget: job,
    bytes: 0,
    purpose: "retry",
    execute() {
      return 2;
    },
  });
  retry.release();
  source.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("cancels queued work despite multiple throwing input releases and removes its job slot", async () => {
  const { MemoryScope } = await import("./memory-scope.js");
  const pool = new MemoryBudget(24);
  const source = new MemoryScope(pool);
  const scheduler = new MemoryStageScheduler(pool);
  const releases: number[] = [];
  const inputs = [0, 1].map((id) => {
    const original = source.allocate(8, "input", () => ({ id }));
    return {
      ...original,
      retainIn(budget: MemoryBudget) {
        const held = original.retainIn(budget);
        return {
          ...held,
          release() {
            held.release();
            releases.push(id);
            throw new Error("release failure");
          },
        };
      },
    };
  });
  let finish!: () => void;
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const active = scheduler.run({
    budget: pool.child(8),
    bytes: 8,
    purpose: "active",
    async execute() {
      await gate;
      return 1;
    },
  });
  const job = pool.child(24);
  const controller = new AbortController();
  const queued = scheduler.run({
    budget: job,
    bytes: 8,
    purpose: "queued",
    inputs,
    signal: controller.signal,
    execute() {
      throw new Error("Must not execute");
    },
  });
  const rejected = expect(queued).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(scheduler.queuedStages).toBe(1);
  controller.abort();
  await rejected;
  expect(releases).toEqual([0, 1]);
  expect(job.usedBytes).toBe(0);
  const retry = await scheduler.run({
    budget: job,
    bytes: 0,
    purpose: "reuse cancelled job",
    execute() {
      return 2;
    },
  });
  retry.release();
  finish();
  (await active).release();
  source.dispose();
  expect(scheduler.activeStages).toBe(0);
  expect(scheduler.queuedStages).toBe(0);
  expect(pool.usedBytes).toBe(0);
});

it("preserves an undefined execution error while returning all scheduler capacity", async () => {
  const pool = new MemoryBudget(8);
  const scheduler = new MemoryStageScheduler(pool);
  const job = pool.child(8);
  await expect(
    scheduler.run({
      budget: job,
      bytes: 8,
      purpose: "undefined error",
      execute() {
        throw undefined;
      },
    }),
  ).rejects.toBeUndefined();
  const result = await scheduler.run({
    budget: job,
    bytes: 8,
    purpose: "retry",
    execute() {
      return 1;
    },
  });
  result.release();
  expect(scheduler.activeStages).toBe(0);
  expect(pool.usedBytes).toBe(0);
});
