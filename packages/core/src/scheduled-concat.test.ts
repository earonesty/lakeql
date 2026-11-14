import { expect, it } from "vitest";
import { type Batch, materializeBatchRows } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { MemoryStageScheduler } from "./memory-stage-scheduler.js";
import { concatScheduledBatches } from "./vector-sort.js";

function inputs(root: MemoryBudget) {
  const source = new MemoryScope(root.child());
  const batches: Batch[] = [10, 20].map((value) => ({
    rowCount: 1,
    columns: {
      x: {
        type: "dict",
        indices: source.allocate(4, "indices", () => Uint32Array.of(0)).value,
        dictionary: {
          type: "i32",
          values: source.allocate(4, "values", () => Int32Array.of(value)).value,
        },
      },
    },
  }));
  const owners = batches.map((batch) => source.retainAllocations(batch));
  source.dispose();
  return owners;
}

it("schedules exact output without double admission and detaches producer inputs", async () => {
  const root = new MemoryBudget(24);
  const source = inputs(root);
  const pending = concatScheduledBatches(source, new MemoryStageScheduler(root), root.child());
  for (const owner of source) owner.release();
  const result = await pending;
  expect(materializeBatchRows(result.value)).toEqual([{ x: 10 }, { x: 20 }]);
  expect(root.peakBytes).toBe(24);
  expect(root.usedBytes).toBe(8);
  result.release();
  expect(root.usedBytes).toBe(0);
});

it("cancels a queued concat while retaining inputs until cancellation cleanup", async () => {
  const root = new MemoryBudget(24);
  const source = inputs(root);
  const scheduler = new MemoryStageScheduler(root);
  let finish!: () => void;
  const waiting = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const blocker = scheduler.run({
    budget: root.child(),
    bytes: 8,
    purpose: "active job",
    async execute(scope) {
      const owner = scope.allocate(8, "scratch", () => new Uint8Array(8));
      await waiting;
      owner.release();
    },
  });
  const controller = new AbortController();
  const pending = concatScheduledBatches(source, scheduler, root.child(), controller.signal);
  for (const owner of source) owner.release();
  expect(scheduler.queuedStages).toBe(1);
  expect(root.usedBytes).toBe(24);
  controller.abort();
  await expect(pending).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(root.usedBytes).toBe(8);
  finish();
  (await blocker).release();
  expect(root.usedBytes).toBe(0);
});

it("rejects unsupported physical shapes before scheduler admission", async () => {
  const root = new MemoryBudget(100);
  const scope = new MemoryScope(root);
  const batch: Batch = { rowCount: 1, columns: { x: { type: "utf8", values: ["\ud800"] } } };
  const owner = scope.retainAllocations(batch);
  const scheduler = new MemoryStageScheduler(root);
  await expect(concatScheduledBatches([owner], scheduler, root.child())).rejects.toThrow(
    /complete allocation plan/,
  );
  expect(scheduler.activeStages).toBe(0);
  expect(root.peakBytes).toBe(0);
  owner.release();
  scope.dispose();
});

it("runs fitting independent jobs under one shared pool", async () => {
  const root = new MemoryBudget(48);
  const first = inputs(root);
  const second = inputs(root);
  const scheduler = new MemoryStageScheduler(root);
  const a = concatScheduledBatches(first, scheduler, root.child());
  const b = concatScheduledBatches(second, scheduler, root.child());
  expect(scheduler.activeStages).toBe(2);
  for (const owner of [...first, ...second]) owner.release();
  const results = await Promise.all([a, b]);
  expect(root.peakBytes).toBe(48);
  expect(root.usedBytes).toBe(16);
  for (const result of results) result.release();
  expect(root.usedBytes).toBe(0);
});

it("preserves shared dictionary ownership after scheduled output handoff", async () => {
  const root = new MemoryBudget(16);
  const source = new MemoryScope(root.child());
  const batch: Batch = {
    rowCount: 1,
    columns: {
      x: {
        type: "dict",
        indices: source.allocate(4, "indices", () => Uint32Array.of(0)).value,
        dictionary: {
          type: "i32",
          values: source.allocate(4, "values", () => Int32Array.of(10)).value,
        },
      },
    },
  };
  const owner = source.retainAllocations(batch);
  source.dispose();
  const pending = concatScheduledBatches(
    [owner, owner],
    new MemoryStageScheduler(root),
    root.child(),
  );
  owner.release();
  const output = await pending;
  expect(materializeBatchRows(output.value)).toEqual([{ x: 10 }, { x: 10 }]);
  expect(root.usedBytes).toBe(16);
  output.release();
  expect(root.usedBytes).toBe(0);
});
