import { expect, it, vi } from "vitest";
import { batchFromColumns } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { CpuPhysicalBackend } from "./physical.js";
import * as aggregates from "./vector-aggregate.js";

it("continues across batches without implicit checkpoints and retains explicit output", async () => {
  const pool = new MemoryBudget(1_000_000);
  const parent = new MemoryScope(pool);
  const backend = new CpuPhysicalBackend();
  const continuation = backend.openReduction(
    {
      kind: "reduce",
      aggregates: { n: { op: "count" }, median: { op: "median", column: "value" } },
    },
    { allocationScope: parent },
  );
  const snapshot = vi.spyOn(aggregates, "snapshotVectorAggregateStates");
  try {
    for (let i = 0; i < 100; i++) {
      const metrics = await continuation.append(
        batchFromColumns({ value: [2 * i, 2 * i + 1] }),
        new Uint8Array([1, 0]),
      );
      expect(metrics).toMatchObject({ inputRows: 2, selectedRows: 1 });
    }
    expect(snapshot).not.toHaveBeenCalled();
    const checkpoint = await continuation.checkpoint();
    expect(checkpoint.value.kind).toBe("aggregate-snapshot");
    const calls = snapshot.mock.calls.length;
    await continuation.append(batchFromColumns({ value: [200] }));
    expect(snapshot.mock.calls.length).toBe(calls);
    const result = await continuation.finish();
    parent.dispose();
    expect(result.value).toEqual([{ n: 101, median: 100 }]);
    expect(checkpoint.value.kind === "aggregate-snapshot" && checkpoint.value.snapshot.n).toEqual({
      op: "count",
      count: 100,
    });
    checkpoint.release();
    expect(pool.usedBytes).toBe(result.bytes);
    result.release();
    expect(pool.usedBytes).toBe(0);
    await expect(continuation.append(batchFromColumns({ value: [1] }))).rejects.toThrow(/disposed/);
  } finally {
    snapshot.mockRestore();
    continuation.dispose();
    parent.dispose();
  }
});

it("restores an explicit checkpoint into a new independent continuation", async () => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  const backend = new CpuPhysicalBackend();
  const operator = { kind: "reduce" as const, aggregates: { n: { op: "count" as const } } };
  const first = backend.openReduction(operator, { allocationScope: parent });
  await first.append(batchFromColumns({ value: [1, 2] }));
  const checkpoint = await first.checkpoint();
  first.dispose();
  if (checkpoint.value.kind !== "aggregate-snapshot") throw Error("Missing checkpoint");
  const second = backend.openReduction(operator, {
    allocationScope: parent,
    priorOutput: checkpoint.value,
  });
  checkpoint.release();
  await second.append(batchFromColumns({ value: [3] }));
  const result = await second.finish();
  expect(result.value).toEqual([{ n: 3 }]);
  result.release();
  parent.dispose();
  expect(parent.budget.usedBytes).toBe(0);
});

it("closes failed appends and releases state on cancellation", async () => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  const backend = new CpuPhysicalBackend();
  const operator = {
    kind: "reduce" as const,
    aggregates: { value: { op: "median" as const, column: "value" } },
  };
  const failed = backend.openReduction(operator, {
    allocationScope: parent,
    budget: { maxBufferedRows: 1 },
  });
  await expect(failed.append(batchFromColumns({ value: [1, 2] }))).rejects.toThrow(/budget/);
  expect(parent.budget.usedBytes).toBe(0);
  await expect(failed.finish()).rejects.toThrow(/disposed/);
  const controller = new AbortController();
  const canceled = backend.openReduction(operator, {
    allocationScope: parent,
    budget: { signal: controller.signal },
  });
  await canceled.append(batchFromColumns({ value: [1] }));
  controller.abort();
  expect(parent.budget.usedBytes).toBe(0);
  await expect(canceled.checkpoint()).rejects.toThrow();
  parent.dispose();
});

it("enforces final row and elapsed budgets without leaking state", async () => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  const backend = new CpuPhysicalBackend();
  const operator = { kind: "reduce" as const, aggregates: { n: { op: "count" as const } } };
  const rows = backend.openReduction(operator, {
    allocationScope: parent,
    budget: { maxOutputRows: 0 },
  });
  await expect(rows.finish()).rejects.toThrow(/output rows/);
  expect(parent.budget.usedBytes).toBe(0);
  let clock = 0;
  const timed = backend.openReduction(operator, {
    allocationScope: parent,
    now: () => clock,
    budget: { maxElapsedMs: 10 },
  });
  clock = 11;
  await expect(timed.append(batchFromColumns({ n: [1] }))).rejects.toThrow(/elapsed/);
  expect(parent.budget.usedBytes).toBe(0);
  parent.dispose();
});

it("continues grouped reductions and restores explicit checkpoints without batch snapshots", async () => {
  const pool = new MemoryBudget(1024 * 1024);
  const parent = new MemoryScope(pool);
  const backend = new CpuPhysicalBackend();
  const operator = {
    kind: "grouped-reduce" as const,
    keys: ["key"],
    aggregates: {
      n: { op: "count" as const },
      median: { op: "median" as const, column: "value" },
    },
  };
  const first = backend.openReduction(operator, { allocationScope: parent });
  const snapshot = vi.spyOn(aggregates, "snapshotVectorAggregateStates");
  try {
    for (let i = 0; i < 100; i++)
      await first.append(batchFromColumns({ key: ["a", "b"], value: [i, i + 1] }));
    expect(snapshot).not.toHaveBeenCalled();
    const checkpoint = await first.checkpoint();
    expect(checkpoint.value.kind).toBe("grouped-aggregate-snapshot");
    first.dispose();
    if (checkpoint.value.kind !== "grouped-aggregate-snapshot")
      throw Error("Missing grouped checkpoint");
    const second = backend.openReduction(operator, {
      allocationScope: parent,
      priorOutput: checkpoint.value,
    });
    checkpoint.release();
    await second.append(batchFromColumns({ key: ["a", "c"], value: [100, 7] }));
    const result = await second.finish();
    parent.dispose();
    expect(result.value).toEqual([
      { key: "a", n: 101, median: 50 },
      { key: "b", n: 100, median: 50.5 },
      { key: "c", n: 1, median: 7 },
    ]);
    expect(pool.usedBytes).toBe(result.bytes);
    result.release();
    expect(pool.usedBytes).toBe(0);
  } finally {
    snapshot.mockRestore();
    first.dispose();
    parent.dispose();
  }
});

it("closes grouped failures and honors final output enforcement policy", async () => {
  const parent = new MemoryScope(new MemoryBudget(65536));
  const backend = new CpuPhysicalBackend();
  const operator = {
    kind: "grouped-reduce" as const,
    keys: ["key"],
    maxGroups: 1,
    aggregates: { n: { op: "count" as const } },
  };
  const failed = backend.openReduction(operator, { allocationScope: parent });
  await expect(failed.append(batchFromColumns({ key: ["a", "b"] }))).rejects.toThrow(
    /group budget/,
  );
  expect(parent.budget.usedBytes).toBe(0);
  const empty = backend.openReduction(operator, {
    allocationScope: parent,
    budget: { maxOutputRows: 0 },
  });
  const emptyRows = await empty.finish();
  expect(emptyRows.value).toEqual([]);
  emptyRows.release();
  for (const enforceOutputRows of [true, false]) {
    const continuation = backend.openReduction(operator, {
      allocationScope: parent,
      budget: { maxOutputRows: 0 },
      enforceOutputRows,
    });
    await continuation.append(batchFromColumns({ key: ["a"] }));
    if (enforceOutputRows) await expect(continuation.finish()).rejects.toThrow(/output rows/);
    else {
      const rows = await continuation.finish();
      expect(rows.value).toEqual([{ key: "a", n: 1 }]);
      rows.release();
    }
    expect(parent.budget.usedBytes).toBe(0);
  }
  const controller = new AbortController();
  const canceled = backend.openReduction(operator, {
    allocationScope: parent,
    budget: { signal: controller.signal },
  });
  await canceled.append(batchFromColumns({ key: ["a"] }));
  controller.abort();
  expect(parent.budget.usedBytes).toBe(0);
  await expect(canceled.checkpoint()).rejects.toThrow();
  parent.dispose();
});

it.each([
  false,
  true,
])("merges checkpoints into a live owned continuation (grouped=%s)", async (grouped) => {
  const pool = new MemoryBudget(1024 * 1024);
  const scope = new MemoryScope(pool);
  const backend = new CpuPhysicalBackend();
  const aggregates = {
    n: { op: "count" as const },
    total: { op: "sum" as const, column: "value" },
    middle: { op: "median" as const, column: "value" },
  };
  const operator = grouped
    ? { kind: "grouped-reduce" as const, keys: ["g"], aggregates }
    : { kind: "reduce" as const, aggregates };
  const source = backend.openReduction(operator, { allocationScope: scope });
  const target = backend.openReduction(operator, { allocationScope: scope });
  try {
    await source.append(batchFromColumns({ g: ["a", "a"], value: [1, 3] }));
    const checkpoint = await source.checkpoint();
    source.dispose();
    const before = JSON.stringify(checkpoint.value);
    try {
      await target.append(batchFromColumns({ g: ["a"], value: [5] }));
      expect(target.mergeCheckpoint).toBeTypeOf("function");
      await target.mergeCheckpoint?.(checkpoint.value);
      expect(JSON.stringify(checkpoint.value)).toBe(before);
    } finally {
      checkpoint.release();
    }
    await target.append(batchFromColumns({ g: ["a"], value: [7] }));
    const output = await target.finish();
    try {
      expect(output.value).toEqual([
        { ...(grouped ? { g: "a" } : {}), n: 4, total: 16, middle: 4 },
      ]);
    } finally {
      output.release();
    }
  } finally {
    source.dispose();
    target.dispose();
    scope.dispose();
  }
  expect(pool.usedBytes).toBe(0);
});

it("closes a continuation after checkpoint admission failure", async () => {
  const pool = new MemoryBudget(16384);
  const scope = new MemoryScope(pool);
  const target = new CpuPhysicalBackend().openReduction(
    { kind: "reduce", aggregates: { value: { op: "median", column: "value" } } },
    { allocationScope: scope },
  );
  await expect(
    target.mergeCheckpoint?.({
      kind: "aggregate-snapshot",
      snapshot: { value: { op: "median", values: Array(4096).fill(1) } },
    }),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(pool.usedBytes).toBe(0);
  await expect(target.append(batchFromColumns({ value: [1] }))).rejects.toBeDefined();
  scope.dispose();
});

it("rejects mismatched checkpoint kinds and closes the merge target", async () => {
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const target = new CpuPhysicalBackend().openReduction(
    { kind: "grouped-reduce", keys: ["g"], aggregates: { n: { op: "count" } } },
    { allocationScope: scope },
  );
  await expect(
    target.mergeCheckpoint?.({
      kind: "aggregate-snapshot",
      snapshot: { n: { op: "count", count: 1 } },
    }),
  ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
  expect(pool.usedBytes).toBe(0);
  scope.dispose();
});

it.each(["abort", "elapsed"] as const)("checks %s before merging a checkpoint", async (failure) => {
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const controller = new AbortController();
  let now = 0;
  const target = new CpuPhysicalBackend().openReduction(
    { kind: "reduce", aggregates: { n: { op: "count" } } },
    {
      allocationScope: scope,
      now: () => now,
      budget: { signal: controller.signal, maxElapsedMs: 1 },
    },
  );
  if (failure === "abort") controller.abort();
  else now = 2;
  await expect(
    target.mergeCheckpoint?.({
      kind: "aggregate-snapshot",
      snapshot: { n: { op: "count", count: 1 } },
    }),
  ).rejects.toMatchObject({
    code: failure === "abort" ? "LAKEQL_ABORTED" : "LAKEQL_BUDGET_EXCEEDED",
  });
  expect(pool.usedBytes).toBe(0);
  scope.dispose();
});
