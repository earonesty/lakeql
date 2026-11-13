import { expect, it } from "vitest";
import { batchFromColumns, materializeBatchRows } from "./batch.js";
import { gatherDeferredColumns, type PositionedBatch } from "./deferred-gather.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";

function input(pool: MemoryBudget, offset = 100): OwnedMemory<PositionedBatch> {
  const scope = new MemoryScope(pool);
  try {
    return scope.retainAllocations({
      rowOffset: offset,
      batch: batchFromColumns({ id: [0, 1, 2, 3, 4, 5] }, scope),
    });
  } finally {
    scope.dispose();
  }
}

it("joins sparse selected positions across bounded windows and fetched batch boundaries", async () => {
  const pool = new MemoryBudget(4096);
  const source = input(pool);
  const windows: number[][] = [];
  const retained: OwnedMemory<import("./batch.js").Batch>[] = [];
  let closed = 0;
  async function* read(start: number, end: number) {
    windows.push([start, end]);
    try {
      for (let position = start; position < end; position++) {
        const scope = new MemoryScope(pool);
        try {
          yield scope.retainAllocations({
            rowOffset: position,
            batch: batchFromColumns({ text: [`v${position}`] }, scope),
          });
        } finally {
          scope.dispose();
        }
      }
    } finally {
      closed++;
    }
  }
  for await (const output of gatherDeferredColumns(source, Uint32Array.of(0, 2, 5), 3, read))
    retained.push(output);
  source.release();
  expect(windows).toEqual([
    [100, 103],
    [105, 106],
  ]);
  expect(closed).toBe(2);
  expect(retained.flatMap((owner) => materializeBatchRows(owner.value))).toEqual([
    { id: 0, text: "v100" },
    { id: 2, text: "v102" },
    { id: 5, text: "v105" },
  ]);
  expect(pool.usedBytes).toBeGreaterThan(0);
  for (const owner of retained) owner.release();
  expect(pool.usedBytes).toBe(0);
});

it.each([
  Uint32Array.of(2, 1),
  Uint32Array.of(1, 1),
  Uint32Array.of(6),
])("rejects invalid selected positions before reading: %s", async (indices) => {
  const pool = new MemoryBudget(4096);
  const source = input(pool);
  let reads = 0;
  async function* read() {
    reads++;
    yield* [];
  }
  await expect(gatherDeferredColumns(source, indices, 3, read).next()).rejects.toMatchObject({
    code: "LAKEQL_VALIDATION_ERROR",
  });
  expect(reads).toBe(0);
  source.release();
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "missing",
  "overlap",
  "failure",
  "budget",
])("cleans up deferred %s failures", async (mode) => {
  const pool = new MemoryBudget(mode === "budget" ? 64 : 4096);
  const source = input(pool);
  let closed = false;
  async function* read() {
    const scope = new MemoryScope(pool);
    try {
      if (mode === "failure") throw new Error("remote read failed");
      yield scope.retainAllocations({
        rowOffset: mode === "missing" ? 101 : 100,
        batch: batchFromColumns({ [mode === "overlap" ? "id" : "payload"]: [10, 20] }, scope),
      });
    } finally {
      scope.dispose();
      closed = true;
    }
  }
  const pending = gatherDeferredColumns(source, Uint32Array.of(0, 2), 3, read).next();
  if (mode === "failure") await expect(pending).rejects.toThrow("remote read failed");
  else
    await expect(pending).rejects.toMatchObject({
      code: mode === "budget" ? "LAKEQL_BUDGET_EXCEEDED" : "LAKEQL_VALIDATION_ERROR",
    });
  expect(closed).toBe(true);
  source.release();
  expect(pool.usedBytes).toBe(0);
});

it("closes an interrupted fetched iterator while independently retained output stays valid", async () => {
  const pool = new MemoryBudget(4096);
  const source = input(pool);
  let closed = false;
  async function* read() {
    const scope = new MemoryScope(pool);
    try {
      yield scope.retainAllocations({
        rowOffset: 100,
        batch: batchFromColumns({ payload: [Uint8Array.of(7)] }, scope),
      });
      throw new Error("must not request another batch");
    } finally {
      closed = true;
      scope.dispose();
    }
  }
  let retained: OwnedMemory<import("./batch.js").Batch> | undefined;
  for await (const result of gatherDeferredColumns(source, Uint32Array.of(0, 2), 3, read)) {
    retained = result;
    break;
  }
  source.release();
  expect(closed).toBe(true);
  if (!retained) throw new Error("Missing output");
  expect(materializeBatchRows(retained.value)).toEqual([{ id: 0, payload: Uint8Array.of(7) }]);
  retained.release();
  expect(pool.usedBytes).toBe(0);
});
