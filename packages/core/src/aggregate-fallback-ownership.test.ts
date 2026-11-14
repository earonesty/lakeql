import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { memoryStore } from "./memory-store.js";
import { CpuPhysicalBackend } from "./physical.js";
import { Lake } from "./query.js";

it.each([false, true])("retains only current fallback checkpoint (grouped=%s)", async (grouped) => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const run = async (batches: number, fail = false) => {
    const pool = new MemoryBudget(1024 * 1024);
    let executions = 0;
    class OneShotBackend extends CpuPhysicalBackend {
      override assess(...args: Parameters<CpuPhysicalBackend["assess"]>) {
        const result = super.assess(...args);
        return args[1].requireReductionContinuation ? { ...result, supported: false } : result;
      }
      override async execute(...args: Parameters<CpuPhysicalBackend["execute"]>) {
        expect(args[2]?.allocationScope).toBeDefined();
        if (++executions === 3 && fail) throw Error("checkpoint execution failed");
        return super.execute(...args);
      }
    }
    const lake = new Lake({
      store,
      memoryPool: pool,
      physicalExecution: { backends: [new OneShotBackend()] },
      scanner: {
        scan() {
          throw Error("Unexpected row fallback");
        },
        async *scanColumns() {
          for (let i = 0; i < batches; i++)
            yield batchFromColumns({ key: ["a", "a"], value: [1, 2] });
        },
      },
    });
    try {
      const result = await lake
        .path("table")
        .groupBy(grouped ? ["key"] : [])
        .aggregate({ n: { op: "count" }, total: { op: "sum", column: "value" } });
      expect(result).toEqual([
        { ...(grouped ? { key: "a" } : {}), n: 2 * batches, total: 3 * batches },
      ]);
      expect(executions).toBe(batches);
      return pool.peakBytes;
    } finally {
      expect(pool.usedBytes).toBe(0);
    }
  };
  expect(await run(64)).toBe(await run(16));
  await expect(run(16, true)).rejects.toThrow("checkpoint execution failed");
});

it("routes unsupported reduction shapes to row aggregation without unadmitted snapshots", async () => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(65536);
  let columnReads = 0;
  let rowReads = 0;
  const values = [Uint8Array.of(1), null];
  const lake = new Lake({
    store,
    memoryPool: pool,
    scanner: {
      async *scan() {
        rowReads++;
        yield values.map((value) => ({ value }));
      },
      async *scanColumns() {
        columnReads++;
        yield batchFromColumns({ value: values });
      },
    },
  });
  expect(
    await lake
      .path("table")
      .groupBy([])
      .aggregate({ n: { op: "count", column: "value" } }),
  ).toEqual([{ n: 1 }]);
  expect(columnReads).toBe(1);
  expect(rowReads).toBe(1);
  expect(pool.usedBytes).toBe(0);
});
