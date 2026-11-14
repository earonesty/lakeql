import { expect, it } from "vitest";
import { batchFromColumns } from "./batch.js";
import { col, gt, mul } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { memoryStore } from "./memory-store.js";
import { Lake, type ScanAdapter } from "./query.js";

it.each([
  false,
  true,
])("releases aggregate scan ownership on success/failure (%s)", async (fail) => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(65536);
  let closed = 0;
  let reads = 0;
  const scanner: ScanAdapter = {
    scan() {
      throw new Error("Unexpected row fallback");
    },
    async *scanOwnedVectorBatches(_path, options) {
      try {
        for (let index = 0; index < 3; index++) {
          const scope = new MemoryScope(options.memoryBudget);
          const batch = batchFromColumns({ id: [index, index + 1] }, scope);
          const owner = scope.retainAllocations({ rowOffset: index * 2, batch });
          scope.dispose();
          reads++;
          yield owner;
          expect(() => owner.value).toThrow(/released/);
        }
      } finally {
        closed++;
      }
    },
  };
  const lake = new Lake({ store, scanner, memoryPool: pool });
  const result = lake
    .path("table")
    .groupBy(fail ? ["id"] : [])
    .aggregate({ total: { op: "sum", column: "id" } }, fail ? { maxGroups: 1 } : {});
  if (fail) await expect(result).rejects.toThrow();
  else expect(await result).toEqual([{ total: 9 }]);
  expect(reads).toBe(fail ? 1 : 3);
  expect(closed).toBe(1);
  expect(pool.peakBytes).toBeGreaterThan(16);
  expect(pool.peakBytes).toBeLessThanOrEqual(pool.maxBytes);
  expect(pool.usedBytes).toBe(0);
});

it("bounds retained top-k across owned scan batches and releases failed merges", async () => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  async function run(groups: number, maxBytes: number) {
    const pool = new MemoryBudget(maxBytes);
    let closed = false;
    const scanner: ScanAdapter = {
      scan() {
        throw new Error("Unexpected borrowed scan");
      },
      async *scanOwnedVectorBatches(_path, options) {
        try {
          for (let group = 0; group < groups; group++) {
            const scope = new MemoryScope(options.memoryBudget);
            try {
              const batch = batchFromColumns({ id: [7, 1, 5, 3] }, scope);
              yield scope.retainAllocations({ rowOffset: group * 4, batch });
            } finally {
              scope.dispose();
            }
          }
        } finally {
          closed = true;
        }
      },
    };
    try {
      const lake = new Lake({ store, scanner, memoryPool: pool });
      const rows = await lake
        .path("table")
        .orderBy([{ column: "id" }])
        .limit(2)
        .toArray();
      expect(rows).toEqual([{ id: 1 }, { id: 1 }]);
      return pool.peakBytes;
    } finally {
      expect(closed).toBe(true);
      expect(pool.usedBytes).toBe(0);
    }
  }
  const peak = await run(3, 65536);
  expect(await run(30, peak)).toBe(peak);
  await expect(run(30, peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});

it.each([
  false,
  true,
])("releases projected owned scans after early limit (filtered=%s)", async (filtered) => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  async function run(maxBytes: number) {
    const pool = new MemoryBudget(maxBytes);
    let reads = 0;
    let closed = false;
    const scanner: ScanAdapter = {
      scan() {
        throw new Error("Unexpected borrowed scan");
      },
      async *scanOwnedVectorBatches(_path, options) {
        try {
          for (let group = 0; group < 10; group++) {
            expect(pool.usedBytes).toBe(0);
            const scope = new MemoryScope(options.memoryBudget);
            try {
              const batch = batchFromColumns({ id: [1, 2, 3] }, scope);
              reads++;
              yield scope.retainAllocations({ rowOffset: group * 3, batch });
            } finally {
              scope.dispose();
            }
          }
        } finally {
          closed = true;
        }
      },
    };
    try {
      const lake = new Lake({ store, scanner, memoryPool: pool });
      let query = lake
        .path("table")
        .project({ doubled: mul(col("id"), 2) })
        .limit(1);
      if (filtered) query = query.where(gt("id", 1));
      expect(await query.toArray()).toEqual([{ id: filtered ? 2 : 1, doubled: filtered ? 4 : 2 }]);
      expect(reads).toBe(1);
      return pool.peakBytes;
    } finally {
      expect(closed).toBe(true);
      expect(pool.usedBytes).toBe(0);
    }
  }
  const peak = await run(65536);
  expect(await run(peak)).toBe(peak);
  await expect(run(peak - 1)).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
});

it.each([
  "grouped-count",
  "median",
  "count_distinct",
] as const)("admits public %s state beyond its two-row scan backing", async (operation) => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  for (const maxBytes of [16, 1024 * 1024]) {
    const pool = new MemoryBudget(maxBytes);
    let closed = false;
    const scanner: ScanAdapter = {
      scan() {
        throw Error("Unexpected row fallback");
      },
      async *scanOwnedVectorBatches(_path, options) {
        try {
          for (let index = 0; index < 128; index += 2) {
            const scope = new MemoryScope(options.memoryBudget);
            try {
              const batch = batchFromColumns({ id: [index, index + 1] }, scope);
              yield scope.retainAllocations({ rowOffset: index, batch });
            } finally {
              scope.dispose();
            }
          }
        } finally {
          closed = true;
        }
      },
    };
    const lake = new Lake({ store, scanner, memoryPool: pool });
    const query =
      operation === "grouped-count"
        ? lake
            .path("table")
            .groupBy(["id"])
            .aggregate({ n: { op: "count" } })
        : lake
            .path("table")
            .groupBy([])
            .aggregate({ value: { op: operation, column: "id" } });
    if (maxBytes === 16)
      await expect(query).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    else {
      const rows = await query;
      expect(rows).toEqual(
        operation === "grouped-count"
          ? Array.from({ length: 128 }, (_, id) => ({ id, n: 1 }))
          : [{ value: operation === "median" ? 63.5 : 128 }],
      );
      expect(pool.peakBytes).toBeGreaterThan(16);
    }
    expect(closed).toBe(true);
    expect(pool.usedBytes).toBe(0);
    expect(pool.peakBytes).toBeLessThanOrEqual(maxBytes);
  }
});

it("preserves aggregate state when continuation placement changes between batches", async () => {
  const { CpuPhysicalBackend } = await import("./physical.js");
  let opens = 0;
  class SelectiveBackend extends CpuPhysicalBackend {
    override openReduction(...args: Parameters<CpuPhysicalBackend["openReduction"]>) {
      opens++;
      return super.openReduction(...args);
    }
    override assess(...args: Parameters<CpuPhysicalBackend["assess"]>) {
      const assessment = super.assess(...args);
      if (args[1].requireReductionContinuation && args[0].input.rowCount === 1)
        return { ...assessment, supported: false };
      return assessment;
    }
  }
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(1024 * 1024);
  const scanner: ScanAdapter = {
    scan() {
      throw Error("Unexpected row fallback");
    },
    async *scanOwnedVectorBatches(_path, options) {
      let rowOffset = 0;
      for (const ids of [[1, 2], [3], [4, 5]]) {
        const scope = new MemoryScope(options.memoryBudget);
        try {
          const batch = batchFromColumns({ id: ids }, scope);
          yield scope.retainAllocations({ rowOffset, batch });
          rowOffset += ids.length;
        } finally {
          scope.dispose();
        }
      }
    },
  };
  const lake = new Lake({
    store,
    scanner,
    memoryPool: pool,
    physicalExecution: { backends: [new SelectiveBackend("aaa")] },
  });
  const rows = await lake
    .path("table")
    .groupBy([])
    .aggregate({
      n: { op: "count" },
      total: { op: "sum", column: "id" },
      median: { op: "median", column: "id" },
    });
  expect(rows).toEqual([{ n: 5, total: 15, median: 3 }]);
  expect(opens).toBe(2);
  expect(pool.usedBytes).toBe(0);
});

it.each(
  [false, true].flatMap((grouped) =>
    [0, 7, 99].flatMap((threshold) =>
      [false, true].map((borrowed) => ({ grouped, threshold, borrowed })),
    ),
  ),
)("joins deferred aggregate vectors (grouped=$grouped, threshold=$threshold, borrowed=$borrowed)", async ({
  grouped,
  threshold,
  borrowed,
}) => {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = new MemoryBudget(1024 * 1024);
  const reads: { columns: string[]; start: number; end: number }[] = [];
  let released = 0;
  const scanner: ScanAdapter = {
    scan() {
      throw Error("Unexpected row fallback");
    },
    async *scanOwnedVectorBatches(_path, options) {
      const start = options.rowStart ?? 0;
      const end = options.rowEnd ?? 10;
      const columns = options.columns ?? ["id", "value"];
      reads.push({ columns: [...columns], start, end });
      const scope = new MemoryScope(options.memoryBudget);
      try {
        const values: Record<string, number[]> = {};
        for (const column of columns)
          values[column] = Array.from(
            { length: end - start },
            (_, i) => (i + start) * (column === "value" ? 10 : 1),
          );
        const batch = batchFromColumns(values, scope);
        const owner = scope.retainAllocations({ rowOffset: start, batch });
        yield owner;
      } finally {
        scope.dispose();
        released++;
      }
    },
  };
  if (borrowed) {
    const source = scanner.scanOwnedVectorBatches;
    if (!source) throw Error("Missing source");
    delete scanner.scanOwnedVectorBatches;
    scanner.scanVectorBatches = async function* (path, options) {
      for await (const owner of source(path, options)) {
        try {
          yield owner.value;
        } finally {
          owner.release();
        }
      }
    };
    scanner.scanColumns = async function* (path, options) {
      for await (const input of scanner.scanVectorBatches?.(path, options) ?? []) yield input.batch;
    };
  }
  const lake = new Lake({ store, scanner, memoryPool: pool });
  const rows = await lake
    .path("table")
    .batchSize(10)
    .where(gt("id", threshold))
    .groupBy(grouped ? ["id"] : [])
    .aggregate({ total: { op: "sum", column: "value" }, n: { op: "count" } });
  const ids = Array.from({ length: 10 }, (_, id) => id).filter((id) => id > threshold);
  expect(rows).toEqual(
    grouped
      ? ids.map((id) => ({ id, total: id * 10, n: 1 }))
      : [{ total: ids.reduce((sum, id) => sum + id * 10, 0), n: ids.length }],
  );
  const expectedReads = [{ columns: ["id"], start: 0, end: 10 }];
  if (threshold === 7) expectedReads.push({ columns: ["value"], start: 8, end: 10 });
  if (threshold === 0) expectedReads.push({ columns: ["id", "value"], start: 0, end: 10 });
  expect(reads).toEqual(expectedReads);
  expect(released).toBe(expectedReads.length);
  expect(pool.usedBytes).toBe(0);
});
