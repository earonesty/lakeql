import { describe, expect, it } from "vitest";
import { col, lit } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import type { WindowExpr } from "./window.js";
import {
  evaluateWindowWorkUnitPartials,
  partitionWindowWorkUnitRows,
  type WindowWorkUnitPartial,
  type WindowWorkUnitRow,
  windowPartitionBucket,
} from "./window-work-unit.js";

const windows: Record<string, WindowExpr> = {
  rn: {
    fn: "row_number",
    args: [],
    over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
  },
  running: {
    fn: { aggregate: "sum" },
    args: [col("amount")],
    over: { partitionBy: [col("region")], orderBy: [{ expr: col("id") }] },
  },
};

describe("window work units", () => {
  it("allocates only occupied buckets even for a large partition directory", () => {
    const rows: WindowWorkUnitRow[] = Array.from({ length: 12 }, (_, row) => ({
      ordinal: { task: 0, row },
      row: { id: row, region: row % 2 === 0 ? "east" : "west" },
    }));
    const partial = partitionWindowWorkUnitRows(rows, windows, Number.MAX_SAFE_INTEGER);
    expect(partial.buckets).toHaveLength(2);
    expect(partial.buckets.map((bucket) => bucket.bucket)).toEqual(
      partial.buckets.map((bucket) => bucket.bucket).sort((left, right) => left - right),
    );
    expect(partial.buckets.flatMap((bucket) => bucket.rows)).toHaveLength(rows.length);
    for (const bucket of partial.buckets) {
      for (const entry of bucket.rows) {
        expect(entry.row).not.toBe(rows[entry.ordinal.row]?.row);
        expect(windowPartitionBucket(entry.row, windows, partial.bucketCount)).toBe(bucket.bucket);
      }
    }
  });

  it.each([
    0,
    -1,
    1.5,
    NaN,
    Infinity,
    Number.MAX_SAFE_INTEGER + 1,
  ])("rejects invalid bucket count %s before processing even an empty input", async (bucketCount) => {
    expect(() => partitionWindowWorkUnitRows([], windows, bucketCount)).toThrow(
      "bucket count must be a positive integer",
    );
    await expect(
      evaluateWindowWorkUnitPartials([], windows, { bucketCount }),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
  });

  it("admits merge storage at the exact peak and releases it on rejection", async () => {
    const partial = partitionWindowWorkUnitRows(
      Array.from({ length: 20 }, (_, row) => ({
        ordinal: { task: 0, row },
        row: { id: row, region: "east", amount: row },
      })),
      windows,
      3,
    );
    const measured = new MemoryBudget(1_000_000);
    const expected = await evaluateWindowWorkUnitPartials([partial], windows, {
      memoryBudget: measured,
    });
    expect(measured.usedBytes).toBe(0);
    const exact = new MemoryBudget(measured.peakBytes);
    await expect(
      evaluateWindowWorkUnitPartials([partial], windows, { memoryBudget: exact }),
    ).resolves.toEqual(expected);
    expect(exact.usedBytes).toBe(0);
    const short = new MemoryBudget(measured.peakBytes - 1);
    await expect(
      evaluateWindowWorkUnitPartials([partial], windows, { memoryBudget: short }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    expect(short.usedBytes).toBe(0);
    const retained = new MemoryScope(measured);
    await evaluateWindowWorkUnitPartials([partial], windows, { resultScope: retained });
    expect(measured.usedBytes).toBeGreaterThan(0);
    retained.dispose();
    expect(measured.usedBytes).toBe(0);
  });

  it("detaches mutable partial payloads and keeps their charges with owned results", async () => {
    const bytes = new Uint8Array(8192).fill(7);
    const partial = partitionWindowWorkUnitRows(
      [{ ordinal: { task: 0, row: 0 }, row: { id: 1, region: "east", amount: 1, bytes } }],
      windows,
      2,
    );
    const pool = new MemoryBudget(65536);
    const scope = new MemoryScope(pool);
    try {
      const result = await evaluateWindowWorkUnitPartials([partial], windows, {
        resultScope: scope,
      });
      expect(pool.usedBytes).toBeGreaterThan(bytes.byteLength);
      bytes.fill(9);
      expect((result[0]?.bytes as Uint8Array)[0]).toBe(7);
    } finally {
      scope.dispose();
    }
    expect(pool.usedBytes).toBe(0);
    const small = new MemoryBudget(4096);
    await expect(
      evaluateWindowWorkUnitPartials([partial], windows, { memoryBudget: small }),
    ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    expect(small.usedBytes).toBe(0);
  });

  it("admits partition retention at its measured peak and cleans up failed admission", () => {
    const rows = [
      { ordinal: { task: 0, row: 0 }, row: { id: 1, region: "east", bytes: new Uint8Array(8192) } },
    ];
    const measured = new MemoryBudget(65536);
    const expected = partitionWindowWorkUnitRows(rows, windows, 3, { memoryBudget: measured });
    expect(measured.usedBytes).toBe(0);
    const exact = new MemoryBudget(measured.peakBytes);
    expect(partitionWindowWorkUnitRows(rows, windows, 3, { memoryBudget: exact })).toEqual(
      expected,
    );
    expect(exact.usedBytes).toBe(0);
    const short = new MemoryBudget(measured.peakBytes - 1);
    expect(() => partitionWindowWorkUnitRows(rows, windows, 3, { memoryBudget: short })).toThrow(
      /budget/i,
    );
    expect(short.usedBytes).toBe(0);
  });

  it("rejects oversized partition-key expressions and releases per-row scratch", () => {
    const pool = new MemoryBudget(65536);
    const scope = new MemoryScope(pool);
    const expanding: Record<string, WindowExpr> = {
      rn: {
        fn: "row_number",
        args: [],
        over: {
          partitionBy: [
            {
              kind: "call",
              fn: "replace",
              args: [col("region"), lit("x"), lit("a".repeat(50000))],
            },
          ],
          orderBy: [],
        },
      },
    };
    expect(() => windowPartitionBucket({ region: "xxx" }, expanding, 2, scope)).toThrow(/budget/i);
    expect(pool.usedBytes).toBe(0);
    for (let i = 0; i < 100; i++) {
      windowPartitionBucket({ region: "east" }, windows, 2, scope);
      expect(pool.usedBytes).toBe(0);
    }
    scope.dispose();
  });

  it("repartitions rows by window partition key and restores source order after evaluation", async () => {
    const firstRows: WindowWorkUnitRow[] = [
      { ordinal: { task: 0, row: 0 }, row: { id: 2, region: "east", amount: 20 } },
      { ordinal: { task: 0, row: 1 }, row: { id: 1, region: "west", amount: 10 } },
    ];
    const secondRows: WindowWorkUnitRow[] = [
      { ordinal: { task: 1, row: 0 }, row: { id: 4, region: "east", amount: 40 } },
      { ordinal: { task: 1, row: 1 }, row: { id: 3, region: "west", amount: 30 } },
    ];
    const partials = [
      partitionWindowWorkUnitRows(firstRows, windows, 3),
      partitionWindowWorkUnitRows(secondRows, windows, 3),
    ];

    expect(
      partials.flatMap((partial) => partial.buckets).every((bucket) => bucket.rows.length > 0),
    ).toBe(true);
    expect(windowPartitionBucket({ id: 1, region: "west", amount: 10 }, windows, 3)).toBe(
      windowPartitionBucket({ id: 3, region: "west", amount: 30 }, windows, 3),
    );

    await expect(
      evaluateWindowWorkUnitPartials(partials, windows, { bucketCount: 3 }),
    ).resolves.toEqual([
      { id: 2, region: "east", amount: 20, rn: 1, running: 20 },
      { id: 1, region: "west", amount: 10, rn: 1, running: 10 },
      { id: 4, region: "east", amount: 40, rn: 2, running: 60 },
      { id: 3, region: "west", amount: 30, rn: 2, running: 40 },
    ]);
  });

  it("rejects invalid bucket shapes with typed errors", async () => {
    expect(() => windowPartitionBucket({ region: "west" }, windows, 0)).toThrow(
      "bucket count must be a positive integer",
    );

    const mismatched: WindowWorkUnitPartial[] = [
      { bucketCount: 2, buckets: [] },
      { bucketCount: 3, buckets: [] },
    ];
    await expect(
      evaluateWindowWorkUnitPartials(mismatched, windows, { bucketCount: 2 }),
    ).rejects.toMatchObject({ code: "LAKEQL_TYPE_ERROR" });
  });
});
