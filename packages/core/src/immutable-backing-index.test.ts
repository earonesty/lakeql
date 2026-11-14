import { expect, it } from "vitest";
import { col } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { indexImmutableBacking, MemoryScope, type OwnedMemory } from "./memory-scope.js";
import { detachOwnedRow } from "./owned-row-detachment.js";
import type { WindowExpr } from "./window.js";
import { evaluateWindowWorkUnitPartials, partitionWindowWorkUnitRows } from "./window-work-unit.js";

const windows: Record<string, WindowExpr> = {
  rn: { fn: "row_number", args: [], over: { partitionBy: [], orderBy: [{ expr: col("id") }] } },
};

it.each(["partition", "evaluate"] as const)("indexes scalar backing once for %s", async (phase) => {
  const measure = async (reuse: boolean) => {
    const pool = new MemoryBudget(1024 * 1024);
    const source = new MemoryScope(pool);
    const result = new MemoryScope(pool);
    try {
      const row = source.adopt(
        detachOwnedRow({ id: 1, text: "x".repeat(20000), bytes: new Uint8Array([7]) }, pool),
      ).value;
      const owner = source.retainAllocations(undefined);
      let index: MemoryScope | undefined;
      try {
        if (reuse) index = indexImmutableBacking(owner, result);
      } finally {
        owner.release();
      }
      const options = {
        resultScope: result,
        ...(index === undefined ? {} : { immutableSource: index }),
      };
      const entry = { ordinal: { task: 0, row: 0 }, row };
      const output =
        phase === "partition"
          ? partitionWindowWorkUnitRows([entry], windows, 1, options).buckets[0]?.rows[0]?.row
          : (
              await evaluateWindowWorkUnitPartials(
                [{ bucketCount: 1, buckets: [{ bucket: 0, rows: [entry] }] }],
                windows,
                options,
              )
            )[0];
      (row.bytes as Uint8Array)[0] = 9;
      index?.dispose();
      source.dispose();
      expect(output?.text).toBe("x".repeat(20000));
      expect((output?.bytes as Uint8Array)[0]).toBe(7);
      expect(pool.usedBytes).toBeGreaterThanOrEqual(40000);
      return pool.peakBytes;
    } finally {
      source.dispose();
      result.dispose();
      expect(pool.usedBytes).toBe(0);
    }
  };
  const copied = await measure(false);
  const indexed = await measure(true);
  expect(copied - indexed).toBeGreaterThan(38000);
});

it("cleans up index admission failure without releasing source backing", () => {
  const pool = new MemoryBudget(4096);
  const source = new MemoryScope(pool);
  const parent = new MemoryScope(pool);
  const owner = source.allocate(4000, "source", () => "text").retain();
  try {
    expect(() => indexImmutableBacking(owner, parent)).toThrow(/budget/i);
    expect(pool.usedBytes).toBe(4000);
    expect(owner.value).toBe("text");
  } finally {
    owner.release();
    source.dispose();
    parent.dispose();
  }
  expect(pool.usedBytes).toBe(0);
});

it("does not treat opaque external ownership as proven scalar backing", () => {
  const pool = new MemoryBudget(4096);
  const source = new MemoryScope(pool);
  const parent = new MemoryScope(pool);
  const atomic = source.allocate(128, "source", () => "text");
  const opaque: OwnedMemory<string> = {
    value: atomic.value,
    bytes: atomic.bytes,
    budget: pool,
    retain: () => ({ ...opaque }),
    retainIn: () => ({ ...opaque }),
    release() {},
  };
  const index = indexImmutableBacking(opaque, parent);
  expect(index.tryRetainAllocation("text")).toBeUndefined();
  index.dispose();
  parent.dispose();
  source.dispose();
  expect(pool.usedBytes).toBe(0);
});
