import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import type { Batch } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import { OwnedTopKRuns } from "./owned-top-k-runs.js";

function batch(pool: MemoryBudget, ids: number[]): OwnedMemory<Batch> {
  const scope = new MemoryScope(pool);
  try {
    const result = scope.allocate(256 + ids.length * 4, "fixture", () => ({
      rowCount: ids.length,
      columns: { id: { type: "i32" as const, values: Int32Array.from(ids) } },
    }));
    return result.retain();
  } finally {
    scope.dispose();
  }
}
function ids(owner: OwnedMemory<Batch>): number[] {
  const vector = owner.value.columns.id;
  if (vector?.type !== "i32") throw Error("fixture shape");
  return Array.from(vector.values);
}

it.each([0, 1, 7, 64, 1000])("preserves stable winners across unequal batches at k=%i", (k) => {
  const pool = new MemoryBudget(1 << 20);
  const parent = new MemoryScope(pool);
  const compare = (a: number, b: number) => (a % 7) - (b % 7);
  const runs = new OwnedTopKRuns(parent, k, (a, b) =>
    batch(pool, [...ids(a), ...ids(b)].sort(compare).slice(0, k)),
  );
  const all: number[] = [];
  for (let start = 0; start < 300; ) {
    const size = Math.min(1 + ((start * 13) % 31), 300 - start);
    const values = Array.from({ length: size }, (_, i) => start + i);
    all.push(...values);
    const input = batch(pool, values.sort(compare).slice(0, k));
    runs.append(input);
    input.release();
    start += size;
  }
  const output = runs.finish();
  parent.dispose();
  expect(output === undefined ? [] : ids(output)).toEqual(all.sort(compare).slice(0, k));
  output?.release();
  expect(pool.usedBytes).toBe(0);
});

it("bounds full-limit merge volume logarithmically instead of rescanning every prefix", () => {
  const volumes: number[] = [];
  for (const count of [128, 256, 512]) {
    const pool = new MemoryBudget(1 << 20);
    const parent = new MemoryScope(pool);
    let visited = 0;
    const runs = new OwnedTopKRuns(parent, count, (a, b) => {
      visited += a.value.rowCount + b.value.rowCount;
      return batch(pool, [...ids(a), ...ids(b)]);
    });
    for (let i = 0; i < count; i++) {
      const input = batch(pool, [i]);
      runs.append(input);
      input.release();
    }
    const output = runs.finish();
    if (output === undefined) throw Error("missing winners");
    expect(ids(output)).toEqual(Array.from({ length: count }, (_, i) => i));
    expect(visited).toBeLessThanOrEqual(count * (Math.ceil(Math.log2(count)) + 1));
    volumes.push(visited);
    output?.release();
    parent.dispose();
    expect(pool.usedBytes).toBe(0);
  }
  expect((volumes[2] ?? Infinity) / (volumes[1] ?? 1)).toBeLessThan(2.4);
});

it("keeps exact prefix winners available when eager checks are requested", () => {
  const pool = new MemoryBudget(65536);
  const parent = new MemoryScope(pool);
  const runs = new OwnedTopKRuns(
    parent,
    8,
    (a, b) => batch(pool, [...ids(a), ...ids(b)].sort((a, b) => b - a).slice(0, 8)),
    true,
  );
  for (let i = 0; i < 32; i++) {
    const input = batch(pool, [i]);
    runs.append(input);
    input.release();
    expect(runs.prefix?.rowCount).toBe(Math.min(i + 1, 8));
  }
  runs.finish()?.release();
  parent.dispose();
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "throw",
  "invalid",
  "dispose",
])("cleans up %s reducer failure and preserves caller ownership", (mode) => {
  const pool = new MemoryBudget(65536);
  const parent = new MemoryScope(pool);
  const runs = new OwnedTopKRuns(parent, 2, () => {
    if (mode === "throw") throw Error("reducer");
    if (mode === "dispose") runs.dispose();
    return batch(pool, mode === "invalid" ? [1, 2, 3] : [1, 2]);
  });
  const input = batch(pool, [1]);
  runs.append(input);
  expect(() => runs.append(input)).toThrow();
  expect(ids(input)).toEqual([1]);
  expect(pool.usedBytes).toBe(input.bytes);
  expect(() => runs.finish()).toThrow(/disposed/);
  input.release();
  parent.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("admits the bounded directory before construction at exact and one-byte-short caps", () => {
  const bytes = c.object + 6 * c.property + c.array + 8 * c.arraySlot;
  for (const cap of [bytes - 1, bytes]) {
    const pool = new MemoryBudget(cap);
    const parent = new MemoryScope(pool);
    const create = () =>
      new OwnedTopKRuns(parent, 64, () => {
        throw Error("unused");
      });
    if (cap < bytes) expect(create).toThrow(/budget/i);
    else create().dispose();
    parent.dispose();
    expect(pool.usedBytes).toBe(0);
  }
});

it("releases retained runs when the parent is disposed before finish", () => {
  const pool = new MemoryBudget(65536);
  const parent = new MemoryScope(pool);
  const runs = new OwnedTopKRuns(parent, 64, () => {
    throw Error("unused");
  });
  const input = batch(pool, [1, 2, 3]);
  runs.append(input);
  input.release();
  parent.dispose();
  expect(pool.usedBytes).toBe(0);
  expect(() => runs.finish()).toThrow(/disposed/);
  runs.dispose();
});

it("releases both sides of a failed final merge", () => {
  const pool = new MemoryBudget(65536);
  const parent = new MemoryScope(pool);
  const runs = new OwnedTopKRuns(parent, 64, () => {
    throw Error("final merge");
  });
  for (const values of [[1, 2, 3, 4, 5], [6]]) {
    const input = batch(pool, values);
    runs.append(input);
    input.release();
  }
  expect(() => runs.finish()).toThrow("final merge");
  expect(pool.usedBytes).toBe(0);
  parent.dispose();
});

it("bounds retained payload independently of the number of appended batches", () => {
  const pool = new MemoryBudget(16384);
  const parent = new MemoryScope(pool);
  const k = 32;
  const runs = new OwnedTopKRuns(parent, k, (a, b) =>
    batch(pool, [...ids(a), ...ids(b)].sort((a, b) => b - a).slice(0, k)),
  );
  for (let i = 0; i < 2048; i++) {
    const input = batch(pool, [i]);
    runs.append(input);
    input.release();
    expect(pool.usedBytes).toBeLessThan(4096);
  }
  const output = runs.finish();
  if (output === undefined) throw Error("missing winners");
  expect(ids(output)).toEqual(Array.from({ length: k }, (_, i) => 2047 - i));
  parent.dispose();
  expect(pool.usedBytes).toBe(output.bytes);
  output.release();
  expect(pool.usedBytes).toBe(0);
});
