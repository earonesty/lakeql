import { expect, it } from "vitest";
import { type Batch, type Vector, vectorFromValues, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { compactOwnedUnionBatch, needsUnionCompaction } from "./union-compaction.js";
import { planUnionConcat } from "./union-concat.js";
import { concatOwnedBatches, gatherOwnedBatch } from "./vector-sort.js";

it("automatically limits append history while retaining correct owned payload", () => {
  const pool = new MemoryBudget(256 * 1024);
  const input = (values: unknown[]) => {
    const scope = new MemoryScope(pool);
    const batch: Batch = {
      rowCount: values.length,
      columns: { v: vectorFromValues(values, scope) },
    };
    const owner = scope.retainAllocations(batch);
    scope.dispose();
    return owner;
  };
  let output = input(["seed", 0]);
  for (let i = 1; i <= 128; i++) {
    const next = input([i]);
    const joined = concatOwnedBatches([output, next]);
    output.release();
    next.release();
    output = joined;
    expect(needsUnionCompaction(output.value)).toBe(false);
  }
  const vector = output.value.columns.v;
  if (vector === undefined) throw Error("Missing output");
  expect(vectorValue(vector, 0)).toBe("seed");
  for (let i = 0; i <= 128; i++) expect(vectorValue(vector, i + 1)).toBe(i);
  expect(pool.usedBytes).toBeLessThan(65536);
  output.release();
  expect(pool.usedBytes).toBe(0);
});

it("rolls back failed compaction without invalidating source ownership", () => {
  const pool = new MemoryBudget(1024 * 1024);
  const producer = new MemoryScope(pool);
  let vector: Vector = vectorFromValues(["seed", 0], producer);
  for (let i = 1; i <= 20; i++)
    vector = planUnionConcat([vector, vectorFromValues([i], producer)]).execute(producer);
  const source = producer.retainAllocations({ rowCount: 22, columns: { v: vector } });
  producer.dispose();
  const roomy = pool.child(512 * 1024);
  const compacted = compactOwnedUnionBatch(source, roomy);
  expect(needsUnionCompaction(compacted.value)).toBe(false);
  const peak = roomy.peakBytes;
  compacted.release();
  expect(roomy.usedBytes).toBe(0);
  const exact = pool.child(peak);
  compactOwnedUnionBatch(source, exact).release();
  expect(exact.usedBytes).toBe(0);
  const tight = pool.child(peak - 1);
  expect(() => compactOwnedUnionBatch(source, tight)).toThrow(/budget/);
  expect(tight.usedBytes).toBe(0);
  expect(vectorValue(source.value.columns.v, 21)).toBe(20);
  source.release();
  expect(pool.usedBytes).toBe(0);
});

it("compacts selection-hidden history preserving reordered duplicates and rollback", () => {
  const pool = new MemoryBudget(1024 * 1024);
  const producer = new MemoryScope(pool);
  let vector: Vector = vectorFromValues(["seed", 0], producer);
  for (let i = 1; i <= 20; i++)
    vector = planUnionConcat([vector, vectorFromValues([i], producer)]).execute(producer);
  vector = {
    type: "dict",
    dictionary: vector,
    indices: producer.allocate(16, "selection", () => Uint32Array.of(21, 0, 21, 1)).value,
  };
  const source = producer.retainAllocations({ rowCount: 4, columns: { v: vector } });
  producer.dispose();
  expect(needsUnionCompaction(source.value)).toBe(true);
  const roomy = pool.child(512 * 1024);
  const output = compactOwnedUnionBatch(source, roomy);
  expect(needsUnionCompaction(output.value)).toBe(false);
  expect([0, 1, 2, 3].map((row) => vectorValue(output.value.columns.v, row))).toEqual([
    20,
    "seed",
    20,
    0,
  ]);
  const peak = roomy.peakBytes;
  output.release();
  const exact = pool.child(peak);
  compactOwnedUnionBatch(source, exact).release();
  expect(exact.usedBytes).toBe(0);
  const tight = pool.child(peak - 1);
  expect(() => compactOwnedUnionBatch(source, tight)).toThrow(/budget/);
  expect(tight.usedBytes).toBe(0);
  expect(vectorValue(source.value.columns.v, 0)).toBe(20);
  source.release();
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "numbers",
  "strings",
  "binary",
  "struct",
  "list",
])("bounds repeated concat/selection ownership, mode=%s", (mode) => {
  const nested = mode === "struct" || mode === "list";
  const pool = new MemoryBudget((nested ? 32 : 16) * 1024);
  const input = (values: unknown[]) => {
    const scope = new MemoryScope(pool);
    try {
      return scope.retainAllocations({
        rowCount: values.length,
        columns: { v: vectorFromValues(values, scope) },
      });
    } finally {
      scope.dispose();
    }
  };
  let output = input(["seed", 0]);
  for (let i = 1; i <= 512; i++) {
    const valueAt = (n: number) => {
      if (mode === "struct") return { id: n, nested: { values: [n, `row-${n}`, null] } };
      if (mode === "list") return [n, `row-${n}`, [n + 1, null]];
      if (mode === "strings") return `row-${n}`;
      if (mode === "binary") {
        const backing = new Uint8Array(256).fill(n % 251);
        new DataView(backing.buffer).setUint32(16, n, true);
        return backing.subarray(16, 144);
      }
      return n;
    };
    const next = input([valueAt(i)]);
    const joined = concatOwnedBatches([output, next]);
    output.release();
    next.release();
    expect(needsUnionCompaction(joined.value)).toBe(false);
    const selected = gatherOwnedBatch(joined, [2, 0]);
    joined.release();
    output = selected;
    expect(pool.usedBytes).toBeLessThan(nested ? 16384 : 4096);
    expect(vectorValue(output.value.columns.v, 0)).toEqual(valueAt(i));
    expect(vectorValue(output.value.columns.v, 1)).toEqual(i === 1 ? "seed" : valueAt(i - 1));
  }
  output.release();
  expect(pool.usedBytes).toBe(0);
});

it("drops obsolete typed backing but preserves differently typed aliases and binary cells", () => {
  const pool = new MemoryBudget(65536);
  const producer = new MemoryScope(pool);
  const obsolete = producer.allocate(4096, "obsolete", () => new Uint8Array(4096));
  const shared = producer.allocate(16, "shared", () => new Uint8Array(16)).value;
  new Float64Array(shared.buffer).set([12, 34]);
  const binary = producer.allocate(8, "binary cell", () =>
    Uint8Array.of(1, 2, 3, 4, 5, 6, 7, 8),
  ).value;
  const source = producer.retainAllocations({
    rowCount: 1,
    columns: {
      number: { type: "f64", values: new Float64Array(shared.buffer, 8, 1) } as Vector,
      bytes: { type: "binary", values: [binary.subarray(2, 4)] } as Vector,
    },
  });
  expect(obsolete.bytes).toBe(4096);
  producer.dispose();
  const output = compactOwnedUnionBatch(source);
  source.release();
  // New columns + batch metadata, and precisely the two live backing allocations.
  expect(pool.usedBytes).toBe(192 + 192 + 16 + 8);
  expect(vectorValue(output.value.columns.number, 0)).toBe(34);
  expect(vectorValue(output.value.columns.bytes, 0)).toEqual(Uint8Array.of(3, 4));
  output.release();
  expect(pool.usedBytes).toBe(0);
});
