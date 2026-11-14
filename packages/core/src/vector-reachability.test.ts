import { expect, it } from "vitest";
import type { Vector } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, retainSelectedAllocations } from "./memory-scope.js";
import {
  isUnionConcatRoutingAllocation,
  planUnionConcat,
  retainsUnionConcatRouting,
} from "./union-concat.js";
import { indexVectorStorage, reachableVectors } from "./vector-reachability.js";

it("walks shared and cyclic descriptor graphs once with admitted workspace", () => {
  const leaf: Vector = { type: "f64", values: Float64Array.of(7) };
  const fields: Record<string, Vector> = Object.create(null);
  const root: Vector = { type: "struct", fields, length: 1 };
  fields.a = leaf;
  fields.b = leaf;
  fields.self = root;
  const scope = new MemoryScope(new MemoryBudget(65536));
  const seen = reachableVectors([root], scope);
  expect([...seen]).toEqual([root, leaf]);
  expect(scope.budget.usedBytes).toBe(64 + 2 * 64);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => reachableVectors([root], tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it("discards unreachable concat routing while retaining all other allocations conservatively", () => {
  const pool = new MemoryBudget(65536);
  const producer = new MemoryScope(pool);
  const values = producer.allocate(16, "payload", () => Float64Array.of(7, 9)).value;
  const leaf: Vector = { type: "f64", values };
  const old = planUnionConcat([leaf, { type: "null", length: 1 }]).execute(producer);
  const source = producer.retainAllocations(old);
  const target = new MemoryScope(pool);
  const scratch = target.childScope();
  const reachable = reachableVectors([leaf], scratch);
  const index = indexVectorStorage(reachable, scratch);
  retainSelectedAllocations(
    source,
    target,
    (value) => !isUnionConcatRoutingAllocation(value) || retainsUnionConcatRouting(value, index),
  );
  scratch.dispose();
  source.release();
  producer.dispose();
  expect(pool.usedBytes).toBe(16);
  expect(values[1]).toBe(9);
  target.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("retains a concat allocation reachable through collection and dictionary aliases", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const union = planUnionConcat([
    { type: "null", length: 1 },
    { type: "null", length: 1 },
  ]).execute(scope);
  const dictionary: Vector = { type: "dict", indices: Uint32Array.of(0), dictionary: union };
  const root: Vector = { type: "list", offsets: Int32Array.of(0, 1), child: dictionary };
  const scratch = scope.childScope();
  expect(reachableVectors([root], scratch).has(union)).toBe(true);
  scratch.dispose();
  scope.dispose();
});

it("walks 12000 nested descriptors without recursive stack growth", () => {
  let vector: Vector = { type: "null", length: 1 };
  for (let i = 0; i < 12000; i++)
    vector = { type: "dict", indices: Uint32Array.of(0), dictionary: vector };
  const scope = new MemoryScope(new MemoryBudget(2 * 1024 * 1024));
  expect(reachableVectors([vector], scope).size).toBe(12001);
  expect(scope.budget.usedBytes).toBe(64 + 12001 * 64);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("recognizes sliced routing and reinterpreted backing aliases", async () => {
  const { sliceVector } = await import("./vector-slice.js");
  const scope = new MemoryScope(new MemoryBudget(65536));
  const union = planUnionConcat([
    { type: "null", length: 8 },
    { type: "null", length: 8 },
  ]).execute(scope);
  if (union.type !== "union") throw Error("Expected union");
  for (const root of [
    sliceVector(union, 2, 4),
    { type: "f64" as const, values: new Float64Array(union.tags.buffer) },
  ]) {
    const scratch = scope.childScope();
    const reachable = reachableVectors([root], scratch);
    expect(reachable.has(union)).toBe(false);
    expect(retainsUnionConcatRouting(union, indexVectorStorage(reachable, scratch))).toBe(true);
    scratch.dispose();
  }
  scope.dispose();
});

it("indexes each unique backing once and never rescans values during routing checks", () => {
  const backing = new ArrayBuffer(32);
  let reads = 0;
  const leaf: Vector = {
    type: "f64",
    get values() {
      reads++;
      return new Float64Array(backing);
    },
  };
  const scope = new MemoryScope(new MemoryBudget(65536));
  const vectors = reachableVectors(
    [leaf, { type: "binary", values: [new Uint8Array(backing), new Uint8Array(backing, 8)] }],
    scope,
  );
  const before = scope.budget.usedBytes;
  const index = indexVectorStorage(vectors, scope);
  expect(index.buffers.size).toBe(1);
  expect(scope.budget.usedBytes - before).toBe(2 * 64 + 64 + 64 + 3 * 64);
  expect(reads).toBe(1);
  const allocation: Extract<Vector, { type: "union" }> = {
    type: "union",
    tags: new Uint8Array(backing),
    offsets: new Uint32Array(0),
    children: [],
  };
  for (let i = 0; i < 1024; i++) expect(retainsUnionConcatRouting(allocation, index)).toBe(true);
  expect(reads).toBe(1);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("admits the alias index before entries and rolls back partial construction", () => {
  const vectors = new Set<Vector>([
    { type: "f64", values: Float64Array.of(1) },
    { type: "f64", values: Float64Array.of(2) },
  ]);
  const scope = new MemoryScope(new MemoryBudget(65536));
  indexVectorStorage(vectors, scope);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(indexVectorStorage(vectors, exact).buffers.size).toBe(2);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => indexVectorStorage(vectors, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it("admits wide terminal roots without allocating per-leaf queue records", () => {
  const roots: Vector[] = Array.from({ length: 1024 }, () => ({ type: "null", length: 1 }));
  const bytes = 64 + roots.length * 64;
  const scope = new MemoryScope(new MemoryBudget(bytes));
  expect(reachableVectors(roots, scope).size).toBe(1024);
  expect(scope.budget.peakBytes).toBe(bytes);
  scope.dispose();
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  expect(() => reachableVectors(roots, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});
