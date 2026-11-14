import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";

it("keeps partition byte sizes and ownership independent when reusing the source ledger", () => {
  const budget = new MemoryBudget(9);
  const original = budget.reserve(9, "stage");
  const parts = original.partition([2, 7]);
  const first = parts[0];
  const last = parts[1];
  if (!first || !last) throw new Error("Missing partition");
  expect(original.bytes).toBe(9);
  expect(first.bytes).toBe(2);
  expect(last.bytes).toBe(7);
  const retained = first.retain();
  last.release();
  expect(budget.usedBytes).toBe(2);
  first.release();
  expect(budget.usedBytes).toBe(2);
  original.release();
  retained.release();
  expect(budget.usedBytes).toBe(0);
});

it("transfers whole-stage admission into independent scratch and output lifetimes", () => {
  const root = new MemoryBudget(10);
  const job = root.child(10);
  const stage = job.reserve(10, "complete working set");
  let reclamations = 0;
  root.registerReclaimer(() => reclamations++);
  const parts = stage.partition([6, 4]);
  const scratch = parts[0];
  const output = parts[1];
  if (!scratch || !output) throw new Error("Missing partitions");
  stage.release(); // Consumed source cannot release transferred capacity.
  expect(() => stage.retain()).toThrow("released");
  expect(root.usedBytes).toBe(10);
  expect(job.usedBytes).toBe(10);
  expect(root.peakBytes).toBe(10);
  expect(reclamations).toBe(0);
  scratch.release();
  expect(root.usedBytes).toBe(4);
  const sibling = root.child(10);
  const nextStage = sibling.reserve(6, "next stage");
  const transferred = output.retainIn(sibling);
  expect(sibling.usedBytes).toBe(10);
  output.release();
  expect(job.usedBytes).toBe(0);
  transferred.release();
  nextStage.release();
  expect(root.usedBytes).toBe(0);
});

it("rejects invalid partitions without consuming or changing the reservation", () => {
  const budget = new MemoryBudget(10);
  const stage = budget.reserve(10, "stage");
  for (const parts of [[9], [11], [-1, 11], [NaN], [Infinity], [0.5, 9.5], []]) {
    expect(() => stage.partition(parts)).toThrow();
    expect(budget.usedBytes).toBe(10);
  }
  const alias = stage.retain();
  expect(() => stage.partition([5, 5])).toThrow("exclusive");
  alias.release();
  const parts = stage.partition([0, 10]);
  for (const part of parts) part.release();
  expect(budget.usedBytes).toBe(0);
  expect(() => stage.partition([10])).toThrow("released");
});

it("prevents splitting shared sibling extents and permits recursive exclusive partitioning", () => {
  const root = new MemoryBudget(12);
  const left = root.child();
  const right = root.child();
  const owner = left.reserve(12, "stage");
  const alias = owner.retainIn(right);
  expect(() => owner.partition([6, 6])).toThrow("exclusive");
  owner.release();
  const parts = alias.partition([7, 5]);
  const first = parts[0];
  if (!first) throw new Error("Missing partition");
  const nested = first.partition([3, 4]);
  for (const part of [...parts, ...nested]) part.release();
  expect(root.usedBytes).toBe(0);
  expect(left.usedBytes).toBe(0);
  expect(right.usedBytes).toBe(0);
  expect(root.peakBytes).toBe(12);
});

it("allows consuming an empty reservation without leaving charges", () => {
  const budget = new MemoryBudget(0);
  const empty = budget.reserve(0, "empty");
  expect(empty.partition([])).toEqual([]);
  expect(() => empty.retain()).toThrow("released");
  empty.release();
  expect(budget.usedBytes).toBe(0);
});
