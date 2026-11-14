import { expect, it } from "vitest";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { buildUnionRouting } from "./union-routing.js";

it("routes interleaved domains with stable compact children and single classification", () => {
  const tags = [2, 0, 2, 1, 0, 2];
  const calls: number[] = [];
  const scope = new MemoryScope(new MemoryBudget(65536));
  const routing = buildUnionRouting(
    tags.length,
    4,
    (row) => {
      calls.push(row);
      return tags[row] ?? 0;
    },
    scope,
  );
  expect(calls).toEqual([0, 1, 2, 3, 4, 5]);
  expect([...routing.tags]).toEqual(tags);
  expect([...routing.offsets]).toEqual([0, 0, 1, 0, 1, 2]);
  expect([...routing.childStarts]).toEqual([0, 2, 3, 6, 6]);
  expect([...routing.sourceRows]).toEqual([1, 4, 3, 0, 2, 5]);
  const peak = scope.budget.peakBytes;
  expect(scope.budget.usedBytes).toBeLessThan(peak);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(buildUnionRouting(tags.length, 4, (row) => tags[row] ?? 0, exact)).toEqual(routing);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  let invoked = false;
  expect(() =>
    buildUnionRouting(
      tags.length,
      4,
      () => {
        invoked = true;
        return 0;
      },
      tight,
    ),
  ).toThrow(/budget/);
  expect(invoked).toBe(false);
  expect(tight.budget.peakBytes).toBe(0);
  tight.dispose();
});

it("supports empty inputs and every byte tag", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const empty = buildUnionRouting(
    0,
    256,
    () => {
      throw Error("unreachable");
    },
    scope,
  );
  expect(empty.childStarts).toEqual(new Uint32Array(257));
  const full = buildUnionRouting(256, 256, (row) => 255 - row, scope);
  expect([...full.offsets]).toEqual(new Array(256).fill(0));
  expect([...full.sourceRows]).toEqual(Array.from({ length: 256 }, (_, i) => 255 - i));
  scope.dispose();
});

it.each([-1, 256, 1.5, NaN])("rolls back invalid child tag %s", (tag) => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(() => buildUnionRouting(3, 256, (row) => (row === 2 ? tag : 0), scope)).toThrow(/tag/);
  expect(scope.budget.usedBytes).toBe(0);
  scope.dispose();
});

it("rolls back a throwing classifier and validates extents before callbacks", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  expect(() =>
    buildUnionRouting(
      3,
      2,
      () => {
        throw Error("classifier");
      },
      scope,
    ),
  ).toThrow(/classifier/);
  expect(scope.budget.usedBytes).toBe(0);
  for (const [length, children] of [
    [-1, 2],
    [0x100000000, 2],
    [1.5, 2],
    [2, 0],
    [2, 257],
  ])
    expect(() =>
      buildUnionRouting(
        length ?? 0,
        children ?? 0,
        () => {
          throw Error("unexpected");
        },
        scope,
      ),
    ).toThrow(/extent/);
  scope.dispose();
});

it("keeps routing linear in rows with bounded domain scratch", () => {
  for (const length of [1, 1024, 8192]) {
    const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
    let calls = 0;
    const routing = buildUnionRouting(
      length,
      7,
      (row) => {
        calls++;
        return row % 7;
      },
      scope,
    );
    expect(calls).toBe(length);
    expect(
      routing.tags.byteLength + routing.offsets.byteLength + routing.sourceRows.byteLength,
    ).toBe(9 * length);
    expect(routing.childStarts.byteLength).toBe(32);
    for (let row = 0; row < length; row++) {
      const tag = routing.tags[row] ?? 0;
      expect(
        routing.sourceRows[(routing.childStarts[tag] ?? 0) + (routing.offsets[row] ?? 0)],
      ).toBe(row);
    }
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it("rejects oversized routing admission without allocating or classifying", () => {
  const scope = new MemoryScope(new MemoryBudget(64 * 1024 * 1024));
  let calls = 0;
  expect(() =>
    buildUnionRouting(
      0xffffffff,
      256,
      () => {
        calls++;
        return 0;
      },
      scope,
    ),
  ).toThrow(/budget/);
  expect(calls).toBe(0);
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});

it("releases construction indices independently while retained tags and offsets stay valid", () => {
  const scope = new MemoryScope(new MemoryBudget(65536));
  const scratch = scope.childScope();
  const result = buildUnionRouting(1024, 4, (row) => row % 4, scope, scratch);
  const before = scope.budget.usedBytes;
  scratch.dispose();
  const { tags, offsets } = result;
  expect(scope.budget.usedBytes).toBe(5 * 1024 + 2 * 128);
  expect(before - scope.budget.usedBytes).toBe(4 * 1024 + 4 * 5 + 2 * 128 + 64 + 4 * 64);
  expect(tags[1023]).toBe(3);
  expect(offsets[1023]).toBe(255);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
