import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import type { NestedRowFragment } from "./nested-column-rows.js";
import { indexNestedFragments, windowNestedFragments } from "./nested-fragment-window.js";

// Index-only metadata fixture; this helper neither decodes nor reads payloads.
function fragment(continuation = false): NestedRowFragment {
  return {
    page: {
      rows: {
        eventOffsets: Uint32Array.of(0, 1),
        valueOffsets: Uint32Array.of(0, 1),
        continuesPrevious: continuation,
      },
    } as NestedRowFragment["page"],
    eventStart: 0,
    eventEnd: 1,
    valueStart: 0,
    valueEnd: 1,
  };
}

it.each([
  128, 1024, 8192,
])("indexes %i fragmented rows once for linear total window visits", (count) => {
  let visits = 0;
  const fragments = Array.from({ length: count }, () => {
    const value = fragment();
    const rows = value.page.rows;
    Object.defineProperty(value.page, "rows", {
      get() {
        visits++;
        return rows;
      },
    });
    return value;
  });
  const budget = new MemoryBudget(4 * 1024 * 1024);
  const scope = new MemoryScope(budget);
  try {
    const index = indexNestedFragments(fragments, scope);
    expect(visits).toBe(count);
    for (let row = 0; row < count; row++) {
      const output = scope.childScope();
      try {
        const selected = windowNestedFragments(index, row, 1, output);
        expect(selected).toHaveLength(1);
        expect(selected[0]?.page).toBe(fragments[row]?.page);
      } finally {
        output.dispose();
      }
    }
    // One construction visit and at most two lookup visits per fragment.
    expect(visits).toBeLessThanOrEqual(3 * count);
  } finally {
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it("includes arbitrarily many continuation-only pages without selecting the following row", () => {
  const fragments = [fragment(), ...Array.from({ length: 7 }, () => fragment(true)), fragment()];
  const scope = new MemoryScope(new MemoryBudget(65536));
  try {
    const index = indexNestedFragments(fragments, scope);
    expect(Array.from(index.rowEnds)).toEqual([1, 1, 1, 1, 1, 1, 1, 1, 2]);
    expect(windowNestedFragments(index, 0, 1, scope).map((f) => f.page)).toEqual(
      fragments.slice(0, 8).map((f) => f.page),
    );
    expect(windowNestedFragments(index, 1, 1, scope).map((f) => f.page)).toEqual([
      fragments[8]?.page,
    ]);
    for (const [start, count] of [
      [-1, 1],
      [0, 0],
      [0, NaN],
      [2, 1],
      [Number.MAX_SAFE_INTEGER, 1],
    ])
      expect(() => windowNestedFragments(index, start as number, count as number, scope)).toThrow(
        /window/,
      );
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it("admits index storage atomically and rejects inconsistent event/value boundaries", () => {
  const fragments = [fragment(), fragment()];
  const measured = new MemoryScope(new MemoryBudget(65536));
  indexNestedFragments(fragments, measured);
  const peak = measured.budget.peakBytes;
  measured.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  indexNestedFragments(fragments, exact);
  exact.dispose();
  const denied = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => indexNestedFragments(fragments, denied)).toThrow(/budget/);
  expect(denied.budget.usedBytes).toBe(0);
  denied.dispose();
  for (const bad of [
    fragment(true),
    { ...fragment(), valueEnd: 0 },
    { ...fragment(), eventEnd: 2 },
  ]) {
    const scope = new MemoryScope(new MemoryBudget(65536));
    expect(() => indexNestedFragments([bad], scope)).toThrow(/window/);
    expect(scope.budget.usedBytes).toBe(0);
    scope.dispose();
  }
});
