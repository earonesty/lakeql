import { describe, expect, it, vi } from "vitest";
import { batchFromColumns } from "./batch.js";
import { LakeqlError } from "./errors.js";
import {
  createVectorGroupByState,
  enforceVectorGroupByBudget,
  getOrCreateVectorGroupByValues,
  mergeVectorGroupByStates,
  restoreVectorGroupByState,
  snapshotVectorGroupByState,
  updateVectorGroupAggregateValue,
  updateVectorGroupByState,
  type VectorGroupByState,
} from "./vector-group-by.js";

function measuredBytes(state: VectorGroupByState): number {
  try {
    enforceVectorGroupByBudget(state, { maxMemoryBytes: 0 });
    return 0;
  } catch (error) {
    if (!(error instanceof LakeqlError)) throw error;
    expect(error.code).toBe("LAKEQL_BUDGET_EXCEEDED");
    return error.details.actual as number;
  }
}

function checkAccounting(state: VectorGroupByState): void {
  // A plain map exercises the original full-scan estimator as an independent oracle.
  const expected = measuredBytes({ ...state, groups: new Map(state.groups) });
  expect(measuredBytes(state)).toBe(expected);
  expect(() => enforceVectorGroupByBudget(state, { maxMemoryBytes: expected })).not.toThrow();
  if (expected > 0) {
    expect(() => enforceVectorGroupByBudget(state, { maxMemoryBytes: expected - 1 })).toThrow();
  }
}

describe("incremental group-by memory accounting", () => {
  it("matches full accounting across updates, budget changes, restore, and merge", () => {
    const spec = {
      rows: { op: "count" },
      distinct: { op: "count_distinct", column: "label" },
      mode: { op: "mode", column: "label" },
      median: { op: "median", column: "value" },
      quantile: { op: "quantile", column: "value", quantile: 0.5 },
    } as const;
    const state = createVectorGroupByState(["key"], spec);
    for (let i = 0; i < 30; i += 1) {
      updateVectorGroupByState(
        state,
        batchFromColumns({
          key: [i % 5, (i + 1) % 5],
          label: ["é😀", `label-${i}`],
          value: [i, i + 1],
        }),
        undefined,
        i % 3 === 0 ? {} : { budget: { maxMemoryBytes: 1_000_000 } },
      );
      checkAccounting(state);
    }
    const restored = restoreVectorGroupByState(
      ["key"],
      spec,
      JSON.parse(JSON.stringify(snapshotVectorGroupByState(state))),
      { budget: { maxMemoryBytes: 1_000_000 } },
    );
    checkAccounting(restored);
    mergeVectorGroupByStates(restored, state, { budget: { maxMemoryBytes: 1_000_000 } });
    checkAccounting(restored);
    const empty = createVectorGroupByState(["key"], spec);
    mergeVectorGroupByStates(empty, restored, { budget: { maxMemoryBytes: 1_000_000 } });
    checkAccounting(empty);
  });

  it("tracks removal, replacement, shared group entries, and failed aggregate updates", () => {
    const state = createVectorGroupByState(["key"], {
      labels: { op: "count_distinct", column: "label" },
    });
    const group = getOrCreateVectorGroupByValues(state, ["a"]);
    state.groups.set("alias", group);
    updateVectorGroupAggregateValue(group, "labels", "one", { budget: { maxMemoryBytes: 1_000 } });
    checkAccounting(state);
    expect(() =>
      updateVectorGroupAggregateValue(group, "labels", "too long", {
        budget: { maxMemoryBytes: 1 },
      }),
    ).toThrow();
    checkAccounting(state);
    state.groups.delete("alias");
    checkAccounting(state);
    const other = getOrCreateVectorGroupByValues(state, ["b"]);
    const firstKey = state.groups.keys().next().value as string;
    state.groups.set(firstKey, other);
    checkAccounting(state);
    state.groups.clear();
    updateVectorGroupAggregateValue(group, "labels", "detached");
    checkAccounting(state);
  });

  it("does not traverse all groups during high-cardinality budgeted updates", () => {
    const state = createVectorGroupByState(["key"], { rows: { op: "count" } });
    const iterator = vi.spyOn(state.groups, Symbol.iterator);
    const values = Array.from({ length: 10_000 }, (_, i) => i);
    updateVectorGroupByState(state, batchFromColumns({ key: values }), undefined, {
      budget: { maxMemoryBytes: 64 * 1024 * 1024 },
    });
    expect(state.groups.size).toBe(10_000);
    expect(iterator).not.toHaveBeenCalled();
    iterator.mockRestore();
    checkAccounting(state);
  });
});

it("does not rescan accumulated groups after restore or during repeated merges", () => {
  const spec = { rows: { op: "count" }, total: { op: "sum", column: "value" } } as const;
  const initial = createVectorGroupByState(["key"], spec);
  updateVectorGroupByState(
    initial,
    batchFromColumns({
      key: Array.from({ length: 1024 }, (_, i) => i),
      value: Array.from({ length: 1024 }, (_, i) => i),
    }),
  );
  const restored = restoreVectorGroupByState(
    ["key"],
    spec,
    JSON.parse(JSON.stringify(snapshotVectorGroupByState(initial))),
  );
  const traversal = vi.spyOn(restored.groups, Symbol.iterator);
  try {
    for (let i = 0; i < 32; i++) {
      const source = createVectorGroupByState(["key"], spec);
      updateVectorGroupByState(source, batchFromColumns({ key: [i, 1024 + i], value: [i, i] }));
      mergeVectorGroupByStates(restored, source, { budget: { maxMemoryBytes: 1024 * 1024 } });
      updateVectorGroupByState(restored, batchFromColumns({ key: [i], value: [1] }), undefined, {
        budget: { maxMemoryBytes: 1024 * 1024 },
      });
    }
    expect(traversal).not.toHaveBeenCalled();
  } finally {
    traversal.mockRestore();
  }
  checkAccounting(restored);
});
