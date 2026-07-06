import { describe, expect, it } from "vitest";
import {
  addDistinctSortedStringRun,
  addDistinctStringValues,
  addDistinctValue,
  addDistinctValues,
  cloneDistinctAggregateState,
  createDistinctAggregateState,
  distinctKey,
  distinctMemoryBytes,
  distinctSnapshotValues,
  distinctValueCount,
  enforceDistinctStateBudget,
  mergeDistinctSortedValues,
} from "./vector-aggregate-distinct.js";

describe("vector aggregate distinct state", () => {
  it("tracks distinct keys across set and sorted-run paths", () => {
    const state = createDistinctAggregateState("count_distinct");
    expect(distinctKey(1)).toBe("number:1");
    addDistinctValue(state, "number:1");
    addDistinctValue(state, "number:1");
    addDistinctValues(state, ["number:2", "number:3"]);
    addDistinctStringValues(state, ["b", "a", "a"]);
    expect(distinctValueCount(state)).toBe(5);
    expect(distinctSnapshotValues(state)).toEqual([
      "number:1",
      "number:2",
      "number:3",
      "string:a",
      "string:b",
    ]);

    mergeDistinctSortedValues(
      state,
      Array.from({ length: 1024 }, (_, index) => `string:x${index}`),
    );
    mergeDistinctSortedValues(
      state,
      Array.from({ length: 1024 }, (_, index) => `string:y${index}`),
    );
    expect(distinctValueCount(state)).toBe(2053);
    expect(distinctSnapshotValues(state)[0]).toBe("number:1");

    const clone = cloneDistinctAggregateState(state);
    addDistinctValue(clone, "string:zzz");
    expect(distinctValueCount(clone)).toBe(distinctValueCount(state) + 1);
  });

  it("covers sorted-run boundary paths", () => {
    const lowCardinality = createDistinctAggregateState("count_distinct");
    addDistinctSortedStringRun(
      lowCardinality,
      Array.from({ length: 1024 }, () => "same"),
    );
    expect(distinctSnapshotValues(lowCardinality)).toEqual(["string:same"]);

    const shortMerge = createDistinctAggregateState("count_distinct");
    mergeDistinctSortedValues(shortMerge, []);
    expect(distinctValueCount(shortMerge)).toBe(0);
    mergeDistinctSortedValues(shortMerge, ["number:2", "number:1"]);
    expect(distinctSnapshotValues(shortMerge)).toEqual(["number:1", "number:2"]);

    const withSortedValues = createDistinctAggregateState("count_distinct");
    withSortedValues.sortedValues = ["number:0"];
    mergeDistinctSortedValues(
      withSortedValues,
      Array.from({ length: 1024 }, (_, index) => `number:${index + 1}`),
    );
    expect(distinctValueCount(withSortedValues)).toBe(1025);
    expect(cloneDistinctAggregateState(withSortedValues).sortedRuns?.[0]).toEqual(["number:0"]);
  });

  it("uses bounded paths and enforces row and memory budgets", () => {
    const state = createDistinctAggregateState("approx_count_distinct");
    addDistinctStringValues(state, ["a", "b"], { maxBufferedRows: 10 });
    addDistinctSortedStringRun(state, ["b", "c"], { maxBufferedRows: 10 });
    mergeDistinctSortedValues(state, ["string:c", "string:d"], { maxBufferedRows: 10 });
    expect(distinctValueCount(state)).toBe(4);
    expect(distinctMemoryBytes(new Set(["string:a"]))).toBe(0);
    expect(distinctMemoryBytes(new Set(["string:a"]), { maxMemoryBytes: 1000 })).toBeGreaterThan(0);

    expect(() => enforceDistinctStateBudget(state, { maxBufferedRows: 1 })).toThrow(
      "buffered rows budget",
    );
    expect(() => addDistinctValue(state, "string:e", { maxMemoryBytes: 1 })).toThrow(
      "operator memory bytes budget",
    );
  });
});
