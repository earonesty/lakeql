import { afterEach, describe, expect, it, vi } from "vitest";
import { jsonItemBytes, jsonListBytes, jsonListBytesAfterAppend } from "./aggregate-memory.js";
import { batchFromColumns } from "./batch.js";
import type { QueryBudget } from "./query.js";
import {
  createVectorAggregateStates,
  mergeVectorAggregateStates,
  restoreVectorAggregateStates,
  snapshotVectorAggregateStates,
  updateVectorAggregateStates,
  updateVectorAggregateStateValue,
  type VectorAggregateState,
} from "./vector-aggregate.js";

const textEncoder = new TextEncoder();
const jsonBytes = (value: unknown) => textEncoder.encode(JSON.stringify(value)).byteLength;

// What each buffering aggregate charges a memory budget: its buffered list, serialised.
function serialisedBytes(state: VectorAggregateState | undefined): number {
  switch (state?.op) {
    case "count_distinct":
    case "approx_count_distinct":
      return jsonBytes([...state.values]);
    case "mode":
      return jsonBytes([...state.values.keys()]);
    case "median":
    case "quantile":
      return jsonBytes(state.values);
    default:
      throw new Error(`Not a buffering aggregate: ${state?.op}`);
  }
}

const spec = {
  labels: { op: "count_distinct", column: "label" },
  approxLabels: { op: "approx_count_distinct", column: "label" },
  commonLabel: { op: "mode", column: "label" },
  medianAmount: { op: "median", column: "amount" },
  p90: { op: "quantile", column: "amount", quantile: 0.9 },
} as const;
const aliases = Object.keys(spec) as (keyof typeof spec)[];

const labels = ["", "plain", 'quo"te', "back\\slash", "tab\tnew\nline", "\u0000\u001f\u007f"];
const unicode = ["é", "日本語", "😀", "\ud800", "a\u2028b"];
const amounts = [0, -0, 1 / 3, -1.5, 1e21, 123456789.125, Number.NaN];

function batch(offset: number, rows: number) {
  const all = [...labels, ...unicode];
  return batchFromColumns({
    label: Array.from(
      { length: rows },
      (_, index) => `${all[(offset + index) % all.length]}${index % 5}`,
    ),
    amount: Array.from(
      { length: rows },
      (_, index) => amounts[(offset + index) % amounts.length] ?? 0,
    ),
  });
}

describe("buffering aggregate memory", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("sizes items and lists exactly as JSON.stringify writes them", () => {
    for (const value of [...labels, ...unicode, ...amounts, Number.POSITIVE_INFINITY]) {
      expect(jsonItemBytes(value)).toBe(jsonBytes(value));
    }
    const values: (string | number)[] = [];
    let bytes = jsonListBytes(values);
    expect(bytes).toBe(2);
    for (const value of [...labels, ...unicode, ...amounts]) {
      bytes = jsonListBytesAfterAppend(bytes, values.length, jsonItemBytes(value));
      values.push(value);
      expect(bytes).toBe(jsonBytes(values));
      expect(jsonListBytes(values)).toBe(bytes);
    }
  });

  it("keeps each budgeted aggregate's size equal to its serialised list", () => {
    const budget: QueryBudget = { maxMemoryBytes: 1 << 30 };
    const options = { budget };
    const expectSerialisedSizes = (states: ReturnType<typeof createVectorAggregateStates>) => {
      for (const alias of aliases) {
        expect(states[alias]?.memoryBytes, alias).toBe(serialisedBytes(states[alias]));
      }
    };

    // Built with a budget, then fed batch by batch.
    const states = createVectorAggregateStates(spec, options);
    for (let offset = 0; offset < 40; offset += 7) {
      updateVectorAggregateStates(states, spec, batch(offset, 9), undefined, options);
      expectSerialisedSizes(states);
    }

    // Built without one (so never measured), merged into and then fed with one.
    const unbudgeted = createVectorAggregateStates(spec);
    updateVectorAggregateStates(unbudgeted, spec, batch(3, 50));
    mergeVectorAggregateStates(unbudgeted, states, options);
    expectSerialisedSizes(unbudgeted);
    updateVectorAggregateStates(unbudgeted, spec, batch(11, 20), undefined, options);
    expectSerialisedSizes(unbudgeted);

    // Restored from a snapshot, as in deployment fan-in.
    const restored = restoreVectorAggregateStates(
      JSON.parse(JSON.stringify(snapshotVectorAggregateStates(unbudgeted))),
    );
    updateVectorAggregateStates(restored, spec, batch(17, 13), undefined, options);
    expectSerialisedSizes(restored);
  });

  it("refuses a list exactly one byte over the budget", () => {
    const rows = batch(0, 60);
    for (const alias of aliases) {
      const one = { [alias]: spec[alias] };
      const measured = createVectorAggregateStates(one, { budget: { maxMemoryBytes: 1 << 30 } });
      updateVectorAggregateStates(measured, one, rows, undefined, {
        budget: { maxMemoryBytes: 1 << 30 },
      });
      const size = serialisedBytes(measured[alias]);

      const fits = { budget: { maxMemoryBytes: size } };
      updateVectorAggregateStates(
        createVectorAggregateStates(one, fits),
        one,
        rows,
        undefined,
        fits,
      );

      const over = { budget: { maxMemoryBytes: size - 1 } };
      expect(() =>
        updateVectorAggregateStates(
          createVectorAggregateStates(one, over),
          one,
          rows,
          undefined,
          over,
        ),
      ).toThrowError(
        expect.objectContaining({
          code: "LAKEQL_BUDGET_EXCEEDED",
          details: expect.objectContaining({ metric: "operator memory bytes", actual: size }),
        }),
      );
    }
  });

  it.each([
    "median",
    "quantile",
    "mode",
    "count_distinct",
    "approx_count_distinct",
  ] as const)("%s measures restored storage once per accounting epoch", (op) => {
    const spec = { value: { op, quantile: 0.5 } };
    const initial = createVectorAggregateStates(spec);
    const source = initial.value;
    if (!source) throw new Error("Missing source accumulator");
    for (let value = 0; value < 1024; value++) updateVectorAggregateStateValue(source, value);
    const states = restoreVectorAggregateStates(snapshotVectorAggregateStates(initial));
    const state = states.value;
    if (!state || !("values" in state)) throw new Error("Missing buffered accumulator");
    const traversal =
      state.op === "mode"
        ? vi.spyOn(state.values, "keys")
        : vi.spyOn(state.values, Symbol.iterator);
    const options = { budget: { maxMemoryBytes: 1024 * 1024 } };
    try {
      for (let value = 1024; value < 1536; value++)
        updateVectorAggregateStateValue(state, value, options);
      expect(traversal).toHaveBeenCalledTimes(1);
      // An unmeasured append invalidates the cached size. Re-enabling the
      // byte limit must measure once again, rather than trust a stale total.
      updateVectorAggregateStateValue(state, 1536);
      for (let value = 1537; value < 2048; value++)
        updateVectorAggregateStateValue(state, value, options);
      expect(traversal).toHaveBeenCalledTimes(2);
    } finally {
      traversal.mockRestore();
    }
    expect(state.memoryBytes).toBe(serialisedBytes(state));
  });
});

it.each([
  "median",
  "quantile",
  "mode",
  "count_distinct",
  "approx_count_distinct",
] as const)("rejects %s growth before mutation and permits a later admitted update", (op) => {
  for (const metric of ["rows", "bytes"] as const) {
    const states = createVectorAggregateStates({ value: { op, column: "value", quantile: 0.5 } });
    const state = states.value;
    if (!state) throw Error("Missing accumulator");
    updateVectorAggregateStateValue(state, 7, { budget: { maxMemoryBytes: 10000 } });
    const before = snapshotVectorAggregateStates(states);
    const bytes = serialisedBytes(state);
    const budget = metric === "rows" ? { maxBufferedRows: 1 } : { maxMemoryBytes: bytes + 1 };
    expect(() => updateVectorAggregateStateValue(state, 8, { budget })).toThrow(/budget/);
    expect(snapshotVectorAggregateStates(states)).toEqual(before);
    expect(serialisedBytes(state)).toBe(bytes);
    updateVectorAggregateStateValue(state, 8, {
      budget: { maxMemoryBytes: 10000, maxBufferedRows: 2 },
    });
    expect(snapshotVectorAggregateStates(states)).not.toEqual(before);
  }
});

it.each([
  "median",
  "quantile",
] as const)("%s merge keeps only the admitted prefix and leaves its source unchanged", (op) => {
  const spec = { value: { op, column: "value", quantile: 0.5 } };
  const target = createVectorAggregateStates(spec);
  const source = createVectorAggregateStates(spec);
  const targetState = target.value;
  const sourceState = source.value;
  if (!targetState || !sourceState) throw Error("Missing accumulator");
  const values = op === "median" ? (["a", "é", "😀"] as const) : ([1, 2, 3] as const);
  updateVectorAggregateStateValue(targetState, values[0]);
  updateVectorAggregateStateValue(sourceState, values[1]);
  updateVectorAggregateStateValue(sourceState, values[2]);
  const sourceBefore = snapshotVectorAggregateStates(source);
  const expected = createVectorAggregateStates(spec);
  const expectedState = expected.value;
  if (!expectedState) throw Error("Missing accumulator");
  updateVectorAggregateStateValue(expectedState, values[0]);
  updateVectorAggregateStateValue(expectedState, values[1]);
  const byteLimit = jsonBytes(values.slice(0, 2));
  expect(() =>
    mergeVectorAggregateStates(target, source, {
      budget: { maxMemoryBytes: byteLimit },
    }),
  ).toThrow(/budget/);
  expect(snapshotVectorAggregateStates(target)).toEqual(snapshotVectorAggregateStates(expected));
  expect(snapshotVectorAggregateStates(source)).toEqual(sourceBefore);
  expect(serialisedBytes(targetState)).toBe(byteLimit);
  updateVectorAggregateStateValue(targetState, values[2], {
    budget: { maxMemoryBytes: jsonBytes(values) },
  });
  expect(serialisedBytes(targetState)).toBe(jsonBytes(values));
});
