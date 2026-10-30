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

  it("serialises each value once, not the whole list per value", () => {
    const rows = 50_000;
    const large = batchFromColumns({
      label: Array.from({ length: rows }, (_, index) => `asset-${index % 30_000}`),
      amount: Array.from({ length: rows }, (_, index) => index / 7),
    });
    const options = { budget: { maxMemoryBytes: 1 << 30 } };
    const states = createVectorAggregateStates(spec, options);
    let serialised = 0;
    const stringify = JSON.stringify;
    vi.spyOn(JSON, "stringify").mockImplementation((...args: Parameters<typeof stringify>) => {
      const json = stringify(...args);
      serialised += json?.length ?? 0;
      return json;
    });

    updateVectorAggregateStates(states, spec, large, undefined, options);

    const listBytes = aliases.reduce(
      (total, alias) => total + (states[alias]?.memoryBytes ?? 0),
      0,
    );
    // Each value once, plus its key form; re-serialising per value would be about rows / 2 times this.
    expect(serialised).toBeLessThan(listBytes * 2);
  });
});
