import { expect, it } from "vitest";
import { materializeBatchRows, type Vector } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { concatPackedDictionaries } from "./packed-gather.js";
import { concatBatches } from "./vector-sort.js";

const left: Vector = {
  type: "dict",
  indices: Uint32Array.of(0, 1),
  dictionary: {
    type: "dict",
    indices: Uint32Array.of(0, 1),
    dictionary: {
      type: "utf8-view",
      data: new TextEncoder().encode("xaé"),
      ranges: Uint32Array.of(0, 1, 1, 1, 2, 2),
      rowOffset: 1,
      length: 2,
      valid: Uint8Array.of(1, 0),
    },
  },
};
const right: Vector = {
  type: "dict",
  indices: Uint32Array.of(0, 1),
  dictionary: {
    type: "utf8-view",
    data: new TextEncoder().encode("中"),
    ranges: Uint32Array.of(0, 3, 3, 0),
    length: 2,
  },
};

it.each([40, 39])("copies nested packed text dictionaries at output cap %i", (cap) => {
  const scope = new MemoryScope(new MemoryBudget(cap));
  const batches = [left, right].map((value) => ({ rowCount: 2, columns: { value } }));
  try {
    if (cap === 39) expect(() => concatBatches(batches, scope)).toThrow("budget");
    else {
      const output = concatBatches(batches, scope);
      expect(output.columns.value?.type).toBe("utf8-view");
      expect(materializeBatchRows(output)).toEqual([
        { value: "a" },
        { value: null },
        { value: "中" },
        { value: "" },
      ]);
      expect(scope.budget.usedBytes).toBe(40);
    }
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it("combines binary arrays and packed fixed bytes without text conversion", () => {
  const vectors = [
    {
      type: "dict" as const,
      indices: Uint32Array.of(0, 0),
      dictionary: { type: "binary" as const, values: [Uint8Array.of(0, 255)] },
    },
    {
      type: "dict" as const,
      indices: Uint32Array.of(0),
      dictionary: {
        type: "binary-view" as const,
        data: Uint8Array.of(128, 42),
        fixedLength: 2,
        length: 1,
      },
    },
  ];
  const output = concatPackedDictionaries(vectors, undefined);
  expect(output?.data).toEqual(Uint8Array.of(0, 255, 0, 255, 128, 42));
  expect(output?.valid).toBeUndefined();
  const first = vectors[0];
  if (first === undefined || right.type !== "dict") throw new Error("Missing dictionary");
  expect(concatPackedDictionaries([first, right], undefined)).toBeUndefined();
});

it("encodes plain strings directly into mixed packed output and preserves lone surrogates via fallback", () => {
  const indices = Uint32Array.of(0, 1);
  const stringDictionary = { type: "utf8" as const, values: ["😀", ""] };
  const strings = { type: "dict" as const, indices, dictionary: stringDictionary };
  if (right.type !== "dict") throw new Error("Expected dictionary");
  const scope = new MemoryScope(new MemoryBudget(39)); // 4 ranges + 7 bytes, no validity.
  const output = concatPackedDictionaries([strings, right], undefined, scope);
  if (output === undefined) throw new Error("Expected packed output");
  expect(materializeBatchRows({ rowCount: 4, columns: { value: output } })).toEqual([
    { value: "😀" },
    { value: "" },
    { value: "中" },
    { value: "" },
  ]);
  expect(scope.budget.usedBytes).toBe(39);
  scope.dispose();
  const lone = { ...strings, dictionary: { ...stringDictionary, values: ["\ud800", ""] } };
  expect(concatPackedDictionaries([lone, right], undefined)).toBeUndefined();
  const combined = concatBatches([
    { rowCount: 2, columns: { value: lone } },
    { rowCount: 2, columns: { value: strings } },
  ]);
  expect(materializeBatchRows(combined)[0]?.value).toBe("\ud800");
});

it("preserves leading and embedded Unicode byte-order-mark characters in packed values", () => {
  const values = ["\ufeff", "\ufefftext", "a\ufeffb", "\ufeff\ufeff"];
  const vectors = values.map((value) => ({
    type: "dict" as const,
    indices: Uint32Array.of(0),
    dictionary: { type: "utf8" as const, values: [value] },
  }));
  const output = concatPackedDictionaries(vectors, undefined);
  if (output === undefined) throw new Error("Expected lossless packed text");
  expect(materializeBatchRows({ rowCount: values.length, columns: { value: output } })).toEqual(
    values.map((value) => ({ value })),
  );
});
