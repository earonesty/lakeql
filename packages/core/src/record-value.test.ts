import { expect, it } from "vitest";
import { batchFromColumns, materializeBatchRows } from "./batch.js";
import { encodeJsonLine } from "./evaluator.js";
import { setRecordValue } from "./record-value.js";

it("preserves prototype-named map entries through row and JSON materialization", () => {
  const input = new Map<string, unknown>([
    ["__proto__", 7],
    ["constructor", 8],
    ["toString", null],
    ["ordinary", 9],
  ]);
  const [row] = materializeBatchRows(batchFromColumns({ attrs: [input] }));
  if (row === undefined) throw new Error("Missing materialized row");
  const attrs = row.attrs as Record<string, unknown>;
  expect(Object.getPrototypeOf(attrs)).toBe(Object.prototype);
  expect(Object.entries(attrs)).toEqual([...input]);
  expect(JSON.parse(new TextDecoder().decode(encodeJsonLine(row)))).toEqual({
    attrs: Object.fromEntries(input),
  });
});

it("writes and replaces prototype-named values as ordinary own data properties", () => {
  const record = {};
  setRecordValue(record, "__proto__", { marker: true });
  setRecordValue(record, "__proto__", null);
  expect(Object.getPrototypeOf(record)).toBe(Object.prototype);
  expect(Object.getOwnPropertyDescriptor(record, "__proto__")).toEqual({
    value: null,
    enumerable: true,
    configurable: true,
    writable: true,
  });
});
