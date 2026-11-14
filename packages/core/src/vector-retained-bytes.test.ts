import { expect, it } from "vitest";
import type { Vector } from "./batch.js";
import { retainedBatchBytes, retainedVectorBytes } from "./vector-retained-bytes.js";

it("counts a short view's complete backing allocation", () => {
  const backing = new Uint8Array(64 * 1024 * 1024);
  expect(retainedVectorBytes({ type: "u8", values: backing.subarray(0, 1) })).toBe(
    backing.byteLength,
  );
});

it("deduplicates backing buffers across nested vectors, validity, dictionaries, and columns", () => {
  const backing = new ArrayBuffer(128);
  const binary: Vector = {
    type: "binary",
    values: [new Uint8Array(backing, 0, 2), new Uint8Array(backing, 8, 2)],
  };
  const dict: Vector = {
    type: "dict",
    indices: new Uint32Array(backing, 16, 2),
    dictionary: binary,
    valid: new Uint8Array(backing, 32, 2),
  };
  const nested: Vector = {
    type: "map",
    offsets: new Int32Array(backing, 40, 3),
    keys: dict,
    values: { type: "list", offsets: new Int32Array(backing, 56, 3), child: binary },
  };
  const struct: Vector = { type: "struct", length: 2, fields: { nested, dict } };
  expect(retainedBatchBytes({ rowCount: 2, columns: { a: struct, b: dict, c: binary } })).toBe(128);
});

it("counts distinct backing buffers and conservative UTF-16 string payload", () => {
  expect(
    retainedBatchBytes({
      rowCount: 1,
      columns: {
        text: { type: "utf8", values: ["😀a"] },
        value: { type: "f64", values: new Float64Array(1), valid: new Uint8Array(1) },
        absent: { type: "null", length: 1 },
      },
    }),
  ).toBe(15);
});
