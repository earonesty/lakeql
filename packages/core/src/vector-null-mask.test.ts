import { expect, it, vi } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import {
  predicateSelection,
  type Vector,
  vectorFromValues,
  vectorLength,
  vectorValue,
} from "./batch.js";
import { isNotNull, isNull } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";

it.each([
  vectorFromValues([1, null, 2]),
  vectorFromValues([true, null, false]),
  vectorFromValues(["long text", null, ""]),
  vectorFromValues([[1, null], null, []]),
  vectorFromValues([{ a: null }, null, { a: 2 }]),
  {
    type: "union",
    tags: Uint8Array.of(0, 1, 0),
    offsets: Uint32Array.of(0, 0, 1),
    children: [vectorFromValues(["a", "b"]), { type: "null", length: 1 }],
  } as Vector,
  {
    type: "dict",
    indices: Uint32Array.of(2, 0, 1),
    dictionary: vectorFromValues([null, "b", "a"]),
    valid: Uint8Array.of(0, 1, 1),
  } as Vector,
])("matches row null semantics for $type", (vector) => {
  const batch = { rowCount: vectorLength(vector), columns: { value: vector } };
  for (const [negated, predicate] of [
    [false, isNull("value")],
    [true, isNotNull("value")],
  ] as const) {
    const expected = Array.from({ length: batch.rowCount }, (_, row) =>
      Number((vectorValue(vector, row) === null) !== negated),
    );
    expect([...predicateSelection(batch, predicate)]).toEqual(expected);
  }
});

it("does not decode packed text at exact mask capacity, independent of text width", () => {
  const decode = vi.spyOn(TextDecoder.prototype, "decode");
  try {
    for (const width of [1, 1024 * 1024]) {
      const vector: Vector = {
        type: "utf8-view",
        data: new Uint8Array(width * 3).fill(97),
        fixedLength: width,
        length: 3,
        valid: Uint8Array.of(1, 0, 1),
      };
      const pool = new MemoryBudget(c.view + 3);
      const scope = new MemoryScope(pool);
      try {
        expect([
          ...predicateSelection(
            { rowCount: 3, columns: { value: vector } },
            isNull("value"),
            scope,
          ),
        ]).toEqual([0, 1, 0]);
        expect(pool.peakBytes).toBe(c.view + 3);
      } finally {
        scope.dispose();
      }
      expect(pool.usedBytes).toBe(0);
    }
    expect(decode).not.toHaveBeenCalled();
  } finally {
    decode.mockRestore();
  }
});

it.each([
  "list",
  "struct",
  "map",
] as const)("does not visit $0 payload when testing container nullness", (shape) => {
  const values = {
    type: "utf8" as const,
    get values(): string[] {
      throw new Error("payload was visited");
    },
  };
  const offsets = Int32Array.of(0, 1000000, 1000000);
  const valid = Uint8Array.of(1, 0);
  const list: Vector =
    shape === "list"
      ? { type: "list", offsets, child: values, valid }
      : shape === "map"
        ? { type: "map", offsets, keys: values, values, valid }
        : { type: "struct", fields: { value: values }, length: 2, valid };
  expect([
    ...predicateSelection({ rowCount: 2, columns: { value: list } }, isNotNull("value")),
  ]).toEqual([1, 0]);
});

it("follows deep dictionary indirection without recursive stack growth", () => {
  let vector: Vector = { type: "null", length: 1 };
  for (let i = 0; i < 20000; i++)
    vector = { type: "dict", indices: Uint32Array.of(0), dictionary: vector };
  expect([
    ...predicateSelection({ rowCount: 1, columns: { value: vector } }, isNull("value")),
  ]).toEqual([1]);
});

it.each([
  { type: "dict", indices: Uint32Array.of(2), dictionary: { type: "null", length: 1 } },
  { type: "union", tags: Uint8Array.of(3), offsets: Uint32Array.of(0), children: [] },
])("rejects invalid indirection for $type and cleans the output mask", (shape) => {
  const pool = new MemoryBudget(c.view + 1);
  const scope = new MemoryScope(pool);
  try {
    expect(() =>
      predicateSelection(
        { rowCount: 1, columns: { value: shape as Vector } },
        isNull("value"),
        scope,
      ),
    ).toThrow();
  } finally {
    scope.dispose();
  }
  expect(pool.usedBytes).toBe(0);
});

it("rejects a one-byte-short mask without disturbing a sibling reservation", () => {
  const pool = new MemoryBudget(c.view + 39);
  const sibling = pool.reserve(37, "sibling");
  const scope = new MemoryScope(pool);
  try {
    expect(() =>
      predicateSelection(
        { rowCount: 3, columns: { value: { type: "null", length: 3 } } },
        isNull("value"),
        scope,
      ),
    ).toThrow(expect.objectContaining({ code: "LAKEQL_BUDGET_EXCEEDED" }));
    expect(pool.usedBytes).toBe(37);
  } finally {
    scope.dispose();
    sibling.release();
  }
  expect(pool.usedBytes).toBe(0);
});

it.each([
  "dict",
  "union",
] as const)("rejects cyclic %s indirection without retaining a mask", (kind) => {
  const vector: Vector =
    kind === "dict"
      ? { type: "dict", indices: Uint32Array.of(0), dictionary: { type: "null", length: 1 } }
      : { type: "union", tags: Uint8Array.of(0), offsets: Uint32Array.of(0), children: [] };
  if (vector.type === "dict") vector.dictionary = vector;
  else if (vector.type === "union") vector.children.push(vector);
  const pool = new MemoryBudget(c.view + 1);
  const scope = new MemoryScope(pool);
  expect(() =>
    predicateSelection({ rowCount: 1, columns: { value: vector } }, isNull("value"), scope),
  ).toThrow(/Cyclic/);
  expect(pool.usedBytes).toBe(0);
  scope.dispose();
});

it("allows revisiting a vector at different rows when indirection terminates at null", () => {
  const vector: Extract<Vector, { type: "dict" }> = {
    type: "dict",
    indices: Uint32Array.of(1, 2, 2),
    dictionary: { type: "null", length: 3 },
    valid: Uint8Array.of(1, 1, 0),
  };
  vector.dictionary = vector;
  expect([
    ...predicateSelection({ rowCount: 3, columns: { value: vector } }, isNull("value")),
  ]).toEqual([1, 1, 1]);
});
