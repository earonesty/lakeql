import { isTimestampValue, MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it } from "vitest";
import { decodeBsonValue } from "./bson-values.js";

const i32 = (value: number) => {
  const out = Buffer.alloc(4);
  out.writeInt32LE(value);
  return out;
};
const i64 = (value: bigint) => {
  const out = Buffer.alloc(8);
  out.writeBigInt64LE(value);
  return out;
};
const element = (type: number, payload: Uint8Array, key = "a") =>
  Buffer.concat([Buffer.from([type]), Buffer.from(`${key}\0`), payload]);
const document = (...elements: Uint8Array[]) => {
  const body = Buffer.concat(elements);
  return Buffer.concat([i32(body.length + 5), body, Buffer.from([0])]);
};
const string = (value: string) => {
  const text = Buffer.from(value);
  return Buffer.concat([i32(text.length + 1), text, Buffer.from([0])]);
};

it("decodes the documented real-fixture BSON spellings and preserves null fields", () => {
  for (const [hex, expected] of [
    ["0c0000001061000100000000", new Map([["a", 1]])],
    [
      "0f000000106100010000000a620000",
      new Map<string, unknown>([
        ["a", 1],
        ["b", null],
      ]),
    ],
  ] as const) {
    const scope = new MemoryScope(new MemoryBudget(4096));
    expect(decodeBsonValue(Buffer.from(hex, "hex"), scope)).toEqual(expected);
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
  const scope = new MemoryScope(new MemoryBudget(4096));
  expect(
    decodeBsonValue(document(element(16, i32(1)), element(10, Buffer.alloc(0), "b")), scope),
  ).toEqual(
    new Map<string, unknown>([
      ["a", 1],
      ["b", null],
    ]),
  );
  scope.dispose();
});

it("retains BSON-specific scalar type information without executing or rounding values", () => {
  const decimal = Uint8Array.from({ length: 16 }, (_, index) => index * 17);
  const oid = new Uint8Array(12).fill(0xff);
  const scoped = Buffer.concat([i32(14), string(""), document()]);
  const cases: [number, Uint8Array, unknown][] = [
    [2, string("é😀\0"), "é😀\0"],
    [3, document(), new Map()],
    [4, document(element(10, Buffer.alloc(0), "0")), [null]],
    [
      5,
      Buffer.concat([i32(3), Buffer.from([128, 1, 2, 3])]),
      new Map<string, unknown>([
        ["$bson", "binary"],
        ["subtype", 128],
        ["bytes", new Uint8Array([1, 2, 3])],
      ]),
    ],
    [
      5,
      Buffer.concat([i32(7), Buffer.from([2]), i32(3), Buffer.from([1, 2, 3])]),
      new Map<string, unknown>([
        ["$bson", "binary"],
        ["subtype", 2],
        ["bytes", new Uint8Array([1, 2, 3])],
      ]),
    ],
    [6, Buffer.alloc(0), new Map([["$bson", "undefined"]])],
    [
      7,
      oid,
      new Map<string, unknown>([
        ["$bson", "objectId"],
        ["bytes", oid],
      ]),
    ],
    [8, Buffer.from([1]), true],
    [10, Buffer.alloc(0), null],
    [
      11,
      Buffer.from("x\0im\0"),
      new Map([
        ["$bson", "regex"],
        ["pattern", "x"],
        ["options", "im"],
      ]),
    ],
    [
      12,
      Buffer.concat([string("db"), oid]),
      new Map<string, unknown>([
        ["$bson", "dbPointer"],
        ["namespace", "db"],
        ["id", oid],
      ]),
    ],
    [
      13,
      string("throw Error()"),
      new Map([
        ["$bson", "code"],
        ["value", "throw Error()"],
      ]),
    ],
    [
      14,
      string("name"),
      new Map([
        ["$bson", "symbol"],
        ["value", "name"],
      ]),
    ],
    [
      15,
      scoped,
      new Map<string, unknown>([
        ["$bson", "codeWithScope"],
        ["code", ""],
        ["scope", new Map()],
      ]),
    ],
    [16, i32(-2147483648), -2147483648],
    [
      17,
      Buffer.concat([i32(-1), i32(-1)]),
      new Map<string, unknown>([
        ["$bson", "timestamp"],
        ["increment", 4294967295],
        ["seconds", 4294967295],
      ]),
    ],
    [18, i64(-9223372036854775808n), -9223372036854775808n],
    [
      19,
      decimal,
      new Map<string, unknown>([
        ["$bson", "decimal128"],
        ["bytes", decimal],
      ]),
    ],
    [127, Buffer.alloc(0), new Map([["$bson", "maxKey"]])],
    [255, Buffer.alloc(0), new Map([["$bson", "minKey"]])],
  ];
  for (const [type, bytes, expected] of cases) {
    const scope = new MemoryScope(new MemoryBudget(65536));
    const input = document(element(type, bytes));
    const result = decodeBsonValue(input, scope);
    input.fill(0);
    expect(result.get("a")).toEqual(expected);
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it("preserves negative zero and exact int64 UTC milliseconds", () => {
  const zero = Buffer.alloc(8);
  zero.writeDoubleLE(-0);
  const scope = new MemoryScope(new MemoryBudget(4096));
  const result = decodeBsonValue(
    document(element(1, zero), element(9, i64(-9223372036854775808n), "date")),
    scope,
  );
  expect(Object.is(result.get("a"), -0)).toBe(true);
  const date = result.get("date");
  expect(isTimestampValue(date)).toBe(true);
  if (isTimestampValue(date)) expect(date.epochNanoseconds).toBe(-9223372036854775808000000n);
  scope.dispose();
});

it("rejects duplicate keys and invalid UTF-8 while releasing partially built values", () => {
  for (const input of [
    document(element(16, i32(1)), element(16, i32(2))),
    document(Buffer.from([2, 97, 0, 2, 0, 0, 0, 255, 0])),
    document(Buffer.from([10, 255, 0])),
    document(element(11, Buffer.from("x\0mi\0"))),
    document(element(11, Buffer.from("x\0ii\0"))),
    document(element(11, Buffer.from("x\0g\0"))),
  ]) {
    const scope = new MemoryScope(new MemoryBudget(65536));
    expect(() => decodeBsonValue(input, scope)).toThrow(/Invalid BSON/);
    expect(scope.budget.usedBytes).toBe(0);
    scope.dispose();
  }
});

it("admits construction before allocation and retains output only after success", () => {
  const input = document(
    element(
      3,
      document(
        element(2, string("雪".repeat(100))),
        element(4, document(element(18, i64(123n), "0")), "list"),
      ),
      "__proto__",
    ),
  );
  const probe = new MemoryScope(new MemoryBudget(65536));
  const expected = decodeBsonValue(input, probe);
  const peak = probe.budget.peakBytes;
  expect(expected.has("__proto__")).toBe(true);
  expect(probe.budget.usedBytes).toBeGreaterThan(0);
  probe.dispose();
  for (const cap of [peak, peak - 1]) {
    const scope = new MemoryScope(new MemoryBudget(cap));
    if (cap === peak) expect(decodeBsonValue(input, scope)).toEqual(expected);
    else {
      expect(() => decodeBsonValue(input, scope)).toThrow(/budget/i);
      expect(scope.budget.usedBytes).toBe(0);
    }
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it("constructs deeply nested documents without a recursive JS call stack", () => {
  const depth = 2000;
  const bytes = new Uint8Array(5 + depth * 8);
  const view = new DataView(bytes.buffer);
  for (let level = 0; level <= depth; level++) {
    const offset = level * 7;
    view.setInt32(offset, bytes.length - level * 8, true);
    if (level < depth) {
      bytes[offset + 4] = 3;
      bytes[offset + 5] = 97;
    }
  }
  const scope = new MemoryScope(new MemoryBudget(8 * 1024 * 1024));
  let value = decodeBsonValue(bytes, scope);
  for (let level = 0; level < depth; level++) {
    const child = value.get("a");
    if (!(child instanceof Map)) throw Error("Missing nested BSON document");
    value = child;
  }
  expect(value.size).toBe(0);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});
