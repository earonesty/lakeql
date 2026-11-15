import { expect, it, vi } from "vitest";
import { type Batch, materializeBatchRows, type PackedByteVector, vectorValue } from "./batch.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { comparePackedTextPrefix, PackedTextOrderProbe } from "./packed-text-order.js";
import { vectorSortIndices, vectorTopKOwnedBatch } from "./vector-sort.js";

it("only decides orders consistent with decoded UTF16, including malformed suffixes", () => {
  const encoder = new TextEncoder();
  const inputs = [
    "",
    "a",
    "aa",
    "ab",
    "a😀",
    "aé",
    "b\ufeff",
    "éa",
    "😀",
    "\ue000",
    "\uffff",
    "\0z",
    "\x7f",
  ].map((text) => encoder.encode(text));
  inputs.push(
    Uint8Array.of(97, 0xc0, 0x80),
    Uint8Array.of(98, 0xed, 0xa0, 0x80),
    Uint8Array.of(0xf0, 0x9f),
  );
  const backing = new Uint8Array(2 + inputs.reduce((sum, bytes) => sum + bytes.length, 0));
  const data = backing.subarray(2);
  const ranges = new Uint32Array((inputs.length + 1) * 2);
  let offset = 0;
  for (const [i, bytes] of inputs.entries()) {
    ranges.set([offset, bytes.length], (i + 1) * 2);
    data.set(bytes, offset);
    offset += bytes.length;
  }
  const vector: PackedByteVector = {
    type: "utf8-view",
    data,
    ranges,
    rowOffset: 1,
    length: inputs.length,
  };
  const decoder = new TextDecoder("utf-8", { ignoreBOM: true });
  const decoded = inputs.map((bytes) => decoder.decode(bytes));
  let decisions = 0;
  let fallbacks = 0;
  for (let a = 0; a < inputs.length; a++)
    for (let b = 0; b < inputs.length; b++) {
      const direct = comparePackedTextPrefix(vector, a, b);
      if (direct === undefined) fallbacks++;
      else {
        decisions++;
        expect(direct).toBe(
          (decoded[a] as string) < (decoded[b] as string)
            ? -1
            : (decoded[a] as string) > (decoded[b] as string)
              ? 1
              : 0,
        );
      }
    }
  expect(decisions).toBeGreaterThan(0);
  expect(fallbacks).toBeGreaterThan(0);
  const expected = decoded
    .map((_, i) => i)
    .sort((a, b) =>
      (decoded[a] as string) < (decoded[b] as string)
        ? -1
        : (decoded[a] as string) > (decoded[b] as string)
          ? 1
          : a - b,
    );
  expect(
    vectorSortIndices({ rowCount: inputs.length, columns: { key: vector } }, [{ column: "key" }]),
  ).toEqual(expected);
});

it.each([
  [false, false],
  [true, false],
  [false, true],
  [true, true],
])("avoids decoded comparison workspace, dictionary=%s Unicode=%s", (dictionary, unicode) => {
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const data = scope.allocate(4 * 1024, "packed source", () => new Uint8Array(4 * 1024)).value;
  for (const [i, byte] of [122, 98, 109, 97].entries()) {
    data.fill(byte, i * 1024, (i + 1) * 1024);
    if (unicode) data.set([0xc3, 0xa9], i * 1024);
  }
  const packed: PackedByteVector = { type: "utf8-view", data, fixedLength: 1024, length: 4 };
  const batch: Batch = {
    rowCount: 4,
    columns: {
      key: dictionary
        ? { type: "dict", dictionary: packed, indices: Uint32Array.of(0, 1, 2, 3) }
        : packed,
    },
  };
  const input = scope.retainAllocations(batch);
  scope.dispose();
  const budget = pool.child(input.bytes + 2048);
  const pin = input.retainIn(budget);
  const decode = vi.spyOn(TextDecoder.prototype, "decode");
  let output: ReturnType<typeof vectorTopKOwnedBatch>;
  try {
    output = vectorTopKOwnedBatch(pin, [{ column: "key" }], { limit: 2 });
    expect(decode).not.toHaveBeenCalled();
  } finally {
    decode.mockRestore();
  }
  expect(materializeBatchRows(output.value)).toEqual([
    { key: unicode ? `é${"a".repeat(1022)}` : "a".repeat(1024) },
    { key: unicode ? `é${"b".repeat(1022)}` : "b".repeat(1024) },
  ]);
  output.release();
  pin.release();
  input.release();
  expect(pool.usedBytes).toBe(0);
});

it("falls back for outer/leaf nulls and preserves secondary keys and stable ties", () => {
  const packed: PackedByteVector = {
    type: "utf8-view",
    data: new TextEncoder().encode("é"),
    ranges: Uint32Array.of(999, 999, 0, 2),
    valid: Uint8Array.of(0, 1),
    length: 2,
  };
  const key = {
    type: "dict" as const,
    dictionary: packed,
    indices: Uint32Array.of(0, 1, 1, 999),
    valid: Uint8Array.of(1, 1, 1, 0),
  };
  expect(comparePackedTextPrefix(key, 0, 1)).toBeUndefined();
  expect(comparePackedTextPrefix(key, 3, 1)).toBeUndefined();
  expect(comparePackedTextPrefix(key, 1, 2)).toBe(0);
  const batch: Batch = {
    rowCount: 4,
    columns: { key, secondary: { type: "i32", values: Int32Array.of(9, 2, 1, 0) } },
  };
  expect(
    vectorSortIndices(batch, [{ column: "key", nulls: "last" }, { column: "secondary" }]),
  ).toEqual([2, 1, 3, 0]);
  expect(vectorValue(key, 0)).toBeNull();
});

it("matches decoder ordering across scalar boundaries and malformed UTF8", () => {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder("utf-8", { ignoreBOM: true });
  const boundaries = [
    0, 0x7f, 0x80, 0x7ff, 0x800, 0xd7ff, 0xe000, 0xfeff, 0xffff, 0x10000, 0x103ff, 0x10400, 0x1ffff,
    0xfffff, 0x100000, 0x10ffff,
  ];
  let state = 0x12345678;
  const random = () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return state >>> 0;
  };
  let decided = 0;
  const check = (a: Uint8Array, b: Uint8Array) => {
    const data = new Uint8Array(a.length + b.length);
    data.set(a);
    data.set(b, a.length);
    const vector: PackedByteVector = {
      type: "utf8-view",
      data,
      ranges: Uint32Array.of(0, a.length, a.length, b.length),
      length: 2,
    };
    const result = comparePackedTextPrefix(vector, 0, 1);
    if (result === undefined) return;
    const left = decoder.decode(a),
      right = decoder.decode(b);
    expect(result).toBe(left < right ? -1 : left > right ? 1 : 0);
    decided++;
  };
  for (let scalar = 0; scalar <= 0xffff; scalar++) {
    const other = boundaries[scalar % boundaries.length] as number;
    check(
      encoder.encode(String.fromCodePoint(scalar)),
      encoder.encode(String.fromCodePoint(other)),
    );
  }
  for (let i = 0; i < 4096; i++) {
    const a = String.fromCodePoint(0x10000 + (random() % 0x100000));
    const b = String.fromCodePoint(0x10000 + (random() % 0x100000));
    check(encoder.encode(`${a}a`), encoder.encode(`${b}b`));
    const prefix = encoder.encode(i % 2 ? "雪" : "");
    const left = Uint8Array.from({ length: random() % 8 }, () => random() & 255);
    const right = Uint8Array.from({ length: random() % 8 }, () => random() & 255);
    check(Uint8Array.of(...prefix, ...left), Uint8Array.of(...prefix, ...right));
  }
  expect(decided).toBeGreaterThan(69000);
});

it("bounds prefix scans and stops repeated unsuccessful probes per operation", () => {
  const data = new TextEncoder().encode(`${"x".repeat(32)}a${"x".repeat(32)}b`);
  const long: PackedByteVector = { type: "utf8-view", data, fixedLength: 33, length: 2 };
  const short: PackedByteVector = {
    type: "utf8-view",
    data: Uint8Array.of(97, 98),
    fixedLength: 1,
    length: 2,
  };
  expect(comparePackedTextPrefix(long, 0, 1)).toBeUndefined();
  const probe = new PackedTextOrderProbe();
  const nulls: PackedByteVector = { ...short, valid: Uint8Array.of(0, 0) };
  const numeric = { type: "i32" as const, values: Int32Array.of(1, 2) };
  const dictNulls = { type: "dict" as const, dictionary: nulls, indices: Uint32Array.of(0, 1) };
  for (let i = 0; i < 16; i++) {
    expect(probe.compare(nulls, 0, 1)).toBeUndefined();
    expect(probe.compare(dictNulls, 0, 1)).toBeUndefined();
    expect(probe.compare(numeric, 0, 1)).toBeUndefined();
  }
  expect(probe.compare(short, 0, 1)).toBe(-1);
  for (let i = 0; i < 7; i++) expect(probe.compare(long, 0, 1)).toBeUndefined();
  expect(probe.compare(short, 0, 1)).toBe(-1);
  for (let i = 0; i < 8; i++) expect(probe.compare(long, 0, 1)).toBeUndefined();
  expect(probe.compare(short, 0, 1)).toBeUndefined();
  expect(new PackedTextOrderProbe().compare(short, 0, 1)).toBe(-1);
});
