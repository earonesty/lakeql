import { deserializeTCompactProtocol } from "hyparquet/src/thrift.js";
import { expect, it } from "vitest";
import { PAGE_HEADER_PROJECTION, readCompactHeader } from "./compact-header.js";

function reader(bytes: number[]) {
  return { view: new DataView(Uint8Array.from(bytes).buffer), offset: 0 };
}
it("matches valid compact scalar, binary, list and nested fields", () => {
  const bytes = [0x15, 3, 0x16, 0xac, 2, 0x18, 3, 65, 66, 67, 0x19, 0x25, 2, 4, 0x1c, 0x11, 0, 0];
  const actual = reader(bytes);
  const expected = reader(bytes);
  expect(readCompactHeader(actual)).toEqual(deserializeTCompactProtocol(expected));
  expect(actual.offset).toBe(expected.offset);
});
it("rejects truncated and oversized allocations before constructing lists or binary views", () => {
  for (const bytes of [
    [0x19, 0xf5, 0xff, 0xff, 0xff, 0xff, 7],
    [0x18, 0xff, 0xff, 0xff, 0xff, 7],
    [0x15, 0x80, 0x80, 0x80, 0x80, 0x10],
    [0x16, ...Array(10).fill(0x80)],
    [0x15, 2],
    [0x1c, 0],
    [0x19, 0x1c],
  ])
    expect(() => readCompactHeader(reader(bytes))).toThrow();
  // Underlying backing is larger than the permitted view: it must not supply
  // bytes outside the header probe.
  const backing = Uint8Array.of(0x18, 4, 1, 2, 3, 4, 0);
  expect(() => readCompactHeader({ view: new DataView(backing.buffer, 0, 3), offset: 0 })).toThrow(
    /Truncated/,
  );
});
it("bounds nested parser stack use", () => {
  expect(() =>
    readCompactHeader(reader([...Array(130).fill(0x1c), ...Array(131).fill(0)])),
  ).toThrow(/nesting/);
});

it("preserves signed 32-bit boundaries with bounded unsigned varints", () => {
  expect(readCompactHeader(reader([0x15, 0xff, 0xff, 0xff, 0xff, 0x0f, 0])).field_1).toBe(
    -2147483648,
  );
  expect(readCompactHeader(reader([0x15, 0xfe, 0xff, 0xff, 0xff, 0x0f, 0])).field_1).toBe(
    2147483647,
  );
});

it("projects nested fields while validating skipped lists and binary payloads", () => {
  const bytes = [0x15, 2, 0x1c, 0x15, 4, 0x18, 3, 65, 66, 67, 0, 0x19, 0x25, 6, 8, 0];
  const input = reader(bytes);
  expect(readCompactHeader(input, { 1: true, 2: { 1: true } })).toEqual({
    field_1: 1,
    field_2: { field_1: 2 },
  });
  expect(input.offset).toBe(bytes.length);
  expect(() => readCompactHeader(reader([0x19, 0xf5, 0xff, 0xff, 0xff, 0xff, 7]), {})).toThrow();
  expect(() => readCompactHeader(reader([0x18, 4, 1]), {})).toThrow();
});

it("rejects incompatible projected wire types before reading their payload", () => {
  for (const bytes of [[0x19], [0x18], [0x55], [0x8c, 0x75]]) {
    const input = reader(bytes);
    expect(() => readCompactHeader(input, PAGE_HEADER_PROJECTION)).toThrow(
      /Unexpected compact type/,
    );
    expect(input.offset).toBe(bytes.length);
  }
  expect(readCompactHeader(reader([0x19, 0x15, 2, 0]), { 1: true })).toEqual({ field_1: [1] });
});

it("projects fields within struct lists without retaining skipped payloads", () => {
  const bytes = [0x19, 0x2c, 0x18, 2, 1, 2, 0x15, 6, 0, 0x18, 1, 3, 0x15, 8, 0, 0];
  const input = reader(bytes);
  const result = readCompactHeader(input, { 1: { list: { 1: "binary" } } });
  expect(result).toEqual({
    field_1: [{ field_1: Uint8Array.of(1, 2) }, { field_1: Uint8Array.of(3) }],
  });
  expect(input.offset).toBe(bytes.length);
  const items = result.field_1 as { field_1: Uint8Array }[];
  expect(items[0]?.field_1.buffer).toBe(input.view.buffer);
  for (const malformed of [[0x19, 0x15, 2, 0], [0x1c], [0x19, 0x1c, 0x15]]) {
    expect(() => readCompactHeader(reader(malformed), { 1: { list: { 1: "binary" } } })).toThrow(
      /Unexpected compact/,
    );
  }
});

it("visits projected list entries without retaining the list and skips unselected structs", () => {
  const input = reader([0x19, 0x3c, 0x15, 2, 0, 0x15, 4, 0, 0x15, 6, 0, 0]);
  const visited: unknown[] = [];
  expect(
    readCompactHeader(input, {
      1: {
        list: (index) => (index === 1 ? false : { 1: "i32" }),
        visit: (value, index) => visited.push([index, value]),
      },
    }),
  ).toEqual({ field_1: 0 });
  expect(visited).toEqual([
    [0, { field_1: 1 }],
    [1, 0],
    [2, { field_1: 3 }],
  ]);
  expect(input.offset).toBe(input.view.byteLength);
  const interrupted = reader([0x19, 0x2c, 0, 0, 0]);
  expect(() =>
    readCompactHeader(interrupted, {
      1: {
        list: {},
        visit: () => {
          throw new Error("stop visitor");
        },
      },
    }),
  ).toThrow("stop visitor");
  expect(interrupted.offset).toBe(3);
});

it("preflights structural allocation counts without retaining decoded fields", async () => {
  const { inspectCompactStructure } = await import("./compact-header.js");
  const input = reader([0x19, 0x2c, 0x18, 2, 1, 2, 0, 0x18, 1, 3, 0, 0]);
  expect(inspectCompactStructure(input)).toEqual({
    structs: 3,
    fields: 3,
    lists: 1,
    listItems: 2,
    binaryValues: 2,
    binaryBytes: 3,
    maxDepth: 2,
  });
  expect(input.offset).toBe(input.view.byteLength);
  for (const bytes of [[0x19, 0xfc, 127], [0x18, 127], [0x1c]]) {
    expect(() => inspectCompactStructure(reader(bytes))).toThrow(/Truncated/);
  }
});

it("locates complete struct-list entries without decoding their fields", () => {
  const bytes = [0x19, 0x2c, 0x18, 2, 1, 2, 0, 0x15, 4, 0, 0];
  const ranges: [number, number, number][] = [];
  const input = reader(bytes);
  expect(
    readCompactHeader(input, {
      1: { list: false, visitRange: (start, end, index) => ranges.push([start, end, index]) },
    }),
  ).toEqual({ field_1: 0 });
  expect(ranges).toEqual([
    [2, 7, 0],
    [7, 10, 1],
  ]);
  for (const [start, end] of ranges) {
    const view = new DataView(input.view.buffer, start, end - start);
    const actual = { view, offset: 0 };
    const expected = { view, offset: 0 };
    expect(readCompactHeader(actual)).toEqual(deserializeTCompactProtocol(expected));
    expect(actual.offset).toBe(view.byteLength);
  }
  const visited: number[] = [];
  expect(() =>
    readCompactHeader(reader([0x19, 0x2c, 0, 0x18, 5, 1]), {
      1: { list: false, visitRange: (_start, _end, index) => visited.push(index) },
    }),
  ).toThrow(/Truncated/);
  expect(visited).toEqual([0]);
});

it("admits projected structures before construction without charging borrowed payload copies", () => {
  const bytes = [0x18, 3, 65, 66, 67, 0x19, 0x25, 2, 4, 0x1c, 0x11, 0, 0];
  const allocations: unknown[] = [];
  const input = reader(bytes);
  const result = readCompactHeader(input, undefined, undefined, (event) => allocations.push(event));
  expect(result).toEqual(deserializeTCompactProtocol(reader(bytes)));
  expect(allocations).toEqual([
    { kind: "struct" },
    { kind: "field" },
    { kind: "binary-view", borrowedBytes: 3 },
    { kind: "field" },
    { kind: "list", length: 2 },
    { kind: "field" },
    { kind: "struct" },
    { kind: "field" },
  ]);
  const skipped: unknown[] = [];
  readCompactHeader(reader(bytes), {}, undefined, (event) => skipped.push(event));
  expect(skipped).toEqual([{ kind: "struct" }]);
});

it("stops at rejected list admission before reading any entries", () => {
  const input = reader([0x19, 0x25, 2, 4, 0]);
  const failure = new Error("allocation denied");
  expect(() =>
    readCompactHeader(input, undefined, undefined, (event) => {
      if (event.kind === "list") throw failure;
    }),
  ).toThrow(failure);
  expect(input.offset).toBe(2);
});

it("does not admit a retained list for incremental visitors", () => {
  const allocations: unknown[] = [];
  const visited: unknown[] = [];
  readCompactHeader(
    reader([0x19, 0x2c, 0, 0, 0]),
    { 1: { list: true, visit: (value) => visited.push(value) } },
    undefined,
    (event) => allocations.push(event),
  );
  expect(visited).toEqual([{}, {}]);
  expect(allocations).toEqual([
    { kind: "struct" },
    { kind: "field" },
    { kind: "struct" },
    { kind: "struct" },
  ]);
});

it("preflights projected allocations without invoking conversion visitors", async () => {
  const { inspectCompactAllocations } = await import("./compact-header.js");
  const bytes = [0x19, 0x2c, 0x18, 2, 65, 66, 0, 0x18, 1, 67, 0, 0x19, 0x25, 2, 4, 0];
  const projection = {
    1: {
      list: { 1: "binary" as const },
      visit: () => {
        throw new Error("Must not convert");
      },
    },
    2: true as const,
  };
  const input = reader(bytes);
  expect(inspectCompactAllocations(input, projection)).toEqual({
    structs: 3,
    fields: 4,
    lists: 1,
    listSlots: 2,
    binaryViews: 2,
    borrowedBytes: 3,
  });
  expect(input.offset).toBe(bytes.length);
  expect(inspectCompactAllocations(reader(bytes), {})).toEqual({
    structs: 1,
    fields: 0,
    lists: 0,
    listSlots: 0,
    binaryViews: 0,
    borrowedBytes: 0,
  });
  expect(() => inspectCompactAllocations(reader(bytes.slice(0, 7)), projection)).toThrow(
    /Truncated/,
  );
});

it("decodes signed i64 values exactly across Number/BigInt accumulation boundaries", () => {
  const values = new Set([0n, -1n, 1n, -(1n << 63n), (1n << 63n) - 1n]);
  for (let bits = 1n; bits < 63n; bits++)
    for (const delta of [-1n, 0n, 1n]) {
      values.add((1n << bits) + delta);
      values.add(-((1n << bits) + delta));
    }
  for (const value of values) {
    let unsigned = (value << 1n) ^ (value >> 63n);
    const bytes = [0x16];
    do {
      const digit = Number(unsigned & 127n);
      unsigned >>= 7n;
      bytes.push(digit | (unsigned ? 128 : 0));
    } while (unsigned);
    bytes.push(0);
    expect(readCompactHeader(reader(bytes)).field_1).toBe(value);
    expect(readCompactHeader(reader(bytes), {})).toEqual({});
  }
  // Non-minimal, but valid, encodings still decode; the tenth digit cannot
  // introduce bits beyond the unsigned 64-bit zigzag payload.
  expect(readCompactHeader(reader([0x16, ...Array(9).fill(128), 0, 0])).field_1).toBe(0n);
  for (const projection of [undefined, {}])
    expect(() =>
      readCompactHeader(reader([0x16, ...Array(9).fill(128), 2, 0]), projection),
    ).toThrow(/exceeds 64 bits/);
});
