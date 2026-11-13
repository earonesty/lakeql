import { zstdCompressSync } from "node:zlib";
import { DEFAULT_PARSERS } from "hyparquet/src/convert.js";
import { getSchemaPath } from "hyparquet/src/schema.js";
import type { ColumnDecoder, PageHeader } from "hyparquet/src/types.js";
import { parquetWriteBuffer } from "hyparquet-writer";
import { batchFromVectors, MemoryBudget, MemoryScope, materializeBatchRows } from "lakeql-core";
import { expect, it } from "vitest";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { planFlatNumericPage, readNumericPage } from "./numeric-page.js";
import { planFlatNumericVector, planFlatNumericVectorHeader } from "./vector-batches.js";
import { planZstdWorkspaceBytes } from "./zstd-workspace-plan.js";

async function decoder(): Promise<ColumnDecoder> {
  const buffer = parquetWriteBuffer({
    columnData: [{ name: "id", type: "INT32", data: [10, null, 30, null] }],
  });
  const metadata = await readParquetMetadataFromFile({
    byteLength: buffer.byteLength,
    async slice(start, end) {
      return buffer.slice(start, end);
    },
  });
  const column = metadata.row_groups[0]?.columns[0]?.meta_data;
  if (!column) throw new Error("missing column");
  const path = getSchemaPath(metadata.schema, column.path_in_schema);
  const leaf = path.at(-1);
  if (!leaf) throw new Error("missing schema");
  return {
    ...column,
    codec: "UNCOMPRESSED",
    pathInSchema: column.path_in_schema,
    schemaPath: path,
    element: leaf.element,
    parsers: DEFAULT_PARSERS,
  };
}

function page(
  v2: boolean,
  runs = [3, 5],
  values = [10, 30],
): { bytes: Uint8Array; header: PageHeader } {
  const prefix = v2 ? 0 : 4;
  const bytes = new Uint8Array(prefix + runs.length + values.length * 4);
  const view = new DataView(bytes.buffer);
  if (!v2) view.setUint32(0, runs.length, true);
  bytes.set(runs, prefix);
  values.forEach((value, index) => {
    view.setInt32(prefix + runs.length + index * 4, value, true);
  });
  const header: PageHeader = {
    type: v2 ? "DATA_PAGE_V2" : "DATA_PAGE",
    compressed_page_size: bytes.length,
    uncompressed_page_size: bytes.length,
    ...(v2
      ? {
          data_page_header_v2: {
            num_values: 4,
            num_nulls: 4 - values.length,
            num_rows: 4,
            encoding: "PLAIN" as const,
            definition_levels_byte_length: runs.length,
            repetition_levels_byte_length: 0,
            is_compressed: false,
          },
        }
      : {
          data_page_header: {
            num_values: 4,
            encoding: "PLAIN" as const,
            definition_level_encoding: "RLE" as const,
            repetition_level_encoding: "RLE" as const,
          },
        }),
  };
  return { bytes, header };
}

it.each([false, true])("admits V2=%s definition levels and numeric backing exactly", async (v2) => {
  const { bytes, header } = page(v2);
  const column = await decoder();
  const scope = new MemoryScope(new MemoryBudget(12));
  const result = readNumericPage(bytes, header, column, scope);
  expect(result?.definitionLevels).toEqual(Uint8Array.of(1, 0, 1, 0));
  expect(result?.values).toEqual(Int32Array.of(10, 30));
  expect(scope.budget.usedBytes).toBe(12);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(11));
  expect(() => readNumericPage(bytes, header, column, tight)).toThrow(/budget/);
  expect(tight.budget.peakBytes).toBe(4);
  tight.dispose();
});

it.each([
  { runs: [0] },
  { runs: [3] },
  { runs: [8, 2] },
  { runs: [254, 255, 255, 255, 15, 1] },
])("rejects malformed level runs %j before level allocation", async ({ runs }) => {
  const { bytes, header } = page(true, runs);
  const column = await decoder();
  const scope = new MemoryScope(new MemoryBudget(64));
  expect(() => readNumericPage(bytes, header, column, scope)).toThrow();
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it("decodes every four-row null pattern with RLE and padded bit packing in both page versions", async () => {
  const column = await decoder();
  for (const v2 of [false, true]) {
    for (let mask = 0; mask < 16; mask += 1) {
      const levels = Array.from({ length: 4 }, (_, index) => (mask >> index) & 1);
      const values = levels.flatMap((level, index) => (level ? [index * 10] : []));
      for (const runs of [[3, mask], levels.flatMap((level) => [2, level])]) {
        const encoded = page(v2, runs, values);
        const scope = new MemoryScope(new MemoryBudget(32));
        const result = readNumericPage(encoded.bytes, encoded.header, column, scope);
        expect(result).toBeDefined();
        expect([...(result?.values ?? [])]).toEqual(values);
        expect(result?.definitionLevels).toEqual(mask === 15 ? undefined : Uint8Array.from(levels));
        expect(scope.budget.usedBytes).toBeLessThanOrEqual(20);
        scope.dispose();
        expect(scope.budget.usedBytes).toBe(0);
      }
    }
  }
});

it("admits dictionary indices, validates dictionary bounds, and handles zero-width indices", async () => {
  const column = await decoder();
  for (const zeroWidth of [false, true]) {
    const bytes = Uint8Array.from(zeroWidth ? [3, 15, 0] : [3, 15, 2, 3, 145, 0]);
    const header: PageHeader = {
      type: "DATA_PAGE_V2",
      compressed_page_size: bytes.length,
      uncompressed_page_size: bytes.length,
      data_page_header_v2: {
        num_values: 4,
        num_nulls: 0,
        num_rows: 4,
        encoding: "RLE_DICTIONARY",
        definition_levels_byte_length: 2,
        repetition_levels_byte_length: 0,
        is_compressed: false,
      },
    };
    const scope = new MemoryScope(new MemoryBudget(16));
    const result = readNumericPage(bytes, header, column, scope, zeroWidth ? 1 : 3);
    expect(result?.dictionaryEncoded).toBe(true);
    expect(result?.values).toEqual(Uint32Array.from(zeroWidth ? [0, 0, 0, 0] : [1, 0, 1, 2]));
    expect(scope.budget.usedBytes).toBe(16);
    scope.dispose();
    const tight = new MemoryScope(new MemoryBudget(15));
    expect(() => readNumericPage(bytes, header, column, tight, 3)).toThrow(/budget/);
    expect(tight.budget.peakBytes).toBe(0);
    tight.dispose();
    const invalid = new MemoryScope(new MemoryBudget(16));
    expect(() => readNumericPage(bytes, header, column, invalid, zeroWidth ? 0 : 2)).toThrow(
      /bounds/,
    );
    invalid.dispose();
    expect(invalid.budget.usedBytes).toBe(0);
  }
});

it.each([false, true])("decodes admitted boolean PLAIN/RLE pages in V2=%s", async (v2) => {
  const original = await decoder();
  const column: ColumnDecoder = {
    ...original,
    type: "BOOLEAN",
    element: { ...original.element, type: "BOOLEAN" },
  };
  for (const encoding of ["PLAIN", "RLE"] as const) {
    const levels = v2 ? [3, 15] : [2, 0, 0, 0, 3, 15];
    const body = encoding === "PLAIN" ? [9] : [2, 0, 0, 0, 3, 9];
    const bytes = Uint8Array.from([...levels, ...body]);
    const header = page(v2, [3, 15], [0, 0, 0, 0]).header;
    header.compressed_page_size = header.uncompressed_page_size = bytes.length;
    if (header.data_page_header) header.data_page_header.encoding = encoding;
    if (header.data_page_header_v2) header.data_page_header_v2.encoding = encoding;
    const scope = new MemoryScope(new MemoryBudget(4));
    const result = readNumericPage(bytes, header, column, scope);
    expect(result?.booleanValues).toBe(true);
    expect(result?.values).toEqual(Uint8Array.of(1, 0, 0, 1));
    expect(scope.budget.usedBytes).toBe(4);
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it.each([
  false,
  true,
])("decodes packed binary values and rejects truncated V2=%s pages", async (v2) => {
  const column = await decoder();
  column.type = "BYTE_ARRAY";
  column.element = { ...column.element, type: "BYTE_ARRAY" };
  // Two present values among four rows: empty and [255, 128].
  const { header } = page(v2);
  const prefix = v2 ? [] : [2, 0, 0, 0];
  const bytes = Uint8Array.from([...prefix, 3, 5, 0, 0, 0, 0, 2, 0, 0, 0, 255, 128]);
  header.compressed_page_size = bytes.length;
  header.uncompressed_page_size = bytes.length;
  const scope = new MemoryScope(new MemoryBudget(36));
  const decoded = readNumericPage(bytes, header, column, scope);
  expect(decoded?.packedVector).toMatchObject({
    type: "dict",
    indices: Uint32Array.of(0, 0, 1, 0),
    valid: Uint8Array.of(1, 0, 1, 0),
    dictionary: { type: "binary-view", length: 2, ranges: Uint32Array.of(4, 0, 8, 2) },
  });
  expect(scope.budget.peakBytes).toBe(36);
  const owner = scope.retainAllocations(decoded);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(36);
  expect(owner.value?.packedVector).toMatchObject({ valid: Uint8Array.of(1, 0, 1, 0) });
  owner.release();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(35));
  expect(() => readNumericPage(bytes, header, column, tight)).toThrow(/budget/);
  tight.dispose();
  expect(tight.budget.usedBytes).toBe(0);
  const truncated = new MemoryScope(new MemoryBudget(36));
  expect(() =>
    readNumericPage(
      bytes.subarray(0, bytes.length - 1),
      {
        ...header,
        compressed_page_size: bytes.length - 1,
        uncompressed_page_size: bytes.length - 1,
      },
      column,
      truncated,
    ),
  ).toThrow(/binary value range/);
  expect(truncated.budget.peakBytes).toBe(4);
  truncated.dispose();
  expect(truncated.budget.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("decodes all-present levels without scratch under exact admission V2=%s", async (v2) => {
  const column = await decoder();
  const { bytes, header } = page(v2, [3, 15], [10, 20, 30, 40]);
  const scope = new MemoryScope(new MemoryBudget(16));
  scope.admit(16, "complete numeric output");
  const result = readNumericPage(bytes, header, column, scope);
  expect(result?.values).toEqual(Int32Array.of(10, 20, 30, 40));
  expect(result?.definitionLevels).toBeUndefined();
  expect(scope.budget.peakBytes).toBe(16);
  expect(scope.budget.usedBytes).toBe(16);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(15));
  expect(() => readNumericPage(bytes, header, column, tight)).toThrow(/budget/);
  expect(tight.budget.usedBytes).toBe(0);
  tight.dispose();
});

it.each([
  false,
  true,
])("releases decoded numeric scratch only when values do not alias it (aligned=%s)", async (aligned) => {
  const column = await decoder();
  const { bytes, header } = page(false, aligned ? [2, 1, 2, 0, 2, 1, 2, 0] : [3, 5]);
  const budget = new MemoryBudget(1024);
  const scope = new MemoryScope(budget);
  let decoded: Uint8Array | undefined;
  try {
    const result = readNumericPage(
      bytes,
      header,
      {
        ...column,
        codec: "SNAPPY",
        compressors: {
          SNAPPY(input) {
            decoded = scope.allocate(input.length, "test decoded backing", () =>
              input.slice(),
            ).value;
            return decoded;
          },
        },
      },
      scope,
    );
    expect(Array.from(result?.values ?? [])).toEqual([10, 30]);
    expect(Array.from(result?.definitionLevels ?? [])).toEqual([1, 0, 1, 0]);
    expect(budget.usedBytes).toBe(aligned ? bytes.length + 4 : 8 + 4);
    expect(scope.tryReleaseAllocation(decoded)).toBe(aligned);
  } finally {
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it.each([
  [false, false],
  [false, true],
  [true, false],
  [true, true],
])("releases compressed independent output backing (v2=%s, boolean=%s)", async (v2, boolean) => {
  const original = await decoder();
  const column: ColumnDecoder = boolean
    ? {
        ...original,
        type: "BOOLEAN",
        element: { ...original.element, type: "BOOLEAN" },
      }
    : original;
  const levels = v2 ? [3, 15] : [2, 0, 0, 0, 3, 15];
  const body = boolean ? [9] : [2, 3, 145, 0];
  const bytes = Uint8Array.from([...levels, ...body]);
  const header = page(v2, [3, 15], [0, 0, 0, 0]).header;
  header.compressed_page_size = header.uncompressed_page_size = bytes.length;
  const encoding = boolean ? "PLAIN" : "RLE_DICTIONARY";
  if (header.data_page_header) header.data_page_header.encoding = encoding;
  if (header.data_page_header_v2) {
    header.data_page_header_v2.encoding = encoding;
    header.data_page_header_v2.is_compressed = true;
  }
  const budget = new MemoryBudget(128);
  const scope = new MemoryScope(budget);
  let decoded: Uint8Array | undefined;
  try {
    const result = readNumericPage(
      bytes,
      header,
      {
        ...column,
        codec: "SNAPPY",
        compressors: {
          SNAPPY(input) {
            decoded = scope.allocate(input.length, "test decompression output", () =>
              input.slice(),
            ).value;
            return decoded;
          },
        },
      },
      scope,
      3,
    );
    expect(Array.from(result?.values ?? [])).toEqual(boolean ? [1, 0, 0, 1] : [1, 0, 1, 2]);
    expect(result?.definitionLevels).toBeUndefined();
    expect(budget.usedBytes).toBe(boolean ? 4 : 16);
    expect(decoded).toBeDefined();
    expect(scope.tryReleaseAllocation(decoded)).toBe(false);
  } finally {
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it("keeps failed compressed dictionary allocations owned until stage cleanup", async () => {
  const column = await decoder();
  const bytes = Uint8Array.of(3, 15, 2, 3, 145, 0);
  const header: PageHeader = {
    type: "DATA_PAGE_V2",
    compressed_page_size: bytes.length,
    uncompressed_page_size: bytes.length,
    data_page_header_v2: {
      num_values: 4,
      num_nulls: 0,
      num_rows: 4,
      encoding: "RLE_DICTIONARY",
      definition_levels_byte_length: 2,
      repetition_levels_byte_length: 0,
      is_compressed: true,
    },
  };
  const budget = new MemoryBudget(20);
  const scope = new MemoryScope(budget);
  try {
    expect(() =>
      readNumericPage(
        bytes,
        header,
        {
          ...column,
          codec: "SNAPPY",
          compressors: {
            SNAPPY(input) {
              return scope.allocate(input.length, "failed page backing", () => input.slice()).value;
            },
          },
        },
        scope,
        2,
      ),
    ).toThrow(/bounds/);
    expect(budget.usedBytes).toBe(20);
  } finally {
    scope.dispose();
  }
  expect(budget.usedBytes).toBe(0);
});

it.each([false, true])("plans full flat numeric backing at exact admission V2=%s", async (v2) => {
  const column = await decoder();
  for (const mask of [0, 5, 15]) {
    const values = Array.from({ length: 4 }, (_, i) => i).filter((i) => mask & (1 << i));
    const encoded = page(v2, [3, mask], values);
    for (const offset of [0, 1, 2, 3]) {
      const backing = new Uint8Array(encoded.bytes.length + offset);
      backing.set(encoded.bytes, offset);
      const bytes = backing.subarray(offset);
      const plan = planFlatNumericPage(bytes, encoded.header, column);
      if (plan === undefined) throw new Error("Missing page plan");
      const scope = new MemoryScope(new MemoryBudget(plan.bytes));
      scope.admit(plan.bytes, "flat decode");
      const output = plan.execute(scope);
      expect([...(output.values ?? [])]).toEqual(values);
      expect(scope.availableAllocationBytes).toBe(0);
      expect(scope.budget.peakBytes).toBe(plan.bytes);
      if (ArrayBuffer.isView(output.values) && output.values.byteLength > 0)
        expect(output.values.buffer === bytes.buffer).toBe(plan.borrowsInput);
      scope.dispose();
      expect(scope.budget.usedBytes).toBe(0);
    }
  }
  const encoded = page(v2);
  const compressed = planFlatNumericPage(encoded.bytes, encoded.header, {
    ...column,
    codec: "SNAPPY",
  });
  if (v2) expect(compressed).toBeDefined();
  else expect(compressed).toBeUndefined();
});

it("plans byte-stream-split output as detached even with aligned source backing", async () => {
  const column = await decoder();
  const { bytes, header } = page(true, [4, 1, 4, 1], [10, 20, 30, 40]);
  const original = bytes.slice(4);
  for (let lane = 0; lane < 4; lane++)
    for (let row = 0; row < 4; row++) bytes[4 + lane * 4 + row] = original[row * 4 + lane] ?? 0;
  if (header.data_page_header_v2 === undefined) throw new Error("Missing V2 header");
  header.data_page_header_v2.encoding = "BYTE_STREAM_SPLIT";
  const plan = planFlatNumericPage(bytes, header, column);
  expect(plan?.bytes).toBe(16);
  expect(plan?.borrowsInput).toBe(false);
  if (plan === undefined) throw new Error("Missing split plan");
  const scope = new MemoryScope(new MemoryBudget(16));
  scope.admit(plan.bytes, "split decode");
  expect(plan.execute(scope).values).toEqual(Int32Array.of(10, 20, 30, 40));
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
});

it.each([
  "SNAPPY",
  "ZSTD",
] as const)("plans V2 %s backing without overriding custom codecs and cleans up malformed input", async (codec) => {
  const column = { ...(await decoder()), codec };
  const plain = page(true, [3, 15], [10, 20, 30, 40]);
  const compressed =
    codec === "ZSTD"
      ? zstdCompressSync(plain.bytes.subarray(2))
      : [16, 60, ...plain.bytes.subarray(2)];
  const bytes = Uint8Array.from([3, 15, ...compressed]);
  const extent = 16 + (codec === "ZSTD" ? planZstdWorkspaceBytes(Uint8Array.from(compressed)) : 0);
  if (plain.header.data_page_header_v2 === undefined) throw new Error("Missing V2 header");
  const header: PageHeader = {
    ...plain.header,
    compressed_page_size: bytes.length,
    data_page_header_v2: { ...plain.header.data_page_header_v2, is_compressed: true },
  };
  const plan = planFlatNumericPage(bytes, header, column);
  expect(plan?.bytes).toBe(extent);
  expect(plan?.borrowsInput).toBe(false);
  expect(
    planFlatNumericPage(bytes, header, {
      ...column,
      compressors: {
        [codec]() {
          throw new Error("custom");
        },
      },
    }),
  ).toBeUndefined();
  if (plan === undefined) throw new Error("Missing Snappy plan");
  const scope = new MemoryScope(new MemoryBudget(extent));
  scope.admit(extent, "snappy stage");
  expect(plan.execute(scope).values).toEqual(Int32Array.of(10, 20, 30, 40));
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  if (codec === "ZSTD") {
    expect(() => planFlatNumericPage(bytes.subarray(0, bytes.length - 1), header, column)).toThrow(
      /Truncated/,
    );
    return;
  }
  const malformed = planFlatNumericPage(bytes.subarray(0, bytes.length - 1), header, column);
  if (malformed === undefined) throw new Error("Missing malformed extent");
  const failed = new MemoryScope(new MemoryBudget(extent));
  failed.admit(extent, "malformed snappy");
  expect(() => malformed.execute(failed)).toThrow();
  failed.dispose();
  expect(failed.budget.usedBytes).toBe(0);
});

it.each([false, true])("plans nullable and decimal final vectors V2=%s", async (v2) => {
  const raw = await decoder();
  for (const decimal of [false, true]) {
    const column = decimal
      ? {
          ...raw,
          element: { ...raw.element, converted_type: "DECIMAL" as const, scale: 2, precision: 9 },
        }
      : raw;
    for (const mask of [0, 5, 15]) {
      const values = [-1234, 0, 5678, 99].filter((_, i) => mask & (1 << i));
      const encoded = page(v2, [3, mask], values);
      const plan = planFlatNumericVector(encoded.bytes, encoded.header, column);
      if (!plan) throw new Error("Missing vector capability");
      const scope = new MemoryScope(new MemoryBudget(plan.bytes));
      scope.admit(plan.bytes, "complete numeric vector");
      const vector = plan.execute(scope);
      const rows = materializeBatchRows(batchFromVectors({ x: vector }));
      expect(rows).toEqual(
        [-1234, 0, 5678, 99].map((value, i) => ({
          x: mask & (1 << i) ? value * (decimal ? 0.01 : 1) : null,
        })),
      );
      expect(scope.availableAllocationBytes).toBe(0);
      scope.dispose();
      expect(scope.budget.usedBytes).toBe(0);
      if (plan.bytes > 0) {
        const tight = new MemoryScope(new MemoryBudget(plan.bytes - 1));
        expect(() => tight.admit(plan.bytes, "complete vector")).toThrow(/budget/);
        expect(tight.budget.peakBytes).toBe(0);
        tight.dispose();
      }
    }
  }
  const encoded = page(v2, [3, 15], [1, 2, 3, 4]);
  expect(
    planFlatNumericVector(encoded.bytes, encoded.header, {
      ...raw,
      element: { ...raw.element, converted_type: "DATE" },
    }),
  ).toBeUndefined();
});

it.each([
  "UNCOMPRESSED",
  "SNAPPY",
  "ZSTD",
] as const)("retains unsigned reinterpretation backing through composed %s plans", async (codec) => {
  const raw = await decoder();
  const column = { ...raw, codec, element: { ...raw.element, converted_type: "UINT_32" as const } };
  for (const mask of [5, 15]) {
    const inputValues = [-1, 0, -2147483648, 123];
    const encoded = page(
      true,
      [3, mask],
      inputValues.filter((_, i) => mask & (1 << i)),
    );
    const payload = encoded.bytes.subarray(2);
    const compressed =
      codec === "ZSTD"
        ? zstdCompressSync(payload)
        : codec === "SNAPPY"
          ? Uint8Array.from([payload.length, (payload.length - 1) << 2, ...payload])
          : payload;
    const bytes = Uint8Array.from([3, mask, ...compressed]);
    if (encoded.header.data_page_header_v2 === undefined) throw new Error("Missing V2 header");
    const header: PageHeader = {
      ...encoded.header,
      compressed_page_size: bytes.length,
      data_page_header_v2: {
        ...encoded.header.data_page_header_v2,
        is_compressed: codec !== "UNCOMPRESSED",
      },
    };
    const plan = planFlatNumericVector(bytes, header, column);
    if (!plan) throw new Error("Missing unsigned capability");
    const scope = new MemoryScope(new MemoryBudget(plan.bytes));
    scope.admit(plan.bytes, "unsigned vector");
    const vector = plan.execute(scope);
    expect(vector.type).toBe("u32");
    const retained = scope.retainAllocations(vector);
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(16 + (mask === 15 ? 0 : 4));
    expect(materializeBatchRows(batchFromVectors({ x: retained.value }))).toEqual(
      inputValues.map((value, i) => ({ x: mask & (1 << i) ? value >>> 0 : null })),
    );
    retained.release();
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it("validates V2 header-planned null counts during execution without exceeding admission", async () => {
  const column = await decoder();
  const encoded = page(true, [3, 5], [10, 20]);
  const original = encoded.header.data_page_header_v2;
  if (original === undefined) throw new Error("Missing V2 header");
  for (const num_nulls of [-1, 5, 1.5, Number.NaN]) {
    expect(() =>
      planFlatNumericVector(
        encoded.bytes,
        { ...encoded.header, data_page_header_v2: { ...original, num_nulls } },
        column,
      ),
    ).toThrow(/counts/);
  }
  // Keep sufficient encoded payload so count mismatch is checked by execution,
  // rather than rejected by the independent physical payload-length check.
  const bytes = new Uint8Array(encoded.bytes.length + 16);
  bytes.set(encoded.bytes);
  for (const num_nulls of [0, 4]) {
    const plan = planFlatNumericVector(
      bytes,
      { ...encoded.header, data_page_header_v2: { ...original, num_nulls } },
      column,
    );
    if (!plan) throw new Error("Missing plan");
    const scope = new MemoryScope(new MemoryBudget(plan.bytes));
    scope.admit(plan.bytes, "untrusted V2 counts");
    try {
      expect(() => plan.execute(scope)).toThrow(/counts|extent/);
      expect(scope.budget.peakBytes).toBeLessThanOrEqual(plan.bytes);
    } finally {
      scope.dispose();
    }
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it.each([
  "UNCOMPRESSED",
  "SNAPPY",
  "ZSTD",
] as const)("bounds V1 %s final vectors including hidden null counts", async (codec) => {
  const raw = await decoder();
  for (const mask of [0, 5, 15])
    for (const decimal of [false, true]) {
      const values = [10, 20, 30, 40];
      const encoded = page(
        false,
        [3, mask],
        values.filter((_, i) => mask & (1 << i)),
      );
      const input =
        codec === "ZSTD"
          ? new Uint8Array(zstdCompressSync(encoded.bytes))
          : codec === "SNAPPY"
            ? Uint8Array.from([
                encoded.bytes.length,
                (encoded.bytes.length - 1) << 2,
                ...encoded.bytes,
              ])
            : encoded.bytes;
      const header = { ...encoded.header, compressed_page_size: input.length };
      const column = {
        ...raw,
        codec,
        ...(decimal
          ? {
              element: {
                ...raw.element,
                converted_type: "DECIMAL" as const,
                precision: 9,
                scale: 2,
              },
            }
          : {}),
      };
      const plan = planFlatNumericVectorHeader(header, column);
      if (!plan) throw new Error("Missing V1 header plan");
      const budget = new MemoryBudget(plan.bytes);
      const scope = new MemoryScope(budget);
      scope.admit(plan.bytes, "V1 planned vector");
      const vector = plan.execute(input, scope);
      const owner = scope.retainAllocations(vector);
      scope.dispose();
      if (mask !== 0) expect(budget.usedBytes).toBeGreaterThan(0);
      expect(materializeBatchRows(batchFromVectors({ x: owner.value }))).toEqual(
        values.map((value, i) => ({ x: mask & (1 << i) ? value / (decimal ? 100 : 1) : null })),
      );
      owner.release();
      expect(budget.usedBytes).toBe(0);
    }
});

it.each([
  false,
  true,
])("plans boolean PLAIN/RLE vectors with nulls and codecs V2=%s", async (v2) => {
  const original = await decoder();
  for (const encoding of ["PLAIN", "RLE"] as const) {
    for (const mask of [0, 5, 15]) {
      for (const codec of ["UNCOMPRESSED", "SNAPPY", "ZSTD"] as const) {
        const column: ColumnDecoder = {
          ...original,
          codec,
          type: "BOOLEAN",
          element: { ...original.element, type: "BOOLEAN" },
        };
        const present = mask === 0 ? 0 : mask === 5 ? 2 : 4;
        const valueBits = mask === 15 ? 9 : 1;
        const levels = v2 ? [3, mask] : [2, 0, 0, 0, 3, mask];
        const values = encoding === "PLAIN" ? [valueBits] : [2, 0, 0, 0, 3, valueBits];
        const plain = Uint8Array.from([...levels, ...values]);
        const valueOffset = v2 ? levels.length : 0;
        const payload = plain.subarray(valueOffset);
        const compressed =
          codec === "UNCOMPRESSED"
            ? payload
            : codec === "SNAPPY"
              ? Uint8Array.of(payload.length, (payload.length - 1) << 2, ...payload)
              : zstdCompressSync(payload);
        const bytes = Uint8Array.from([...plain.subarray(0, valueOffset), ...compressed]);
        const header: PageHeader = {
          type: v2 ? "DATA_PAGE_V2" : "DATA_PAGE",
          compressed_page_size: bytes.length,
          uncompressed_page_size: plain.length,
          ...(v2
            ? {
                data_page_header_v2: {
                  num_values: 4,
                  num_nulls: 4 - present,
                  num_rows: 4,
                  encoding,
                  definition_levels_byte_length: levels.length,
                  repetition_levels_byte_length: 0,
                  is_compressed: codec !== "UNCOMPRESSED",
                },
              }
            : {
                data_page_header: {
                  num_values: 4,
                  encoding,
                  definition_level_encoding: "RLE",
                  repetition_level_encoding: "RLE",
                },
              }),
        };
        const plan = planFlatNumericVectorHeader(header, column);
        if (!plan) throw new Error("Missing boolean vector plan");
        const budget = new MemoryBudget(plan.bytes);
        const scope = new MemoryScope(budget);
        scope.admit(plan.bytes, "boolean vector");
        const vector = plan.execute(bytes, scope);
        const owner = scope.retainAllocations(vector);
        scope.dispose();
        const expected =
          mask === 0
            ? [null, null, null, null]
            : mask === 5
              ? [true, null, false, null]
              : [true, false, false, true];
        expect(materializeBatchRows(batchFromVectors({ flag: owner.value }))).toEqual(
          expected.map((flag) => ({ flag })),
        );
        expect(budget.peakBytes).toBeLessThanOrEqual(plan.bytes);
        owner.release();
        expect(budget.usedBytes).toBe(0);
      }
    }
  }
});

it("validates explicit zero-bit V2 repetition runs without allocating levels", async () => {
  const column = await decoder();
  const original = page(true);
  if (original.header.data_page_header_v2 === undefined) throw new Error("Missing V2 header");
  for (const repetitions of [[8], [3]]) {
    const bytes = Uint8Array.from([...repetitions, ...original.bytes]);
    const header: PageHeader = {
      ...original.header,
      compressed_page_size: bytes.length,
      uncompressed_page_size: bytes.length,
      data_page_header_v2: {
        ...original.header.data_page_header_v2,
        repetition_levels_byte_length: repetitions.length,
      },
    };
    const plan = planFlatNumericVector(bytes, header, column);
    if (!plan) throw new Error("Missing explicit-level plan");
    const scope = new MemoryScope(new MemoryBudget(plan.bytes));
    scope.admit(plan.bytes, "explicit zero repetition");
    const vector = plan.execute(scope);
    expect(materializeBatchRows(batchFromVectors({ x: vector }))).toEqual([
      { x: 10 },
      { x: null },
      { x: 30 },
      { x: null },
    ]);
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(0);
  }
});

it.each(
  [[0], [10], [8, 1], [128]].map((repetitions) => ({ repetitions })),
)("rejects malformed flat repetition runs %j before allocation", async ({ repetitions }) => {
  const column = await decoder();
  const original = page(true);
  if (original.header.data_page_header_v2 === undefined) throw new Error("Missing V2 header");
  const bytes = Uint8Array.from([...repetitions, ...original.bytes]);
  const header: PageHeader = {
    ...original.header,
    compressed_page_size: bytes.length,
    uncompressed_page_size: bytes.length,
    data_page_header_v2: {
      ...original.header.data_page_header_v2,
      repetition_levels_byte_length: repetitions.length,
    },
  };
  const scope = new MemoryScope(new MemoryBudget(1024));
  expect(() => readNumericPage(bytes, header, column, scope)).toThrow(
    /Malformed Parquet level runs/,
  );
  expect(scope.budget.peakBytes).toBe(0);
  scope.dispose();
});

it.each([
  false,
  true,
])("bounds output when a V2=%s bit-packed level run pads multiple groups", async (v2) => {
  const column = await decoder();
  // Eight encoded groups (64 levels), but the page declares only four rows.
  // DuckDB emits multi-group padding; padding must not become output or scratch.
  const { bytes, header } = page(v2, [17, 5, 255, 255, 255, 255, 255, 255, 255]);
  const scope = new MemoryScope(new MemoryBudget(12));
  try {
    const result = readNumericPage(bytes, header, column, scope);
    expect(result?.definitionLevels).toEqual(Uint8Array.of(1, 0, 1, 0));
    expect(result?.values).toEqual(Int32Array.of(10, 30));
    expect(scope.budget.peakBytes).toBe(12);
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
  const truncated = page(v2, [17, 5]);
  const rejected = new MemoryScope(new MemoryBudget(12));
  try {
    expect(() => readNumericPage(truncated.bytes, truncated.header, column, rejected)).toThrow(
      /Malformed Parquet level runs/,
    );
    expect(rejected.budget.peakBytes).toBe(0);
  } finally {
    rejected.dispose();
  }
});

it.each([false, true])("admits logical binary PLAIN carriers for V2=%s", async (v2) => {
  const { bytes, header } = page(v2);
  const base = await decoder();
  const scope = new MemoryScope(new MemoryBudget(65536));
  const column: ColumnDecoder = {
    ...base,
    type: "FIXED_LEN_BYTE_ARRAY",
    element: {
      ...base.element,
      type: "FIXED_LEN_BYTE_ARRAY",
      type_length: 4,
      converted_type: "DECIMAL",
      precision: 8,
      scale: 2,
    },
  };
  const result = readNumericPage(bytes, header, column, scope);
  expect(result).toBeDefined();
  expect(result?.definitionLevels).toEqual(Uint8Array.of(1, 0, 1, 0));
  expect((result?.values as Uint8Array[]).map((value) => [...value])).toEqual([
    [10, 0, 0, 0],
    [30, 0, 0, 0],
  ]);
  expect((result?.values as Uint8Array[])[0]?.buffer).toBe(bytes.buffer);
  const peak = scope.budget.peakBytes;
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => readNumericPage(bytes, header, column, tight)).toThrow(/budget/);
  tight.dispose();
  expect(tight.budget.usedBytes).toBe(0);
});

it("retains admitted decompression backing borrowed by logical binary carriers", async () => {
  const { bytes, header } = page(false);
  const base = await decoder();
  const scope = new MemoryScope(new MemoryBudget(65536));
  let backing: Uint8Array | undefined;
  const column: ColumnDecoder = {
    ...base,
    type: "FIXED_LEN_BYTE_ARRAY",
    codec: "GZIP",
    element: {
      ...base.element,
      type: "FIXED_LEN_BYTE_ARRAY",
      type_length: 4,
      converted_type: "DECIMAL",
      precision: 8,
      scale: 2,
    },
    compressors: {
      GZIP() {
        backing = scope.allocate(bytes.length, "test decoded page", () => bytes.slice()).value;
        return backing;
      },
    },
  };
  const result = readNumericPage(bytes, header, column, scope);
  expect((result?.values as Uint8Array[])[0]?.buffer).toBe(backing?.buffer);
  if (backing === undefined) throw new Error("Missing decoded page");
  const owner = scope.retainAllocation(backing);
  expect(owner.value).toBe(backing);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(bytes.length);
  owner.release();
  expect(scope.budget.usedBytes).toBe(0);
});
