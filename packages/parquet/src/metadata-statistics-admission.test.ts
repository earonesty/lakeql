import type { SchemaElement } from "hyparquet";
import {
  MATERIALIZATION_CHARGES,
  MemoryBudget,
  MemoryScope,
  timestampFromEpoch,
} from "lakeql-core";
import { expect, it } from "vitest";
import {
  columnMetadataStructureBytes,
  convertColumnChunkMetadata,
  convertRowGroupMetadata,
} from "./row-group-metadata.js";

it("admits textual statistics before conversion and propagates budget failures", () => {
  const encoded = new TextEncoder().encode("\ufeffé😀");
  const chunk = { field_3: { field_3: [], field_12: { field_5: encoded } } };
  const schema: SchemaElement = { name: "text", type: "BYTE_ARRAY" };
  const bytes =
    columnMetadataStructureBytes(chunk) +
    MATERIALIZATION_CHARGES.string +
    encoded.length * MATERIALIZATION_CHARGES.stringCodeUnit;
  const scope = new MemoryScope(new MemoryBudget(bytes));
  try {
    const result = convertColumnChunkMetadata(chunk, schema, true, scope);
    expect(result.meta_data?.statistics?.max_value).toBe("\ufeffé😀");
    expect(scope.budget.usedBytes).toBe(bytes);
  } finally {
    scope.dispose();
  }
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  try {
    expect(() => convertColumnChunkMetadata(chunk, schema, true, tight)).toThrow(/budget/);
    expect(tight.budget.usedBytes).toBe(0);
  } finally {
    tight.dispose();
  }
});

it("converts scalar decimal bounds and omits unsafe pruning bounds", () => {
  const schema: SchemaElement = {
    name: "amount",
    type: "FIXED_LEN_BYTE_ARRAY",
    converted_type: "DECIMAL",
    precision: 18,
    scale: 2,
  };
  const stats = convertColumnChunkMetadata(
    {
      field_3: {
        field_3: [],
        field_12: {
          field_5: new Uint8Array([0x04, 0xd2]),
          field_6: new Uint8Array([0xfb, 0x2e]),
          field_1: new Uint8Array([0x20, 0, 0, 0, 0, 0, 0]),
        },
      },
    },
    schema,
    true,
  ).meta_data?.statistics;
  expect(stats?.max_value).toBe(12.34);
  expect(stats?.min_value).toBe(-12.34);
  expect(stats).not.toHaveProperty("max");
});

it("admits mapped containers while preserving borrowed histogram payloads", () => {
  const histogram = [1n, 2n];
  const chunk = {
    field_3: {
      field_2: [0, 3],
      field_3: [],
      field_13: [{ field_1: 0, field_2: 0, field_3: 2 }],
      field_16: { field_2: histogram },
    },
  };
  const bytes = columnMetadataStructureBytes(chunk);
  const scope = new MemoryScope(new MemoryBudget(bytes));
  const result = convertColumnChunkMetadata(chunk, undefined, false, scope);
  expect(result.meta_data?.encodings).toEqual(["PLAIN", "RLE"]);
  expect(result.meta_data?.size_statistics?.repetition_level_histogram).toBe(histogram);
  expect(scope.budget.usedBytes).toBe(bytes);
  const owner = scope.retainAllocations(result);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(bytes);
  owner.release();
  expect(scope.budget.usedBytes).toBe(0);
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  try {
    expect(() => convertColumnChunkMetadata(chunk, undefined, false, tight)).toThrow(/budget/);
    expect(tight.budget.peakBytes).toBe(0);
  } finally {
    tight.dispose();
  }
});

it("admits row-group and sorting containers at their exact shape", () => {
  const c = MATERIALIZATION_CHARGES;
  const bytes =
    c.object + 7 * c.property + c.array + c.arraySlot + c.object + 3 * c.property + c.array;
  const raw = {
    field_1: [],
    field_2: 0n,
    field_3: 0n,
    field_4: [{ field_1: 0, field_2: true, field_3: false }],
  };
  const scope = new MemoryScope(new MemoryBudget(bytes));
  try {
    const group = convertRowGroupMetadata(raw, [], undefined, scope);
    expect(group.sorting_columns).toEqual([
      { column_idx: 0, descending: true, nulls_first: false },
    ]);
    expect(group.columns).toEqual([]);
    expect(scope.budget.usedBytes).toBe(bytes);
  } finally {
    scope.dispose();
  }
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  try {
    expect(() => convertRowGroupMetadata(raw, [], undefined, tight)).toThrow(/budget/);
    expect(tight.budget.usedBytes).toBe(0);
  } finally {
    tight.dispose();
  }
});

it("propagates fixed-width decimal admission failure instead of dropping the bound", () => {
  const encoded = new Uint8Array(8);
  new DataView(encoded.buffer).setBigInt64(0, 1234n, true);
  const chunk = { field_3: { field_3: [], field_12: { field_5: encoded } } };
  const schema: SchemaElement = {
    name: "amount",
    type: "INT64",
    converted_type: "DECIMAL",
    precision: 18,
    scale: 2,
  };
  const c = MATERIALIZATION_CHARGES;
  const bytes = columnMetadataStructureBytes(chunk) + c.view + c.number + c.bigint;
  const scope = new MemoryScope(new MemoryBudget(bytes));
  try {
    expect(
      convertColumnChunkMetadata(chunk, schema, true, scope).meta_data?.statistics?.max_value,
    ).toBe(12.34);
    expect(scope.budget.peakBytes).toBe(bytes);
    expect(scope.budget.usedBytes).toBe(columnMetadataStructureBytes(chunk) + c.number);
  } finally {
    scope.dispose();
  }
  const tight = new MemoryScope(new MemoryBudget(bytes - 1));
  try {
    expect(() => convertColumnChunkMetadata(chunk, schema, true, tight)).toThrow(/budget/);
    expect(tight.budget.usedBytes).toBe(0);
  } finally {
    tight.dispose();
  }
});

it.each([
  false,
  true,
])("retires statistic DataView scratch while retaining scalar output (timestamp=%s)", (temporal) => {
  const encoded = new Uint8Array(8);
  new DataView(encoded.buffer).setBigInt64(0, 42n, true);
  const chunk = { field_3: { field_3: [], field_12: { field_5: encoded } } };
  const schema: SchemaElement = {
    name: "value",
    type: "INT64",
    ...(temporal ? { converted_type: "TIMESTAMP_MICROS" as const } : {}),
  };
  const c = MATERIALIZATION_CHARGES;
  const retained =
    columnMetadataStructureBytes(chunk) + c.number + c.bigint + (temporal ? c.timestamp : 0);
  const capacity = retained + c.view;
  const scope = new MemoryScope(new MemoryBudget(capacity));
  try {
    const output = convertColumnChunkMetadata(chunk, schema, false, scope);
    expect(output.meta_data?.statistics?.max_value).toEqual(
      temporal ? timestampFromEpoch(42n, "micros") : 42n,
    );
    expect(scope.budget.peakBytes).toBe(capacity);
    expect(scope.budget.usedBytes).toBe(retained);
  } finally {
    scope.dispose();
  }
  const tight = new MemoryScope(new MemoryBudget(capacity - 1));
  try {
    expect(() => convertColumnChunkMetadata(chunk, schema, false, tight)).toThrow(/budget/);
    expect(tight.budget.usedBytes).toBe(0);
  } finally {
    tight.dispose();
  }
});

it.each([
  false,
  true,
])("admits fixed-byte statistic conversion workspace, float16=%s", (float16) => {
  const encoded = new Uint8Array([0, 0x3c]);
  const schema: SchemaElement = {
    name: "value",
    type: "FIXED_LEN_BYTE_ARRAY",
    type_length: 2,
    ...(float16 ? { logical_type: { type: "FLOAT16" as const } } : {}),
  };
  const chunk = { field_3: { field_3: [], field_12: { field_5: encoded } } };
  const retained =
    columnMetadataStructureBytes(chunk) + (float16 ? MATERIALIZATION_CHARGES.number : 0);
  const peak = retained + MATERIALIZATION_CHARGES.view;
  const scope = new MemoryScope(new MemoryBudget(peak));
  try {
    const value = convertColumnChunkMetadata(chunk, schema, true, scope).meta_data?.statistics
      ?.max_value;
    if (float16) expect(value).toBe(1);
    else expect(value).toBe(encoded);
    expect(scope.budget.usedBytes).toBe(retained);
    expect(scope.budget.peakBytes).toBe(peak);
  } finally {
    scope.dispose();
  }
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  try {
    expect(() => convertColumnChunkMetadata(chunk, schema, true, tight)).toThrow(/budget/);
    expect(tight.budget.usedBytes).toBe(0);
  } finally {
    tight.dispose();
  }
});
