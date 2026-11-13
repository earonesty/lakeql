import { MATERIALIZATION_CHARGES as c } from "lakeql-core";
import { expect, it } from "vitest";
import {
  columnRawAliasBytes,
  rawMetadataValueBytes,
  rowGroupRawAliasBytes,
} from "./metadata-aliases.js";

it("accounts for nested opaque trees and borrowed views without duplicating payload bytes", () => {
  const value = { field_1: [1n, new Uint8Array(1000)] };
  expect(rawMetadataValueBytes(value)).toBe(
    c.object +
      c.property +
      c.string +
      7 * c.stringCodeUnit +
      c.array +
      2 * c.arraySlot +
      c.bigint +
      c.view,
  );
});
it("keeps every forwarded opaque column shape and excludes separately converted paths", () => {
  const opaque = { field_1: [1n, 2n] };
  const bytes = new Uint8Array(10);
  const chunk = {
    field_1: bytes,
    field_2: 0n,
    field_8: opaque,
    field_9: bytes,
    field_3: {
      field_1: 1,
      field_2: [0],
      field_3: [bytes],
      field_4: 0,
      field_8: [{ field_1: bytes }],
      field_15: opaque,
      field_16: opaque,
      field_17: opaque,
      field_12: opaque,
      field_13: opaque,
    },
  };
  expect(columnRawAliasBytes(chunk)).toBe(c.bigint + c.view + 6 * rawMetadataValueBytes(opaque));
  expect(
    rowGroupRawAliasBytes({ field_1: [chunk], field_2: 1n, field_3: 2n, field_7: opaque }),
  ).toBe(2 * c.bigint + rawMetadataValueBytes(opaque));
});

it("retains encoded opaque and histogram values after parser and consumer scopes close", async () => {
  const { ByteWriter } = await import("hyparquet-writer/src/bytewriter.js");
  const { serializeTCompactProtocol } = await import("hyparquet-writer/src/thrift.js");
  const { MemoryBudget, MemoryScope } = await import("lakeql-core");
  const { decodeRowGroupMetadata } = await import("./row-group-metadata.js");
  const writer = new ByteWriter();
  serializeTCompactProtocol(writer, {
    field_1: [
      {
        field_2: 0n,
        field_3: {
          field_1: 1,
          field_2: [0],
          field_3: ["id"],
          field_4: 0,
          field_5: 2n,
          field_6: 8n,
          field_7: 8n,
          field_9: 4n,
          field_15: { field_1: new Uint8Array([7, 8]) },
          field_16: { field_1: 8n, field_2: [1n, 2n], field_3: [3n, 4n] },
          field_17: { field_1: { field_1: 1.5, field_2: 2.5 }, field_2: [1, 3] },
        },
        field_8: { field_1: { field_1: new Uint8Array([9, 10]) } },
        field_9: new Uint8Array([11, 12]),
      },
    ],
    field_2: 8n,
    field_3: 2n,
    field_4: [{ field_1: 0, field_2: true, field_3: false }],
    field_7: 3,
  });
  const encoded = writer.buffer.slice(0, writer.offset);
  const budget = new MemoryBudget(65536);
  const scope = new MemoryScope(budget);
  const backing = scope.allocate(encoded.byteLength, "encoded fixture", () => encoded).value;
  const result = decodeRowGroupMetadata(
    new DataView(backing),
    [{ name: "id", type: "INT32" }],
    true,
    scope,
  );
  expect(budget.peakBytes).toBeGreaterThan(budget.usedBytes);
  const retained = budget.usedBytes;
  const owner = scope.retainAllocations(result);
  scope.dispose();
  try {
    expect(budget.usedBytes).toBe(retained);
    const column = owner.value.columns[0];
    expect(column?.meta_data?.size_statistics).toEqual({
      unencoded_byte_array_data_bytes: 8n,
      repetition_level_histogram: [1n, 2n],
      definition_level_histogram: [3n, 4n],
    });
    expect(column?.meta_data?.geospatial_statistics?.geospatial_types).toEqual([1, 3]);
    expect(column?.meta_data?.bloom_filter_length).toEqual({ field_1: new Uint8Array([7, 8]) });
    expect(column?.crypto_metadata).toEqual({ field_1: { field_1: new Uint8Array([9, 10]) } });
    expect(column?.encrypted_column_metadata).toEqual(new Uint8Array([11, 12]));
    expect(owner.value.sorting_columns).toEqual([
      { column_idx: 0, descending: true, nulls_first: false },
    ]);
  } finally {
    owner.release();
  }
  expect(budget.usedBytes).toBe(0);
});
