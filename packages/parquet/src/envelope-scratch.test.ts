import { parquetMetadata } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { ByteWriter } from "hyparquet-writer/src/bytewriter.js";
import { serializeTCompactProtocol } from "hyparquet-writer/src/thrift.js";
import { MemoryBudget, MemoryScope } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { readParquetFooterEnvelope } from "./incremental-metadata.js";

it("releases schema and key/value scratch between real footer entries", () => {
  const maximumEntryBytes = [];
  for (const width of [8, 32, 128]) {
    const bytes = parquetWriteBuffer({
      columnData: Array.from({ length: width }, (_, i) => ({
        name: `c${String(i).padStart(3, "0")}`,
        type: "INT32" as const,
        data: [i, i + 1],
      })),
      kvMetadata: Array.from({ length: width }, (_, i) => ({
        key: `k${String(i).padStart(3, "0")}`,
        value: "value ✓",
      })),
    });
    const scope = new MemoryScope(new MemoryBudget(16 * 1024 * 1024));
    const reserve = vi.spyOn(scope.budget, "reserve");
    const output = readParquetFooterEnvelope(bytes, undefined, scope);
    const entries = reserve.mock.calls.filter(([, purpose]) => purpose === "raw compact entry");
    expect(entries).toHaveLength(width * 2 + 1);
    maximumEntryBytes.push(Math.max(...entries.map(([bytes]) => bytes)));
    reserve.mockRestore();
    const expected = parquetMetadata(bytes);
    expect(output.metadata.schema).toEqual(expected.schema);
    expect(output.metadata.key_value_metadata).toEqual(expected.key_value_metadata);
    const peak = scope.budget.peakBytes;
    const retained = scope.budget.usedBytes;
    const owner = scope.retainAllocations(output);
    scope.dispose();
    expect(scope.budget.usedBytes).toBe(retained);
    owner.release();
    expect(scope.budget.usedBytes).toBe(0);
    for (const cap of [peak - 1, peak]) {
      const tight = new MemoryScope(new MemoryBudget(cap));
      try {
        if (cap < peak)
          expect(() => readParquetFooterEnvelope(bytes, undefined, tight)).toThrow(/budget/i);
        else
          expect(readParquetFooterEnvelope(bytes, undefined, tight).metadata).toEqual(
            output.metadata,
          );
      } finally {
        tight.dispose();
      }
      expect(tight.budget.usedBytes).toBe(0);
    }
  }
  expect(new Set(maximumEntryBytes).size).toBe(1);
});

it("transfers unknown logical metadata aliases out of entry scratch", () => {
  const writer = new ByteWriter();
  serializeTCompactProtocol(writer, {
    field_1: 1,
    field_2: [
      { field_4: "root", field_5: 1 },
      {
        field_1: 1,
        field_3: 0,
        field_4: "id",
        field_10: { field_30: { field_1: new Uint8Array([7, 8]), field_2: [1n, 2n] } },
      },
    ],
    field_3: 0n,
    field_4: [],
  });
  const footer = new Uint8Array(writer.offset + 8);
  footer.set(new Uint8Array(writer.buffer, 0, writer.offset));
  new DataView(footer.buffer).setUint32(writer.offset, writer.offset, true);
  // Generic writer guesses the empty list element type; Parquet requires STRUCT.
  footer[writer.offset - 2] = 0x0c;
  footer.set([80, 65, 82, 49], writer.offset + 4);
  const scope = new MemoryScope(new MemoryBudget(65536));
  const backing = scope.allocate(footer.byteLength, "fixture footer", () => footer.buffer).value;
  const result = readParquetFooterEnvelope(backing, undefined, scope);
  const owner = scope.retainAllocations(result);
  scope.dispose();
  expect(owner.value.metadata.schema[1]?.logical_type).toEqual({
    field_30: { field_1: Uint8Array.of(7, 8), field_2: [1n, 2n] },
  });
  expect(scope.budget.usedBytes).toBeGreaterThan(footer.byteLength);
  owner.release();
  expect(scope.budget.usedBytes).toBe(0);
});
