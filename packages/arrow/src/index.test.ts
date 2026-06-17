import { tableFromArrays, tableFromIPC } from "apache-arrow";
import { batchFromColumns, createInMemoryLake, gt } from "lakeql-core";
import { describe, expect, it } from "vitest";
import {
  arrowTableToRows,
  batchToArrowIPC,
  batchToArrowTable,
  createArrowLake,
  queryToArrowIPC,
  queryToArrowIPCStream,
  queryToArrowTable,
  rowsToArrowIPC,
  rowsToArrowTable,
} from "./index.js";

describe("Arrow output", () => {
  it("converts scalar rows with nulls into an Arrow table", () => {
    const table = rowsToArrowTable([
      { id: 1, label: "a", ok: true, amount: 12.5 },
      { id: 2, label: null, ok: false, amount: null },
    ]);

    expect(table.numRows).toBe(2);
    expect(table.numCols).toBe(4);
    expect(table.schema.fields.map((field) => field.name)).toEqual(["id", "label", "ok", "amount"]);
    expect(table.get(0)?.toJSON()).toEqual({ id: 1, label: "a", ok: true, amount: 12.5 });
    expect(table.get(1)?.toJSON()).toEqual({ id: 2, label: null, ok: false, amount: null });
  });

  it("projects requested columns for row conversion", () => {
    const table = rowsToArrowTable([{ id: 1, ignored: "x" }], { columns: ["id", "missing"] });

    expect(table.schema.fields.map((field) => field.name)).toEqual(["id", "missing"]);
    expect(table.get(0)?.toJSON()).toEqual({ id: 1, missing: null });
  });

  it("converts lakeql column batches without row materialization at the API boundary", () => {
    const batch = batchFromColumns({
      id: [1, 2, null],
      label: ["a", null, "c"],
      ok: [true, false, null],
      big: [1n, 2n, null],
    });

    const table = batchToArrowTable(batch);

    expect(table.numRows).toBe(3);
    expect(table.schema.fields.map((field) => `${field.name}:${field.type}`)).toEqual([
      "id:Float64",
      "label:Dictionary<Int32, Utf8>",
      "ok:Bool",
      "big:Int64",
    ]);
    expect(table.get(2)?.toJSON()).toEqual({ id: null, label: "c", ok: null, big: null });
  });

  it("converts query results to Arrow table and IPC payloads", async () => {
    const lake = createInMemoryLake({
      rows: [
        { id: 1, amount: 10 },
        { id: 2, amount: 30 },
        { id: 3, amount: 50 },
      ],
    });
    const query = lake.path("rows").select(["id", "amount"]).where(gt("amount", 20));

    const table = await queryToArrowTable(query);
    const ipc = await queryToArrowIPC(query);
    const restored = tableFromIPC(ipc);

    expect(table.numRows).toBe(2);
    expect(restored.numRows).toBe(2);
    expect(restored.get(0)?.toJSON()).toEqual({ id: 2, amount: 30 });
    expect(restored.get(1)?.toJSON()).toEqual({ id: 3, amount: 50 });
  });

  it("streams query batches as one Arrow IPC stream", async () => {
    const lake = createInMemoryLake({
      rows: [
        { id: 1, amount: 10, label: "a" },
        { id: 2, amount: 30, label: "b" },
        { id: 3, amount: 50, label: "c" },
      ],
    });
    const query = lake.path("rows").batchSize(1).select(["id", "label"]).where(gt("amount", 10));

    const restored = tableFromIPC(await streamBytes(queryToArrowIPCStream(query)));

    expect(restored.numRows).toBe(2);
    expect(restored.schema.fields.map((field) => field.name)).toEqual(["id", "label"]);
    expect(restored.get(0)?.toJSON()).toEqual({ id: 2, label: "b" });
    expect(restored.get(1)?.toJSON()).toEqual({ id: 3, label: "c" });
  });

  it("uses requested stream columns as the stable Arrow IPC schema", async () => {
    const lake = createInMemoryLake({
      rows: [{ id: 1 }, { id: 2, later: "x" }],
    });
    const query = lake.path("rows").batchSize(1);

    const restored = tableFromIPC(
      await streamBytes(queryToArrowIPCStream(query, { columns: ["id"] })),
    );

    expect(restored.schema.fields.map((field) => field.name)).toEqual(["id"]);
    expect(restored.get(1)?.toJSON()).toEqual({ id: 2 });
  });

  it("registers Arrow tables as queryable in-memory lake tables", async () => {
    const lake = createArrowLake(
      {
        sales: tableFromArrays({
          id: [1, 2, 3],
          amount: [10, 30, 50],
          region: ["west", "east", "west"],
        }),
      },
      { queryId: () => "arrow-ingest" },
    );

    const result = lake.path("sales").select(["id", "region"]).where(gt("amount", 20)).run();

    await expect(result.toArray()).resolves.toEqual([
      { id: 2, region: "east" },
      { id: 3, region: "west" },
    ]);
    expect(result.stats).toMatchObject({
      queryId: "arrow-ingest",
      filesPlanned: 1,
      rowsDecoded: 3,
      rowsMatched: 2,
      rowsReturned: 2,
    });
  });

  it("ingests Arrow IPC tables through the same scalar row boundary", () => {
    const ipc = rowsToArrowIPC([
      { id: 1, label: "a" },
      { id: 2, label: null },
    ]);

    expect(arrowTableToRows(tableFromIPC(ipc))).toEqual([
      { id: 1, label: "a" },
      { id: 2, label: null },
    ]);
  });

  it("applies in-memory ingest budgets to Arrow table registration", () => {
    expect(() =>
      createArrowLake(
        {
          rows: tableFromArrays({ id: [1, 2] }),
        },
        { maxRows: 1 },
      ),
    ).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_BUDGET_EXCEEDED",
        details: { metric: "ingest rows", limit: 1, actual: 2 },
      }),
    );
  });

  it("round-trips rows and batches through Arrow IPC", () => {
    const rowTable = tableFromIPC(rowsToArrowIPC([{ id: 1, label: "a" }]));
    const batchTable = tableFromIPC(batchToArrowIPC(batchFromColumns({ id: [1, 2] })));

    expect(rowTable.get(0)?.toJSON()).toEqual({ id: 1, label: "a" });
    expect(batchTable.get(1)?.toJSON()).toEqual({ id: 2 });
  });

  it("rejects unsupported nested row values instead of guessing Arrow types", () => {
    expect(() => rowsToArrowTable([{ id: 1, nested: { ok: true } }])).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_VALIDATION_ERROR",
        details: { rowIndex: 0, column: "nested", valueType: "object" },
      }),
    );
  });

  it("rejects unsupported nested Arrow table values instead of silently stringifying them", () => {
    expect(() => arrowTableToRows(tableFromArrays({ nested: [[1, 2]] }))).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_VALIDATION_ERROR",
        details: { rowIndex: 0, column: "nested", valueType: "object" },
      }),
    );
  });

  it("rejects unknown batch columns", () => {
    expect(() =>
      batchToArrowTable(batchFromColumns({ id: [1] }), { columns: ["missing"] }),
    ).toThrowError(
      expect.objectContaining({
        code: "LAKEQL_UNKNOWN_COLUMN",
        details: { column: "missing" },
      }),
    );
  });
});

async function streamBytes(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    total += value.byteLength;
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}
