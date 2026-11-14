import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, MemoryScope, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { readParquetMetadataFromFile } from "./metadata-cache.js";
import { asyncBufferFromStore } from "./store-buffer.js";
import { preparePageWindow, readPreparedPageBody } from "./vector-batches.js";

it("prepares real page extents without retaining probes and loads under a separate scope", async () => {
  const bytes = parquetWriteBuffer({
    codec: "UNCOMPRESSED",
    columnData: [
      {
        name: "id",
        type: "INT32",
        encoding: "PLAIN",
        data: Array.from({ length: 2048 }, (_, i) => i),
      },
    ],
  });
  const store = memoryStore();
  await store.put("pages.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "pages.parquet");
  const metadata = await readParquetMetadataFromFile(source);
  const column = metadata.row_groups[0]?.columns[0]?.meta_data;
  if (column === undefined) throw new Error("Missing column");
  const start = Number(column.data_page_offset);
  const end = start + Number(column.total_compressed_size);
  const ranges: [number, number][] = [];
  const file = {
    ...source,
    async sliceOwned(...args: Parameters<NonNullable<typeof source.sliceOwned>>) {
      ranges.push([args[0], args[1]]);
      if (source.sliceOwned === undefined) throw new Error("Missing owned reads");
      return source.sliceOwned(...args);
    },
  };
  const inspectBudget = new MemoryBudget(256);
  const inspect = new MemoryScope(inspectBudget);
  const prepared = await preparePageWindow(file, start, end, 0, inspect);
  if (prepared === undefined) throw new Error("Missing page");
  expect(ranges).toEqual([[start, start + 256]]);
  expect(inspectBudget.usedBytes).toBe(0);
  inspect.dispose();
  const length = prepared.bodyEnd - prepared.bodyOffset;
  expect(length).toBeGreaterThan(256);
  const executionBudget = new MemoryBudget(length);
  const execution = new MemoryScope(executionBudget);
  const body = await readPreparedPageBody(file, prepared, execution);
  expect(body).toEqual(new Uint8Array(bytes.slice(prepared.bodyOffset, prepared.bodyEnd)));
  expect(ranges).toHaveLength(2);
  expect(executionBudget.usedBytes).toBe(length);
  execution.dispose();
  expect(executionBudget.usedBytes).toBe(0);

  const failureBudget = new MemoryBudget(length);
  const failure = new MemoryScope(failureBudget);
  const short = {
    ...source,
    async sliceOwned(_start: number, _end: number, scope: MemoryScope) {
      return scope.allocate(length - 1, "short response", () => new ArrayBuffer(length - 1));
    },
  };
  await expect(readPreparedPageBody(short, prepared, failure)).rejects.toMatchObject({
    code: "LAKEQL_PARQUET_READ_ERROR",
    details: { expected: length, actual: length - 1 },
  });
  expect(failureBudget.usedBytes).toBe(0);
  failure.dispose();
});

it("rejects invalid preparation extents and canceled borrowed reads before dispatch", async () => {
  let reads = 0;
  const file = {
    byteLength: 100,
    async slice() {
      reads++;
      return new ArrayBuffer(0);
    },
  };
  for (const [start, end, offset] of [
    [-1, 50, 0],
    [0, 101, 0],
    [50, 40, 0],
    [0, 50, -1],
    [0, 50, Number.NaN],
    [1, 50, Number.MAX_SAFE_INTEGER],
  ]) {
    await expect(preparePageWindow(file, start ?? 0, end ?? 0, offset ?? 0)).rejects.toMatchObject({
      code: "LAKEQL_PARQUET_READ_ERROR",
    });
  }
  const scope = new MemoryScope(new MemoryBudget(100));
  scope.dispose();
  await expect(preparePageWindow(file, 0, 100, 0, scope)).rejects.toMatchObject({
    code: "LAKEQL_ABORTED",
  });
  const prepared = {
    header: { type: "DATA_PAGE" as const, compressed_page_size: 4, uncompressed_page_size: 4 },
    headerBytes: 8,
    bodyOffset: 8,
    bodyEnd: 12,
  };
  await expect(readPreparedPageBody(file, prepared, scope)).rejects.toMatchObject({
    code: "LAKEQL_ABORTED",
  });
  for (const bounds of [
    { bodyOffset: -1, bodyEnd: 3 },
    { bodyOffset: 99, bodyEnd: 103 },
    { bodyOffset: 8, bodyEnd: 13 },
  ]) {
    await expect(readPreparedPageBody(file, { ...prepared, ...bounds })).rejects.toMatchObject({
      code: "LAKEQL_PARQUET_READ_ERROR",
    });
  }
  expect(reads).toBe(0);
});

it("rejects a deferred body arriving after its execution scope closes", async () => {
  let complete: ((bytes: ArrayBuffer) => void) | undefined;
  const pending = new Promise<ArrayBuffer>((resolve) => {
    complete = resolve;
  });
  const scope = new MemoryScope(new MemoryBudget(4));
  const read = readPreparedPageBody(
    { byteLength: 12, slice: () => pending },
    {
      header: { type: "DATA_PAGE", compressed_page_size: 4, uncompressed_page_size: 4 },
      headerBytes: 8,
      bodyOffset: 8,
      bodyEnd: 12,
    },
    scope,
  );
  const rejected = expect(read).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  scope.dispose();
  complete?.(new ArrayBuffer(4));
  await rejected;
  expect(scope.budget.usedBytes).toBe(0);
});
