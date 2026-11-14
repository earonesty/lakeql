import { ByteWriter } from "hyparquet-writer/src/bytewriter.js";
import { writePageHeader } from "hyparquet-writer/src/datapage.js";
import { MemoryBudget, MemoryScope, memoryStore, SharedMemoryCache } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { cachedPageWindow } from "./cached-page-window.js";
import { DecodedColumnCache } from "./decoded-column-cache.js";
import { asyncBufferFromStore } from "./store-buffer.js";

async function fixture() {
  const writer = new ByteWriter();
  writePageHeader(writer, {
    type: "DATA_PAGE",
    compressed_page_size: 8,
    uncompressed_page_size: 8,
    data_page_header: {
      num_values: 2,
      encoding: "PLAIN",
      definition_level_encoding: "RLE",
      repetition_level_encoding: "RLE",
    },
  });
  writer.appendBytes(new Uint8Array(8));
  const store = memoryStore();
  await store.put("page", writer.getBytes());
  const file = await asyncBufferFromStore(store, "page");
  const read = file.sliceOwned?.bind(file);
  return { file, read, reads: vi.spyOn(file, "sliceOwned") };
}

it("reuses owned scalar page descriptors without retaining probe bytes", async () => {
  const { file, reads } = await fixture();
  const pool = new MemoryBudget(65536);
  const shared = new SharedMemoryCache({ maxBytes: 65536, memoryBudget: pool });
  const cache = { cache: new DecodedColumnCache(shared, { maxBytes: 65536 }), path: "page" };
  const first = new MemoryScope(pool.child());
  const extent = { start: 0, end: file.byteLength };
  const page = await cachedPageWindow(file, extent, 0, first, cache);
  expect(page?.header.data_page_header?.num_values).toBe(2);
  expect(page?.inlineBody).toBeUndefined();
  first.dispose();
  expect(pool.usedBytes).toBeGreaterThan(0);
  const second = new MemoryScope(pool.child());
  expect(await cachedPageWindow(file, extent, 0, second, cache)).toBe(page);
  expect(reads).toHaveBeenCalledTimes(1);
  shared.deleteWhere(() => true);
  expect(pool.usedBytes).toBeGreaterThan(0);
  expect(page?.bodyEnd).toBe(file.byteLength);
  second.dispose();
  expect(pool.usedBytes).toBe(0);
  const third = new MemoryScope(pool.child());
  await cachedPageWindow(file, extent, 0, third, cache);
  await cachedPageWindow({ ...file, etag: "changed" }, extent, 0, third, cache);
  expect(reads).toHaveBeenCalledTimes(3);
  third.dispose();
  shared.deleteWhere(() => true);
  expect(pool.usedBytes).toBe(0);
});

it("admits escaped cache keys and metadata before I/O and rolls back invalid extents", async () => {
  const { file, reads } = await fixture();
  const pool = new MemoryBudget(65536);
  const shared = new SharedMemoryCache({ maxBytes: 65536, memoryBudget: pool });
  const cache = {
    cache: new DecodedColumnCache(shared, { maxBytes: 65536 }),
    path: '\u0000"\\'.repeat(100),
  };
  const tiny = new MemoryScope(pool.child(1));
  await expect(
    cachedPageWindow(file, { start: 0, end: file.byteLength }, 0, tiny, cache),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(reads).not.toHaveBeenCalled();
  tiny.dispose();
  const scope = new MemoryScope(pool.child());
  await expect(
    cachedPageWindow(file, { start: 0, end: file.byteLength + 1 }, 0, scope, cache),
  ).rejects.toMatchObject({ code: "LAKEQL_PARQUET_READ_ERROR" });
  expect(pool.usedBytes).toBe(0);
  scope.dispose();
});

it("releases pending header admission when its scope closes during I/O", async () => {
  const { file, read, reads } = await fixture();
  if (!read) throw Error("Missing owned read");
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const shared = new SharedMemoryCache({ maxBytes: 65536, memoryBudget: pool });
  const cache = { cache: new DecodedColumnCache(shared, { maxBytes: 65536 }), path: "page" };
  reads.mockImplementation(async (...args) => {
    const result = await read(...args);
    scope.dispose();
    return result;
  });
  await expect(
    cachedPageWindow(file, { start: 0, end: file.byteLength }, 0, scope, cache),
  ).rejects.toMatchObject({ code: "LAKEQL_ABORTED" });
  expect(pool.usedBytes).toBe(0);
});
