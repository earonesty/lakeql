import { ByteWriter } from "hyparquet-writer/src/bytewriter.js";
import { writePageHeader } from "hyparquet-writer/src/datapage.js";
import { MemoryBudget, MemoryScope, memoryStore } from "lakeql-core";
import { expect, it, vi } from "vitest";
import { preparePageWindow, readPreparedPageBody } from "./page-window.js";
import { asyncBufferFromStore } from "./store-buffer.js";

async function fixture(size: number) {
  const body = Uint8Array.from({ length: size }, (_, i) => i % 251);
  const writer = new ByteWriter();
  writePageHeader(writer, {
    type: "DATA_PAGE",
    compressed_page_size: size,
    uncompressed_page_size: size,
    data_page_header: {
      num_values: size / 4,
      encoding: "PLAIN",
      definition_level_encoding: "RLE",
      repetition_level_encoding: "RLE",
    },
  });
  writer.appendBytes(body);
  const store = memoryStore();
  await store.put("page", writer.getBytes());
  const file = await asyncBufferFromStore(store, "page");
  const reads = vi.spyOn(file, "sliceOwned");
  return { body, file, reads };
}

it.each([8, 512])("reuses only complete owned probe bodies (body bytes=%s)", async (size) => {
  const { body, file, reads } = await fixture(size);
  const scope = new MemoryScope(new MemoryBudget(4096));
  try {
    const prepared = await preparePageWindow(file, 0, file.byteLength, 0, scope, true);
    if (!prepared) throw Error("Missing page");
    expect(await readPreparedPageBody(file, prepared, scope)).toEqual(body);
    expect(reads).toHaveBeenCalledTimes(size === 8 ? 1 : 2);
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it("declines optional probe retention when descriptor headroom is unavailable", async () => {
  const { body, file, reads } = await fixture(8);
  const scope = new MemoryScope(new MemoryBudget(file.byteLength + 16));
  try {
    const prepared = await preparePageWindow(file, 0, file.byteLength, 0, scope, true);
    if (!prepared) throw Error("Missing page");
    expect(prepared.inlineBody).toBeUndefined();
    expect(scope.budget.usedBytes).toBe(0);
    expect(await readPreparedPageBody(file, prepared, scope)).toEqual(body);
    expect(reads).toHaveBeenCalledTimes(2);
  } finally {
    scope.dispose();
  }
  expect(scope.budget.usedBytes).toBe(0);
});

it("does not reuse borrowed probe storage across execution scopes", async () => {
  const { body, file, reads } = await fixture(8);
  const planning = new MemoryScope(new MemoryBudget(4096));
  const prepared = await preparePageWindow(file, 0, file.byteLength, 0, planning, true);
  if (!prepared) throw Error("Missing page");
  expect(prepared.inlineBody).toBeDefined();
  planning.dispose();
  const execution = new MemoryScope(new MemoryBudget(4096));
  try {
    expect(await readPreparedPageBody(file, prepared, execution)).toEqual(body);
    expect(reads).toHaveBeenCalledTimes(2);
  } finally {
    execution.dispose();
  }
  expect(execution.budget.usedBytes).toBe(0);
});
