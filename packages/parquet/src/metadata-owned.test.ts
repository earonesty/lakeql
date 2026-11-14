import { MemoryBudget, MemoryScope, memoryStore, SharedMemoryCache } from "lakeql-core";
import { expect, it } from "vitest";
import { writeParquet } from "./index.js";
import {
  readOwnedParquetMetadata,
  readParquetFooterBuffer,
  sharedParquetMetadataCache,
} from "./metadata-cache.js";
import { asyncBufferFromStore } from "./store-buffer.js";

it("pins cached footer backing for each metadata consumer and survives eviction", async () => {
  const store = memoryStore();
  await writeParquet(store, "metadata.parquet", {
    columnData: [{ name: "id", type: "INT32", data: [1, 2, 3] }],
  });
  const source = await asyncBufferFromStore(store, "metadata.parquet");
  let reads = 0;
  const file = {
    ...source,
    sliceOwned: async (...args: Parameters<NonNullable<typeof source.sliceOwned>>) => {
      reads++;
      if (source.sliceOwned === undefined) throw new Error("Missing owned read");
      return source.sliceOwned(...args);
    },
  };
  const root = new MemoryBudget(65536);
  const cache = new SharedMemoryCache({ memoryBudget: root, maxBytes: 65536 });
  const metadataCache = sharedParquetMetadataCache(cache);
  const firstBudget = root.child();
  const first = await readOwnedParquetMetadata(
    "metadata.parquet",
    file,
    firstBudget,
    metadataCache,
  );
  expect(first.cached).toBe(false);
  expect(first.owner.value.num_rows).toBe(3n);
  const backingBytes = first.owner.bytes;
  expect(backingBytes).toBeGreaterThan(0);
  first.owner.release();
  expect(firstBudget.usedBytes).toBe(0);
  expect(root.usedBytes).toBe(backingBytes);
  const secondBudget = root.child();
  const second = await readOwnedParquetMetadata(
    "metadata.parquet",
    file,
    secondBudget,
    metadataCache,
  );
  expect(second.cached).toBe(true);
  expect(reads).toBe(1);
  expect(secondBudget.usedBytes).toBe(backingBytes);
  cache.deleteWhere(() => true);
  expect(root.usedBytes).toBe(backingBytes);
  expect(second.owner.value.schema[1]?.name).toBe("id");
  second.owner.release();
  expect(root.usedBytes).toBe(0);
  const tiny = new MemoryBudget(1);
  await expect(
    readOwnedParquetMetadata("metadata.parquet", file, tiny, undefined),
  ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
  expect(tiny.usedBytes).toBe(0);
});

it("declines metadata caching when backing fits but parsed-object eviction cost does not", async () => {
  const store = memoryStore();
  await writeParquet(store, "capacity.parquet", {
    columnData: [{ name: "id", type: "INT32", data: [1, 2, 3] }],
  });
  const file = await asyncBufferFromStore(store, "capacity.parquet");
  const root = new MemoryBudget(65536);
  const cache = new SharedMemoryCache({ memoryBudget: root, maxBytes: file.byteLength });
  const adapter = sharedParquetMetadataCache(cache);
  for (let attempt = 0; attempt < 2; attempt++) {
    const result = await readOwnedParquetMetadata("capacity.parquet", file, root.child(), adapter);
    expect(result.cached).toBe(false);
    expect(result.owner.value.num_rows).toBe(3n);
    result.owner.release();
    expect(root.usedBytes).toBe(0);
  }
});

it("chooses charged footer assembly or bounded refetch from available headroom", async () => {
  const { parquetWriteBuffer } = await import("hyparquet-writer");
  const payload = "metadata-value-".repeat(6000);
  const bytes = parquetWriteBuffer({
    columnData: [{ name: "id", type: "INT32", data: [1, 2] }],
    kvMetadata: [{ key: "large", value: payload }],
  });
  const footerBytes = new DataView(bytes).getUint32(bytes.byteLength - 8, true) + 8;
  expect(footerBytes).toBeGreaterThan(65536);
  const store = memoryStore();
  await store.put("large-footer.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "large-footer.parquet");
  for (const cap of [footerBytes * 2, footerBytes, footerBytes - 1]) {
    const budget = new MemoryBudget(cap);
    const reads: [number, number][] = [];
    const file = {
      ...source,
      async sliceOwned(...args: Parameters<NonNullable<typeof source.sliceOwned>>) {
        reads.push([args[0], args[1]]);
        if (!source.sliceOwned) throw new Error("Missing owned read");
        return source.sliceOwned(...args);
      },
    };
    const scope = new MemoryScope(budget);
    if (cap < footerBytes) {
      await expect(readParquetFooterBuffer(file, scope)).rejects.toMatchObject({
        code: "LAKEQL_BUDGET_EXCEEDED",
      });
    } else {
      const footer = await readParquetFooterBuffer(file, scope);
      expect(new TextDecoder().decode(footer)).toContain(payload);
      expect(budget.usedBytes).toBe(footerBytes);
      expect(budget.peakBytes).toBe(cap);
      expect(reads).toHaveLength(2);
      expect(reads[1]).toEqual([
        bytes.byteLength - footerBytes,
        cap === footerBytes ? bytes.byteLength : bytes.byteLength - 65536,
      ]);
    }
    scope.dispose();
    expect(budget.usedBytes).toBe(0);
  }
});

it("sizes the initial footer probe to tight owned headroom", async () => {
  const { parquetWriteBuffer } = await import("hyparquet-writer");
  const bytes = parquetWriteBuffer({
    codec: "UNCOMPRESSED",
    columnData: [{ name: "id", type: "INT32", data: Array.from({ length: 100000 }, (_, i) => i) }],
  });
  expect(bytes.byteLength).toBeGreaterThan(65536);
  const store = memoryStore();
  await store.put("small-footer.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "small-footer.parquet");
  const budget = new MemoryBudget(1024);
  const scope = new MemoryScope(budget);
  const footer = await readParquetFooterBuffer(source, scope);
  expect(footer.byteLength).toBe(1024);
  expect(budget.peakBytes).toBeLessThanOrEqual(1024);
  scope.dispose();
  expect(budget.usedBytes).toBe(0);
});

it("releases footer backing on malformed metadata and failed continuation reads", async () => {
  const { parquetWriteBuffer } = await import("hyparquet-writer");
  const valid = parquetWriteBuffer({
    columnData: [{ name: "id", type: "INT32", data: [1, 2] }],
    kvMetadata: [{ key: "large", value: "x".repeat(80000) }],
  });
  for (const failure of ["magic", "length", "prefix", "refetch"] as const) {
    const bytes = valid.slice(0);
    const view = new DataView(bytes);
    if (failure === "magic") view.setUint32(bytes.byteLength - 4, 0, true);
    if (failure === "length") view.setUint32(bytes.byteLength - 8, bytes.byteLength, true);
    const store = memoryStore();
    await store.put("failure.parquet", new Uint8Array(bytes));
    const source = await asyncBufferFromStore(store, "failure.parquet");
    const budget = new MemoryBudget(
      failure === "refetch" ? bytes.byteLength : bytes.byteLength * 2,
    );
    let reads = 0;
    const file = {
      ...source,
      async sliceOwned(...args: Parameters<NonNullable<typeof source.sliceOwned>>) {
        reads++;
        if (reads === 2) throw new Error("Injected continuation failure");
        if (!source.sliceOwned) throw new Error("Missing owned read");
        return source.sliceOwned(...args);
      },
    };
    await expect(
      readOwnedParquetMetadata("failure.parquet", file, budget, undefined),
    ).rejects.toThrow(
      failure === "magic" ? /magic/ : failure === "length" ? /length/ : /continuation/,
    );
    expect(reads).toBe(failure === "magic" || failure === "length" ? 1 : 2);
    expect(budget.usedBytes).toBe(0);
    expect(budget.peakBytes).toBeLessThanOrEqual(budget.maxBytes);
  }
});

it("rejects hostile footer structures before object decoding and releases backing", async () => {
  const { readParquetMetadataFromFile } = await import("./metadata-cache.js");
  for (const payload of [
    [0x19, 0xfc, 0xff, 0xff, 0xff, 0xff, 7],
    [0x18, 0xff, 0xff, 0xff, 0xff, 7],
    [...Array(130).fill(0x1c), ...Array(131).fill(0)],
    [0, 0],
  ]) {
    const bytes = new Uint8Array(payload.length + 8);
    bytes.set(payload);
    new DataView(bytes.buffer).setUint32(payload.length, payload.length, true);
    bytes.set([80, 65, 82, 49], payload.length + 4);
    const store = memoryStore();
    await store.put("hostile.parquet", bytes);
    const source = await asyncBufferFromStore(store, "hostile.parquet");
    const budget = new MemoryBudget(4096);
    for (const read of [
      () => readParquetMetadataFromFile(source),
      () => readOwnedParquetMetadata("hostile.parquet", source, budget, undefined),
    ])
      await expect(read()).rejects.toMatchObject({ code: "LAKEQL_PARQUET_READ_ERROR" });
    expect(budget.usedBytes).toBe(0);
    expect(budget.peakBytes).toBeLessThanOrEqual(2 * bytes.length);
  }
});

it("retries exact footer backing when decoded metadata outgrows speculative headroom", async () => {
  const { parquetWriteBuffer } = await import("hyparquet-writer");
  const { decodeOwnedFullMetadata } = await import("./owned-full-metadata.js");
  const bytes = parquetWriteBuffer({
    codec: "UNCOMPRESSED",
    columnData: [{ name: "id", type: "INT32", data: Array.from({ length: 100000 }, (_, i) => i) }],
  });
  const footerBytes = new DataView(bytes).getUint32(bytes.byteLength - 8, true) + 8;
  const measurement = new MemoryScope(new MemoryBudget(Number.MAX_SAFE_INTEGER));
  const expected = decodeOwnedFullMetadata(bytes, measurement);
  const capacity = footerBytes + measurement.budget.peakBytes;
  measurement.dispose();
  expect(capacity).toBeLessThan(65536);
  const store = memoryStore();
  await store.put("retry.parquet", new Uint8Array(bytes));
  const source = await asyncBufferFromStore(store, "retry.parquet");
  for (const cap of [capacity, capacity - 1]) {
    const budget = new MemoryBudget(cap);
    const reads: [number, number][] = [];
    const file = {
      ...source,
      async sliceOwned(...args: Parameters<NonNullable<typeof source.sliceOwned>>) {
        reads.push([args[0], args[1]]);
        if (!source.sliceOwned) throw new Error("Missing owned read");
        return source.sliceOwned(...args);
      },
    };
    if (cap === capacity) {
      const result = await readOwnedParquetMetadata("retry.parquet", file, budget, undefined);
      expect(result.owner.value).toEqual(expected);
      expect(result.owner.bytes).toBeGreaterThan(footerBytes);
      result.owner.release();
    } else {
      await expect(
        readOwnedParquetMetadata("retry.parquet", file, budget, undefined),
      ).rejects.toMatchObject({ code: "LAKEQL_BUDGET_EXCEEDED" });
    }
    expect(reads).toEqual([
      [bytes.byteLength - cap, bytes.byteLength],
      [bytes.byteLength - footerBytes, bytes.byteLength],
    ]);
    expect(budget.peakBytes).toBeLessThanOrEqual(cap);
    expect(budget.usedBytes).toBe(0);
  }
});
