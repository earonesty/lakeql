import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryBudget, MemoryScope, materializeBatchRows, SharedMemoryCache } from "lakeql-core";
import { expect, it } from "vitest";
import { fileCacheKey } from "./file-cache-key.js";
import {
  readCachedParquetMetadata,
  readOwnedParquetMetadata,
  sharedParquetMetadataCache,
} from "./metadata-cache.js";
import { cachedRangeBuffer } from "./range-cache.js";
import type { StoreAsyncBuffer } from "./types.js";
import { readParquetVectorBatchesFromFile } from "./vector-batches.js";

function fixture(name: string, value: number) {
  return parquetWriteBuffer({
    codec: "UNCOMPRESSED",
    columnData: [{ name, type: "INT32", encoding: "PLAIN", data: [value, value + 1] }],
  });
}
function source(bytes: ArrayBuffer, etag?: string, gate?: Promise<void>): StoreAsyncBuffer {
  return {
    byteLength: bytes.byteLength,
    ...(etag === undefined ? {} : { etag }),
    async slice(start, end = bytes.byteLength) {
      await gate;
      return bytes.slice(start, end);
    },
    async sliceOwned(start, end, scope) {
      return scope.allocateAsync(end - start, "fixture range", async () => {
        await gate;
        return bytes.slice(start, end);
      });
    },
  };
}

it.each([
  "borrowed",
  "owned",
  "pending",
] as const)("keeps distinct Parquet bytes across delimiter-bearing range identities (%s)", async (mode) => {
  const first = fixture("a", 11),
    second = fixture("a", 21);
  expect(first.byteLength).toBe(second.byteLength);
  const pool = new MemoryBudget(1024 * 1024);
  const scope = new MemoryScope(pool);
  const shared = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 1024 * 1024 });
  let finish!: () => void;
  const gate =
    mode === "pending"
      ? new Promise<void>((resolve) => {
          finish = resolve;
        })
      : undefined;
  const options = { maxBytes: 1024 * 1024, coalesceBytes: first.byteLength, sharedCache: shared };
  const a = cachedRangeBuffer(source(first, "c", gate), options, `a:${first.byteLength}:b`);
  const b = cachedRangeBuffer(source(second, `b:${first.byteLength}:c`, gate), options, "a");
  const read = async (file: StoreAsyncBuffer) => {
    if (mode === "borrowed") return file.slice(0, file.byteLength);
    const owner = await file.sliceOwned?.(0, file.byteLength, scope);
    if (owner === undefined) throw new Error("Missing owned fixture reader");
    try {
      return owner.value.slice(0);
    } finally {
      owner.release();
    }
  };
  try {
    if (mode === "pending") {
      const reads = Promise.all([read(a), read(b)]);
      finish();
      expect((await reads).map((bytes) => new Uint8Array(bytes))).toEqual([
        new Uint8Array(first),
        new Uint8Array(second),
      ]);
    } else {
      for (let repeat = 0; repeat < 2; repeat++) {
        expect(new Uint8Array(await read(a))).toEqual(new Uint8Array(first));
        expect(new Uint8Array(await read(b))).toEqual(new Uint8Array(second));
      }
    }
  } finally {
    scope.dispose();
    shared.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
});

it.each([
  false,
  true,
])("keeps metadata identity fields separate and returns the right real columns (owned=%s)", async (owned) => {
  const first = fixture("a", 11),
    second = fixture("b", 21);
  expect(first.byteLength).toBe(second.byteLength);
  const pool = new MemoryBudget(1024 * 1024);
  const shared = new SharedMemoryCache({ memoryBudget: pool, maxBytes: 1024 * 1024 });
  const cache = sharedParquetMetadataCache(shared);
  const inputs = [
    { file: source(first, "c"), path: `a:${first.byteLength}:b`, rows: [{ a: 11 }, { a: 12 }] },
    { file: source(second, `b:${first.byteLength}:c`), path: "a", rows: [{ b: 21 }, { b: 22 }] },
  ];
  try {
    for (let repeat = 0; repeat < 2; repeat++)
      for (const input of inputs) {
        const result = owned
          ? await readOwnedParquetMetadata(input.path, input.file, pool, cache)
          : await readCachedParquetMetadata(input.path, input.file, cache);
        try {
          const metadata = "owner" in result ? result.owner.value : result.metadata;
          const rows = [];
          for await (const batch of readParquetVectorBatchesFromFile(input.file, metadata, {
            columns: Object.keys(input.rows[0] ?? {}),
          }))
            rows.push(...materializeBatchRows(batch.batch));
          expect(rows).toEqual(input.rows);
          expect(result.cached).toBe(repeat > 0);
        } finally {
          if ("owner" in result) result.owner.release();
        }
      }
  } finally {
    shared.deleteWhere(() => true);
  }
  expect(pool.usedBytes).toBe(0);
});

it("separates absent, empty, sentinel and escaped file version tags", () => {
  for (const namespace of [
    "scan-range-v2",
    "owned-scan-range-v2",
    "parquet-metadata-v2",
  ] as const) {
    const tags = [undefined, "", "no-etag", "null", "a:b", "a\u0000b", "\ud800", "猫"];
    expect(new Set(tags.map((tag) => fileCacheKey(namespace, "path", 12, tag, 0, 4))).size).toBe(
      tags.length,
    );
    expect(fileCacheKey(namespace, "path", 12, "tag", 0, 4)).not.toBe(
      fileCacheKey(namespace, "path", 12, "tag", 0, 5),
    );
  }
});
