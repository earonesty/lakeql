import { memoryCache, memoryStore } from "lakeql-core";
import { expect, it } from "vitest";
import { readParquetMetadata, writeParquet } from "./index.js";
import { encodedParquetMetadataCache } from "./metadata-cache.js";
import { parquetMetadataCacheBytes } from "./metadata-size.js";
import type { ParquetMetadata } from "./types.js";

async function actualSize(value: unknown): Promise<number> {
  const storage = memoryCache<Uint8Array>();
  await encodedParquetMetadataCache(storage).set("value", { value: value as ParquetMetadata });
  const entry = await storage.get("value");
  if (entry === undefined) throw new Error("Missing encoding");
  return entry.value.byteLength;
}

it("matches tagged encoding for Unicode, escapes, byte views, bigint, and omitted fields", async () => {
  const repeated = { value: '猫😀é\ud800\udfff"\\\n' };
  const values = [
    new Array(4),
    { repeated, again: repeated, missing: undefined, nope: () => 1, symbol: Symbol("x") },
    {
      bytes: Uint8Array.of(0, 255, 128).subarray(1),
      buffer: new ArrayBuffer(17),
      signed: -9223372036854775808n,
    },
    { array: [undefined, null, NaN, Infinity, -Infinity, -0, 1e30, 0.000001], date: new Date(0) },
    {
      toJSON(key: string) {
        return { key, value: 18446744073709551615n };
      },
    },
    { controls: Array.from({ length: 32 }, (_, i) => String.fromCharCode(i)).join("") },
  ];
  for (const value of values)
    expect(parquetMetadataCacheBytes(value)).toBe(await actualSize(value));
  for (const code of [
    0x7f, 0x80, 0x7ff, 0x800, 0xd7ff, 0xd800, 0xdbff, 0xdc00, 0xdfff, 0xe000, 0xffff,
  ]) {
    const value = { [String.fromCharCode(code)]: String.fromCharCode(code) };
    expect(parquetMetadataCacheBytes(value)).toBe(await actualSize(value));
  }
});

it("matches real multigroup Parquet metadata and rejects ancestor cycles", async () => {
  const store = memoryStore();
  await writeParquet(store, "metadata-size.parquet", {
    rowGroupSize: 2,
    columnData: [
      { name: "id", type: "INT64", data: [1n, 2n, 3n, 4n, 5n] },
      { name: "text", type: "STRING", data: ["猫", null, "", "é", "tail"] },
    ],
  });
  const metadata = await readParquetMetadata(store, "metadata-size.parquet");
  expect(parquetMetadataCacheBytes(metadata)).toBe(await actualSize(metadata));
  const cyclic: { self?: unknown } = {};
  cyclic.self = cyclic;
  expect(() => parquetMetadataCacheBytes(cyclic)).toThrow(/circular/);
  await expect(actualSize(cyclic)).rejects.toThrow();
});
