import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { memoryCache, memoryStore } from "../packages/core/dist/index.js";
import { readParquetMetadata, writeParquet } from "../packages/parquet/dist/index.js";
import { encodedParquetMetadataCache } from "../packages/parquet/dist/metadata-cache.js";
import { parquetMetadataCacheBytes } from "../packages/parquet/dist/metadata-size.js";

const store = memoryStore();
await writeParquet(store, "sizing.parquet", {
  columnData: [
    { name: "id", type: "INT64", data: [1n, 2n] },
    { name: "text", type: "STRING", data: ["猫", "é"] },
  ],
});
const base = await readParquetMetadata(store, "sizing.parquet");
for (const groups of [100, 1000]) {
  // Scale a real parsed row-group shape; this is a sizing kernel, not file decoding.
  const metadata = {
    ...base,
    row_groups: Array.from({ length: groups }, () => base.row_groups[0]),
  };
  const storage = memoryCache();
  const encoded = encodedParquetMetadataCache(storage);
  const baseline = async () => {
    await encoded.set("size", { value: metadata });
    return (await storage.get("size")).value.byteLength;
  };
  const size = await baseline();
  assert.equal(parquetMetadataCacheBytes(metadata), size);
  const measure = async (run) => {
    for (let i = 0; i < 3; i++) await run();
    const timings = [];
    for (let i = 0; i < 7; i++) {
      const start = performance.now();
      assert.equal(await run(), size);
      timings.push(performance.now() - start);
    }
    return timings.sort((a, b) => a - b)[3];
  };
  console.log(
    JSON.stringify({
      groups,
      encodedBytes: size,
      serializeMs: await measure(baseline),
      measureMs: await measure(() => parquetMetadataCacheBytes(metadata)),
    }),
  );
}
