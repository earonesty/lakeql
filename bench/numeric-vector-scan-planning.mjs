import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import { zstdCompressSync } from "node:zlib";
import { MemoryBudget, MemoryScope, materializeBatchRows } from "../packages/core/dist/index.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { readParquetVectorBatchesFromFile } from "../packages/parquet/dist/vector-batches.js";

const require = createRequire(new URL("../packages/parquet/package.json", import.meta.url));
const { parquetWriteBuffer } = await import(pathToFileURL(require.resolve("hyparquet-writer")));
const moduleUrl = new URL("../packages/parquet/dist/vector-batches.js", import.meta.url);
const source = await readFile(moduleUrl, "utf8");
const needle = "const plan = planFlatNumericVector(bytes, header, planningDecoder);";
assert.equal(source.split(needle).length, 2);
const baseline = source
  .replace(needle, "const plan = undefined;")
  .replace(
    /from "([^"]+)"/g,
    (_, spec) =>
      `from "${spec.startsWith(".") ? new URL(spec, moduleUrl).href : spec.startsWith("hyparquet/") ? new URL(`../packages/parquet/node_modules/${spec}`, import.meta.url).href : spec === "hyparquet" ? new URL("../packages/parquet/node_modules/hyparquet/src/index.js", import.meta.url).href : new URL("../packages/core/dist/index.js", import.meta.url).href}"`,
  );
const incremental = (
  await import(`data:text/javascript;base64,${Buffer.from(baseline).toString("base64")}`)
).readParquetVectorBatchesFromFile;
const sourceSha256 = createHash("sha256").update(source).digest("hex");
const memoryScopeSha256 = createHash("sha256")
  .update(await readFile(new URL("../packages/core/dist/memory-scope.js", import.meta.url)))
  .digest("hex");
const numericPageSha256 = createHash("sha256")
  .update(await readFile(new URL("../packages/parquet/dist/numeric-page.js", import.meta.url)))
  .digest("hex");
for (const codec of ["UNCOMPRESSED", "SNAPPY", "ZSTD"]) {
  for (const nullable of [false, true]) {
    const values = Array.from({ length: 65536 }, (_, i) => (nullable && i % 3 === 0 ? null : i));
    const encoded = parquetWriteBuffer({
      codec,
      compressors: { ZSTD: zstdCompressSync },
      pageSize: 8192,
      columnData: [{ name: "x", type: "INT32", encoding: "PLAIN", data: values }],
    });
    const file = {
      byteLength: encoded.byteLength,
      async slice(start, end) {
        return encoded.slice(start, end);
      },
    };
    const metadata = await readParquetMetadataFromFile(file);
    for (const batchSize of [64, 1024]) {
      for (const [mode, scan] of [
        ["incremental", incremental],
        ["planned", readParquetVectorBatchesFromFile],
      ]) {
        const times = [];
        let peakBytes;
        for (let repeat = 0; repeat < 8; repeat++) {
          const budget = new MemoryBudget(64 * 1024 * 1024);
          const scope = new MemoryScope(budget);
          let offset = 0;
          let elapsed = 0;
          const iterator = scan(file, metadata, {
            columns: ["x"],
            batchSize,
            allocationScope: scope,
            pageAllocationLifetime: "iteration",
          })[Symbol.asyncIterator]();
          try {
            for (;;) {
              const start = performance.now();
              const next = await iterator.next();
              elapsed += performance.now() - start;
              if (next.done) break;
              const rows = materializeBatchRows(next.value.batch).map((row) => row.x);
              assert.deepEqual(rows, values.slice(offset, offset + rows.length));
              offset += rows.length;
            }
            assert.equal(offset, values.length);
            peakBytes = budget.peakBytes;
          } finally {
            await iterator.return();
            scope.dispose();
          }
          assert.equal(budget.usedBytes, 0);
          if (repeat >= 3) times.push(elapsed);
        }
        times.sort((a, b) => a - b);
        console.log(
          JSON.stringify({
            codec,
            nullable,
            batchSize,
            mode,
            rows: values.length,
            medianMs: times[2],
            peakBytes,
            samples: times.length,
            sourceSha256,
            memoryScopeSha256,
            numericPageSha256,
            scope:
              "Generated local Parquet normal scan; baseline disables complete planning in current decoder; row verification outside timing; encoded input and JS heap excluded; not cloud",
          }),
        );
      }
    }
  }
}
