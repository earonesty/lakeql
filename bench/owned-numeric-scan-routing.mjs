import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import { MemoryBudget, materializeBatchRows, memoryStore } from "../packages/core/dist/index.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/index.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { readOwnedParquetVectorBatches } from "../packages/parquet/dist/owned-vector-batches.js";

const require = createRequire(new URL("../packages/parquet/package.json", import.meta.url));
const { parquetWriteBuffer } = await import(pathToFileURL(require.resolve("hyparquet-writer")));
const reports = [];
for (const codec of ["UNCOMPRESSED", "SNAPPY"]) {
  const count = 65536;
  const values = Array.from({ length: count }, (_, i) => i);
  const encoded = parquetWriteBuffer({
    codec,
    pageSize: 8192,
    columnData: [
      { name: "id", type: "INT32", encoding: "PLAIN", data: values },
      {
        name: "amount",
        type: "DOUBLE",
        encoding: "PLAIN",
        data: values.map((i) => (i % 3 ? i + 0.5 : null)),
      },
    ],
  });
  const store = memoryStore();
  await store.put("scan.parquet", new Uint8Array(encoded));
  const original = await asyncBufferFromStore(store, "scan.parquet");
  const metadata = await readParquetMetadataFromFile(original);
  for (const early of [false, true]) {
    const records = new Map(["previous", "automatic"].map((mode) => [mode, []]));
    for (let trial = 0; trial < 10; trial++) {
      // Alternate order to reduce systematic warmup/order bias.
      for (const mode of trial % 2 ? ["automatic", "previous"] : ["previous", "automatic"]) {
        let requests = 0,
          bytes = 0;
        const source = {
          ...original,
          ...(mode === "previous" ? { ownedReadAllocationBytes: undefined } : {}),
          async sliceOwned(start, end, scope) {
            requests++;
            bytes += end - start;
            return original.sliceOwned(start, end, scope);
          },
        };
        const budget = new MemoryBudget(64 * 1024 * 1024);
        let rows = 0;
        const begin = performance.now();
        for await (const owner of readOwnedParquetVectorBatches(source, metadata, budget, {
          columns: ["id", "amount"],
          batchSize: 1024,
          canStopEarly: early,
          rowEnd: early ? 1024 : count,
        })) {
          for (const row of materializeBatchRows(owner.value.batch)) {
            assert.equal(row.id, rows);
            assert.equal(row.amount, rows % 3 ? rows + 0.5 : null);
            rows++;
          }
          owner.release();
          if (early && rows >= 1024) break;
        }
        const elapsedMs = performance.now() - begin;
        assert.equal(rows, early ? 1024 : count);
        assert.equal(budget.usedBytes, 0);
        if (trial >= 2)
          records.get(mode).push({ elapsedMs, requests, bytes, peakBytes: budget.peakBytes });
      }
    }
    for (const [mode, samples] of records) {
      const times = samples.map((sample) => sample.elapsedMs).sort((a, b) => a - b);
      reports.push({
        codec,
        early,
        mode,
        rows: early ? 1024 : count,
        medianMs: (times[3] + times[4]) / 2,
        samples,
      });
    }
  }
}
console.log(
  JSON.stringify(
    {
      scope:
        "Local in-memory owned store; actual writer fixtures; includes row materialization/assertions, excludes metadata and source fixture allocations; previous path selected by omitting grouped transport capability; no cloud latency claim",
      reports,
    },
    null,
    2,
  ),
);
