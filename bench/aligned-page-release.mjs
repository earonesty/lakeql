import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import { MemoryBudget, materializeBatchRows, memoryStore } from "../packages/core/dist/index.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { ParquetReadScope } from "../packages/parquet/dist/read-scope.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/store-buffer.js";
import { readParquetVectorBatchesFromFile } from "../packages/parquet/dist/vector-batches.js";

const require = createRequire(new URL("../packages/parquet/package.json", import.meta.url));
const { parquetWriteBuffer } = await import(pathToFileURL(require.resolve("hyparquet-writer")));
const moduleUrl = new URL("../packages/parquet/dist/vector-batches.js", import.meta.url);
const sourceCode = await readFile(moduleUrl, "utf8");
const needle = "current.releaseConsumedPage?.();";
assert.equal(sourceCode.split(needle).length, 2);
const baseline = sourceCode
  .replace(needle, "void 0;")
  .replace(
    /from "([^"]+)"/g,
    (_, spec) =>
      `from "${spec.startsWith(".") ? new URL(spec, moduleUrl).href : spec.startsWith("hyparquet/") ? new URL(`../packages/parquet/node_modules/${spec}`, import.meta.url).href : spec === "hyparquet" ? new URL("../packages/parquet/node_modules/hyparquet/src/index.js", import.meta.url).href : new URL("../packages/core/dist/index.js", import.meta.url).href}"`,
  );
const sequential = (
  await import(`data:text/javascript;base64,${Buffer.from(baseline).toString("base64")}`)
).readParquetVectorBatchesFromFile;
for (const unequal of [false, true])
  for (const window of [false, true]) {
    const rows = Array.from({ length: 1024 }, (_, i) => ({
      a: i * 17,
      b: unequal && i % 3 === 0 ? null : i * 31,
    }));
    const bytes = parquetWriteBuffer({
      codec: "SNAPPY",
      pageSize: 64,
      columnData: [
        { name: "a", type: "INT32", encoding: "PLAIN", data: rows.map((row) => row.a) },
        {
          name: "b",
          type: unequal ? "DOUBLE" : "INT32",
          encoding: "PLAIN",
          data: rows.map((row) => row.b),
        },
      ],
    });
    const store = memoryStore();
    await store.put("aligned.parquet", new Uint8Array(bytes));
    const source = await asyncBufferFromStore(store, "aligned.parquet");
    const metadata = await readParquetMetadataFromFile(source);
    for (const [mode, scan] of [
      ["sequential-release", sequential],
      ["release-consumed-peers", readParquetVectorBatchesFromFile],
    ]) {
      const budget = new MemoryBudget(1 << 20);
      const chargeBeforeReads = [];
      const traced = {
        ...source,
        sliceOwned(...args) {
          chargeBeforeReads.push(budget.usedBytes);
          return source.sliceOwned(...args);
        },
      };
      const scope = new ParquetReadScope(traced, budget);
      const output = [];
      try {
        for await (const result of scan(scope.file, metadata, {
          columns: ["a", "b"],
          batchSize: 13,
          canStopEarly: window,
          allocationScope: scope.allocations,
          pageAllocationLifetime: "iteration",
        }))
          output.push(...materializeBatchRows(result.batch));
        assert.deepEqual(output, rows);
      } finally {
        scope.dispose();
      }
      assert.equal(budget.usedBytes, 0);
      console.log(
        JSON.stringify({
          unequal,
          window,
          mode,
          rows: rows.length,
          peakBytes: budget.peakBytes,
          readCount: chargeBeforeReads.length,
          meanChargeBeforeRead:
            chargeBeforeReads.reduce((a, b) => a + b, 0) / chargeBeforeReads.length,
          scope:
            "Generated local Snappy files; encoded reads and decode backing charged; baseline disables early peer release; no remote or whole-heap claim",
        }),
      );
    }
  }
