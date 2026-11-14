import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { Lake, MemoryBudget, memoryStore } from "../packages/core/dist/index.js";

const results = [];
for (const groups of [100, 1000]) {
  const rows = Array.from({ length: groups * 4 }, (_, i) => ({ key: i % groups, value: i }));
  const expected = Array.from({ length: groups }, (_, key) => ({
    key,
    n: 4,
    median: key + 1.5 * groups,
  }));
  const spec = { n: { op: "count" }, median: { op: "median", column: "value" } };
  const variants = { "rows-only": [], "checkpoint-and-rows": [] };
  for (let round = 0; round < 10; round++) {
    const order = Object.keys(variants);
    if (round % 2) order.reverse();
    for (const variant of order) {
      const pool = new MemoryBudget(64 * 1024 * 1024);
      const store = memoryStore();
      await store.put("rows", new Uint8Array([1]));
      const lake = new Lake({
        store,
        memoryPool: pool,
        scanner: {
          async *scan() {
            for (let start = 0; start < rows.length; start += 128)
              yield rows.slice(start, start + 128);
          },
        },
      });
      const query = lake.path("rows").run();
      const start = performance.now();
      const result =
        variant === "rows-only"
          ? await query.aggregate(["key"], spec)
          : await query.aggregateWithState(["key"], spec);
      const elapsedMs = performance.now() - start;
      assert.deepEqual(Array.isArray(result) ? result : result.rows, expected);
      assert.equal(pool.usedBytes, 0);
      if (round >= 4)
        variants[variant].push({
          elapsedMs,
          peakChargedBytes: pool.peakBytes,
          checkpointBytes: Array.isArray(result) ? 0 : result.operatorState.byteLength,
        });
    }
  }
  results.push({ groups, rows: rows.length, batchRows: 128, variants });
}
const report = {
  baseCommit: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  node: process.version,
  builtQuerySha256: createHash("sha256")
    .update(await readFile(new URL("../packages/core/dist/query.js", import.meta.url)))
    .digest("hex"),
  conditions: { warmupRounds: 4, measuredRounds: 6, alternatingOrder: true },
  results,
  limitations: [
    "Current rows-only versus explicit checkpoint finalization; not a full historical-revision comparison.",
    "Borrowed in-memory row scanner; HTTP and native-cloud latency are outside this benchmark.",
    "Input/oracle fixtures are caller-owned; charged peak is not whole-runtime heap.",
  ],
};
const json = `${JSON.stringify(report, null, 2)}\n`;
if (process.env.LAKEQL_ROW_AGGREGATE_REPORT)
  await writeFile(process.env.LAKEQL_ROW_AGGREGATE_REPORT, json);
else process.stdout.write(json);
