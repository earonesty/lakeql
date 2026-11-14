import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DuckDBInstance } from "@duckdb/node-api";
import { MemoryBudget, memoryStore } from "../packages/core/dist/index.js";
import { createParquetLake } from "../packages/parquet/dist/index.js";

const mode = process.env.LAKEQL_DICTIONARY_QUERY_MODE ?? "materialize";
assert.ok(["materialize", "topk"].includes(mode));
const rows = 8192;
const cases = [];
for (const cardinality of [16, 1024, 8192]) {
  for (const batchSize of [128, 2048]) {
    const expected = Array.from({ length: rows }, (_, id) => ({
      id,
      text: id % 11 === 0 ? null : `${id % cardinality}:界`.padEnd(128, "x"),
    }));
    const directory = await mkdtemp(join(tmpdir(), "lakeql-dictionary-query-"));
    const path = join(directory, "fixture.parquet");
    const db = await DuckDBInstance.create();
    const connection = await db.connect();
    let bytes;
    try {
      await connection.run(`COPY (SELECT i::INTEGER AS id,
        CASE WHEN i % 11 = 0 THEN NULL ELSE rpad((i % ${cardinality})::VARCHAR || ':界', 128, 'x') END AS text
        FROM range(${rows}) t(i)) TO '${path.replaceAll("'", "''")}'
        (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 4096)`);
      bytes = await readFile(path);
    } finally {
      connection.closeSync();
      db.closeSync();
      await rm(directory, { recursive: true, force: true });
    }
    const store = memoryStore();
    await store.put("fixture.parquet", bytes);
    const pool = new MemoryBudget(64 * 1024 * 1024);
    const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
    let query = lake.path("fixture.parquet").batchSize(batchSize);
    let oracle = expected;
    if (mode === "topk") {
      query = query
        .orderBy([
          { column: "text", nulls: "last" },
          { column: "id", direction: "desc" },
        ])
        .offset(7)
        .limit(100);
      oracle = [...expected]
        .sort((a, b) => {
          if (a.text === null || b.text === null) {
            if (a.text !== b.text) return a.text === null ? 1 : -1;
          } else if (a.text !== b.text) return a.text < b.text ? -1 : 1;
          return b.id - a.id;
        })
        .slice(7, 107);
    }
    const start = performance.now();
    const output = await query.run().toOwnedArray();
    const elapsedMs = performance.now() - start;
    try {
      assert.deepEqual(output.value, oracle);
    } finally {
      output.release();
    }
    assert.equal(pool.usedBytes, 0);
    cases.push({
      cardinality,
      batchSize,
      rows,
      elapsedMs,
      peakBytes: pool.peakBytes,
      afterReleaseBytes: pool.usedBytes,
      fixtureSha256: createHash("sha256").update(bytes).digest("hex"),
    });
  }
}
console.log(
  JSON.stringify({
    scope:
      "Local real Parquet dictionary fixtures; output parity checked row by row. Fixture construction excluded from timing and admission. No cloud or total-heap claim.",
    node: process.version,
    mode,
    elapsedMs: cases.reduce((sum, x) => sum + x.elapsedMs, 0),
    cases,
  }),
);
