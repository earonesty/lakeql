// Reproduce the extreme INT96 fixture using Spark 3.4.3's documented arithmetic.

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { DuckDBInstance } from "@duckdb/node-api";
import {
  MemoryBudget,
  MemoryScope,
  memoryStore,
  timestampEpochForUnit,
} from "../packages/core/dist/index.js";
import { lakeqlParquetCompressors } from "../packages/parquet/dist/compressors.js";
import { createParquetLake, readParquetObjects } from "../packages/parquet/dist/index.js";
import { preparePageWindow, readPreparedPageBody } from "../packages/parquet/dist/page-window.js";
import { asyncBufferFromStore } from "../packages/parquet/dist/store-buffer.js";
import { decompressPage } from "../packages/parquet/node_modules/hyparquet/src/datapage.js";
import { parquetMetadata } from "../packages/parquet/node_modules/hyparquet/src/metadata.js";

const fixture = "fixtures/external/parquet-testing/data/int96_from_spark.parquet";
const bytes = readFileSync(fixture);
const metadata = parquetMetadata(
  bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
);
const column = metadata.row_groups[0].columns[0].meta_data;
const store = memoryStore();
await store.put("fixture", bytes);
const file = await asyncBufferFromStore(store, "fixture");
const scope = new MemoryScope(new MemoryBudget(1024 * 1024));
const documented = [
  1704141296123456n,
  1704070800000000n,
  253402225200000000n,
  1735599600000000n,
  9089380393200000000n,
];
const records = [];
try {
  const start = Number(column.dictionary_page_offset);
  const prepared = await preparePageWindow(
    file,
    start,
    start + Number(column.total_compressed_size),
    0,
    scope,
  );
  if (prepared.header.type !== "DICTIONARY_PAGE") throw Error("Expected dictionary");
  const body = await readPreparedPageBody(file, prepared, scope);
  const payload = decompressPage(
    body,
    prepared.header.uncompressed_page_size,
    column.codec,
    lakeqlParquetCompressors,
  );
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  for (let i = 0; i < documented.length; i++) {
    const nanos = view.getBigInt64(i * 12, true);
    const days = BigInt(view.getInt32(i * 12 + 8, true));
    const julianMicros = BigInt.asIntN(64, documented[i] + 2440588n * 86400000000n);
    const encodedDays = julianMicros / 86400000000n;
    const encodedNanos = (julianMicros % 86400000000n) * 1000n;
    const recovered = BigInt.asIntN(64, (days - 2440588n) * 86400000000n + nanos / 1000n);
    if (days !== encodedDays || nanos !== encodedNanos || recovered !== documented[i])
      throw Error("Spark oracle mismatch");
    records.push({
      days: String(days),
      nanos: String(nanos),
      documentedMicros: String(documented[i]),
      recoveredMicros: String(recovered),
    });
  }
} finally {
  scope.dispose();
}
if (scope.budget.usedBytes !== 0) throw Error("Leaked fixture read");
const db = await DuckDBInstance.create();
const connection = await db.connect();
let version, duckdb;
try {
  version = (await connection.runAndReadAll("select version() as version")).getRowObjects()[0]
    .version;
  duckdb = (
    await connection.runAndReadAll(
      `select epoch_us(a)::varchar as micros from read_parquet('${fixture}')`,
    )
  ).getRowObjects();
} finally {
  connection.closeSync();
}
const expectedMicros = [...documented.slice(0, 4), null, documented[4]].map((value) =>
  value === null ? null : String(value),
);
const mismatches = (actual) => {
  assert.equal(actual.length, expectedMicros.length, "fixture row count");
  return actual.flatMap((value, index) =>
    value === expectedMicros[index]
      ? []
      : [{ row: index, expected: expectedMicros[index], actual: value }],
  );
};
const micros = (rows) =>
  rows.map(({ a }) => (a === null ? null : String(timestampEpochForUnit(a, "micros"))));
const publicMicros = micros(await readParquetObjects(store, "fixture"));
const pool = new MemoryBudget(64 * 1024 * 1024);
const lake = createParquetLake({ store, memoryPool: pool, scanRangeCache: { maxBytes: 0 } });
const owned = await lake.path("fixture").batchSize(2).run().toOwnedArray();
let ownedMicros;
try {
  ownedMicros = micros(owned.value);
  assert.deepEqual(
    owned.value.map(({ a }) => (a === null ? null : timestampEpochForUnit(a, "nanos"))),
    expectedMicros.map((value) => (value === null ? null : BigInt(value) * 1000n)),
    "owned timestamp nanoseconds must be exact, with no hidden sub-microsecond remainder",
  );
} finally {
  owned.release();
}
assert.equal(pool.usedBytes, 0, "owned output cleanup");
const comparisons = {
  public: { micros: publicMicros, mismatches: mismatches(publicMicros) },
  owned: { micros: ownedMicros, mismatches: mismatches(ownedMicros) },
  duckdb: {
    micros: duckdb.map(({ micros }) => micros),
    mismatches: mismatches(duckdb.map(({ micros }) => micros)),
  },
};
assert.deepEqual(
  comparisons.owned.mismatches,
  [],
  "owned reader must preserve documented microseconds",
);
const report = {
  fixture,
  sha256: createHash("sha256").update(bytes).digest("hex"),
  source:
    "https://raw.githubusercontent.com/apache/spark/v3.4.3/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/DateTimeUtils.scala",
  contract:
    "Signed int64 microsecond overflow in toJulianDay/fromJulianDay; signed nanos remainder. This reproduces writer arithmetic, not a Spark-runtime execution.",
  records,
  duckdbVersion: version,
  duckdb,
  expectedMicros,
  comparisons,
  ownedPeakAdmittedBytes: pool.peakBytes,
  ownedRemainingBytes: pool.usedBytes,
  conclusion: `Raw dictionary arithmetic matches all documented values. Row mismatches against documented microseconds: public=${comparisons.public.mismatches.length}, owned=${comparisons.owned.mismatches.length}, DuckDB=${comparisons.duckdb.mismatches.length}.`,
};
writeFileSync(
  "docs/benchmarks/int96-public-integrated-2026-11-13.json",
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(report.conclusion);
