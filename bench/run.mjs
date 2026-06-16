import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { eq, gt, memoryStore } from "../packages/core/dist/index.js";
import { loadIcebergTable, planFiles } from "../packages/iceberg/dist/index.js";
import { createParquetLake } from "../packages/parquet/dist/index.js";

const repoRoot = fileURLToPath(new URL("..", import.meta.url));
const fixtureRoot = join(repoRoot, "fixtures/data");
const reportPath = join(repoRoot, "bench/REPORT.md");

const SALES = {
  file: "sales.parquet",
};
const STATS = {
  file: "stats.parquet",
};
const HIVE = {
  files: [
    "hive/date=2026-01-01/country=US/part-000.parquet",
    "hive/date=2026-01-02/country=CA/part-000.parquet",
    "hive/date=2026-01-02/country=US/part-000.parquet",
  ],
};
const ICEBERG = {
  metadataFile: "iceberg/warehouse/places/metadata/v2.metadata.json",
  v1MetadataFile: "iceberg/warehouse/places/metadata/v1.metadata.json",
  manifestListFile: "iceberg/warehouse/places/metadata/snap-2.manifest-list.avro",
  manifestFiles: [
    "iceberg/warehouse/places/metadata/manifest-1.avro",
    "iceberg/warehouse/places/metadata/manifest-2-data.avro",
    "iceberg/warehouse/places/metadata/manifest-2-deletes.avro",
    "iceberg/warehouse/places/metadata/manifest-2-us.avro",
    "iceberg/warehouse/places/metadata/manifest-2-ca.avro",
  ],
  v1ManifestListFile: "iceberg/warehouse/places/metadata/snap-1.v1.manifest-list.avro",
  v1ManifestFile: "iceberg/warehouse/places/metadata/manifest-1.v1.avro",
};

const scenarios = [
  singleParquetCold,
  singleParquetWarmMetadata,
  hivePartitionedSelective,
  manySmallFiles,
  largeRowGroupsSelectivePredicate,
  icebergV1Plan,
  icebergV2PlanWithDeletes,
];

const results = [];
for (const scenario of scenarios) {
  results.push(await scenario());
}

await mkdir(dirname(reportPath), { recursive: true });
await writeFile(reportPath, renderReport(results));
console.log(`wrote ${reportPath}`);

async function singleParquetCold() {
  const { store, counters } = countingStore();
  await putFixture(store, SALES.file, SALES.file);
  const lake = createParquetLake({ store, queryId: () => "bench-parquet-cold" });
  const result = lake
    .path(SALES.file)
    .select(["store_id", "amount"])
    .where(eq("region", "west"))
    .run();
  const { rows, wallTimeMs, peakMemoryBytes } = await timed(() => result.toArray());
  return scenarioResult({
    name: "single Parquet cold",
    rows,
    stats: result.stats,
    counters,
    wallTimeMs,
    peakMemoryBytes,
    notes: "Sales fixture, selective predicate, no metadata cache.",
  });
}

async function singleParquetWarmMetadata() {
  const { store, counters } = countingStore();
  await putFixture(store, SALES.file, SALES.file);
  const metadataCache = new Map();
  const lake = createParquetLake({
    store,
    metadataCache: mapCache(metadataCache),
    queryId: () => "bench-parquet-warm",
  });
  await lake.path(SALES.file).where(eq("region", "west")).run().toArray();
  counters.reset();
  const result = lake.path(SALES.file).where(eq("region", "west")).run();
  const { rows, wallTimeMs, peakMemoryBytes } = await timed(() => result.toArray());
  return scenarioResult({
    name: "single Parquet warm metadata",
    rows,
    stats: result.stats,
    counters,
    wallTimeMs,
    peakMemoryBytes,
    notes: "Second read with Parquet metadata cache already populated.",
  });
}

async function hivePartitionedSelective() {
  const { store, counters } = countingStore();
  for (const file of HIVE.files) await putFixture(store, file, file);
  const lake = createParquetLake({ store, queryId: () => "bench-hive-selective" });
  const result = lake
    .hive("hive/**/*.parquet")
    .select(["id", "date", "country"])
    .where(eq("country", "US"))
    .run();
  const { rows, wallTimeMs, peakMemoryBytes } = await timed(() => result.toArray());
  return scenarioResult({
    name: "Hive partitioned selective",
    rows,
    stats: result.stats,
    counters,
    wallTimeMs,
    peakMemoryBytes,
    notes: "Three-file Hive layout with partition pruning on country.",
  });
}

async function manySmallFiles() {
  const { store, counters } = countingStore();
  for (let index = 0; index < 12; index++) {
    await putFixture(
      store,
      `many-small-files/batch=${String(index).padStart(3, "0")}/part-000.parquet`,
      SALES.file,
    );
  }
  const lake = createParquetLake({ store, queryId: () => "bench-many-small-files" });
  const result = lake
    .hive("many-small-files/**/*.parquet")
    .select(["store_id", "amount"])
    .where(eq("region", "west"))
    .run();
  const { rows, wallTimeMs, peakMemoryBytes } = await timed(() => result.toArray());
  return scenarioResult({
    name: "many small Parquet files",
    rows,
    stats: result.stats,
    counters,
    wallTimeMs,
    peakMemoryBytes,
    notes: "Twelve small Parquet objects sharing the sales schema.",
  });
}

async function largeRowGroupsSelectivePredicate() {
  const { store, counters } = countingStore();
  await putFixture(store, STATS.file, STATS.file);
  const lake = createParquetLake({ store, queryId: () => "bench-large-row-groups-selective" });
  const result = lake.path(STATS.file).select(["id", "metric"]).where(gt("metric", 199)).run();
  const { rows, wallTimeMs, peakMemoryBytes } = await timed(() => result.toArray());
  return scenarioResult({
    name: "large row groups selective predicate",
    rows,
    stats: result.stats,
    counters,
    wallTimeMs,
    peakMemoryBytes,
    notes: "Stats fixture with min/max row-group pruning on metric > 199.",
  });
}

async function icebergV1Plan() {
  const { store, counters } = countingStore();
  await putFixture(store, ICEBERG.v1MetadataFile, ICEBERG.v1MetadataFile);
  await putFixture(store, ICEBERG.v1ManifestListFile, ICEBERG.v1ManifestListFile);
  await putFixture(store, ICEBERG.v1ManifestFile, ICEBERG.v1ManifestFile);
  const {
    rows: plan,
    wallTimeMs,
    peakMemoryBytes,
  } = await timed(async () => {
    const table = await loadIcebergTable({ store, metadataPath: ICEBERG.v1MetadataFile });
    return planFiles(table, { where: eq("country", "US") });
  });
  return icebergScenarioResult({
    name: "Iceberg v1 plan",
    plan,
    counters,
    wallTimeMs,
    peakMemoryBytes,
    notes: "Metadata and manifest hydration plus data-file planning.",
  });
}

async function icebergV2PlanWithDeletes() {
  const { store, counters } = countingStore();
  await putFixture(store, ICEBERG.metadataFile, ICEBERG.metadataFile);
  await putFixture(store, ICEBERG.manifestListFile, ICEBERG.manifestListFile);
  for (const file of ICEBERG.manifestFiles) await putFixture(store, file, file);
  const {
    rows: plan,
    wallTimeMs,
    peakMemoryBytes,
  } = await timed(async () => {
    const table = await loadIcebergTable({ store, metadataPath: ICEBERG.metadataFile });
    return planFiles(table, { where: eq("country", "US"), readMode: "ignore-unsupported-deletes" });
  });
  return icebergScenarioResult({
    name: "Iceberg v2 plan with deletes",
    plan,
    counters,
    wallTimeMs,
    peakMemoryBytes,
    notes: "Current fixture metadata with delete manifests included in planning.",
  });
}

function scenarioResult({ name, rows, stats, counters, wallTimeMs, peakMemoryBytes, notes }) {
  return {
    name,
    wallTimeMs,
    rowsScanned: stats.rowsDecoded,
    rowsReturned: rows.length,
    filesRead: stats.filesRead,
    filesSkipped: stats.filesSkipped,
    rowGroupsRead: stats.rowGroupsRead,
    rowGroupsSkipped: stats.rowGroupsSkipped,
    objectRequests: counters.totalRequests,
    bytesFetched: counters.bytesFetched,
    rangeRequests: stats.rangeRequests,
    bytesRequested: stats.bytesRequested,
    peakMemoryBytes,
    notes,
  };
}

function icebergScenarioResult({ name, plan, counters, wallTimeMs, peakMemoryBytes, notes }) {
  return {
    name,
    wallTimeMs,
    rowsScanned: 0,
    rowsReturned: 0,
    filesRead: plan.filesPlanned,
    filesSkipped: plan.filesSkipped,
    rowGroupsRead: 0,
    rowGroupsSkipped: 0,
    objectRequests: counters.totalRequests,
    bytesFetched: counters.bytesFetched,
    rangeRequests: 0,
    bytesRequested: counters.bytesFetched,
    peakMemoryBytes,
    notes: `${notes} Planned ${plan.filesPlanned} data files; delete files planned ${plan.deleteFilesPlanned}.`,
  };
}

async function timed(fn) {
  let peakMemoryBytes = process.memoryUsage().rss;
  const sampleMemory = () => {
    peakMemoryBytes = Math.max(peakMemoryBytes, process.memoryUsage().rss);
  };
  const sampler = setInterval(sampleMemory, 1);
  sampler.unref();
  const start = performance.now();
  try {
    const rows = await fn();
    sampleMemory();
    return { rows, wallTimeMs: performance.now() - start, peakMemoryBytes };
  } finally {
    clearInterval(sampler);
  }
}

function countingStore() {
  const inner = memoryStore();
  const counters = {
    get: 0,
    getRange: 0,
    head: 0,
    list: 0,
    put: 0,
    delete: 0,
    bytesFetched: 0,
    get totalRequests() {
      return this.get + this.getRange + this.head + this.list + this.put + this.delete;
    },
    reset() {
      this.get = 0;
      this.getRange = 0;
      this.head = 0;
      this.list = 0;
      this.put = 0;
      this.delete = 0;
      this.bytesFetched = 0;
    },
  };
  return {
    counters,
    store: {
      async get(path) {
        counters.get += 1;
        const bytes = await inner.get(path);
        if (bytes !== null) counters.bytesFetched += bytes.byteLength;
        return bytes;
      },
      async getRange(path, range) {
        counters.getRange += 1;
        const bytes = await inner.getRange(path, range);
        counters.bytesFetched += bytes.byteLength;
        return bytes;
      },
      async put(path, body, options) {
        counters.put += 1;
        return await inner.put(path, body, options);
      },
      async delete(path) {
        counters.delete += 1;
        return await inner.delete(path);
      },
      async head(path) {
        counters.head += 1;
        return await inner.head(path);
      },
      async *list(prefix, options) {
        counters.list += 1;
        yield* inner.list(prefix, options);
      },
    },
  };
}

function mapCache(map) {
  return {
    async get(key) {
      return map.get(key);
    },
    async set(key, entry) {
      map.set(key, entry);
    },
    async delete(key) {
      map.delete(key);
    },
  };
}

async function putFixture(store, objectPath, fixturePath) {
  await store.put(objectPath, await readFile(join(fixtureRoot, fixturePath)));
}

function renderReport(rows) {
  const generatedAt = new Date().toISOString().slice(0, 10);
  const headers = [
    "Scenario",
    "Wall ms",
    "Object requests",
    "Bytes fetched",
    "Rows scanned",
    "Rows returned",
    "Files read",
    "Files skipped",
    "Row groups read",
    "Row groups skipped",
    "Peak memory",
  ];
  const tableRows = rows.map((row) => [
    row.name,
    row.wallTimeMs.toFixed(2),
    String(row.objectRequests),
    String(row.bytesFetched),
    String(row.rowsScanned),
    String(row.rowsReturned),
    String(row.filesRead),
    String(row.filesSkipped),
    String(row.rowGroupsRead),
    String(row.rowGroupsSkipped),
    formatBytes(row.peakMemoryBytes),
  ]);
  return `# Benchmark Report

Generated by \`pnpm bench\` on ${generatedAt}.

These are local fixture baselines, intended to catch request-count and pruning regressions. Wall
time is machine-dependent; request, byte, row, file, and row-group counts are the stable signals.
Peak memory is sampled process RSS during each scenario.

${markdownTable(headers, tableRows)}

## Notes

${rows.map((row) => `- **${row.name}:** ${row.notes}`).join("\n")}
`;
}

function markdownTable(headers, rows) {
  return [
    `| ${headers.join(" | ")} |`,
    `| ${headers.map(() => "---").join(" | ")} |`,
    ...rows.map((row) => `| ${row.join(" | ")} |`),
  ].join("\n");
}

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KiB", "MiB", "GiB"];
  let value = bytes / 1024;
  for (const unit of units) {
    if (value < 1024) return `${value.toFixed(1)} ${unit}`;
    value /= 1024;
  }
  return `${value.toFixed(1)} TiB`;
}
