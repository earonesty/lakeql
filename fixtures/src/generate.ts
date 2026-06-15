// Deterministic fixture generation: same input, same bytes, no clock, no RNG.
// Run via `pnpm fixtures` (root) or `pnpm generate` (this package).
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { parquetWriteFile } from "hyparquet-writer";
import {
  fixtureDataDir,
  fixturePath,
  GEO,
  GROUPBY,
  H3,
  HIVE,
  ICEBERG,
  MANIFESTS,
  SALES,
  STATS,
  TYPES,
  WIDE,
  WRITE,
} from "./index.ts";

mkdirSync(fixtureDataDir, { recursive: true });

function generateSales() {
  const n = SALES.rows;
  const storeId: string[] = [];
  const date: string[] = [];
  const amount: number[] = [];
  const region: string[] = [];

  for (let i = 0; i < n; i++) {
    storeId.push(`store-${String(i % 7).padStart(3, "0")}`);
    date.push(`2026-01-${String((i % 28) + 1).padStart(2, "0")}`);
    amount.push(((i * 37) % 1000) + i / 100);
    region.push(SALES.regions[i % SALES.regions.length] as string);
  }

  parquetWriteFile({
    filename: fixturePath(SALES.file),
    rowGroupSize: [SALES.rowGroupSize],
    columnData: [
      { name: "store_id", data: storeId, type: "STRING" },
      { name: "date", data: date, type: "STRING" },
      { name: "amount", data: amount, type: "DOUBLE" },
      { name: "region", data: region, type: "STRING" },
    ],
  });
}

function generateTypes() {
  const n = TYPES.rows;
  const id: number[] = [];
  const big: bigint[] = [];
  const flag: boolean[] = [];
  const name: (string | null)[] = [];
  const score: number[] = [];

  for (let i = 0; i < n; i++) {
    id.push(i);
    big.push(9007199254740991n + BigInt(i)); // crosses MAX_SAFE_INTEGER
    flag.push(i % 2 === 0);
    name.push(i % 3 === 0 ? null : `name-${i}`);
    score.push(i * 1.5);
  }

  parquetWriteFile({
    filename: fixturePath(TYPES.file),
    columnData: [
      { name: "id", data: id, type: "INT32" },
      { name: "big", data: big, type: "INT64" },
      { name: "flag", data: flag, type: "BOOLEAN" },
      { name: "name", data: name, type: "STRING", nullable: true },
      { name: "score", data: score, type: "DOUBLE" },
    ],
  });
}

function generateWide() {
  const columnData: { name: string; data: number[]; type: "INT32" }[] = [];
  for (let c = 0; c < WIDE.columns; c++) {
    const data: number[] = [];
    for (let row = 0; row < WIDE.rows; row++) data.push(c * 1000 + row);
    columnData.push({ name: `c${String(c).padStart(2, "0")}`, data, type: "INT32" });
  }

  parquetWriteFile({
    filename: fixturePath(WIDE.file),
    columnData,
  });
}

function generateStats() {
  const id: number[] = [];
  const metric: number[] = [];
  const label: string[] = [];

  for (let group = 0; group < 3; group++) {
    for (let offset = 0; offset < STATS.rowGroupSize; offset++) {
      const value = group * 100 + offset;
      id.push(group * STATS.rowGroupSize + offset);
      metric.push(value);
      label.push(`g${group}`);
    }
  }

  parquetWriteFile({
    filename: fixturePath(STATS.file),
    rowGroupSize: [STATS.rowGroupSize],
    columnData: [
      { name: "id", data: id, type: "INT32" },
      { name: "metric", data: metric, type: "INT32" },
      { name: "label", data: label, type: "STRING" },
    ],
  });
}

function generateGroupby() {
  parquetWriteFile({
    filename: fixturePath(GROUPBY.file),
    rowGroupSize: [4],
    columnData: [
      {
        name: "region",
        data: ["west", "west", "east", "east", "north", "north", "south", "south"],
        type: "STRING",
      },
      { name: "amount", data: [10, 20, 7, 13, 5, 15, 2, 8], type: "INT32" },
      { name: "id", data: [1, 2, 3, 4, 5, 6, 7, 8], type: "INT32" },
      { name: "label", data: ["w1", "w2", "e1", "e2", "n1", "n2", "s1", "s2"], type: "STRING" },
    ],
  });
}

function generateGeo() {
  parquetWriteFile({
    filename: fixturePath(GEO.file),
    columnData: [
      { name: "id", data: [1, 2, 3], type: "INT32" },
      { name: "name", data: ["downtown", "valley", "harbor"], type: "STRING" },
      {
        name: "geom",
        data: [
          JSON.stringify({ type: "Point", coordinates: [-118.24, 34.05] }),
          JSON.stringify({ type: "Point", coordinates: [-118.45, 34.18] }),
          JSON.stringify({ type: "Point", coordinates: [-117.16, 32.72] }),
        ],
        type: "STRING",
      },
      { name: "minx", data: [-118.24, -118.45, -117.16], type: "DOUBLE" },
      { name: "miny", data: [34.05, 34.18, 32.72], type: "DOUBLE" },
      { name: "maxx", data: [-118.24, -118.45, -117.16], type: "DOUBLE" },
      { name: "maxy", data: [34.05, 34.18, 32.72], type: "DOUBLE" },
    ],
  });
}

function generateH3() {
  parquetWriteFile({
    filename: fixturePath(H3.file),
    columnData: [
      { name: "id", data: [1, 2, 3, 4], type: "INT32" },
      {
        name: "h3_7",
        data: ["8729a1d75ffffff", "8729a1d75ffffff", "8729a1d74ffffff", "8729a1d76ffffff"],
        type: "STRING",
      },
      {
        name: "h3_8",
        data: ["8829a1d757fffff", "8829a1d753fffff", "8829a1d74bfffff", "8829a1d765fffff"],
        type: "STRING",
      },
    ],
  });
}

function generateWriteGolden() {
  const path = fixturePath(WRITE.file);
  mkdirSync(dirname(path), { recursive: true });
  parquetWriteFile({
    filename: path,
    rowGroupSize: [2],
    columnData: [
      { name: "id", data: [1, 2, 3], type: "INT32" },
      { name: "name", data: ["a", "b", "c"], type: "STRING" },
      { name: "score", data: [1.5, 2.5, 3.5], type: "DOUBLE" },
    ],
  });
}

function generateManifestGoldens() {
  const taskInput = {
    path: "b.parquet",
    etag: "v2",
    rowGroupRanges: [
      { start: 10, end: 20 },
      { start: 0, end: 10 },
    ],
    projectedColumns: ["z", "a"],
    partitionValues: { country: "US", date: "2026-01-01" },
  };
  const normalizedTaskInput = normalizeTaskInput(taskInput);
  const taskManifest = {
    version: 1,
    jobId: "job_2",
    planFingerprint: fingerprint({
      version: 1,
      snapshot: "snapshot_2",
      tasks: [normalizedTaskInput],
    }),
    snapshot: "snapshot_2",
    tasks: [
      {
        id: taskId("job_2", 0, taskInput),
        input: normalizedTaskInput,
        outputRole: "data-file",
      },
    ],
  };
  const outputEntry = {
    taskId: taskManifest.tasks[0]?.id ?? "",
    outputPath: "out/date=2026-01-01/file.parquet",
    partitionValues: { country: "US", date: "2026-01-01" },
    rowCount: 12,
    byteSize: 256,
    contentHash: "sha256:abc",
    etag: "out-v1",
    iceberg: {
      recordCount: 12,
      fileSizeInBytes: 256,
      partitionValues: { country: "US", date: "2026-01-01" },
    },
  };
  const bookmark = {
    version: 1,
    planFingerprint: "fp_0123456789abcdef",
    snapshot: "snapshot_1",
    position: {
      fileIndex: 1,
      rowGroup: 2,
      rowOffset: 3,
      taskId: "job-task-000001-deadbeef",
      outputManifestCursor: 4,
    },
  };
  const retryLog = [
    {
      taskId: "job_4-task-000001-a",
      state: "planned",
      idempotencyKey: "idem-1",
      updatedAtMs: 10,
    },
    {
      taskId: "job_4-task-000001-a",
      state: "running",
      idempotencyKey: "idem-1",
      updatedAtMs: 20,
    },
    {
      taskId: "job_4-task-000001-a",
      state: "running",
      idempotencyKey: "idem-2",
      updatedAtMs: 100,
    },
    {
      taskId: "job_4-task-000001-a",
      state: "output-written",
      idempotencyKey: "idem-2",
      updatedAtMs: 110,
      output: {
        taskId: "job_4-task-000001-a",
        outputPath: "out/file.parquet",
        partitionValues: {},
        rowCount: 1,
        byteSize: 2,
      },
    },
  ];

  writeJsonFixture(MANIFESTS.taskManifest, taskManifest);
  const parquetTaskInput = normalizeTaskInput({
    path: "data/stats.parquet",
    etag: "v1",
    rowGroupRanges: [{ start: 1, end: 3 }],
    projectedColumns: ["id", "metric"],
    partitionValues: {},
    residualPredicate: {
      kind: "compare",
      left: { kind: "column", name: "metric" },
      op: "gte",
      right: { kind: "literal", value: 100 },
    },
  });
  writeJsonFixture(MANIFESTS.parquetTaskManifest, {
    version: 1,
    jobId: "job_stats",
    planFingerprint: fingerprint({
      version: 1,
      snapshot: fingerprint([{ etag: "v1", path: "data/stats.parquet" }]),
      tasks: [parquetTaskInput],
    }),
    snapshot: fingerprint([{ etag: "v1", path: "data/stats.parquet" }]),
    tasks: [
      {
        id: taskId("job_stats", 0, parquetTaskInput),
        input: parquetTaskInput,
        outputRole: "rows",
      },
    ],
  });
  writeJsonFixture(MANIFESTS.outputManifest, {
    version: 1,
    jobId: "job_2",
    planFingerprint: taskManifest.planFingerprint,
    entries: [normalizeOutputEntry(outputEntry)],
  });
  writeJsonFixture(MANIFESTS.bookmark, bookmark);
  writeJsonFixture(MANIFESTS.retryLog, retryLog);
}

function generateHive() {
  for (const file of HIVE.files) {
    const path = fixturePath(file);
    mkdirSync(dirname(path), { recursive: true });
    const country = file.includes("country=CA") ? "CA" : "US";
    const date = file.includes("date=2026-01-01") ? "2026-01-01" : "2026-01-02";
    const base = country === "CA" ? 100 : date.endsWith("01") ? 0 : 200;
    const id: number[] = [];
    const amount: number[] = [];
    for (let i = 0; i < HIVE.rowsPerFile; i++) {
      id.push(base + i);
      amount.push(base + i * 10);
    }
    parquetWriteFile({
      filename: path,
      columnData: [
        { name: "id", data: id, type: "INT32" },
        { name: "amount", data: amount, type: "INT32" },
      ],
    });
  }
}

function generateIceberg() {
  mkdirSync(dirname(fixturePath(ICEBERG.metadataFile)), { recursive: true });
  const manifest1 = {
    path: ICEBERG.manifestFiles[0],
    files: [
      {
        path: HIVE.files[0],
        sequenceNumber: 1,
        partition: { country: "US", date: "2026-01-01" },
        recordCount: HIVE.rowsPerFile,
      },
      {
        path: HIVE.files[1],
        sequenceNumber: 2,
        partition: { country: "CA", date: "2026-01-02" },
        recordCount: HIVE.rowsPerFile,
      },
    ],
  };
  const manifest2 = {
    path: ICEBERG.manifestFiles[1],
    files: [
      {
        path: HIVE.files[0],
        sequenceNumber: 1,
        partition: { country: "US", date: "2026-01-01" },
        recordCount: HIVE.rowsPerFile,
        deleteFiles: [{ content: "position-delete", path: ICEBERG.positionDeleteFile }],
      },
      {
        path: HIVE.files[1],
        sequenceNumber: 2,
        partition: { country: "CA", date: "2026-01-02" },
        recordCount: HIVE.rowsPerFile,
        deleteFiles: [{ content: "equality-delete", path: "deletes/country-ca.eq.parquet" }],
      },
      {
        path: HIVE.files[2],
        sequenceNumber: 3,
        partition: { country: "US", date: "2026-01-02" },
        recordCount: HIVE.rowsPerFile,
      },
    ],
  };
  const metadata = {
    "format-version": 2,
    "table-uuid": "00000000-0000-4000-8000-000000000001",
    location: "fixtures/data/iceberg/warehouse/places",
    "current-snapshot-id": 2,
    refs: {
      main: { type: "branch", "snapshot-id": 2 },
      previous: { type: "tag", "snapshot-id": 1 },
    },
    schemas: [
      {
        "schema-id": 1,
        fields: [
          { id: 1, name: "id", type: "int", required: true },
          { id: 2, name: "amount", type: "int", required: false },
          { id: 3, name: "country", type: "string", required: false },
        ],
      },
      {
        "schema-id": 2,
        fields: [
          { id: 1, name: "id", type: "int", required: true },
          { id: 2, name: "amount", type: "int", required: false },
          { id: 4, name: "nation", sourceId: 3, type: "string", required: false },
        ],
      },
    ],
    snapshots: [
      {
        "snapshot-id": 1,
        "timestamp-ms": 1_767_225_600_000,
        "schema-id": 1,
        manifests: [manifest1],
      },
      {
        "snapshot-id": 2,
        "timestamp-ms": 1_767_312_000_000,
        "schema-id": 2,
        manifests: [manifest2],
      },
    ],
  };
  writeFileSync(fixturePath(ICEBERG.metadataFile), `${JSON.stringify(metadata, null, 2)}\n`);
  for (const manifest of [manifest1, manifest2]) {
    writeFileSync(fixturePath(manifest.path), `${JSON.stringify(manifest, null, 2)}\n`);
  }
  const manifestRefMetadata = {
    ...metadata,
    snapshots: metadata.snapshots.map((snapshot) => ({
      ...snapshot,
      manifests: snapshot.manifests.map((manifest) => ({ path: manifest.path })),
    })),
  };
  writeFileSync(
    fixturePath(ICEBERG.manifestRefMetadataFile),
    `${JSON.stringify(manifestRefMetadata, null, 2)}\n`,
  );
  const multiManifestMetadata = {
    ...metadata,
    snapshots: metadata.snapshots.map((snapshot) =>
      snapshot["snapshot-id"] === 2
        ? {
            ...snapshot,
            manifests: [
              {
                path: "iceberg/warehouse/places/metadata/manifest-2-us.json",
                files: manifest2.files.filter((file) => file.partition.country === "US"),
              },
              {
                path: "iceberg/warehouse/places/metadata/manifest-2-ca.json",
                files: manifest2.files.filter((file) => file.partition.country === "CA"),
              },
            ],
          }
        : snapshot,
    ),
  };
  writeFileSync(
    fixturePath(ICEBERG.multiManifestMetadataFile),
    `${JSON.stringify(multiManifestMetadata, null, 2)}\n`,
  );
  writeJsonFixture(ICEBERG.plannedFilesGolden, {
    snapshotId: 2,
    files: manifest2.files
      .filter((file) => file.partition.country === "US")
      .map((file) => ({
        path: file.path,
        sequenceNumber: file.sequenceNumber,
        partition: file.partition,
        recordCount: file.recordCount,
        projectedFieldIds: [1, 3],
        snapshotId: 2,
        ...(file.deleteFiles !== undefined
          ? {
              deleteFiles: file.deleteFiles.map((deleteFile) => ({
                content: deleteFile.content,
                path: deleteFile.path,
              })),
            }
          : {}),
      })),
  });
}

function generateIcebergDeletes() {
  const equalityPath = fixturePath(ICEBERG.equalityDeleteFile);
  mkdirSync(dirname(equalityPath), { recursive: true });
  parquetWriteFile({
    filename: equalityPath,
    columnData: [{ name: "country", data: ["CA"], type: "STRING" }],
  });

  const positionPath = fixturePath(ICEBERG.positionDeleteFile);
  mkdirSync(dirname(positionPath), { recursive: true });
  parquetWriteFile({
    filename: positionPath,
    columnData: [
      { name: "file_path", data: [HIVE.files[0]], type: "STRING" },
      { name: "pos", data: [1n], type: "INT64" },
    ],
  });
}

generateSales();
generateTypes();
generateWide();
generateStats();
generateGroupby();
generateGeo();
generateH3();
generateWriteGolden();
generateManifestGoldens();
generateHive();
generateIcebergDeletes();
generateIceberg();
console.log(`fixtures written to ${fixtureDataDir}`);

function writeJsonFixture(name: string, value: unknown) {
  const path = fixturePath(name);
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${stableStringify(value)}\n`);
}

function normalizeTaskInput(task: {
  path: string;
  etag?: string;
  rowGroupRanges: { start: number; end: number }[];
  projectedColumns?: string[];
  partitionValues: Record<string, string>;
  residualPredicate?: unknown;
}) {
  const normalized = {
    path: task.path,
    etag: task.etag,
    rowGroupRanges: [...task.rowGroupRanges]
      .map((range) => ({ start: range.start, end: range.end }))
      .sort((a, b) => a.start - b.start || a.end - b.end),
    projectedColumns: task.projectedColumns ? [...task.projectedColumns].sort() : undefined,
    partitionValues: sortRecord(task.partitionValues),
  };
  if (task.residualPredicate !== undefined) {
    return { ...normalized, residualPredicate: task.residualPredicate };
  }
  return normalized;
}

function normalizeOutputEntry(entry: {
  taskId: string;
  outputPath: string;
  partitionValues: Record<string, string>;
  rowCount: number;
  byteSize: number;
  contentHash?: string;
  etag?: string;
  iceberg?: {
    recordCount: number;
    fileSizeInBytes: number;
    partitionValues: Record<string, string>;
  };
}) {
  return {
    taskId: entry.taskId,
    outputPath: entry.outputPath,
    partitionValues: sortRecord(entry.partitionValues),
    rowCount: entry.rowCount,
    byteSize: entry.byteSize,
    contentHash: entry.contentHash,
    etag: entry.etag,
    iceberg: entry.iceberg
      ? {
          recordCount: entry.iceberg.recordCount,
          fileSizeInBytes: entry.iceberg.fileSizeInBytes,
          partitionValues: sortRecord(entry.iceberg.partitionValues),
        }
      : undefined,
  };
}

function taskId(jobId: string, index: number, task: unknown): string {
  return `${jobId}-task-${String(index).padStart(6, "0")}-${fingerprint(task).slice(3, 11)}`;
}

function fingerprint(value: unknown): string {
  return `fp_${fnv1a64(stableStringify(value)).toString(16).padStart(16, "0")}`;
}

function fnv1a64(input: string): bigint {
  let hash = 0xcbf29ce484222325n;
  const prime = 0x100000001b3n;
  for (const char of input) {
    hash ^= BigInt(char.codePointAt(0) ?? 0);
    hash = BigInt.asUintN(64, hash * prime);
  }
  return hash;
}

function stableStringify(value: unknown): string {
  return JSON.stringify(toStableJson(value));
}

function toStableJson(value: unknown): unknown {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    if (typeof value === "number" && !Number.isFinite(value)) return String(value);
    return value;
  }
  if (typeof value === "bigint") return value.toString();
  if (value instanceof Date) return value.toISOString();
  if (value instanceof Uint8Array) return base64UrlEncode(value);
  if (Array.isArray(value)) return value.map(toStableJson);
  if (typeof value === "object" && value !== null) {
    const out: Record<string, unknown> = {};
    for (const [key, inner] of Object.entries(value).sort(([a], [b]) => a.localeCompare(b))) {
      if (inner !== undefined) out[key] = toStableJson(inner);
    }
    return out;
  }
  return String(value);
}

function sortRecord(record: Record<string, string>): Record<string, string> {
  return Object.fromEntries(Object.entries(record).sort(([a], [b]) => a.localeCompare(b)));
}

function base64UrlEncode(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
}
