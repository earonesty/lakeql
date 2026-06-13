// Deterministic fixture generation: same input, same bytes, no clock, no RNG.
// Run via `pnpm fixtures` (root) or `pnpm generate` (this package).
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { parquetWriteFile } from "hyparquet-writer";
import { fixtureDataDir, fixturePath, HIVE, ICEBERG, SALES, STATS, TYPES, WIDE } from "./index.ts";

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
        manifests: [
          {
            path: "manifest-1.json",
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
          },
        ],
      },
      {
        "snapshot-id": 2,
        "timestamp-ms": 1_767_312_000_000,
        "schema-id": 2,
        manifests: [
          {
            path: "manifest-2.json",
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
                deleteFiles: [
                  { content: "equality-delete", path: "deletes/country-ca.eq.parquet" },
                ],
              },
              {
                path: HIVE.files[2],
                sequenceNumber: 3,
                partition: { country: "US", date: "2026-01-02" },
                recordCount: HIVE.rowsPerFile,
              },
            ],
          },
        ],
      },
    ],
  };
  writeFileSync(fixturePath(ICEBERG.metadataFile), `${JSON.stringify(metadata, null, 2)}\n`);
}

generateSales();
generateTypes();
generateWide();
generateStats();
generateHive();
generateIceberg();
console.log(`fixtures written to ${fixtureDataDir}`);
