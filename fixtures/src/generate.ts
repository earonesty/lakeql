// Deterministic fixture generation: same input, same bytes, no clock, no RNG.
// Run via `pnpm fixtures` (root) or `pnpm generate` (this package).
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { parquetWriteFile } from "hyparquet-writer";
import { fixtureDataDir, fixturePath, HIVE, SALES, STATS, TYPES, WIDE } from "./index.ts";

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

generateSales();
generateTypes();
generateWide();
generateStats();
generateHive();
console.log(`fixtures written to ${fixtureDataDir}`);
