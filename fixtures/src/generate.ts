// Deterministic fixture generation: same input, same bytes, no clock, no RNG.
// Run via `pnpm fixtures` (root) or `pnpm generate` (this package).
import { mkdirSync } from "node:fs";
import { parquetWriteFile } from "hyparquet-writer";
import { fixtureDataDir, fixturePath, SALES, TYPES } from "./index.ts";

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

generateSales();
generateTypes();
console.log(`fixtures written to ${fixtureDataDir}`);
