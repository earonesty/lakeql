import { fileURLToPath } from "node:url";

export const fixtureDataDir = fileURLToPath(new URL("../data/", import.meta.url));

export function fixturePath(name: string): string {
  return fileURLToPath(new URL(`../data/${name}`, import.meta.url));
}

/** Shapes the generator guarantees; tests assert against these. */
export const SALES = {
  file: "sales.parquet",
  rows: 100,
  rowGroupSize: 40, // 100 rows -> row groups of 40, 40, 20
  regions: ["west", "east", "north", "south"],
} as const;

export const TYPES = {
  file: "types.parquet",
  rows: 10,
} as const;
