import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { memoryStore } from "@laql/core";
import { EXTERNAL_CONFORMANCE, externalFixturePath } from "@laql/fixtures";
import { describe, expect, it } from "vitest";
import { readParquetObjects } from "./index.js";

const parquetTestingDir = externalFixturePath(EXTERNAL_CONFORMANCE.parquetTestingDir);
const parquetFiles = existsSync(parquetTestingDir)
  ? listFiles(parquetTestingDir).filter((path) => path.endsWith(".parquet"))
  : [];
const describeExternal =
  process.env.LAQL_CONFORMANCE === "1" && parquetFiles.length > 0 ? describe : describe.skip;

describeExternal("apache/parquet-testing conformance", () => {
  it.each(parquetFiles)("decodes %s through the public reader", async (file) => {
    const store = memoryStore();
    const objectPath = relative(parquetTestingDir, file);
    await store.put(objectPath, readFileSync(file));

    const rows = await readParquetObjects(store, objectPath);
    expect(Array.isArray(rows)).toBe(true);
  });
});

function listFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir).sort()) {
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) out.push(...listFiles(path));
    else out.push(path);
  }
  return out;
}
