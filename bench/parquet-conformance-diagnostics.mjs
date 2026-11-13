// Diagnostic evidence only: no expected-failure list and no skipped inputs.
import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { memoryStore } from "../packages/core/dist/index.js";
import { readParquetObjects } from "../packages/parquet/dist/index.js";

const root = new URL("../fixtures/external/parquet-testing/", import.meta.url);
const files = (await readdir(root, { recursive: true }))
  .filter((name) => name.endsWith(".parquet"))
  .sort();
const results = [];
for (const path of files) {
  const data = await readFile(new URL(path, root));
  const store = memoryStore();
  await store.put(path, data);
  const result = {
    path,
    bytes: data.byteLength,
    sha256: createHash("sha256").update(data).digest("hex"),
  };
  try {
    const rows = await readParquetObjects(store, path);
    results.push({ ...result, status: "decoded", rows: rows.length });
  } catch (error) {
    const errors = [];
    const seen = new Set();
    for (let current = error; current !== undefined && !seen.has(current); ) {
      seen.add(current);
      errors.push({
        name: current?.name,
        code: current?.code,
        message: String(current?.message ?? current),
      });
      current = current?.cause ?? current?.details?.cause;
    }
    results.push({ ...result, status: "rejected", errors });
  }
}
const groups = new Map();
for (const result of results) {
  if (result.status !== "rejected") continue;
  const cause = result.errors.at(-1);
  const signature = `${cause.code ?? cause.name}: ${cause.message}`;
  const group = groups.get(signature) ?? [];
  group.push(result.path);
  groups.set(signature, group);
}
console.log(
  JSON.stringify(
    {
      scope:
        "Local public-reader diagnostics for every external Parquet fixture; decoding alone does not prove correct values; rejection is not classified as expected",
      total: results.length,
      decoded: results.filter((result) => result.status === "decoded").length,
      rejected: results.filter((result) => result.status === "rejected").length,
      causes: [...groups].map(([cause, paths]) => ({ cause, count: paths.length, paths })),
      results,
    },
    null,
    2,
  ),
);
