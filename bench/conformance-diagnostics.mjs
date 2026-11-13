// Classify an existing failing conformance report without changing test expectations.

import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { relative, resolve } from "node:path";
import { memoryStore } from "../packages/core/dist/index.js";
import { readParquetObjects } from "../packages/parquet/dist/index.js";

const reportPath = process.argv[2];
if (reportPath === undefined) throw new Error("Pass a Vitest JSON conformance report");
const reportBytes = await readFile(reportPath);
const report = JSON.parse(reportBytes.toString("utf8"));
const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");
const root = resolve("fixtures/external/parquet-testing");
const failures = report.testResults
  .flatMap((test) => test.assertionResults)
  .filter((test) => test.status === "failed");
const results = [];
const unclassifiedFailures = [];
for (const test of failures) {
  const match = /^decodes (.+) through the public reader$/.exec(test.title);
  if (!match) {
    unclassifiedFailures.push(test.fullName ?? test.title);
    continue;
  }
  const file = relative(root, match[1]);
  if (file.startsWith("..")) throw new Error("Unexpected fixture path");
  const store = memoryStore();
  const fixture = await readFile(match[1]);
  const fixtureSha256 = sha256(fixture);
  await store.put(file, fixture);
  try {
    const rows = await readParquetObjects(store, file);
    results.push({ file, fixtureSha256, status: "decoded-on-rerun", rows: rows.length });
  } catch (error) {
    const causes = [];
    let cause = error;
    const seen = new Set();
    while (cause !== undefined && cause !== null && !seen.has(cause)) {
      seen.add(cause);
      causes.push({
        name: cause.name ?? typeof cause,
        code: cause.code,
        message: cause.message ?? String(cause),
      });
      cause = cause.details?.cause ?? cause.cause;
    }
    results.push({ file, fixtureSha256, status: "rejected", causes });
  }
}
const counts = {};
for (const result of results) {
  const top = result.causes?.[0];
  const key =
    top?.code === "LAKEQL_UNSUPPORTED_PARQUET_FEATURE"
      ? `${top.code}: ${top.message}`
      : (result.causes?.at(-1)?.message ?? result.status);
  counts[key] = (counts[key] ?? 0) + 1;
}
console.log(
  JSON.stringify(
    {
      scope:
        "Diagnostic rerun of failures only; local public reader, not cloud; rejection is not automatically expected or conformant",
      reportSha256: sha256(reportBytes),
      execution: "Built packages/core/dist and packages/parquet/dist; run pnpm build first",
      reportedFailures: failures.length,
      classifiedFailures: results.length,
      unclassifiedFailures,
      counts,
      results,
    },
    null,
    2,
  ),
);
