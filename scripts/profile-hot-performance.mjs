import { spawnSync } from "node:child_process";
import { mkdir, readdir } from "node:fs/promises";
import { join, resolve } from "node:path";

const profileDir = resolve("bench/generated/profiles");
const testFile = "packages/parquet/src/flights-hot-performance.test.ts";
const vitestBin = resolve("node_modules/vitest/vitest.mjs");
const caseArg = process.argv.find((arg) => arg.startsWith("--case="));
const timeoutArg = process.argv.find((arg) => arg.startsWith("--timeout-ms="));
const extraArgs = process.argv
  .slice(2)
  .filter((arg) => !arg.startsWith("--case=") && !arg.startsWith("--timeout-ms="));

await mkdir(profileDir, { recursive: true });
const before = new Set(await profileFiles());

const env = {
  ...process.env,
  LAKEQL_HOT_PERF: "1",
};
if (caseArg) env.LAKEQL_HOT_PERF_CASES = caseArg.slice("--case=".length);
if (timeoutArg) env.LAKEQL_HOT_PERF_TIMEOUT_MS = timeoutArg.slice("--timeout-ms=".length);

const result = spawnSync(
  process.execPath,
  [
    "--cpu-prof",
    `--cpu-prof-dir=${profileDir}`,
    vitestBin,
    "run",
    testFile,
    "--pool=forks",
    "--maxWorkers=1",
    "--no-file-parallelism",
    ...extraArgs,
  ],
  {
    env,
    stdio: "inherit",
  },
);

const after = await profileFiles();
const created = after.filter((file) => !before.has(file));
if (created.length > 0) {
  console.log("wrote CPU profiles:");
  for (const file of created) console.log(`  ${join(profileDir, file)}`);
}
console.log("wrote hot perf report:");
console.log("  bench/generated/flights-hot-performance.jsonl");

if (result.error) throw result.error;
process.exit(result.status ?? 1);

async function profileFiles() {
  return (await readdir(profileDir).catch(() => []))
    .filter((file) => file.endsWith(".cpuprofile"))
    .sort();
}
