import { spawnSync } from "node:child_process";
import { mkdir, readdir } from "node:fs/promises";
import { join, resolve } from "node:path";

const profileDir = resolve("bench/generated/profiles");
const testFile = "packages/parquet/src/flights-hot-performance.test.ts";
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
  NODE_OPTIONS: appendNodeOptions(process.env.NODE_OPTIONS, [
    `--cpu-prof`,
    `--cpu-prof-dir=${profileDir}`,
  ]),
};
if (caseArg) env.LAKEQL_HOT_PERF_CASES = caseArg.slice("--case=".length);
if (timeoutArg) env.LAKEQL_HOT_PERF_TIMEOUT_MS = timeoutArg.slice("--timeout-ms=".length);

const result = spawnSync("pnpm", ["exec", "vitest", "run", testFile, ...extraArgs], {
  env,
  stdio: "inherit",
});

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

function appendNodeOptions(existing, options) {
  return [...(existing ? [existing] : []), ...options].join(" ");
}
