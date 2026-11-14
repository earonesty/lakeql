import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { Session } from "node:inspector";
import { encodeOwnedJsonOutput } from "../packages/core/dist/json-output-encoding.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";

const rows = Array.from({ length: 1000 }, (_, id) => ({ id, text: "雪😀", flag: true }));
const expected = new TextEncoder().encode(JSON.stringify(rows));
function run(n) {
  for (let i = 0; i < n; i++) {
    const budget = new MemoryBudget(64 * 1024 * 1024);
    const scope = new MemoryScope(budget);
    const output = encodeOwnedJsonOutput(rows, scope);
    if (i === 0) assert.deepEqual(output.value, expected);
    output.release();
    scope.dispose();
    assert.equal(budget.usedBytes, 0);
  }
}
run(1000);
const session = new Session();
session.connect();
const post = (name, args = {}) =>
  new Promise((resolve, reject) =>
    session.post(name, args, (err, result) => (err ? reject(err) : resolve(result))),
  );
await post("Profiler.enable");
await post("Profiler.setSamplingInterval", { interval: 100 });
await post("Profiler.start");
run(20000);
const { profile } = await post("Profiler.stop");
session.disconnect();
const nodes = new Map(profile.nodes.map((node) => [node.id, node]));
const totals = new Map();
let sampledMicroseconds = 0;
for (let i = 0; i < profile.samples.length; i++) {
  const frame = nodes.get(profile.samples[i]).callFrame;
  const key = JSON.stringify({
    function: frame.functionName,
    file: frame.url.split("/").at(-1),
    line: frame.lineNumber + 1,
  });
  const duration = profile.timeDeltas[i];
  totals.set(key, (totals.get(key) ?? 0) + duration);
  sampledMicroseconds += duration;
}
const report = {
  runtimeSource: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  node: process.version,
  workload: {
    rows: rows.length,
    warmupEncodes: 1000,
    measuredEncodes: 20000,
    samplingIntervalMicroseconds: 100,
    budgetBytes: 64 * 1024 * 1024,
  },
  sampledMicroseconds,
  builtModuleHashes: Object.fromEntries(
    await Promise.all(
      [
        "json-output-size",
        "json-output-encoding",
        "json-container-entries",
        "json-string-size",
      ].map(async (name) => [
        name,
        createHash("sha256")
          .update(await readFile(new URL(`../packages/core/dist/${name}.js`, import.meta.url)))
          .digest("hex"),
      ]),
    ),
  ),
  selfTime: [...totals.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([key, microseconds]) => ({
      ...JSON.parse(key),
      microseconds,
      percent: (microseconds / sampledMicroseconds) * 100,
    })),
  verification:
    "Byte oracle checked at the start of each phase; every encode releases all budget charges.",
  limitations: [
    "V8 sampled self time may include inlined work; not exact function attribution.",
    "Profiling changes runtime overhead; use the unprofiled traversal benchmark for comparisons.",
    "One local Node row-array workload; not Worker/cloud throughput or whole-heap evidence.",
  ],
};
const json = `${JSON.stringify(report, null, 2)}\n`;
if (process.env.LAKEQL_JSON_PROFILE_REPORT)
  await writeFile(process.env.LAKEQL_JSON_PROFILE_REPORT, json);
else process.stdout.write(json);
