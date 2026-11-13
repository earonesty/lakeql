// Run after pnpm build. Exercises real workerd ownership across harness requests.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { createWorkerHeapRuntime } from "./worker-heap-runtime.mjs";

const require = createRequire(import.meta.resolve("@cloudflare/vitest-pool-workers"));
const parquetRequire = createRequire(new URL("../packages/parquet/package.json", import.meta.url));
const { parquetWriteBuffer } = parquetRequire("hyparquet-writer");
const { build } = require("esbuild");
const bundle = await build({
  entryPoints: [new URL("./worker-heap-entry.mjs", import.meta.url).pathname],
  bundle: true,
  write: false,
  format: "esm",
  platform: "browser",
  target: "es2022",
});
const bytes = new Uint8Array(
  parquetWriteBuffer({
    columnData: [
      { name: "id", type: "INT32", data: [0, 1, 2, 3] },
      { name: "payload", type: "STRING", data: ["a", "b", "c", "d"] },
    ],
  }),
);
const runtime = await createWorkerHeapRuntime(bundle.outputFiles[0].text, bytes);
async function call(path) {
  const response = await runtime.dispatchFetch(`http://worker${path}`);
  assert.equal(response.status, 200);
  return response.json();
}
try {
  await call("/configure?mib=64");
  let retainedBaseline;
  for (let round = 0; round < 3; round++) {
    const run = await call("/run?jobs=1&mode=owned");
    assert.equal(run.reports[0].status, "ok");
    assert.equal((await call("/held")).rows, 4);
    const retained = await call("/evict");
    assert.ok(retained.usedBytes > 0);
    retainedBaseline ??= retained.usedBytes;
    assert.equal(retained.usedBytes, retainedBaseline);
  }
  await call("/run?jobs=1&mode=owned&retainPrevious=1");
  assert.equal((await call("/held")).rows, 8);
  await call("/release");
  assert.equal((await call("/evict")).usedBytes, 0);
  await call("/run?jobs=1&mode=owned");
  const reconfigured = await call("/configure?mib=128");
  assert.equal(reconfigured.previousUsedBytes, 0);
  assert.equal((await call("/held")).rows, 0);
  assert.equal((await call("/evict")).usedBytes, 0);
  console.log(
    JSON.stringify({
      backend: "workerd/native R2",
      replacementRuns: 3,
      explicitRetainRows: 8,
      releasedBytes: 0,
      reconfiguredPreviousBytes: 0,
    }),
  );
} finally {
  await runtime.dispose();
}
