import assert from "node:assert/strict";
import { createWorkerHeapRuntime } from "./worker-heap-runtime.mjs";

// A separate disposable Worker intentionally retains arrays to test whether the
// runtime accepts an enforced heap constraint. Never run against a deployed app.
const heapMiB = Number(process.env.LAKEQL_HEAP_RUNTIME_MIB ?? 32);
const script = `const held = [];
export default { fetch() {
  held.push(new Array(1048576).fill(Math.random()));
  return Response.json({ arrays: held.length, elements: held.length * 1048576 });
} };`;
const runtime = await createWorkerHeapRuntime(script, Uint8Array.of(1), heapMiB);
const responses = [];
let failure;
try {
  for (let round = 0; round < 24; round++) {
    try {
      const response = await runtime.dispatchFetch("http://worker/retain");
      if (!response.ok)
        throw new Error(`Worker response ${response.status}: ${await response.text()}`);
      const result = await response.json();
      assert.equal(result.arrays, round + 1);
      responses.push(result);
    } catch (error) {
      failure = String(error);
      break;
    }
  }
  // Allow fatal-runtime stderr to drain before preserving diagnostic evidence.
  await new Promise((resolve) => setTimeout(resolve, 50));
  console.log(
    JSON.stringify(
      {
        heapMiB,
        flag: `--max-heap-size=${heapMiB}`,
        responses,
        failure: failure ?? null,
        heapLimitFailure: /Reached heap limit/.test(runtime.runtimeStderr()),
        runtimeStderr: runtime.runtimeStderr(),
        interpretation:
          failure === undefined
            ? "No failure while retaining 24 million-element arrays; the flag does not establish the requested heap cap."
            : "Inspect failure and stderr: process failure alone does not prove the requested heap constraint was applied.",
      },
      null,
      2,
    ),
  );
} finally {
  await runtime.dispose();
}
