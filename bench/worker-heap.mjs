import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { parquetWriteBuffer } from "../packages/parquet/node_modules/hyparquet-writer/src/index.js";

const require = createRequire(import.meta.resolve("@cloudflare/vitest-pool-workers"));

import { createWorkerHeapRuntime } from "./worker-heap-runtime.mjs";

const { build } = require("esbuild");
const bundle = await build({
  entryPoints: ["bench/worker-heap-entry.mjs"],
  bundle: true,
  write: false,
  format: "esm",
  platform: "browser",
  target: "es2022",
});
const script = bundle.outputFiles[0].text;
const rows = Number(process.env.LAKEQL_HEAP_ROWS ?? 32768);
assert.ok(Number.isSafeInteger(rows) && rows > 0 && rows <= 131072);
const mode = process.env.LAKEQL_HEAP_MODE ?? "materialized";
assert.ok(mode === "materialized" || mode === "stream" || mode === "owned");
const recoveryProbe = process.env.LAKEQL_HEAP_RECOVERY_PROBE === "1";
assert.ok(!recoveryProbe || mode === "owned");
const roundCount = Number(process.env.LAKEQL_HEAP_ROUNDS ?? 2);
assert.ok(Number.isSafeInteger(roundCount) && roundCount >= 2 && roundCount <= 32);
const payloadLength = 256;
const bytes = new Uint8Array(
  parquetWriteBuffer({
    rowGroupSize: Array.from({ length: Math.ceil(rows / 4096) }, (_, i) =>
      Math.min(4096, rows - i * 4096),
    ),
    columnData: [
      { name: "id", type: "INT32", data: Array.from({ length: rows }, (_, i) => i) },
      {
        name: "payload",
        type: "STRING",
        data: Array.from({ length: rows }, (_, i) => `${i}:`.padEnd(payloadLength, "x")),
      },
    ],
  }),
);
// Diagnostic only: production Workers cannot rely on an inspector-triggered GC.
// Collect after both measured rounds and eviction so their measurements remain
// unaffected, then distinguish reachable retention from pending reclamation.
const diagnosticGc = process.env.LAKEQL_HEAP_DIAGNOSTIC_GC === "1";
const runtimeHeapMiB =
  process.env.LAKEQL_HEAP_RUNTIME_MIB === undefined
    ? undefined
    : Number(process.env.LAKEQL_HEAP_RUNTIME_MIB);
const scenarios = [];
for (const mib of [64, 128])
  for (const jobs of [1, 4]) {
    const mf = await createWorkerHeapRuntime(script, bytes, runtimeHeapMiB);
    let socket;
    const completedRounds = [];
    try {
      const inspector = await mf.getInspectorURL();
      inspector.protocol = "http:";
      const targets = await (await fetch(new URL("/json", inspector))).json();
      const target = targets.find(
        (t) => t.id === "core:user:heap-stress" || t.id === "heap-stress",
      );
      assert.ok(target);
      socket = new WebSocket(target.webSocketDebuggerUrl);
      await new Promise((resolve, reject) => {
        socket.onopen = resolve;
        socket.onerror = reject;
      });
      let nextId = 0;
      const pending = new Map();
      socket.onclose = () => {
        for (const waiter of pending.values()) {
          clearTimeout(waiter.timer);
          waiter.reject(new Error("Inspector connection closed"));
        }
        pending.clear();
      };
      socket.onmessage = ({ data }) => {
        const message = JSON.parse(data);
        const waiter = pending.get(message.id);
        if (waiter) {
          pending.delete(message.id);
          clearTimeout(waiter.timer);
          message.error
            ? waiter.reject(new Error(JSON.stringify(message.error)))
            : waiter.resolve(message.result);
        }
      };
      const call = (method, timeoutMs = 30000) =>
        new Promise((resolve, reject) => {
          const id = ++nextId;
          const timer = setTimeout(() => {
            pending.delete(id);
            reject(new Error(`Inspector request timed out: ${method}`));
          }, timeoutMs);
          pending.set(id, { resolve, reject, timer });
          socket.send(JSON.stringify({ id, method }));
        });
      const sample = async () => ({
        atMs: performance.now(),
        ...(await call("Runtime.getHeapUsage")),
      });
      const request = async (path) => {
        const response = await mf.dispatchFetch(`http://worker${path}`);
        assert.equal(response.status, 200, await response.clone().text());
        return response.json();
      };
      await request(`/configure?mib=${mib}`);
      const baseline = await sample();
      const rounds = completedRounds;
      for (let roundIndex = 0; roundIndex < roundCount; roundIndex++) {
        const round = roundIndex === 0 ? "cold" : roundIndex === 1 ? "warm" : `warm-${roundIndex}`;
        const samples = [];
        let done = false;
        const start = performance.now();
        const operation = request(`/run?jobs=${jobs}&mode=${mode}`).then(
          (value) => {
            done = true;
            return { value };
          },
          (error) => {
            done = true;
            return { error };
          },
        );
        while (!done) {
          samples.push(await sample());
          await new Promise((resolve) => setTimeout(resolve, 5));
        }
        const outcome = await operation;
        if ("error" in outcome) throw outcome.error;
        const result = outcome.value;
        samples.push(await sample());
        const complete = result.reports.every((report) => report.status === "ok");
        const successes = result.reports.filter((report) => report.status === "ok").length;
        for (const report of result.reports) {
          if (report.status === "budget") {
            assert.equal(report.code, "LAKEQL_BUDGET_EXCEEDED");
            continue;
          }
          assert.equal(report.status, "ok");
          assert.equal(report.rows, rows);
          assert.equal(report.ids, (rows * (rows - 1)) / 2);
          assert.equal(report.characters, rows * payloadLength);
          if (round !== "cold" && complete) assert.equal(report.ranges, 0);
        }
        assert.ok(result.peakBytes <= mib * 1024 * 1024);
        let recovery;
        let recovered = 0;
        if (recoveryProbe && !complete) {
          recovery = await request(`/run?jobs=1&mode=owned&retainPrevious=1`);
          assert.ok(recovery.peakBytes <= mib * 1024 * 1024);
          const report = recovery.reports[0];
          if (report.status === "ok") {
            assert.equal(report.rows, rows);
            assert.equal(report.ids, (rows * (rows - 1)) / 2);
            assert.equal(report.characters, rows * payloadLength);
            recovered = 1;
          } else {
            assert.equal(report.status, "budget");
            assert.equal(report.code, "LAKEQL_BUDGET_EXCEEDED");
          }
        }
        const held = await request("/held");
        assert.equal(held.jobs, mode === "stream" ? 0 : successes + recovered);
        assert.equal(held.rows, mode === "stream" ? 0 : rows * (successes + recovered));
        assert.deepEqual(
          held.lastIds,
          Array(mode === "stream" ? 0 : successes + recovered).fill(rows - 1),
        );
        const retainedOutput = await sample();
        await request("/release");
        const afterRelease = await sample();
        // A rejected job must not poison later admission. Reuse the same lake,
        // pool and cache after releasing output; do not evict or reconfigure.
        let recoveryAfterRelease;
        if (recoveryProbe && !complete) {
          recoveryAfterRelease = await request("/run?jobs=1&mode=owned");
          assert.ok(recoveryAfterRelease.peakBytes <= mib * 1024 * 1024);
          const report = recoveryAfterRelease.reports[0];
          assert.equal(report.status, "ok");
          assert.equal(report.rows, rows);
          assert.equal(report.ids, (rows * (rows - 1)) / 2);
          assert.equal(report.characters, rows * payloadLength);
          const heldRecovery = await request("/held");
          assert.equal(heldRecovery.jobs, 1);
          assert.equal(heldRecovery.rows, rows);
          assert.deepEqual(heldRecovery.lastIds, [rows - 1]);
          await request("/release");
        }
        rounds.push({
          round,
          elapsedMs: performance.now() - start,
          result,
          ...(recovery === undefined ? {} : { recovery }),
          ...(recoveryAfterRelease === undefined ? {} : { recoveryAfterRelease }),
          samples,
          retainedOutput,
          afterRelease,
        });
      }
      const evicted = await request("/evict");
      assert.equal(evicted.usedBytes, 0);
      const afterEvict = await sample();
      let gcDiagnostic;
      if (diagnosticGc) {
        // Some runtimes need an isolate turn to service inspector GC. A failed
        // optional probe must not discard successful measured query evidence.
        const collection = call("HeapProfiler.collectGarbage", 2000).then(
          () => ({ status: "collected" }),
          (error) => ({ status: "unavailable", error: String(error) }),
        );
        await request("/held");
        gcDiagnostic = await collection;
        if (gcDiagnostic.status === "collected") gcDiagnostic.heap = await sample();
      }
      scenarios.push({
        mib,
        jobs,
        status: "complete",
        baseline,
        rounds,
        evicted,
        afterEvict,
        ...(gcDiagnostic === undefined ? {} : { gcDiagnostic }),
      });
    } catch (error) {
      scenarios.push({
        mib,
        jobs,
        status: "failed",
        error: String(error),
        rounds: completedRounds,
        runtimeStderr: mf.runtimeStderr?.() ?? null,
      });
    } finally {
      socket?.close();
      await mf.dispose();
    }
  }
console.log(
  JSON.stringify(
    {
      node: process.version,
      mode,
      diagnosticGc,
      recoveryProbe,
      roundCount,
      runtimeHeapMiB: runtimeHeapMiB ?? null,
      transport: runtimeHeapMiB === undefined ? "native emulated R2" : "localhost HTTP ranges",
      rows,
      payloadLength,
      fixtureBytes: bytes.byteLength,
      fixtureSha256: createHash("sha256").update(bytes).digest("hex"),
      bundleSha256: createHash("sha256").update(script).digest("hex"),
      scope:
        "Local workerd using the recorded transport; host-generated real Parquet fixture; fresh isolate per capacity/concurrency scenario. Runtime.getHeapUsage sampled approximately every 5ms while requests run; samples are lower bounds on peaks, not enforced Cloudflare production heap limits. No forced GC during measured rounds; diagnosticGc optionally collects only after final eviction. In materialized/owned mode output rows are pinned through a post-query snapshot then explicitly released; stream mode keeps only checksums. Only checksums leave Worker. Pool bounds cover tracked allocations only. Optional runtimeHeapMiB supplies a verified V8 managed-heap flag, not a cap on external buffers, total process memory, or production Cloudflare accounting. Raw JS/embedder/backing counters retained separately; no assumed additive total.",
      scenarios,
    },
    null,
    2,
  ),
);
if (scenarios.some((scenario) => scenario.status === "failed")) process.exitCode = 1;
