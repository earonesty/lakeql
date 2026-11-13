import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import {
  batchFromColumns,
  CpuPhysicalBackend,
  Lake,
  MemoryBudget,
  MemoryScope,
  memoryStore,
} from "../packages/core/dist/index.js";

async function run(rows, batchSize, scenario) {
  const store = memoryStore();
  await store.put("table", new Uint8Array(1));
  const pool = scenario?.pool ?? new MemoryBudget(64 * 1024 * 1024);
  let queryBudget;
  let appends = 0,
    checkpoints = 0,
    opens = 0,
    closed = 0;
  class MeasuredCpu extends CpuPhysicalBackend {
    openReduction(...args) {
      opens++;
      const session = super.openReduction(...args);
      return {
        backendId: session.backendId,
        append(...input) {
          appends++;
          return session.append(...input);
        },
        checkpoint() {
          checkpoints++;
          return session.checkpoint();
        },
        finish() {
          return session.finish();
        },
        dispose() {
          session.dispose();
        },
      };
    }
  }
  const scanner = {
    scan() {
      throw Error("Unexpected row fallback");
    },
    async *scanOwnedVectorBatches(_path, options) {
      queryBudget = options.memoryBudget;
      if (scenario) {
        scenario.active++;
        scenario.peakActive = Math.max(scenario.peakActive, scenario.active);
      }
      try {
        for (let offset = 0; offset < rows; offset += batchSize) {
          const scope = new MemoryScope(options.memoryBudget);
          try {
            const batch = batchFromColumns(
              {
                key: Array.from(
                  { length: Math.min(batchSize, rows - offset) },
                  (_, i) => offset + i,
                ),
              },
              scope,
            );
            yield scope.retainAllocations({ rowOffset: offset, batch });
          } finally {
            scope.dispose();
          }
        }
      } finally {
        closed++;
        if (scenario) scenario.active--;
      }
    },
  };
  const lake = new Lake({
    store,
    scanner,
    memoryPool: pool,
    ...(scenario ? { budget: { maxAllocatedMemoryBytes: scenario.perQueryBytes } } : {}),
    physicalExecution: { backends: [new MeasuredCpu("aaa-measured-cpu")] },
  });
  const started = performance.now();
  try {
    const output = await lake
      .path("table")
      .batchSize(batchSize)
      .groupBy(["key"])
      .aggregate({ rows: { op: "count" } });
    const elapsedMs = performance.now() - started;
    assert.equal(output.length, rows);
    for (let i = 0; i < rows; i++) assert.deepEqual(output[i], { key: i, rows: 1 });
    assert.equal(opens, 1);
    assert.equal(appends, Math.ceil(rows / batchSize));
    assert.equal(checkpoints, 0);
    assert.equal(closed, 1);
    if (!scenario) assert.equal(pool.usedBytes, 0);
    assert.ok(pool.peakBytes <= pool.maxBytes);
    return {
      elapsedMs,
      opens,
      appends,
      checkpoints,
      peakAdmittedBytes: pool.peakBytes,
      remainingBytes: queryBudget.usedBytes,
      queryPeakBytes: queryBudget.peakBytes,
    };
  } finally {
    assert.equal(closed, 1);
    assert.equal(queryBudget?.usedBytes, 0);
    if (scenario) assert.ok(queryBudget.peakBytes <= scenario.perQueryBytes);
    else assert.equal(pool.usedBytes, 0);
  }
}

if (process.argv.includes("--concurrent")) {
  const results = [];
  for (const mib of [64, 128]) {
    for (const perQueryMiB of [16, 32]) {
      for (const jobs of [2, 4]) {
        globalThis.gc?.();
        const pool = new MemoryBudget(mib * 1024 * 1024);
        const scenario = {
          pool,
          perQueryBytes: perQueryMiB * 1024 * 1024,
          active: 0,
          peakActive: 0,
        };
        const started = performance.now();
        const settled = await Promise.allSettled(
          Array.from({ length: jobs }, () => run(16000, 128, scenario)),
        );
        const outcomes = settled.map((result) => {
          if (result.status === "fulfilled") return { status: "complete", ...result.value };
          assert.equal(result.reason?.code, "LAKEQL_BUDGET_EXCEEDED");
          return {
            status: "admission-rejected",
            code: result.reason.code,
            details: result.reason.details,
          };
        });
        assert.equal(scenario.active, 0);
        assert.equal(scenario.peakActive, jobs);
        assert.equal(pool.usedBytes, 0);
        assert.ok(pool.peakBytes <= pool.maxBytes);
        results.push({
          poolMiB: mib,
          perQueryMiB,
          jobs,
          rowsPerJob: 16000,
          batchSize: 128,
          elapsedMs: performance.now() - started,
          peakConcurrentScans: scenario.peakActive,
          peakAdmittedBytes: pool.peakBytes,
          remainingBytes: pool.usedBytes,
          outcomes,
        });
      }
    }
  }
  writeFileSync(
    "docs/benchmarks/public-grouped-concurrent-2026-11-13.json",
    `${JSON.stringify(
      {
        description:
          "Concurrent public grouped count over owned synthetic scanners. Shared 64/128 MiB pools and 16/32 MiB per-query caps; every successful row checked, every rejection typed, iterator closure and per-query/root cleanup asserted. No decoded cache, cloud transport or whole-heap guarantee is represented.",
        results,
      },
      null,
      2,
    )}\n`,
  );
  console.log(
    results.map(({ outcomes, ...result }) => ({
      ...result,
      completed: outcomes.filter((o) => o.status === "complete").length,
      rejected: outcomes.filter((o) => o.status !== "complete").length,
    })),
  );
} else {
  const results = [];
  for (const rows of [2000, 4000, 8000]) {
    for (const batchSize of [128, 512]) {
      await run(rows, batchSize);
      const samples = [];
      for (let sample = 0; sample < 3; sample++) {
        globalThis.gc?.();
        samples.push(await run(rows, batchSize));
      }
      results.push({
        rows,
        batchSize,
        medianMs: samples.map((value) => value.elapsedMs).sort((a, b) => a - b)[1],
        samples,
      });
    }
  }
  const report = {
    description:
      "Public grouped count over an owned synthetic scanner. One warmup and three sequential samples per case; optional GC before timed samples. Exact values, zero inter-batch checkpoints, scan closure and final pool cleanup asserted. This is local CPU scaling evidence, not cloud latency or total heap measurement.",
    node: process.version,
    gcAvailable: typeof globalThis.gc === "function",
    poolBytes: 64 * 1024 * 1024,
    results,
  };
  writeFileSync(
    "docs/benchmarks/public-grouped-continuation-2026-11-13.json",
    `${JSON.stringify(report, null, 2)}\n`,
  );
  console.log(
    results.map(({ rows, batchSize, medianMs, samples }) => ({
      rows,
      batchSize,
      medianMs,
      peakAdmittedBytes: samples[0].peakAdmittedBytes,
      checkpoints: samples[0].checkpoints,
    })),
  );
}
