// Run after pnpm build. Optional first argument selects a baseline Git revision.
// Kernel timings isolate CPU work; they are not remote-query latency measurements.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import ts from "typescript";
import { batchFromColumns } from "../packages/core/dist/batch.js";
import { BoundedTopK } from "../packages/core/dist/bounded-top-k.js";
import { col, lit } from "../packages/core/dist/expr.js";
import { compareRows } from "../packages/core/dist/query-sort.js";
import * as currentGroups from "../packages/core/dist/vector-group-by.js";
import { frameSpan } from "../packages/core/dist/window-frame.js";
import { WindowPeers } from "../packages/core/dist/window-peers.js";
import { partitionRows } from "../packages/core/dist/window-utils.js";

const baseline = process.argv[2] ?? "b54d4ed";
const root = new URL("../", import.meta.url);
const directory = await mkdtemp(join(tmpdir(), "lakeql-memory-bench-"));

async function baselineModule(name) {
  const source = execFileSync("git", ["show", `${baseline}:packages/core/src/${name}.ts`], {
    cwd: root,
    encoding: "utf8",
  });
  // Unchanged dependencies use the current build; only the audited kernel is replaced.
  const output = ts
    .transpileModule(source, {
      compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
    })
    .outputText.replace(
      /from "\.\/([^"]+)"/g,
      (_match, file) => `from ${JSON.stringify(new URL(`packages/core/dist/${file}`, root).href)}`,
    );
  const path = join(directory, `${name}.mjs`);
  await writeFile(path, output);
  return import(pathToFileURL(path).href);
}

function median(operation) {
  operation();
  const timings = [];
  for (let i = 0; i < 3; i += 1) {
    const start = performance.now();
    operation();
    timings.push(performance.now() - start);
  }
  return Number(timings.sort((a, b) => a - b)[1].toFixed(3));
}

try {
  const oldGroups = await baselineModule("vector-group-by");
  const oldSort = await baselineModule("query-sort");
  const oldFrames = await baselineModule("window-frame");
  const results = [];
  for (const count of [2_000, 5_000, 10_000]) {
    const batch = batchFromColumns({ key: Array.from({ length: count }, (_, i) => i) });
    const run = (implementation) => {
      const state = implementation.createVectorGroupByState(["key"], { rows: { op: "count" } });
      implementation.updateVectorGroupByState(state, batch, undefined, {
        budget: { maxMemoryBytes: 64 * 1024 * 1024 },
      });
      assert.equal(state.groups.size, count);
      return state;
    };
    assert.deepEqual(
      currentGroups.finalizeVectorGroupByRows(run(currentGroups)),
      oldGroups.finalizeVectorGroupByRows(run(oldGroups)),
    );
    results.push({
      kernel: "budgeted-group-by",
      rows: count,
      beforeMs: median(() => run(oldGroups)),
      afterMs: median(() => run(currentGroups)),
    });
  }
  const rows = Array.from({ length: 20_000 }, (_, i) => ({ score: 20_000 - i, id: i }));
  const order = [{ column: "score" }];
  const oldTopK = () => {
    const retained = [];
    for (const row of rows) oldSort.addOrderedMatch(retained, row, order, 1000);
    return retained.sort((a, b) => compareRows(a, b, order));
  };
  const newTopK = () => {
    const heap = new BoundedTopK(1000, (a, b) => compareRows(a, b, order));
    for (const row of rows) heap.add(row);
    return heap.drainSorted();
  };
  assert.deepEqual(newTopK(), oldTopK());
  results.push({
    kernel: "row-top-k",
    rows: rows.length,
    k: 1000,
    beforeMs: median(oldTopK),
    afterMs: median(newTopK),
  });

  const expr = {
    fn: { aggregate: "sum" },
    args: [col("key")],
    over: {
      partitionBy: [],
      orderBy: [{ expr: col("key") }],
      frame: {
        mode: "range",
        start: { kind: "preceding", offset: lit(1) },
        end: { kind: "following", offset: lit(1) },
        exclude: "no-others",
      },
    },
  };
  const partition = partitionRows(
    Array.from({ length: 10_000 }, (_, key) => ({ key })),
    expr,
  );
  const lookup = (span) => partition.map((_row, index) => span(partition, index, expr));
  assert.deepEqual(lookup(frameSpan), lookup(oldFrames.frameSpan));
  results.push({
    kernel: "range-frame-endpoints",
    rows: partition.length,
    beforeMs: median(() => lookup(oldFrames.frameSpan)),
    afterMs: median(() => lookup(frameSpan)),
  });
  for (const count of [2_000, 5_000]) {
    const groupedExpr = {
      ...expr,
      over: { ...expr.over, frame: { ...expr.over.frame, mode: "groups" } },
    };
    const peersPartition = partitionRows(
      Array.from({ length: count }, (_, index) => ({ key: Math.floor(index / 5) })),
      groupedExpr,
    );
    const before = () =>
      peersPartition.map((_row, index) => oldFrames.frameSpan(peersPartition, index, groupedExpr));
    const after = () => {
      const peers = new WindowPeers(peersPartition);
      return peersPartition.map((_row, index) =>
        frameSpan(peersPartition, index, groupedExpr, peers),
      );
    };
    assert.deepEqual(after(), before());
    results.push({
      kernel: "groups-frame-cursors",
      rows: count,
      peersPerGroup: 5,
      beforeMs: median(before),
      afterMs: median(after),
    });
  }
  console.log(
    JSON.stringify(
      {
        baseline,
        node: process.version,
        method: "one warmup, median of three; output parity asserted; kernel-only timings",
        results,
      },
      null,
      2,
    ),
  );
} finally {
  await rm(directory, { recursive: true, force: true });
}
