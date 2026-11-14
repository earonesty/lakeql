// CPU traversal benchmark; run after building core.
import assert from "node:assert/strict";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { WindowPeers } from "../packages/core/dist/window-peers.js";

const reports = [];
for (const count of [5000, 20000]) {
  const partition = Array.from({ length: count }, (_, index) => ({
    originalIndex: index,
    row: {},
    orderKey: [Math.floor(index / 5)],
    partitionKey: [],
  }));
  const groups = count / 5;
  const run = (indexed) => {
    const budget = new MemoryBudget((groups + 1) * 4);
    const peers = new WindowPeers(partition, indexed ? budget : undefined);
    let sum = 0;
    try {
      for (let query = 0; query < 1000; query += 1) {
        sum += peers.group(query % 2 ? 0 : groups - 1, "start").start;
      }
      assert.equal(sum, 500 * (count - 5));
    } finally {
      peers.dispose();
    }
    assert.equal(budget.usedBytes, 0);
    assert.equal(budget.peakBytes, indexed ? (groups + 1) * 4 : 0);
  };
  const measure = (indexed) => {
    run(indexed);
    const samples = [];
    for (let sample = 0; sample < 5; sample += 1) {
      const start = performance.now();
      run(indexed);
      samples.push(performance.now() - start);
    }
    return samples.sort((a, b) => a - b)[2];
  };
  reports.push({
    rows: count,
    queries: 1000,
    cursorMs: measure(false),
    adaptiveMs: measure(true),
    indexBytes: (groups + 1) * 4,
  });
}
console.log(JSON.stringify({ node: process.version, reports }, null, 2));
