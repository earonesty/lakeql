import assert from "node:assert/strict";
import { schemaPathIndex } from "../packages/parquet/dist/schema-path-index.js";

const reports = [];
for (const depth of [100, 1000, 10000]) {
  const schema = Array.from({ length: depth }, (_, i) => ({ name: `g${i}`, num_children: 1 }));
  schema.push({ name: "leaf", type: "INT32" });
  const names = schema.slice(1).map((e) => e.name);
  const samples = [];
  for (let trial = 0; trial < 7; trial++) {
    const start = performance.now();
    const lookup = schemaPathIndex(schema);
    const nodes = lookup(names);
    const path = nodes.at(-1).path;
    const elapsed = performance.now() - start;
    assert.deepEqual(path, names);
    assert.equal(nodes[0].count, depth + 1);
    if (trial >= 2) samples.push(elapsed);
  }
  reports.push({
    depth,
    nodes: depth + 1,
    priorEagerPathSlots: (depth * (depth + 1)) / 2,
    requestedPathSlots: depth,
    samplesMs: samples,
    medianMs: [...samples].sort((a, b) => a - b)[2],
  });
}
console.log(
  JSON.stringify(
    {
      scope:
        "Local Node synthetic nested schema: index construction, full-depth lookup and one name-path materialization. Two warmups/five samples; slot counts are structural, not heap bytes.",
      node: process.version,
      reports,
    },
    null,
    2,
  ),
);
