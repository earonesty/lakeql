import assert from "node:assert/strict";
import { vectorValue } from "../packages/core/dist/batch.js";
import { MemoryBudget } from "../packages/core/dist/memory-budget.js";
import { MemoryScope } from "../packages/core/dist/memory-scope.js";
import {
  serializedBatchBytes,
  serializedValueBytes,
} from "../packages/core/dist/serialized-size.js";

for (const shape of ["list", "map"])
  for (const rowCount of [2000, 20000]) {
    const entries = rowCount * 16;
    const child = { type: "i32", values: Int32Array.from({ length: entries }, (_, i) => i % 100) };
    const offsets = Int32Array.from({ length: rowCount + 1 }, (_, i) => i * 16);
    const payload =
      shape === "list"
        ? { type: "list", offsets, child }
        : {
            type: "map",
            offsets,
            keys: {
              type: "dict",
              indices: Uint32Array.from({ length: entries }, (_, i) => i % 4),
              dictionary: { type: "utf8", values: ["a", "b", "c", "d"] },
            },
            values: child,
          };
    const batch = { rowCount, columns: { payload } };
    const prior = () => {
      let bytes = 2 + Math.max(0, rowCount - 1) + rowCount * 12;
      for (let row = 0; row < rowCount; row++)
        bytes += serializedValueBytes(vectorValue(payload, row));
      return bytes;
    };
    const expected = prior();
    const medianMs = {};
    const budget = new MemoryBudget(128);
    const scope = new MemoryScope(budget);
    for (const [name, run] of [
      ["cellMaterialization", prior],
      ["physicalVisitor", () => serializedBatchBytes(batch)],
      ["chargedVisitor", () => serializedBatchBytes(batch, scope)],
    ]) {
      const samples = [];
      for (let trial = 0; trial < 8; trial++) {
        const start = performance.now();
        const bytes = run();
        const elapsed = performance.now() - start;
        assert.equal(bytes, expected);
        assert.equal(budget.usedBytes, 0);
        if (trial >= 3) samples.push(elapsed);
      }
      samples.sort((a, b) => a - b);
      medianMs[name] = samples[2];
    }
    scope.dispose();
    console.log(
      JSON.stringify({
        shape,
        rowCount,
        entries,
        serializedBytes: expected,
        medianMs,
        scope:
          "Local serialized-size accounting, five-sample medians; typed map index storage capped at 128 bytes in chargedVisitor; no cloud/query-latency/whole-heap claim",
      }),
    );
  }
