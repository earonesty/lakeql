import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { batchFromColumns } from "../packages/core/dist/batch.js";
import { CpuPhysicalBackend, physicalInputFromBatch } from "../packages/core/dist/physical.js";
import {
  finalizeVectorGroupByRows,
  restoreVectorGroupByState,
} from "../packages/core/dist/vector-group-by.js";

const results = [];
const aggregates = { rows: { op: "count" } };
for (const rows of [2000, 4000, 8000]) {
  for (const batchSize of [128, 512]) {
    const backend = new CpuPhysicalBackend();
    let priorOutput;
    let restoredGroups = 0,
      expectedVisits = 0,
      ms = 0;
    for (let offset = 0; offset < rows; offset += batchSize) {
      const batch = batchFromColumns({
        key: Array.from({ length: Math.min(batchSize, rows - offset) }, (_, i) => offset + i),
      });
      const fragment = {
        id: "grouped-continuation",
        input: physicalInputFromBatch(batch),
        operators: [{ kind: "grouped-reduce", keys: ["key"], aggregates }],
        output: { kind: "grouped-aggregate-snapshot" },
        estimates: {
          rowCount: batch.rowCount,
          inputBytes: batch.rowCount * 8,
          outputBytes: batch.rowCount * 8,
        },
      };
      if (priorOutput) {
        expectedVisits += offset;
        priorOutput = {
          ...priorOutput,
          snapshot: {
            ...priorOutput.snapshot,
            groups: priorOutput.snapshot.groups.map((group) => ({
              ...group,
              get keyValues() {
                restoredGroups++;
                return group.keyValues;
              },
            })),
          },
        };
      }
      const compiled = await backend.compile(fragment);
      const started = performance.now();
      const result = await backend.execute(
        compiled,
        { kind: "batch", batch },
        {
          ...(priorOutput ? { priorOutput } : {}),
          budget: { maxMemoryBytes: 64 * 1024 * 1024 },
        },
      );
      ms += performance.now() - started;
      assert.equal(result.output.kind, "grouped-aggregate-snapshot");
      priorOutput = result.output;
    }
    assert.equal(restoredGroups, expectedVisits);
    const output = finalizeVectorGroupByRows(
      restoreVectorGroupByState(["key"], aggregates, priorOutput.snapshot),
    );
    assert.equal(output.length, rows);
    for (let i = 0; i < rows; i++) assert.deepEqual(output[i], { key: i, rows: 1 });
    results.push({ rows, batchSize, restoredGroups, ms });
  }
}
writeFileSync(
  "docs/benchmarks/grouped-batch-continuation-2026-11-13.json",
  `${JSON.stringify(
    {
      description:
        "CPU physical grouped-aggregate snapshot continuation. Counts prior-group key reads during restore; validates all final groups. One timing sample, compilation/instrumentation excluded. This is not an owned-state, cloud latency or heap-budget benchmark.",
      results,
    },
    null,
    2,
  )}\n`,
);
console.log(results);
