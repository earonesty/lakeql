// Internal typed-operator validation; does not relax the public struct schema gate.
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { DuckDBConnection } from "@duckdb/node-api";
import { planConcat } from "../packages/core/dist/concat-plan.js";
import { jsonSafeValue } from "../packages/core/dist/evaluator.js";
import { MemoryBudget, MemoryScope, materializeBatchRows } from "../packages/core/dist/index.js";
import { stableStringify } from "../packages/core/dist/manifest.js";
import { MemoryStageScheduler } from "../packages/core/dist/memory-stage-scheduler.js";
import { serializedBatchBytes } from "../packages/core/dist/serialized-size.js";
import { concatOwnedBatches, concatScheduledBatches } from "../packages/core/dist/vector-sort.js";
import { readParquetColumnBatch } from "../packages/parquet/dist/column-batches.js";
import { readParquetMetadataFromFile } from "../packages/parquet/dist/metadata-cache.js";
import { nestedDifference } from "./nested-value-comparison.mjs";

const connection = await DuckDBConnection.create();
try {
  for (const file of [
    "nested_structs.rust.parquet",
    "nonnullable.impala.parquet",
    "nullable.impala.parquet",
  ]) {
    const path = resolve("fixtures/external/parquet-testing/data", file);
    const bytes = await readFile(path);
    const source = {
      byteLength: bytes.length,
      async slice(start, end) {
        return bytes.buffer.slice(
          bytes.byteOffset + start,
          bytes.byteOffset + (end ?? bytes.length),
        );
      },
    };
    const metadata = await readParquetMetadataFromFile(source);
    const columns = [
      ...new Set(
        metadata.row_groups.flatMap((group) =>
          group.columns.map((column) => column.meta_data.path_in_schema[0]),
        ),
      ),
    ];
    const batches = [];
    for (let start = 0; start < Number(metadata.num_rows); start += 3)
      batches.push(
        await readParquetColumnBatch(
          source,
          metadata,
          columns,
          start,
          Math.min(start + 3, Number(metadata.num_rows)),
        ),
      );
    const referenceRows = (
      await connection.runAndReadAll(`SELECT * FROM read_parquet('${path.replaceAll("'", "''")}')`)
    ).getRowObjects();
    batches.push(...batches);
    const expected = [...referenceRows, ...referenceRows];
    console.log(
      JSON.stringify({
        file,
        phase: "decoded-input",
        difference: nestedDifference(batches.flatMap(materializeBatchRows), expected) ?? null,
      }),
    );
    let serializedBytes;
    const verifySize = (batch) => {
      const expectedBytes = new TextEncoder().encode(
        stableStringify(jsonSafeValue(materializeBatchRows(batch))),
      ).length;
      assert.equal(serializedBatchBytes(batch), expectedBytes);
      return expectedBytes;
    };
    const selectBatches = (dictionaryWrapped) =>
      dictionaryWrapped
        ? batches.map((batch) => ({
            rowCount: batch.rowCount,
            columns: Object.fromEntries(
              Object.entries(batch.columns).map(([name, vector]) => [
                name,
                {
                  type: "dict",
                  indices: Uint32Array.from({ length: batch.rowCount }, (_, i) => i),
                  dictionary: { ...vector },
                },
              ]),
            ),
          }))
        : batches;
    const run = (cap, dictionaryWrapped) => {
      const budget = new MemoryBudget(cap);
      const scope = new MemoryScope(budget);
      const selected = selectBatches(dictionaryWrapped);
      const inputs = selected.map((batch) => scope.retainAllocations(batch));
      for (const batch of selected) verifySize(batch);
      try {
        const output = concatOwnedBatches(inputs);
        try {
          assert.equal(nestedDifference(materializeBatchRows(output.value), expected), undefined);
          serializedBytes = verifySize(output.value);
          return budget.peakBytes;
        } finally {
          output.release();
        }
      } finally {
        for (const input of inputs) input.release();
        scope.dispose();
        assert.equal(budget.usedBytes, 0);
      }
    };
    const runScheduled = async (dictionaryWrapped) => {
      const selected = selectBatches(dictionaryWrapped);
      const plan = planConcat(selected);
      if (plan === undefined) {
        console.log(
          JSON.stringify({
            file,
            dictionaryWrapped,
            scheduled: false,
            reason: "No complete physical plan",
            scope: "Local fixture; scheduling capability gap",
          }),
        );
        return;
      }
      const root = new MemoryBudget(plan.bytes * 2);
      const scheduler = new MemoryStageScheduler(root);
      const source = new MemoryScope(root.child());
      const inputs = selected.map((batch) => source.retainAllocations(batch));
      const a = concatScheduledBatches(inputs, scheduler, root.child(plan.bytes));
      const b = concatScheduledBatches(inputs, scheduler, root.child(plan.bytes));
      for (const input of inputs) input.release();
      source.dispose();
      const outputs = await Promise.all([a, b]);
      try {
        for (const output of outputs) {
          assert.equal(nestedDifference(materializeBatchRows(output.value), expected), undefined);
          verifySize(output.value);
        }
        assert.equal(root.peakBytes, plan.bytes * 2);
      } finally {
        for (const output of outputs) output.release();
      }
      assert.equal(root.usedBytes, 0);
      const small = new MemoryBudget(plan.bytes - 1);
      const smallScope = new MemoryScope(small);
      const smallInputs = selected.map((batch) => smallScope.retainAllocations(batch));
      try {
        await assert.rejects(
          concatScheduledBatches(smallInputs, new MemoryStageScheduler(small), small.child()),
          /budget/,
        );
        assert.equal(small.peakBytes, 0);
      } finally {
        for (const input of smallInputs) input.release();
        smallScope.dispose();
      }
      assert.equal(small.usedBytes, 0);
      console.log(
        JSON.stringify({
          file,
          dictionaryWrapped,
          scheduled: true,
          concurrentJobs: 2,
          stageBytes: plan.bytes,
          peakTrackedBytes: root.peakBytes,
          finalBytes: root.usedBytes,
          scope:
            "Internal scheduled concat; local Parquet/DuckDB; borrowed decoder inputs; public struct gate bypassed; not remote or whole heap",
        }),
      );
    };
    for (const dictionaryWrapped of [false, true]) {
      await runScheduled(dictionaryWrapped);
      const peak = run(64 * 1024 * 1024, dictionaryWrapped);
      assert.equal(run(peak, dictionaryWrapped), peak);
      if (peak > 0) assert.throws(() => run(peak - 1, dictionaryWrapped), /budget|admitted/);
      console.log(
        JSON.stringify({
          file,
          rows: expected.length,
          batches: batches.length,
          peakTrackedOutputBytes: peak,
          dictionaryWrapped,
          verifiedSerializedBytes: serializedBytes,
          scope:
            "Internal struct concat, public schema gate bypassed; local fixture/DuckDB, borrowed decoder inputs; not remote or complete heap accounting",
        }),
      );
    }
  }
} finally {
  connection.disconnectSync();
}
