import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Batch, batchFromColumns, materializeBatchRows, vectorValue } from "./batch.js";
import { DecimalValue, decimalFromParts } from "./decimal-value.js";
import { createInMemoryLake } from "./in-memory.js";
import { materializeOwnedBatchRows } from "./materialization-plan.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { selectVectorIndices } from "./vector-selection.js";
import { sliceVector } from "./vector-slice.js";
import { concatOwnedBatches, gatherOwnedBatch } from "./vector-sort.js";

const a = decimalFromParts(123456789987654321n, 9);
const b = decimalFromParts(-9876543210123456789n, 9);

it("preserves a distinct decimal shape across nullable, nested and union batches", () => {
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const batch = batchFromColumns(
    {
      value: [a, null, b],
      nested: [[a], [], [b, null]],
      mixed: [a, "text", b],
    },
    scope,
  );
  expect(batch.columns.value?.type).toBe("decimal");
  expect(batch.columns.mixed?.type).toBe("union");
  expect(materializeBatchRows(batch)).toEqual([
    { value: a, nested: [a], mixed: a },
    { value: null, nested: [], mixed: "text" },
    { value: b, nested: [b, null], mixed: b },
  ]);
  expect(vectorValue(column(batch), 0)).toBeInstanceOf(DecimalValue);
  expect(vectorValue(sliceVector(column(batch), 1, 3), 1)).toEqual(b);
  scope.dispose();
  expect(pool.usedBytes).toBe(0);
});

it("keeps decimal materialization live after the producer closes", () => {
  const pool = new MemoryBudget(65536);
  const producer = new MemoryScope(pool);
  const input = producer.retainAllocations(batchFromColumns({ value: [a, null, b] }, producer));
  producer.dispose();
  const output = materializeOwnedBatchRows(input);
  input.release();
  expect(output.value).toEqual([{ value: a }, { value: null }, { value: b }]);
  expect(output.value[0]?.value).toBeInstanceOf(DecimalValue);
  expect(pool.usedBytes).toBe(output.bytes);
  output.release();
  expect(pool.usedBytes).toBe(0);
});

it("gathers and concatenates owned decimals without losing class, scale or nulls", () => {
  const pool = new MemoryBudget(1024 * 1024);
  const producer = new MemoryScope(pool);
  const input = producer.retainAllocations(
    batchFromColumns(
      {
        value: Array.from({ length: 100 }, (_, i) => (i % 3 === 0 ? null : i % 2 ? a : b)),
      },
      producer,
    ),
  );
  producer.dispose();
  const sourceBytes = input.bytes;
  const selected = gatherOwnedBatch(input, [1, 0, 2]);
  expect(selected.bytes).toBeLessThan(sourceBytes);
  input.release();
  expect(materializeBatchRows(selected.value)).toEqual([
    { value: a },
    { value: null },
    { value: b },
  ]);
  const merged = concatOwnedBatches([selected, selected]);
  selected.release();
  expect(materializeBatchRows(merged.value)).toEqual([
    { value: a },
    { value: null },
    { value: b },
    { value: a },
    { value: null },
    { value: b },
  ]);
  expect(vectorValue(column(merged.value), 0)).toBeInstanceOf(DecimalValue);
  merged.release();
  expect(pool.usedBytes).toBe(0);
});

it("admits decimal vector construction at the measured peak and rejects one byte short", () => {
  const values = { value: [a, null, b] };
  const probe = new MemoryScope(new MemoryBudget(65536));
  batchFromColumns(values, probe);
  const peak = probe.budget.peakBytes;
  probe.dispose();
  const exact = new MemoryScope(new MemoryBudget(peak));
  expect(materializeBatchRows(batchFromColumns(values, exact))).toEqual([
    { value: a },
    { value: null },
    { value: b },
  ]);
  exact.dispose();
  const tight = new MemoryScope(new MemoryBudget(peak - 1));
  expect(() => batchFromColumns(values, tight)).toThrow(/budget/i);
  tight.dispose();
  expect(tight.budget.usedBytes).toBe(0);
});

function column(batch: Batch) {
  const value = batch.columns.value;
  if (value === undefined) throw new Error("Missing test column");
  return value;
}

it("projects exact decimal rows through the normal query output path", async () => {
  const pool = new MemoryBudget(65536);
  const lake = createInMemoryLake(
    { rows: [{ value: a }, { value: null }, { value: b }] },
    { memoryPool: pool },
  );
  const rows = await lake.path("rows").select(["value"]).run().toArray();
  expect(rows).toEqual([{ value: a }, { value: null }, { value: b }]);
  expect(rows[0]?.value).toBeInstanceOf(DecimalValue);
  expect(pool.usedBytes).toBe(0);
});

it("rejects malformed decimal values and invalid selected indices", () => {
  const batch = batchFromColumns({ value: [a] });
  for (const index of [-1, 1, 0.5, NaN]) {
    const selected = selectVectorIndices(column(batch), [index]);
    expect(() => selected.forEach(() => {})).toThrow(/index/);
  }
  expect(() => vectorValue({ type: "decimal", values: new Array(1) }, 0)).toThrow(/decimal vector/);
});

it("shares one index buffer across decimal columns when detached output does not fit", () => {
  const values = { value: [a, null, b], other: [b, a, null] };
  const probe = new MemoryScope(new MemoryBudget(65536));
  batchFromColumns(values, probe);
  const peak = probe.budget.peakBytes;
  probe.dispose();
  const pool = new MemoryBudget(peak + 12 + c.view);
  const producer = new MemoryScope(pool);
  const input = producer.retainAllocations(batchFromColumns(values, producer));
  producer.dispose();
  const held = pool.usedBytes;
  const selected = gatherOwnedBatch(input, [2, 1, 0]);
  input.release();
  expect(pool.usedBytes).toBe(held + 12 + c.view);
  const first = selected.value.columns.value;
  const second = selected.value.columns.other;
  expect(first?.type).toBe("dict");
  expect(second?.type).toBe("dict");
  if (first?.type === "dict" && second?.type === "dict") expect(first.indices).toBe(second.indices);
  expect(materializeBatchRows(selected.value)).toEqual([
    { value: b, other: null },
    { value: null, other: a },
    { value: a, other: b },
  ]);
  selected.release();
  expect(pool.usedBytes).toBe(0);
});

it("concatenates null-only inputs and mixed-domain decimal unions", () => {
  const pool = new MemoryBudget(65536);
  const scope = new MemoryScope(pool);
  const sources = [
    scope.retainAllocations(batchFromColumns({ value: [null] }, scope)),
    scope.retainAllocations(batchFromColumns({ value: [a, "text", b] }, scope)),
  ];
  scope.dispose();
  const merged = concatOwnedBatches(sources);
  for (const source of sources) source.release();
  expect(materializeBatchRows(merged.value)).toEqual([
    { value: null },
    { value: a },
    { value: "text" },
    { value: b },
  ]);
  merged.release();
  expect(pool.usedBytes).toBe(0);
});
