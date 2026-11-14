import { expect, it } from "vitest";
import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { type Batch, materializeBatchRows, type Vector } from "./batch.js";
import { col } from "./expr.js";
import { MemoryBudget } from "./memory-budget.js";
import { MemoryScope } from "./memory-scope.js";
import { vectorProjectBatch } from "./vector-project.js";

const descriptors =
  2 * c.object +
  2 * c.property +
  ["source", "alias"].reduce(
    (bytes, name) => bytes + c.property + c.string + name.length * c.stringCodeUnit,
    0,
  );

const shapes: Vector[] = [
  { type: "i32", values: Int32Array.of(1, 2), valid: Uint8Array.of(1, 0) },
  { type: "utf8-view", data: new TextEncoder().encode("ab"), fixedLength: 1, length: 2 },
  { type: "binary-view", data: Uint8Array.of(0, 255), fixedLength: 1, length: 2 },
  { type: "dict", indices: Uint32Array.of(1, 0), dictionary: { type: "utf8", values: ["a", "b"] } },
  {
    type: "list",
    offsets: Uint32Array.of(0, 1, 2),
    child: { type: "i32", values: Int32Array.of(3, 4) },
  },
];
it.each(shapes)("preserves alias shape $type without allocating output backing", (vector) => {
  const batch: Batch = { rowCount: 2, columns: { source: vector } };
  const scope = new MemoryScope(new MemoryBudget(descriptors));
  const projected = vectorProjectBatch(batch, ["source"], { alias: col("source") }, scope);
  expect(projected.columns.alias).toBe(vector);
  expect(materializeBatchRows(projected)).toEqual(
    materializeBatchRows(batch).map((row) => ({
      source: row.source,
      alias: row.source,
    })),
  );
  expect(scope.budget.peakBytes).toBe(descriptors);
  scope.dispose();
});

it("retains aliased input through the execution-stage owner without a second charge", () => {
  const scope = new MemoryScope(new MemoryBudget(8 + descriptors));
  const values = scope.allocate(8, "input", () => Int32Array.of(1, 2)).value;
  const batch: Batch = { rowCount: 2, columns: { source: { type: "i32", values } } };
  const projected = vectorProjectBatch(batch, ["source"], { alias: col("source") }, scope);
  const owner = scope.retainAllocations(projected);
  scope.dispose();
  expect(scope.budget.usedBytes).toBe(8 + descriptors);
  expect(materializeBatchRows(owner.value)).toEqual([
    { source: 1, alias: 1 },
    { source: 2, alias: 2 },
  ]);
  owner.release();
  expect(scope.budget.usedBytes).toBe(0);
});
