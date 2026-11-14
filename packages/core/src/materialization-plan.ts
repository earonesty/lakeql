import { MATERIALIZATION_CHARGES } from "./allocation-charges.js";
import {
  type Batch,
  materializeBatchRow,
  materializeSelectedBatchRows,
  type Selection,
  type Vector,
  vectorLength,
} from "./batch.js";
import { decimalFormatAllocationBytes, requireDecimalValue } from "./decimal-value.js";
import { LakeqlError } from "./errors.js";
import type { MemoryBudget } from "./memory-budget.js";
import { MemoryScope, type OwnedMemory } from "./memory-scope.js";
import type { Row } from "./types.js";

export { MATERIALIZATION_CHARGES } from "./allocation-charges.js";

export interface MaterializationPlan {
  readonly rows: number;
  readonly allocationBytes: number;
  readonly retainedBytes: number;
  readonly retainsInput: boolean;
  /** Input vectors and selection must stay immutable from planning to execution. */
  execute(scope: MemoryScope): OwnedMemory<Row[]>;
}

/** Walk physical shape without decoding packed strings or constructing output
 * rows, timestamps, lists or maps. Counts upper bounds for transient map key/value
 * work as well as retained output; duplicate/null map keys can over-reserve.
 */
export function planBatchMaterialization(
  batch: Batch,
  selection?: Selection,
  scope?: MemoryScope,
): MaterializationPlan {
  return planMaterialization(batch, selection, undefined, scope);
}

/** Plan exactly one candidate without a batch-sized mask or traversal.
 * Execution returns a one-element row array under the same plan contract.
 */
export function planBatchRowMaterialization(
  batch: Batch,
  index: number,
  scope?: MemoryScope,
): MaterializationPlan {
  return planMaterialization(batch, undefined, index, scope);
}

function planMaterialization(
  batch: Batch,
  selection?: Selection,
  rowIndex?: number,
  parent?: MemoryScope,
): MaterializationPlan {
  if (
    rowIndex !== undefined &&
    (!Number.isSafeInteger(rowIndex) || rowIndex < 0 || rowIndex >= batch.rowCount)
  )
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid materialization row index");
  if (
    !Number.isSafeInteger(batch.rowCount) ||
    batch.rowCount < 0 ||
    batch.rowCount > 0xffffffff ||
    (selection !== undefined && selection.length !== batch.rowCount)
  )
    throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid materialization row extent");
  const planning = parent?.childScope();
  try {
    const cost = MATERIALIZATION_CHARGES;
    let retainsInput = false;
    let allocationBytes = cost.array,
      rows = 0;
    const add = (bytes: number) => {
      allocationBytes += bytes;
      if (!Number.isSafeInteger(bytes) || bytes < 0 || !Number.isSafeInteger(allocationBytes))
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "Materialization allocation extent exceeds safe capacity",
        );
    };
    let scratchBytes = 0;
    const scratch = (bytes: number) => {
      add(bytes);
      scratchBytes += bytes;
    };
    let fieldCount = 0;
    for (const name in batch.columns) {
      if (!Object.hasOwn(batch.columns, name)) continue;
      fieldCount++;
      add(cost.string + name.length * cost.stringCodeUnit);
      const vector = batch.columns[name];
      if (vector === undefined || vectorLength(vector) !== batch.rowCount)
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Materialization column length mismatch");
    }
    // Execution still constructs Object.entries descriptors; planning does not.
    scratch(cost.array + fieldCount * (cost.arraySlot + cost.array + 2 * cost.arraySlot));
    const active =
      planning === undefined
        ? new Set<Vector>()
        : planning.allocate(cost.object, "materialization traversal", () => new Set<Vector>())
            .value;
    function visit(vector: Vector, index: number): number {
      if (index < 0 || index >= vectorLength(vector) || !Number.isSafeInteger(index))
        throw new LakeqlError(
          "LAKEQL_VALIDATION_ERROR",
          "Materialization vector index out of bounds",
        );
      if (active.has(vector))
        throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Cyclic materialization vector");
      if ("valid" in vector && vector.valid?.[index] === 0) return 4;
      const frame = planning?.reserve(cost.property, "materialization traversal frame");
      try {
        active.add(vector);
        switch (vector.type) {
          case "union": {
            const child = vector.children[vector.tags[index] ?? -1];
            const offset = vector.offsets[index];
            if (child === undefined || offset === undefined)
              throw new LakeqlError("LAKEQL_VALIDATION_ERROR", "Invalid union row");
            return visit(child, offset);
          }
          case "null":
            return 4;
          case "bool":
            return 5;
          case "u8":
          case "i32":
          case "u32":
          case "f32":
          case "f64":
            add(cost.number);
            return 32;
          case "i64":
          case "u64":
            add(cost.bigint);
            return 32;
          case "timestamp":
          case "timestamp96":
            add(cost.timestamp + cost.bigint);
            return 64;
          case "decimal":
            retainsInput = true;
            return decimalFormatAllocationBytes(requireDecimalValue(vector.values[index]));
          case "utf8":
            // Legacy JS string vectors may not carry payload allocation charges.
            // The output owns immutable text; retaining the producer would also
            // pin unrelated decoded pages after their scope has finished.
            add(cost.string + (vector.values[index]?.length ?? 0) * cost.stringCodeUnit);
            return vector.values[index]?.length ?? 0;
          case "binary":
            if (vector.values[index] === undefined) add(cost.view);
            else retainsInput = true;
            return (vector.values[index]?.byteLength ?? 0) * 4;
          case "utf8-view":
          case "binary-view": {
            const row = (vector.rowOffset ?? 0) + index;
            const start =
              vector.ranges === undefined
                ? row * (vector.fixedLength ?? 0)
                : (vector.ranges[row * 2] ?? 0);
            const length =
              vector.ranges === undefined
                ? (vector.fixedLength ?? 0)
                : (vector.ranges[row * 2 + 1] ?? 0);
            if (
              !Number.isSafeInteger(start) ||
              !Number.isSafeInteger(length) ||
              start < 0 ||
              length < 0 ||
              start + length > vector.data.length
            )
              throw new LakeqlError(
                "LAKEQL_VALIDATION_ERROR",
                "Invalid materialization packed range",
              );
            if (vector.type === "utf8-view") scratch(cost.view);
            else add(cost.view);
            if (vector.type === "binary-view") {
              retainsInput = true;
              return length * 4;
            }
            // UTF-8 replacement decoding produces at most one UTF-16 unit per byte.
            add(cost.string + length * cost.stringCodeUnit);
            return length;
          }
          case "dict":
            return visit(vector.dictionary, vector.indices[index] ?? 0);
          case "list":
          case "map": {
            const start = vector.offsets[index] ?? 0,
              end = vector.offsets[index + 1] ?? start;
            const child = vector.type === "list" ? vector.child : vector.keys;
            if (
              start < 0 ||
              end < start ||
              end > vectorLength(child) ||
              (vector.type === "map" && end > vectorLength(vector.values))
            )
              throw new LakeqlError(
                "LAKEQL_VALIDATION_ERROR",
                "Invalid materialization child range",
              );
            if (vector.type === "list") {
              add(cost.array + (end - start) * cost.arraySlot);
              let keyUnits = Math.max(0, end - start - 1);
              for (let i = start; i < end; i++) keyUnits += visit(vector.child, i);
              return keyUnits;
            }
            add(cost.object + (end - start) * cost.property);
            for (let i = start; i < end; i++) {
              const keyUnits = visit(vector.keys, i);
              add(cost.string + keyUnits * cost.stringCodeUnit);
              visit(vector.values, i);
            }
            return 15; // String(object) => [object Object].
          }
          case "struct": {
            add(cost.object);
            for (const name in vector.fields) {
              if (!Object.hasOwn(vector.fields, name)) continue;
              add(cost.property + cost.string + name.length * cost.stringCodeUnit);
              visit(vector.fields[name] as Vector, index);
            }
            return 15;
          }
          default:
            throw new LakeqlError("LAKEQL_TYPE_ERROR", "Unsupported materialization vector shape");
        }
      } finally {
        active.delete(vector);
        frame?.release();
      }
    }
    for (
      let index = rowIndex ?? 0;
      index < (rowIndex === undefined ? batch.rowCount : rowIndex + 1);
      index++
    ) {
      if (selection !== undefined && selection[index] !== 1) continue;
      rows++;
      add(cost.arraySlot + cost.object + fieldCount * cost.property);
      for (const name in batch.columns)
        if (Object.hasOwn(batch.columns, name)) visit(batch.columns[name] as Vector, index);
    }
    return {
      rows,
      allocationBytes,
      retainedBytes: allocationBytes - scratchBytes,
      retainsInput,
      execute(scope) {
        const stage = scope.childStage(allocationBytes, "materialized output");
        try {
          const output = stage.allocate(allocationBytes - scratchBytes, "materialized rows", () =>
            rowIndex === undefined
              ? materializeSelectedBatchRows(batch, selection)
              : [materializeBatchRow(batch, rowIndex)],
          );
          return scope.adopt(output.retain());
        } finally {
          stage.dispose();
        }
      },
    };
  } finally {
    planning?.dispose();
  }
}

/** Explicit output ownership pins borrowed binary sources as well as new
 * output reservations. Release the owner only after all borrowed aliases expire.
 */
export function materializeOwnedBatchRows(
  input: OwnedMemory<Batch>,
  selection?: Selection,
  budget: MemoryBudget = input.budget,
): OwnedMemory<Row[]> {
  return materializeOwnedPlan(
    input,
    (scope) => planBatchMaterialization(input.value, selection, scope),
    budget,
    (rows) => rows,
  );
}

/** Materialize one candidate without a batch-sized selection mask or scan. */
export function materializeOwnedBatchRow(
  input: OwnedMemory<Batch>,
  index: number,
  budget: MemoryBudget = input.budget,
): OwnedMemory<Row> {
  return materializeOwnedPlan(
    input,
    (scope) => planBatchRowMaterialization(input.value, index, scope),
    budget,
    (rows) => rows[0] as Row,
  );
}

function materializeOwnedPlan<T>(
  input: OwnedMemory<Batch>,
  createPlan: (scope: MemoryScope) => MaterializationPlan,
  budget: MemoryBudget,
  select: (rows: Row[]) => T,
): OwnedMemory<T> {
  const scope = new MemoryScope(budget);
  try {
    const source = scope.adopt(input.retainIn(budget));
    const plan = createPlan(scope);
    const rows = plan.execute(scope);
    if (!plan.retainsInput) source.release();
    // The output allocation and input pins must outlive the temporary scope.
    const output = scope.retainAllocations(select(rows.value));
    return output;
  } finally {
    scope.dispose();
  }
}
