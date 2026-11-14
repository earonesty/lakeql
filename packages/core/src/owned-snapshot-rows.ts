import { MATERIALIZATION_CHARGES as c } from "./allocation-charges.js";
import { isDecimalValue } from "./decimal-value.js";
import { LakeqlError } from "./errors.js";
import { jsonSafeValue } from "./evaluator.js";
import { intervalStringAllocationBytes, isIntervalValue } from "./interval.js";
import type { MemoryScope, OwnedMemory } from "./memory-scope.js";
import { scalarStringAllocationBytes } from "./scalar-cast-allocation.js";
import { serializedValueBytes } from "./serialized-size.js";
import { isTimestampValue } from "./timestamp.js";
import type { Row } from "./types.js";

type SnapshotScalar = null | string | number | boolean;

/** Snapshot rows contain JSON scalars. Admit containers and scalar formatting
 * before construction, without recursively normalizing rejected nested values.
 */
export function snapshotOwnedRows(
  rows: readonly Row[],
  parent: MemoryScope,
): OwnedMemory<Record<string, SnapshotScalar>[]> {
  const scope = parent.childScope();
  try {
    const output = scope.allocate(
      c.array + rows.length * c.arraySlot,
      "snapshot rows",
      () => new Array<Record<string, SnapshotScalar>>(rows.length),
    ).value;
    for (let index = 0; index < rows.length; index++) {
      const row = rows[index];
      if (row === undefined)
        throw new LakeqlError("LAKEQL_TYPE_ERROR", "Snapshot rows must be dense");
      const record = scope.allocate(
        c.object,
        "snapshot record",
        () => Object.create(null) as Record<string, SnapshotScalar>,
      ).value;
      for (const key in row) {
        if (!Object.hasOwn(row, key)) continue;
        const value = row[key];
        scope.allocate(
          c.property + c.string + key.length * c.stringCodeUnit,
          "snapshot scalar property",
          () => {
            record[key] = snapshotScalarWithAdmission(value, scope);
          },
        );
      }
      output[index] = record;
    }
    return scope.retainAllocations(output);
  } finally {
    scope.dispose();
  }
}

function snapshotScalarAllocationBytes(value: unknown): number {
  let bytes = 0;
  if (typeof value === "string") bytes = c.string + value.length * c.stringCodeUnit;
  else if (isIntervalValue(value)) bytes = intervalStringAllocationBytes(value);
  else if (
    typeof value === "bigint" ||
    value instanceof Uint8Array ||
    isDecimalValue(value) ||
    isTimestampValue(value)
  )
    bytes = scalarStringAllocationBytes(value);
  else if (value !== null && typeof value !== "number" && typeof value !== "boolean")
    throw new LakeqlError(
      "LAKEQL_TYPE_ERROR",
      "Aggregate operator state values must be JSON scalars",
    );
  return bytes;
}

export function snapshotScalarWithAdmission(value: unknown, scope: MemoryScope): SnapshotScalar {
  return scope.allocate(
    snapshotScalarAllocationBytes(value),
    "snapshot scalar conversion",
    () => jsonSafeValue(value) as SnapshotScalar,
  ).value;
}

/** Match scalar snapshot serialization without building a normalized record.
 * Bytes and text can be measured directly; native scalar formatting is admitted.
 */
export function snapshotScalarSerializedBytes(value: unknown, scope: MemoryScope): number {
  const bytes = snapshotScalarAllocationBytes(value);
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "string" ||
    value instanceof Uint8Array
  )
    return serializedValueBytes(value);
  const scratch = scope.reserve(Math.max(bytes, c.string + 64), "snapshot scalar size scratch");
  try {
    return serializedValueBytes(value);
  } finally {
    scratch.release();
  }
}
