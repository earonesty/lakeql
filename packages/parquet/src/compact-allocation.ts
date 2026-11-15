import type { DataReader, ThriftObject } from "hyparquet/src/types.js";
import { LakeqlError, MATERIALIZATION_CHARGES, type MemoryScope } from "lakeql-core";
import {
  type CompactAllocation,
  type CompactProjection,
  type CompactStructureCounts,
  inspectCompactAllocations,
  readCompactHeader,
} from "./compact-header.js";

/** Reservation model for the selected raw compact tree, not portable VM sizes.
 * Binary bytes are borrowed; only their views are new. Field keys use the longest
 * signed i32 field name. Scalar slots conservatively allow bigint boxing.
 * Conversion visitors must admit their own output separately.
 */
export function planCompactAllocation(reader: DataReader, projection?: CompactProjection): number {
  const counts = inspectCompactAllocations(
    { view: reader.view, offset: reader.offset },
    projection,
  );
  const cost = MATERIALIZATION_CHARGES;
  const bytes =
    counts.structs * cost.object +
    counts.fields * (cost.property + cost.string + 17 * cost.stringCodeUnit + cost.bigint) +
    counts.lists * cost.array +
    counts.listSlots * (cost.arraySlot + cost.bigint) +
    counts.binaryViews * cost.view;
  if (!Number.isSafeInteger(bytes) || bytes < 0)
    throw new LakeqlError(
      "LAKEQL_VALIDATION_ERROR",
      "Compact allocation extent exceeds safe capacity",
    );
  return bytes;
}

/** Admit before raw tree construction; the scope keeps its reservation alive.
 * Preflight does not invoke conversion visitors or decode borrowed text.
 */
export function readAdmittedCompactHeader(
  reader: DataReader,
  scope: MemoryScope,
  projection?: CompactProjection,
  counts?: CompactStructureCounts,
  admit?: (allocation: CompactAllocation) => void,
): ThriftObject {
  const bytes = planCompactAllocation(reader, projection);
  return scope.allocate(bytes, "raw compact metadata", () =>
    readCompactHeader(reader, projection, counts, admit),
  ).value;
}

/** Consume a previously validated compact struct range with one raw-entry stage.
 * Consumers convert into their output scope and transfer any surviving raw aliases
 * from scratch before returning. Sibling entries do not retain raw trees.
 */
export function consumeCompactRange<T>(
  view: DataView,
  start: number,
  end: number,
  projection: CompactProjection,
  scope: MemoryScope,
  consume: (value: ThriftObject, scratch: MemoryScope) => T,
  admit?: (allocation: CompactAllocation) => void,
): T {
  if (
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(end) ||
    start < 0 ||
    end <= start ||
    end > view.byteLength
  )
    throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid compact metadata range");
  const reader = { view, offset: start };
  const scratch = scope.childStage(planCompactAllocation(reader, projection), "raw compact entry");
  try {
    const value = readCompactHeader(reader, projection, undefined, admit);
    if (reader.offset !== end)
      throw new LakeqlError("LAKEQL_PARQUET_READ_ERROR", "Invalid compact metadata extent");
    return consume(value, scratch);
  } finally {
    scratch.dispose();
  }
}
