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
